from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.gateway import (
    AgentAuthorizationError,
    AgentInputError,
    AgentMarketGateway,
    AgentRateLimitError,
    FileSystemAgentGatewayStore,
)
from cashbox.ingest import FileSystemMarketStore
from cashbox.research import ResearchMarketReadPath


def _sample_market(
    *,
    market_id: str,
    question: str,
    category: str = "Politics",
    active: bool = True,
    closed: bool = False,
    volume: str = "1000",
    liquidity: str = "500",
) -> dict[str, object]:
    return {
        "id": f"raw-{market_id}",
        "slug": market_id,
        "eventSlug": "us-election",
        "question": question,
        "category": category,
        "active": active,
        "closed": closed,
        "archived": False,
        "enableOrderBook": True,
        "outcomes": json.dumps(["Yes", "No"]),
        "clobTokenIds": json.dumps([f"{market_id}-yes", f"{market_id}-no"]),
        "liquidity": liquidity,
        "volume": volume,
        "endDate": "2026-11-05T00:00:00Z",
    }


class AgentGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.market_store = FileSystemMarketStore(self.root)
        self.gateway_store = FileSystemAgentGatewayStore(self.root)
        self.read_path = ResearchMarketReadPath(self.market_store)
        self.gateway = AgentMarketGateway(self.gateway_store, self.read_path)
        self.market_store.ingest_market_payloads(
            [
                _sample_market(market_id="election-2028", question="Will X win in 2028?", category="Politics"),
                _sample_market(market_id="btc-150k", question="Will BTC hit 150k?", category="Crypto"),
            ],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        self.market_store.ingest_order_book_snapshots(
            [
                {
                    "token_id": "btc-150k-yes",
                    "timestamp": "2026-04-24T10:04:00Z",
                    "bids": [["0.42", "100"]],
                    "asks": [["0.45", "70"]],
                }
            ],
            received_at=datetime(2026, 4, 24, 10, 4, tzinfo=timezone.utc),
        )
        self.market_store.ingest_clob_trades(
            [
                {
                    "id": "trade-001",
                    "token_id": "btc-150k-yes",
                    "timestamp": "2026-04-24T10:04:30Z",
                    "price": "0.44",
                    "size": "12",
                    "side": "BUY",
                }
            ],
            received_at=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_gateway_calls_read_tool_and_audits_request(self) -> None:
        self.gateway.issue_read_only_credential(subject="hermes", token="test-token")

        result = self.gateway.call_tool(
            "list_active_markets",
            {"query": "btc", "limit": 1},
            token="test-token",
            user_id="hermes",
            session_id="session-001",
            now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )

        self.assertTrue(result["ok"])
        self.assertEqual([item["market_id"] for item in result["result"]], ["btc-150k"])

        audit_rows = self.gateway_store.load_audit_records()
        self.assertEqual(len(audit_rows), 1)
        self.assertEqual(audit_rows[0]["tool_name"], "list_active_markets")
        self.assertEqual(audit_rows[0]["status"], "ok")
        self.assertEqual(audit_rows[0]["subject"], "hermes")

    def test_gateway_exposes_clob_read_tools(self) -> None:
        self.gateway.issue_read_only_credential(subject="hermes", token="test-token")

        top = self.gateway.call_tool(
            "get_top_of_book",
            {"token_id": "btc-150k-yes", "at": "2026-04-24T10:05:00Z"},
            token="test-token",
            user_id="hermes",
            session_id="session-001",
            now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )
        trades = self.gateway.call_tool(
            "get_trade_history",
            {"market_id": "btc-150k", "start": "2026-04-24T10:00:00Z", "end": "2026-04-24T10:10:00Z"},
            token="test-token",
            user_id="hermes",
            session_id="session-001",
            now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(top["result"]["best_bid"]["price"], "0.42")
        self.assertEqual([trade["trade_id"] for trade in trades["result"]], ["trade-001"])

    def test_gateway_rejects_unscoped_tools(self) -> None:
        self.gateway.issue_read_only_credential(
            subject="hermes",
            allowed_tools=("get_ingest_health",),
            token="test-token",
        )

        with self.assertRaises(AgentAuthorizationError):
            self.gateway.call_tool(
                "get_market_metadata",
                {"market_id": "btc-150k"},
                token="test-token",
                user_id="hermes",
                session_id="session-001",
                now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
            )

        audit_rows = self.gateway_store.load_audit_records()
        self.assertEqual(audit_rows[-1]["status"], "authorization_failed")

    def test_gateway_rate_limits_credentials(self) -> None:
        self.gateway.issue_read_only_credential(
            subject="hermes",
            token="test-token",
            rate_limit_count=1,
            rate_limit_window_seconds=60,
        )

        self.gateway.call_tool(
            "get_ingest_health",
            {},
            token="test-token",
            user_id="hermes",
            session_id="session-001",
            now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )

        with self.assertRaises(AgentRateLimitError):
            self.gateway.call_tool(
                "get_ingest_health",
                {},
                token="test-token",
                user_id="hermes",
                session_id="session-001",
                now=datetime(2026, 4, 24, 10, 5, 30, tzinfo=timezone.utc),
            )

        audit_rows = self.gateway_store.load_audit_records()
        self.assertEqual(audit_rows[-1]["status"], "rate_limited")

    def test_gateway_blocks_suspicious_arguments(self) -> None:
        self.gateway.issue_read_only_credential(subject="hermes", token="test-token")

        with self.assertRaises(AgentInputError):
            self.gateway.call_tool(
                "list_active_markets",
                {"query": "$(cat /etc/passwd)"},
                token="test-token",
                user_id="hermes",
                session_id="session-001",
                now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
            )

        audit_rows = self.gateway_store.load_audit_records()
        self.assertEqual(audit_rows[-1]["status"], "invalid_arguments")

    def test_gateway_returns_structured_error_for_unknown_market(self) -> None:
        self.gateway.issue_read_only_credential(subject="hermes", token="test-token")

        with self.assertRaises(AgentInputError):
            self.gateway.call_tool(
                "get_market_metadata",
                {"market_id": "missing-market"},
                token="test-token",
                user_id="hermes",
                session_id="session-001",
                now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
            )

        audit_rows = self.gateway_store.load_audit_records()
        self.assertEqual(audit_rows[-1]["status"], "invalid_arguments")


if __name__ == "__main__":
    unittest.main()
