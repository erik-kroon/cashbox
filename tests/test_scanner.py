from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from cashbox.cli import run_live_scan_loop
from cashbox.models import BinaryMarketSnapshot, FeeSchedule, RiskBuffer
from cashbox.polymarket import _best_level, _parse_binary_market
from cashbox.scanner import scan_market, scan_snapshots


class ScannerTests(unittest.TestCase):
    def test_fee_schedule_uses_polymarket_style_formula(self) -> None:
        schedule = FeeSchedule.for_category("crypto")

        fee = schedule.taker_fee(shares=Decimal("10"), price=Decimal("0.5"))

        self.assertEqual(fee, Decimal("0.1800"))

    def test_detects_buy_full_set_when_edge_survives_fees_and_buffers(self) -> None:
        market = BinaryMarketSnapshot.from_dict(
            {
                "market_id": "market-1",
                "category": "geopolitics",
                "yes": {"bid": "0.49", "ask": "0.46", "bid_size": "100", "ask_size": "75"},
                "no": {"bid": "0.50", "ask": "0.52", "bid_size": "90", "ask_size": "80"},
            }
        )

        opportunities = scan_market(
            market,
            risk=RiskBuffer.from_values(slippage="0.002", precision_buffer="0.001", safety_margin="0.003"),
        )

        self.assertEqual(len(opportunities), 1)
        opportunity = opportunities[0]
        self.assertEqual(opportunity.side, "buy_full_set")
        self.assertEqual(opportunity.quantity, Decimal("75"))
        self.assertEqual(opportunity.net_edge_per_share, Decimal("0.01400"))
        self.assertEqual(opportunity.expected_pnl, Decimal("1.05000"))

    def test_filters_out_edges_consumed_by_fees(self) -> None:
        market = BinaryMarketSnapshot.from_dict(
            {
                "market_id": "market-2",
                "category": "crypto",
                "yes": {"bid": "0.50", "ask": "0.49", "bid_size": "100", "ask_size": "100"},
                "no": {"bid": "0.49", "ask": "0.50", "bid_size": "100", "ask_size": "100"},
            }
        )

        opportunities = scan_market(market)

        self.assertEqual(opportunities, [])

    def test_scan_snapshots_ranks_highest_expected_pnl_first(self) -> None:
        high_pnl = BinaryMarketSnapshot.from_dict(
            {
                "market_id": "high-pnl",
                "category": "geopolitics",
                "yes": {"bid": "0.40", "ask": "0.44", "bid_size": "100", "ask_size": "80"},
                "no": {"bid": "0.55", "ask": "0.48", "bid_size": "100", "ask_size": "80"},
            }
        )
        low_pnl = BinaryMarketSnapshot.from_dict(
            {
                "market_id": "low-pnl",
                "category": "geopolitics",
                "yes": {"bid": "0.45", "ask": "0.47", "bid_size": "20", "ask_size": "10"},
                "no": {"bid": "0.50", "ask": "0.50", "bid_size": "20", "ask_size": "10"},
            }
        )

        opportunities = scan_snapshots([low_pnl, high_pnl])

        self.assertEqual([opportunity.market_id for opportunity in opportunities], ["high-pnl", "low-pnl"])
        self.assertGreater(opportunities[0].expected_pnl, opportunities[1].expected_pnl)

    def test_parse_binary_market_decodes_json_encoded_fields(self) -> None:
        market = _parse_binary_market(
            {
                "id": "123",
                "slug": "example-market",
                "question": "Example market?",
                "category": "Politics",
                "active": True,
                "closed": False,
                "enableOrderBook": True,
                "outcomes": "[\"Yes\", \"No\"]",
                "clobTokenIds": "[\"yes-token\", \"no-token\"]",
            }
        )

        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual(market.market_id, "example-market")
        self.assertEqual(market.yes_token_id, "yes-token")
        self.assertEqual(market.no_token_id, "no-token")

    def test_best_level_uses_price_not_array_position(self) -> None:
        bid = _best_level(
            [
                {"price": "0.10", "size": "5"},
                {"price": "0.52", "size": "12"},
                {"price": "0.40", "size": "8"},
            ],
            side="bid",
        )
        ask = _best_level(
            [
                {"price": "0.90", "size": "5"},
                {"price": "0.53", "size": "12"},
                {"price": "0.72", "size": "8"},
            ],
            side="ask",
        )

        self.assertEqual(bid, (Decimal("0.52"), Decimal("12")))
        self.assertEqual(ask, (Decimal("0.53"), Decimal("12")))

    def test_live_scan_loop_keeps_running_after_poll_error(self) -> None:
        high_pnl = BinaryMarketSnapshot.from_dict(
            {
                "market_id": "high-pnl",
                "category": "geopolitics",
                "yes": {"bid": "0.40", "ask": "0.44", "bid_size": "100", "ask_size": "80"},
                "no": {"bid": "0.55", "ask": "0.48", "bid_size": "100", "ask_size": "80"},
            }
        )
        low_pnl = BinaryMarketSnapshot.from_dict(
            {
                "market_id": "low-pnl",
                "category": "geopolitics",
                "yes": {"bid": "0.45", "ask": "0.47", "bid_size": "20", "ask_size": "10"},
                "no": {"bid": "0.50", "ask": "0.50", "bid_size": "20", "ask_size": "10"},
            }
        )
        outputs: list[str] = []
        sleeps: list[float] = []
        scan_times = iter(
            [
                datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 24, 10, 0, 5, tzinfo=timezone.utc),
            ]
        )
        responses = iter([RuntimeError("temporary failure"), [low_pnl, high_pnl]])

        def loader(*, limit: int, offset: int, category: str | None) -> list[BinaryMarketSnapshot]:
            response = next(responses)
            if isinstance(response, Exception):
                raise response
            return response

        exit_code = run_live_scan_loop(
            risk=RiskBuffer(),
            limit=25,
            offset=0,
            category=None,
            poll_interval=5.0,
            max_iterations=2,
            snapshot_loader=loader,
            output=outputs.append,
            sleep=sleeps.append,
            clock=lambda: next(scan_times),
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(sleeps, [5.0])
        self.assertEqual(
            outputs,
            [
                "scan=1 at=2026-04-24T10:00:00Z status=error error=temporary failure",
                "scan=2 at=2026-04-24T10:00:05Z opportunities=2",
                "1. high-pnl buy_full_set qty=80 gross=0.080000 net=0.080000 pnl=6.400000",
                "2. low-pnl buy_full_set qty=10 gross=0.030000 net=0.030000 pnl=0.300000",
            ],
        )


if __name__ == "__main__":
    unittest.main()
