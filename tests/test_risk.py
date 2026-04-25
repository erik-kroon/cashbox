from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.experiments import ExperimentService
from cashbox.ingest import FileSystemMarketStore
from cashbox.risk import build_risk_gateway_service


def _sample_market(
    *,
    market_id: str,
    question: str,
    category: str = "Politics",
    active: bool = True,
    closed: bool = False,
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
        "liquidity": "500",
        "volume": "1000",
        "endDate": "2026-11-05T00:00:00Z",
    }


def _advance_to_tiny_live_eligible(experiments: ExperimentService, experiment_id: str) -> None:
    for status in (
        "VALIDATED_CONFIG",
        "BACKTEST_QUEUED",
        "BACKTESTED",
        "WALK_FORWARD_TESTED",
        "PAPER_ELIGIBLE",
        "PAPER_RUNNING",
        "PAPER_PASSED",
        "TINY_LIVE_ELIGIBLE",
    ):
        experiments.transition_experiment_status(
            experiment_id,
            to_status=status,
            changed_by="test-harness",
        )


class RiskGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.market_store = FileSystemMarketStore(self.root)
        self.market_store.ingest_market_payloads(
            [
                _sample_market(market_id="election-2028", question="Will X win in 2028?"),
            ],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        self.risk = build_risk_gateway_service(self.root)
        self.experiments = self.risk.experiments
        experiment = self.experiments.create_experiment(
            hypothesis="Fade temporary headline-driven dislocations.",
            strategy_family="midpoint_reversion",
            config={
                "market_id": "election-2028",
                "lookback_minutes": 30,
                "entry_zscore": 2.0,
                "exit_zscore": 0.8,
                "max_position_usd": 25,
            },
            dataset_id=self.market_store.load_manifest().dataset_id,
            code_version="local-dev",
            generated_by="hermes",
        )
        self.experiment_id = experiment["experiment_id"]
        _advance_to_tiny_live_eligible(self.experiments, self.experiment_id)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _create_intent(self, *, price: str = "0.50") -> dict[str, object]:
        return self.risk.create_trade_intent(
            self.experiment_id,
            {
                "market_id": "election-2028",
                "outcome": "Yes",
                "side": "buy",
                "order_class": "taker_ioc",
                "time_in_force": "ioc",
                "price": price,
                "quantity": "20",
                "estimated_fee_bps": "10",
                "estimated_slippage_bps": "8",
            },
            submitted_by="hermes",
            rationale="Tiny-live tracer bullet",
            now=datetime(2026, 4, 24, 10, 0, 30, tzinfo=timezone.utc),
        )

    def test_trade_intent_waits_for_human_approval_before_allowing(self) -> None:
        intent = self._create_intent()

        decision = self.risk.evaluate_trade_intent(
            intent["intent_id"],
            now=datetime(2026, 4, 24, 10, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(decision["outcome"], "PENDING_HUMAN_APPROVAL")
        self.assertEqual(decision["failed_checks"], ["human_approval"])
        self.assertIn("requires explicit human approval", decision["notes"][0])

        refreshed = self.risk.get_trade_intent(intent["intent_id"])
        self.assertEqual(refreshed["state"]["current_status"], "PENDING_HUMAN_APPROVAL")
        self.assertEqual(refreshed["state"]["latest_decision_outcome"], "PENDING_HUMAN_APPROVAL")

    def test_human_rejects_trade_intent_and_risk_gateway_records_rejection(self) -> None:
        intent = self._create_intent()
        self.risk.review_trade_intent(
            intent["intent_id"],
            reviewer="ops-oncall",
            decision="reject",
            reason="Need manual market review before first live order.",
            now=datetime(2026, 4, 24, 10, 1, tzinfo=timezone.utc),
        )

        decision = self.risk.evaluate_trade_intent(
            intent["intent_id"],
            now=datetime(2026, 4, 24, 10, 1, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(decision["outcome"], "REJECT")
        self.assertEqual(decision["checks"]["human_approval"]["observed"], "REJECT")
        self.assertIn("human reviewer rejected the trade intent", decision["notes"])

        refreshed = self.risk.get_trade_intent(intent["intent_id"])
        self.assertEqual(refreshed["state"]["current_status"], "REJECTED")
        self.assertEqual(refreshed["state"]["human_review_status"], "REJECT")

    def test_human_approval_emits_approval_token_when_checks_pass(self) -> None:
        intent = self._create_intent()
        self.risk.review_trade_intent(
            intent["intent_id"],
            reviewer="ops-oncall",
            decision="approve",
            reason="Approved within tiny-live envelope.",
            now=datetime(2026, 4, 24, 10, 1, tzinfo=timezone.utc),
        )

        decision = self.risk.evaluate_trade_intent(
            intent["intent_id"],
            now=datetime(2026, 4, 24, 10, 1, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(decision["outcome"], "ALLOW")
        self.assertEqual(decision["failed_checks"], [])
        self.assertTrue(decision["approval_token"].startswith("approve-"))

        refreshed = self.risk.get_trade_intent(intent["intent_id"])
        self.assertEqual(refreshed["state"]["current_status"], "ALLOWED")
        self.assertEqual(refreshed["state"]["approval_token"], decision["approval_token"])

    def test_tick_misalignment_rejects_even_before_hitl(self) -> None:
        intent = self._create_intent(price="0.503")

        decision = self.risk.evaluate_trade_intent(
            intent["intent_id"],
            now=datetime(2026, 4, 24, 10, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(decision["outcome"], "REJECT")
        self.assertIn("price_tick_aligned", decision["failed_checks"])
        self.assertNotIn("PENDING_HUMAN_APPROVAL", decision["outcome"])


if __name__ == "__main__":
    unittest.main()
