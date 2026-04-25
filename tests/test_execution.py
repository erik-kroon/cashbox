from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.experiments import ExperimentService
from cashbox.execution import ExecutionValidationError, build_execution_service
from cashbox.ingest import FileSystemMarketStore


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


class ExecutionServiceTests(unittest.TestCase):
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
        self.execution = build_execution_service(self.root)
        self.risk = self.execution.risk
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

    def _create_approved_intent(self) -> tuple[dict[str, object], dict[str, object]]:
        intent = self.risk.create_trade_intent(
            self.experiment_id,
            {
                "market_id": "election-2028",
                "outcome": "Yes",
                "side": "buy",
                "order_class": "taker_ioc",
                "time_in_force": "ioc",
                "price": "0.50",
                "quantity": "20",
                "estimated_fee_bps": "10",
                "estimated_slippage_bps": "8",
            },
            submitted_by="hermes",
            rationale="Tiny-live tracer bullet",
            now=datetime(2026, 4, 24, 10, 0, 30, tzinfo=timezone.utc),
        )
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
        return intent, decision

    def test_submit_approved_order_signs_and_submits(self) -> None:
        intent, decision = self._create_approved_intent()

        execution = self.execution.submit_approved_order(
            intent["intent_id"],
            approval_token=decision["approval_token"],
            now=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(execution["status"], "SUBMITTED")
        self.assertEqual(execution["risk_decision_id"], decision["decision_id"])
        self.assertEqual(execution["signature"]["service"], "signer-service")
        self.assertEqual(execution["live_executor"]["service"], "live-executor")
        self.assertEqual(execution["order_payload"]["token_id"], "election-2028-yes")

        stored = self.execution.get_execution_record(execution["execution_id"])
        self.assertEqual(stored["execution_id"], execution["execution_id"])

        state = self.execution.get_execution_state(intent["intent_id"])
        self.assertEqual(state["current_status"], "SUBMITTED")
        self.assertEqual(state["latest_execution_id"], execution["execution_id"])
        self.assertEqual(state["latest_execution"]["live_executor"]["order_id"], execution["live_executor"]["order_id"])

    def test_submit_approved_order_rejects_mismatched_approval_token(self) -> None:
        intent, _decision = self._create_approved_intent()

        with self.assertRaises(ExecutionValidationError):
            self.execution.submit_approved_order(
                intent["intent_id"],
                approval_token="approve-does-not-match",
                now=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
            )

        state = self.execution.get_execution_state(intent["intent_id"])
        self.assertEqual(state["current_status"], "NOT_SUBMITTED")
        self.assertIsNone(state["latest_execution"])

    def test_submit_approved_order_rejects_replay_after_submission(self) -> None:
        intent, decision = self._create_approved_intent()
        self.execution.submit_approved_order(
            intent["intent_id"],
            approval_token=decision["approval_token"],
            now=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
        )

        with self.assertRaises(ExecutionValidationError):
            self.execution.submit_approved_order(
                intent["intent_id"],
                approval_token=decision["approval_token"],
                now=datetime(2026, 4, 24, 10, 2, 5, tzinfo=timezone.utc),
            )

    def test_submit_approved_order_fails_fast_when_live_executor_is_unhealthy(self) -> None:
        intent, decision = self._create_approved_intent()

        with self.assertRaises(ExecutionValidationError):
            self.execution.submit_approved_order(
                intent["intent_id"],
                approval_token=decision["approval_token"],
                policy={"live_executor_healthy": False},
                now=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
            )

        state = self.execution.get_execution_state(intent["intent_id"])
        self.assertEqual(state["current_status"], "NOT_SUBMITTED")


if __name__ == "__main__":
    unittest.main()
