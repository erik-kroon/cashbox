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

    def _create_live_ready_experiment(self, *, hypothesis: str) -> str:
        experiment = self.experiments.create_experiment(
            hypothesis=hypothesis,
            strategy_family="midpoint_reversion",
            config={
                "market_id": "election-2028",
                "lookback_minutes": 45,
                "entry_zscore": 1.8,
                "exit_zscore": 0.9,
                "max_position_usd": 25,
            },
            dataset_id=self.market_store.load_manifest().dataset_id,
            code_version="local-dev",
            generated_by="hermes",
        )
        _advance_to_tiny_live_eligible(self.experiments, experiment["experiment_id"])
        return experiment["experiment_id"]

    def _create_approved_intent_for_experiment(self, experiment_id: str) -> tuple[dict[str, object], dict[str, object]]:
        intent = self.risk.create_trade_intent(
            experiment_id,
            {
                "market_id": "election-2028",
                "outcome": "Yes",
                "side": "buy",
                "order_class": "taker_ioc",
                "time_in_force": "ioc",
                "price": "0.51",
                "quantity": "10",
                "estimated_fee_bps": "10",
                "estimated_slippage_bps": "8",
            },
            submitted_by="hermes",
            rationale="Scoped cancel-all coverage",
            now=datetime(2026, 4, 24, 10, 0, 45, tzinfo=timezone.utc),
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
            now=datetime(2026, 4, 24, 10, 1, 15, tzinfo=timezone.utc),
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

    def test_request_strategy_cancel_all_cancels_only_target_experiment_orders(self) -> None:
        second_experiment_id = self._create_live_ready_experiment(hypothesis="Second strategy for cancel-all scope.")
        first_intent, first_decision = self._create_approved_intent()
        second_intent, second_decision = self._create_approved_intent_for_experiment(second_experiment_id)

        first_execution = self.execution.submit_approved_order(
            first_intent["intent_id"],
            approval_token=first_decision["approval_token"],
            now=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
        )
        second_execution = self.execution.submit_approved_order(
            second_intent["intent_id"],
            approval_token=second_decision["approval_token"],
            now=datetime(2026, 4, 24, 10, 1, 30, tzinfo=timezone.utc),
        )

        cancel_result = self.execution.request_strategy_cancel_all(
            self.experiment_id,
            reason="Operator requested strategy stop.",
            requested_by="ops-oncall",
            now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(cancel_result["scope"], "EXPERIMENT")
        self.assertEqual(cancel_result["experiment_id"], self.experiment_id)
        self.assertEqual(cancel_result["cancelled_execution_ids"], [first_execution["execution_id"]])
        self.assertEqual(
            self.execution.get_execution_record(first_execution["execution_id"])["status"],
            "CANCELLED",
        )
        self.assertEqual(
            self.execution.get_execution_record(second_execution["execution_id"])["status"],
            "SUBMITTED",
        )

    def test_request_global_halt_cancels_open_orders_and_blocks_new_submission(self) -> None:
        first_intent, first_decision = self._create_approved_intent()
        submitted = self.execution.submit_approved_order(
            first_intent["intent_id"],
            approval_token=first_decision["approval_token"],
            now=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
        )

        halt_result = self.execution.request_global_halt(
            reason="Operator requested hard halt.",
            requested_by="ops-oncall",
            now=datetime(2026, 4, 24, 10, 2, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(halt_result["scope"], "GLOBAL")
        self.assertEqual(halt_result["cancelled_execution_ids"], [submitted["execution_id"]])
        self.assertTrue(self.execution.get_live_controls()["global_halt"]["active"])
        self.assertEqual(
            self.execution.get_execution_record(submitted["execution_id"])["status"],
            "CANCELLED",
        )

        second_experiment_id = self._create_live_ready_experiment(hypothesis="Post-halt release should fail.")
        second_intent, second_decision = self._create_approved_intent_for_experiment(second_experiment_id)
        with self.assertRaises(ExecutionValidationError):
            self.execution.submit_approved_order(
                second_intent["intent_id"],
                approval_token=second_decision["approval_token"],
                now=datetime(2026, 4, 24, 10, 1, 45, tzinfo=timezone.utc),
            )

    def test_reconcile_live_state_detects_unexpected_orders_and_position_mismatch(self) -> None:
        intent, decision = self._create_approved_intent()
        submitted = self.execution.submit_approved_order(
            intent["intent_id"],
            approval_token=decision["approval_token"],
            now=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
        )
        self.execution.record_live_fill(
            submitted["execution_id"],
            filled_quantity="8",
            fill_price="0.50",
            recorded_by="live-executor",
            now=datetime(2026, 4, 24, 10, 2, 10, tzinfo=timezone.utc),
        )

        snapshot = self.execution.reconcile_live_state(
            venue_orders=[
                {
                    "order_id": submitted["live_executor"]["order_id"],
                    "status": "SUBMITTED",
                },
                {
                    "order_id": "ord-unexpected-001",
                    "status": "SUBMITTED",
                },
            ],
            venue_positions=[
                {
                    "market_id": "election-2028",
                    "outcome": "Yes",
                    "net_quantity": "6",
                }
            ],
            reconciled_by="ops-oncall",
            now=datetime(2026, 4, 24, 10, 2, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(snapshot["status"], "MISMATCH")
        self.assertEqual(snapshot["unexpected_live_order_ids"], ["ord-unexpected-001"])
        self.assertEqual(snapshot["missing_live_order_ids"], [])
        self.assertIn("unexpected_live_order", snapshot["alerts"])
        self.assertIn("position_reconciliation_mismatch", snapshot["alerts"])
        self.assertEqual(snapshot["position_mismatches"][0]["local_net_quantity"], "8")
        self.assertEqual(snapshot["position_mismatches"][0]["venue_net_quantity"], "6")


if __name__ == "__main__":
    unittest.main()
