from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.gateway import AgentMarketGateway, FileSystemAgentGatewayStore
from cashbox.governance import (
    FileSystemGovernanceStore,
    GovernanceAuthorizationError,
    GovernanceService,
    GovernanceValidationError,
)
from cashbox.ingest import FileSystemMarketStore
from cashbox.research import ResearchMarketReadPath
from cashbox.runtime import build_workspace


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


def _advance_to_scale_review(experiments: object, experiment_id: str) -> None:
    for status in (
        "VALIDATED_CONFIG",
        "BACKTEST_QUEUED",
        "BACKTESTED",
        "WALK_FORWARD_TESTED",
        "PAPER_ELIGIBLE",
        "PAPER_RUNNING",
        "PAPER_PASSED",
        "TINY_LIVE_ELIGIBLE",
        "TINY_LIVE_RUNNING",
        "SCALE_REVIEW",
    ):
        experiments.transition_experiment_status(
            experiment_id,
            to_status=status,
            changed_by="test-harness",
        )


def _advance_to_tiny_live_eligible(experiments: object, experiment_id: str) -> None:
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


class GovernanceServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.market_store = FileSystemMarketStore(self.root)
        self.market_store.ingest_market_payloads(
            [_sample_market(market_id="election-2028", question="Will X win in 2028?")],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        self.workspace = build_workspace(self.root)
        self.gateway = AgentMarketGateway(FileSystemAgentGatewayStore(self.root), ResearchMarketReadPath(self.market_store))
        self.governance = GovernanceService(
            FileSystemGovernanceStore(self.root),
            experiments=self.workspace.experiments,
            execution=self.workspace.execution,
            risk=self.workspace.risk,
        )
        experiment = self.workspace.experiments.create_experiment(
            hypothesis="Promote only after governance approval.",
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
            now=datetime(2026, 4, 24, 10, 0, 10, tzinfo=timezone.utc),
        )
        self.experiment_id = experiment["experiment_id"]
        _advance_to_scale_review(self.workspace.experiments, self.experiment_id)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _create_live_ready_experiment(self, *, hypothesis: str) -> str:
        experiment = self.workspace.experiments.create_experiment(
            hypothesis=hypothesis,
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
            now=datetime(2026, 4, 24, 10, 0, 20, tzinfo=timezone.utc),
        )
        _advance_to_tiny_live_eligible(self.workspace.experiments, experiment["experiment_id"])
        return experiment["experiment_id"]

    def _create_approved_intent(self) -> tuple[dict[str, object], dict[str, object]]:
        live_experiment_id = self._create_live_ready_experiment(hypothesis="Live-eligible experiment for audit coverage.")
        intent = self.workspace.risk.create_trade_intent(
            live_experiment_id,
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
            rationale="Governance audit coverage",
            now=datetime(2026, 4, 24, 10, 1, tzinfo=timezone.utc),
        )
        self.workspace.risk.review_trade_intent(
            intent["intent_id"],
            reviewer="ops-oncall",
            decision="approve",
            reason="Approved for audit aggregation test.",
            now=datetime(2026, 4, 24, 10, 1, 30, tzinfo=timezone.utc),
        )
        decision = self.workspace.risk.evaluate_trade_intent(
            intent["intent_id"],
            now=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
        )
        return intent, decision

    def test_rbac_blocks_policy_change_request_without_operator_role(self) -> None:
        self.governance.bootstrap_subject(
            "governor-alice",
            roles=["governor"],
            now=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        self.governance.assign_role(
            "viewer-vic",
            role="viewer",
            granted_by="governor-alice",
            now=datetime(2026, 4, 24, 10, 0, 30, tzinfo=timezone.utc),
        )

        with self.assertRaises(GovernanceAuthorizationError):
            self.governance.request_policy_change(
                "risk",
                {"max_notional_usd": "40"},
                requested_by="viewer-vic",
                reason="Viewer should not be able to request live policy changes.",
                now=datetime(2026, 4, 24, 10, 1, tzinfo=timezone.utc),
            )

    def test_strategy_promotion_requires_governor_approval_before_apply(self) -> None:
        self.governance.bootstrap_subject(
            "governor-alice",
            roles=["governor"],
            now=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        self.governance.assign_role(
            "ops-bob",
            role="operator",
            granted_by="governor-alice",
            now=datetime(2026, 4, 24, 10, 0, 30, tzinfo=timezone.utc),
        )

        request = self.governance.request_strategy_promotion(
            self.experiment_id,
            requested_by="ops-bob",
            reason="Tiny-live run completed and operator requests production approval.",
            now=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
        )

        with self.assertRaises(GovernanceValidationError):
            self.governance.apply_request(
                request["request_id"],
                applied_by="ops-bob",
                now=datetime(2026, 4, 24, 10, 2, 30, tzinfo=timezone.utc),
            )

        reviewed = self.governance.review_request(
            request["request_id"],
            reviewer="governor-alice",
            decision="approve",
            reason="Paper and tiny-live evidence are satisfactory.",
            now=datetime(2026, 4, 24, 10, 3, tzinfo=timezone.utc),
        )
        applied = self.governance.apply_request(
            request["request_id"],
            applied_by="ops-bob",
            now=datetime(2026, 4, 24, 10, 4, tzinfo=timezone.utc),
        )

        self.assertEqual(reviewed["status"], "APPROVED")
        self.assertEqual(applied["status"], "APPLIED")
        self.assertEqual(
            self.workspace.experiments.get_experiment(self.experiment_id)["current_status"],
            "PRODUCTION_APPROVED",
        )

    def test_capital_limit_policy_change_is_versioned_after_governor_approval(self) -> None:
        self.governance.bootstrap_subject(
            "governor-alice",
            roles=["governor"],
            now=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        self.governance.assign_role(
            "ops-bob",
            role="operator",
            granted_by="governor-alice",
            now=datetime(2026, 4, 24, 10, 0, 30, tzinfo=timezone.utc),
        )

        request = self.governance.request_policy_change(
            "risk",
            {"max_notional_usd": "50", "portfolio_exposure_limit_usd": "75"},
            requested_by="ops-bob",
            reason="Raise tiny-live envelope after manual review.",
            now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )
        self.assertEqual(request["change_scope"], "CAPITAL_LIMIT")

        self.governance.review_request(
            request["request_id"],
            reviewer="governor-alice",
            decision="approve",
            reason="Approved capital increase for next controlled phase.",
            now=datetime(2026, 4, 24, 10, 6, tzinfo=timezone.utc),
        )
        applied = self.governance.apply_request(
            request["request_id"],
            applied_by="ops-bob",
            now=datetime(2026, 4, 24, 10, 7, tzinfo=timezone.utc),
        )
        active = self.governance.get_active_policy("risk")
        version_one = self.governance.get_policy_version("risk", 1)

        self.assertEqual(applied["status"], "APPLIED")
        self.assertEqual(active["version"], 1)
        self.assertEqual(version_one["policy"]["max_notional_usd"], "50")
        self.assertEqual(version_one["policy"]["portfolio_exposure_limit_usd"], "75")

    def test_audit_console_aggregates_governance_gateway_risk_and_execution_events(self) -> None:
        self.governance.bootstrap_subject(
            "governor-alice",
            roles=["governor", "operator"],
            now=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )

        self.gateway.issue_read_only_credential(subject="hermes", token="test-token")
        self.gateway.call_tool(
            "get_ingest_health",
            {},
            token="test-token",
            user_id="hermes",
            session_id="session-001",
            now=datetime(2026, 4, 24, 10, 1, tzinfo=timezone.utc),
        )

        intent, decision = self._create_approved_intent()
        self.workspace.execution.submit_approved_order(
            intent["intent_id"],
            approval_token=decision["approval_token"],
            now=datetime(2026, 4, 24, 10, 2, 30, tzinfo=timezone.utc),
        )
        self.governance.request_emergency_halt(
            requested_by="governor-alice",
            reason="Operator drills halt path for audit review.",
            now=datetime(2026, 4, 24, 10, 3, tzinfo=timezone.utc),
        )

        audit = self.governance.list_audit_events(limit=20)
        services = {event["service"] for event in audit["events"]}

        self.assertIn("governance", services)
        self.assertIn("gateway", services)
        self.assertIn("risk", services)
        self.assertIn("execution", services)


if __name__ == "__main__":
    unittest.main()
