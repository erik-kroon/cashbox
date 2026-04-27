from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.execution import DEFAULT_EXECUTION_POLICY
from cashbox.persistence import canonical_copy, write_json
from cashbox.risk import DEFAULT_RISK_POLICY
from cashbox.runtime import build_workspace


def _sample_market(*, market_id: str, question: str) -> dict[str, object]:
    return {
        "id": f"raw-{market_id}",
        "slug": market_id,
        "eventSlug": "us-election",
        "question": question,
        "category": "Politics",
        "active": True,
        "closed": False,
        "archived": False,
        "enableOrderBook": True,
        "outcomes": json.dumps(["Yes", "No"]),
        "clobTokenIds": json.dumps([f"{market_id}-yes", f"{market_id}-no"]),
        "liquidity": "500",
        "volume": "1000",
        "endDate": "2026-11-05T00:00:00Z",
    }


class SystemHealthServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.workspace = build_workspace(self.root)
        self.workspace.market_store.ingest_market_payloads(
            [_sample_market(market_id="election-2028", question="Will X win in 2028?")],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        self.workspace.market_store.ingest_order_book_snapshots(
            [
                {
                    "token_id": "election-2028-yes",
                    "timestamp": "2026-04-24T10:04:00Z",
                    "bids": [["0.47", "100"]],
                    "asks": [["0.52", "80"]],
                },
                {
                    "token_id": "election-2028-no",
                    "timestamp": "2026-04-24T10:04:00Z",
                    "bids": [["0.48", "80"]],
                    "asks": [["0.53", "100"]],
                },
            ],
            received_at=datetime(2026, 4, 24, 10, 4, tzinfo=timezone.utc),
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_system_health_reports_healthy_core_state(self) -> None:
        report = self.workspace.health.get_system_health(
            now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
            stale_after=timedelta(hours=1),
        )

        self.assertEqual(report["overall_status"], "OK")
        self.assertEqual(report["checks"]["ingest_freshness"]["status"], "OK")
        self.assertEqual(report["checks"]["book_coverage"]["status"], "OK")
        self.assertEqual(report["checks"]["risk_policy_health"]["status"], "OK")
        self.assertEqual(report["checks"]["execution_policy_health"]["status"], "OK")
        self.assertEqual(report["checks"]["execution_global_halt"]["status"], "OK")
        self.assertEqual(report["summaries"]["open_execution_count"], 0)

    def test_system_health_surfaces_distinct_degraded_conditions(self) -> None:
        risk_policy = canonical_copy(DEFAULT_RISK_POLICY)
        risk_policy["signer_healthy"] = False
        execution_policy = canonical_copy(DEFAULT_EXECUTION_POLICY)
        execution_policy["live_executor_healthy"] = False
        write_json(
            self.workspace.governance.store.policy_version_path("risk", 1),
            {
                "activated_at": "2026-04-24T10:01:00Z",
                "activated_by": "test",
                "policy": risk_policy,
                "policy_sha256": "test-risk-policy",
                "policy_type": "risk",
                "source_request_id": "test-risk",
                "version": 1,
            },
        )
        write_json(
            self.workspace.governance.store.policy_version_path("execution", 1),
            {
                "activated_at": "2026-04-24T10:01:00Z",
                "activated_by": "test",
                "policy": execution_policy,
                "policy_sha256": "test-execution-policy",
                "policy_type": "execution",
                "source_request_id": "test-execution",
                "version": 1,
            },
        )
        self.workspace.execution.request_global_halt(
            reason="Operator halt drill.",
            requested_by="ops-oncall",
            now=datetime(2026, 4, 24, 10, 10, tzinfo=timezone.utc),
        )
        write_json(
            self.workspace.execution.store.reconciliation_path("rec-mismatch"),
            {
                "alerts": ["unexpected_live_order"],
                "reconciled_at": "2026-04-24T10:11:00Z",
                "snapshot_id": "rec-mismatch",
                "status": "MISMATCH",
            },
        )
        write_json(
            self.workspace.governance.store.request_path("gov-pending"),
            {
                "request_id": "gov-pending",
                "requested_at": "2026-04-24T10:12:00Z",
                "status": "PENDING",
            },
        )

        report = self.workspace.health.get_system_health(
            now=datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc),
            stale_after=timedelta(minutes=30),
        )

        self.assertEqual(report["overall_status"], "DEGRADED")
        self.assertIn("ingest_freshness", report["degraded_checks"])
        self.assertIn("book_coverage", report["degraded_checks"])
        self.assertIn("risk_policy_health", report["degraded_checks"])
        self.assertIn("execution_policy_health", report["degraded_checks"])
        self.assertIn("execution_global_halt", report["degraded_checks"])
        self.assertIn("execution_reconciliation", report["degraded_checks"])
        self.assertIn("governance_pending_requests", report["degraded_checks"])
        self.assertEqual(report["checks"]["ingest_freshness"]["stale_market_ids"], ["election-2028"])
        self.assertEqual(report["checks"]["book_coverage"]["stale_token_ids"], ["election-2028-no", "election-2028-yes"])
        self.assertEqual(report["checks"]["risk_policy_health"]["failed_fields"], ["signer_healthy"])
        self.assertEqual(report["checks"]["execution_policy_health"]["failed_fields"], ["live_executor_healthy"])
        self.assertTrue(report["checks"]["execution_global_halt"]["global_halt"]["active"])
        self.assertEqual(report["checks"]["execution_reconciliation"]["mismatch_count"], 1)
        self.assertEqual(report["checks"]["governance_pending_requests"]["pending_request_ids"], ["gov-pending"])


if __name__ == "__main__":
    unittest.main()
