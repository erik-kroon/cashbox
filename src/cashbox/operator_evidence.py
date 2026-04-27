from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .audit import AuditTrailService
from .backtests import BacktestService
from .execution import ExecutionService, OPEN_EXECUTION_STATUSES
from .experiments import ExperimentService
from .governance import GovernanceService
from .paper import PaperService
from .persistence import read_json


@dataclass(frozen=True)
class OperatorEvidenceService:
    experiments: ExperimentService
    backtests: BacktestService
    paper: PaperService
    execution: ExecutionService
    governance: GovernanceService
    audit: AuditTrailService

    def get_gateway_audit_evidence(self) -> dict[str, Any]:
        records = self.audit.list_audit_events(service="gateway")["events"]
        return {
            "audit_path": str(self.audit.gateway_audit_path),
            "records": records,
        }

    def get_policy_evidence(self, policy_type: str) -> dict[str, Any]:
        return self.governance.get_active_policy(policy_type)

    def get_execution_controls_evidence(self) -> dict[str, Any]:
        return self.execution.get_live_controls()

    def get_reconciliation_evidence(self) -> dict[str, Any]:
        snapshots = self._json_payloads(self.execution.store.reconciliations_dir)
        mismatches = [payload for payload in snapshots if payload.get("status") == "MISMATCH"]
        return {
            "latest_snapshot": self._latest_json_payload(snapshots),
            "mismatch_count": len(mismatches),
            "mismatches": mismatches,
        }

    def get_pending_governance_request_evidence(self) -> dict[str, Any]:
        pending_requests = [
            payload
            for payload in self._json_payloads(self.governance.store.requests_dir)
            if payload.get("status") == "PENDING"
        ]
        return {
            "pending_count": len(pending_requests),
            "pending_requests": pending_requests,
        }

    def get_operator_summaries(self) -> dict[str, Any]:
        return {
            "experiments_by_status": self._count_experiments_by_status(),
            "backtests_by_status": self._count_statuses(self._json_payloads(self.backtests.store.runs_dir)),
            "paper_runs_by_status": self._count_statuses(self._json_payloads(self.paper.store.runs_dir)),
            "open_execution_count": self._open_execution_count(),
            "reconciliation_mismatch_count": self.get_reconciliation_evidence()["mismatch_count"],
            "pending_governance_request_count": self.get_pending_governance_request_evidence()["pending_count"],
        }

    def _count_experiments_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for experiment in self.experiments.list_experiments():
            status = str(experiment["current_status"])
            counts[status] = counts.get(status, 0) + 1
        return dict(sorted(counts.items()))

    def _count_statuses(self, payloads: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for payload in payloads:
            status = str(payload.get("status", "UNKNOWN"))
            counts[status] = counts.get(status, 0) + 1
        return dict(sorted(counts.items()))

    def _open_execution_count(self) -> int:
        return sum(
            1
            for payload in self._json_payloads(self.execution.store.orders_dir)
            if payload.get("status") in OPEN_EXECUTION_STATUSES
        )

    def _latest_json_payload(self, payloads: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
        if not payloads:
            return None
        return max(
            payloads,
            key=lambda payload: (
                str(payload.get("reconciled_at", "")),
                str(payload.get("created_at", "")),
                str(payload.get("snapshot_id", "")),
            ),
        )

    def _json_payloads(self, directory: Path) -> list[dict[str, Any]]:
        if not directory.exists():
            return []
        payloads: list[dict[str, Any]] = []
        for path in sorted(directory.glob("*.json")):
            payload = read_json(path)
            if isinstance(payload, dict):
                payloads.append(payload)
        return payloads


OperatorEvidenceServiceType = OperatorEvidenceService


def build_operator_evidence_service(root: Path) -> OperatorEvidenceService:
    from .runtime import build_workspace

    return build_workspace(root).evidence
