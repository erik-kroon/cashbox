from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from .backtests import BacktestService
from .execution import ExecutionService, OPEN_EXECUTION_STATUSES
from .experiments import ExperimentService
from .gateway import AgentMarketGateway
from .governance import GovernanceService
from .models import format_datetime, utc_now
from .paper import PaperService
from .persistence import read_json
from .research import ResearchMarketReadPath

HEALTH_STATUS_OK = "OK"
HEALTH_STATUS_DEGRADED = "DEGRADED"
HEALTH_STATUS_UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class SystemHealthService:
    read_path: ResearchMarketReadPath
    gateway: AgentMarketGateway
    experiments: ExperimentService
    backtests: BacktestService
    paper: PaperService
    execution: ExecutionService
    governance: GovernanceService

    def get_system_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(hours=1),
        dataset_id: Optional[str] = None,
    ) -> dict[str, Any]:
        current_time = now or utc_now()
        checks = {
            "ingest_freshness": self._ingest_freshness_check(
                now=current_time,
                stale_after=stale_after,
                dataset_id=dataset_id,
            ),
            "book_coverage": self._book_coverage_check(
                now=current_time,
                stale_after=stale_after,
                dataset_id=dataset_id,
            ),
            "gateway_audit_presence": self._gateway_audit_presence_check(),
            "risk_policy_health": self._risk_policy_health_check(),
            "execution_policy_health": self._execution_policy_health_check(),
            "execution_global_halt": self._execution_global_halt_check(),
            "execution_reconciliation": self._execution_reconciliation_check(),
            "governance_pending_requests": self._governance_pending_requests_check(),
        }
        degraded_checks = sorted(
            name for name, check in checks.items() if check["status"] == HEALTH_STATUS_DEGRADED
        )
        unknown_checks = sorted(
            name for name, check in checks.items() if check["status"] == HEALTH_STATUS_UNKNOWN
        )
        overall_status = HEALTH_STATUS_DEGRADED if degraded_checks else HEALTH_STATUS_OK
        return {
            "checked_at": format_datetime(current_time),
            "overall_status": overall_status,
            "degraded_checks": degraded_checks,
            "unknown_checks": unknown_checks,
            "checks": checks,
            "summaries": {
                "experiments_by_status": self._count_experiments_by_status(),
                "backtests_by_status": self._count_json_statuses(self.backtests.store.runs_dir),
                "paper_runs_by_status": self._count_json_statuses(self.paper.store.runs_dir),
                "open_execution_count": self._open_execution_count(),
                "reconciliation_mismatch_count": self._reconciliation_mismatch_count(),
                "pending_governance_request_count": self._pending_governance_request_count(),
            },
        }

    def _ingest_freshness_check(
        self,
        *,
        now: datetime,
        stale_after: timedelta,
        dataset_id: Optional[str],
    ) -> dict[str, Any]:
        try:
            report = self.read_path.get_ingest_health(
                now=now,
                stale_after=stale_after,
                dataset_id=dataset_id,
            ).to_dict()
        except FileNotFoundError as exc:
            return {
                "status": HEALTH_STATUS_DEGRADED,
                "message": "market ingest dataset is unavailable",
                "error": str(exc),
            }
        stale_market_ids = report["stale_market_ids"]
        return {
            "status": HEALTH_STATUS_OK if not stale_market_ids else HEALTH_STATUS_DEGRADED,
            "dataset_id": report["dataset_id"],
            "source_name": report["source_name"],
            "ingested_at": report["ingested_at"],
            "market_count": report["market_count"],
            "active_market_count": report["active_market_count"],
            "stale_after_seconds": int(stale_after.total_seconds()),
            "stale_market_count": len(stale_market_ids),
            "stale_market_ids": stale_market_ids,
        }

    def _book_coverage_check(
        self,
        *,
        now: datetime,
        stale_after: timedelta,
        dataset_id: Optional[str],
    ) -> dict[str, Any]:
        try:
            return self.read_path.get_book_health(
                now=now,
                stale_after=stale_after,
                dataset_id=dataset_id,
            )
        except FileNotFoundError as exc:
            return {
                "status": HEALTH_STATUS_DEGRADED,
                "message": "market ingest dataset is unavailable for book coverage",
                "error": str(exc),
            }

    def _gateway_audit_presence_check(self) -> dict[str, Any]:
        records = self.gateway.store.load_audit_records()
        if not records:
            return {
                "status": HEALTH_STATUS_UNKNOWN,
                "audit_path": str(self.gateway.store.audit_path),
                "audit_record_count": 0,
                "message": "no gateway audit records have been written yet",
            }
        latest = max(str(row.get("called_at", "")) for row in records)
        return {
            "status": HEALTH_STATUS_OK,
            "audit_path": str(self.gateway.store.audit_path),
            "audit_record_count": len(records),
            "latest_called_at": latest,
        }

    def _risk_policy_health_check(self) -> dict[str, Any]:
        active_policy = self.governance.get_active_policy("risk")
        policy = active_policy["policy"]
        failed = []
        for field_name in (
            "event_relation_constraints_valid",
            "exchange_healthy",
            "external_model_fresh",
            "signer_healthy",
        ):
            if not bool(policy[field_name]):
                failed.append(field_name)
        for field_name in ("global_halt", "strategy_halt"):
            if bool(policy[field_name]):
                failed.append(field_name)
        return {
            "status": HEALTH_STATUS_OK if not failed else HEALTH_STATUS_DEGRADED,
            "failed_fields": failed,
            "policy_version": active_policy["version"],
            "policy": {
                "event_relation_constraints_valid": policy["event_relation_constraints_valid"],
                "exchange_healthy": policy["exchange_healthy"],
                "external_model_fresh": policy["external_model_fresh"],
                "global_halt": policy["global_halt"],
                "signer_healthy": policy["signer_healthy"],
                "strategy_halt": policy["strategy_halt"],
            },
        }

    def _execution_policy_health_check(self) -> dict[str, Any]:
        active_policy = self.governance.get_active_policy("execution")
        policy = active_policy["policy"]
        failed = [
            field_name
            for field_name in ("live_executor_healthy", "signer_service_healthy")
            if not bool(policy[field_name])
        ]
        return {
            "status": HEALTH_STATUS_OK if not failed else HEALTH_STATUS_DEGRADED,
            "failed_fields": failed,
            "policy_version": active_policy["version"],
            "policy": {
                "live_executor_healthy": policy["live_executor_healthy"],
                "signer_service_healthy": policy["signer_service_healthy"],
            },
        }

    def _execution_global_halt_check(self) -> dict[str, Any]:
        controls = self.execution.get_live_controls()
        global_halt = controls["global_halt"]
        return {
            "status": HEALTH_STATUS_DEGRADED if global_halt["active"] else HEALTH_STATUS_OK,
            "global_halt": global_halt,
            "updated_at": controls["updated_at"],
        }

    def _execution_reconciliation_check(self) -> dict[str, Any]:
        mismatch_count = self._reconciliation_mismatch_count()
        latest = self._latest_json_payload(self.execution.store.reconciliations_dir)
        return {
            "status": HEALTH_STATUS_OK if mismatch_count == 0 else HEALTH_STATUS_DEGRADED,
            "mismatch_count": mismatch_count,
            "latest_snapshot": latest,
        }

    def _governance_pending_requests_check(self) -> dict[str, Any]:
        pending_requests = self._pending_governance_requests()
        return {
            "status": HEALTH_STATUS_OK if not pending_requests else HEALTH_STATUS_DEGRADED,
            "pending_count": len(pending_requests),
            "pending_request_ids": [request["request_id"] for request in pending_requests],
        }

    def _count_experiments_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for experiment in self.experiments.list_experiments():
            status = str(experiment["current_status"])
            counts[status] = counts.get(status, 0) + 1
        return dict(sorted(counts.items()))

    def _count_json_statuses(self, directory: Path) -> dict[str, int]:
        counts: dict[str, int] = {}
        for payload in self._json_payloads(directory):
            status = str(payload.get("status", "UNKNOWN"))
            counts[status] = counts.get(status, 0) + 1
        return dict(sorted(counts.items()))

    def _open_execution_count(self) -> int:
        return sum(
            1
            for payload in self._json_payloads(self.execution.store.orders_dir)
            if payload.get("status") in OPEN_EXECUTION_STATUSES
        )

    def _reconciliation_mismatch_count(self) -> int:
        return sum(
            1
            for payload in self._json_payloads(self.execution.store.reconciliations_dir)
            if payload.get("status") == "MISMATCH"
        )

    def _pending_governance_request_count(self) -> int:
        return len(self._pending_governance_requests())

    def _pending_governance_requests(self) -> list[dict[str, Any]]:
        return [
            payload
            for payload in self._json_payloads(self.governance.store.requests_dir)
            if payload.get("status") == "PENDING"
        ]

    def _latest_json_payload(self, directory: Path) -> Optional[dict[str, Any]]:
        payloads = self._json_payloads(directory)
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


def build_system_health_service(root: Path) -> SystemHealthService:
    from .runtime import build_workspace

    return build_workspace(root).health
