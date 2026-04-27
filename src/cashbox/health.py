from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from .models import format_datetime, utc_now
from .operator_evidence import OperatorEvidenceService
from .research import ResearchMarketReader

HEALTH_STATUS_OK = "OK"
HEALTH_STATUS_DEGRADED = "DEGRADED"
HEALTH_STATUS_UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class SystemHealthService:
    read_path: ResearchMarketReader
    evidence: OperatorEvidenceService

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
            "summaries": self.evidence.get_operator_summaries(),
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
        audit_evidence = self.evidence.get_gateway_audit_evidence()
        records = audit_evidence["records"]
        if not records:
            return {
                "status": HEALTH_STATUS_UNKNOWN,
                "audit_path": audit_evidence["audit_path"],
                "audit_record_count": 0,
                "message": "no gateway audit records have been written yet",
            }
        latest = max(str(row.get("timestamp") or row.get("called_at") or "") for row in records)
        return {
            "status": HEALTH_STATUS_OK,
            "audit_path": audit_evidence["audit_path"],
            "audit_record_count": len(records),
            "latest_called_at": latest,
        }

    def _risk_policy_health_check(self) -> dict[str, Any]:
        active_policy = self.evidence.get_policy_evidence("risk")
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
        active_policy = self.evidence.get_policy_evidence("execution")
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
        controls = self.evidence.get_execution_controls_evidence()
        global_halt = controls["global_halt"]
        return {
            "status": HEALTH_STATUS_DEGRADED if global_halt["active"] else HEALTH_STATUS_OK,
            "global_halt": global_halt,
            "updated_at": controls["updated_at"],
        }

    def _execution_reconciliation_check(self) -> dict[str, Any]:
        reconciliation = self.evidence.get_reconciliation_evidence()
        mismatch_count = reconciliation["mismatch_count"]
        return {
            "status": HEALTH_STATUS_OK if mismatch_count == 0 else HEALTH_STATUS_DEGRADED,
            "mismatch_count": mismatch_count,
            "latest_snapshot": reconciliation["latest_snapshot"],
        }

    def _governance_pending_requests_check(self) -> dict[str, Any]:
        pending_requests = self.evidence.get_pending_governance_request_evidence()["pending_requests"]
        return {
            "status": HEALTH_STATUS_OK if not pending_requests else HEALTH_STATUS_DEGRADED,
            "pending_count": len(pending_requests),
            "pending_request_ids": [request["request_id"] for request in pending_requests],
        }


def build_system_health_service(root: Path) -> SystemHealthService:
    from .runtime import build_workspace

    return build_workspace(root).health
