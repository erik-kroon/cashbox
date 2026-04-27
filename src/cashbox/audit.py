from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Optional

from .execution import ExecutionService
from .experiments import ExperimentService
from .persistence import canonical_copy, canonical_json, read_json, read_jsonl
from .risk import RiskGatewayService


class AuditTrailServiceError(Exception):
    pass


class AuditTrailValidationError(AuditTrailServiceError):
    pass


def _require_text(name: str, value: Any, *, max_length: int = 2000) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise AuditTrailValidationError(f"{name} must be non-empty")
    if len(normalized) > max_length:
        raise AuditTrailValidationError(f"{name} exceeds max length {max_length}")
    return normalized


class AuditTrailService:
    def __init__(
        self,
        root: Path,
        *,
        experiments: ExperimentService,
        execution: ExecutionService,
        risk: RiskGatewayService,
    ) -> None:
        self.root = Path(root)
        self.experiments = experiments
        self.execution = execution
        self.risk = risk

    @property
    def governance_audit_path(self) -> Path:
        return self.root / "governance" / "audit.jsonl"

    @property
    def governance_requests_dir(self) -> Path:
        return self.root / "governance" / "requests"

    @property
    def gateway_audit_path(self) -> Path:
        return self.root / "gateway" / "audit.jsonl"

    def list_audit_events(
        self,
        *,
        service: Optional[str] = None,
        actor: Optional[str] = None,
        status: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> dict[str, Any]:
        events = [
            *self._load_governance_audit_events(),
            *self._load_gateway_audit_events(),
            *self._load_execution_audit_events(),
            *self._load_risk_audit_events(),
        ]
        normalized_service = None if service is None else _require_text("service", service, max_length=120).lower()
        normalized_actor = None if actor is None else _require_text("actor", actor, max_length=200)
        normalized_status = None if status is None else _require_text("status", status, max_length=120).lower()
        filtered: list[dict[str, Any]] = []
        for event in events:
            if normalized_service is not None and event["service"] != normalized_service:
                continue
            if normalized_actor is not None and event["actor"] != normalized_actor:
                continue
            if normalized_status is not None and str(event["status"]).lower() != normalized_status:
                continue
            filtered.append(event)
        filtered.sort(key=lambda item: (item["timestamp"], item["event_id"]), reverse=True)
        if limit is not None:
            filtered = filtered[: int(limit)]
        return {"events": filtered, "total": len(filtered)}

    def get_audit_timeline(
        self,
        *,
        experiment_id: Optional[str] = None,
        market_id: Optional[str] = None,
        intent_id: Optional[str] = None,
        decision_id: Optional[str] = None,
        execution_id: Optional[str] = None,
        request_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> dict[str, Any]:
        filters = {
            "decision_id": None if decision_id is None else _require_text("decision_id", decision_id, max_length=160),
            "execution_id": None if execution_id is None else _require_text("execution_id", execution_id, max_length=160),
            "experiment_id": None if experiment_id is None else _require_text("experiment_id", experiment_id, max_length=160),
            "intent_id": None if intent_id is None else _require_text("intent_id", intent_id, max_length=160),
            "market_id": None if market_id is None else _require_text("market_id", market_id, max_length=160),
            "request_id": None if request_id is None else _require_text("request_id", request_id, max_length=160),
        }
        active_filters = {name: value for name, value in filters.items() if value is not None}
        events = self._load_timeline_events()
        reference_index = self._build_timeline_reference_index(events)
        resolved_references = self._resolve_timeline_references(active_filters, reference_index, events)
        missing_filters = {
            name: value
            for name, value in active_filters.items()
            if value not in reference_index.get(name, set())
        }
        if active_filters:
            events = [
                event
                for event in events
                if self._timeline_event_matches(event, resolved_references, active_filters)
            ]
        events.sort(key=lambda item: (item["timestamp"] or "", item["event_id"]))
        if limit is not None:
            events = events[: int(limit)]
        return {
            "events": events,
            "filters": active_filters,
            "missing_filters": missing_filters,
            "resolved_references": {key: sorted(value) for key, value in sorted(resolved_references.items()) if value},
            "total": len(events),
        }

    def _load_governance_audit_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for row in read_jsonl(self.governance_audit_path):
            events.append(
                self._audit_event(
                    actor=row["actor"],
                    event_id=f"governance-{hashlib.sha256(canonical_json(row).encode('utf-8')).hexdigest()[:12]}",
                    event_type=row["action"],
                    payload=row,
                    service="governance",
                    status=row["status"],
                    timestamp=row["occurred_at"],
                )
            )
        return events

    def _load_gateway_audit_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for row in read_jsonl(self.gateway_audit_path):
            actor = row["subject"] if row.get("subject") is not None else row["user_id"]
            events.append(
                self._audit_event(
                    actor=actor,
                    event_id=f"gateway-{hashlib.sha256(canonical_json(row).encode('utf-8')).hexdigest()[:12]}",
                    event_type="tool_call",
                    payload=row,
                    service="gateway",
                    status=row["status"],
                    timestamp=row["called_at"],
                )
            )
        return events

    def _load_execution_audit_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for row in read_jsonl(self.execution.store.audit_path):
            actor = (
                row.get("requested_by")
                or row.get("recorded_by")
                or row.get("reconciled_by")
                or row.get("submitted_by")
            )
            timestamp = (
                row.get("attempted_at")
                or row.get("requested_at")
                or row.get("recorded_at")
                or row.get("reconciled_at")
            )
            event_type = row.get("action", "submit_approved_order")
            events.append(
                self._audit_event(
                    actor=actor,
                    event_id=f"execution-{hashlib.sha256(canonical_json(row).encode('utf-8')).hexdigest()[:12]}",
                    event_type=event_type,
                    payload=row,
                    service="execution",
                    status=row.get("status", "ok"),
                    timestamp=timestamp,
                )
            )
        return events

    def _load_risk_audit_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for path in sorted(self.risk.store.decisions_dir.glob("*.json")):
            row = read_json(path)
            events.append(
                self._audit_event(
                    actor=row["decided_by"],
                    event_id=row["decision_id"],
                    event_type="risk_decision",
                    payload=row,
                    service="risk",
                    status=row["outcome"],
                    timestamp=row["created_at"],
                )
            )
        for path in sorted(self.risk.store.reviews_dir.glob("*.jsonl")):
            for row in read_jsonl(path):
                events.append(
                    self._audit_event(
                        actor=row["reviewer"],
                        event_id=row["review_id"],
                        event_type="human_review",
                        payload=row,
                        service="risk",
                        status=row["decision"],
                        timestamp=row["created_at"],
                    )
                )
        return events

    def _audit_event(
        self,
        *,
        actor: Optional[str],
        event_id: str,
        event_type: str,
        payload: dict[str, Any],
        service: str,
        status: str,
        timestamp: Optional[str],
    ) -> dict[str, Any]:
        return {
            "actor": actor,
            "event_id": event_id,
            "event_type": event_type,
            "payload": canonical_copy(payload),
            "service": service,
            "status": status,
            "timestamp": timestamp,
        }

    def _load_timeline_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        events.extend(self._load_experiment_timeline_events())
        events.extend(self._load_backtest_timeline_events())
        events.extend(self._load_paper_timeline_events())
        events.extend(self._load_risk_timeline_events())
        events.extend(self._load_execution_timeline_events())
        events.extend(self._load_governance_timeline_events())
        return events

    def _load_experiment_timeline_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for experiment in self.experiments.list_experiments():
            events.append(
                self._timeline_event(
                    actor=experiment["generated_by"],
                    event_id=f"{experiment['experiment_id']}:created",
                    event_type="experiment_created",
                    payload=experiment,
                    references={"experiment_id": experiment["experiment_id"]},
                    service="experiments",
                    status=experiment["current_status"],
                    summary=f"Experiment {experiment['experiment_id']} was created.",
                    timestamp=experiment["created_at"],
                )
            )
            for row in read_jsonl(self.experiments.store.status_path(experiment["experiment_id"])):
                events.append(
                    self._timeline_event(
                        actor=row.get("changed_by"),
                        event_id=(
                            f"{row['experiment_id']}:status:"
                            f"{row.get('changed_at')}:{row.get('to_status')}"
                        ),
                        event_type="experiment_status_changed",
                        payload=row,
                        references={"experiment_id": row["experiment_id"]},
                        service="experiments",
                        status=row["to_status"],
                        summary=(
                            f"Experiment {row['experiment_id']} moved "
                            f"{row.get('from_status')} -> {row['to_status']}."
                        ),
                        timestamp=row["changed_at"],
                    )
                )
        return events

    def _load_backtest_timeline_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for path in sorted(self.root.joinpath("backtests", "runs").glob("*.json")):
            row = read_json(path)
            events.append(
                self._timeline_event(
                    actor="backtest-runner",
                    event_id=row["run_id"],
                    event_type="backtest_run",
                    payload=row,
                    references={"experiment_id": row.get("experiment_id"), "run_id": row.get("run_id")},
                    service="backtests",
                    status=row.get("status"),
                    summary=f"Backtest {row['run_id']} finished with status {row.get('status')}.",
                    timestamp=row.get("created_at"),
                )
            )
        return events

    def _load_paper_timeline_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for path in sorted(self.root.joinpath("paper", "runs").glob("*.json")):
            row = read_json(path)
            events.append(
                self._timeline_event(
                    actor="paper-runner",
                    event_id=row["paper_run_id"],
                    event_type="paper_run",
                    payload=row,
                    references={
                        "backtest_run_id": row.get("backtest_run_id"),
                        "experiment_id": row.get("experiment_id"),
                        "paper_run_id": row.get("paper_run_id"),
                    },
                    service="paper",
                    status=row.get("status"),
                    summary=f"Paper run {row['paper_run_id']} is {row.get('status')}.",
                    timestamp=row.get("started_at") or row.get("created_at"),
                )
            )
        return events

    def _load_risk_timeline_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for path in sorted(self.risk.store.intents_dir.glob("*.json")):
            row = read_json(path)
            events.append(
                self._timeline_event(
                    actor=row.get("submitted_by"),
                    event_id=row["intent_id"],
                    event_type="trade_intent_created",
                    payload=row,
                    references={
                        "experiment_id": row.get("experiment_id"),
                        "intent_id": row.get("intent_id"),
                        "market_id": row.get("market_id"),
                    },
                    service="risk",
                    status="CREATED",
                    summary=f"Trade intent {row['intent_id']} was created for market {row.get('market_id')}.",
                    timestamp=row.get("created_at"),
                )
            )
        for path in sorted(self.risk.store.reviews_dir.glob("*.jsonl")):
            for row in read_jsonl(path):
                intent = self._read_timeline_json(self.risk.store.intent_path(row["intent_id"]))
                events.append(
                    self._timeline_event(
                        actor=row.get("reviewer"),
                        event_id=row["review_id"],
                        event_type="trade_intent_reviewed",
                        payload=row,
                        references={
                            "experiment_id": None if intent is None else intent.get("experiment_id"),
                            "intent_id": row.get("intent_id"),
                            "market_id": None if intent is None else intent.get("market_id"),
                        },
                        service="risk",
                        status=row.get("decision"),
                        summary=f"Trade intent {row['intent_id']} received {row.get('decision')} review.",
                        timestamp=row.get("created_at"),
                    )
                )
        for path in sorted(self.risk.store.decisions_dir.glob("*.json")):
            row = read_json(path)
            events.append(
                self._timeline_event(
                    actor=row.get("decided_by"),
                    event_id=row["decision_id"],
                    event_type="risk_decision",
                    payload=row,
                    references={
                        "decision_id": row.get("decision_id"),
                        "experiment_id": row.get("experiment_id"),
                        "intent_id": row.get("intent_id"),
                        "market_id": row.get("market_id"),
                    },
                    service="risk",
                    status=row.get("outcome"),
                    summary=f"Risk decision {row['decision_id']} returned {row.get('outcome')}.",
                    timestamp=row.get("created_at"),
                )
            )
        return events

    def _load_execution_timeline_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        execution_records = self._timeline_execution_records_by_id()
        for row in execution_records.values():
            events.append(
                self._timeline_event(
                    actor=row.get("submitted_by"),
                    event_id=row["execution_id"],
                    event_type="execution_submitted",
                    payload=row,
                    references=self._execution_references(row),
                    service="execution",
                    status=row.get("status"),
                    summary=f"Execution {row['execution_id']} was submitted with status {row.get('status')}.",
                    timestamp=row.get("submitted_at") or row.get("created_at"),
                )
            )
            for fill in row.get("fills", []):
                events.append(
                    self._timeline_event(
                        actor=fill.get("recorded_by"),
                        event_id=fill["fill_id"],
                        event_type="execution_fill_recorded",
                        payload=fill,
                        references=self._execution_references(row),
                        service="execution",
                        status=row.get("status"),
                        summary=f"Fill {fill['fill_id']} was recorded for execution {row['execution_id']}.",
                        timestamp=fill.get("recorded_at"),
                    )
                )
        for row in read_jsonl(self.execution.store.audit_path):
            references = self._execution_audit_references(row, execution_records)
            event_type = row.get("action", "submit_approved_order")
            timestamp = (
                row.get("attempted_at")
                or row.get("requested_at")
                or row.get("recorded_at")
                or row.get("reconciled_at")
            )
            actor = (
                row.get("submitted_by")
                or row.get("requested_by")
                or row.get("recorded_by")
                or row.get("reconciled_by")
            )
            events.append(
                self._timeline_event(
                    actor=actor,
                    event_id=f"execution-audit-{hashlib.sha256(canonical_json(row).encode('utf-8')).hexdigest()[:12]}",
                    event_type=event_type,
                    payload=row,
                    references=references,
                    service="execution",
                    status=row.get("status", "ok"),
                    summary=f"Execution audit event {event_type} recorded status {row.get('status', 'ok')}.",
                    timestamp=timestamp,
                )
            )
        return events

    def _load_governance_timeline_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for path in sorted(self.governance_requests_dir.glob("*.json")):
            row = read_json(path)
            references = self._governance_request_references(row)
            events.append(
                self._timeline_event(
                    actor=row.get("requested_by"),
                    event_id=row["request_id"],
                    event_type="governance_request_created",
                    payload=row,
                    references=references,
                    service="governance",
                    status=row.get("status"),
                    summary=f"Governance request {row['request_id']} was created.",
                    timestamp=row.get("requested_at"),
                )
            )
            for index, review in enumerate(row.get("reviews", [])):
                events.append(
                    self._timeline_event(
                        actor=review.get("reviewer"),
                        event_id=f"{row['request_id']}:review:{index}",
                        event_type="governance_request_reviewed",
                        payload=review,
                        references=references,
                        service="governance",
                        status=review.get("decision"),
                        summary=f"Governance request {row['request_id']} received {review.get('decision')} review.",
                        timestamp=review.get("reviewed_at"),
                    )
                )
            if row.get("applied_at") is not None:
                events.append(
                    self._timeline_event(
                        actor=row.get("applied_by"),
                        event_id=f"{row['request_id']}:applied",
                        event_type="governance_request_applied",
                        payload=row.get("apply_result") or {},
                        references=references,
                        service="governance",
                        status="APPLIED",
                        summary=f"Governance request {row['request_id']} was applied.",
                        timestamp=row.get("applied_at"),
                    )
                )
        for row in read_jsonl(self.governance_audit_path):
            events.append(
                self._timeline_event(
                    actor=row.get("actor"),
                    event_id=f"governance-audit-{hashlib.sha256(canonical_json(row).encode('utf-8')).hexdigest()[:12]}",
                    event_type=row.get("action", "governance_audit"),
                    payload=row,
                    references={},
                    service="governance",
                    status=row.get("status"),
                    summary=f"Governance audit event {row.get('action')} recorded status {row.get('status')}.",
                    timestamp=row.get("occurred_at"),
                )
            )
        return events

    def _build_timeline_reference_index(self, events: list[dict[str, Any]]) -> dict[str, set[str]]:
        index: dict[str, set[str]] = {
            "decision_id": set(),
            "execution_id": set(),
            "experiment_id": set(),
            "intent_id": set(),
            "market_id": set(),
            "request_id": set(),
        }
        for event in events:
            for key in index:
                value = event["references"].get(key)
                if value is not None:
                    index[key].add(str(value))
        return index

    def _resolve_timeline_references(
        self,
        filters: dict[str, str],
        reference_index: dict[str, set[str]],
        events: list[dict[str, Any]],
    ) -> dict[str, set[str]]:
        resolved: dict[str, set[str]] = {key: set() for key in reference_index}
        for key, value in filters.items():
            if value in reference_index.get(key, set()):
                resolved[key].add(value)
        changed = True
        while changed:
            changed = False
            for event in events:
                references = event["references"]
                if not any(value in resolved.get(key, set()) for key, value in references.items() if key in resolved):
                    continue
                for key in resolved:
                    value = references.get(key)
                    if value is not None and str(value) not in resolved[key]:
                        resolved[key].add(str(value))
                        changed = True
        return resolved

    def _timeline_event_matches(
        self,
        event: dict[str, Any],
        resolved_references: dict[str, set[str]],
        active_filters: dict[str, str],
    ) -> bool:
        references = event["references"]
        for key, value in references.items():
            if key in resolved_references and str(value) in resolved_references[key]:
                return True
        for key, value in active_filters.items():
            if references.get(key) == value:
                return True
        return False

    def _timeline_event(
        self,
        *,
        actor: Optional[str],
        event_id: str,
        event_type: str,
        payload: dict[str, Any],
        references: dict[str, Optional[str]],
        service: str,
        status: Optional[str],
        summary: str,
        timestamp: Optional[str],
    ) -> dict[str, Any]:
        return {
            "actor": actor,
            "event_id": event_id,
            "event_type": event_type,
            "payload": canonical_copy(payload),
            "references": {key: value for key, value in references.items() if value is not None},
            "service": service,
            "status": status,
            "summary": summary,
            "timestamp": timestamp,
        }

    def _timeline_execution_records_by_id(self) -> dict[str, dict[str, Any]]:
        records: dict[str, dict[str, Any]] = {}
        if not self.execution.store.orders_dir.exists():
            return records
        for path in sorted(self.execution.store.orders_dir.glob("*.json")):
            row = read_json(path)
            records[str(row["execution_id"])] = row
        return records

    def _execution_references(self, row: dict[str, Any]) -> dict[str, Optional[str]]:
        return {
            "decision_id": row.get("risk_decision_id"),
            "execution_id": row.get("execution_id"),
            "experiment_id": row.get("experiment_id"),
            "intent_id": row.get("intent_id"),
            "market_id": row.get("market_id"),
        }

    def _execution_audit_references(
        self,
        row: dict[str, Any],
        execution_records: dict[str, dict[str, Any]],
    ) -> dict[str, Optional[str]]:
        references = {
            "decision_id": row.get("risk_decision_id"),
            "execution_id": row.get("execution_id"),
            "experiment_id": row.get("experiment_id"),
            "intent_id": row.get("intent_id"),
            "request_id": row.get("request_id"),
        }
        execution_id = references["execution_id"]
        if execution_id is not None and execution_id in execution_records:
            references.update(self._execution_references(execution_records[execution_id]))
        intent_id = references["intent_id"]
        if intent_id is not None:
            intent = self._read_timeline_json(self.risk.store.intent_path(str(intent_id)))
            if intent is not None:
                references["experiment_id"] = references.get("experiment_id") or intent.get("experiment_id")
                references["market_id"] = intent.get("market_id")
        decision_id = references["decision_id"]
        if decision_id is not None:
            decision = self._read_timeline_json(self.risk.store.decision_path(str(decision_id)))
            if decision is not None:
                references["experiment_id"] = references.get("experiment_id") or decision.get("experiment_id")
                references["intent_id"] = references.get("intent_id") or decision.get("intent_id")
                references["market_id"] = references.get("market_id") or decision.get("market_id")
        return references

    def _governance_request_references(self, row: dict[str, Any]) -> dict[str, Optional[str]]:
        payload = row.get("payload") or {}
        references = {
            "experiment_id": payload.get("experiment_id"),
            "request_id": row.get("request_id"),
        }
        apply_result = row.get("apply_result") or {}
        if references["experiment_id"] is None:
            references["experiment_id"] = apply_result.get("experiment_id")
        return references

    def _read_timeline_json(self, path: Path) -> Optional[dict[str, Any]]:
        if not path.exists():
            return None
        payload = read_json(path)
        return payload if isinstance(payload, dict) else None


AuditTrailServiceType = AuditTrailService


def build_audit_trail_service(root: Path) -> AuditTrailService:
    from .runtime import build_workspace

    return build_workspace(root).audit
