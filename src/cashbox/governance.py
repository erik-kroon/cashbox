from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
from pathlib import Path
from typing import Any, Optional

from .execution import DEFAULT_EXECUTION_POLICY, ExecutionService
from .experiments import ExperimentLifecycleError, ExperimentService
from .models import format_datetime, utc_now
from .persistence import append_jsonl, canonical_copy, canonical_json, read_json, read_jsonl, write_json
from .risk import DEFAULT_RISK_POLICY, RiskGatewayService

GOVERNANCE_ROLES = (
    "VIEWER",
    "RESEARCHER",
    "OPERATOR",
    "GOVERNOR",
    "SECURITY_ADMIN",
    "BREAK_GLASS_ADMIN",
)
GOVERNANCE_POLICY_TYPES = ("risk", "execution")
GOVERNANCE_REQUEST_STATUSES = ("PENDING", "APPROVED", "REJECTED", "APPLIED")
GOVERNANCE_REVIEW_DECISIONS = ("APPROVE", "REJECT")
CAPITAL_LIMIT_FIELDS = (
    "max_notional_usd",
    "market_exposure_limit_usd",
    "event_exposure_limit_usd",
    "portfolio_exposure_limit_usd",
    "daily_loss_limit_usd",
    "drawdown_limit_usd",
)

ACTION_PERMISSIONS: dict[str, set[str]] = {
    "apply_governance_request": {"OPERATOR", "GOVERNOR", "BREAK_GLASS_ADMIN"},
    "manage_roles": {"GOVERNOR", "BREAK_GLASS_ADMIN"},
    "request_emergency_halt": {"OPERATOR", "GOVERNOR", "BREAK_GLASS_ADMIN"},
    "request_policy_change": {"OPERATOR", "GOVERNOR", "BREAK_GLASS_ADMIN"},
    "request_strategy_promotion": {"OPERATOR", "GOVERNOR", "BREAK_GLASS_ADMIN"},
    "view_audit_console": set(GOVERNANCE_ROLES),
    "view_policy": set(GOVERNANCE_ROLES),
    "view_subjects": set(GOVERNANCE_ROLES),
}


def _require_text(name: str, value: Any, *, max_length: int = 2000) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise GovernanceValidationError(f"{name} must be non-empty")
    if len(normalized) > max_length:
        raise GovernanceValidationError(f"{name} exceeds max length {max_length}")
    return normalized


def _normalize_role(role: Any) -> str:
    normalized = _require_text("role", role, max_length=120).upper()
    if normalized not in GOVERNANCE_ROLES:
        raise GovernanceValidationError(f"unsupported governance role: {normalized}")
    return normalized


def _normalize_review_decision(decision: Any) -> str:
    normalized = _require_text("decision", decision, max_length=120).upper()
    if normalized not in GOVERNANCE_REVIEW_DECISIONS:
        raise GovernanceValidationError(f"decision must be one of: {', '.join(GOVERNANCE_REVIEW_DECISIONS)}")
    return normalized


def _normalize_policy_type(policy_type: Any) -> str:
    normalized = _require_text("policy_type", policy_type, max_length=120).lower()
    if normalized not in GOVERNANCE_POLICY_TYPES:
        raise GovernanceValidationError(f"policy_type must be one of: {', '.join(GOVERNANCE_POLICY_TYPES)}")
    return normalized


def _decimal_text(value: Any, *, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise GovernanceValidationError(f"{field_name} must be numeric") from exc


class GovernanceServiceError(Exception):
    pass


class GovernanceNotFoundError(GovernanceServiceError):
    pass


class GovernanceAuthorizationError(GovernanceServiceError):
    pass


class GovernanceValidationError(GovernanceServiceError):
    pass


@dataclass
class FileSystemGovernanceStore:
    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def governance_dir(self) -> Path:
        return self.root / "governance"

    @property
    def audit_path(self) -> Path:
        return self.governance_dir / "audit.jsonl"

    @property
    def requests_dir(self) -> Path:
        return self.governance_dir / "requests"

    @property
    def subjects_dir(self) -> Path:
        return self.governance_dir / "subjects"

    @property
    def policies_dir(self) -> Path:
        return self.governance_dir / "policies"

    def request_path(self, request_id: str) -> Path:
        return self.requests_dir / f"{request_id}.json"

    def subject_path(self, subject: str) -> Path:
        return self.subjects_dir / f"{subject}.json"

    def policy_type_dir(self, policy_type: str) -> Path:
        return self.policies_dir / policy_type

    def policy_version_path(self, policy_type: str, version: int) -> Path:
        return self.policy_type_dir(policy_type) / f"{version}.json"


class GovernanceService:
    def __init__(
        self,
        store: FileSystemGovernanceStore,
        *,
        experiments: ExperimentService,
        execution: ExecutionService,
        risk: RiskGatewayService,
    ) -> None:
        self.store = store
        self.experiments = experiments
        self.execution = execution
        self.risk = risk

    def bootstrap_subject(
        self,
        subject: str,
        *,
        roles: list[str] | tuple[str, ...],
        bootstrapped_by: str = "system-bootstrap",
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        if any(self.store.subjects_dir.glob("*.json")):
            raise GovernanceValidationError("governance subjects already exist; use assign_role instead")
        normalized_subject = _require_text("subject", subject, max_length=200)
        normalized_roles = self._normalize_roles(roles)
        created_at = format_datetime(now or utc_now()) or ""
        payload = {
            "roles": normalized_roles,
            "subject": normalized_subject,
            "updated_at": created_at,
        }
        write_json(self.store.subject_path(normalized_subject), payload)
        self._audit(
            action="bootstrap_subject",
            actor=_require_text("bootstrapped_by", bootstrapped_by, max_length=200),
            occurred_at=created_at,
            status="ok",
            payload={"roles": normalized_roles, "subject": normalized_subject},
        )
        return payload

    def assign_role(
        self,
        subject: str,
        *,
        role: str,
        granted_by: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        self._authorize(granted_by, action="manage_roles")
        normalized_subject = _require_text("subject", subject, max_length=200)
        normalized_role = _normalize_role(role)
        created_at = format_datetime(now or utc_now()) or ""
        payload = self._load_subject(normalized_subject, allow_missing=True)
        roles = sorted(set(payload.get("roles", [])) | {normalized_role})
        next_payload = {
            "roles": roles,
            "subject": normalized_subject,
            "updated_at": created_at,
        }
        write_json(self.store.subject_path(normalized_subject), next_payload)
        self._audit(
            action="assign_role",
            actor=_require_text("granted_by", granted_by, max_length=200),
            occurred_at=created_at,
            status="ok",
            payload={"role": normalized_role, "subject": normalized_subject},
        )
        return next_payload

    def get_subject(self, subject: str) -> dict[str, Any]:
        return self._load_subject(_require_text("subject", subject, max_length=200))

    def list_subjects(self) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for path in sorted(self.store.subjects_dir.glob("*.json")):
            payloads.append(read_json(path))
        return payloads

    def request_strategy_promotion(
        self,
        experiment_id: str,
        *,
        requested_by: str,
        reason: str,
        target_status: str = "PRODUCTION_APPROVED",
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        self._authorize(requested_by, action="request_strategy_promotion")
        experiment = self.experiments.get_experiment(_require_text("experiment_id", experiment_id, max_length=160))
        normalized_target_status = _require_text("target_status", target_status, max_length=120).upper()
        if normalized_target_status != "PRODUCTION_APPROVED":
            raise GovernanceValidationError("strategy promotion requests only support target_status=PRODUCTION_APPROVED")
        if experiment["current_status"] != "SCALE_REVIEW":
            raise GovernanceValidationError("experiment must be in SCALE_REVIEW before governance promotion")
        return self._create_request(
            kind="STRATEGY_PROMOTION",
            change_scope="STRATEGY_PROMOTION",
            payload={
                "current_status": experiment["current_status"],
                "experiment_id": experiment["experiment_id"],
                "target_status": normalized_target_status,
            },
            requested_by=requested_by,
            reason=reason,
            required_role="GOVERNOR",
            now=now,
        )

    def request_policy_change(
        self,
        policy_type: str,
        updates: dict[str, Any],
        *,
        requested_by: str,
        reason: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        self._authorize(requested_by, action="request_policy_change")
        normalized_policy_type = _normalize_policy_type(policy_type)
        if not isinstance(updates, dict) or not updates:
            raise GovernanceValidationError("policy updates must be a non-empty JSON object")
        active_policy = self.get_active_policy(normalized_policy_type)
        proposed_policy = self._merge_policy_updates(normalized_policy_type, active_policy["policy"], updates)
        change_scope = self._classify_policy_change_scope(
            normalized_policy_type,
            current_policy=active_policy["policy"],
            proposed_policy=proposed_policy,
        )
        return self._create_request(
            kind="POLICY_CHANGE",
            change_scope=change_scope,
            payload={
                "policy_type": normalized_policy_type,
                "previous_policy_sha256": active_policy["policy_sha256"],
                "previous_version": active_policy["version"],
                "proposed_policy": proposed_policy,
                "proposed_policy_sha256": hashlib.sha256(canonical_json(proposed_policy).encode("utf-8")).hexdigest(),
                "updates": canonical_copy(updates),
            },
            requested_by=requested_by,
            reason=reason,
            required_role="GOVERNOR",
            now=now,
        )

    def get_request(self, request_id: str) -> dict[str, Any]:
        normalized_request_id = _require_text("request_id", request_id, max_length=160)
        path = self.store.request_path(normalized_request_id)
        if not path.exists():
            raise GovernanceNotFoundError(f"unknown governance request_id: {normalized_request_id}")
        return read_json(path)

    def review_request(
        self,
        request_id: str,
        *,
        reviewer: str,
        decision: str,
        reason: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        request = self.get_request(request_id)
        self._authorize_required_role(reviewer, request["required_role"])
        if request["status"] != "PENDING":
            raise GovernanceValidationError(f"governance request {request['request_id']} is already reviewed")
        created_at = format_datetime(now or utc_now()) or ""
        review = {
            "decision": _normalize_review_decision(decision),
            "reason": _require_text("reason", reason, max_length=2000),
            "reviewed_at": created_at,
            "reviewer": _require_text("reviewer", reviewer, max_length=200),
        }
        request["reviews"] = [*request.get("reviews", []), review]
        request["status"] = "APPROVED" if review["decision"] == "APPROVE" else "REJECTED"
        request["updated_at"] = created_at
        write_json(self.store.request_path(request["request_id"]), request)
        self._audit(
            action="review_request",
            actor=review["reviewer"],
            occurred_at=created_at,
            status=request["status"].lower(),
            payload={"decision": review["decision"], "request_id": request["request_id"]},
        )
        return request

    def apply_request(
        self,
        request_id: str,
        *,
        applied_by: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        self._authorize(applied_by, action="apply_governance_request")
        request = self.get_request(request_id)
        if request["status"] != "APPROVED":
            raise GovernanceValidationError(f"governance request {request['request_id']} must be APPROVED before apply")
        created_at = format_datetime(now or utc_now()) or ""
        result: dict[str, Any]
        if request["kind"] == "STRATEGY_PROMOTION":
            result = self.experiments.transition_experiment_status(
                request["payload"]["experiment_id"],
                to_status=request["payload"]["target_status"],
                changed_by=_require_text("applied_by", applied_by, max_length=200),
                reason=f"governance_request_id={request['request_id']}",
                now=now,
            )
        elif request["kind"] == "POLICY_CHANGE":
            policy_type = request["payload"]["policy_type"]
            version = self._next_policy_version(policy_type)
            result = {
                "activated_at": created_at,
                "activated_by": _require_text("applied_by", applied_by, max_length=200),
                "policy": request["payload"]["proposed_policy"],
                "policy_sha256": request["payload"]["proposed_policy_sha256"],
                "policy_type": policy_type,
                "source_request_id": request["request_id"],
                "version": version,
            }
            write_json(self.store.policy_version_path(policy_type, version), result)
        else:
            raise GovernanceValidationError(f"unsupported governance request kind: {request['kind']}")

        request["applied_at"] = created_at
        request["applied_by"] = _require_text("applied_by", applied_by, max_length=200)
        request["apply_result"] = result
        request["status"] = "APPLIED"
        request["updated_at"] = created_at
        write_json(self.store.request_path(request["request_id"]), request)
        self._audit(
            action="apply_request",
            actor=request["applied_by"],
            occurred_at=created_at,
            status="ok",
            payload={"kind": request["kind"], "request_id": request["request_id"]},
        )
        return request

    def request_emergency_halt(
        self,
        *,
        requested_by: str,
        reason: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        self._authorize(requested_by, action="request_emergency_halt")
        created_at = format_datetime(now or utc_now()) or ""
        result = self.execution.request_global_halt(
            reason=reason,
            requested_by=_require_text("requested_by", requested_by, max_length=200),
            now=now,
        )
        self._audit(
            action="request_emergency_halt",
            actor=result["requested_by"],
            occurred_at=created_at,
            status="ok",
            payload={"execution_request_id": result["request_id"], "reason": result["reason"]},
        )
        return result

    def get_active_policy(self, policy_type: str) -> dict[str, Any]:
        normalized_policy_type = _normalize_policy_type(policy_type)
        versions = self._list_policy_versions(normalized_policy_type)
        if not versions:
            default_policy = self._default_policy(normalized_policy_type)
            return {
                "activated_at": None,
                "activated_by": None,
                "policy": default_policy,
                "policy_sha256": hashlib.sha256(canonical_json(default_policy).encode("utf-8")).hexdigest(),
                "policy_type": normalized_policy_type,
                "source_request_id": None,
                "version": 0,
            }
        return versions[-1]

    def get_policy_version(self, policy_type: str, version: int) -> dict[str, Any]:
        normalized_policy_type = _normalize_policy_type(policy_type)
        if version == 0:
            default_policy = self._default_policy(normalized_policy_type)
            return {
                "activated_at": None,
                "activated_by": None,
                "policy": default_policy,
                "policy_sha256": hashlib.sha256(canonical_json(default_policy).encode("utf-8")).hexdigest(),
                "policy_type": normalized_policy_type,
                "source_request_id": None,
                "version": 0,
            }
        path = self.store.policy_version_path(normalized_policy_type, int(version))
        if not path.exists():
            raise GovernanceNotFoundError(
                f"unknown policy version: type={normalized_policy_type} version={int(version)}"
            )
        return read_json(path)

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

    def _create_request(
        self,
        *,
        kind: str,
        change_scope: str,
        payload: dict[str, Any],
        requested_by: str,
        reason: str,
        required_role: str,
        now: Optional[datetime],
    ) -> dict[str, Any]:
        actor = _require_text("requested_by", requested_by, max_length=200)
        created_at = format_datetime(now or utc_now()) or ""
        request_id = self._build_request_id(kind, actor, created_at, payload)
        request = {
            "applied_at": None,
            "applied_by": None,
            "apply_result": None,
            "change_scope": change_scope,
            "kind": kind,
            "payload": canonical_copy(payload),
            "reason": _require_text("reason", reason, max_length=2000),
            "request_id": request_id,
            "requested_at": created_at,
            "requested_by": actor,
            "required_role": _normalize_role(required_role),
            "reviews": [],
            "status": "PENDING",
            "updated_at": created_at,
        }
        write_json(self.store.request_path(request_id), request)
        self._audit(
            action="create_request",
            actor=actor,
            occurred_at=created_at,
            status="pending",
            payload={"change_scope": change_scope, "kind": kind, "request_id": request_id},
        )
        return request

    def _build_request_id(self, kind: str, requested_by: str, requested_at: str, payload: dict[str, Any]) -> str:
        basis = canonical_json(
            {
                "kind": kind,
                "payload": payload,
                "requested_at": requested_at,
                "requested_by": requested_by,
            }
        )
        return f"gov-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"

    def _audit(self, *, action: str, actor: str, occurred_at: str, status: str, payload: dict[str, Any]) -> None:
        append_jsonl(
            self.store.audit_path,
            {
                "action": action,
                "actor": actor,
                "occurred_at": occurred_at,
                "payload_sha256": hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest(),
                "status": status,
            },
        )

    def _normalize_roles(self, roles: list[str] | tuple[str, ...]) -> list[str]:
        if not isinstance(roles, (list, tuple)) or not roles:
            raise GovernanceValidationError("roles must be a non-empty list")
        return sorted({_normalize_role(role) for role in roles})

    def _load_subject(self, subject: str, *, allow_missing: bool = False) -> dict[str, Any]:
        path = self.store.subject_path(subject)
        if not path.exists():
            if allow_missing:
                return {"roles": [], "subject": subject, "updated_at": None}
            raise GovernanceNotFoundError(f"unknown governance subject: {subject}")
        return read_json(path)

    def _authorize(self, subject: str, *, action: str) -> None:
        normalized_subject = _require_text("subject", subject, max_length=200)
        subject_payload = self._load_subject(normalized_subject)
        roles = set(subject_payload["roles"])
        if "BREAK_GLASS_ADMIN" in roles:
            return
        if roles & ACTION_PERMISSIONS[action]:
            return
        raise GovernanceAuthorizationError(f"subject {normalized_subject} is not authorized for {action}")

    def _authorize_required_role(self, subject: str, required_role: str) -> None:
        normalized_subject = _require_text("subject", subject, max_length=200)
        roles = set(self._load_subject(normalized_subject)["roles"])
        if "BREAK_GLASS_ADMIN" in roles or _normalize_role(required_role) in roles:
            return
        raise GovernanceAuthorizationError(
            f"subject {normalized_subject} must hold role {_normalize_role(required_role)} for this approval"
        )

    def _merge_policy_updates(
        self,
        policy_type: str,
        current_policy: dict[str, Any],
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        merged = canonical_copy(current_policy)
        for key, value in updates.items():
            merged[str(key)] = canonical_copy(value)
        if policy_type == "risk":
            return self.risk._normalize_policy(merged)
        return self.execution._normalize_policy(merged)

    def _classify_policy_change_scope(
        self,
        policy_type: str,
        *,
        current_policy: dict[str, Any],
        proposed_policy: dict[str, Any],
    ) -> str:
        if policy_type != "risk":
            return "EXECUTION_POLICY"
        for field_name in CAPITAL_LIMIT_FIELDS:
            if field_name not in proposed_policy or field_name not in current_policy:
                continue
            if _decimal_text(proposed_policy[field_name], field_name=field_name) > _decimal_text(
                current_policy[field_name], field_name=field_name
            ):
                return "CAPITAL_LIMIT"
        return "RISK_POLICY"

    def _next_policy_version(self, policy_type: str) -> int:
        return len(self._list_policy_versions(policy_type)) + 1

    def _list_policy_versions(self, policy_type: str) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for path in sorted(
            self.store.policy_type_dir(policy_type).glob("*.json"),
            key=lambda item: int(item.stem),
        ):
            payloads.append(read_json(path))
        return payloads

    def _default_policy(self, policy_type: str) -> dict[str, Any]:
        if policy_type == "risk":
            return canonical_copy(DEFAULT_RISK_POLICY)
        return canonical_copy(DEFAULT_EXECUTION_POLICY)

    def _load_governance_audit_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for row in read_jsonl(self.store.audit_path):
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
        for row in read_jsonl(self.store.root / "gateway" / "audit.jsonl"):
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


GovernanceServiceType = GovernanceService


def build_governance_service(root: Path) -> GovernanceService:
    from .runtime import build_workspace

    return build_workspace(root).governance
