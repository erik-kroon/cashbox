from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
from pathlib import Path
from typing import Any, Optional

from .models import format_datetime, utc_now
from .persistence import append_jsonl, canonical_copy, canonical_json, read_json, write_json
from .risk import RiskGatewayService, RiskNotFoundError

EXECUTION_POLICY_VERSION = 1
EXECUTION_STATUSES = ("NOT_SUBMITTED", "SUBMITTED")

DEFAULT_EXECUTION_POLICY: dict[str, Any] = {
    "live_executor_healthy": True,
    "signer_service_healthy": True,
}


def _require_text(name: str, value: Any, *, max_length: int = 2000) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ExecutionValidationError(f"{name} must be non-empty")
    if len(normalized) > max_length:
        raise ExecutionValidationError(f"{name} exceeds max length {max_length}")
    return normalized


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    raise ExecutionValidationError("execution policy booleans must be bool-compatible")


class ExecutionServiceError(Exception):
    pass


class ExecutionNotFoundError(ExecutionServiceError):
    pass


class ExecutionValidationError(ExecutionServiceError):
    pass


@dataclass
class FileSystemExecutionStore:
    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def execution_dir(self) -> Path:
        return self.root / "execution"

    @property
    def orders_dir(self) -> Path:
        return self.execution_dir / "orders"

    @property
    def signatures_dir(self) -> Path:
        return self.execution_dir / "signatures"

    @property
    def state_dir(self) -> Path:
        return self.execution_dir / "state"

    @property
    def audit_path(self) -> Path:
        return self.execution_dir / "audit.jsonl"

    def order_path(self, execution_id: str) -> Path:
        return self.orders_dir / f"{execution_id}.json"

    def signature_path(self, signature_id: str) -> Path:
        return self.signatures_dir / f"{signature_id}.json"

    def state_path(self, intent_id: str) -> Path:
        return self.state_dir / f"{intent_id}.json"


class ExecutionService:
    def __init__(self, store: FileSystemExecutionStore, *, risk: RiskGatewayService) -> None:
        self.store = store
        self.risk = risk

    def submit_approved_order(
        self,
        intent_id: str,
        *,
        approval_token: str,
        submitted_by: str = "risk-gateway",
        policy: Optional[dict[str, Any]] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        normalized_intent_id = _require_text("intent_id", intent_id, max_length=160)
        normalized_token = _require_text("approval_token", approval_token, max_length=200)
        actor = _require_text("submitted_by", submitted_by, max_length=200)
        current_time = now or utc_now()
        created_at = format_datetime(current_time) or ""
        execution_policy = self._normalize_policy(policy)
        policy_sha256 = hashlib.sha256(canonical_json(execution_policy).encode("utf-8")).hexdigest()
        audit_payload: dict[str, Any] = {
            "attempted_at": created_at,
            "approval_token_sha256": hashlib.sha256(normalized_token.encode("utf-8")).hexdigest(),
            "execution_id": None,
            "intent_id": normalized_intent_id,
            "policy_sha256": policy_sha256,
            "risk_decision_id": None,
            "status": "validation_failed",
            "submitted_by": actor,
        }

        try:
            trade_intent = self.risk.get_trade_intent(normalized_intent_id)
            decision = trade_intent["latest_decision"]
            intent_state = trade_intent["state"]
            if decision is None:
                raise ExecutionValidationError(f"trade intent {normalized_intent_id} has no risk decision")
            audit_payload["risk_decision_id"] = decision["decision_id"]
            if decision["outcome"] != "ALLOW":
                raise ExecutionValidationError(
                    f"trade intent {normalized_intent_id} is not approved for release: {decision['outcome']}"
                )
            if intent_state["approval_token"] != normalized_token or decision["approval_token"] != normalized_token:
                raise ExecutionValidationError("approval token does not match the latest approved risk decision")

            execution_state = self._load_execution_state(normalized_intent_id)
            if execution_state["approval_token_used"] == normalized_token:
                raise ExecutionValidationError(
                    f"approval token for trade intent {normalized_intent_id} was already consumed by signer/executor"
                )
            if execution_state["latest_execution_id"] is not None:
                raise ExecutionValidationError(
                    f"trade intent {normalized_intent_id} already has a submitted live execution"
                )
            if not execution_policy["signer_service_healthy"]:
                raise ExecutionValidationError("signer-service is unhealthy and cannot sign approved payloads")
            if not execution_policy["live_executor_healthy"]:
                raise ExecutionValidationError("live-executor is unhealthy and cannot submit approved payloads")

            try:
                unsigned_order = self._build_unsigned_order(
                    trade_intent=trade_intent,
                    decision=decision,
                    approval_token=normalized_token,
                    submitted_by=actor,
                    submitted_at=created_at,
                )
            except KeyError as exc:
                raise ExecutionValidationError(
                    f"market metadata is missing for approved trade intent {normalized_intent_id}"
                ) from exc
            signature_record = self._sign_payload(
                unsigned_order=unsigned_order,
                trade_intent=trade_intent,
                decision=decision,
                approval_token=normalized_token,
                signed_at=created_at,
            )
            execution_record = self._submit_to_live_executor(
                trade_intent=trade_intent,
                decision=decision,
                unsigned_order=unsigned_order,
                signature_record=signature_record,
                execution_policy=execution_policy,
                created_at=created_at,
            )

            write_json(self.store.signature_path(signature_record["signature_id"]), signature_record)
            write_json(self.store.order_path(execution_record["execution_id"]), execution_record)
            write_json(
                self.store.state_path(normalized_intent_id),
                {
                    "approval_token_used": normalized_token,
                    "current_status": execution_record["status"],
                    "intent_id": normalized_intent_id,
                    "latest_execution_id": execution_record["execution_id"],
                    "updated_at": created_at,
                },
            )
            audit_payload["execution_id"] = execution_record["execution_id"]
            audit_payload["status"] = "ok"
            return execution_record
        except RiskNotFoundError as exc:
            raise ExecutionNotFoundError(str(exc)) from exc
        finally:
            self.store_audit_row(audit_payload)

    def get_execution_record(self, execution_id: str) -> dict[str, Any]:
        normalized_execution_id = _require_text("execution_id", execution_id, max_length=160)
        path = self.store.order_path(normalized_execution_id)
        if not path.exists():
            raise ExecutionNotFoundError(f"unknown execution_id: {normalized_execution_id}")
        return read_json(path)

    def get_execution_state(self, intent_id: str) -> dict[str, Any]:
        normalized_intent_id = _require_text("intent_id", intent_id, max_length=160)
        try:
            self.risk.get_trade_intent(normalized_intent_id)
        except RiskNotFoundError as exc:
            raise ExecutionNotFoundError(str(exc)) from exc
        state = self._load_execution_state(normalized_intent_id)
        payload = dict(state)
        if state["latest_execution_id"] is None:
            payload["latest_execution"] = None
        else:
            payload["latest_execution"] = self.get_execution_record(state["latest_execution_id"])
        return payload

    def store_audit_row(self, payload: dict[str, Any]) -> None:
        append_jsonl(self.store.audit_path, payload)

    def _load_execution_state(self, intent_id: str) -> dict[str, Any]:
        path = self.store.state_path(intent_id)
        if not path.exists():
            return {
                "approval_token_used": None,
                "current_status": "NOT_SUBMITTED",
                "intent_id": intent_id,
                "latest_execution_id": None,
                "updated_at": None,
            }
        return read_json(path)

    def _normalize_policy(self, policy: Optional[dict[str, Any]]) -> dict[str, Any]:
        if policy is None:
            merged = canonical_copy(DEFAULT_EXECUTION_POLICY)
        else:
            if not isinstance(policy, dict):
                raise ExecutionValidationError("policy must be a JSON object")
            merged = canonical_copy(DEFAULT_EXECUTION_POLICY)
            for key, value in policy.items():
                if key not in DEFAULT_EXECUTION_POLICY:
                    raise ExecutionValidationError(f"unsupported execution policy field: {key}")
                merged[key] = canonical_copy(value)
        for field_name in DEFAULT_EXECUTION_POLICY:
            merged[field_name] = _normalize_bool(merged[field_name])
        return merged

    def _build_unsigned_order(
        self,
        *,
        trade_intent: dict[str, Any],
        decision: dict[str, Any],
        approval_token: str,
        submitted_by: str,
        submitted_at: str,
    ) -> dict[str, Any]:
        order_request = trade_intent["order_request"]
        market_metadata = self.risk.read_path.get_market_metadata(trade_intent["market_id"])
        outcome_token_id = None
        for outcome in market_metadata.get("outcomes", []):
            if outcome.get("outcome") == order_request["outcome"]:
                outcome_token_id = outcome.get("token_id")
                break

        return {
            "approval_token": approval_token,
            "event_id": market_metadata.get("event_id"),
            "experiment_id": trade_intent["experiment_id"],
            "intent_id": trade_intent["intent_id"],
            "market_id": trade_intent["market_id"],
            "notional_usd": order_request["notional_usd"],
            "order_class": order_request["order_class"],
            "outcome": order_request["outcome"],
            "policy_sha256": decision["policy_sha256"],
            "price": order_request["price"],
            "quantity": order_request["quantity"],
            "requested_at": order_request["requested_at"],
            "risk_decision_id": decision["decision_id"],
            "side": order_request["side"],
            "submitted_at": submitted_at,
            "submitted_by": submitted_by,
            "time_in_force": order_request["time_in_force"],
            "token_id": outcome_token_id,
        }

    def _sign_payload(
        self,
        *,
        unsigned_order: dict[str, Any],
        trade_intent: dict[str, Any],
        decision: dict[str, Any],
        approval_token: str,
        signed_at: str,
    ) -> dict[str, Any]:
        payload_sha256 = hashlib.sha256(canonical_json(unsigned_order).encode("utf-8")).hexdigest()
        signature_id = self._build_signature_id(trade_intent["intent_id"], approval_token, payload_sha256)
        return {
            "approval_token": approval_token,
            "created_at": signed_at,
            "intent_id": trade_intent["intent_id"],
            "key_id": "cashbox-signer",
            "payload_sha256": payload_sha256,
            "risk_decision_id": decision["decision_id"],
            "service": "signer-service",
            "signature": self._build_signature(signature_id, payload_sha256),
            "signature_id": signature_id,
            "unsigned_order": unsigned_order,
        }

    def _submit_to_live_executor(
        self,
        *,
        trade_intent: dict[str, Any],
        decision: dict[str, Any],
        unsigned_order: dict[str, Any],
        signature_record: dict[str, Any],
        execution_policy: dict[str, Any],
        created_at: str,
    ) -> dict[str, Any]:
        execution_id = self._build_execution_id(
            trade_intent["intent_id"], decision["decision_id"], signature_record["signature_id"]
        )
        order_id = self._build_order_id(execution_id)
        return {
            "approval_token": signature_record["approval_token"],
            "created_at": created_at,
            "execution_id": execution_id,
            "execution_policy": execution_policy,
            "experiment_id": trade_intent["experiment_id"],
            "intent_id": trade_intent["intent_id"],
            "live_executor": {
                "order_id": order_id,
                "service": "live-executor",
                "status": "SUBMITTED",
                "submitted_at": created_at,
                "venue": "polymarket-clob",
            },
            "market_id": trade_intent["market_id"],
            "order_payload": unsigned_order,
            "policy_version": EXECUTION_POLICY_VERSION,
            "risk_decision_id": decision["decision_id"],
            "signature": {
                "key_id": signature_record["key_id"],
                "payload_sha256": signature_record["payload_sha256"],
                "service": signature_record["service"],
                "signature": signature_record["signature"],
                "signature_id": signature_record["signature_id"],
            },
            "status": "SUBMITTED",
            "submitted_by": unsigned_order["submitted_by"],
        }

    def _build_signature_id(self, intent_id: str, approval_token: str, payload_sha256: str) -> str:
        basis = f"{intent_id}:{approval_token}:{payload_sha256}:{EXECUTION_POLICY_VERSION}"
        return f"sign-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"

    def _build_signature(self, signature_id: str, payload_sha256: str) -> str:
        basis = f"{signature_id}:{payload_sha256}:cashbox-signer"
        return f"sig-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:16]}"

    def _build_execution_id(self, intent_id: str, decision_id: str, signature_id: str) -> str:
        basis = f"{intent_id}:{decision_id}:{signature_id}:{EXECUTION_POLICY_VERSION}"
        return f"exec-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"

    def _build_order_id(self, execution_id: str) -> str:
        return f"ord-{hashlib.sha256(execution_id.encode('utf-8')).hexdigest()[:12]}"


def build_execution_service(root: Path) -> ExecutionService:
    from .runtime import build_workspace

    return build_workspace(root).execution
