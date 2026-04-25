from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
from pathlib import Path
from typing import Any, Optional

from .experiments import ExperimentNotFoundError
from .models import format_datetime, utc_now
from .persistence import append_jsonl, canonical_copy, canonical_json, read_json, write_json
from .risk import RiskGatewayService, RiskNotFoundError

EXECUTION_POLICY_VERSION = 1
EXECUTION_STATUSES = ("NOT_SUBMITTED", "SUBMITTED", "PARTIALLY_FILLED", "FILLED", "CANCELLED")
OPEN_EXECUTION_STATUSES = {"SUBMITTED", "PARTIALLY_FILLED"}
TERMINAL_EXECUTION_STATUSES = {"FILLED", "CANCELLED"}

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


def _decimal_text(value: Any, *, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ExecutionValidationError(f"{field_name} must be numeric") from exc


def _format_decimal(value: Decimal, *, places: str = "0.00000001") -> str:
    quantized = value.quantize(Decimal(places))
    return format(quantized.normalize(), "f")


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

    @property
    def controls_path(self) -> Path:
        return self.execution_dir / "controls.json"

    @property
    def positions_path(self) -> Path:
        return self.execution_dir / "positions.json"

    @property
    def reconciliations_dir(self) -> Path:
        return self.execution_dir / "reconciliations"

    def order_path(self, execution_id: str) -> Path:
        return self.orders_dir / f"{execution_id}.json"

    def signature_path(self, signature_id: str) -> Path:
        return self.signatures_dir / f"{signature_id}.json"

    def state_path(self, intent_id: str) -> Path:
        return self.state_dir / f"{intent_id}.json"

    def reconciliation_path(self, snapshot_id: str) -> Path:
        return self.reconciliations_dir / f"{snapshot_id}.json"


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
            controls = self._load_controls()
            if controls["global_halt"]["active"]:
                raise ExecutionValidationError("global halt is active and blocks signer/executor release")
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

    def request_strategy_cancel_all(
        self,
        experiment_id: str,
        *,
        reason: str,
        requested_by: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        normalized_experiment_id = _require_text("experiment_id", experiment_id, max_length=160)
        request_reason = _require_text("reason", reason, max_length=2000)
        actor = _require_text("requested_by", requested_by, max_length=200)
        created_at = format_datetime(now or utc_now()) or ""
        try:
            self.risk.experiments.get_experiment(normalized_experiment_id)
        except ExperimentNotFoundError as exc:
            raise ExecutionNotFoundError(str(exc)) from exc
        cancelled = self._cancel_open_orders(
            scope="EXPERIMENT",
            scope_value=normalized_experiment_id,
            reason=request_reason,
            requested_by=actor,
            requested_at=created_at,
        )
        payload = {
            "action": "request_strategy_cancel_all",
            "cancelled_execution_ids": [item["execution_id"] for item in cancelled],
            "experiment_id": normalized_experiment_id,
            "reason": request_reason,
            "requested_at": created_at,
            "requested_by": actor,
            "scope": "EXPERIMENT",
        }
        self.store_audit_row(dict(payload))
        return payload

    def request_global_halt(
        self,
        *,
        reason: str,
        requested_by: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        request_reason = _require_text("reason", reason, max_length=2000)
        actor = _require_text("requested_by", requested_by, max_length=200)
        created_at = format_datetime(now or utc_now()) or ""
        request_id = self._build_request_id("global-halt", request_reason, actor, created_at)
        controls = self._load_controls()
        controls["global_halt"] = {
            "active": True,
            "reason": request_reason,
            "request_id": request_id,
            "requested_at": created_at,
            "requested_by": actor,
        }
        controls["updated_at"] = created_at
        write_json(self.store.controls_path, controls)
        cancelled = self._cancel_open_orders(
            scope="GLOBAL",
            scope_value=None,
            reason=request_reason,
            requested_by=actor,
            requested_at=created_at,
        )
        payload = {
            "action": "request_global_halt",
            "cancelled_execution_ids": [item["execution_id"] for item in cancelled],
            "reason": request_reason,
            "request_id": request_id,
            "requested_at": created_at,
            "requested_by": actor,
            "scope": "GLOBAL",
        }
        self.store_audit_row(dict(payload))
        return payload

    def get_live_controls(self) -> dict[str, Any]:
        return self._load_controls()

    def record_live_fill(
        self,
        execution_id: str,
        *,
        filled_quantity: Any,
        fill_price: Any,
        recorded_by: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        normalized_execution_id = _require_text("execution_id", execution_id, max_length=160)
        actor = _require_text("recorded_by", recorded_by, max_length=200)
        created_at = format_datetime(now or utc_now()) or ""
        record = self.get_execution_record(normalized_execution_id)
        if record["status"] == "CANCELLED":
            raise ExecutionValidationError(f"execution {normalized_execution_id} is cancelled and cannot accept fills")

        quantity_delta = _decimal_text(filled_quantity, field_name="filled_quantity")
        fill_price_decimal = _decimal_text(fill_price, field_name="fill_price")
        if quantity_delta <= 0:
            raise ExecutionValidationError("filled_quantity must be > 0")
        if fill_price_decimal <= 0:
            raise ExecutionValidationError("fill_price must be > 0")

        remaining_quantity = _decimal_text(record["remaining_quantity"], field_name="execution.remaining_quantity")
        if quantity_delta > remaining_quantity:
            raise ExecutionValidationError("filled_quantity exceeds remaining live quantity")

        fill_notional = quantity_delta * fill_price_decimal
        total_filled_quantity = _decimal_text(record["filled_quantity"], field_name="execution.filled_quantity") + quantity_delta
        total_filled_notional = _decimal_text(record["filled_notional_usd"], field_name="execution.filled_notional_usd") + fill_notional
        next_remaining = remaining_quantity - quantity_delta
        next_status = "FILLED" if next_remaining == 0 else "PARTIALLY_FILLED"
        fill_payload = {
            "fill_id": self._build_fill_id(normalized_execution_id, actor, created_at, quantity_delta, fill_price_decimal),
            "fill_price": _format_decimal(fill_price_decimal),
            "filled_quantity": _format_decimal(quantity_delta),
            "notional_usd": _format_decimal(fill_notional),
            "recorded_at": created_at,
            "recorded_by": actor,
        }
        record["fills"] = [*record.get("fills", []), fill_payload]
        record["filled_notional_usd"] = _format_decimal(total_filled_notional)
        record["filled_quantity"] = _format_decimal(total_filled_quantity)
        record["remaining_quantity"] = _format_decimal(next_remaining)
        record["status"] = next_status
        record["updated_at"] = created_at
        record["live_executor"]["filled_quantity"] = record["filled_quantity"]
        record["live_executor"]["last_fill_at"] = created_at
        record["live_executor"]["remaining_quantity"] = record["remaining_quantity"]
        record["live_executor"]["status"] = next_status
        write_json(self.store.order_path(normalized_execution_id), record)
        self._apply_fill_to_positions(record, quantity_delta, fill_notional, updated_at=created_at)
        self._sync_execution_state(record["intent_id"], record, updated_at=created_at)
        self.store_audit_row(
            {
                "action": "record_live_fill",
                "execution_id": normalized_execution_id,
                "fill_id": fill_payload["fill_id"],
                "recorded_at": created_at,
                "recorded_by": actor,
            }
        )
        return record

    def reconcile_live_state(
        self,
        *,
        venue_orders: list[dict[str, Any]],
        venue_positions: list[dict[str, Any]],
        reconciled_by: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        if not isinstance(venue_orders, list):
            raise ExecutionValidationError("venue_orders must be a JSON list")
        if not isinstance(venue_positions, list):
            raise ExecutionValidationError("venue_positions must be a JSON list")

        actor = _require_text("reconciled_by", reconciled_by, max_length=200)
        created_at = format_datetime(now or utc_now()) or ""
        local_records = self._list_execution_records()
        local_open_order_ids = sorted(
            record["live_executor"]["order_id"] for record in local_records if record["status"] in OPEN_EXECUTION_STATUSES
        )
        all_local_order_ids = {record["live_executor"]["order_id"] for record in local_records}
        venue_open_order_ids = sorted(self._normalize_venue_open_order_ids(venue_orders))
        unexpected_live_order_ids = sorted(order_id for order_id in venue_open_order_ids if order_id not in all_local_order_ids)
        missing_live_order_ids = sorted(order_id for order_id in local_open_order_ids if order_id not in venue_open_order_ids)
        local_positions = self._load_positions()
        venue_position_map = self._normalize_venue_positions(venue_positions)
        position_mismatches = self._position_mismatches(local_positions, venue_position_map)

        alerts: list[str] = []
        if unexpected_live_order_ids:
            alerts.append("unexpected_live_order")
        if missing_live_order_ids:
            alerts.append("open_order_missing_from_venue")
        if position_mismatches:
            alerts.append("position_reconciliation_mismatch")
        status = "MATCHED" if not alerts else "MISMATCH"
        snapshot_id = self._build_reconciliation_id(created_at, local_open_order_ids, venue_open_order_ids, position_mismatches)
        payload = {
            "alerts": alerts,
            "local_open_order_ids": local_open_order_ids,
            "missing_live_order_ids": missing_live_order_ids,
            "position_mismatches": position_mismatches,
            "reconciled_at": created_at,
            "reconciled_by": actor,
            "snapshot_id": snapshot_id,
            "status": status,
            "unexpected_live_order_ids": unexpected_live_order_ids,
            "venue_open_order_ids": venue_open_order_ids,
        }
        write_json(self.store.reconciliation_path(snapshot_id), payload)
        self.store_audit_row(
            {
                "action": "reconcile_live_state",
                "reconciled_at": created_at,
                "reconciled_by": actor,
                "snapshot_id": snapshot_id,
                "status": status,
            }
        )
        return payload

    def get_reconciliation_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        normalized_snapshot_id = _require_text("snapshot_id", snapshot_id, max_length=160)
        path = self.store.reconciliation_path(normalized_snapshot_id)
        if not path.exists():
            raise ExecutionNotFoundError(f"unknown reconciliation snapshot_id: {normalized_snapshot_id}")
        return read_json(path)

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

    def _load_controls(self) -> dict[str, Any]:
        path = self.store.controls_path
        if not path.exists():
            return {
                "global_halt": {
                    "active": False,
                    "reason": None,
                    "request_id": None,
                    "requested_at": None,
                    "requested_by": None,
                },
                "updated_at": None,
            }
        return read_json(path)

    def _list_execution_records(self) -> list[dict[str, Any]]:
        if not self.store.orders_dir.exists():
            return []
        payloads: list[dict[str, Any]] = []
        for path in sorted(self.store.orders_dir.glob("*.json")):
            payloads.append(read_json(path))
        return payloads

    def _load_positions(self) -> dict[tuple[str, str], dict[str, Any]]:
        path = self.store.positions_path
        if not path.exists():
            return {}
        payload = read_json(path)
        positions: dict[tuple[str, str], dict[str, Any]] = {}
        for item in payload.get("positions", []):
            positions[(str(item["market_id"]), str(item["outcome"]))] = item
        return positions

    def _write_positions(self, positions: dict[tuple[str, str], dict[str, Any]], *, updated_at: str) -> None:
        write_json(
            self.store.positions_path,
            {
                "positions": [positions[key] for key in sorted(positions)],
                "updated_at": updated_at,
            },
        )

    def _cancel_open_orders(
        self,
        *,
        scope: str,
        scope_value: Optional[str],
        reason: str,
        requested_by: str,
        requested_at: str,
    ) -> list[dict[str, Any]]:
        cancelled: list[dict[str, Any]] = []
        for record in self._list_execution_records():
            if record["status"] not in OPEN_EXECUTION_STATUSES:
                continue
            if scope == "EXPERIMENT" and record["experiment_id"] != scope_value:
                continue
            record["status"] = "CANCELLED"
            record["updated_at"] = requested_at
            record["cancel_request"] = {
                "reason": reason,
                "requested_at": requested_at,
                "requested_by": requested_by,
                "scope": scope,
            }
            record["live_executor"]["status"] = "CANCELLED"
            record["live_executor"]["cancelled_at"] = requested_at
            record["live_executor"]["cancelled_by"] = requested_by
            record["live_executor"]["cancel_reason"] = reason
            write_json(self.store.order_path(record["execution_id"]), record)
            self._sync_execution_state(record["intent_id"], record, updated_at=requested_at)
            cancelled.append(record)
        return cancelled

    def _sync_execution_state(self, intent_id: str, execution_record: dict[str, Any], *, updated_at: str) -> None:
        state = self._load_execution_state(intent_id)
        state.update(
            {
                "current_status": execution_record["status"],
                "latest_execution_id": execution_record["execution_id"],
                "updated_at": updated_at,
            }
        )
        write_json(self.store.state_path(intent_id), state)

    def _apply_fill_to_positions(
        self,
        record: dict[str, Any],
        quantity_delta: Decimal,
        fill_notional: Decimal,
        *,
        updated_at: str,
    ) -> None:
        positions = self._load_positions()
        key = (str(record["market_id"]), str(record["order_payload"]["outcome"]))
        existing = positions.get(
            key,
            {
                "market_id": record["market_id"],
                "net_notional_usd": "0",
                "net_quantity": "0",
                "outcome": record["order_payload"]["outcome"],
                "updated_at": updated_at,
            },
        )
        side_sign = Decimal("1") if str(record["order_payload"]["side"]).upper() == "BUY" else Decimal("-1")
        existing_quantity = _decimal_text(existing["net_quantity"], field_name="position.net_quantity")
        existing_notional = _decimal_text(existing["net_notional_usd"], field_name="position.net_notional_usd")
        existing["net_quantity"] = _format_decimal(existing_quantity + (quantity_delta * side_sign))
        existing["net_notional_usd"] = _format_decimal(existing_notional + (fill_notional * side_sign))
        existing["updated_at"] = updated_at
        positions[key] = existing
        self._write_positions(positions, updated_at=updated_at)

    def _normalize_venue_open_order_ids(self, venue_orders: list[dict[str, Any]]) -> set[str]:
        open_order_ids: set[str] = set()
        for item in venue_orders:
            if not isinstance(item, dict):
                raise ExecutionValidationError("venue_orders items must be JSON objects")
            order_id = _require_text("venue_orders[].order_id", item.get("order_id"), max_length=160)
            status = _require_text("venue_orders[].status", item.get("status", "SUBMITTED"), max_length=80).upper()
            if status not in TERMINAL_EXECUTION_STATUSES:
                open_order_ids.add(order_id)
        return open_order_ids

    def _normalize_venue_positions(self, venue_positions: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
        normalized: dict[tuple[str, str], dict[str, Any]] = {}
        for item in venue_positions:
            if not isinstance(item, dict):
                raise ExecutionValidationError("venue_positions items must be JSON objects")
            market_id = _require_text("venue_positions[].market_id", item.get("market_id"), max_length=160)
            outcome = _require_text("venue_positions[].outcome", item.get("outcome"), max_length=160)
            normalized[(market_id, outcome)] = {
                "market_id": market_id,
                "net_quantity": _format_decimal(
                    _decimal_text(item.get("net_quantity"), field_name="venue_positions[].net_quantity")
                ),
                "outcome": outcome,
            }
        return normalized

    def _position_mismatches(
        self,
        local_positions: dict[tuple[str, str], dict[str, Any]],
        venue_positions: dict[tuple[str, str], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        mismatches: list[dict[str, Any]] = []
        for key in sorted(set(local_positions) | set(venue_positions)):
            local = local_positions.get(key)
            venue = venue_positions.get(key)
            local_quantity = None if local is None else str(local["net_quantity"])
            venue_quantity = None if venue is None else str(venue["net_quantity"])
            if local_quantity != venue_quantity:
                mismatches.append(
                    {
                        "local_net_quantity": local_quantity,
                        "market_id": key[0],
                        "outcome": key[1],
                        "venue_net_quantity": venue_quantity,
                    }
                )
        return mismatches

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
            "fills": [],
            "filled_notional_usd": "0",
            "filled_quantity": "0",
            "intent_id": trade_intent["intent_id"],
            "live_executor": {
                "order_id": order_id,
                "remaining_quantity": unsigned_order["quantity"],
                "service": "live-executor",
                "status": "SUBMITTED",
                "submitted_at": created_at,
                "venue": "polymarket-clob",
            },
            "market_id": trade_intent["market_id"],
            "order_payload": unsigned_order,
            "policy_version": EXECUTION_POLICY_VERSION,
            "remaining_quantity": unsigned_order["quantity"],
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
            "updated_at": created_at,
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

    def _build_request_id(self, prefix: str, reason: str, requested_by: str, requested_at: str) -> str:
        basis = f"{prefix}:{reason}:{requested_by}:{requested_at}:{EXECUTION_POLICY_VERSION}"
        return f"{prefix}-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"

    def _build_fill_id(
        self,
        execution_id: str,
        recorded_by: str,
        recorded_at: str,
        quantity_delta: Decimal,
        fill_price: Decimal,
    ) -> str:
        basis = f"{execution_id}:{recorded_by}:{recorded_at}:{quantity_delta}:{fill_price}"
        return f"fill-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"

    def _build_reconciliation_id(
        self,
        reconciled_at: str,
        local_open_order_ids: list[str],
        venue_open_order_ids: list[str],
        position_mismatches: list[dict[str, Any]],
    ) -> str:
        basis = canonical_json(
            {
                "local_open_order_ids": local_open_order_ids,
                "position_mismatches": position_mismatches,
                "reconciled_at": reconciled_at,
                "venue_open_order_ids": venue_open_order_ids,
            }
        )
        return f"recon-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"


def build_execution_service(root: Path) -> ExecutionService:
    from .runtime import build_workspace

    return build_workspace(root).execution
