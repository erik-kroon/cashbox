from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
from pathlib import Path
from typing import Any, Optional

from .experiments import ExperimentService, LIVE_TRADING_PERMITTED_EXPERIMENT_STATUSES
from .ingest import FileSystemMarketStore
from .models import format_datetime, parse_datetime, utc_now
from .persistence import append_jsonl, canonical_copy, canonical_json, read_json, read_jsonl, write_json
from .research import ResearchMarketReader

RISK_POLICY_VERSION = 1
RISK_DECISION_OUTCOMES = ("ALLOW", "REJECT", "PENDING_HUMAN_APPROVAL")
HUMAN_REVIEW_DECISIONS = ("APPROVE", "REJECT")
LIVE_TRADING_EXPERIMENT_STATUSES = LIVE_TRADING_PERMITTED_EXPERIMENT_STATUSES
TERMINAL_INTENT_STATUSES = ("ALLOWED", "REJECTED")

DEFAULT_RISK_POLICY: dict[str, Any] = {
    "allowed_categories": [],
    "allowed_order_classes": ["TAKER_FOK", "TAKER_IOC", "POST_ONLY_LIMIT_GTC", "POST_ONLY_LIMIT_GTD", "CANCEL_ONLY"],
    "allowed_time_in_force": ["FOK", "IOC", "GTC", "GTD"],
    "drawdown_limit_usd": "100",
    "event_exposure_limit_usd": "25",
    "event_relation_constraints_valid": True,
    "exchange_healthy": True,
    "external_model_fresh": True,
    "global_halt": False,
    "current_event_exposure_usd": "0",
    "current_market_exposure_usd": "0",
    "current_portfolio_exposure_usd": "0",
    "daily_loss_limit_usd": "50",
    "max_book_age_seconds": 120,
    "market_exposure_limit_usd": "25",
    "max_fee_bps": "25",
    "max_notional_usd": "25",
    "max_slippage_bps": "40",
    "min_time_to_resolution_seconds": 3600,
    "open_order_count": 0,
    "open_order_limit": 4,
    "portfolio_exposure_limit_usd": "50",
    "projected_daily_loss_usd": "0",
    "projected_drawdown_usd": "0",
    "quantity_precision_dp": 4,
    "require_human_approval": True,
    "signer_healthy": True,
    "strategy_halt": False,
    "tick_size": "0.01",
}

def _require_text(name: str, value: Any, *, max_length: int = 2000) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise RiskValidationError(f"{name} must be non-empty")
    if len(normalized) > max_length:
        raise RiskValidationError(f"{name} exceeds max length {max_length}")
    return normalized


def _decimal_text(value: Any, *, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise RiskValidationError(f"{field_name} must be numeric") from exc


def _format_decimal(value: Decimal, *, places: str = "0.00000001") -> str:
    quantized = value.quantize(Decimal(places))
    return format(quantized.normalize(), "f")


def _normalize_upper(name: str, value: Any, *, choices: tuple[str, ...]) -> str:
    normalized = _require_text(name, value, max_length=80).upper().replace("-", "_")
    if normalized not in choices:
        raise RiskValidationError(f"{name} must be one of: {', '.join(choices)}")
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
    raise RiskValidationError("boolean policy fields must be bool-compatible")


def _normalize_int(name: str, value: Any, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise RiskValidationError(f"{name} must be an integer")
    if value < minimum:
        raise RiskValidationError(f"{name} must be >= {minimum}")
    return value


def _is_tick_aligned(value: Decimal, tick_size: Decimal) -> bool:
    if tick_size <= 0:
        return False
    remainder = value % tick_size
    return remainder == 0


def _is_precision_aligned(value: Decimal, precision_dp: int) -> bool:
    if precision_dp < 0:
        return False
    return value == value.quantize(Decimal(1).scaleb(-precision_dp))


class RiskServiceError(Exception):
    pass


class RiskNotFoundError(RiskServiceError):
    pass


class RiskValidationError(RiskServiceError):
    pass


@dataclass
class FileSystemRiskStore:
    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def risk_dir(self) -> Path:
        return self.root / "risk"

    @property
    def intents_dir(self) -> Path:
        return self.risk_dir / "trade-intents"

    @property
    def decisions_dir(self) -> Path:
        return self.risk_dir / "decisions"

    @property
    def reviews_dir(self) -> Path:
        return self.risk_dir / "reviews"

    @property
    def state_dir(self) -> Path:
        return self.risk_dir / "state"

    def intent_path(self, intent_id: str) -> Path:
        return self.intents_dir / f"{intent_id}.json"

    def decision_path(self, decision_id: str) -> Path:
        return self.decisions_dir / f"{decision_id}.json"

    def review_path(self, intent_id: str) -> Path:
        return self.reviews_dir / f"{intent_id}.jsonl"

    def state_path(self, intent_id: str) -> Path:
        return self.state_dir / f"{intent_id}.json"


class RiskGatewayService:
    def __init__(
        self,
        store: FileSystemRiskStore,
        *,
        experiments: ExperimentService,
        market_store: FileSystemMarketStore,
        read_path: ResearchMarketReader,
    ) -> None:
        self.store = store
        self.experiments = experiments
        self.market_store = market_store
        self.read_path = read_path

    def create_trade_intent(
        self,
        experiment_id: str,
        order_request: dict[str, Any],
        *,
        submitted_by: str,
        rationale: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        experiment = self.experiments.get_experiment(_require_text("experiment_id", experiment_id, max_length=120))
        if not isinstance(order_request, dict):
            raise RiskValidationError("order_request must be a JSON object")

        created_at = format_datetime(now or utc_now()) or ""
        actor = _require_text("submitted_by", submitted_by, max_length=200)
        normalized_order = self._normalize_order_request(order_request, created_at=created_at)
        intent_id = self._build_intent_id(
            experiment_id=experiment["experiment_id"],
            order_request=normalized_order,
            created_at=created_at,
            submitted_by=actor,
        )
        path = self.store.intent_path(intent_id)
        if path.exists():
            return self.get_trade_intent(intent_id)

        payload = {
            "intent_id": intent_id,
            "created_at": created_at,
            "experiment_id": experiment["experiment_id"],
            "strategy_family": experiment["strategy_family"],
            "experiment_status": experiment["current_status"],
            "submitted_by": actor,
            "rationale": None if rationale is None else _require_text("rationale", rationale, max_length=4000),
            "order_request": normalized_order,
            "market_id": normalized_order["market_id"],
            "event_id": None,
            "category": None,
            "notional_usd": normalized_order["notional_usd"],
            "policy_version": RISK_POLICY_VERSION,
        }
        state = {
            "intent_id": intent_id,
            "experiment_id": experiment["experiment_id"],
            "created_at": created_at,
            "current_status": "CREATED",
            "latest_decision_id": None,
            "latest_decision_outcome": None,
            "approval_token": None,
            "human_review_status": None,
            "human_review_reason": None,
            "updated_at": created_at,
        }
        write_json(path, payload)
        write_json(self.store.state_path(intent_id), state)
        return self.get_trade_intent(intent_id)

    def review_trade_intent(
        self,
        intent_id: str,
        *,
        reviewer: str,
        decision: str,
        reason: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        intent = self._load_intent(intent_id)
        state = self._load_state(intent["intent_id"])
        if state["current_status"] in TERMINAL_INTENT_STATUSES:
            raise RiskValidationError(f"trade intent {intent['intent_id']} is already terminal: {state['current_status']}")

        created_at = format_datetime(now or utc_now()) or ""
        normalized_decision = _normalize_upper("decision", decision, choices=HUMAN_REVIEW_DECISIONS)
        review_payload = {
            "review_id": self._build_review_id(
                intent_id=intent["intent_id"],
                reviewer=_require_text("reviewer", reviewer, max_length=200),
                decision=normalized_decision,
                created_at=created_at,
                reason=_require_text("reason", reason, max_length=2000),
            ),
            "intent_id": intent["intent_id"],
            "reviewer": _require_text("reviewer", reviewer, max_length=200),
            "decision": normalized_decision,
            "reason": _require_text("reason", reason, max_length=2000),
            "created_at": created_at,
        }
        append_jsonl(self.store.review_path(intent["intent_id"]), review_payload)
        state["human_review_status"] = normalized_decision
        state["human_review_reason"] = review_payload["reason"]
        state["updated_at"] = created_at
        write_json(self.store.state_path(intent["intent_id"]), state)
        return review_payload

    def evaluate_trade_intent(
        self,
        intent_id: str,
        *,
        policy: Optional[dict[str, Any]] = None,
        decided_by: str = "risk-gateway",
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        intent = self._load_intent(intent_id)
        experiment = self.experiments.get_experiment(intent["experiment_id"])
        progression = self.experiments.get_progression_state(experiment["experiment_id"])
        state = self._load_state(intent["intent_id"])
        reviews = self._load_reviews(intent["intent_id"])
        latest_review = reviews[-1] if reviews else None
        current_time = now or utc_now()
        created_at = format_datetime(current_time) or ""
        merged_policy = self._normalize_policy(policy)
        policy_sha256 = hashlib.sha256(canonical_json(merged_policy).encode("utf-8")).hexdigest()
        market_metadata = self._load_market_metadata(intent["market_id"])
        order_request = intent["order_request"]
        order_notional = _decimal_text(order_request["notional_usd"], field_name="order_request.notional_usd")
        market_exposure = _decimal_text(merged_policy["current_market_exposure_usd"], field_name="policy.current_market_exposure_usd")
        event_exposure = _decimal_text(merged_policy["current_event_exposure_usd"], field_name="policy.current_event_exposure_usd")
        portfolio_exposure = _decimal_text(
            merged_policy["current_portfolio_exposure_usd"], field_name="policy.current_portfolio_exposure_usd"
        )
        time_to_resolution_seconds = self._time_to_resolution_seconds(market_metadata, current_time)
        book_age_seconds = self._book_age_seconds(market_metadata, current_time)
        tick_size = _decimal_text(merged_policy["tick_size"], field_name="policy.tick_size")
        quantity_precision_dp = int(merged_policy["quantity_precision_dp"])
        fee_bps = _decimal_text(order_request["estimated_fee_bps"], field_name="order_request.estimated_fee_bps")
        slippage_bps = _decimal_text(
            order_request["estimated_slippage_bps"], field_name="order_request.estimated_slippage_bps"
        )
        daily_loss = _decimal_text(merged_policy["projected_daily_loss_usd"], field_name="policy.projected_daily_loss_usd")
        drawdown = _decimal_text(merged_policy["projected_drawdown_usd"], field_name="policy.projected_drawdown_usd")
        price = _decimal_text(order_request["price"], field_name="order_request.price")
        quantity = _decimal_text(order_request["quantity"], field_name="order_request.quantity")

        hard_checks = {
            "strategy_enabled": self._check(
                passed=not bool(progression["is_terminal"]),
                observed=experiment["current_status"],
                required="not terminal",
            ),
            "strategy_stage_permits_live": self._check(
                passed=bool(progression["permits_live_trading"]),
                observed=experiment["current_status"],
                required=True,
            ),
            "strategy_has_approved_capital_limits": self._check(
                passed=_decimal_text(merged_policy["max_notional_usd"], field_name="policy.max_notional_usd") > 0,
                observed=merged_policy["max_notional_usd"],
                required="> 0",
            ),
            "market_allowed": self._check(
                passed=market_metadata is not None
                and bool(market_metadata["active"])
                and not bool(market_metadata["closed"])
                and not bool(market_metadata["archived"]),
                observed=None
                if market_metadata is None
                else {
                    "active": market_metadata["active"],
                    "closed": market_metadata["closed"],
                    "archived": market_metadata["archived"],
                },
                required=True,
            ),
            "category_allowed": self._check(
                passed=market_metadata is not None
                and (
                    not merged_policy["allowed_categories"]
                    or str(market_metadata["category"]).lower() in merged_policy["allowed_categories"]
                ),
                observed=None if market_metadata is None else market_metadata["category"],
                required=merged_policy["allowed_categories"] or "any",
            ),
            "event_relation_constraints_valid": self._check(
                passed=bool(merged_policy["event_relation_constraints_valid"]),
                observed=bool(merged_policy["event_relation_constraints_valid"]),
                required=True,
            ),
            "time_to_resolution_within_policy": self._check(
                passed=time_to_resolution_seconds is not None
                and time_to_resolution_seconds >= int(merged_policy["min_time_to_resolution_seconds"]),
                observed=time_to_resolution_seconds,
                required=f">= {merged_policy['min_time_to_resolution_seconds']}",
            ),
            "order_book_fresh": self._check(
                passed=book_age_seconds is not None and book_age_seconds <= int(merged_policy["max_book_age_seconds"]),
                observed=book_age_seconds,
                required=f"<= {merged_policy['max_book_age_seconds']}",
            ),
            "external_model_fresh": self._check(
                passed=bool(merged_policy["external_model_fresh"]),
                observed=bool(merged_policy["external_model_fresh"]),
                required=True,
            ),
            "price_tick_aligned": self._check(
                passed=_is_tick_aligned(price, tick_size),
                observed=order_request["price"],
                required=f"tick_size={merged_policy['tick_size']}",
            ),
            "quantity_precision_aligned": self._check(
                passed=_is_precision_aligned(quantity, quantity_precision_dp),
                observed=order_request["quantity"],
                required=f"precision_dp={quantity_precision_dp}",
            ),
            "order_class_allowed": self._check(
                passed=order_request["order_class"] in merged_policy["allowed_order_classes"],
                observed=order_request["order_class"],
                required=merged_policy["allowed_order_classes"],
            ),
            "time_in_force_allowed": self._check(
                passed=order_request["time_in_force"] in merged_policy["allowed_time_in_force"],
                observed=order_request["time_in_force"],
                required=merged_policy["allowed_time_in_force"],
            ),
            "estimated_fee_within_assumptions": self._check(
                passed=fee_bps <= _decimal_text(merged_policy["max_fee_bps"], field_name="policy.max_fee_bps"),
                observed=order_request["estimated_fee_bps"],
                required=f"<= {merged_policy['max_fee_bps']}",
            ),
            "estimated_slippage_within_assumptions": self._check(
                passed=slippage_bps <= _decimal_text(merged_policy["max_slippage_bps"], field_name="policy.max_slippage_bps"),
                observed=order_request["estimated_slippage_bps"],
                required=f"<= {merged_policy['max_slippage_bps']}",
            ),
            "market_exposure_cap_not_exceeded": self._check(
                passed=market_exposure + order_notional
                <= _decimal_text(merged_policy["market_exposure_limit_usd"], field_name="policy.market_exposure_limit_usd"),
                observed=_format_decimal(market_exposure + order_notional),
                required=f"<= {merged_policy['market_exposure_limit_usd']}",
            ),
            "event_exposure_cap_not_exceeded": self._check(
                passed=event_exposure + order_notional
                <= _decimal_text(merged_policy["event_exposure_limit_usd"], field_name="policy.event_exposure_limit_usd"),
                observed=_format_decimal(event_exposure + order_notional),
                required=f"<= {merged_policy['event_exposure_limit_usd']}",
            ),
            "portfolio_exposure_cap_not_exceeded": self._check(
                passed=portfolio_exposure + order_notional
                <= _decimal_text(
                    merged_policy["portfolio_exposure_limit_usd"], field_name="policy.portfolio_exposure_limit_usd"
                ),
                observed=_format_decimal(portfolio_exposure + order_notional),
                required=f"<= {merged_policy['portfolio_exposure_limit_usd']}",
            ),
            "daily_loss_cap_not_exceeded": self._check(
                passed=daily_loss <= _decimal_text(merged_policy["daily_loss_limit_usd"], field_name="policy.daily_loss_limit_usd"),
                observed=merged_policy["projected_daily_loss_usd"],
                required=f"<= {merged_policy['daily_loss_limit_usd']}",
            ),
            "drawdown_cap_not_exceeded": self._check(
                passed=drawdown <= _decimal_text(merged_policy["drawdown_limit_usd"], field_name="policy.drawdown_limit_usd"),
                observed=merged_policy["projected_drawdown_usd"],
                required=f"<= {merged_policy['drawdown_limit_usd']}",
            ),
            "open_order_cap_not_exceeded": self._check(
                passed=int(merged_policy["open_order_count"]) + 1 <= int(merged_policy["open_order_limit"]),
                observed=int(merged_policy["open_order_count"]) + 1,
                required=f"<= {merged_policy['open_order_limit']}",
            ),
            "signer_service_healthy": self._check(
                passed=bool(merged_policy["signer_healthy"]),
                observed=bool(merged_policy["signer_healthy"]),
                required=True,
            ),
            "exchange_health_acceptable": self._check(
                passed=bool(merged_policy["exchange_healthy"]),
                observed=bool(merged_policy["exchange_healthy"]),
                required=True,
            ),
            "no_global_halt": self._check(
                passed=not bool(merged_policy["global_halt"]),
                observed=bool(merged_policy["global_halt"]),
                required=False,
            ),
            "no_strategy_halt": self._check(
                passed=not bool(merged_policy["strategy_halt"]),
                observed=bool(merged_policy["strategy_halt"]),
                required=False,
            ),
        }
        human_check = self._human_approval_check(latest_review=latest_review, policy=merged_policy)
        checks = dict(hard_checks)
        checks["human_approval"] = human_check
        hard_failed_checks = [name for name, item in hard_checks.items() if not item["passed"]]
        all_failed_checks = [name for name, item in checks.items() if not item["passed"]]

        if hard_failed_checks:
            outcome = "REJECT"
            approval_token = None
            current_status = "REJECTED"
        elif not human_check["passed"] and human_check["observed"] == "PENDING":
            outcome = "PENDING_HUMAN_APPROVAL"
            approval_token = None
            current_status = "PENDING_HUMAN_APPROVAL"
        elif not human_check["passed"]:
            outcome = "REJECT"
            approval_token = None
            current_status = "REJECTED"
        else:
            outcome = "ALLOW"
            approval_token = self._build_approval_token(intent["intent_id"], policy_sha256, latest_review)
            current_status = "ALLOWED"

        decision_id = self._build_decision_id(
            intent_id=intent["intent_id"],
            policy_sha256=policy_sha256,
            human_review_id=None if latest_review is None else latest_review["review_id"],
            outcome=outcome,
        )
        path = self.store.decision_path(decision_id)
        if path.exists():
            payload = read_json(path)
        else:
            payload = {
                "decision_id": decision_id,
                "created_at": created_at,
                "policy_version": RISK_POLICY_VERSION,
                "intent_id": intent["intent_id"],
                "experiment_id": intent["experiment_id"],
                "market_id": intent["market_id"],
                "outcome": outcome,
                "approval_token": approval_token,
                "decided_by": _require_text("decided_by", decided_by, max_length=200),
                "policy_sha256": policy_sha256,
                "policy_snapshot": merged_policy,
                "checks": checks,
                "failed_checks": all_failed_checks,
                "notes": self._decision_notes(checks, outcome),
                "human_review": latest_review,
                "intent_summary": {
                    "order_class": order_request["order_class"],
                    "time_in_force": order_request["time_in_force"],
                    "price": order_request["price"],
                    "quantity": order_request["quantity"],
                    "notional_usd": order_request["notional_usd"],
                },
            }
            write_json(path, payload)

        state.update(
            {
                "current_status": current_status,
                "latest_decision_id": payload["decision_id"],
                "latest_decision_outcome": payload["outcome"],
                "approval_token": payload["approval_token"],
                "human_review_status": None if latest_review is None else latest_review["decision"],
                "human_review_reason": None if latest_review is None else latest_review["reason"],
                "updated_at": created_at,
            }
        )
        write_json(self.store.state_path(intent["intent_id"]), state)
        return payload

    def get_trade_intent(self, intent_id: str) -> dict[str, Any]:
        intent = self._load_intent(intent_id)
        payload = dict(intent)
        payload["state"] = self._load_state(intent["intent_id"])
        payload["reviews"] = self._load_reviews(intent["intent_id"])
        if payload["state"]["latest_decision_id"] is not None:
            payload["latest_decision"] = self.get_risk_decision(payload["state"]["latest_decision_id"])
        else:
            payload["latest_decision"] = None
        return payload

    def get_risk_decision(self, decision_id: str) -> dict[str, Any]:
        normalized_decision_id = _require_text("decision_id", decision_id, max_length=160)
        path = self.store.decision_path(normalized_decision_id)
        if not path.exists():
            raise RiskNotFoundError(f"unknown decision_id: {normalized_decision_id}")
        return read_json(path)

    def _load_intent(self, intent_id: str) -> dict[str, Any]:
        normalized_intent_id = _require_text("intent_id", intent_id, max_length=160)
        path = self.store.intent_path(normalized_intent_id)
        if not path.exists():
            raise RiskNotFoundError(f"unknown intent_id: {normalized_intent_id}")
        return read_json(path)

    def _load_reviews(self, intent_id: str) -> list[dict[str, Any]]:
        return read_jsonl(self.store.review_path(intent_id))

    def _load_state(self, intent_id: str) -> dict[str, Any]:
        path = self.store.state_path(intent_id)
        if not path.exists():
            raise RiskNotFoundError(f"missing state for intent_id: {intent_id}")
        return read_json(path)

    def _normalize_order_request(self, payload: dict[str, Any], *, created_at: str) -> dict[str, Any]:
        market_id = _require_text("order_request.market_id", payload.get("market_id"), max_length=120)
        outcome = _require_text("order_request.outcome", payload.get("outcome"), max_length=120)
        side = _normalize_upper("order_request.side", payload.get("side"), choices=("BUY", "SELL"))
        order_class = _normalize_upper(
            "order_request.order_class",
            payload.get("order_class"),
            choices=("TAKER_FOK", "TAKER_IOC", "POST_ONLY_LIMIT_GTC", "POST_ONLY_LIMIT_GTD", "CANCEL_ONLY"),
        )
        time_in_force = _normalize_upper(
            "order_request.time_in_force",
            payload.get("time_in_force"),
            choices=("FOK", "IOC", "GTC", "GTD"),
        )
        price = _decimal_text(payload.get("price"), field_name="order_request.price")
        quantity = _decimal_text(payload.get("quantity"), field_name="order_request.quantity")
        if price <= 0:
            raise RiskValidationError("order_request.price must be > 0")
        if quantity <= 0:
            raise RiskValidationError("order_request.quantity must be > 0")

        estimated_fee_bps = _decimal_text(payload.get("estimated_fee_bps", "0"), field_name="order_request.estimated_fee_bps")
        estimated_slippage_bps = _decimal_text(
            payload.get("estimated_slippage_bps", "0"), field_name="order_request.estimated_slippage_bps"
        )
        if estimated_fee_bps < 0 or estimated_slippage_bps < 0:
            raise RiskValidationError("estimated fee and slippage must be >= 0")

        requested_at = payload.get("requested_at")
        parsed_requested_at = parse_datetime(requested_at) if requested_at is not None else None
        return {
            "market_id": market_id,
            "outcome": outcome,
            "side": side,
            "order_class": order_class,
            "time_in_force": time_in_force,
            "price": _format_decimal(price),
            "quantity": _format_decimal(quantity),
            "notional_usd": _format_decimal(price * quantity),
            "estimated_fee_bps": _format_decimal(estimated_fee_bps),
            "estimated_slippage_bps": _format_decimal(estimated_slippage_bps),
            "requested_at": format_datetime(parsed_requested_at) or created_at,
        }

    def _normalize_policy(self, policy: Optional[dict[str, Any]]) -> dict[str, Any]:
        if policy is None:
            merged = canonical_copy(DEFAULT_RISK_POLICY)
        else:
            if not isinstance(policy, dict):
                raise RiskValidationError("policy must be a JSON object")
            merged = canonical_copy(DEFAULT_RISK_POLICY)
            for key, value in policy.items():
                merged[key] = canonical_copy(value)

        merged["allowed_categories"] = [str(item).strip().lower() for item in merged["allowed_categories"] if str(item).strip()]
        merged["allowed_order_classes"] = [
            _normalize_upper("policy.allowed_order_classes", item, choices=DEFAULT_RISK_POLICY["allowed_order_classes"])
            for item in merged["allowed_order_classes"]
        ]
        merged["allowed_time_in_force"] = [
            _normalize_upper("policy.allowed_time_in_force", item, choices=("FOK", "IOC", "GTC", "GTD"))
            for item in merged["allowed_time_in_force"]
        ]
        merged["quantity_precision_dp"] = _normalize_int(
            "policy.quantity_precision_dp",
            merged["quantity_precision_dp"],
            minimum=0,
        )
        merged["max_book_age_seconds"] = _normalize_int(
            "policy.max_book_age_seconds",
            merged["max_book_age_seconds"],
            minimum=1,
        )
        merged["min_time_to_resolution_seconds"] = _normalize_int(
            "policy.min_time_to_resolution_seconds",
            merged["min_time_to_resolution_seconds"],
            minimum=0,
        )
        merged["open_order_count"] = _normalize_int("policy.open_order_count", merged["open_order_count"], minimum=0)
        merged["open_order_limit"] = _normalize_int("policy.open_order_limit", merged["open_order_limit"], minimum=1)
        for field_name in (
            "drawdown_limit_usd",
            "event_exposure_limit_usd",
            "current_event_exposure_usd",
            "current_market_exposure_usd",
            "current_portfolio_exposure_usd",
            "daily_loss_limit_usd",
            "market_exposure_limit_usd",
            "max_fee_bps",
            "max_notional_usd",
            "max_slippage_bps",
            "portfolio_exposure_limit_usd",
            "projected_daily_loss_usd",
            "projected_drawdown_usd",
            "tick_size",
        ):
            merged[field_name] = _format_decimal(_decimal_text(merged[field_name], field_name=f"policy.{field_name}"))
        for field_name in (
            "event_relation_constraints_valid",
            "exchange_healthy",
            "external_model_fresh",
            "global_halt",
            "require_human_approval",
            "signer_healthy",
            "strategy_halt",
        ):
            merged[field_name] = _normalize_bool(merged[field_name])
        return merged

    def _load_market_metadata(self, market_id: str) -> Optional[dict[str, Any]]:
        try:
            return self.read_path.get_market_metadata(market_id)
        except KeyError:
            return None

    def _time_to_resolution_seconds(self, market_metadata: Optional[dict[str, Any]], now: datetime) -> Optional[int]:
        if market_metadata is None:
            return None
        end_time = parse_datetime(market_metadata.get("end_time"))
        if end_time is None:
            return None
        return max(0, int((end_time - now).total_seconds()))

    def _book_age_seconds(self, market_metadata: Optional[dict[str, Any]], now: datetime) -> Optional[int]:
        if market_metadata is None:
            return None
        source_received_at = parse_datetime(market_metadata.get("source_received_at"))
        if source_received_at is None:
            return None
        return max(0, int((now - source_received_at).total_seconds()))

    def _human_approval_check(self, *, latest_review: Optional[dict[str, Any]], policy: dict[str, Any]) -> dict[str, Any]:
        if not policy["require_human_approval"]:
            return self._check(passed=True, observed="NOT_REQUIRED", required=False)
        if latest_review is None:
            return self._check(passed=False, observed="PENDING", required="APPROVE")
        if latest_review["decision"] == "APPROVE":
            return self._check(passed=True, observed="APPROVE", required="APPROVE")
        return self._check(passed=False, observed="REJECT", required="APPROVE")

    def _check(self, *, passed: bool, observed: Any, required: Any) -> dict[str, Any]:
        return {
            "passed": bool(passed),
            "observed": observed,
            "required": required,
        }

    def _decision_notes(self, checks: dict[str, dict[str, Any]], outcome: str) -> list[str]:
        notes: list[str] = []
        if outcome == "PENDING_HUMAN_APPROVAL":
            notes.append("trade intent requires explicit human approval before signer release")
        if not checks["strategy_stage_permits_live"]["passed"]:
            notes.append("strategy stage does not currently permit live trading")
        if not checks["price_tick_aligned"]["passed"]:
            notes.append("limit price is not aligned to the configured tick size")
        if not checks["quantity_precision_aligned"]["passed"]:
            notes.append("order quantity exceeds the configured decimal precision")
        if not checks["order_book_fresh"]["passed"]:
            notes.append("market data snapshot is stale for live execution")
        if not checks["human_approval"]["passed"] and checks["human_approval"]["observed"] == "REJECT":
            notes.append("human reviewer rejected the trade intent")
        if not checks["no_global_halt"]["passed"]:
            notes.append("global halt is active")
        return notes

    def _build_intent_id(
        self,
        *,
        experiment_id: str,
        order_request: dict[str, Any],
        created_at: str,
        submitted_by: str,
    ) -> str:
        basis = f"{experiment_id}:{created_at}:{submitted_by}:{canonical_json(order_request)}"
        return f"intent-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"

    def _build_review_id(
        self,
        *,
        intent_id: str,
        reviewer: str,
        decision: str,
        created_at: str,
        reason: str,
    ) -> str:
        basis = f"{intent_id}:{reviewer}:{decision}:{created_at}:{reason}"
        return f"review-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"

    def _build_decision_id(
        self,
        *,
        intent_id: str,
        policy_sha256: str,
        human_review_id: Optional[str],
        outcome: str,
    ) -> str:
        basis = f"{intent_id}:{policy_sha256}:{human_review_id}:{outcome}:{RISK_POLICY_VERSION}"
        return f"risk-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"

    def _build_approval_token(
        self,
        intent_id: str,
        policy_sha256: str,
        latest_review: Optional[dict[str, Any]],
    ) -> str:
        basis = f"{intent_id}:{policy_sha256}:{None if latest_review is None else latest_review['review_id']}"
        return f"approve-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:16]}"


RiskService = RiskGatewayService


def build_risk_gateway_service(root: Path) -> RiskGatewayService:
    from .runtime import build_workspace

    return build_workspace(root).risk
