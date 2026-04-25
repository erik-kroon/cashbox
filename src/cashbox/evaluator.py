from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import hashlib
from pathlib import Path
from typing import Any, Optional

from .backtests import BacktestNotFoundError, BacktestService
from .experiments import ExperimentService, ExperimentServiceError
from .models import format_datetime, utc_now
from .persistence import canonical_json, read_json, write_json

EVALUATOR_POLICY_VERSION = 1
PROMOTION_TARGET_STAGES = ("paper", "tiny_live", "scaled_live")

def _require_text(name: str, value: Any, *, max_length: int = 2000) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise EvaluationValidationError(f"{name} must be non-empty")
    if len(normalized) > max_length:
        raise EvaluationValidationError(f"{name} exceeds max length {max_length}")
    return normalized


def _decimal_text(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _format_decimal(value: Decimal, *, places: str = "0.00000001") -> str:
    quantized = value.quantize(Decimal(places))
    return format(quantized.normalize(), "f")


def _normalize_target_stage(value: str) -> str:
    normalized = _require_text("target_stage", value, max_length=80).lower().replace("-", "_")
    if normalized not in PROMOTION_TARGET_STAGES:
        raise EvaluationValidationError(f"unsupported target_stage: {normalized}")
    return normalized


class EvaluationServiceError(Exception):
    pass


class EvaluationValidationError(EvaluationServiceError):
    pass


@dataclass
class FileSystemEvaluationStore:
    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def evaluator_dir(self) -> Path:
        return self.root / "evaluator"

    @property
    def scores_dir(self) -> Path:
        return self.evaluator_dir / "scores"

    @property
    def promotions_dir(self) -> Path:
        return self.evaluator_dir / "promotions"

    def score_path(self, score_id: str) -> Path:
        return self.scores_dir / f"{score_id}.json"

    def promotion_path(self, decision_id: str) -> Path:
        return self.promotions_dir / f"{decision_id}.json"


class EvaluatorService:
    def __init__(
        self,
        store: FileSystemEvaluationStore,
        *,
        experiments: ExperimentService,
        backtests: BacktestService,
    ) -> None:
        self.store = store
        self.experiments = experiments
        self.backtests = backtests

    def score_experiment(
        self,
        experiment_id: str,
        *,
        run_id: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        experiment = self.experiments.get_experiment(_require_text("experiment_id", experiment_id, max_length=120))
        run = self._resolve_successful_run(experiment["experiment_id"], run_id=run_id)
        artifact = run["artifact"]
        trades = artifact.get("trades", [])
        assumptions = artifact["assumptions"]
        train_trades = [trade for trade in trades if trade.get("split") == "train"]
        validation_trades = [trade for trade in trades if trade.get("split") == "validation"]
        test_trades = [trade for trade in trades if trade.get("split") == "test"]
        oos_trades = [trade for trade in trades if trade.get("split") in {"validation", "test"}]
        configured_markets = self._configured_market_ids(experiment)
        traded_markets = self._trade_market_ids(trades)
        oos_traded_markets = self._trade_market_ids(oos_trades)
        baseline_name = str(assumptions.get("baseline", ""))
        baseline_net_pnl = self._baseline_net_pnl(baseline_name)
        metrics = {
            "trade_count": len(trades),
            "oos_trade_count": len(oos_trades),
            "validation_trade_count": len(validation_trades),
            "test_trade_count": len(test_trades),
            "configured_market_count": len(configured_markets),
            "traded_market_count": len(traded_markets),
            "oos_traded_market_count": len(oos_traded_markets),
            "distinct_market_count": max(len(configured_markets), len(traded_markets)),
            "oos_distinct_market_count": max(len(configured_markets), len(oos_traded_markets)),
            "net_pnl_usd": _format_decimal(self._net_pnl(trades)),
            "oos_net_pnl_usd": _format_decimal(self._net_pnl(oos_trades)),
            "validation_net_pnl_usd": _format_decimal(self._net_pnl(validation_trades)),
            "test_net_pnl_usd": _format_decimal(self._net_pnl(test_trades)),
            "max_drawdown_usd": _format_decimal(self._max_drawdown(trades)),
            "oos_max_drawdown_usd": _format_decimal(self._max_drawdown(oos_trades)),
            "baseline_name": baseline_name,
            "baseline_net_pnl_usd": None if baseline_net_pnl is None else _format_decimal(baseline_net_pnl),
        }
        checks = {
            "config_schema_valid": experiment["config_schema_version"] >= 1,
            "fees_included": bool(assumptions.get("fee_model_version")) and _decimal_text(assumptions.get("fee_bps")) > 0,
            "slippage_included": bool(assumptions.get("slippage_model_version"))
            and _decimal_text(assumptions.get("slippage_bps")) > 0,
            "latency_model_included": bool(assumptions.get("latency_model_version")),
            "no_lookahead_leakage": assumptions.get("split_method") == "chronological",
            "overfitting_detected": self._detect_overfitting(
                train_pnl=self._net_pnl(train_trades),
                validation_pnl=self._net_pnl(validation_trades),
                test_pnl=self._net_pnl(test_trades),
            ),
            "unsupported_baseline": baseline_net_pnl is None,
        }
        reasons = self._score_reasons(
            metrics=metrics,
            checks=checks,
            assumptions=assumptions,
        )
        score_id = self._build_score_id(
            experiment_id=experiment["experiment_id"],
            run_id=run["run_id"],
            artifact_sha256=run["artifact_sha256"],
            config_sha256=experiment["config_sha256"],
        )
        path = self.store.score_path(score_id)
        if path.exists():
            return read_json(path)
        payload = {
            "score_id": score_id,
            "created_at": format_datetime(now or utc_now()) or "",
            "policy_version": EVALUATOR_POLICY_VERSION,
            "experiment_id": experiment["experiment_id"],
            "run_id": run["run_id"],
            "current_status": experiment["current_status"],
            "input_fingerprints": {
                "artifact_sha256": run["artifact_sha256"],
                "config_sha256": experiment["config_sha256"],
                "dataset_id": run["dataset_id"],
                "code_version": run["code_version"],
            },
            "metrics": metrics,
            "checks": checks,
            "reasons": reasons,
        }
        write_json(path, payload)
        return payload

    def check_promotion_eligibility(
        self,
        experiment_id: str,
        target_stage: str,
        *,
        run_id: Optional[str] = None,
        changed_by: str = "evaluator",
        promote: bool = False,
        min_out_of_sample_trades: int = 250,
        min_distinct_markets: int = 25,
        max_drawdown_limit_usd: Optional[Any] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        normalized_target = _normalize_target_stage(target_stage)
        if normalized_target != "paper":
            raise EvaluationValidationError(
                f"target_stage {normalized_target} is not implemented in evaluator policy v{EVALUATOR_POLICY_VERSION}"
            )

        actor = _require_text("changed_by", changed_by, max_length=200)
        if min_out_of_sample_trades < 1:
            raise EvaluationValidationError("min_out_of_sample_trades must be >= 1")
        if min_distinct_markets < 1:
            raise EvaluationValidationError("min_distinct_markets must be >= 1")

        experiment = self.experiments.get_experiment(experiment_id)
        score = self.score_experiment(experiment_id, run_id=run_id, now=now)
        metrics = score["metrics"]
        score_checks = score["checks"]
        observed_drawdown_limit = (
            _decimal_text(max_drawdown_limit_usd)
            if max_drawdown_limit_usd is not None
            else _decimal_text(experiment["config"].get("max_position_usd")) * Decimal("2")
        )
        gate_checks = {
            "config_schema_valid": {
                "passed": bool(score_checks["config_schema_valid"]),
                "observed": bool(score_checks["config_schema_valid"]),
                "required": True,
            },
            "min_out_of_sample_trades": {
                "passed": int(metrics["oos_trade_count"]) >= min_out_of_sample_trades,
                "observed": int(metrics["oos_trade_count"]),
                "required": min_out_of_sample_trades,
            },
            "min_distinct_markets": {
                "passed": int(metrics["oos_distinct_market_count"]) >= min_distinct_markets,
                "observed": int(metrics["oos_distinct_market_count"]),
                "required": min_distinct_markets,
            },
            "positive_oos_ev": {
                "passed": _decimal_text(metrics["oos_net_pnl_usd"]) > 0,
                "observed": metrics["oos_net_pnl_usd"],
                "required": "> 0",
            },
            "conservative_fees_included": {
                "passed": bool(score_checks["fees_included"]),
                "observed": bool(score_checks["fees_included"]),
                "required": True,
            },
            "conservative_slippage_included": {
                "passed": bool(score_checks["slippage_included"]),
                "observed": bool(score_checks["slippage_included"]),
                "required": True,
            },
            "latency_model_included": {
                "passed": bool(score_checks["latency_model_included"]),
                "observed": bool(score_checks["latency_model_included"]),
                "required": True,
            },
            "max_drawdown_within_policy": {
                "passed": _decimal_text(metrics["oos_max_drawdown_usd"]) <= observed_drawdown_limit,
                "observed": metrics["oos_max_drawdown_usd"],
                "required": _format_decimal(observed_drawdown_limit),
            },
            "beats_baseline": {
                "passed": not score_checks["unsupported_baseline"]
                and _decimal_text(metrics["oos_net_pnl_usd"]) > _decimal_text(metrics["baseline_net_pnl_usd"]),
                "observed": metrics["oos_net_pnl_usd"],
                "required": metrics["baseline_net_pnl_usd"],
            },
            "no_lookahead_leakage": {
                "passed": bool(score_checks["no_lookahead_leakage"]),
                "observed": bool(score_checks["no_lookahead_leakage"]),
                "required": True,
            },
            "no_overfitting_detected": {
                "passed": not bool(score_checks["overfitting_detected"]),
                "observed": bool(score_checks["overfitting_detected"]),
                "required": False,
            },
        }
        non_evaluator_passed = all(item["passed"] for item in gate_checks.values())
        gate_checks["evaluator_approved"] = {
            "passed": non_evaluator_passed,
            "observed": non_evaluator_passed,
            "required": True,
        }
        eligible = all(item["passed"] for item in gate_checks.values())
        failed_checks = [name for name, item in gate_checks.items() if not item["passed"]]
        promotion_applied = False
        resulting_status = experiment["current_status"]
        promotion_blockers: list[str] = []

        if promote and eligible:
            promotion = self.experiments.promote_to_paper_eligible(
                experiment["experiment_id"],
                changed_by=actor,
                reason=f"promotion_gate=paper score_id={score['score_id']}",
                now=now,
            )
            promotion_applied = bool(promotion["applied"])
            resulting_status = promotion["resulting_status"]
            promotion_blockers.extend(promotion["blockers"])

        decision_id = self._build_decision_id(
            score_id=score["score_id"],
            target_stage=normalized_target,
            promote=promote,
            current_status=experiment["current_status"],
            changed_by=actor,
            min_out_of_sample_trades=min_out_of_sample_trades,
            min_distinct_markets=min_distinct_markets,
            max_drawdown_limit_usd=_format_decimal(observed_drawdown_limit),
        )
        path = self.store.promotion_path(decision_id)
        if path.exists():
            return read_json(path)
        payload = {
            "decision_id": decision_id,
            "created_at": format_datetime(now or utc_now()) or "",
            "policy_version": EVALUATOR_POLICY_VERSION,
            "experiment_id": experiment["experiment_id"],
            "run_id": score["run_id"],
            "score_id": score["score_id"],
            "target_stage": normalized_target,
            "eligible": eligible,
            "current_status": experiment["current_status"],
            "resulting_status": resulting_status,
            "promotion_applied": promotion_applied,
            "promotion_blockers": promotion_blockers,
            "thresholds": {
                "min_out_of_sample_trades": min_out_of_sample_trades,
                "min_distinct_markets": min_distinct_markets,
                "max_drawdown_limit_usd": _format_decimal(observed_drawdown_limit),
            },
            "gate_checks": gate_checks,
            "failed_checks": failed_checks,
            "notes": score["reasons"],
        }
        write_json(path, payload)
        return payload

    def _resolve_successful_run(self, experiment_id: str, *, run_id: Optional[str]) -> dict[str, Any]:
        if run_id is not None:
            return self._load_successful_run(run_id, experiment_id=experiment_id)

        runs_dir = self.backtests.store.runs_dir
        candidates: list[dict[str, Any]] = []
        for path in sorted(runs_dir.glob("*.json")):
            payload = read_json(path)
            if payload.get("experiment_id") != experiment_id or payload.get("status") != "SUCCEEDED":
                continue
            candidates.append(payload)
        if not candidates:
            raise EvaluationValidationError(f"no successful backtest run found for experiment_id: {experiment_id}")
        candidates.sort(key=lambda item: (str(item.get("created_at", "")), str(item.get("run_id", ""))), reverse=True)
        return self._load_successful_run(str(candidates[0]["run_id"]), experiment_id=experiment_id)

    def _load_successful_run(self, run_id: str, *, experiment_id: str) -> dict[str, Any]:
        normalized_run_id = _require_text("run_id", run_id, max_length=160)
        run_path = self.backtests.store.run_path(normalized_run_id)
        if not run_path.exists():
            raise BacktestNotFoundError(f"unknown run_id: {normalized_run_id}")
        payload = read_json(run_path)
        if payload.get("experiment_id") != experiment_id:
            raise EvaluationValidationError(
                f"run_id {normalized_run_id} does not belong to experiment_id {experiment_id}"
            )
        if payload.get("status") != "SUCCEEDED":
            raise EvaluationValidationError(f"run_id {normalized_run_id} must succeed before evaluation")
        payload["artifact"] = self.backtests.get_backtest_artifacts(normalized_run_id)
        return payload

    def _configured_market_ids(self, experiment: dict[str, Any]) -> set[str]:
        config = experiment["config"]
        if experiment["strategy_family"] == "cross_market_arbitrage":
            return {str(item).strip() for item in config.get("market_ids", []) if str(item).strip()}
        market_id = str(config.get("market_id", "")).strip()
        return {market_id} if market_id else set()

    def _trade_market_ids(self, trades: list[dict[str, Any]]) -> set[str]:
        market_ids: set[str] = set()
        for trade in trades:
            raw_market_id = str(trade.get("market_id", "")).strip()
            if not raw_market_id:
                continue
            for market_id in raw_market_id.split("|"):
                normalized = market_id.strip()
                if normalized:
                    market_ids.add(normalized)
        return market_ids

    def _baseline_net_pnl(self, baseline_name: str) -> Optional[Decimal]:
        if baseline_name == "hold":
            return Decimal("0")
        return None

    def _net_pnl(self, trades: list[dict[str, Any]]) -> Decimal:
        return sum((_decimal_text(item.get("net_pnl_usd")) for item in trades), Decimal("0"))

    def _max_drawdown(self, trades: list[dict[str, Any]]) -> Decimal:
        equity = Decimal("0")
        peak = Decimal("0")
        max_drawdown = Decimal("0")
        for trade in trades:
            equity += _decimal_text(trade.get("net_pnl_usd"))
            if equity > peak:
                peak = equity
            drawdown = peak - equity
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        return max_drawdown

    def _detect_overfitting(self, *, train_pnl: Decimal, validation_pnl: Decimal, test_pnl: Decimal) -> bool:
        if train_pnl <= 0:
            return False
        return validation_pnl <= 0 or test_pnl <= 0

    def _score_reasons(
        self,
        *,
        metrics: dict[str, Any],
        checks: dict[str, Any],
        assumptions: dict[str, Any],
    ) -> list[str]:
        reasons: list[str] = []
        if int(metrics["oos_trade_count"]) == 0:
            reasons.append("no out-of-sample trades were produced by the backtest")
        if checks["overfitting_detected"]:
            reasons.append("train performance is positive while validation or test performance is non-positive")
        if checks["unsupported_baseline"]:
            reasons.append(f"baseline {assumptions.get('baseline')} is not supported by evaluator policy v1")
        if not checks["fees_included"]:
            reasons.append("fee assumptions are missing or zero")
        if not checks["slippage_included"]:
            reasons.append("slippage assumptions are missing or zero")
        if not checks["latency_model_included"]:
            reasons.append("latency model version is missing")
        return reasons

    def _build_score_id(
        self,
        *,
        experiment_id: str,
        run_id: str,
        artifact_sha256: str,
        config_sha256: str,
    ) -> str:
        basis = f"{experiment_id}:{run_id}:{artifact_sha256}:{config_sha256}:{EVALUATOR_POLICY_VERSION}"
        return f"score-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"

    def _build_decision_id(
        self,
        *,
        score_id: str,
        target_stage: str,
        promote: bool,
        current_status: str,
        changed_by: str,
        min_out_of_sample_trades: int,
        min_distinct_markets: int,
        max_drawdown_limit_usd: str,
    ) -> str:
        basis = (
            f"{score_id}:{target_stage}:{promote}:{current_status}:{changed_by}:"
            f"{min_out_of_sample_trades}:{min_distinct_markets}:{max_drawdown_limit_usd}:{EVALUATOR_POLICY_VERSION}"
        )
        return f"decision-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"


EvaluationService = EvaluatorService


def build_evaluator_service(root: Path) -> EvaluatorService:
    from .runtime import build_workspace

    return build_workspace(root).evaluator
