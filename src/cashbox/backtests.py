from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, getcontext
import hashlib
from pathlib import Path
from typing import Any, Optional

from .experiments import ExperimentService
from .ingest import FileSystemMarketStore
from .models import format_datetime, utc_now
from .persistence import canonical_copy, canonical_json, read_json, write_json
from .strategy_replay import HistoryPoint, StrategyReplayService

getcontext().prec = 28

BACKTEST_RUN_STATUSES = ("SUCCEEDED", "FAILED")
BACKTEST_SIMULATION_LEVELS = ("price_replay", "top_of_book", "full_order_book")
BACKTEST_ENGINE_VERSION = 1


def _require_text(name: str, value: Any, *, max_length: int = 2000) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise BacktestValidationError(f"{name} must be non-empty", code="invalid_request")
    if len(normalized) > max_length:
        raise BacktestValidationError(f"{name} exceeds max length {max_length}", code="invalid_request")
    return normalized


def _decimal_from_value(name: str, value: Any, *, minimum: Optional[Decimal] = None) -> Decimal:
    try:
        decimal_value = Decimal(str(value))
    except Exception as exc:  # pragma: no cover - Decimal raises multiple exception types.
        raise BacktestValidationError(f"{name} must be numeric", code="invalid_assumptions") from exc
    if minimum is not None and decimal_value < minimum:
        raise BacktestValidationError(
            f"{name} must be >= {minimum}",
            code="invalid_assumptions",
            violations=[{"field": name, "issue": "below_minimum", "minimum": str(minimum)}],
        )
    return decimal_value


def _format_decimal(value: Decimal, *, places: str = "0.00000001") -> str:
    quantized = value.quantize(Decimal(places))
    return format(quantized.normalize(), "f")


def _replay_validation_error(
    message: str,
    *,
    code: str = "invalid_backtest",
    violations: Optional[list[dict[str, Any]]] = None,
) -> "BacktestValidationError":
    return BacktestValidationError(message, code=code, violations=violations)


@dataclass
class FileSystemBacktestStore:
    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def backtests_dir(self) -> Path:
        return self.root / "backtests"

    @property
    def runs_dir(self) -> Path:
        return self.backtests_dir / "runs"

    @property
    def artifacts_dir(self) -> Path:
        return self.backtests_dir / "artifacts"

    def run_path(self, run_id: str) -> Path:
        return self.runs_dir / f"{run_id}.json"

    def artifact_path(self, run_id: str) -> Path:
        return self.artifacts_dir / f"{run_id}.json"


class BacktestServiceError(Exception):
    pass


class BacktestNotFoundError(BacktestServiceError):
    pass


class BacktestValidationError(BacktestServiceError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "invalid_backtest",
        violations: Optional[list[dict[str, Any]]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.violations = violations or []


class BacktestService:
    def __init__(
        self,
        store: FileSystemBacktestStore,
        *,
        experiments: ExperimentService,
        market_store: FileSystemMarketStore,
    ) -> None:
        self.store = store
        self.experiments = experiments
        self.market_store = market_store
        self.replay = StrategyReplayService(market_store)

    def run_backtest(
        self,
        experiment_id: str,
        *,
        assumptions: dict[str, Any],
        dataset_id: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        experiment = self.experiments.get_experiment(_require_text("experiment_id", experiment_id, max_length=120))
        progression = self.experiments.get_progression_state(experiment["experiment_id"])
        if not progression["permits_backtest"]:
            raise BacktestValidationError(
                f"experiment is not backtest-ready; got {progression['current_status']}",
                code="invalid_experiment_status",
            )

        resolved_dataset_id = self._resolve_dataset_id(experiment, dataset_id)
        manifest = self.market_store.load_manifest(resolved_dataset_id)
        raw_assumptions = canonical_copy(assumptions)
        assumptions_sha256 = hashlib.sha256(canonical_json(raw_assumptions).encode("utf-8")).hexdigest()
        run_id = self._build_run_id(
            experiment_id=experiment["experiment_id"],
            dataset_id=manifest.dataset_id,
            config_sha256=experiment["config_sha256"],
            code_version=experiment["code_version"],
            assumptions_sha256=assumptions_sha256,
        )

        if self.store.run_path(run_id).exists() and self.store.artifact_path(run_id).exists():
            return self._load_run(run_id)

        created_at = format_datetime(now or utc_now()) or ""
        try:
            normalized_assumptions = self._normalize_assumptions(raw_assumptions)
            loaded_histories = self.replay.load_backtest_histories(
                experiment,
                manifest.dataset_id,
                validation_error=_replay_validation_error,
            )
            artifact = self._simulate(
                run_id=run_id,
                created_at=created_at,
                experiment=experiment,
                manifest=manifest.to_dict(),
                assumptions=normalized_assumptions,
                histories=loaded_histories.histories,
                timeline_points=loaded_histories.timeline_points,
                history_sha256=loaded_histories.history_sha256,
            )
            run_status = "SUCCEEDED"
            failure_code = None
            failure_message = None
            self._promote_experiment_to_backtested(experiment_id=experiment["experiment_id"], run_id=run_id, now=now)
        except BacktestValidationError as exc:
            normalized_assumptions = self._best_effort_assumptions(raw_assumptions)
            artifact = self._failed_artifact(
                run_id=run_id,
                created_at=created_at,
                experiment=experiment,
                manifest=manifest.to_dict(),
                assumptions=normalized_assumptions,
                failure_code=exc.code,
                failure_message=str(exc),
                violations=exc.violations,
            )
            run_status = "FAILED"
            failure_code = exc.code
            failure_message = str(exc)

        artifact_sha256 = hashlib.sha256(canonical_json(artifact).encode("utf-8")).hexdigest()
        run_payload = {
            "run_id": run_id,
            "experiment_id": experiment["experiment_id"],
            "dataset_id": manifest.dataset_id,
            "status": run_status,
            "created_at": created_at,
            "code_version": experiment["code_version"],
            "config_sha256": experiment["config_sha256"],
            "assumptions_sha256": assumptions_sha256,
            "artifact_sha256": artifact_sha256,
            "engine_version": BACKTEST_ENGINE_VERSION,
            "failure_code": failure_code,
            "failure_message": failure_message,
        }
        write_json(self.store.run_path(run_id), run_payload)
        write_json(self.store.artifact_path(run_id), artifact)
        return self._load_run(run_id)

    def get_backtest_artifacts(self, run_id: str) -> dict[str, Any]:
        _require_text("run_id", run_id, max_length=160)
        path = self.store.artifact_path(run_id)
        if not path.exists():
            raise BacktestNotFoundError(f"unknown run_id: {run_id}")
        return read_json(path)

    def explain_backtest_failure(self, run_id: str) -> dict[str, Any]:
        artifact = self.get_backtest_artifacts(run_id)
        if artifact["status"] != "FAILED":
            return {
                "run_id": run_id,
                "status": artifact["status"],
                "explanation": "backtest succeeded",
                "failure_code": None,
                "failure_message": None,
                "violations": [],
            }
        failure = artifact["failure"]
        return {
            "run_id": run_id,
            "status": artifact["status"],
            "explanation": failure["message"],
            "failure_code": failure["code"],
            "failure_message": failure["message"],
            "violations": failure["violations"],
        }

    def _best_effort_assumptions(self, payload: dict[str, Any]) -> dict[str, Any]:
        return canonical_copy(payload) if isinstance(payload, dict) else {"raw": payload}

    def _resolve_dataset_id(self, experiment: dict[str, Any], dataset_id: Optional[str]) -> str:
        immutable_dataset_id = _require_text("dataset_id", experiment["dataset_id"], max_length=200)
        if dataset_id is None:
            return immutable_dataset_id
        requested = _require_text("dataset_id", dataset_id, max_length=200)
        if requested != immutable_dataset_id:
            raise BacktestValidationError(
                "backtests must use the experiment's immutable dataset_id; clone the experiment to change datasets",
                code="immutable_dataset_mismatch",
            )
        return requested

    def _normalize_assumptions(self, assumptions: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(assumptions, dict):
            raise BacktestValidationError(
                "assumptions must be a JSON object",
                code="invalid_assumptions",
                violations=[{"field": "assumptions", "issue": "not_object"}],
            )

        simulation_level = _require_text(
            "assumptions.simulation_level",
            assumptions.get("simulation_level"),
            max_length=40,
        )
        if simulation_level not in BACKTEST_SIMULATION_LEVELS:
            raise BacktestValidationError(
                f"unsupported simulation_level: {simulation_level}",
                code="invalid_assumptions",
                violations=[{"field": "simulation_level", "issue": "unsupported_value"}],
            )

        split_method = _require_text("assumptions.split_method", assumptions.get("split_method"), max_length=40)
        if split_method != "chronological":
            raise BacktestValidationError(
                "split_method must be chronological",
                code="non_deterministic_split",
                violations=[{"field": "split_method", "issue": "must_be_chronological"}],
            )

        normalized = {
            "simulation_level": simulation_level,
            "fee_model_version": _require_text("assumptions.fee_model_version", assumptions.get("fee_model_version")),
            "latency_model_version": _require_text(
                "assumptions.latency_model_version",
                assumptions.get("latency_model_version"),
            ),
            "slippage_model_version": _require_text(
                "assumptions.slippage_model_version",
                assumptions.get("slippage_model_version"),
            ),
            "fill_model_version": _require_text("assumptions.fill_model_version", assumptions.get("fill_model_version")),
            "tick_size": str(_decimal_from_value("assumptions.tick_size", assumptions.get("tick_size"), minimum=Decimal("0.00000001"))),
            "price_precision_dp": int(_decimal_from_value("assumptions.price_precision_dp", assumptions.get("price_precision_dp"), minimum=Decimal("0"))),
            "quantity_precision_dp": int(_decimal_from_value("assumptions.quantity_precision_dp", assumptions.get("quantity_precision_dp"), minimum=Decimal("0"))),
            "stale_book_threshold_seconds": int(
                _decimal_from_value(
                    "assumptions.stale_book_threshold_seconds",
                    assumptions.get("stale_book_threshold_seconds"),
                    minimum=Decimal("1"),
                )
            ),
            "fee_bps": str(_decimal_from_value("assumptions.fee_bps", assumptions.get("fee_bps"), minimum=Decimal("0"))),
            "slippage_bps": str(
                _decimal_from_value("assumptions.slippage_bps", assumptions.get("slippage_bps"), minimum=Decimal("0"))
            ),
            "latency_seconds": int(
                _decimal_from_value("assumptions.latency_seconds", assumptions.get("latency_seconds"), minimum=Decimal("0"))
            ),
            "partial_fill_ratio": str(
                _decimal_from_value("assumptions.partial_fill_ratio", assumptions.get("partial_fill_ratio"), minimum=Decimal("0"))
            ),
            "split_method": split_method,
            "train_ratio": str(_decimal_from_value("assumptions.train_ratio", assumptions.get("train_ratio"), minimum=Decimal("0"))),
            "validation_ratio": str(
                _decimal_from_value("assumptions.validation_ratio", assumptions.get("validation_ratio"), minimum=Decimal("0"))
            ),
            "test_ratio": str(_decimal_from_value("assumptions.test_ratio", assumptions.get("test_ratio"), minimum=Decimal("0"))),
            "baseline": _require_text("assumptions.baseline", assumptions.get("baseline"), max_length=120),
        }

        partial_fill_ratio = Decimal(normalized["partial_fill_ratio"])
        if partial_fill_ratio <= 0 or partial_fill_ratio > 1:
            raise BacktestValidationError(
                "partial_fill_ratio must be > 0 and <= 1",
                code="invalid_assumptions",
                violations=[{"field": "partial_fill_ratio", "issue": "outside_range"}],
            )

        ratio_sum = Decimal(normalized["train_ratio"]) + Decimal(normalized["validation_ratio"]) + Decimal(normalized["test_ratio"])
        if ratio_sum != Decimal("1"):
            raise BacktestValidationError(
                "train_ratio + validation_ratio + test_ratio must equal 1",
                code="invalid_assumptions",
                violations=[{"field": "split_ratio_sum", "issue": "must_equal_1"}],
            )

        return normalized

    def _simulate(
        self,
        *,
        run_id: str,
        created_at: str,
        experiment: dict[str, Any],
        manifest: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
        timeline_points: int,
        history_sha256: str,
    ) -> dict[str, Any]:
        replay_result = self.replay.replay_strategy(
            experiment,
            assumptions,
            histories,
            validation_error=_replay_validation_error,
        )
        metrics = self._summarize_metrics(replay_result.trades, replay_result.rejections, assumptions)
        return {
            "run_id": run_id,
            "status": "SUCCEEDED",
            "created_at": created_at,
            "experiment_id": experiment["experiment_id"],
            "dataset_id": manifest["dataset_id"],
            "strategy_family": experiment["strategy_family"],
            "simulation_level": assumptions["simulation_level"],
            "input_fingerprints": {
                "dataset_id": manifest["dataset_id"],
                "dataset_sha256": manifest["raw_payload_sha256"],
                "history_sha256": history_sha256,
                "config_sha256": experiment["config_sha256"],
                "code_version": experiment["code_version"],
                "engine_version": BACKTEST_ENGINE_VERSION,
                "assumptions_sha256": hashlib.sha256(canonical_json(assumptions).encode("utf-8")).hexdigest(),
            },
            "assumptions": canonical_copy(assumptions),
            "timeline_points": timeline_points,
            "metrics": metrics,
            "trades": replay_result.trades,
            "rejections": replay_result.rejections,
        }

    def _failed_artifact(
        self,
        *,
        run_id: str,
        created_at: str,
        experiment: dict[str, Any],
        manifest: dict[str, Any],
        assumptions: dict[str, Any],
        failure_code: str,
        failure_message: str,
        violations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "status": "FAILED",
            "created_at": created_at,
            "experiment_id": experiment["experiment_id"],
            "dataset_id": manifest["dataset_id"],
            "strategy_family": experiment["strategy_family"],
            "assumptions": assumptions,
            "failure": {
                "code": failure_code,
                "message": failure_message,
                "violations": violations,
            },
            "trades": [],
            "rejections": [],
        }

    def _summarize_metrics(
        self,
        trades: list[dict[str, Any]],
        rejections: list[dict[str, Any]],
        assumptions: dict[str, Any],
    ) -> dict[str, Any]:
        gross_pnl = sum(Decimal(item["gross_pnl_usd"]) for item in trades) if trades else Decimal("0")
        net_pnl = sum(Decimal(item["net_pnl_usd"]) for item in trades) if trades else Decimal("0")
        fees = sum(Decimal(item["fees_usd"]) for item in trades) if trades else Decimal("0")
        slippage = sum(Decimal(item["slippage_usd"]) for item in trades) if trades else Decimal("0")
        notional = sum(Decimal(item["filled_notional_usd"]) for item in trades) if trades else Decimal("0")
        split_metrics: dict[str, dict[str, Any]] = {}
        for split_name in ("train", "validation", "test"):
            split_trades = [item for item in trades if item["split"] == split_name]
            split_metrics[split_name] = {
                "trade_count": len(split_trades),
                "net_pnl_usd": _format_decimal(sum(Decimal(item["net_pnl_usd"]) for item in split_trades) if split_trades else Decimal("0")),
            }

        return {
            "trade_count": len(trades),
            "rejection_count": len(rejections),
            "stale_rejection_count": sum(1 for item in rejections if "stale" in item["reason"]),
            "gross_pnl_usd": _format_decimal(gross_pnl),
            "net_pnl_usd": _format_decimal(net_pnl),
            "fees_usd": _format_decimal(fees),
            "slippage_usd": _format_decimal(slippage),
            "filled_notional_usd": _format_decimal(notional),
            "baseline": assumptions["baseline"],
            "splits": split_metrics,
        }

    def _promote_experiment_to_backtested(
        self,
        *,
        experiment_id: str,
        run_id: str,
        now: Optional[datetime],
    ) -> None:
        self.experiments.record_backtest_completed(
            experiment_id,
            changed_by="backtest-runner",
            reason=f"run_id={run_id}",
            now=now or utc_now(),
        )

    def _build_run_id(
        self,
        *,
        experiment_id: str,
        dataset_id: str,
        config_sha256: str,
        code_version: str,
        assumptions_sha256: str,
    ) -> str:
        basis = f"{experiment_id}:{dataset_id}:{config_sha256}:{code_version}:{assumptions_sha256}:{BACKTEST_ENGINE_VERSION}"
        suffix = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:12]
        return f"bt-{suffix}"

    def _load_run(self, run_id: str) -> dict[str, Any]:
        run_path = self.store.run_path(run_id)
        artifact_path = self.store.artifact_path(run_id)
        if not run_path.exists() or not artifact_path.exists():
            raise BacktestNotFoundError(f"unknown run_id: {run_id}")
        payload = read_json(run_path)
        payload["artifact"] = read_json(artifact_path)
        return payload


def build_backtest_service(root: Path) -> BacktestService:
    from .runtime import build_workspace

    return build_workspace(root).backtests
