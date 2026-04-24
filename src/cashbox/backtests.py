from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
import hashlib
import json
from math import ceil
from pathlib import Path
from typing import Any, Optional

from .experiments import ExperimentLifecycleError, ExperimentService, build_experiment_service
from .ingest import FileSystemMarketStore
from .models import NormalizedMarketRecord, format_datetime, parse_datetime, utc_now

getcontext().prec = 28

BACKTEST_RUN_STATUSES = ("SUCCEEDED", "FAILED")
BACKTEST_SIMULATION_LEVELS = ("price_replay", "top_of_book", "full_order_book")
BACKTEST_ENGINE_VERSION = 1


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _canonical_copy(payload: Any) -> Any:
    return json.loads(_canonical_json(payload))


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _quantize_down(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    increments = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return increments * step


def _format_decimal(value: Decimal, *, places: str = "0.00000001") -> str:
    quantized = value.quantize(Decimal(places))
    return format(quantized.normalize(), "f")


def _safe_divide(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


def _split_name(index: int, total_points: int, assumptions: dict[str, Any]) -> str:
    train_ratio = Decimal(assumptions["train_ratio"])
    validation_ratio = Decimal(assumptions["validation_ratio"])
    train_cutoff = int((Decimal(total_points) * train_ratio).to_integral_value(rounding=ROUND_DOWN))
    validation_cutoff = train_cutoff + int(
        (Decimal(total_points) * validation_ratio).to_integral_value(rounding=ROUND_DOWN)
    )
    if index < train_cutoff:
        return "train"
    if index < validation_cutoff:
        return "validation"
    return "test"


@dataclass(frozen=True)
class HistoryPoint:
    market_id: str
    timestamp: datetime
    end_time: Optional[datetime]
    price_proxy: Decimal
    liquidity: Decimal
    volume: Decimal


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

    def run_backtest(
        self,
        experiment_id: str,
        *,
        assumptions: dict[str, Any],
        dataset_id: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        experiment = self.experiments.get_experiment(_require_text("experiment_id", experiment_id, max_length=120))
        current_status = experiment.get("current_status")
        if current_status not in {"VALIDATED_CONFIG", "BACKTEST_QUEUED", "BACKTESTED"}:
            raise BacktestValidationError(
                f"experiment must be VALIDATED_CONFIG, BACKTEST_QUEUED, or BACKTESTED before backtesting; got {current_status}",
                code="invalid_experiment_status",
            )

        resolved_dataset_id = self._resolve_dataset_id(experiment, dataset_id)
        manifest = self.market_store.load_manifest(resolved_dataset_id)
        raw_assumptions = _canonical_copy(assumptions)
        assumptions_sha256 = hashlib.sha256(_canonical_json(raw_assumptions).encode("utf-8")).hexdigest()
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
            histories, timeline_points, history_sha256 = self._load_histories(experiment, manifest.dataset_id)
            artifact = self._simulate(
                run_id=run_id,
                created_at=created_at,
                experiment=experiment,
                manifest=manifest.to_dict(),
                assumptions=normalized_assumptions,
                histories=histories,
                timeline_points=timeline_points,
                history_sha256=history_sha256,
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

        artifact_sha256 = hashlib.sha256(_canonical_json(artifact).encode("utf-8")).hexdigest()
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
        _json_dump(self.store.run_path(run_id), run_payload)
        _json_dump(self.store.artifact_path(run_id), artifact)
        return self._load_run(run_id)

    def get_backtest_artifacts(self, run_id: str) -> dict[str, Any]:
        _require_text("run_id", run_id, max_length=160)
        path = self.store.artifact_path(run_id)
        if not path.exists():
            raise BacktestNotFoundError(f"unknown run_id: {run_id}")
        return _json_load(path)

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
        return _canonical_copy(payload) if isinstance(payload, dict) else {"raw": payload}

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

    def _load_histories(
        self,
        experiment: dict[str, Any],
        dataset_id: str,
    ) -> tuple[dict[str, list[HistoryPoint]], int, str]:
        dataset_manifest = self.market_store.load_manifest(dataset_id)
        dataset_time = parse_datetime(dataset_manifest.ingested_at)
        if dataset_time is None:
            raise BacktestValidationError(
                "dataset manifest is missing a versioned ingest timestamp",
                code="unversioned_dataset",
            )

        market_ids = self._market_ids_for_experiment(experiment)
        histories: dict[str, list[HistoryPoint]] = {}
        history_fingerprint_rows: list[dict[str, Any]] = []
        for market_id in market_ids:
            points: list[HistoryPoint] = []
            for row in self.market_store.load_history(market_id):
                recorded_at = parse_datetime(row.get("recorded_at"))
                if recorded_at is None or recorded_at > dataset_time:
                    continue
                record = NormalizedMarketRecord.from_dict(row["record"])
                end_time = parse_datetime(record.end_time)
                if end_time is not None and recorded_at > end_time:
                    raise BacktestValidationError(
                        f"history for {market_id} includes post-resolution data at {format_datetime(recorded_at)}",
                        code="post_resolution_data",
                        violations=[
                            {
                                "market_id": market_id,
                                "recorded_at": format_datetime(recorded_at),
                                "end_time": format_datetime(end_time),
                                "issue": "post_resolution_data",
                            }
                        ],
                    )
                point = HistoryPoint(
                    market_id=market_id,
                    timestamp=recorded_at,
                    end_time=end_time,
                    price_proxy=self._price_proxy(record),
                    liquidity=self._decimal_text(record.liquidity),
                    volume=self._decimal_text(record.volume),
                )
                points.append(point)
                history_fingerprint_rows.append(
                    {
                        "market_id": market_id,
                        "timestamp": format_datetime(recorded_at),
                        "price_proxy": _format_decimal(point.price_proxy),
                        "liquidity": _format_decimal(point.liquidity),
                        "volume": _format_decimal(point.volume),
                    }
                )
            points.sort(key=lambda item: item.timestamp)
            if len(points) < 2:
                raise BacktestValidationError(
                    f"insufficient history for market {market_id}; need at least 2 point-in-time records",
                    code="insufficient_history",
                    violations=[{"market_id": market_id, "issue": "insufficient_history"}],
                )
            histories[market_id] = points

        timeline_points = min(len(points) for points in histories.values())
        history_sha256 = hashlib.sha256(_canonical_json(history_fingerprint_rows).encode("utf-8")).hexdigest()
        return histories, timeline_points, history_sha256

    def _market_ids_for_experiment(self, experiment: dict[str, Any]) -> list[str]:
        config = experiment["config"]
        if experiment["strategy_family"] == "cross_market_arbitrage":
            market_ids = [_require_text("config.market_ids[]", item, max_length=160) for item in config["market_ids"]]
            return sorted(dict.fromkeys(market_ids))
        return [_require_text("config.market_id", config["market_id"], max_length=160)]

    def _price_proxy(self, record: NormalizedMarketRecord) -> Decimal:
        volume = self._decimal_text(record.volume)
        liquidity = self._decimal_text(record.liquidity)
        return (volume + Decimal("1")) / (volume + liquidity + Decimal("2"))

    def _decimal_text(self, value: Optional[str]) -> Decimal:
        if value in (None, ""):
            return Decimal("0")
        return Decimal(str(value))

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
        if experiment["strategy_family"] == "midpoint_reversion":
            trades, rejections = self._simulate_midpoint_reversion(experiment, assumptions, histories)
        elif experiment["strategy_family"] == "resolution_drift":
            trades, rejections = self._simulate_resolution_drift(experiment, assumptions, histories)
        elif experiment["strategy_family"] == "cross_market_arbitrage":
            trades, rejections = self._simulate_cross_market_arbitrage(experiment, assumptions, histories)
        else:  # pragma: no cover - experiment validation blocks this path.
            raise BacktestValidationError(
                f"unsupported strategy_family: {experiment['strategy_family']}",
                code="unsupported_strategy_family",
            )

        metrics = self._summarize_metrics(trades, rejections, assumptions)
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
                "assumptions_sha256": hashlib.sha256(_canonical_json(assumptions).encode("utf-8")).hexdigest(),
            },
            "assumptions": _canonical_copy(assumptions),
            "timeline_points": timeline_points,
            "metrics": metrics,
            "trades": trades,
            "rejections": rejections,
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

    def _simulate_midpoint_reversion(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        points = histories[experiment["config"]["market_id"]]
        lookback = int(experiment["config"]["lookback_minutes"])
        entry_threshold = Decimal(str(experiment["config"]["entry_zscore"]))
        exit_threshold = Decimal(str(experiment["config"]["exit_zscore"]))
        latency_steps = self._latency_steps(points, assumptions)
        trades: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []
        zscores = self._zscores(points, lookback)

        for signal_index in range(lookback, len(points) - 1):
            zscore = zscores[signal_index]
            if zscore is None:
                continue
            direction = None
            if zscore <= -entry_threshold:
                direction = "LONG"
            elif zscore >= entry_threshold:
                direction = "SHORT"
            if direction is None:
                continue

            fill_index = signal_index + latency_steps
            if fill_index >= len(points) - 1:
                rejections.append(self._rejection(points[signal_index], "latency_overflow", signal_index))
                continue
            if points[fill_index].timestamp - points[signal_index].timestamp > timedelta(
                seconds=int(assumptions["stale_book_threshold_seconds"])
            ):
                rejections.append(self._rejection(points[signal_index], "stale_book", signal_index))
                continue

            exit_index = fill_index + 1
            for candidate_index in range(fill_index + 1, len(points)):
                candidate = zscores[candidate_index]
                if candidate is not None and abs(candidate) <= exit_threshold:
                    exit_index = candidate_index
                    break
            trades.append(
                self._completed_trade(
                    trade_number=len(trades) + 1,
                    split=_split_name(signal_index, len(points), assumptions),
                    market_id=points[signal_index].market_id,
                    signal_index=signal_index,
                    signal_time=points[signal_index].timestamp,
                    entry_time=points[fill_index].timestamp,
                    exit_time=points[exit_index].timestamp,
                    direction=direction,
                    signal_strength_bps=self._return_bps(points[max(signal_index - 1, 0)].price_proxy, points[signal_index].price_proxy),
                    entry_price=points[fill_index].price_proxy,
                    exit_price=points[exit_index].price_proxy,
                    notional_cap=Decimal(str(experiment["config"]["max_position_usd"])),
                    assumptions=assumptions,
                    leg_count=1,
                )
            )

        return trades, rejections

    def _simulate_resolution_drift(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        points = histories[experiment["config"]["market_id"]]
        signal_window = timedelta(minutes=int(experiment["config"]["signal_window_minutes"]))
        entry_edge_bps = Decimal(str(experiment["config"]["entry_edge_bps"]))
        max_holding = timedelta(minutes=int(experiment["config"]["max_holding_minutes"]))
        latency_steps = self._latency_steps(points, assumptions)
        trades: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []

        for signal_index in range(1, len(points) - 1):
            end_time = points[signal_index].end_time
            if end_time is None:
                continue
            time_to_end = end_time - points[signal_index].timestamp
            if time_to_end <= timedelta(0) or time_to_end > signal_window:
                continue

            move_bps = self._return_bps(points[signal_index - 1].price_proxy, points[signal_index].price_proxy).copy_abs()
            if move_bps < entry_edge_bps:
                continue

            fill_index = signal_index + latency_steps
            if fill_index >= len(points) - 1:
                rejections.append(self._rejection(points[signal_index], "latency_overflow", signal_index))
                continue
            if points[fill_index].timestamp - points[signal_index].timestamp > timedelta(
                seconds=int(assumptions["stale_book_threshold_seconds"])
            ):
                rejections.append(self._rejection(points[signal_index], "stale_book", signal_index))
                continue

            direction = "LONG" if points[signal_index].price_proxy >= points[signal_index - 1].price_proxy else "SHORT"
            exit_index = fill_index + 1
            for candidate_index in range(fill_index + 1, len(points)):
                if points[candidate_index].timestamp - points[fill_index].timestamp >= max_holding:
                    exit_index = candidate_index
                    break
            trades.append(
                self._completed_trade(
                    trade_number=len(trades) + 1,
                    split=_split_name(signal_index, len(points), assumptions),
                    market_id=points[signal_index].market_id,
                    signal_index=signal_index,
                    signal_time=points[signal_index].timestamp,
                    entry_time=points[fill_index].timestamp,
                    exit_time=points[exit_index].timestamp,
                    direction=direction,
                    signal_strength_bps=move_bps,
                    entry_price=points[fill_index].price_proxy,
                    exit_price=points[exit_index].price_proxy,
                    notional_cap=Decimal(str(experiment["config"]["max_position_usd"])),
                    assumptions=assumptions,
                    leg_count=1,
                )
            )

        return trades, rejections

    def _simulate_cross_market_arbitrage(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        market_ids = sorted(histories)
        min_points = min(len(points) for points in histories.values())
        base_points = histories[market_ids[0]]
        latency_steps = self._latency_steps(base_points, assumptions)
        max_spread_bps = Decimal(str(experiment["config"]["max_spread_bps"]))
        min_edge_bps = Decimal(str(experiment["config"]["min_edge_bps"]))
        rebalance_seconds = int(experiment["config"]["rebalance_interval_seconds"])
        rebalance_steps = max(1, latency_steps, ceil(rebalance_seconds / max(self._median_step_seconds(base_points), 1)))
        trades: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []

        for signal_index in range(min_points - 1):
            signal_points = {market_id: histories[market_id][signal_index] for market_id in market_ids}
            timestamps = [point.timestamp for point in signal_points.values()]
            if max(timestamps) - min(timestamps) > timedelta(seconds=int(assumptions["stale_book_threshold_seconds"])):
                rejections.append(self._rejection(base_points[signal_index], "stale_cross_market_snapshot", signal_index))
                continue

            sorted_by_price = sorted(
                ((point.price_proxy, market_id) for market_id, point in signal_points.items()),
                key=lambda item: (item[0], item[1]),
            )
            low_price, low_market_id = sorted_by_price[0]
            high_price, high_market_id = sorted_by_price[-1]
            spread_bps = self._return_bps(low_price, high_price)
            if spread_bps < min_edge_bps or spread_bps > max_spread_bps:
                continue

            fill_index = signal_index + latency_steps
            exit_index = fill_index + rebalance_steps
            if exit_index >= min_points:
                rejections.append(self._rejection(base_points[signal_index], "latency_overflow", signal_index))
                continue

            low_fill = histories[low_market_id][fill_index]
            high_fill = histories[high_market_id][fill_index]
            if max(low_fill.timestamp, high_fill.timestamp) - min(low_fill.timestamp, high_fill.timestamp) > timedelta(
                seconds=int(assumptions["stale_book_threshold_seconds"])
            ):
                rejections.append(self._rejection(base_points[signal_index], "stale_book", signal_index))
                continue

            low_exit = histories[low_market_id][exit_index]
            high_exit = histories[high_market_id][exit_index]
            trade = self._completed_trade(
                trade_number=len(trades) + 1,
                split=_split_name(signal_index, min_points, assumptions),
                market_id=f"{low_market_id}|{high_market_id}",
                signal_index=signal_index,
                signal_time=max(signal_points[low_market_id].timestamp, signal_points[high_market_id].timestamp),
                entry_time=max(low_fill.timestamp, high_fill.timestamp),
                exit_time=max(low_exit.timestamp, high_exit.timestamp),
                direction="CONVERGENCE",
                signal_strength_bps=spread_bps,
                entry_price=(low_fill.price_proxy + high_fill.price_proxy) / 2,
                exit_price=(low_exit.price_proxy + high_exit.price_proxy) / 2,
                notional_cap=Decimal(str(experiment["config"]["max_position_usd"])),
                assumptions=assumptions,
                leg_count=2,
                extra_fields={
                    "low_market_id": low_market_id,
                    "high_market_id": high_market_id,
                    "low_entry_price": _format_decimal(low_fill.price_proxy),
                    "high_entry_price": _format_decimal(high_fill.price_proxy),
                    "low_exit_price": _format_decimal(low_exit.price_proxy),
                    "high_exit_price": _format_decimal(high_exit.price_proxy),
                },
            )
            gross_pnl = (
                Decimal(trade["quantity"]) * (low_exit.price_proxy - low_fill.price_proxy)
                + Decimal(trade["quantity"]) * (high_fill.price_proxy - high_exit.price_proxy)
            )
            trade["gross_pnl_usd"] = _format_decimal(gross_pnl)
            total_cost = Decimal(trade["fees_usd"]) + Decimal(trade["slippage_usd"])
            trade["net_pnl_usd"] = _format_decimal(gross_pnl - total_cost)
            trades.append(trade)

        return trades, rejections

    def _zscores(self, points: list[HistoryPoint], lookback: int) -> list[Optional[Decimal]]:
        zscores: list[Optional[Decimal]] = [None] * len(points)
        for index in range(lookback, len(points)):
            window = [point.price_proxy for point in points[index - lookback : index]]
            mean = sum(window) / Decimal(len(window))
            variance = sum((value - mean) ** 2 for value in window) / Decimal(len(window))
            if variance == 0:
                zscores[index] = Decimal("0")
                continue
            zscores[index] = (points[index].price_proxy - mean) / variance.sqrt()
        return zscores

    def _latency_steps(self, points: list[HistoryPoint], assumptions: dict[str, Any]) -> int:
        latency_seconds = int(assumptions["latency_seconds"])
        if latency_seconds <= 0:
            return 0
        median_step = max(self._median_step_seconds(points), 1)
        return max(1, ceil(latency_seconds / median_step))

    def _median_step_seconds(self, points: list[HistoryPoint]) -> int:
        if len(points) < 2:
            return 1
        deltas = sorted(
            max(1, int((points[index].timestamp - points[index - 1].timestamp).total_seconds()))
            for index in range(1, len(points))
        )
        return deltas[len(deltas) // 2]

    def _completed_trade(
        self,
        *,
        trade_number: int,
        split: str,
        market_id: str,
        signal_index: int,
        signal_time: datetime,
        entry_time: datetime,
        exit_time: datetime,
        direction: str,
        signal_strength_bps: Decimal,
        entry_price: Decimal,
        exit_price: Decimal,
        notional_cap: Decimal,
        assumptions: dict[str, Any],
        leg_count: int,
        extra_fields: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        tick_size = Decimal(assumptions["tick_size"])
        quantity_step = Decimal("1").scaleb(-int(assumptions["quantity_precision_dp"]))
        partial_fill_ratio = Decimal(assumptions["partial_fill_ratio"])
        effective_notional = notional_cap * partial_fill_ratio
        quantity = _quantize_down(_safe_divide(effective_notional, max(entry_price, tick_size)), quantity_step)
        if quantity <= 0:
            raise BacktestValidationError(
                "effective quantity rounded to zero under configured precision constraints",
                code="precision_rejection",
            )

        if direction == "LONG":
            gross_pnl = quantity * (exit_price - entry_price)
        elif direction == "SHORT":
            gross_pnl = quantity * (entry_price - exit_price)
        else:
            gross_pnl = quantity * (exit_price - entry_price)

        traded_notional = effective_notional * Decimal(leg_count)
        fees = traded_notional * Decimal(assumptions["fee_bps"]) / Decimal("10000")
        slippage = traded_notional * Decimal(assumptions["slippage_bps"]) / Decimal("10000")
        payload = {
            "trade_id": f"trade-{trade_number:04d}",
            "split": split,
            "market_id": market_id,
            "signal_index": signal_index,
            "signal_time": format_datetime(signal_time),
            "entry_time": format_datetime(entry_time),
            "exit_time": format_datetime(exit_time),
            "direction": direction,
            "signal_strength_bps": _format_decimal(signal_strength_bps),
            "entry_price": _format_decimal(entry_price),
            "exit_price": _format_decimal(exit_price),
            "quantity": _format_decimal(quantity),
            "filled_notional_usd": _format_decimal(traded_notional),
            "fees_usd": _format_decimal(fees),
            "slippage_usd": _format_decimal(slippage),
            "gross_pnl_usd": _format_decimal(gross_pnl),
            "net_pnl_usd": _format_decimal(gross_pnl - fees - slippage),
            "partial_fill_ratio": _format_decimal(partial_fill_ratio),
        }
        if extra_fields:
            payload.update(extra_fields)
        return payload

    def _rejection(self, point: HistoryPoint, reason: str, signal_index: int) -> dict[str, Any]:
        return {
            "market_id": point.market_id,
            "reason": reason,
            "signal_index": signal_index,
            "signal_time": format_datetime(point.timestamp),
        }

    def _return_bps(self, start: Decimal, end: Decimal) -> Decimal:
        if start <= 0:
            return Decimal("0")
        return ((end - start) / start) * Decimal("10000")

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
        experiment = self.experiments.get_experiment(experiment_id)
        current_status = experiment["current_status"]
        timestamp = now or utc_now()
        if current_status == "VALIDATED_CONFIG":
            self.experiments.transition_experiment_status(
                experiment_id,
                to_status="BACKTEST_QUEUED",
                changed_by="backtest-runner",
                reason=f"run_id={run_id}",
                now=timestamp,
            )
            current_status = "BACKTEST_QUEUED"
        if current_status == "BACKTEST_QUEUED":
            self.experiments.transition_experiment_status(
                experiment_id,
                to_status="BACKTESTED",
                changed_by="backtest-runner",
                reason=f"run_id={run_id}",
                now=timestamp,
            )
        elif current_status != "BACKTESTED":
            raise ExperimentLifecycleError(f"cannot promote experiment from status {current_status} during backtest")

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
        payload = _json_load(run_path)
        payload["artifact"] = _json_load(artifact_path)
        return payload


def build_backtest_service(root: Path) -> BacktestService:
    root_path = Path(root)
    return BacktestService(
        FileSystemBacktestStore(root_path),
        experiments=build_experiment_service(root_path),
        market_store=FileSystemMarketStore(root_path),
    )
