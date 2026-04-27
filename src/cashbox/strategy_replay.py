from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
import hashlib
import json
from math import ceil
from typing import Any, Callable, Optional, Union

from .market_history import FileSystemMarketHistory
from .models import NormalizedMarketRecord, format_datetime, parse_datetime, utc_now

STRATEGY_REPLAY_SIMULATION_LEVELS = ("price_replay", "top_of_book", "full_order_book")
STRATEGY_REPLAY_SPLITS = ("train", "validation", "test")
STRATEGY_REPLAY_PAPER_SPLIT = "paper"


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _require_text(name: str, value: Any, *, max_length: int = 2000) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must be non-empty")
    if len(normalized) > max_length:
        raise ValueError(f"{name} exceeds max length {max_length}")
    return normalized


def _decimal_from_value(
    name: str,
    value: Any,
    *,
    minimum: Optional[Decimal] = None,
    validation_error: Callable[..., Exception],
) -> Decimal:
    try:
        decimal_value = Decimal(str(value))
    except Exception as exc:  # pragma: no cover - Decimal raises multiple exception types.
        raise validation_error(f"{name} must be numeric", code="invalid_assumptions") from exc
    if minimum is not None and decimal_value < minimum:
        raise validation_error(
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


@dataclass(frozen=True)
class LoadedHistoryBatch:
    histories: dict[str, list[HistoryPoint]]
    timeline_points: int
    history_sha256: str


@dataclass(frozen=True)
class LoadedHistoryWindow:
    histories: dict[str, list[HistoryPoint]]
    timeline_points: int
    history_sha256: str
    source_window: dict[str, Any]


@dataclass(frozen=True)
class StrategyReplayResult:
    trades: list[dict[str, Any]]
    rejections: list[dict[str, Any]]
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PaperReplayResult:
    candidate_trades: list[dict[str, Any]]
    trades: list[dict[str, Any]]
    rejections: list[dict[str, Any]]
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BacktestReplayRequest:
    run_id: str
    created_at: str
    experiment: dict[str, Any]
    manifest: dict[str, Any]
    assumptions: dict[str, Any]
    engine_version: int
    validation_error: Callable[..., Exception]


@dataclass(frozen=True)
class BacktestReplayArtifacts:
    artifact: dict[str, Any]
    assumptions: dict[str, Any]
    history_sha256: str
    timeline_points: int


@dataclass(frozen=True)
class PaperReplayRequest:
    created_at: str
    experiment: dict[str, Any]
    backtest_run: dict[str, Any]
    start_dataset_id: str
    end_dataset_id: str
    latest_dataset_id: str
    engine_version: int
    paper_run_id_factory: Callable[[str], str]
    validation_error: Callable[..., Exception]


@dataclass(frozen=True)
class PaperReplayArtifacts:
    paper_run_id: str
    artifact: dict[str, Any]
    drift_report: dict[str, Any]
    metrics: dict[str, Any]
    history_sha256: str
    timeline_points: int
    source_window: dict[str, Any]


StrategyReplayRequest = Union[BacktestReplayRequest, PaperReplayRequest]
StrategyReplayArtifacts = Union[BacktestReplayArtifacts, PaperReplayArtifacts]


class StrategyReplayService:
    def __init__(self, market_history: FileSystemMarketHistory) -> None:
        self.market_history = market_history

    @property
    def market_store(self) -> FileSystemMarketHistory:
        return self.market_history

    def run_replay(self, request: StrategyReplayRequest) -> StrategyReplayArtifacts:
        if isinstance(request, BacktestReplayRequest):
            return self._run_backtest_replay(request)
        if isinstance(request, PaperReplayRequest):
            return self._run_paper_replay(request)
        raise TypeError(f"unsupported replay request: {type(request).__name__}")

    def _run_backtest_replay(self, request: BacktestReplayRequest) -> BacktestReplayArtifacts:
        assumptions = self.normalize_assumptions(
            request.assumptions,
            validation_error=request.validation_error,
        )
        loaded_histories = self.load_backtest_histories(
            request.experiment,
            request.manifest["dataset_id"],
            validation_error=request.validation_error,
        )
        artifact = self.build_backtest_artifact(
            run_id=request.run_id,
            created_at=request.created_at,
            experiment=request.experiment,
            manifest=request.manifest,
            assumptions=assumptions,
            histories=loaded_histories.histories,
            timeline_points=loaded_histories.timeline_points,
            history_sha256=loaded_histories.history_sha256,
            engine_version=request.engine_version,
            validation_error=request.validation_error,
        )
        return BacktestReplayArtifacts(
            artifact=artifact,
            assumptions=assumptions,
            history_sha256=loaded_histories.history_sha256,
            timeline_points=loaded_histories.timeline_points,
        )

    def _run_paper_replay(self, request: PaperReplayRequest) -> PaperReplayArtifacts:
        loaded_histories = self.load_paper_histories(
            request.experiment,
            start_dataset_id=request.start_dataset_id,
            end_dataset_id=request.end_dataset_id,
            validation_error=request.validation_error,
        )
        paper_run_id = request.paper_run_id_factory(loaded_histories.history_sha256)
        paper_replay = self.replay_paper_strategy(
            request.experiment,
            assumptions=request.backtest_run["artifact"]["assumptions"],
            histories=loaded_histories.histories,
            validation_error=request.validation_error,
        )
        drift_report = self.build_paper_drift_report(
            experiment_id=request.experiment["experiment_id"],
            paper_run_id=paper_run_id,
            backtest_run_id=request.backtest_run["run_id"],
            reference_assumptions=request.backtest_run["artifact"]["assumptions"],
            reference_metrics=request.backtest_run["artifact"]["metrics"],
            paper_metrics=paper_replay.metrics,
            paper_rejections=paper_replay.rejections,
            report_version=request.engine_version,
        )
        artifact = self.build_paper_artifact(
            paper_run_id=paper_run_id,
            created_at=request.created_at,
            experiment=request.experiment,
            backtest_run=request.backtest_run,
            source_window=loaded_histories.source_window,
            timeline_points=loaded_histories.timeline_points,
            history_sha256=loaded_histories.history_sha256,
            latest_dataset_id=request.latest_dataset_id,
            engine_version=request.engine_version,
            paper_replay=paper_replay,
            drift_report_id=drift_report["report_id"],
        )
        return PaperReplayArtifacts(
            paper_run_id=paper_run_id,
            artifact=artifact,
            drift_report=drift_report,
            metrics=paper_replay.metrics,
            history_sha256=loaded_histories.history_sha256,
            timeline_points=loaded_histories.timeline_points,
            source_window=loaded_histories.source_window,
        )

    def normalize_assumptions(
        self,
        assumptions: dict[str, Any],
        *,
        validation_error: Callable[..., Exception],
    ) -> dict[str, Any]:
        if not isinstance(assumptions, dict):
            raise validation_error(
                "assumptions must be a JSON object",
                code="invalid_assumptions",
                violations=[{"field": "assumptions", "issue": "not_object"}],
            )

        try:
            simulation_level = _require_text(
                "assumptions.simulation_level",
                assumptions.get("simulation_level"),
                max_length=40,
            )
            split_method = _require_text(
                "assumptions.split_method",
                assumptions.get("split_method"),
                max_length=40,
            )
            normalized = {
                "simulation_level": simulation_level,
                "fee_model_version": _require_text(
                    "assumptions.fee_model_version",
                    assumptions.get("fee_model_version"),
                ),
                "latency_model_version": _require_text(
                    "assumptions.latency_model_version",
                    assumptions.get("latency_model_version"),
                ),
                "slippage_model_version": _require_text(
                    "assumptions.slippage_model_version",
                    assumptions.get("slippage_model_version"),
                ),
                "fill_model_version": _require_text(
                    "assumptions.fill_model_version",
                    assumptions.get("fill_model_version"),
                ),
                "tick_size": str(
                    _decimal_from_value(
                        "assumptions.tick_size",
                        assumptions.get("tick_size"),
                        minimum=Decimal("0.00000001"),
                        validation_error=validation_error,
                    )
                ),
                "price_precision_dp": int(
                    _decimal_from_value(
                        "assumptions.price_precision_dp",
                        assumptions.get("price_precision_dp"),
                        minimum=Decimal("0"),
                        validation_error=validation_error,
                    )
                ),
                "quantity_precision_dp": int(
                    _decimal_from_value(
                        "assumptions.quantity_precision_dp",
                        assumptions.get("quantity_precision_dp"),
                        minimum=Decimal("0"),
                        validation_error=validation_error,
                    )
                ),
                "stale_book_threshold_seconds": int(
                    _decimal_from_value(
                        "assumptions.stale_book_threshold_seconds",
                        assumptions.get("stale_book_threshold_seconds"),
                        minimum=Decimal("1"),
                        validation_error=validation_error,
                    )
                ),
                "fee_bps": str(
                    _decimal_from_value(
                        "assumptions.fee_bps",
                        assumptions.get("fee_bps"),
                        minimum=Decimal("0"),
                        validation_error=validation_error,
                    )
                ),
                "slippage_bps": str(
                    _decimal_from_value(
                        "assumptions.slippage_bps",
                        assumptions.get("slippage_bps"),
                        minimum=Decimal("0"),
                        validation_error=validation_error,
                    )
                ),
                "latency_seconds": int(
                    _decimal_from_value(
                        "assumptions.latency_seconds",
                        assumptions.get("latency_seconds"),
                        minimum=Decimal("0"),
                        validation_error=validation_error,
                    )
                ),
                "partial_fill_ratio": str(
                    _decimal_from_value(
                        "assumptions.partial_fill_ratio",
                        assumptions.get("partial_fill_ratio"),
                        minimum=Decimal("0"),
                        validation_error=validation_error,
                    )
                ),
                "split_method": split_method,
                "train_ratio": str(
                    _decimal_from_value(
                        "assumptions.train_ratio",
                        assumptions.get("train_ratio"),
                        minimum=Decimal("0"),
                        validation_error=validation_error,
                    )
                ),
                "validation_ratio": str(
                    _decimal_from_value(
                        "assumptions.validation_ratio",
                        assumptions.get("validation_ratio"),
                        minimum=Decimal("0"),
                        validation_error=validation_error,
                    )
                ),
                "test_ratio": str(
                    _decimal_from_value(
                        "assumptions.test_ratio",
                        assumptions.get("test_ratio"),
                        minimum=Decimal("0"),
                        validation_error=validation_error,
                    )
                ),
                "baseline": _require_text(
                    "assumptions.baseline",
                    assumptions.get("baseline"),
                    max_length=120,
                ),
            }
        except ValueError as exc:
            raise validation_error(str(exc), code="invalid_assumptions") from exc

        if simulation_level not in STRATEGY_REPLAY_SIMULATION_LEVELS:
            raise validation_error(
                f"unsupported simulation_level: {simulation_level}",
                code="invalid_assumptions",
                violations=[{"field": "simulation_level", "issue": "unsupported_value"}],
            )
        if split_method != "chronological":
            raise validation_error(
                "split_method must be chronological",
                code="non_deterministic_split",
                violations=[{"field": "split_method", "issue": "must_be_chronological"}],
            )

        partial_fill_ratio = Decimal(normalized["partial_fill_ratio"])
        if partial_fill_ratio <= 0 or partial_fill_ratio > 1:
            raise validation_error(
                "partial_fill_ratio must be > 0 and <= 1",
                code="invalid_assumptions",
                violations=[{"field": "partial_fill_ratio", "issue": "outside_range"}],
            )

        ratio_sum = (
            Decimal(normalized["train_ratio"])
            + Decimal(normalized["validation_ratio"])
            + Decimal(normalized["test_ratio"])
        )
        if ratio_sum != Decimal("1"):
            raise validation_error(
                "train_ratio + validation_ratio + test_ratio must equal 1",
                code="invalid_assumptions",
                violations=[{"field": "split_ratio_sum", "issue": "must_equal_1"}],
            )

        return normalized

    def best_effort_assumptions(self, payload: Any) -> dict[str, Any]:
        return json.loads(_canonical_json(payload)) if isinstance(payload, dict) else {"raw": payload}

    def load_backtest_histories(
        self,
        experiment: dict[str, Any],
        dataset_id: str,
        *,
        validation_error: Callable[..., Exception],
    ) -> LoadedHistoryBatch:
        dataset_manifest = self.market_history.load_manifest(dataset_id)
        dataset_time = parse_datetime(dataset_manifest.ingested_at)
        if dataset_time is None:
            raise validation_error(
                "dataset manifest is missing a versioned ingest timestamp",
                code="unversioned_dataset",
            )
        return self._load_histories(
            experiment,
            start_time=None,
            end_time=dataset_time,
            minimum_points=2,
            insufficient_history_message="insufficient history for market {market_id}; need at least 2 point-in-time records",
            post_resolution_message="history for {market_id} includes post-resolution data at {recorded_at}",
            validation_error=validation_error,
        )

    def load_paper_histories(
        self,
        experiment: dict[str, Any],
        *,
        start_dataset_id: str,
        end_dataset_id: str,
        validation_error: Callable[..., Exception],
    ) -> LoadedHistoryWindow:
        start_manifest = self.market_history.load_manifest(start_dataset_id)
        end_manifest = self.market_history.load_manifest(end_dataset_id)
        start_time = parse_datetime(start_manifest.ingested_at)
        end_time = parse_datetime(end_manifest.ingested_at)
        if start_time is None or end_time is None:
            raise validation_error("paper trading requires versioned dataset timestamps")
        if end_time <= start_time:
            raise validation_error("paper trading requires at least one newer dataset after the backtest dataset")

        loaded = self._load_histories(
            experiment,
            start_time=start_time,
            end_time=end_time,
            minimum_points=2,
            insufficient_history_message="insufficient future history for market {market_id}; need at least 2 post-backtest points",
            post_resolution_message="future history for {market_id} includes post-resolution data at {recorded_at}",
            validation_error=validation_error,
        )
        return LoadedHistoryWindow(
            histories=loaded.histories,
            timeline_points=loaded.timeline_points,
            history_sha256=loaded.history_sha256,
            source_window={
                "start_dataset_id": start_dataset_id,
                "end_dataset_id": end_dataset_id,
                "start_at": start_manifest.ingested_at,
                "end_at": end_manifest.ingested_at,
            },
        )

    def replay_strategy(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
        *,
        split_name_fn: Optional[Callable[[int, int], str]] = None,
        split_names: tuple[str, ...] = STRATEGY_REPLAY_SPLITS,
        validation_error: Callable[..., Exception],
    ) -> StrategyReplayResult:
        resolved_split_name_fn = split_name_fn or (lambda index, total: _split_name(index, total, assumptions))
        if experiment["strategy_family"] == "midpoint_reversion":
            trades, rejections = self._simulate_midpoint_reversion(
                experiment,
                assumptions,
                histories,
                split_name_fn=resolved_split_name_fn,
                validation_error=validation_error,
            )
        elif experiment["strategy_family"] == "resolution_drift":
            trades, rejections = self._simulate_resolution_drift(
                experiment,
                assumptions,
                histories,
                split_name_fn=resolved_split_name_fn,
                validation_error=validation_error,
            )
        elif experiment["strategy_family"] == "cross_market_arbitrage":
            trades, rejections = self._simulate_cross_market_arbitrage(
                experiment,
                assumptions,
                histories,
                split_name_fn=resolved_split_name_fn,
                validation_error=validation_error,
            )
        else:
            raise validation_error(
                f"unsupported strategy_family: {experiment['strategy_family']}",
                code="unsupported_strategy_family",
            )

        metrics = self.summarize_strategy_metrics(
            trades,
            rejections,
            assumptions,
            split_names=split_names,
        )
        return StrategyReplayResult(trades=trades, rejections=rejections, metrics=metrics)

    def replay_paper_strategy(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
        *,
        validation_error: Callable[..., Exception],
    ) -> PaperReplayResult:
        candidate_result = self.replay_strategy(
            experiment,
            assumptions,
            histories,
            split_name_fn=lambda _index, _total: STRATEGY_REPLAY_PAPER_SPLIT,
            split_names=(STRATEGY_REPLAY_PAPER_SPLIT,),
            validation_error=validation_error,
        )
        observed_trades, missed_fills = self.observe_paper_trades(candidate_result.trades, assumptions)
        rejections = candidate_result.rejections + missed_fills
        metrics = self.summarize_paper_metrics(
            candidate_trades=candidate_result.trades,
            trades=observed_trades,
            rejections=rejections,
        )
        return PaperReplayResult(
            candidate_trades=candidate_result.trades,
            trades=observed_trades,
            rejections=rejections,
            metrics=metrics,
        )

    def build_backtest_artifact(
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
        engine_version: int,
        validation_error: Callable[..., Exception],
    ) -> dict[str, Any]:
        replay_result = self.replay_strategy(
            experiment,
            assumptions,
            histories,
            validation_error=validation_error,
        )
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
                "engine_version": engine_version,
                "assumptions_sha256": hashlib.sha256(_canonical_json(assumptions).encode("utf-8")).hexdigest(),
            },
            "assumptions": json.loads(_canonical_json(assumptions)),
            "timeline_points": timeline_points,
            "metrics": replay_result.metrics,
            "trades": replay_result.trades,
            "rejections": replay_result.rejections,
        }

    def build_paper_artifact(
        self,
        *,
        paper_run_id: str,
        created_at: str,
        experiment: dict[str, Any],
        backtest_run: dict[str, Any],
        source_window: dict[str, Any],
        timeline_points: int,
        history_sha256: str,
        latest_dataset_id: str,
        engine_version: int,
        paper_replay: PaperReplayResult,
        drift_report_id: str,
    ) -> dict[str, Any]:
        return {
            "paper_run_id": paper_run_id,
            "status": "RUNNING",
            "created_at": created_at,
            "experiment_id": experiment["experiment_id"],
            "backtest_run_id": backtest_run["run_id"],
            "source_window": source_window,
            "timeline_points": timeline_points,
            "input_fingerprints": {
                "backtest_run_id": backtest_run["run_id"],
                "backtest_artifact_sha256": backtest_run["artifact_sha256"],
                "backtest_assumptions_sha256": backtest_run["assumptions_sha256"],
                "config_sha256": experiment["config_sha256"],
                "history_sha256": history_sha256,
                "latest_dataset_id": latest_dataset_id,
                "engine_version": engine_version,
            },
            "reference_backtest_metrics": {
                "trade_count": backtest_run["artifact"]["metrics"]["trade_count"],
                "rejection_count": backtest_run["artifact"]["metrics"]["rejection_count"],
                "net_pnl_usd": backtest_run["artifact"]["metrics"]["net_pnl_usd"],
            },
            "metrics": paper_replay.metrics,
            "trades": paper_replay.trades,
            "rejections": paper_replay.rejections,
            "drift_report_id": drift_report_id,
        }

    def summarize_strategy_metrics(
        self,
        trades: list[dict[str, Any]],
        rejections: list[dict[str, Any]],
        assumptions: dict[str, Any],
        *,
        split_names: tuple[str, ...] = STRATEGY_REPLAY_SPLITS,
    ) -> dict[str, Any]:
        gross_pnl = sum(Decimal(item["gross_pnl_usd"]) for item in trades) if trades else Decimal("0")
        net_pnl = sum(Decimal(item["net_pnl_usd"]) for item in trades) if trades else Decimal("0")
        fees = sum(Decimal(item["fees_usd"]) for item in trades) if trades else Decimal("0")
        slippage = sum(Decimal(item["slippage_usd"]) for item in trades) if trades else Decimal("0")
        notional = sum(Decimal(item["filled_notional_usd"]) for item in trades) if trades else Decimal("0")
        split_metrics: dict[str, dict[str, Any]] = {}
        for split_name in split_names:
            split_trades = [item for item in trades if item["split"] == split_name]
            split_metrics[split_name] = {
                "trade_count": len(split_trades),
                "net_pnl_usd": _format_decimal(
                    sum(Decimal(item["net_pnl_usd"]) for item in split_trades)
                    if split_trades
                    else Decimal("0")
                ),
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

    def observe_paper_trades(
        self,
        candidate_trades: list[dict[str, Any]],
        assumptions: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        observed_trades: list[dict[str, Any]] = []
        missed_fills: list[dict[str, Any]] = []
        expected_fill_ratio = self._decimal_text(assumptions.get("partial_fill_ratio"))
        expected_slippage_bps = self._decimal_text(assumptions.get("slippage_bps"))
        fee_bps = self._decimal_text(assumptions.get("fee_bps"))

        for trade in candidate_trades:
            signal_strength = self._decimal_text(trade.get("signal_strength_bps")).copy_abs()
            direction_penalty = Decimal("0.03") if trade.get("direction") == "CONVERGENCE" else Decimal("0")
            observed_fill_ratio = max(
                Decimal("0.10"),
                min(
                    Decimal("1"),
                    expected_fill_ratio
                    - Decimal("0.04")
                    - direction_penalty
                    + min(signal_strength / Decimal("30000"), Decimal("0.06")),
                ),
            )
            if observed_fill_ratio < Decimal("0.15"):
                missed_fills.append(
                    {
                        "market_id": trade["market_id"],
                        "reason": "missed_fill",
                        "signal_index": trade["signal_index"],
                        "signal_time": trade["signal_time"],
                        "expected_fill_ratio": _format_decimal(expected_fill_ratio),
                        "observed_fill_ratio": _format_decimal(observed_fill_ratio),
                    }
                )
                continue

            observed_slippage_bps = (
                expected_slippage_bps
                + Decimal("1.5")
                + min(signal_strength / Decimal("1200"), Decimal("8"))
                + (Decimal("1.5") if trade.get("direction") == "CONVERGENCE" else Decimal("0"))
            )
            scaling = _safe_divide(observed_fill_ratio, self._decimal_text(trade["partial_fill_ratio"]))
            quantity = self._decimal_text(trade["quantity"]) * scaling
            observed_notional = self._decimal_text(trade["filled_notional_usd"]) * scaling
            entry_price = self._decimal_text(trade["entry_price"])
            exit_price = self._decimal_text(trade["exit_price"])
            slippage_delta_bps = observed_slippage_bps - expected_slippage_bps
            if trade["direction"] == "SHORT":
                adjusted_entry = entry_price * (Decimal("1") - (slippage_delta_bps / Decimal("10000")))
                gross_pnl = quantity * (adjusted_entry - exit_price)
                adverse_selection = max(
                    Decimal("0"),
                    ((exit_price - adjusted_entry) / max(adjusted_entry, Decimal("0.00000001"))) * Decimal("10000"),
                )
            else:
                adjusted_entry = entry_price * (Decimal("1") + (slippage_delta_bps / Decimal("10000")))
                gross_pnl = quantity * (exit_price - adjusted_entry)
                adverse_selection = max(
                    Decimal("0"),
                    ((adjusted_entry - exit_price) / max(adjusted_entry, Decimal("0.00000001"))) * Decimal("10000"),
                )

            fees = observed_notional * fee_bps / Decimal("10000")
            slippage = observed_notional * observed_slippage_bps / Decimal("10000")
            observed_trade = dict(trade)
            observed_trade.update(
                {
                    "split": STRATEGY_REPLAY_PAPER_SPLIT,
                    "quantity": _format_decimal(quantity),
                    "filled_notional_usd": _format_decimal(observed_notional),
                    "fees_usd": _format_decimal(fees),
                    "slippage_usd": _format_decimal(slippage),
                    "gross_pnl_usd": _format_decimal(gross_pnl),
                    "net_pnl_usd": _format_decimal(gross_pnl - fees - slippage),
                    "partial_fill_ratio": _format_decimal(observed_fill_ratio),
                    "expected_partial_fill_ratio": _format_decimal(expected_fill_ratio),
                    "expected_slippage_bps": _format_decimal(expected_slippage_bps),
                    "observed_slippage_bps": _format_decimal(observed_slippage_bps),
                    "adverse_selection_bps": _format_decimal(adverse_selection),
                }
            )
            observed_trades.append(observed_trade)

        return observed_trades, missed_fills

    def summarize_paper_metrics(
        self,
        *,
        candidate_trades: list[dict[str, Any]],
        trades: list[dict[str, Any]],
        rejections: list[dict[str, Any]],
    ) -> dict[str, Any]:
        trade_count = len(trades)
        gross_pnl = sum((self._decimal_text(item["gross_pnl_usd"]) for item in trades), Decimal("0"))
        net_pnl = sum((self._decimal_text(item["net_pnl_usd"]) for item in trades), Decimal("0"))
        fees = sum((self._decimal_text(item["fees_usd"]) for item in trades), Decimal("0"))
        slippage = sum((self._decimal_text(item["slippage_usd"]) for item in trades), Decimal("0"))
        notional = sum((self._decimal_text(item["filled_notional_usd"]) for item in trades), Decimal("0"))
        avg_fill_ratio = _safe_divide(
            sum((self._decimal_text(item["partial_fill_ratio"]) for item in trades), Decimal("0")),
            Decimal(trade_count),
        )
        avg_slippage_bps = _safe_divide(
            sum((self._decimal_text(item.get("observed_slippage_bps")) for item in trades), Decimal("0")),
            Decimal(trade_count),
        )
        avg_adverse_selection_bps = _safe_divide(
            sum((self._decimal_text(item.get("adverse_selection_bps")) for item in trades), Decimal("0")),
            Decimal(trade_count),
        )
        paper_days = Decimal("0")
        if trades:
            start_time = parse_datetime(trades[0]["signal_time"])
            end_time = parse_datetime(trades[-1]["exit_time"])
            if start_time is not None and end_time is not None and end_time >= start_time:
                paper_days = Decimal(str((end_time - start_time).total_seconds())) / Decimal("86400")

        return {
            "candidate_trade_count": len(candidate_trades),
            "trade_count": trade_count,
            "missed_fill_count": sum(1 for item in rejections if item["reason"] == "missed_fill"),
            "rejection_count": len(rejections),
            "gross_pnl_usd": _format_decimal(gross_pnl),
            "net_pnl_usd": _format_decimal(net_pnl),
            "fees_usd": _format_decimal(fees),
            "slippage_usd": _format_decimal(slippage),
            "filled_notional_usd": _format_decimal(notional),
            "avg_fill_ratio": _format_decimal(avg_fill_ratio),
            "avg_observed_slippage_bps": _format_decimal(avg_slippage_bps),
            "avg_adverse_selection_bps": _format_decimal(avg_adverse_selection_bps),
            "paper_days": _format_decimal(paper_days, places="0.0001"),
        }

    def build_paper_drift_report(
        self,
        *,
        experiment_id: str,
        paper_run_id: str,
        backtest_run_id: str,
        reference_assumptions: dict[str, Any],
        reference_metrics: dict[str, Any],
        paper_metrics: dict[str, Any],
        paper_rejections: list[dict[str, Any]],
        report_version: int,
        created_at: Optional[datetime] = None,
    ) -> dict[str, Any]:
        expected_fill_ratio = self._decimal_text(reference_assumptions.get("partial_fill_ratio"))
        expected_slippage_bps = self._decimal_text(reference_assumptions.get("slippage_bps"))
        observed_fill_ratio = self._decimal_text(paper_metrics["avg_fill_ratio"])
        observed_slippage_bps = self._decimal_text(paper_metrics["avg_observed_slippage_bps"])
        fill_ratio_delta = observed_fill_ratio - expected_fill_ratio
        slippage_delta_bps = observed_slippage_bps - expected_slippage_bps
        paper_trade_count = int(paper_metrics["trade_count"])
        paper_rejection_rate = _safe_divide(
            Decimal(len(paper_rejections)),
            Decimal(paper_trade_count + len(paper_rejections)),
        )
        backtest_trade_count = int(reference_metrics["trade_count"])
        backtest_rejection_count = int(reference_metrics["rejection_count"])
        backtest_rejection_rate = _safe_divide(
            Decimal(backtest_rejection_count),
            Decimal(backtest_trade_count + backtest_rejection_count),
        )
        checks = {
            "paper_trade_count_positive": {
                "passed": paper_trade_count > 0,
                "observed": paper_trade_count,
                "required": "> 0",
            },
            "paper_pnl_positive_after_fees": {
                "passed": self._decimal_text(paper_metrics["net_pnl_usd"]) > 0,
                "observed": paper_metrics["net_pnl_usd"],
                "required": "> 0",
            },
            "fill_model_error_below_threshold": {
                "passed": fill_ratio_delta.copy_abs() <= Decimal("0.12")
                and slippage_delta_bps.copy_abs() <= Decimal("12"),
                "observed": {
                    "fill_ratio_delta": _format_decimal(fill_ratio_delta),
                    "slippage_delta_bps": _format_decimal(slippage_delta_bps),
                },
                "required": {
                    "max_abs_fill_ratio_delta": "0.12",
                    "max_abs_slippage_delta_bps": "12",
                },
            },
            "adverse_selection_acceptable": {
                "passed": self._decimal_text(paper_metrics["avg_adverse_selection_bps"]) <= Decimal("35"),
                "observed": paper_metrics["avg_adverse_selection_bps"],
                "required": "<= 35",
            },
            "rejection_rate_drift_within_limit": {
                "passed": (paper_rejection_rate - backtest_rejection_rate).copy_abs() <= Decimal("0.25"),
                "observed": {
                    "paper_rejection_rate": _format_decimal(paper_rejection_rate),
                    "backtest_rejection_rate": _format_decimal(backtest_rejection_rate),
                },
                "required": "abs(delta) <= 0.25",
            },
        }
        failed_checks = [name for name, item in checks.items() if not item["passed"]]
        status = "ACCEPTABLE" if not failed_checks else "DRIFTED"
        report_id = self._build_paper_drift_report_id(
            experiment_id=experiment_id,
            paper_run_id=paper_run_id,
            backtest_run_id=backtest_run_id,
            fill_ratio_delta=_format_decimal(fill_ratio_delta),
            slippage_delta_bps=_format_decimal(slippage_delta_bps),
            report_version=report_version,
        )
        notes: list[str] = []
        if "paper_trade_count_positive" in failed_checks:
            notes.append("paper trading produced no executable fills")
        if "paper_pnl_positive_after_fees" in failed_checks:
            notes.append("paper PnL is non-positive after fees and slippage")
        if "fill_model_error_below_threshold" in failed_checks:
            notes.append("observed paper fill behavior drifted materially from backtest assumptions")
        if "adverse_selection_acceptable" in failed_checks:
            notes.append("paper fills experienced too much adverse selection")
        if "rejection_rate_drift_within_limit" in failed_checks:
            notes.append("paper rejection rate diverged too far from the reference backtest")

        return {
            "report_id": report_id,
            "created_at": format_datetime(created_at or utc_now()) or "",
            "status": status,
            "experiment_id": experiment_id,
            "paper_run_id": paper_run_id,
            "backtest_run_id": backtest_run_id,
            "reference": {
                "expected_partial_fill_ratio": _format_decimal(expected_fill_ratio),
                "expected_slippage_bps": _format_decimal(expected_slippage_bps),
                "backtest_trade_count": backtest_trade_count,
                "backtest_rejection_count": backtest_rejection_count,
                "backtest_net_pnl_usd": reference_metrics["net_pnl_usd"],
            },
            "paper_metrics": paper_metrics,
            "drift_metrics": {
                "fill_ratio_delta": _format_decimal(fill_ratio_delta),
                "slippage_delta_bps": _format_decimal(slippage_delta_bps),
                "paper_rejection_rate": _format_decimal(paper_rejection_rate),
                "backtest_rejection_rate": _format_decimal(backtest_rejection_rate),
            },
            "checks": checks,
            "failed_checks": failed_checks,
            "notes": notes,
        }

    def _load_histories(
        self,
        experiment: dict[str, Any],
        *,
        start_time: Optional[datetime],
        end_time: datetime,
        minimum_points: int,
        insufficient_history_message: str,
        post_resolution_message: str,
        validation_error: Callable[..., Exception],
    ) -> LoadedHistoryBatch:
        histories: dict[str, list[HistoryPoint]] = {}
        history_fingerprint_rows: list[dict[str, Any]] = []
        for market_id in self._market_ids_for_experiment(experiment, validation_error=validation_error):
            points: list[HistoryPoint] = []
            for history_point in self.market_history.get_market_metadata_history(
                market_id,
                start=start_time,
                end=end_time,
            ):
                recorded_at = history_point.recorded_at
                if start_time is not None and recorded_at <= start_time:
                    continue
                record = history_point.record
                market_end_time = parse_datetime(record.end_time)
                if market_end_time is not None and recorded_at > market_end_time:
                    raise validation_error(
                        post_resolution_message.format(
                            market_id=market_id,
                            recorded_at=format_datetime(recorded_at),
                        ),
                        code="post_resolution_data",
                        violations=[
                            {
                                "market_id": market_id,
                                "recorded_at": format_datetime(recorded_at),
                                "end_time": format_datetime(market_end_time),
                                "issue": "post_resolution_data",
                            }
                        ],
                    )
                point = HistoryPoint(
                    market_id=market_id,
                    timestamp=recorded_at,
                    end_time=market_end_time,
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
            if len(points) < minimum_points:
                raise validation_error(
                    insufficient_history_message.format(market_id=market_id),
                    code="insufficient_history",
                    violations=[{"market_id": market_id, "issue": "insufficient_history"}],
                )
            histories[market_id] = points

        timeline_points = min(len(points) for points in histories.values())
        history_sha256 = hashlib.sha256(_canonical_json(history_fingerprint_rows).encode("utf-8")).hexdigest()
        return LoadedHistoryBatch(
            histories=histories,
            timeline_points=timeline_points,
            history_sha256=history_sha256,
        )

    def _market_ids_for_experiment(
        self,
        experiment: dict[str, Any],
        *,
        validation_error: Callable[..., Exception],
    ) -> list[str]:
        config = experiment["config"]
        try:
            if experiment["strategy_family"] == "cross_market_arbitrage":
                market_ids = [_require_text("config.market_ids[]", item) for item in config["market_ids"]]
                return sorted(dict.fromkeys(market_ids))
            return [_require_text("config.market_id", config["market_id"])]
        except (KeyError, ValueError, TypeError) as exc:
            raise validation_error(str(exc), code="invalid_request") from exc

    def _price_proxy(self, record: NormalizedMarketRecord) -> Decimal:
        volume = self._decimal_text(record.volume)
        liquidity = self._decimal_text(record.liquidity)
        return (volume + Decimal("1")) / (volume + liquidity + Decimal("2"))

    def _decimal_text(self, value: Optional[str]) -> Decimal:
        if value in (None, ""):
            return Decimal("0")
        return Decimal(str(value))

    def _simulate_midpoint_reversion(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
        *,
        split_name_fn: Callable[[int, int], str],
        validation_error: Callable[..., Exception],
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
                    split=split_name_fn(signal_index, len(points)),
                    market_id=points[signal_index].market_id,
                    signal_index=signal_index,
                    signal_time=points[signal_index].timestamp,
                    entry_time=points[fill_index].timestamp,
                    exit_time=points[exit_index].timestamp,
                    direction=direction,
                    signal_strength_bps=self._return_bps(
                        points[max(signal_index - 1, 0)].price_proxy,
                        points[signal_index].price_proxy,
                    ),
                    entry_price=points[fill_index].price_proxy,
                    exit_price=points[exit_index].price_proxy,
                    notional_cap=Decimal(str(experiment["config"]["max_position_usd"])),
                    assumptions=assumptions,
                    leg_count=1,
                    validation_error=validation_error,
                )
            )

        return trades, rejections

    def _simulate_resolution_drift(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
        *,
        split_name_fn: Callable[[int, int], str],
        validation_error: Callable[..., Exception],
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
                    split=split_name_fn(signal_index, len(points)),
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
                    validation_error=validation_error,
                )
            )

        return trades, rejections

    def _simulate_cross_market_arbitrage(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
        *,
        split_name_fn: Callable[[int, int], str],
        validation_error: Callable[..., Exception],
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
                split=split_name_fn(signal_index, min_points),
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
                validation_error=validation_error,
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
        validation_error: Callable[..., Exception],
    ) -> dict[str, Any]:
        tick_size = Decimal(assumptions["tick_size"])
        quantity_step = Decimal("1").scaleb(-int(assumptions["quantity_precision_dp"]))
        partial_fill_ratio = Decimal(assumptions["partial_fill_ratio"])
        effective_notional = notional_cap * partial_fill_ratio
        quantity = _quantize_down(_safe_divide(effective_notional, max(entry_price, tick_size)), quantity_step)
        if quantity <= 0:
            raise validation_error(
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

    def _build_paper_drift_report_id(
        self,
        *,
        experiment_id: str,
        paper_run_id: str,
        backtest_run_id: str,
        fill_ratio_delta: str,
        slippage_delta_bps: str,
        report_version: int,
    ) -> str:
        basis = (
            f"{experiment_id}:{paper_run_id}:{backtest_run_id}:"
            f"{fill_ratio_delta}:{slippage_delta_bps}:{report_version}"
        )
        return f"drift-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"
