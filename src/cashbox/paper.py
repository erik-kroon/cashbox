from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from .backtests import BacktestNotFoundError, BacktestService, HistoryPoint, build_backtest_service
from .experiments import ExperimentService, build_experiment_service
from .ingest import FileSystemMarketStore
from .models import NormalizedMarketRecord, format_datetime, parse_datetime, utc_now

PAPER_RUN_STATUSES = ("RUNNING", "STOPPED")
PAPER_DRIFT_STATUSES = ("ACCEPTABLE", "DRIFTED")
PAPER_ENGINE_VERSION = 1


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _require_text(name: str, value: Any, *, max_length: int = 2000) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise PaperValidationError(f"{name} must be non-empty")
    if len(normalized) > max_length:
        raise PaperValidationError(f"{name} exceeds max length {max_length}")
    return normalized


def _decimal_text(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _format_decimal(value: Decimal, *, places: str = "0.00000001") -> str:
    quantized = value.quantize(Decimal(places))
    return format(quantized.normalize(), "f")


def _safe_divide(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


class PaperServiceError(Exception):
    pass


class PaperNotFoundError(PaperServiceError):
    pass


class PaperValidationError(PaperServiceError):
    pass


@dataclass
class FileSystemPaperStore:
    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def paper_dir(self) -> Path:
        return self.root / "paper"

    @property
    def runs_dir(self) -> Path:
        return self.paper_dir / "runs"

    @property
    def results_dir(self) -> Path:
        return self.paper_dir / "results"

    @property
    def drift_dir(self) -> Path:
        return self.paper_dir / "drift"

    @property
    def state_dir(self) -> Path:
        return self.paper_dir / "state"

    def run_path(self, paper_run_id: str) -> Path:
        return self.runs_dir / f"{paper_run_id}.json"

    def result_path(self, paper_run_id: str) -> Path:
        return self.results_dir / f"{paper_run_id}.json"

    def drift_path(self, report_id: str) -> Path:
        return self.drift_dir / f"{report_id}.json"

    def state_path(self, experiment_id: str) -> Path:
        return self.state_dir / f"{experiment_id}.json"


class PaperService:
    def __init__(
        self,
        store: FileSystemPaperStore,
        *,
        experiments: ExperimentService,
        backtests: BacktestService,
        market_store: FileSystemMarketStore,
    ) -> None:
        self.store = store
        self.experiments = experiments
        self.backtests = backtests
        self.market_store = market_store

    def start_paper_strategy(
        self,
        experiment_id: str,
        *,
        run_id: Optional[str] = None,
        started_by: str = "paper-executor",
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        experiment = self.experiments.get_experiment(_require_text("experiment_id", experiment_id, max_length=120))
        if experiment["current_status"] not in {"PAPER_ELIGIBLE", "PAPER_RUNNING", "PAPER_PASSED"}:
            raise PaperValidationError(
                f"experiment must be PAPER_ELIGIBLE, PAPER_RUNNING, or PAPER_PASSED before paper trading; got {experiment['current_status']}"
            )

        actor = _require_text("started_by", started_by, max_length=200)
        backtest_run = self._resolve_successful_backtest(experiment["experiment_id"], run_id=run_id)
        latest_manifest = self.market_store.load_manifest()
        histories, timeline_points, history_sha256, source_window = self._load_future_histories(
            experiment,
            start_dataset_id=experiment["dataset_id"],
            end_dataset_id=latest_manifest.dataset_id,
        )
        paper_run_id = self._build_paper_run_id(
            experiment_id=experiment["experiment_id"],
            backtest_run_id=backtest_run["run_id"],
            latest_dataset_id=latest_manifest.dataset_id,
            history_sha256=history_sha256,
        )
        if self.store.run_path(paper_run_id).exists() and self.store.result_path(paper_run_id).exists():
            return self._load_run(paper_run_id)

        created_at = format_datetime(now or utc_now()) or ""
        resulting_status = experiment["current_status"]
        if experiment["current_status"] == "PAPER_ELIGIBLE":
            self.experiments.transition_experiment_status(
                experiment["experiment_id"],
                to_status="PAPER_RUNNING",
                changed_by=actor,
                reason=f"paper_run_id={paper_run_id}",
                now=now,
            )
            resulting_status = "PAPER_RUNNING"

        candidate_trades, strategy_rejections = self._simulate_candidate_trades(
            experiment,
            assumptions=backtest_run["artifact"]["assumptions"],
            histories=histories,
        )
        observed_trades, missed_fills = self._observe_candidate_trades(
            candidate_trades,
            assumptions=backtest_run["artifact"]["assumptions"],
        )
        all_rejections = strategy_rejections + missed_fills
        metrics = self._summarize_paper_metrics(
            candidate_trades=candidate_trades,
            trades=observed_trades,
            rejections=all_rejections,
        )
        drift_report = self._build_drift_report(
            experiment=experiment,
            paper_run_id=paper_run_id,
            backtest_run=backtest_run,
            paper_metrics=metrics,
            paper_rejections=all_rejections,
        )
        artifact = {
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
                "latest_dataset_id": latest_manifest.dataset_id,
                "engine_version": PAPER_ENGINE_VERSION,
            },
            "reference_backtest_metrics": {
                "trade_count": backtest_run["artifact"]["metrics"]["trade_count"],
                "rejection_count": backtest_run["artifact"]["metrics"]["rejection_count"],
                "net_pnl_usd": backtest_run["artifact"]["metrics"]["net_pnl_usd"],
            },
            "metrics": metrics,
            "trades": observed_trades,
            "rejections": all_rejections,
            "drift_report_id": drift_report["report_id"],
        }
        run_payload = {
            "paper_run_id": paper_run_id,
            "experiment_id": experiment["experiment_id"],
            "backtest_run_id": backtest_run["run_id"],
            "status": "RUNNING",
            "created_at": created_at,
            "latest_dataset_id": latest_manifest.dataset_id,
            "history_sha256": history_sha256,
            "artifact_sha256": hashlib.sha256(_canonical_json(artifact).encode("utf-8")).hexdigest(),
            "engine_version": PAPER_ENGINE_VERSION,
        }
        state_payload = {
            "experiment_id": experiment["experiment_id"],
            "paper_run_id": paper_run_id,
            "backtest_run_id": backtest_run["run_id"],
            "lifecycle_status": "RUNNING",
            "created_at": created_at,
            "stopped_at": None,
            "experiment_status": resulting_status,
            "drift_status": drift_report["status"],
            "metrics": metrics,
            "promotion_applied": False,
            "promotion_blockers": [],
        }
        _json_dump(self.store.run_path(paper_run_id), run_payload)
        _json_dump(self.store.result_path(paper_run_id), artifact)
        _json_dump(self.store.drift_path(drift_report["report_id"]), drift_report)
        _json_dump(self.store.state_path(experiment["experiment_id"]), state_payload)
        return self._load_run(paper_run_id)

    def stop_paper_strategy(
        self,
        experiment_id: str,
        *,
        stopped_by: str = "paper-executor",
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        actor = _require_text("stopped_by", stopped_by, max_length=200)
        state = self.get_paper_state(experiment_id)
        if state["lifecycle_status"] == "STOPPED":
            return state

        experiment = self.experiments.get_experiment(experiment_id)
        artifact = self.get_paper_results(state["paper_run_id"])
        drift_report = self._load_drift_report(artifact["drift_report_id"])
        run_payload = _json_load(self.store.run_path(state["paper_run_id"]))
        stopped_at = format_datetime(now or utc_now()) or ""
        promotion_applied = False
        promotion_blockers: list[str] = []
        resulting_status = experiment["current_status"]

        if drift_report["status"] == "ACCEPTABLE":
            if experiment["current_status"] == "PAPER_RUNNING":
                self.experiments.transition_experiment_status(
                    experiment["experiment_id"],
                    to_status="PAPER_PASSED",
                    changed_by=actor,
                    reason=f"paper_run_id={state['paper_run_id']}",
                    now=now,
                )
                promotion_applied = True
                resulting_status = "PAPER_PASSED"
            elif experiment["current_status"] == "PAPER_PASSED":
                promotion_blockers.append("experiment already in PAPER_PASSED")
                resulting_status = "PAPER_PASSED"
            else:
                promotion_blockers.append("experiment must be PAPER_RUNNING before paper completion promotion")
        else:
            promotion_blockers.append("paper drift report is not acceptable")

        state["lifecycle_status"] = "STOPPED"
        state["stopped_at"] = stopped_at
        state["experiment_status"] = resulting_status
        state["promotion_applied"] = promotion_applied
        state["promotion_blockers"] = promotion_blockers
        run_payload["status"] = "STOPPED"
        run_payload["stopped_at"] = stopped_at

        _json_dump(self.store.run_path(state["paper_run_id"]), run_payload)
        _json_dump(self.store.state_path(experiment["experiment_id"]), state)
        return state

    def get_paper_state(self, experiment_id: str) -> dict[str, Any]:
        normalized_experiment_id = _require_text("experiment_id", experiment_id, max_length=120)
        path = self.store.state_path(normalized_experiment_id)
        if not path.exists():
            raise PaperNotFoundError(f"no paper state found for experiment_id: {normalized_experiment_id}")
        return _json_load(path)

    def get_paper_results(self, paper_run_id: str) -> dict[str, Any]:
        normalized_run_id = _require_text("paper_run_id", paper_run_id, max_length=160)
        path = self.store.result_path(normalized_run_id)
        if not path.exists():
            raise PaperNotFoundError(f"unknown paper_run_id: {normalized_run_id}")
        return _json_load(path)

    def analyze_paper_vs_backtest_drift(
        self,
        experiment_id: str,
        *,
        paper_run_id: Optional[str] = None,
    ) -> dict[str, Any]:
        _require_text("experiment_id", experiment_id, max_length=120)
        resolved_paper_run_id = paper_run_id
        if resolved_paper_run_id is None:
            state = self.get_paper_state(experiment_id)
            resolved_paper_run_id = state["paper_run_id"]
        artifact = self.get_paper_results(resolved_paper_run_id)
        return self._load_drift_report(artifact["drift_report_id"])

    def _load_run(self, paper_run_id: str) -> dict[str, Any]:
        payload = _json_load(self.store.run_path(paper_run_id))
        payload["artifact"] = self.get_paper_results(paper_run_id)
        payload["drift_report"] = self._load_drift_report(payload["artifact"]["drift_report_id"])
        return payload

    def _load_drift_report(self, report_id: str) -> dict[str, Any]:
        normalized_report_id = _require_text("report_id", report_id, max_length=160)
        path = self.store.drift_path(normalized_report_id)
        if not path.exists():
            raise PaperNotFoundError(f"unknown drift report: {normalized_report_id}")
        return _json_load(path)

    def _resolve_successful_backtest(self, experiment_id: str, *, run_id: Optional[str]) -> dict[str, Any]:
        if run_id is not None:
            return self._load_successful_backtest(run_id, experiment_id=experiment_id)

        candidates: list[dict[str, Any]] = []
        for path in sorted(self.backtests.store.runs_dir.glob("*.json")):
            payload = _json_load(path)
            if payload.get("experiment_id") != experiment_id or payload.get("status") != "SUCCEEDED":
                continue
            candidates.append(payload)
        if not candidates:
            raise PaperValidationError(f"no successful backtest run found for experiment_id: {experiment_id}")
        candidates.sort(key=lambda item: (str(item.get("created_at", "")), str(item.get("run_id", ""))), reverse=True)
        return self._load_successful_backtest(str(candidates[0]["run_id"]), experiment_id=experiment_id)

    def _load_successful_backtest(self, run_id: str, *, experiment_id: str) -> dict[str, Any]:
        normalized_run_id = _require_text("run_id", run_id, max_length=160)
        run_path = self.backtests.store.run_path(normalized_run_id)
        if not run_path.exists():
            raise BacktestNotFoundError(f"unknown run_id: {normalized_run_id}")
        payload = _json_load(run_path)
        if payload.get("experiment_id") != experiment_id:
            raise PaperValidationError(f"run_id {normalized_run_id} does not belong to experiment_id {experiment_id}")
        if payload.get("status") != "SUCCEEDED":
            raise PaperValidationError(f"run_id {normalized_run_id} must succeed before paper trading")
        payload["artifact"] = self.backtests.get_backtest_artifacts(normalized_run_id)
        return payload

    def _load_future_histories(
        self,
        experiment: dict[str, Any],
        *,
        start_dataset_id: str,
        end_dataset_id: str,
    ) -> tuple[dict[str, list[HistoryPoint]], int, str, dict[str, Any]]:
        start_manifest = self.market_store.load_manifest(start_dataset_id)
        end_manifest = self.market_store.load_manifest(end_dataset_id)
        start_time = parse_datetime(start_manifest.ingested_at)
        end_time = parse_datetime(end_manifest.ingested_at)
        if start_time is None or end_time is None:
            raise PaperValidationError("paper trading requires versioned dataset timestamps")
        if end_time <= start_time:
            raise PaperValidationError("paper trading requires at least one newer dataset after the backtest dataset")

        histories: dict[str, list[HistoryPoint]] = {}
        history_fingerprint_rows: list[dict[str, Any]] = []
        market_ids = self.backtests._market_ids_for_experiment(experiment)
        for market_id in market_ids:
            points: list[HistoryPoint] = []
            for row in self.market_store.load_history(market_id):
                recorded_at = parse_datetime(row.get("recorded_at"))
                if recorded_at is None or recorded_at <= start_time or recorded_at > end_time:
                    continue
                record = NormalizedMarketRecord.from_dict(row["record"])
                market_end_time = parse_datetime(record.end_time)
                if market_end_time is not None and recorded_at > market_end_time:
                    raise PaperValidationError(
                        f"future history for {market_id} includes post-resolution data at {format_datetime(recorded_at)}"
                    )
                point = HistoryPoint(
                    market_id=market_id,
                    timestamp=recorded_at,
                    end_time=market_end_time,
                    price_proxy=self.backtests._price_proxy(record),
                    liquidity=self.backtests._decimal_text(record.liquidity),
                    volume=self.backtests._decimal_text(record.volume),
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
                raise PaperValidationError(
                    f"insufficient future history for market {market_id}; need at least 2 post-backtest points"
                )
            histories[market_id] = points

        timeline_points = min(len(points) for points in histories.values())
        history_sha256 = hashlib.sha256(_canonical_json(history_fingerprint_rows).encode("utf-8")).hexdigest()
        return (
            histories,
            timeline_points,
            history_sha256,
            {
                "start_dataset_id": start_dataset_id,
                "end_dataset_id": end_dataset_id,
                "start_at": start_manifest.ingested_at,
                "end_at": end_manifest.ingested_at,
            },
        )

    def _simulate_candidate_trades(
        self,
        experiment: dict[str, Any],
        *,
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if experiment["strategy_family"] == "midpoint_reversion":
            trades, rejections = self.backtests._simulate_midpoint_reversion(experiment, assumptions, histories)
        elif experiment["strategy_family"] == "resolution_drift":
            trades, rejections = self.backtests._simulate_resolution_drift(experiment, assumptions, histories)
        elif experiment["strategy_family"] == "cross_market_arbitrage":
            trades, rejections = self.backtests._simulate_cross_market_arbitrage(experiment, assumptions, histories)
        else:
            raise PaperValidationError(f"unsupported strategy_family: {experiment['strategy_family']}")

        for trade in trades:
            trade["split"] = "paper"
        return trades, rejections

    def _observe_candidate_trades(
        self,
        candidate_trades: list[dict[str, Any]],
        *,
        assumptions: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        observed_trades: list[dict[str, Any]] = []
        missed_fills: list[dict[str, Any]] = []
        expected_fill_ratio = _decimal_text(assumptions.get("partial_fill_ratio"))
        expected_slippage_bps = _decimal_text(assumptions.get("slippage_bps"))
        fee_bps = _decimal_text(assumptions.get("fee_bps"))

        for trade in candidate_trades:
            signal_strength = _decimal_text(trade.get("signal_strength_bps")).copy_abs()
            direction_penalty = Decimal("0.03") if trade.get("direction") == "CONVERGENCE" else Decimal("0")
            observed_fill_ratio = max(
                Decimal("0.10"),
                min(
                    Decimal("1"),
                    expected_fill_ratio - Decimal("0.04") - direction_penalty + min(signal_strength / Decimal("30000"), Decimal("0.06")),
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
            scaling = _safe_divide(observed_fill_ratio, _decimal_text(trade["partial_fill_ratio"]))
            quantity = _decimal_text(trade["quantity"]) * scaling
            observed_notional = _decimal_text(trade["filled_notional_usd"]) * scaling
            entry_price = _decimal_text(trade["entry_price"])
            exit_price = _decimal_text(trade["exit_price"])
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
                    "split": "paper",
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

    def _summarize_paper_metrics(
        self,
        *,
        candidate_trades: list[dict[str, Any]],
        trades: list[dict[str, Any]],
        rejections: list[dict[str, Any]],
    ) -> dict[str, Any]:
        trade_count = len(trades)
        gross_pnl = sum((_decimal_text(item["gross_pnl_usd"]) for item in trades), Decimal("0"))
        net_pnl = sum((_decimal_text(item["net_pnl_usd"]) for item in trades), Decimal("0"))
        fees = sum((_decimal_text(item["fees_usd"]) for item in trades), Decimal("0"))
        slippage = sum((_decimal_text(item["slippage_usd"]) for item in trades), Decimal("0"))
        notional = sum((_decimal_text(item["filled_notional_usd"]) for item in trades), Decimal("0"))
        avg_fill_ratio = _safe_divide(
            sum((_decimal_text(item["partial_fill_ratio"]) for item in trades), Decimal("0")),
            Decimal(trade_count),
        )
        avg_slippage_bps = _safe_divide(
            sum((_decimal_text(item.get("observed_slippage_bps")) for item in trades), Decimal("0")),
            Decimal(trade_count),
        )
        avg_adverse_selection_bps = _safe_divide(
            sum((_decimal_text(item.get("adverse_selection_bps")) for item in trades), Decimal("0")),
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

    def _build_drift_report(
        self,
        *,
        experiment: dict[str, Any],
        paper_run_id: str,
        backtest_run: dict[str, Any],
        paper_metrics: dict[str, Any],
        paper_rejections: list[dict[str, Any]],
    ) -> dict[str, Any]:
        expected_fill_ratio = _decimal_text(backtest_run["artifact"]["assumptions"].get("partial_fill_ratio"))
        expected_slippage_bps = _decimal_text(backtest_run["artifact"]["assumptions"].get("slippage_bps"))
        observed_fill_ratio = _decimal_text(paper_metrics["avg_fill_ratio"])
        observed_slippage_bps = _decimal_text(paper_metrics["avg_observed_slippage_bps"])
        fill_ratio_delta = observed_fill_ratio - expected_fill_ratio
        slippage_delta_bps = observed_slippage_bps - expected_slippage_bps
        paper_trade_count = int(paper_metrics["trade_count"])
        paper_rejection_rate = _safe_divide(Decimal(len(paper_rejections)), Decimal(paper_trade_count + len(paper_rejections)))
        backtest_trade_count = int(backtest_run["artifact"]["metrics"]["trade_count"])
        backtest_rejection_count = int(backtest_run["artifact"]["metrics"]["rejection_count"])
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
                "passed": _decimal_text(paper_metrics["net_pnl_usd"]) > 0,
                "observed": paper_metrics["net_pnl_usd"],
                "required": "> 0",
            },
            "fill_model_error_below_threshold": {
                "passed": fill_ratio_delta.copy_abs() <= Decimal("0.12") and slippage_delta_bps.copy_abs() <= Decimal("12"),
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
                "passed": _decimal_text(paper_metrics["avg_adverse_selection_bps"]) <= Decimal("35"),
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
        report_id = self._build_report_id(
            experiment_id=experiment["experiment_id"],
            paper_run_id=paper_run_id,
            backtest_run_id=backtest_run["run_id"],
            fill_ratio_delta=_format_decimal(fill_ratio_delta),
            slippage_delta_bps=_format_decimal(slippage_delta_bps),
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
            "created_at": format_datetime(utc_now()) or "",
            "status": status,
            "experiment_id": experiment["experiment_id"],
            "paper_run_id": paper_run_id,
            "backtest_run_id": backtest_run["run_id"],
            "reference": {
                "expected_partial_fill_ratio": _format_decimal(expected_fill_ratio),
                "expected_slippage_bps": _format_decimal(expected_slippage_bps),
                "backtest_trade_count": backtest_trade_count,
                "backtest_rejection_count": backtest_rejection_count,
                "backtest_net_pnl_usd": backtest_run["artifact"]["metrics"]["net_pnl_usd"],
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

    def _build_paper_run_id(
        self,
        *,
        experiment_id: str,
        backtest_run_id: str,
        latest_dataset_id: str,
        history_sha256: str,
    ) -> str:
        basis = (
            f"{experiment_id}:{backtest_run_id}:{latest_dataset_id}:{history_sha256}:{PAPER_ENGINE_VERSION}"
        )
        return f"paper-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"

    def _build_report_id(
        self,
        *,
        experiment_id: str,
        paper_run_id: str,
        backtest_run_id: str,
        fill_ratio_delta: str,
        slippage_delta_bps: str,
    ) -> str:
        basis = (
            f"{experiment_id}:{paper_run_id}:{backtest_run_id}:{fill_ratio_delta}:{slippage_delta_bps}:{PAPER_ENGINE_VERSION}"
        )
        return f"drift-{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:12]}"


def build_paper_service(root: Path) -> PaperService:
    return PaperService(
        FileSystemPaperStore(root),
        experiments=build_experiment_service(root),
        backtests=build_backtest_service(root),
        market_store=FileSystemMarketStore(root),
    )
