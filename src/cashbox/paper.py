from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import hashlib
from pathlib import Path
from typing import Any, Optional

from .backtests import BacktestNotFoundError, BacktestService
from .experiments import ExperimentService
from .ingest import FileSystemMarketStore
from .models import format_datetime, parse_datetime, utc_now
from .persistence import canonical_json, read_json, write_json
from .strategy_replay import HistoryPoint, StrategyReplayService

PAPER_RUN_STATUSES = ("RUNNING", "STOPPED")
PAPER_DRIFT_STATUSES = ("ACCEPTABLE", "DRIFTED")
PAPER_ENGINE_VERSION = 1

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


def _replay_validation_error(message: str, **_: Any) -> "PaperValidationError":
    return PaperValidationError(message)


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
        self.replay = StrategyReplayService(market_store)

    def start_paper_strategy(
        self,
        experiment_id: str,
        *,
        run_id: Optional[str] = None,
        started_by: str = "paper-executor",
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        experiment = self.experiments.get_experiment(_require_text("experiment_id", experiment_id, max_length=120))
        progression = self.experiments.get_progression_state(experiment["experiment_id"])
        if not progression["permits_paper_run"]:
            raise PaperValidationError(
                f"experiment is not paper-run ready; got {progression['current_status']}"
            )

        actor = _require_text("started_by", started_by, max_length=200)
        backtest_run = self._resolve_successful_backtest(experiment["experiment_id"], run_id=run_id)
        latest_manifest = self.market_store.load_manifest()
        loaded_histories = self.replay.load_paper_histories(
            experiment,
            start_dataset_id=experiment["dataset_id"],
            end_dataset_id=latest_manifest.dataset_id,
            validation_error=_replay_validation_error,
        )
        paper_run_id = self._build_paper_run_id(
            experiment_id=experiment["experiment_id"],
            backtest_run_id=backtest_run["run_id"],
            latest_dataset_id=latest_manifest.dataset_id,
            history_sha256=loaded_histories.history_sha256,
        )
        if self.store.run_path(paper_run_id).exists() and self.store.result_path(paper_run_id).exists():
            return self._load_run(paper_run_id)

        created_at = format_datetime(now or utc_now()) or ""
        progression_result = self.experiments.record_paper_run_started(
            experiment["experiment_id"],
            changed_by=actor,
            reason=f"paper_run_id={paper_run_id}",
            now=now,
        )
        resulting_status = progression_result["resulting_status"]

        candidate_trades, strategy_rejections = self._simulate_candidate_trades(
            experiment,
            assumptions=backtest_run["artifact"]["assumptions"],
            histories=loaded_histories.histories,
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
            "source_window": loaded_histories.source_window,
            "timeline_points": loaded_histories.timeline_points,
            "input_fingerprints": {
                "backtest_run_id": backtest_run["run_id"],
                "backtest_artifact_sha256": backtest_run["artifact_sha256"],
                "backtest_assumptions_sha256": backtest_run["assumptions_sha256"],
                "config_sha256": experiment["config_sha256"],
                "history_sha256": loaded_histories.history_sha256,
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
            "history_sha256": loaded_histories.history_sha256,
            "artifact_sha256": hashlib.sha256(canonical_json(artifact).encode("utf-8")).hexdigest(),
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
        write_json(self.store.run_path(paper_run_id), run_payload)
        write_json(self.store.result_path(paper_run_id), artifact)
        write_json(self.store.drift_path(drift_report["report_id"]), drift_report)
        write_json(self.store.state_path(experiment["experiment_id"]), state_payload)
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
        run_payload = read_json(self.store.run_path(state["paper_run_id"]))
        stopped_at = format_datetime(now or utc_now()) or ""
        promotion_applied = False
        promotion_blockers: list[str] = []
        resulting_status = experiment["current_status"]

        if drift_report["status"] == "ACCEPTABLE":
            promotion = self.experiments.record_paper_run_accepted(
                experiment["experiment_id"],
                changed_by=actor,
                reason=f"paper_run_id={state['paper_run_id']}",
                now=now,
            )
            promotion_applied = bool(promotion["applied"])
            resulting_status = promotion["resulting_status"]
            promotion_blockers.extend(promotion["blockers"])
        else:
            promotion_blockers.append("paper drift report is not acceptable")

        state["lifecycle_status"] = "STOPPED"
        state["stopped_at"] = stopped_at
        state["experiment_status"] = resulting_status
        state["promotion_applied"] = promotion_applied
        state["promotion_blockers"] = promotion_blockers
        run_payload["status"] = "STOPPED"
        run_payload["stopped_at"] = stopped_at

        write_json(self.store.run_path(state["paper_run_id"]), run_payload)
        write_json(self.store.state_path(experiment["experiment_id"]), state)
        return state

    def get_paper_state(self, experiment_id: str) -> dict[str, Any]:
        normalized_experiment_id = _require_text("experiment_id", experiment_id, max_length=120)
        path = self.store.state_path(normalized_experiment_id)
        if not path.exists():
            raise PaperNotFoundError(f"no paper state found for experiment_id: {normalized_experiment_id}")
        return read_json(path)

    def get_paper_results(self, paper_run_id: str) -> dict[str, Any]:
        normalized_run_id = _require_text("paper_run_id", paper_run_id, max_length=160)
        path = self.store.result_path(normalized_run_id)
        if not path.exists():
            raise PaperNotFoundError(f"unknown paper_run_id: {normalized_run_id}")
        return read_json(path)

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
        payload = read_json(self.store.run_path(paper_run_id))
        payload["artifact"] = self.get_paper_results(paper_run_id)
        payload["drift_report"] = self._load_drift_report(payload["artifact"]["drift_report_id"])
        return payload

    def _load_drift_report(self, report_id: str) -> dict[str, Any]:
        normalized_report_id = _require_text("report_id", report_id, max_length=160)
        path = self.store.drift_path(normalized_report_id)
        if not path.exists():
            raise PaperNotFoundError(f"unknown drift report: {normalized_report_id}")
        return read_json(path)

    def _resolve_successful_backtest(self, experiment_id: str, *, run_id: Optional[str]) -> dict[str, Any]:
        if run_id is not None:
            return self._load_successful_backtest(run_id, experiment_id=experiment_id)

        candidates: list[dict[str, Any]] = []
        for path in sorted(self.backtests.store.runs_dir.glob("*.json")):
            payload = read_json(path)
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
        payload = read_json(run_path)
        if payload.get("experiment_id") != experiment_id:
            raise PaperValidationError(f"run_id {normalized_run_id} does not belong to experiment_id {experiment_id}")
        if payload.get("status") != "SUCCEEDED":
            raise PaperValidationError(f"run_id {normalized_run_id} must succeed before paper trading")
        payload["artifact"] = self.backtests.get_backtest_artifacts(normalized_run_id)
        return payload

    def _simulate_candidate_trades(
        self,
        experiment: dict[str, Any],
        *,
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        replay_result = self.replay.replay_strategy(
            experiment,
            assumptions,
            histories,
            split_name_fn=lambda _index, _total: "paper",
            validation_error=_replay_validation_error,
        )
        return replay_result.trades, replay_result.rejections

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

    def _replay_validation_error(
        self,
        message: str,
        *,
        code: str = "invalid_paper",
        violations: Optional[list[dict[str, Any]]] = None,
    ) -> PaperValidationError:
        del code, violations
        return PaperValidationError(message)

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
    from .runtime import build_workspace

    return build_workspace(root).paper
