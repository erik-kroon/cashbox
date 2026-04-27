from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
from pathlib import Path
from typing import Any, Optional

from .backtests import BacktestNotFoundError, BacktestService
from .experiments import ExperimentService
from .ingest import FileSystemMarketStore
from .models import format_datetime, utc_now
from .persistence import canonical_json, read_json, write_json
from .strategy_replay import PaperReplayRequest, StrategyReplayService

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
        created_at = format_datetime(now or utc_now()) or ""
        replay_artifacts = self.replay.run_replay(
            PaperReplayRequest(
                created_at=created_at,
                experiment=experiment,
                backtest_run=backtest_run,
                start_dataset_id=experiment["dataset_id"],
                end_dataset_id=latest_manifest.dataset_id,
                latest_dataset_id=latest_manifest.dataset_id,
                engine_version=PAPER_ENGINE_VERSION,
                paper_run_id_factory=lambda history_sha256: self._build_paper_run_id(
                    experiment_id=experiment["experiment_id"],
                    backtest_run_id=backtest_run["run_id"],
                    latest_dataset_id=latest_manifest.dataset_id,
                    history_sha256=history_sha256,
                ),
                validation_error=_replay_validation_error,
            )
        )
        paper_run_id = replay_artifacts.paper_run_id
        if self.store.run_path(paper_run_id).exists() and self.store.result_path(paper_run_id).exists():
            return self._load_run(paper_run_id)

        progression_result = self.experiments.record_paper_run_started(
            experiment["experiment_id"],
            changed_by=actor,
            reason=f"paper_run_id={paper_run_id}",
            now=now,
        )
        resulting_status = progression_result["resulting_status"]

        metrics = replay_artifacts.metrics
        drift_report = replay_artifacts.drift_report
        artifact = replay_artifacts.artifact
        run_payload = {
            "paper_run_id": paper_run_id,
            "experiment_id": experiment["experiment_id"],
            "backtest_run_id": backtest_run["run_id"],
            "status": "RUNNING",
            "created_at": created_at,
            "latest_dataset_id": latest_manifest.dataset_id,
            "history_sha256": replay_artifacts.history_sha256,
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

def build_paper_service(root: Path) -> PaperService:
    from .runtime import build_workspace

    return build_workspace(root).paper
