from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
from pathlib import Path
from typing import Any, Optional

from .experiments import ExperimentService
from .ingest import FileSystemMarketStore
from .models import format_datetime, utc_now
from .persistence import canonical_copy, canonical_json, read_json, write_json
from .strategy_replay import BacktestReplayRequest, STRATEGY_REPLAY_SIMULATION_LEVELS, StrategyReplayService

BACKTEST_RUN_STATUSES = ("SUCCEEDED", "FAILED")
BACKTEST_SIMULATION_LEVELS = STRATEGY_REPLAY_SIMULATION_LEVELS
BACKTEST_ENGINE_VERSION = 1


def _require_text(name: str, value: Any, *, max_length: int = 2000) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise BacktestValidationError(f"{name} must be non-empty", code="invalid_request")
    if len(normalized) > max_length:
        raise BacktestValidationError(f"{name} exceeds max length {max_length}", code="invalid_request")
    return normalized


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
            replay_artifacts = self.replay.run_replay(
                BacktestReplayRequest(
                    run_id=run_id,
                    created_at=created_at,
                    experiment=experiment,
                    manifest=manifest.to_dict(),
                    assumptions=raw_assumptions,
                    engine_version=BACKTEST_ENGINE_VERSION,
                    validation_error=_replay_validation_error,
                )
            )
            normalized_assumptions = replay_artifacts.assumptions
            artifact = replay_artifacts.artifact
            run_status = "SUCCEEDED"
            failure_code = None
            failure_message = None
            self._promote_experiment_to_backtested(experiment_id=experiment["experiment_id"], run_id=run_id, now=now)
        except BacktestValidationError as exc:
            normalized_assumptions = self.replay.best_effort_assumptions(raw_assumptions)
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
