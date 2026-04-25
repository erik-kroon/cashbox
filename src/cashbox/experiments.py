from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
from pathlib import Path
from typing import Any, Optional

from .models import format_datetime, utc_now
from .persistence import append_jsonl, canonical_copy, canonical_json, read_json, read_jsonl, write_json

EXPERIMENT_STATUSES = (
    "DRAFT",
    "VALIDATED_CONFIG",
    "BACKTEST_QUEUED",
    "BACKTESTED",
    "WALK_FORWARD_TESTED",
    "PAPER_ELIGIBLE",
    "PAPER_RUNNING",
    "PAPER_PASSED",
    "TINY_LIVE_ELIGIBLE",
    "TINY_LIVE_RUNNING",
    "SCALE_REVIEW",
    "PRODUCTION_APPROVED",
    "DISABLED",
    "REJECTED",
    "RETIRED",
)
TERMINAL_EXPERIMENT_STATUSES = ("DISABLED", "REJECTED", "RETIRED")
BACKTEST_READY_EXPERIMENT_STATUSES = ("VALIDATED_CONFIG", "BACKTEST_QUEUED", "BACKTESTED")
PAPER_RUN_READY_EXPERIMENT_STATUSES = ("PAPER_ELIGIBLE", "PAPER_RUNNING", "PAPER_PASSED")
LIVE_TRADING_PERMITTED_EXPERIMENT_STATUSES = ("TINY_LIVE_ELIGIBLE", "TINY_LIVE_RUNNING", "PRODUCTION_APPROVED")

STRATEGY_TEMPLATES: dict[str, dict[str, Any]] = {
    "cross_market_arbitrage": {
        "description": "Exploit temporary price inconsistencies across related markets with capped exposure.",
        "config_schema_version": 1,
        "fields": {
            "market_ids": {"type": "list[string]", "required": True},
            "max_spread_bps": {"type": "number", "required": True, "minimum": 0},
            "min_edge_bps": {"type": "number", "required": True, "minimum": 0},
            "rebalance_interval_seconds": {"type": "integer", "required": True, "minimum": 1},
            "max_position_usd": {"type": "number", "required": True, "minimum": 0},
        },
    },
    "midpoint_reversion": {
        "description": "Fade short-horizon deviations from a rolling midpoint anchor on a single market.",
        "config_schema_version": 1,
        "fields": {
            "market_id": {"type": "string", "required": True},
            "lookback_minutes": {"type": "integer", "required": True, "minimum": 1},
            "entry_zscore": {"type": "number", "required": True, "minimum": 0},
            "exit_zscore": {"type": "number", "required": True, "minimum": 0},
            "max_position_usd": {"type": "number", "required": True, "minimum": 0},
        },
    },
    "resolution_drift": {
        "description": "Trade late-stage repricing around resolution-relevant information arrival.",
        "config_schema_version": 1,
        "fields": {
            "market_id": {"type": "string", "required": True},
            "signal_window_minutes": {"type": "integer", "required": True, "minimum": 1},
            "entry_edge_bps": {"type": "number", "required": True, "minimum": 0},
            "max_holding_minutes": {"type": "integer", "required": True, "minimum": 1},
            "max_position_usd": {"type": "number", "required": True, "minimum": 0},
        },
    },
}

def _require_text(name: str, value: str, *, max_length: int = 2000) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ExperimentValidationError(f"{name} must be non-empty")
    if len(normalized) > max_length:
        raise ExperimentValidationError(f"{name} exceeds max length {max_length}")
    return normalized


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_field(field_name: str, value: Any, spec: dict[str, Any]) -> None:
    expected_type = spec["type"]
    if expected_type == "string":
        if not isinstance(value, str) or not value.strip():
            raise ExperimentValidationError(f"config.{field_name} must be a non-empty string")
    elif expected_type == "integer":
        if not isinstance(value, int) or isinstance(value, bool):
            raise ExperimentValidationError(f"config.{field_name} must be an integer")
    elif expected_type == "number":
        if not _is_number(value):
            raise ExperimentValidationError(f"config.{field_name} must be numeric")
    elif expected_type == "list[string]":
        if not isinstance(value, list) or not value or any(not isinstance(item, str) or not item.strip() for item in value):
            raise ExperimentValidationError(f"config.{field_name} must be a non-empty list of strings")
    else:
        raise ExperimentValidationError(f"unsupported field type in template: {expected_type}")

    minimum = spec.get("minimum")
    if minimum is not None and value < minimum:
        raise ExperimentValidationError(f"config.{field_name} must be >= {minimum}")


def _merge_dicts(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    merged = canonical_copy(base)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = canonical_copy(value)
    return merged


class ExperimentServiceError(Exception):
    pass


class ExperimentNotFoundError(ExperimentServiceError):
    pass


class ExperimentLifecycleError(ExperimentServiceError):
    pass


class ExperimentValidationError(ExperimentServiceError):
    pass


@dataclass(frozen=True)
class ExperimentFilter:
    strategy_family: Optional[str] = None
    status: Optional[str] = None
    generated_by: Optional[str] = None
    dataset_id: Optional[str] = None
    limit: Optional[int] = None


@dataclass(frozen=True)
class ExperimentDefinition:
    experiment_id: str
    hypothesis: str
    strategy_family: str
    config: dict[str, Any]
    config_sha256: str
    config_schema_version: int
    dataset_id: str
    code_version: str
    generated_by: str
    parent_experiment_id: Optional[str]
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "hypothesis": self.hypothesis,
            "strategy_family": self.strategy_family,
            "config": canonical_copy(self.config),
            "config_sha256": self.config_sha256,
            "config_schema_version": self.config_schema_version,
            "dataset_id": self.dataset_id,
            "code_version": self.code_version,
            "generated_by": self.generated_by,
            "parent_experiment_id": self.parent_experiment_id,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ExperimentDefinition":
        return cls(
            experiment_id=str(payload["experiment_id"]),
            hypothesis=str(payload["hypothesis"]),
            strategy_family=str(payload["strategy_family"]),
            config=canonical_copy(payload["config"]),
            config_sha256=str(payload["config_sha256"]),
            config_schema_version=int(payload["config_schema_version"]),
            dataset_id=str(payload["dataset_id"]),
            code_version=str(payload["code_version"]),
            generated_by=str(payload["generated_by"]),
            parent_experiment_id=None
            if payload.get("parent_experiment_id") is None
            else str(payload["parent_experiment_id"]),
            created_at=str(payload["created_at"]),
        )


@dataclass(frozen=True)
class ExperimentStatusEvent:
    experiment_id: str
    from_status: Optional[str]
    to_status: str
    changed_by: str
    changed_at: str
    reason: Optional[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "from_status": self.from_status,
            "to_status": self.to_status,
            "changed_by": self.changed_by,
            "changed_at": self.changed_at,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ExperimentStatusEvent":
        return cls(
            experiment_id=str(payload["experiment_id"]),
            from_status=None if payload.get("from_status") is None else str(payload["from_status"]),
            to_status=str(payload["to_status"]),
            changed_by=str(payload["changed_by"]),
            changed_at=str(payload["changed_at"]),
            reason=None if payload.get("reason") is None else str(payload["reason"]),
        )


@dataclass(frozen=True)
class ExperimentResearchNote:
    note_id: str
    experiment_id: str
    author: str
    markdown: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "note_id": self.note_id,
            "experiment_id": self.experiment_id,
            "author": self.author,
            "markdown": self.markdown,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ExperimentResearchNote":
        return cls(
            note_id=str(payload["note_id"]),
            experiment_id=str(payload["experiment_id"]),
            author=str(payload["author"]),
            markdown=str(payload["markdown"]),
            created_at=str(payload["created_at"]),
        )


@dataclass
class FileSystemExperimentStore:
    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def experiments_dir(self) -> Path:
        return self.root / "experiments"

    @property
    def definitions_dir(self) -> Path:
        return self.experiments_dir / "definitions"

    @property
    def status_dir(self) -> Path:
        return self.experiments_dir / "status"

    @property
    def notes_dir(self) -> Path:
        return self.experiments_dir / "notes"

    def definition_path(self, experiment_id: str) -> Path:
        return self.definitions_dir / f"{experiment_id}.json"

    def status_path(self, experiment_id: str) -> Path:
        return self.status_dir / f"{experiment_id}.jsonl"

    def notes_path(self, experiment_id: str) -> Path:
        return self.notes_dir / f"{experiment_id}.jsonl"


class ExperimentService:
    def __init__(self, store: FileSystemExperimentStore) -> None:
        self.store = store

    def list_strategy_families(self) -> list[str]:
        return sorted(STRATEGY_TEMPLATES)

    def get_strategy_template(self, strategy_family: str) -> dict[str, Any]:
        family = _require_text("strategy_family", strategy_family, max_length=120)
        template = STRATEGY_TEMPLATES.get(family)
        if template is None:
            raise ExperimentValidationError(f"unknown strategy_family: {family}")
        return {
            "strategy_family": family,
            "description": template["description"],
            "config_schema_version": template["config_schema_version"],
            "fields": canonical_copy(template["fields"]),
        }

    def validate_strategy_config(self, strategy_family: str, config: dict[str, Any]) -> dict[str, Any]:
        family = _require_text("strategy_family", strategy_family, max_length=120)
        if not isinstance(config, dict):
            raise ExperimentValidationError("config must be a JSON object")

        template = STRATEGY_TEMPLATES.get(family)
        if template is None:
            raise ExperimentValidationError(f"unknown strategy_family: {family}")

        try:
            normalized_config = canonical_copy(config)
        except TypeError as exc:
            raise ExperimentValidationError(f"config must be JSON serializable: {exc}") from exc

        fields = template["fields"]
        missing = sorted(name for name, spec in fields.items() if spec.get("required") and name not in normalized_config)
        if missing:
            raise ExperimentValidationError(f"missing required config fields: {', '.join(missing)}")

        unknown = sorted(set(normalized_config) - set(fields))
        if unknown:
            raise ExperimentValidationError(f"unknown config fields: {', '.join(unknown)}")

        for name, value in normalized_config.items():
            _validate_field(name, value, fields[name])

        return {
            "ok": True,
            "strategy_family": family,
            "config_schema_version": template["config_schema_version"],
            "normalized_config": normalized_config,
        }

    def create_experiment(
        self,
        *,
        hypothesis: str,
        strategy_family: str,
        config: dict[str, Any],
        dataset_id: str,
        code_version: str,
        generated_by: str,
        parent_experiment_id: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        hypothesis_text = _require_text("hypothesis", hypothesis, max_length=4000)
        dataset = _require_text("dataset_id", dataset_id, max_length=200)
        code = _require_text("code_version", code_version, max_length=200)
        generated = _require_text("generated_by", generated_by, max_length=200)
        if parent_experiment_id is not None:
            self._load_definition(parent_experiment_id)

        validation = self.validate_strategy_config(strategy_family, config)
        timestamp = now or utc_now()
        created_at = format_datetime(timestamp) or ""
        config_payload = validation["normalized_config"]
        config_sha256 = hashlib.sha256(canonical_json(config_payload).encode("utf-8")).hexdigest()
        experiment_id = self._build_experiment_id(
            timestamp=timestamp,
            hypothesis=hypothesis_text,
            strategy_family=validation["strategy_family"],
            config_sha256=config_sha256,
        )
        definition = ExperimentDefinition(
            experiment_id=experiment_id,
            hypothesis=hypothesis_text,
            strategy_family=validation["strategy_family"],
            config=config_payload,
            config_sha256=config_sha256,
            config_schema_version=validation["config_schema_version"],
            dataset_id=dataset,
            code_version=code,
            generated_by=generated,
            parent_experiment_id=parent_experiment_id,
            created_at=created_at,
        )
        write_json(self.store.definition_path(experiment_id), definition.to_dict(), if_exists="error")
        self._append_status_event(
            ExperimentStatusEvent(
                experiment_id=experiment_id,
                from_status=None,
                to_status="DRAFT",
                changed_by=generated,
                changed_at=created_at,
                reason="experiment_created",
            )
        )
        return self.get_experiment(experiment_id)

    def clone_experiment(
        self,
        experiment_id: str,
        modifications: dict[str, Any],
        *,
        generated_by: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        base = self._load_definition(experiment_id)
        if not isinstance(modifications, dict):
            raise ExperimentValidationError("modifications must be a JSON object")

        config_updates = modifications.get("config", {})
        if not isinstance(config_updates, dict):
            raise ExperimentValidationError("modifications.config must be a JSON object")

        return self.create_experiment(
            hypothesis=str(modifications.get("hypothesis", base.hypothesis)),
            strategy_family=str(modifications.get("strategy_family", base.strategy_family)),
            config=_merge_dicts(base.config, config_updates),
            dataset_id=str(modifications.get("dataset_id", base.dataset_id)),
            code_version=str(modifications.get("code_version", base.code_version)),
            generated_by=generated_by,
            parent_experiment_id=base.experiment_id,
            now=now,
        )

    def attach_research_note(
        self,
        experiment_id: str,
        *,
        markdown: str,
        author: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        definition = self._load_definition(experiment_id)
        created_at = format_datetime(now or utc_now()) or ""
        note = ExperimentResearchNote(
            note_id=self._build_note_id(experiment_id, markdown, created_at),
            experiment_id=definition.experiment_id,
            author=_require_text("author", author, max_length=200),
            markdown=_require_text("markdown", markdown, max_length=20000),
            created_at=created_at,
        )
        append_jsonl(self.store.notes_path(definition.experiment_id), note.to_dict())
        return note.to_dict()

    def get_progression_state(self, experiment_id: str) -> dict[str, Any]:
        definition = self._load_definition(experiment_id)
        return self._progression_state_payload(definition.experiment_id)

    def record_backtest_completed(
        self,
        experiment_id: str,
        *,
        changed_by: str,
        reason: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        definition = self._load_definition(experiment_id)
        state = self._progression_state_payload(definition.experiment_id)
        current_status = state["current_status"]
        status_events: list[dict[str, Any]] = []

        if current_status == "VALIDATED_CONFIG":
            queued_event = self._transition_status(
                definition,
                target_status="BACKTEST_QUEUED",
                changed_by=changed_by,
                reason=reason,
                now=now,
            )
            status_events.append(queued_event.to_dict())
            current_status = queued_event.to_status

        if current_status == "BACKTEST_QUEUED":
            completed_event = self._transition_status(
                definition,
                target_status="BACKTESTED",
                changed_by=changed_by,
                reason=reason,
                now=now,
            )
            status_events.append(completed_event.to_dict())
            return {
                "experiment_id": definition.experiment_id,
                "operation": "backtest_completed",
                "previous_status": state["current_status"],
                "resulting_status": completed_event.to_status,
                "applied": True,
                "blockers": [],
                "status_events": status_events,
            }

        if current_status == "BACKTESTED":
            return {
                "experiment_id": definition.experiment_id,
                "operation": "backtest_completed",
                "previous_status": state["current_status"],
                "resulting_status": current_status,
                "applied": False,
                "blockers": [],
                "status_events": status_events,
            }

        raise ExperimentLifecycleError(f"experiment is not backtest-ready; got {state['current_status']}")

    def promote_to_paper_eligible(
        self,
        experiment_id: str,
        *,
        changed_by: str,
        reason: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        definition = self._load_definition(experiment_id)
        state = self._progression_state_payload(definition.experiment_id)
        current_status = state["current_status"]
        if current_status == "WALK_FORWARD_TESTED":
            event = self._transition_status(
                definition,
                target_status="PAPER_ELIGIBLE",
                changed_by=changed_by,
                reason=reason,
                now=now,
            )
            return {
                "experiment_id": definition.experiment_id,
                "operation": "paper_eligibility_promoted",
                "previous_status": current_status,
                "resulting_status": event.to_status,
                "applied": True,
                "blockers": [],
                "status_events": [event.to_dict()],
            }

        blockers = (
            ["experiment already in PAPER_ELIGIBLE"]
            if current_status == "PAPER_ELIGIBLE"
            else ["experiment must be WALK_FORWARD_TESTED before promotion to PAPER_ELIGIBLE"]
        )
        return {
            "experiment_id": definition.experiment_id,
            "operation": "paper_eligibility_promoted",
            "previous_status": current_status,
            "resulting_status": current_status,
            "applied": False,
            "blockers": blockers,
            "status_events": [],
        }

    def record_paper_run_started(
        self,
        experiment_id: str,
        *,
        changed_by: str,
        reason: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        definition = self._load_definition(experiment_id)
        state = self._progression_state_payload(definition.experiment_id)
        current_status = state["current_status"]
        if current_status == "PAPER_ELIGIBLE":
            event = self._transition_status(
                definition,
                target_status="PAPER_RUNNING",
                changed_by=changed_by,
                reason=reason,
                now=now,
            )
            return {
                "experiment_id": definition.experiment_id,
                "operation": "paper_run_started",
                "previous_status": current_status,
                "resulting_status": event.to_status,
                "applied": True,
                "blockers": [],
                "status_events": [event.to_dict()],
            }

        if current_status in {"PAPER_RUNNING", "PAPER_PASSED"}:
            return {
                "experiment_id": definition.experiment_id,
                "operation": "paper_run_started",
                "previous_status": current_status,
                "resulting_status": current_status,
                "applied": False,
                "blockers": [],
                "status_events": [],
            }

        raise ExperimentLifecycleError(f"experiment is not paper-run ready; got {current_status}")

    def record_paper_run_accepted(
        self,
        experiment_id: str,
        *,
        changed_by: str,
        reason: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        definition = self._load_definition(experiment_id)
        state = self._progression_state_payload(definition.experiment_id)
        current_status = state["current_status"]
        if current_status == "PAPER_RUNNING":
            event = self._transition_status(
                definition,
                target_status="PAPER_PASSED",
                changed_by=changed_by,
                reason=reason,
                now=now,
            )
            return {
                "experiment_id": definition.experiment_id,
                "operation": "paper_run_accepted",
                "previous_status": current_status,
                "resulting_status": event.to_status,
                "applied": True,
                "blockers": [],
                "status_events": [event.to_dict()],
            }

        blockers = (
            ["experiment already in PAPER_PASSED"]
            if current_status == "PAPER_PASSED"
            else ["experiment must be PAPER_RUNNING before paper completion promotion"]
        )
        return {
            "experiment_id": definition.experiment_id,
            "operation": "paper_run_accepted",
            "previous_status": current_status,
            "resulting_status": current_status,
            "applied": False,
            "blockers": blockers,
            "status_events": [],
        }

    def permits_live_trading(self, experiment_id: str) -> bool:
        return bool(self.get_progression_state(experiment_id)["permits_live_trading"])

    def transition_experiment_status(
        self,
        experiment_id: str,
        *,
        to_status: str,
        changed_by: str,
        reason: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        definition = self._load_definition(experiment_id)
        event = self._transition_status(
            definition,
            target_status=to_status,
            changed_by=changed_by,
            reason=reason,
            now=now,
        )
        return event.to_dict()

    def list_experiments(self, filters: Optional[ExperimentFilter] = None) -> list[dict[str, Any]]:
        requested_status = None if filters is None or filters.status is None else filters.status.strip()
        requested_family = None if filters is None or filters.strategy_family is None else filters.strategy_family.strip()
        requested_generated_by = (
            None if filters is None or filters.generated_by is None else filters.generated_by.strip()
        )
        requested_dataset = None if filters is None or filters.dataset_id is None else filters.dataset_id.strip()
        limit = None if filters is None else filters.limit

        summaries: list[dict[str, Any]] = []
        for path in sorted(self.store.definitions_dir.glob("*.json")):
            definition = ExperimentDefinition.from_dict(read_json(path))
            current_status = self._current_status(definition.experiment_id)
            if requested_status and current_status != requested_status:
                continue
            if requested_family and definition.strategy_family != requested_family:
                continue
            if requested_generated_by and definition.generated_by != requested_generated_by:
                continue
            if requested_dataset and definition.dataset_id != requested_dataset:
                continue
            notes_count = len(read_jsonl(self.store.notes_path(definition.experiment_id)))
            summaries.append(
                {
                    "experiment_id": definition.experiment_id,
                    "hypothesis": definition.hypothesis,
                    "strategy_family": definition.strategy_family,
                    "dataset_id": definition.dataset_id,
                    "code_version": definition.code_version,
                    "generated_by": definition.generated_by,
                    "created_at": definition.created_at,
                    "current_status": current_status,
                    "parent_experiment_id": definition.parent_experiment_id,
                    "config_sha256": definition.config_sha256,
                    "notes_count": notes_count,
                }
            )

        summaries.sort(key=lambda item: (item["created_at"], item["experiment_id"]), reverse=True)
        if limit is not None:
            return summaries[:limit]
        return summaries

    def get_experiment(self, experiment_id: str) -> dict[str, Any]:
        definition = self._load_definition(experiment_id)
        status_history = [event.to_dict() for event in self._load_status_history(experiment_id)]
        notes = [ExperimentResearchNote.from_dict(row).to_dict() for row in read_jsonl(self.store.notes_path(experiment_id))]
        payload = definition.to_dict()
        payload["current_status"] = status_history[-1]["to_status"] if status_history else None
        payload["status_history"] = status_history
        payload["notes"] = notes
        return payload

    def _append_status_event(self, event: ExperimentStatusEvent) -> None:
        append_jsonl(self.store.status_path(event.experiment_id), event.to_dict())

    def _progression_state_payload(self, experiment_id: str) -> dict[str, Any]:
        current_status = self._current_status(experiment_id)
        return {
            "experiment_id": experiment_id,
            "current_status": current_status,
            "is_terminal": current_status in TERMINAL_EXPERIMENT_STATUSES,
            "permits_backtest": current_status in BACKTEST_READY_EXPERIMENT_STATUSES,
            "permits_paper_run": current_status in PAPER_RUN_READY_EXPERIMENT_STATUSES,
            "permits_live_trading": current_status in LIVE_TRADING_PERMITTED_EXPERIMENT_STATUSES,
        }

    def _load_definition(self, experiment_id: str) -> ExperimentDefinition:
        path = self.store.definition_path(experiment_id)
        if not path.exists():
            raise ExperimentNotFoundError(f"unknown experiment_id: {experiment_id}")
        return ExperimentDefinition.from_dict(read_json(path))

    def _transition_status(
        self,
        definition: ExperimentDefinition,
        *,
        target_status: str,
        changed_by: str,
        reason: Optional[str],
        now: Optional[datetime],
    ) -> ExperimentStatusEvent:
        normalized_target_status = _require_text("status", target_status, max_length=120)
        actor = _require_text("changed_by", changed_by, max_length=200)
        cleaned_reason = None if reason is None else _require_text("reason", reason, max_length=2000)
        if normalized_target_status not in EXPERIMENT_STATUSES:
            raise ExperimentLifecycleError(f"unknown experiment status: {normalized_target_status}")

        history = self._load_status_history(definition.experiment_id)
        current_status = history[-1].to_status if history else None
        self._validate_transition(current_status, normalized_target_status)

        event = ExperimentStatusEvent(
            experiment_id=definition.experiment_id,
            from_status=current_status,
            to_status=normalized_target_status,
            changed_by=actor,
            changed_at=format_datetime(now or utc_now()) or "",
            reason=cleaned_reason,
        )
        self._append_status_event(event)
        return event

    def _load_status_history(self, experiment_id: str) -> list[ExperimentStatusEvent]:
        return [ExperimentStatusEvent.from_dict(row) for row in read_jsonl(self.store.status_path(experiment_id))]

    def _current_status(self, experiment_id: str) -> Optional[str]:
        history = self._load_status_history(experiment_id)
        return history[-1].to_status if history else None

    def _build_experiment_id(
        self,
        *,
        timestamp: datetime,
        hypothesis: str,
        strategy_family: str,
        config_sha256: str,
    ) -> str:
        basis = f"{format_datetime(timestamp)}:{hypothesis}:{strategy_family}:{config_sha256}"
        suffix = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:10]
        return f"exp-{timestamp.strftime('%Y%m%dT%H%M%SZ').lower()}-{suffix}"

    def _build_note_id(self, experiment_id: str, markdown: str, created_at: str) -> str:
        digest = hashlib.sha256(f"{experiment_id}:{created_at}:{markdown}".encode("utf-8")).hexdigest()
        return f"note-{digest[:10]}"

    def _validate_transition(self, current_status: Optional[str], target_status: str) -> None:
        if current_status == target_status:
            raise ExperimentLifecycleError(f"experiment already in status {target_status}")
        if current_status in TERMINAL_EXPERIMENT_STATUSES:
            raise ExperimentLifecycleError(f"cannot transition terminal experiment from {current_status}")
        if current_status is None:
            if target_status != "DRAFT":
                raise ExperimentLifecycleError("first experiment status must be DRAFT")
            return
        if target_status in TERMINAL_EXPERIMENT_STATUSES:
            return
        if target_status not in EXPERIMENT_STATUSES:
            raise ExperimentLifecycleError(f"unknown experiment status: {target_status}")
        current_index = EXPERIMENT_STATUSES.index(current_status)
        target_index = EXPERIMENT_STATUSES.index(target_status)
        if target_index != current_index + 1:
            raise ExperimentLifecycleError(f"invalid status transition from {current_status} to {target_status}")


def build_experiment_service(root: Path) -> ExperimentService:
    return ExperimentService(FileSystemExperimentStore(root))
