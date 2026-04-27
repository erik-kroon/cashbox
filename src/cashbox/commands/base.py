from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

from ..audit import AuditTrailService
from ..backtests import BacktestService
from ..evaluator import EvaluatorService
from ..execution import ExecutionService
from ..experiments import ExperimentService
from ..gateway import AgentMarketGateway
from ..governance import GovernanceService
from ..health import SystemHealthService
from ..ingest import FileSystemMarketStore
from ..models import parse_datetime
from ..operator_evidence import OperatorEvidenceService
from ..paper import PaperService
from ..research import ResearchMarketReader
from ..risk import RiskGatewayService
from ..runtime import (
    CashboxWorkspace,
    ExecutionGovernanceModule,
    ExperimentReplayModule,
    MarketResearchModule,
    OperatorEvidenceModule,
    build_workspace,
)

CommandHandler = Callable[["CLIContext", argparse.Namespace], int]


@dataclass
class CLIContext:
    parser: argparse.ArgumentParser
    root: Path
    workspace: CashboxWorkspace
    market_research: MarketResearchModule
    experiment_replay: ExperimentReplayModule
    execution_governance: ExecutionGovernanceModule
    operator_evidence: OperatorEvidenceModule

    @property
    def store(self) -> FileSystemMarketStore:
        return self.market_research.market_store

    @property
    def read_path(self) -> ResearchMarketReader:
        return self.market_research.read_path

    @property
    def gateway(self) -> AgentMarketGateway:
        return self.market_research.gateway

    @property
    def experiments(self) -> ExperimentService:
        return self.experiment_replay.experiments

    @property
    def backtests(self) -> BacktestService:
        return self.experiment_replay.backtests

    @property
    def evaluator(self) -> EvaluatorService:
        return self.experiment_replay.evaluator

    @property
    def paper(self) -> PaperService:
        return self.experiment_replay.paper

    @property
    def risk(self) -> RiskGatewayService:
        return self.execution_governance.risk

    @property
    def execution(self) -> ExecutionService:
        return self.execution_governance.execution

    @property
    def governance(self) -> GovernanceService:
        return self.execution_governance.governance

    @property
    def audit(self) -> AuditTrailService:
        return self.operator_evidence.audit

    @property
    def evidence(self) -> OperatorEvidenceService:
        return self.operator_evidence.evidence

    @property
    def health(self) -> SystemHealthService:
        return self.operator_evidence.health

    def emit(self, payload: Any) -> int:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    def fail(self, message: str) -> None:
        self.parser.error(message)


def build_context(*, root: Path, parser: argparse.ArgumentParser) -> CLIContext:
    workspace = build_workspace(root)
    return CLIContext(
        parser=parser,
        root=workspace.root,
        workspace=workspace,
        market_research=workspace.market_research,
        experiment_replay=workspace.experiment_replay,
        execution_governance=workspace.execution_governance,
        operator_evidence=workspace.operator_evidence,
    )


def register_command(
    subparsers: Any,
    *,
    name: str,
    help_text: str,
    handler: CommandHandler,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(name, help=help_text)
    parser.set_defaults(_cashbox_handler=handler)
    return parser


def parse_json_argument(context: CLIContext, raw: str, *, flag_name: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        context.fail(f"invalid {flag_name} payload: {exc}")
        raise AssertionError("unreachable")


def parse_duration_seconds(value: int) -> timedelta:
    return timedelta(seconds=value)


__all__ = [
    "CLIContext",
    "CommandHandler",
    "build_context",
    "parse_datetime",
    "parse_duration_seconds",
    "parse_json_argument",
    "register_command",
]
