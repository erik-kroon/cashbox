from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

from ..backtests import BacktestService
from ..evaluator import EvaluatorService
from ..experiments import ExperimentService
from ..gateway import AgentMarketGateway
from ..ingest import FileSystemMarketStore
from ..models import parse_datetime
from ..paper import PaperService
from ..research import ResearchMarketReadPath
from ..risk import RiskGatewayService
from ..runtime import CashboxWorkspace, build_workspace

CommandHandler = Callable[["CLIContext", argparse.Namespace], int]


@dataclass
class CLIContext:
    parser: argparse.ArgumentParser
    root: Path
    workspace: CashboxWorkspace
    store: FileSystemMarketStore
    read_path: ResearchMarketReadPath
    gateway: AgentMarketGateway
    experiments: ExperimentService
    backtests: BacktestService
    evaluator: EvaluatorService
    paper: PaperService
    risk: RiskGatewayService

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
        store=workspace.market_store,
        read_path=workspace.read_path,
        gateway=workspace.gateway,
        experiments=workspace.experiments,
        backtests=workspace.backtests,
        evaluator=workspace.evaluator,
        paper=workspace.paper,
        risk=workspace.risk,
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
