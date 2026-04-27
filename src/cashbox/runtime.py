from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .backtests import BacktestService, FileSystemBacktestStore
from .evaluator import EvaluatorService, FileSystemEvaluationStore
from .execution import ExecutionService, FileSystemExecutionStore
from .experiments import ExperimentService, FileSystemExperimentStore
from .gateway import AgentMarketGateway, FileSystemAgentGatewayStore
from .governance import FileSystemGovernanceStore, GovernanceService
from .health import SystemHealthService
from .ingest import FileSystemMarketStore
from .paper import FileSystemPaperStore, PaperService
from .research import ResearchMarketReadPath
from .risk import FileSystemRiskStore, RiskGatewayService


@dataclass(frozen=True)
class CashboxWorkspace:
    root: Path
    market_store: FileSystemMarketStore
    read_path: ResearchMarketReadPath
    experiments: ExperimentService
    backtests: BacktestService
    evaluator: EvaluatorService
    paper: PaperService
    risk: RiskGatewayService
    execution: ExecutionService
    governance: GovernanceService
    gateway: AgentMarketGateway
    health: SystemHealthService


def build_workspace(root: Path) -> CashboxWorkspace:
    root_path = Path(root)
    market_store = FileSystemMarketStore(root_path)
    read_path = ResearchMarketReadPath(market_store)
    experiments = ExperimentService(FileSystemExperimentStore(root_path))
    backtests = BacktestService(
        FileSystemBacktestStore(root_path),
        experiments=experiments,
        market_store=market_store,
    )
    evaluator = EvaluatorService(
        FileSystemEvaluationStore(root_path),
        experiments=experiments,
        backtests=backtests,
    )
    paper = PaperService(
        FileSystemPaperStore(root_path),
        experiments=experiments,
        backtests=backtests,
        market_store=market_store,
    )
    risk = RiskGatewayService(
        FileSystemRiskStore(root_path),
        experiments=experiments,
        market_store=market_store,
        read_path=read_path,
    )
    execution = ExecutionService(FileSystemExecutionStore(root_path), risk=risk)
    governance = GovernanceService(
        FileSystemGovernanceStore(root_path),
        experiments=experiments,
        execution=execution,
        risk=risk,
    )
    gateway = AgentMarketGateway(FileSystemAgentGatewayStore(root_path), read_path)
    health = SystemHealthService(
        read_path=read_path,
        gateway=gateway,
        experiments=experiments,
        backtests=backtests,
        paper=paper,
        execution=execution,
        governance=governance,
    )
    return CashboxWorkspace(
        root=root_path,
        market_store=market_store,
        read_path=read_path,
        experiments=experiments,
        backtests=backtests,
        evaluator=evaluator,
        paper=paper,
        risk=risk,
        execution=execution,
        governance=governance,
        gateway=gateway,
        health=health,
    )
