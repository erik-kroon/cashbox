from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .audit import AuditTrailService
from .backtests import BacktestService, FileSystemBacktestStore
from .evaluator import EvaluatorService, FileSystemEvaluationStore
from .execution import ExecutionService, FileSystemExecutionStore
from .experiments import ExperimentService, FileSystemExperimentStore
from .gateway import AgentMarketGateway, FileSystemAgentGatewayStore
from .governance import FileSystemGovernanceStore, GovernanceService
from .health import SystemHealthService
from .ingest import FileSystemMarketStore
from .operator_evidence import OperatorEvidenceService
from .paper import FileSystemPaperStore, PaperService
from .research import ResearchMarketReader, ResearchMarketReadPath
from .risk import FileSystemRiskStore, RiskGatewayService


@dataclass(frozen=True)
class MarketResearchModule:
    market_store: FileSystemMarketStore
    read_path: ResearchMarketReader
    gateway: AgentMarketGateway


@dataclass(frozen=True)
class ExperimentReplayModule:
    experiments: ExperimentService
    backtests: BacktestService
    evaluator: EvaluatorService
    paper: PaperService


@dataclass(frozen=True)
class ExecutionGovernanceModule:
    risk: RiskGatewayService
    execution: ExecutionService
    governance: GovernanceService


@dataclass(frozen=True)
class OperatorEvidenceModule:
    audit: AuditTrailService
    evidence: OperatorEvidenceService
    health: SystemHealthService


@dataclass(frozen=True)
class CashboxWorkspace:
    root: Path
    market_research: MarketResearchModule
    experiment_replay: ExperimentReplayModule
    execution_governance: ExecutionGovernanceModule
    operator_evidence: OperatorEvidenceModule

    @property
    def market_store(self) -> FileSystemMarketStore:
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


def build_market_research_module(root: Path) -> MarketResearchModule:
    root_path = Path(root)
    market_store = FileSystemMarketStore(root_path)
    read_path = ResearchMarketReadPath(market_store)
    gateway = AgentMarketGateway(FileSystemAgentGatewayStore(root_path), read_path)
    return MarketResearchModule(
        market_store=market_store,
        read_path=read_path,
        gateway=gateway,
    )


def build_experiment_replay_module(
    root: Path,
    *,
    market_research: MarketResearchModule,
) -> ExperimentReplayModule:
    root_path = Path(root)
    experiments = ExperimentService(FileSystemExperimentStore(root_path))
    backtests = BacktestService(
        FileSystemBacktestStore(root_path),
        experiments=experiments,
        market_store=market_research.market_store,
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
        market_store=market_research.market_store,
    )
    return ExperimentReplayModule(
        experiments=experiments,
        backtests=backtests,
        evaluator=evaluator,
        paper=paper,
    )


def build_execution_governance_module(
    root: Path,
    *,
    market_research: MarketResearchModule,
    experiment_replay: ExperimentReplayModule,
) -> ExecutionGovernanceModule:
    root_path = Path(root)
    risk = RiskGatewayService(
        FileSystemRiskStore(root_path),
        experiments=experiment_replay.experiments,
        market_store=market_research.market_store,
        read_path=market_research.read_path,
    )
    execution = ExecutionService(FileSystemExecutionStore(root_path), risk=risk)
    governance = GovernanceService(
        FileSystemGovernanceStore(root_path),
        experiments=experiment_replay.experiments,
        execution=execution,
        risk=risk,
    )
    return ExecutionGovernanceModule(
        risk=risk,
        execution=execution,
        governance=governance,
    )


def build_operator_evidence_module(
    root: Path,
    *,
    market_research: MarketResearchModule,
    experiment_replay: ExperimentReplayModule,
    execution_governance: ExecutionGovernanceModule,
) -> OperatorEvidenceModule:
    root_path = Path(root)
    audit = AuditTrailService(
        root_path,
        experiments=experiment_replay.experiments,
        execution=execution_governance.execution,
        risk=execution_governance.risk,
    )
    evidence = OperatorEvidenceService(
        experiments=experiment_replay.experiments,
        backtests=experiment_replay.backtests,
        paper=experiment_replay.paper,
        execution=execution_governance.execution,
        governance=execution_governance.governance,
        audit=audit,
    )
    health = SystemHealthService(
        read_path=market_research.read_path,
        evidence=evidence,
    )
    return OperatorEvidenceModule(
        audit=audit,
        evidence=evidence,
        health=health,
    )


def build_workspace(root: Path) -> CashboxWorkspace:
    root_path = Path(root)
    market_research = build_market_research_module(root_path)
    experiment_replay = build_experiment_replay_module(
        root_path,
        market_research=market_research,
    )
    execution_governance = build_execution_governance_module(
        root_path,
        market_research=market_research,
        experiment_replay=experiment_replay,
    )
    operator_evidence = build_operator_evidence_module(
        root_path,
        market_research=market_research,
        experiment_replay=experiment_replay,
        execution_governance=execution_governance,
    )
    return CashboxWorkspace(
        root=root_path,
        market_research=market_research,
        experiment_replay=experiment_replay,
        execution_governance=execution_governance,
        operator_evidence=operator_evidence,
    )
