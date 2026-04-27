from __future__ import annotations

import argparse
from pathlib import Path
import tempfile
import unittest

from cashbox.commands.base import build_context
from cashbox.runtime import build_workspace


class CashboxWorkspaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_build_workspace_shares_single_runtime_graph(self) -> None:
        workspace = build_workspace(self.root)

        self.assertEqual(workspace.root, self.root)
        market_research = workspace.market_research
        experiment_replay = workspace.experiment_replay
        execution_governance = workspace.execution_governance
        operator_evidence = workspace.operator_evidence

        self.assertIs(market_research.gateway.read_path, market_research.read_path)
        self.assertIs(experiment_replay.backtests.experiments, experiment_replay.experiments)
        self.assertIs(experiment_replay.backtests.market_store, market_research.market_store)
        self.assertIs(experiment_replay.evaluator.experiments, experiment_replay.experiments)
        self.assertIs(experiment_replay.evaluator.backtests, experiment_replay.backtests)
        self.assertIs(experiment_replay.paper.experiments, experiment_replay.experiments)
        self.assertIs(experiment_replay.paper.backtests, experiment_replay.backtests)
        self.assertIs(experiment_replay.paper.market_store, market_research.market_store)
        self.assertIs(execution_governance.risk.experiments, experiment_replay.experiments)
        self.assertIs(execution_governance.risk.market_store, market_research.market_store)
        self.assertIs(execution_governance.risk.read_path, market_research.read_path)
        self.assertIs(execution_governance.execution.risk, execution_governance.risk)
        self.assertIs(operator_evidence.audit.experiments, experiment_replay.experiments)
        self.assertIs(operator_evidence.audit.execution, execution_governance.execution)
        self.assertIs(operator_evidence.audit.risk, execution_governance.risk)
        self.assertIs(operator_evidence.evidence.audit, operator_evidence.audit)
        self.assertIs(operator_evidence.evidence.experiments, experiment_replay.experiments)
        self.assertIs(operator_evidence.evidence.backtests, experiment_replay.backtests)
        self.assertIs(operator_evidence.evidence.paper, experiment_replay.paper)
        self.assertIs(operator_evidence.evidence.execution, execution_governance.execution)
        self.assertIs(operator_evidence.evidence.governance, execution_governance.governance)
        self.assertIs(operator_evidence.health.read_path, market_research.read_path)
        self.assertIs(operator_evidence.health.evidence, operator_evidence.evidence)

    def test_cli_context_uses_single_workspace_runtime(self) -> None:
        context = build_context(root=self.root, parser=argparse.ArgumentParser())

        self.assertEqual(context.root, self.root)
        self.assertIs(context.workspace.market_research, context.market_research)
        self.assertIs(context.workspace.experiment_replay, context.experiment_replay)
        self.assertIs(context.workspace.execution_governance, context.execution_governance)
        self.assertIs(context.workspace.operator_evidence, context.operator_evidence)
        self.assertIs(context.market_research.market_store, context.store)
        self.assertIs(context.market_research.read_path, context.read_path)
        self.assertIs(context.market_research.gateway, context.gateway)
        self.assertIs(context.experiment_replay.experiments, context.experiments)
        self.assertIs(context.experiment_replay.backtests, context.backtests)
        self.assertIs(context.experiment_replay.evaluator, context.evaluator)
        self.assertIs(context.experiment_replay.paper, context.paper)
        self.assertIs(context.execution_governance.risk, context.risk)
        self.assertIs(context.execution_governance.execution, context.execution)
        self.assertIs(context.execution_governance.governance, context.governance)
        self.assertIs(context.operator_evidence.audit, context.audit)
        self.assertIs(context.operator_evidence.evidence, context.evidence)
        self.assertIs(context.operator_evidence.health, context.health)


if __name__ == "__main__":
    unittest.main()
