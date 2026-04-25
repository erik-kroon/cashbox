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
        self.assertIs(workspace.backtests.experiments, workspace.experiments)
        self.assertIs(workspace.backtests.market_store, workspace.market_store)
        self.assertIs(workspace.evaluator.experiments, workspace.experiments)
        self.assertIs(workspace.evaluator.backtests, workspace.backtests)
        self.assertIs(workspace.paper.experiments, workspace.experiments)
        self.assertIs(workspace.paper.backtests, workspace.backtests)
        self.assertIs(workspace.paper.market_store, workspace.market_store)
        self.assertIs(workspace.risk.experiments, workspace.experiments)
        self.assertIs(workspace.risk.market_store, workspace.market_store)
        self.assertIs(workspace.risk.read_path, workspace.read_path)
        self.assertIs(workspace.gateway.read_path, workspace.read_path)

    def test_cli_context_uses_single_workspace_runtime(self) -> None:
        context = build_context(root=self.root, parser=argparse.ArgumentParser())

        self.assertEqual(context.root, self.root)
        self.assertIs(context.workspace.market_store, context.store)
        self.assertIs(context.workspace.read_path, context.read_path)
        self.assertIs(context.workspace.experiments, context.experiments)
        self.assertIs(context.workspace.backtests, context.backtests)
        self.assertIs(context.workspace.evaluator, context.evaluator)
        self.assertIs(context.workspace.paper, context.paper)
        self.assertIs(context.workspace.risk, context.risk)
        self.assertIs(context.workspace.gateway, context.gateway)


if __name__ == "__main__":
    unittest.main()
