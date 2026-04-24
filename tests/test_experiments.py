from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.experiments import (
    ExperimentFilter,
    ExperimentLifecycleError,
    ExperimentService,
    ExperimentValidationError,
    FileSystemExperimentStore,
)


class ExperimentServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.service = ExperimentService(FileSystemExperimentStore(self.root))

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_create_experiment_persists_definition_and_initial_status(self) -> None:
        experiment = self.service.create_experiment(
            hypothesis="Exploit temporary divergence between related election contracts.",
            strategy_family="cross_market_arbitrage",
            config={
                "market_ids": ["us-pres-2028", "senate-2028"],
                "max_spread_bps": 80,
                "min_edge_bps": 25,
                "rebalance_interval_seconds": 60,
                "max_position_usd": 500,
            },
            dataset_id="20260424T100000Z-abc123",
            code_version="git:abc1234",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(experiment["strategy_family"], "cross_market_arbitrage")
        self.assertEqual(experiment["current_status"], "DRAFT")
        self.assertEqual(experiment["status_history"][0]["to_status"], "DRAFT")
        self.assertTrue(
            self.root.joinpath("experiments", "definitions", f"{experiment['experiment_id']}.json").exists()
        )

    def test_validation_rejects_missing_and_unknown_fields(self) -> None:
        with self.assertRaises(ExperimentValidationError):
            self.service.validate_strategy_config(
                "midpoint_reversion",
                {
                    "market_id": "btc-150k",
                    "lookback_minutes": 30,
                    "entry_zscore": 2.0,
                    "max_position_usd": 200,
                },
            )

        with self.assertRaises(ExperimentValidationError):
            self.service.validate_strategy_config(
                "midpoint_reversion",
                {
                    "market_id": "btc-150k",
                    "lookback_minutes": 30,
                    "entry_zscore": 2.0,
                    "exit_zscore": 0.8,
                    "max_position_usd": 200,
                    "extra": True,
                },
            )

    def test_status_transitions_are_append_only_and_definition_remains_unchanged(self) -> None:
        experiment = self.service.create_experiment(
            hypothesis="Fade overnight overreactions before US market open.",
            strategy_family="midpoint_reversion",
            config={
                "market_id": "btc-150k",
                "lookback_minutes": 15,
                "entry_zscore": 2.0,
                "exit_zscore": 0.6,
                "max_position_usd": 250,
            },
            dataset_id="20260424T100000Z-abc123",
            code_version="git:def5678",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        original_config_sha256 = experiment["config_sha256"]

        self.service.transition_experiment_status(
            experiment["experiment_id"],
            to_status="VALIDATED_CONFIG",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )
        self.service.transition_experiment_status(
            experiment["experiment_id"],
            to_status="BACKTEST_QUEUED",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 10, 6, tzinfo=timezone.utc),
        )

        updated = self.service.get_experiment(experiment["experiment_id"])
        self.assertEqual(updated["config_sha256"], original_config_sha256)
        self.assertEqual(updated["current_status"], "BACKTEST_QUEUED")
        self.assertEqual([event["to_status"] for event in updated["status_history"]], ["DRAFT", "VALIDATED_CONFIG", "BACKTEST_QUEUED"])

        with self.assertRaises(ExperimentLifecycleError):
            self.service.transition_experiment_status(
                experiment["experiment_id"],
                to_status="PAPER_RUNNING",
                changed_by="evaluator",
            )

    def test_clone_and_notes_create_new_registry_entries(self) -> None:
        parent = self.service.create_experiment(
            hypothesis="Trade resolution drift in thin contracts.",
            strategy_family="resolution_drift",
            config={
                "market_id": "fed-cut-2026",
                "signal_window_minutes": 20,
                "entry_edge_bps": 18,
                "max_holding_minutes": 90,
                "max_position_usd": 150,
            },
            dataset_id="20260424T100000Z-abc123",
            code_version="git:1234567",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )

        clone = self.service.clone_experiment(
            parent["experiment_id"],
            {
                "hypothesis": "Trade resolution drift with wider entry threshold.",
                "config": {"entry_edge_bps": 24},
                "code_version": "git:89abcde",
            },
            generated_by="hermes",
            now=datetime(2026, 4, 24, 10, 10, tzinfo=timezone.utc),
        )
        note = self.service.attach_research_note(
            clone["experiment_id"],
            author="hermes",
            markdown="Widened entry threshold after false positives in prior week.",
            now=datetime(2026, 4, 24, 10, 12, tzinfo=timezone.utc),
        )

        fetched = self.service.get_experiment(clone["experiment_id"])
        listed = self.service.list_experiments(ExperimentFilter(strategy_family="resolution_drift"))

        self.assertEqual(clone["parent_experiment_id"], parent["experiment_id"])
        self.assertEqual(clone["config"]["entry_edge_bps"], 24)
        self.assertEqual(fetched["notes"][0]["note_id"], note["note_id"])
        self.assertEqual([item["experiment_id"] for item in listed], [clone["experiment_id"], parent["experiment_id"]])


if __name__ == "__main__":
    unittest.main()
