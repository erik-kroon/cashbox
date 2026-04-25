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

    def test_progression_methods_own_readiness_moves_and_live_permissions(self) -> None:
        experiment = self.service.create_experiment(
            hypothesis="Promote experiments through explicit progression moves.",
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

        initial_progression = self.service.get_progression_state(experiment["experiment_id"])
        self.assertFalse(initial_progression["permits_backtest"])
        self.assertFalse(initial_progression["permits_paper_run"])
        self.assertFalse(initial_progression["permits_live_trading"])

        self.service.transition_experiment_status(
            experiment["experiment_id"],
            to_status="VALIDATED_CONFIG",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )
        backtest_progression = self.service.record_backtest_completed(
            experiment["experiment_id"],
            changed_by="backtest-runner",
            reason="run_id=bt-123",
            now=datetime(2026, 4, 24, 10, 6, tzinfo=timezone.utc),
        )
        self.assertTrue(backtest_progression["applied"])
        self.assertEqual(backtest_progression["resulting_status"], "BACKTESTED")
        self.assertEqual(
            [event["to_status"] for event in backtest_progression["status_events"]],
            ["BACKTEST_QUEUED", "BACKTESTED"],
        )

        self.service.transition_experiment_status(
            experiment["experiment_id"],
            to_status="WALK_FORWARD_TESTED",
            changed_by="walk-forward-runner",
            now=datetime(2026, 4, 24, 10, 7, tzinfo=timezone.utc),
        )
        paper_gate = self.service.promote_to_paper_eligible(
            experiment["experiment_id"],
            changed_by="evaluator",
            reason="promotion_gate=paper score_id=score-123",
            now=datetime(2026, 4, 24, 10, 8, tzinfo=timezone.utc),
        )
        self.assertTrue(paper_gate["applied"])
        self.assertEqual(paper_gate["resulting_status"], "PAPER_ELIGIBLE")

        paper_started = self.service.record_paper_run_started(
            experiment["experiment_id"],
            changed_by="paper-runner",
            reason="paper_run_id=paper-123",
            now=datetime(2026, 4, 24, 10, 9, tzinfo=timezone.utc),
        )
        self.assertTrue(paper_started["applied"])
        self.assertEqual(paper_started["resulting_status"], "PAPER_RUNNING")

        paper_accepted = self.service.record_paper_run_accepted(
            experiment["experiment_id"],
            changed_by="paper-runner",
            reason="paper_run_id=paper-123",
            now=datetime(2026, 4, 24, 10, 10, tzinfo=timezone.utc),
        )
        self.assertTrue(paper_accepted["applied"])
        self.assertEqual(paper_accepted["resulting_status"], "PAPER_PASSED")

        self.service.transition_experiment_status(
            experiment["experiment_id"],
            to_status="TINY_LIVE_ELIGIBLE",
            changed_by="ops",
            now=datetime(2026, 4, 24, 10, 11, tzinfo=timezone.utc),
        )
        live_progression = self.service.get_progression_state(experiment["experiment_id"])
        self.assertTrue(live_progression["permits_live_trading"])
        self.assertTrue(self.service.permits_live_trading(experiment["experiment_id"]))

    def test_progression_methods_report_blockers_without_throwing_for_noop_promotions(self) -> None:
        experiment = self.service.create_experiment(
            hypothesis="Report centralized blockers for paper promotion.",
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

        blocked_promotion = self.service.promote_to_paper_eligible(
            experiment["experiment_id"],
            changed_by="evaluator",
        )
        self.assertFalse(blocked_promotion["applied"])
        self.assertEqual(
            blocked_promotion["blockers"],
            ["experiment must be WALK_FORWARD_TESTED before promotion to PAPER_ELIGIBLE"],
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
