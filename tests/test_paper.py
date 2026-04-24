from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.backtests import BacktestService, FileSystemBacktestStore
from cashbox.experiments import ExperimentService, FileSystemExperimentStore
from cashbox.ingest import FileSystemMarketStore
from cashbox.paper import FileSystemPaperStore, PaperService, PaperValidationError


def _sample_market(
    *,
    market_id: str,
    question: str,
    volume: str,
    liquidity: str,
    end_time: str = "2026-11-05T00:00:00Z",
) -> dict[str, object]:
    return {
        "id": f"raw-{market_id}",
        "slug": market_id,
        "eventSlug": "us-election",
        "question": question,
        "category": "Crypto",
        "active": True,
        "closed": False,
        "archived": False,
        "enableOrderBook": True,
        "outcomes": json.dumps(["Yes", "No"]),
        "clobTokenIds": json.dumps([f"{market_id}-yes", f"{market_id}-no"]),
        "liquidity": liquidity,
        "volume": volume,
        "endDate": end_time,
    }


def _valid_assumptions() -> dict[str, object]:
    return {
        "simulation_level": "top_of_book",
        "fee_model_version": "fees-v1",
        "latency_model_version": "latency-v1",
        "slippage_model_version": "slippage-v1",
        "fill_model_version": "fills-v1",
        "tick_size": "0.01",
        "price_precision_dp": 4,
        "quantity_precision_dp": 4,
        "stale_book_threshold_seconds": 600,
        "fee_bps": 1,
        "slippage_bps": 1,
        "latency_seconds": 0,
        "partial_fill_ratio": "0.8",
        "split_method": "chronological",
        "train_ratio": "0.4",
        "validation_ratio": "0.2",
        "test_ratio": "0.4",
        "baseline": "hold",
    }


class PaperServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.market_store = FileSystemMarketStore(self.root)
        self.experiments = ExperimentService(FileSystemExperimentStore(self.root))
        self.backtests = BacktestService(
            FileSystemBacktestStore(self.root),
            experiments=self.experiments,
            market_store=self.market_store,
        )
        self.paper = PaperService(
            FileSystemPaperStore(self.root),
            experiments=self.experiments,
            backtests=self.backtests,
            market_store=self.market_store,
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_start_and_stop_paper_strategy_persists_results_and_drift(self) -> None:
        baseline_manifest = self._ingest_history(
            "btc-150k",
            start=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
            volumes=["100", "130", "70", "140", "75"],
            liquidity="100",
        )
        experiment = self.experiments.create_experiment(
            hypothesis="Fade deviations from the rolling proxy midpoint.",
            strategy_family="midpoint_reversion",
            config={
                "market_id": "btc-150k",
                "lookback_minutes": 2,
                "entry_zscore": 0.5,
                "exit_zscore": 0.2,
                "max_position_usd": 100,
            },
            dataset_id=baseline_manifest.dataset_id,
            code_version="git:paper001",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 10, 30, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="VALIDATED_CONFIG",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 10, 31, tzinfo=timezone.utc),
        )
        backtest_run = self.backtests.run_backtest(
            experiment["experiment_id"],
            assumptions=_valid_assumptions(),
            now=datetime(2026, 4, 24, 10, 32, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="WALK_FORWARD_TESTED",
            changed_by="walk-forward-runner",
            now=datetime(2026, 4, 24, 10, 32, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="PAPER_ELIGIBLE",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 10, 33, tzinfo=timezone.utc),
        )
        self._ingest_history(
            "btc-150k",
            start=datetime(2026, 4, 24, 10, 35, tzinfo=timezone.utc),
            volumes=["150", "60", "145", "55", "140", "50"],
            liquidity="100",
        )

        run = self.paper.start_paper_strategy(
            experiment["experiment_id"],
            run_id=backtest_run["run_id"],
            now=datetime(2026, 4, 24, 10, 50, tzinfo=timezone.utc),
        )

        self.assertEqual(run["status"], "RUNNING")
        self.assertGreaterEqual(run["artifact"]["metrics"]["trade_count"], 1)
        self.assertEqual(run["drift_report"]["status"], "ACCEPTABLE")
        self.assertEqual(
            self.experiments.get_experiment(experiment["experiment_id"])["current_status"],
            "PAPER_RUNNING",
        )

        state = self.paper.get_paper_state(experiment["experiment_id"])
        self.assertEqual(state["lifecycle_status"], "RUNNING")
        self.assertEqual(state["drift_status"], "ACCEPTABLE")

        drift = self.paper.analyze_paper_vs_backtest_drift(experiment["experiment_id"])
        self.assertEqual(drift["paper_run_id"], run["paper_run_id"])
        self.assertEqual(drift["failed_checks"], [])

        stopped = self.paper.stop_paper_strategy(
            experiment["experiment_id"],
            now=datetime(2026, 4, 24, 10, 55, tzinfo=timezone.utc),
        )
        self.assertEqual(stopped["lifecycle_status"], "STOPPED")
        self.assertTrue(stopped["promotion_applied"])
        self.assertEqual(stopped["experiment_status"], "PAPER_PASSED")
        self.assertEqual(
            self.experiments.get_experiment(experiment["experiment_id"])["current_status"],
            "PAPER_PASSED",
        )
        self.assertTrue(self.root.joinpath("paper", "runs", f"{run['paper_run_id']}.json").exists())
        self.assertTrue(self.root.joinpath("paper", "results", f"{run['paper_run_id']}.json").exists())
        self.assertTrue(self.root.joinpath("paper", "drift", f"{drift['report_id']}.json").exists())

    def test_start_paper_strategy_requires_paper_eligible_status(self) -> None:
        baseline_manifest = self._ingest_history(
            "btc-200k",
            start=datetime(2026, 4, 24, 11, 0, tzinfo=timezone.utc),
            volumes=["90", "120", "80", "130", "75"],
            liquidity="100",
        )
        experiment = self.experiments.create_experiment(
            hypothesis="Fade repeated midpoint dislocations.",
            strategy_family="midpoint_reversion",
            config={
                "market_id": "btc-200k",
                "lookback_minutes": 2,
                "entry_zscore": 0.5,
                "exit_zscore": 0.2,
                "max_position_usd": 100,
            },
            dataset_id=baseline_manifest.dataset_id,
            code_version="git:paper002",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 11, 20, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="VALIDATED_CONFIG",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 11, 21, tzinfo=timezone.utc),
        )
        backtest_run = self.backtests.run_backtest(
            experiment["experiment_id"],
            assumptions=_valid_assumptions(),
            now=datetime(2026, 4, 24, 11, 22, tzinfo=timezone.utc),
        )
        self._ingest_history(
            "btc-200k",
            start=datetime(2026, 4, 24, 11, 25, tzinfo=timezone.utc),
            volumes=["140", "60", "135", "55"],
            liquidity="100",
        )

        with self.assertRaises(PaperValidationError):
            self.paper.start_paper_strategy(
                experiment["experiment_id"],
                run_id=backtest_run["run_id"],
                now=datetime(2026, 4, 24, 11, 30, tzinfo=timezone.utc),
            )

    def _ingest_history(
        self,
        market_id: str,
        *,
        start: datetime,
        volumes: list[str],
        liquidity: str,
    ):
        manifests = []
        for index, volume in enumerate(volumes):
            manifests.append(
                self.market_store.ingest_market_payloads(
                    [
                        _sample_market(
                            market_id=market_id,
                            question=f"Will {market_id} move higher?",
                            volume=volume,
                            liquidity=liquidity,
                        )
                    ],
                    received_at=start + timedelta(minutes=5 * index),
                )
            )
        return manifests[-1]


if __name__ == "__main__":
    unittest.main()
