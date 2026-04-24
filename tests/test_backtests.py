from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.backtests import BacktestService, FileSystemBacktestStore
from cashbox.experiments import ExperimentService, FileSystemExperimentStore
from cashbox.ingest import FileSystemMarketStore


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
        "fee_bps": 10,
        "slippage_bps": 5,
        "latency_seconds": 0,
        "partial_fill_ratio": "0.75",
        "split_method": "chronological",
        "train_ratio": "0.6",
        "validation_ratio": "0.2",
        "test_ratio": "0.2",
        "baseline": "hold",
    }


class BacktestServiceTests(unittest.TestCase):
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

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_run_backtest_is_repeatable_and_persists_artifacts(self) -> None:
        latest_manifest = self._ingest_midpoint_history("btc-150k")
        experiment = self.experiments.create_experiment(
            hypothesis="Fade deviations from the rolling proxy midpoint.",
            strategy_family="midpoint_reversion",
            config={
                "market_id": "btc-150k",
                "lookback_minutes": 2,
                "entry_zscore": 0.5,
                "exit_zscore": 0.2,
                "max_position_usd": 250,
            },
            dataset_id=latest_manifest.dataset_id,
            code_version="git:backtest001",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 10, 25, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="VALIDATED_CONFIG",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 10, 26, tzinfo=timezone.utc),
        )

        first = self.backtests.run_backtest(
            experiment["experiment_id"],
            assumptions=_valid_assumptions(),
            now=datetime(2026, 4, 24, 10, 27, tzinfo=timezone.utc),
        )
        second = self.backtests.run_backtest(
            experiment["experiment_id"],
            assumptions=_valid_assumptions(),
            now=datetime(2026, 4, 24, 10, 28, tzinfo=timezone.utc),
        )

        self.assertEqual(first["run_id"], second["run_id"])
        self.assertEqual(first["status"], "SUCCEEDED")
        self.assertGreaterEqual(first["artifact"]["metrics"]["trade_count"], 1)
        self.assertEqual(
            self.experiments.get_experiment(experiment["experiment_id"])["current_status"],
            "BACKTESTED",
        )

        artifact = self.backtests.get_backtest_artifacts(first["run_id"])
        self.assertEqual(artifact["run_id"], first["run_id"])
        self.assertEqual(artifact["status"], "SUCCEEDED")

    def test_run_backtest_persists_deterministic_failure_for_non_chronological_split(self) -> None:
        latest_manifest = self._ingest_midpoint_history("btc-150k")
        experiment = self.experiments.create_experiment(
            hypothesis="Use deterministic gating for midpoint trades.",
            strategy_family="midpoint_reversion",
            config={
                "market_id": "btc-150k",
                "lookback_minutes": 2,
                "entry_zscore": 0.5,
                "exit_zscore": 0.2,
                "max_position_usd": 250,
            },
            dataset_id=latest_manifest.dataset_id,
            code_version="git:backtest002",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 10, 25, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="VALIDATED_CONFIG",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 10, 26, tzinfo=timezone.utc),
        )

        assumptions = _valid_assumptions()
        assumptions["split_method"] = "random"
        result = self.backtests.run_backtest(
            experiment["experiment_id"],
            assumptions=assumptions,
            now=datetime(2026, 4, 24, 10, 27, tzinfo=timezone.utc),
        )

        self.assertEqual(result["status"], "FAILED")
        explanation = self.backtests.explain_backtest_failure(result["run_id"])
        self.assertEqual(explanation["failure_code"], "non_deterministic_split")
        self.assertEqual(
            self.experiments.get_experiment(experiment["experiment_id"])["current_status"],
            "VALIDATED_CONFIG",
        )

    def test_run_backtest_rejects_post_resolution_data(self) -> None:
        latest_manifest = self._ingest_midpoint_history("fed-cut-2026", end_time="2026-04-24T10:07:00Z")
        experiment = self.experiments.create_experiment(
            hypothesis="Trade late-stage repricing into resolution.",
            strategy_family="resolution_drift",
            config={
                "market_id": "fed-cut-2026",
                "signal_window_minutes": 30,
                "entry_edge_bps": 10,
                "max_holding_minutes": 10,
                "max_position_usd": 150,
            },
            dataset_id=latest_manifest.dataset_id,
            code_version="git:backtest003",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 10, 25, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="VALIDATED_CONFIG",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 10, 26, tzinfo=timezone.utc),
        )

        result = self.backtests.run_backtest(
            experiment["experiment_id"],
            assumptions=_valid_assumptions(),
            now=datetime(2026, 4, 24, 10, 27, tzinfo=timezone.utc),
        )

        self.assertEqual(result["status"], "FAILED")
        explanation = self.backtests.explain_backtest_failure(result["run_id"])
        self.assertEqual(explanation["failure_code"], "post_resolution_data")
        self.assertEqual(explanation["violations"][0]["market_id"], "fed-cut-2026")

    def _ingest_midpoint_history(self, market_id: str, *, end_time: str = "2026-11-05T00:00:00Z"):
        manifests = []
        snapshots = (
            ("100", "100", datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc)),
            ("120", "100", datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc)),
            ("85", "100", datetime(2026, 4, 24, 10, 10, tzinfo=timezone.utc)),
            ("140", "100", datetime(2026, 4, 24, 10, 15, tzinfo=timezone.utc)),
            ("75", "100", datetime(2026, 4, 24, 10, 20, tzinfo=timezone.utc)),
        )
        for volume, liquidity, received_at in snapshots:
            manifests.append(
                self.market_store.ingest_market_payloads(
                    [
                        _sample_market(
                            market_id=market_id,
                            question=f"Will {market_id} move higher?",
                            volume=volume,
                            liquidity=liquidity,
                            end_time=end_time,
                        )
                    ],
                    received_at=received_at,
                )
            )
        return manifests[-1]


if __name__ == "__main__":
    unittest.main()
