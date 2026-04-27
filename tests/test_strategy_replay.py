from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.experiments import ExperimentService, FileSystemExperimentStore
from cashbox.ingest import FileSystemMarketStore
from cashbox.strategy_replay import StrategyReplayService


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


class StrategyReplayServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.market_store = FileSystemMarketStore(self.root)
        self.experiments = ExperimentService(FileSystemExperimentStore(self.root))
        self.replay = StrategyReplayService(self.market_store)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_replay_service_supports_backtest_and_paper_windows(self) -> None:
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
            code_version="git:replay001",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 10, 30, tzinfo=timezone.utc),
        )
        self._ingest_history(
            "btc-150k",
            start=datetime(2026, 4, 24, 10, 35, tzinfo=timezone.utc),
            volumes=["150", "60", "145", "55", "140", "50"],
            liquidity="100",
        )

        assumptions = self.replay.normalize_assumptions(
            _valid_assumptions(),
            validation_error=self._validation_error,
        )
        backtest_batch = self.replay.load_backtest_histories(
            experiment,
            baseline_manifest.dataset_id,
            validation_error=self._validation_error,
        )
        backtest_result = self.replay.replay_strategy(
            experiment,
            assumptions,
            backtest_batch.histories,
            validation_error=self._validation_error,
        )
        paper_window = self.replay.load_paper_histories(
            experiment,
            start_dataset_id=baseline_manifest.dataset_id,
            end_dataset_id=self.market_store.load_manifest().dataset_id,
            validation_error=self._validation_error,
        )
        paper_result = self.replay.replay_paper_strategy(
            experiment,
            assumptions,
            paper_window.histories,
            validation_error=self._validation_error,
        )
        drift_report = self.replay.build_paper_drift_report(
            experiment_id=experiment["experiment_id"],
            paper_run_id="paper-test",
            backtest_run_id="bt-test",
            reference_assumptions=assumptions,
            reference_metrics=backtest_result.metrics,
            paper_metrics=paper_result.metrics,
            paper_rejections=paper_result.rejections,
            report_version=1,
            created_at=datetime(2026, 4, 24, 10, 55, tzinfo=timezone.utc),
        )

        self.assertGreaterEqual(backtest_batch.timeline_points, 2)
        self.assertGreaterEqual(paper_window.timeline_points, 2)
        self.assertGreaterEqual(len(backtest_result.trades), 1)
        self.assertGreaterEqual(len(paper_result.trades), 1)
        self.assertEqual(backtest_result.metrics["trade_count"], len(backtest_result.trades))
        self.assertEqual(paper_result.metrics["candidate_trade_count"], len(paper_result.candidate_trades))
        self.assertTrue(all(trade["split"] in {"train", "validation", "test"} for trade in backtest_result.trades))
        self.assertTrue(all(trade["split"] == "paper" for trade in paper_result.trades))
        self.assertEqual(paper_window.source_window["start_dataset_id"], baseline_manifest.dataset_id)
        self.assertEqual(drift_report["paper_run_id"], "paper-test")
        self.assertEqual(drift_report["reference"]["backtest_trade_count"], backtest_result.metrics["trade_count"])
        self.assertIn(drift_report["status"], {"ACCEPTABLE", "DRIFTED"})

    def test_replay_service_rejects_non_chronological_assumptions(self) -> None:
        assumptions = _valid_assumptions()
        assumptions["split_method"] = "random"

        with self.assertRaises(ValueError) as context:
            self.replay.normalize_assumptions(
                assumptions,
                validation_error=self._validation_error,
            )

        self.assertEqual(str(context.exception), "split_method must be chronological")

    def _validation_error(self, message: str, **_: object) -> Exception:
        return ValueError(message)

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
