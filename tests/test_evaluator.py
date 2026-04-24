from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.backtests import BacktestService, FileSystemBacktestStore
from cashbox.evaluator import EvaluatorService, FileSystemEvaluationStore
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
        "category": "Politics",
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


def _valid_assumptions(
    *,
    train_ratio: str = "0.6",
    validation_ratio: str = "0.2",
    test_ratio: str = "0.2",
    fee_bps: str = "10",
    slippage_bps: str = "5",
) -> dict[str, object]:
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
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
        "latency_seconds": 0,
        "partial_fill_ratio": "0.75",
        "split_method": "chronological",
        "train_ratio": train_ratio,
        "validation_ratio": validation_ratio,
        "test_ratio": test_ratio,
        "baseline": "hold",
    }


class EvaluatorServiceTests(unittest.TestCase):
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
        self.evaluator = EvaluatorService(
            FileSystemEvaluationStore(self.root),
            experiments=self.experiments,
            backtests=self.backtests,
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_score_experiment_and_promote_paper_when_gate_passes(self) -> None:
        latest_manifest = self._ingest_cross_market_history(num_markets=25, num_snapshots=300)
        market_ids = [f"arb-{index:02d}" for index in range(25)]
        experiment = self.experiments.create_experiment(
            hypothesis="Exploit broad cross-market dislocations with bounded exposure.",
            strategy_family="cross_market_arbitrage",
            config={
                "market_ids": market_ids,
                "max_spread_bps": 4000,
                "min_edge_bps": 150,
                "rebalance_interval_seconds": 60,
                "max_position_usd": 500,
            },
            dataset_id=latest_manifest.dataset_id,
            code_version="git:evaluator001",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 11, 0, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="VALIDATED_CONFIG",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 11, 1, tzinfo=timezone.utc),
        )
        run = self.backtests.run_backtest(
            experiment["experiment_id"],
            assumptions=_valid_assumptions(
                train_ratio="0.1",
                validation_ratio="0.1",
                test_ratio="0.8",
                fee_bps="0.01",
                slippage_bps="0.01",
            ),
            now=datetime(2026, 4, 24, 11, 2, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="WALK_FORWARD_TESTED",
            changed_by="walk-forward-runner",
            now=datetime(2026, 4, 24, 11, 3, tzinfo=timezone.utc),
        )

        score = self.evaluator.score_experiment(
            experiment["experiment_id"],
            run_id=run["run_id"],
            now=datetime(2026, 4, 24, 11, 4, tzinfo=timezone.utc),
        )
        decision = self.evaluator.check_promotion_eligibility(
            experiment["experiment_id"],
            "paper",
            run_id=run["run_id"],
            changed_by="evaluator",
            promote=True,
            now=datetime(2026, 4, 24, 11, 5, tzinfo=timezone.utc),
        )

        self.assertGreaterEqual(score["metrics"]["oos_trade_count"], 250)
        self.assertEqual(score["metrics"]["oos_distinct_market_count"], 25)
        self.assertEqual(score["metrics"]["baseline_net_pnl_usd"], "0")
        self.assertTrue(decision["eligible"])
        self.assertTrue(decision["promotion_applied"])
        self.assertEqual(decision["resulting_status"], "PAPER_ELIGIBLE")
        self.assertEqual(decision["failed_checks"], [])
        self.assertEqual(
            self.experiments.get_experiment(experiment["experiment_id"])["current_status"],
            "PAPER_ELIGIBLE",
        )
        self.assertTrue(
            self.root.joinpath("evaluator", "scores", f"{score['score_id']}.json").exists()
        )
        self.assertTrue(
            self.root.joinpath("evaluator", "promotions", f"{decision['decision_id']}.json").exists()
        )

    def test_paper_gate_rejects_small_strategy_and_does_not_promote(self) -> None:
        latest_manifest = self._ingest_midpoint_history("btc-150k")
        experiment = self.experiments.create_experiment(
            hypothesis="Fade short-horizon midpoint dislocations.",
            strategy_family="midpoint_reversion",
            config={
                "market_id": "btc-150k",
                "lookback_minutes": 2,
                "entry_zscore": 0.5,
                "exit_zscore": 0.2,
                "max_position_usd": 250,
            },
            dataset_id=latest_manifest.dataset_id,
            code_version="git:evaluator002",
            generated_by="hermes",
            now=datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="VALIDATED_CONFIG",
            changed_by="evaluator",
            now=datetime(2026, 4, 24, 12, 1, tzinfo=timezone.utc),
        )
        run = self.backtests.run_backtest(
            experiment["experiment_id"],
            assumptions=_valid_assumptions(),
            now=datetime(2026, 4, 24, 12, 2, tzinfo=timezone.utc),
        )
        self.experiments.transition_experiment_status(
            experiment["experiment_id"],
            to_status="WALK_FORWARD_TESTED",
            changed_by="walk-forward-runner",
            now=datetime(2026, 4, 24, 12, 3, tzinfo=timezone.utc),
        )

        decision = self.evaluator.check_promotion_eligibility(
            experiment["experiment_id"],
            "paper",
            run_id=run["run_id"],
            changed_by="evaluator",
            promote=True,
            now=datetime(2026, 4, 24, 12, 4, tzinfo=timezone.utc),
        )

        self.assertFalse(decision["eligible"])
        self.assertFalse(decision["promotion_applied"])
        self.assertIn("min_out_of_sample_trades", decision["failed_checks"])
        self.assertIn("min_distinct_markets", decision["failed_checks"])
        self.assertEqual(
            self.experiments.get_experiment(experiment["experiment_id"])["current_status"],
            "WALK_FORWARD_TESTED",
        )

    def _ingest_midpoint_history(self, market_id: str):
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
                        )
                    ],
                    received_at=received_at,
                )
            )
        return manifests[-1]

    def _ingest_cross_market_history(self, *, num_markets: int, num_snapshots: int):
        manifests = []
        start = datetime(2026, 4, 24, 8, 0, tzinfo=timezone.utc)
        for snapshot_index in range(num_snapshots):
            payload = []
            for market_index in range(num_markets):
                market_id = f"arb-{market_index:02d}"
                if market_index == 0:
                    volume = str(4000 + (snapshot_index * 2))
                elif market_index == num_markets - 1:
                    volume = str(6000 - (snapshot_index * 2))
                else:
                    volume = str(5000 + market_index)
                payload.append(
                    _sample_market(
                        market_id=market_id,
                        question=f"Will {market_id} converge?",
                        volume=volume,
                        liquidity="10000",
                    )
                )
            manifests.append(
                self.market_store.ingest_market_payloads(
                    payload,
                    received_at=start + timedelta(minutes=5 * snapshot_index),
                )
            )
        return manifests[-1]


if __name__ == "__main__":
    unittest.main()
