from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from cashbox.ingest import FileSystemMarketStore
from cashbox.models import MarketFilter
from cashbox.research import ResearchMarketReadPath


def _sample_market(
    *,
    market_id: str,
    question: str,
    category: str = "Politics",
    active: bool = True,
    closed: bool = False,
    volume: str = "1000",
    liquidity: str = "500",
) -> dict[str, object]:
    return {
        "id": f"raw-{market_id}",
        "slug": market_id,
        "eventSlug": "us-election",
        "question": question,
        "category": category,
        "active": active,
        "closed": closed,
        "archived": False,
        "enableOrderBook": True,
        "outcomes": json.dumps(["Yes", "No"]),
        "clobTokenIds": json.dumps([f"{market_id}-yes", f"{market_id}-no"]),
        "liquidity": liquidity,
        "volume": volume,
        "endDate": "2026-11-05T00:00:00Z",
    }


class MarketDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.store = FileSystemMarketStore(self.root)
        self.read_path = ResearchMarketReadPath(self.store)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_ingest_persists_raw_normalized_and_manifest(self) -> None:
        manifest = self.store.ingest_market_payloads(
            [
                _sample_market(market_id="election-2028", question="Will X win in 2028?", category="Politics"),
                _sample_market(market_id="btc-150k", question="Will BTC hit 150k?", category="Crypto"),
            ],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(manifest.market_count, 2)
        self.assertTrue(self.store.raw_path(manifest.dataset_id).exists())
        self.assertTrue(self.store.normalized_path(manifest.dataset_id).exists())
        self.assertTrue(self.store.manifest_path(manifest.dataset_id).exists())

        dataset = self.store.load_dataset(manifest.dataset_id)
        self.assertEqual([record.market_id for record in dataset], ["btc-150k", "election-2028"])
        self.assertEqual(dataset[0].category, "crypto")
        self.assertEqual([outcome.outcome for outcome in dataset[0].outcomes], ["Yes", "No"])

    def test_list_active_markets_filters_query_category_and_limit(self) -> None:
        self.store.ingest_market_payloads(
            [
                _sample_market(market_id="election-2028", question="Will X win in 2028?", category="Politics"),
                _sample_market(market_id="btc-150k", question="Will BTC hit 150k?", category="Crypto"),
                _sample_market(market_id="old-market", question="Closed market", active=False, closed=True),
            ],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )

        politics = self.read_path.list_active_markets(MarketFilter(category="politics"))
        self.assertEqual([market["market_id"] for market in politics], ["election-2028"])

        queried = self.read_path.list_active_markets(MarketFilter(query="btc", limit=1))
        self.assertEqual([market["market_id"] for market in queried], ["btc-150k"])

        all_markets = self.read_path.list_active_markets(MarketFilter(active_only=False))
        self.assertEqual([market["market_id"] for market in all_markets], ["btc-150k", "old-market", "election-2028"])

    def test_market_metadata_can_be_read_point_in_time_by_dataset(self) -> None:
        first = self.store.ingest_market_payloads(
            [_sample_market(market_id="election-2028", question="Original wording")],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        second = self.store.ingest_market_payloads(
            [_sample_market(market_id="election-2028", question="Updated wording", volume="2000")],
            received_at=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )

        original = self.read_path.get_market_metadata("election-2028", dataset_id=first.dataset_id)
        latest = self.read_path.get_market_metadata("election-2028", dataset_id=second.dataset_id)

        self.assertEqual(original["question"], "Original wording")
        self.assertEqual(latest["question"], "Updated wording")
        self.assertEqual(latest["volume"], "2000")

    def test_market_timeseries_reads_append_only_history(self) -> None:
        self.store.ingest_market_payloads(
            [_sample_market(market_id="election-2028", question="Original wording", volume="1000")],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        self.store.ingest_market_payloads(
            [_sample_market(market_id="election-2028", question="Updated wording", volume="2000")],
            received_at=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )

        series = self.read_path.get_market_timeseries("election-2028", fields=["question", "volume"])

        self.assertEqual(len(series), 2)
        self.assertEqual(series[0]["values"], {"question": "Original wording", "volume": "1000"})
        self.assertEqual(series[1]["values"], {"question": "Updated wording", "volume": "2000"})

    def test_token_lookup_uses_ingested_market_outcomes(self) -> None:
        self.store.ingest_market_payloads(
            [_sample_market(market_id="election-2028", question="Will X win in 2028?")],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )

        lookup = self.read_path.get_market_for_token("election-2028-yes")

        self.assertEqual(lookup["market_id"], "election-2028")
        self.assertEqual(lookup["token_id"], "election-2028-yes")
        self.assertEqual(lookup["outcome"], "Yes")

    def test_clob_books_persist_raw_normalized_and_read_point_in_time_top_of_book(self) -> None:
        self.store.ingest_market_payloads(
            [_sample_market(market_id="election-2028", question="Will X win in 2028?")],
            received_at=datetime(2026, 4, 24, 9, 55, tzinfo=timezone.utc),
        )
        self.store.ingest_order_book_snapshots(
            [
                {
                    "token_id": "election-2028-yes",
                    "timestamp": "2026-04-24T10:00:00Z",
                    "bids": [["0.47", "100"], ["0.46", "40"]],
                    "asks": [["0.52", "80"], ["0.53", "20"]],
                },
                {
                    "token_id": "election-2028-yes",
                    "timestamp": "2026-04-24T10:05:00Z",
                    "bids": [["0.49", "120"]],
                    "asks": [["0.51", "90"], ["0.54", "30"]],
                },
            ],
            received_at=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )

        self.assertTrue(self.store.raw_order_book_path("election-2028-yes").exists())
        self.assertTrue(self.store.normalized_order_book_path("election-2028-yes").exists())

        top = self.read_path.get_top_of_book(
            "election-2028-yes",
            at=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
        )
        latest = self.read_path.get_top_of_book("election-2028-yes")
        history = self.read_path.get_order_book_history(
            "election-2028-yes",
            start=datetime(2026, 4, 24, 10, 1, tzinfo=timezone.utc),
            end=datetime(2026, 4, 24, 10, 6, tzinfo=timezone.utc),
            depth=1,
        )

        self.assertEqual(top["recorded_at"], "2026-04-24T10:00:00Z")
        self.assertEqual(top["best_bid"], {"price": "0.47", "size": "100"})
        self.assertEqual(top["best_ask"], {"price": "0.52", "size": "80"})
        self.assertEqual(top["spread"], "0.05")
        self.assertEqual(latest["recorded_at"], "2026-04-24T10:05:00Z")
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["bids"], [{"price": "0.49", "size": "120"}])
        self.assertEqual(history[0]["asks"], [{"price": "0.51", "size": "90"}])

    def test_clob_trades_persist_raw_normalized_and_read_by_token_or_market(self) -> None:
        self.store.ingest_market_payloads(
            [_sample_market(market_id="election-2028", question="Will X win in 2028?")],
            received_at=datetime(2026, 4, 24, 9, 55, tzinfo=timezone.utc),
        )
        self.store.ingest_clob_trades(
            [
                {
                    "id": "trade-001",
                    "token_id": "election-2028-yes",
                    "timestamp": "2026-04-24T10:00:30Z",
                    "price": "0.50",
                    "size": "25",
                    "side": "BUY",
                },
                {
                    "id": "trade-002",
                    "token_id": "election-2028-no",
                    "timestamp": "2026-04-24T10:04:00Z",
                    "price": "0.48",
                    "size": "10",
                    "side": "SELL",
                },
            ],
            received_at=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )

        self.assertTrue(self.store.raw_trade_path("election-2028-yes").exists())
        self.assertTrue(self.store.normalized_trade_path("election-2028-yes").exists())

        token_trades = self.read_path.get_trade_history(
            token_id="election-2028-yes",
            start=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
            end=datetime(2026, 4, 24, 10, 1, tzinfo=timezone.utc),
        )
        market_trades = self.read_path.get_trade_history(market_id="election-2028")

        self.assertEqual([trade["trade_id"] for trade in token_trades], ["trade-001"])
        self.assertEqual([trade["token_id"] for trade in market_trades], ["election-2028-yes", "election-2028-no"])
        self.assertEqual(market_trades[0]["market_id"], "election-2028")

    def test_book_health_reports_missing_and_stale_coverage_separately_from_metadata(self) -> None:
        self.store.ingest_market_payloads(
            [
                _sample_market(market_id="election-2028", question="Will X win in 2028?"),
                _sample_market(market_id="btc-150k", question="Will BTC hit 150k?", category="Crypto"),
            ],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )
        self.store.ingest_order_book_snapshots(
            [
                {
                    "token_id": "election-2028-yes",
                    "timestamp": "2026-04-24T10:10:00Z",
                    "bids": [["0.47", "100"]],
                    "asks": [["0.52", "80"]],
                }
            ],
            received_at=datetime(2026, 4, 24, 10, 10, tzinfo=timezone.utc),
        )

        metadata_health = self.read_path.get_ingest_health(
            now=datetime(2026, 4, 24, 10, 20, tzinfo=timezone.utc),
            stale_after=timedelta(hours=1),
        )
        book_health = self.read_path.get_book_health(
            now=datetime(2026, 4, 24, 10, 20, tzinfo=timezone.utc),
            stale_after=timedelta(minutes=5),
        )

        self.assertEqual(metadata_health.stale_market_ids, ())
        self.assertEqual(book_health["status"], "DEGRADED")
        self.assertEqual(book_health["stale_token_ids"], ["election-2028-yes"])
        self.assertEqual(
            book_health["missing_token_ids"],
            ["btc-150k-no", "btc-150k-yes", "election-2028-no"],
        )

    def test_ingest_health_flags_stale_records(self) -> None:
        manifest = self.store.ingest_market_payloads(
            [
                _sample_market(market_id="election-2028", question="Will X win in 2028?"),
                _sample_market(market_id="btc-150k", question="Will BTC hit 150k?"),
            ],
            received_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
        )

        report = self.read_path.get_ingest_health(
            dataset_id=manifest.dataset_id,
            now=datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc),
            stale_after=timedelta(minutes=30),
        )

        self.assertEqual(report.market_count, 2)
        self.assertEqual(report.active_market_count, 2)
        self.assertEqual(report.stale_market_ids, ("btc-150k", "election-2028"))


if __name__ == "__main__":
    unittest.main()
