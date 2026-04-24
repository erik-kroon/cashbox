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
