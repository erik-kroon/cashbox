from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

from .ingest import FileSystemMarketStore
from .models import (
    IngestHealthReport,
    MarketFilter,
    NormalizedMarketRecord,
    format_datetime,
    parse_datetime,
    utc_now,
)


class ResearchMarketReadPath:
    def __init__(self, store: FileSystemMarketStore) -> None:
        self.store = store

    def list_active_markets(
        self,
        filters: Optional[MarketFilter] = None,
        *,
        dataset_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        manifest = self.store.load_manifest(dataset_id)
        records = self.store.load_dataset(manifest.dataset_id)
        query = None if filters is None or filters.query is None else filters.query.strip().lower()
        category = None if filters is None or filters.category is None else filters.category.strip().lower()
        active_only = True if filters is None else filters.active_only
        limit = None if filters is None else filters.limit

        filtered: list[dict[str, Any]] = []
        for record in records:
            if active_only and (not record.active or record.closed or record.archived):
                continue
            if category and record.category != category:
                continue
            if query and query not in record.question.lower() and query not in record.market_id.lower():
                continue
            filtered.append(self._summary(record, dataset_id=manifest.dataset_id))

        filtered.sort(key=lambda item: (item["category"], item["question"], item["market_id"]))
        if limit is not None:
            return filtered[:limit]
        return filtered

    def get_market_metadata(self, market_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]:
        manifest = self.store.load_manifest(dataset_id)
        for record in self.store.load_dataset(manifest.dataset_id):
            if record.market_id == market_id:
                payload = record.to_dict()
                payload["dataset_id"] = manifest.dataset_id
                payload["source_name"] = manifest.source_name
                return payload
        raise KeyError(f"unknown market_id: {market_id}")

    def get_market_timeseries(
        self,
        market_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        fields: Optional[Iterable[str]] = None,
    ) -> list[dict[str, Any]]:
        selected_fields = tuple(fields or ("active", "closed", "category", "liquidity", "volume", "end_time"))
        rows = self.store.load_history(market_id)
        points: list[dict[str, Any]] = []
        for row in rows:
            recorded_at = parse_datetime(row["recorded_at"])
            if recorded_at is None:
                continue
            if start is not None and recorded_at < start:
                continue
            if end is not None and recorded_at > end:
                continue
            record = NormalizedMarketRecord.from_dict(row["record"])
            values = {field: getattr(record, field) for field in selected_fields}
            points.append(
                {
                    "market_id": market_id,
                    "dataset_id": row["dataset_id"],
                    "recorded_at": format_datetime(recorded_at),
                    "values": values,
                }
            )
        return points

    def get_ingest_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(hours=1),
        dataset_id: Optional[str] = None,
    ) -> IngestHealthReport:
        manifest = self.store.load_manifest(dataset_id)
        records = self.store.load_dataset(manifest.dataset_id)
        current_time = now or utc_now()
        stale_market_ids = []
        for record in records:
            received_at = parse_datetime(record.source_received_at)
            if received_at is None:
                stale_market_ids.append(record.market_id)
                continue
            if current_time - received_at > stale_after:
                stale_market_ids.append(record.market_id)
        active_market_count = sum(
            1 for record in records if record.active and not record.closed and not record.archived
        )
        return IngestHealthReport(
            dataset_id=manifest.dataset_id,
            source_name=manifest.source_name,
            ingested_at=manifest.ingested_at,
            market_count=manifest.market_count,
            active_market_count=active_market_count,
            stale_market_ids=tuple(sorted(stale_market_ids)),
        )

    def _summary(self, record: NormalizedMarketRecord, *, dataset_id: str) -> dict[str, Any]:
        return {
            "market_id": record.market_id,
            "question": record.question,
            "category": record.category,
            "event_id": record.event_id,
            "outcome_count": len(record.outcomes),
            "active": record.active,
            "closed": record.closed,
            "archived": record.archived,
            "end_time": record.end_time,
            "dataset_id": dataset_id,
            "source_received_at": record.source_received_at,
        }
