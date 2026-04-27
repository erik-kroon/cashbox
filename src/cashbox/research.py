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

    def get_market_for_token(self, token_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]:
        return self.store.get_market_for_token(token_id, dataset_id=dataset_id)

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

    def get_top_of_book(
        self,
        token_id: str,
        *,
        at: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> dict[str, Any]:
        snapshots = self.get_order_book_history(
            token_id,
            end=at,
            depth=depth,
        )
        if not snapshots:
            raise KeyError(f"no order-book snapshots found for token_id: {token_id}")
        return snapshots[-1]

    def get_order_book_history(
        self,
        token_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        if depth is not None and depth < 1:
            raise ValueError("depth must be >= 1")
        points: list[dict[str, Any]] = []
        for row in self.store.load_order_book_history(token_id):
            recorded_at = parse_datetime(row.get("recorded_at"))
            if recorded_at is None:
                continue
            if start is not None and recorded_at < start:
                continue
            if end is not None and recorded_at > end:
                continue
            point = dict(row)
            point["bids"] = [dict(level) for level in row.get("bids", [])]
            point["asks"] = [dict(level) for level in row.get("asks", [])]
            if depth is not None:
                point["bids"] = point["bids"][:depth]
                point["asks"] = point["asks"][:depth]
            if point["bids"]:
                point["best_bid"] = dict(point["bids"][0])
            else:
                point["best_bid"] = None
            if point["asks"]:
                point["best_ask"] = dict(point["asks"][0])
            else:
                point["best_ask"] = None
            points.append(point)
        return sorted(points, key=lambda item: (item["recorded_at"], item["snapshot_id"]))

    def get_trade_history(
        self,
        *,
        market_id: Optional[str] = None,
        token_id: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        if market_id is None and token_id is None:
            raise ValueError("market_id or token_id is required")
        if limit is not None and limit < 1:
            raise ValueError("limit must be >= 1")

        token_ids = [token_id] if token_id is not None else self._token_ids_for_market(str(market_id))
        trades: list[dict[str, Any]] = []
        for selected_token_id in token_ids:
            if selected_token_id is None:
                continue
            for row in self.store.load_trade_history(selected_token_id):
                executed_at = parse_datetime(row.get("executed_at"))
                if executed_at is None:
                    continue
                if start is not None and executed_at < start:
                    continue
                if end is not None and executed_at > end:
                    continue
                if market_id is not None and row.get("market_id") != market_id:
                    continue
                trades.append(dict(row))
        trades.sort(key=lambda item: (item["executed_at"], item["trade_id"]))
        if limit is not None:
            return trades[:limit]
        return trades

    def get_book_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(minutes=5),
        dataset_id: Optional[str] = None,
    ) -> dict[str, Any]:
        current_time = now or utc_now()
        manifest = self.store.load_manifest(dataset_id)
        expected_tokens = []
        token_to_market: dict[str, str] = {}
        for record in self.store.load_dataset(manifest.dataset_id):
            if not record.active or record.closed or record.archived or not record.enable_order_book:
                continue
            for outcome in record.outcomes:
                if outcome.token_id is None:
                    continue
                expected_tokens.append(outcome.token_id)
                token_to_market[outcome.token_id] = record.market_id

        missing_token_ids: list[str] = []
        stale_token_ids: list[str] = []
        latest_books: dict[str, dict[str, Any]] = {}
        for token_id in sorted(expected_tokens):
            try:
                latest = self.get_top_of_book(token_id, at=current_time)
            except KeyError:
                missing_token_ids.append(token_id)
                continue
            latest_books[token_id] = {
                "market_id": latest.get("market_id"),
                "recorded_at": latest["recorded_at"],
                "snapshot_id": latest["snapshot_id"],
            }
            recorded_at = parse_datetime(latest["recorded_at"])
            if recorded_at is None or current_time - recorded_at > stale_after:
                stale_token_ids.append(token_id)

        degraded = bool(missing_token_ids or stale_token_ids)
        return {
            "status": "DEGRADED" if degraded else "OK",
            "dataset_id": manifest.dataset_id,
            "checked_at": format_datetime(current_time),
            "stale_after_seconds": int(stale_after.total_seconds()),
            "expected_token_count": len(expected_tokens),
            "covered_token_count": len(latest_books),
            "missing_token_count": len(missing_token_ids),
            "stale_token_count": len(stale_token_ids),
            "missing_token_ids": missing_token_ids,
            "stale_token_ids": stale_token_ids,
            "missing_market_ids": sorted({token_to_market[token_id] for token_id in missing_token_ids}),
            "stale_market_ids": sorted({token_to_market[token_id] for token_id in stale_token_ids}),
            "latest_books": latest_books,
        }

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

    def _token_ids_for_market(self, market_id: str) -> list[str]:
        metadata = self.get_market_metadata(market_id)
        token_ids = [
            str(outcome["token_id"])
            for outcome in metadata.get("outcomes", [])
            if outcome.get("token_id") is not None
        ]
        if not token_ids:
            raise KeyError(f"market has no token ids: {market_id}")
        return token_ids

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
