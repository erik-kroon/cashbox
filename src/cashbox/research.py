from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable, Optional, Protocol

from .models import IngestHealthReport, MarketFilter

RESEARCH_TIMESERIES_FIELDS = frozenset(
    {
        "active",
        "archived",
        "category",
        "closed",
        "enable_order_book",
        "end_time",
        "event_id",
        "liquidity",
        "question",
        "resolution_source",
        "source_market_id",
        "source_received_at",
        "volume",
    }
)
RESEARCH_MARKET_SUMMARY_FIELDS = frozenset(
    {
        "active",
        "archived",
        "category",
        "closed",
        "dataset_id",
        "end_time",
        "event_id",
        "market_id",
        "outcome_count",
        "question",
        "source_received_at",
    }
)
RESEARCH_MARKET_METADATA_FIELDS = RESEARCH_MARKET_SUMMARY_FIELDS | frozenset(
    {
        "enable_order_book",
        "liquidity",
        "outcomes",
        "resolution_source",
        "source_name",
        "source_market_id",
        "volume",
    }
)
RESEARCH_TOKEN_LOOKUP_FIELDS = frozenset({"dataset_id", "market_id", "outcome", "token_id"})
RESEARCH_BOOK_FIELDS = frozenset(
    {
        "ask_depth_size",
        "asks",
        "best_ask",
        "best_bid",
        "bid_depth_size",
        "bids",
        "market_id",
        "midpoint",
        "received_at",
        "recorded_at",
        "snapshot_id",
        "source_name",
        "spread",
        "token_id",
    }
)
RESEARCH_TRADE_FIELDS = frozenset(
    {
        "executed_at",
        "market_id",
        "price",
        "received_at",
        "side",
        "size",
        "source_name",
        "token_id",
        "trade_id",
    }
)
RESEARCH_BOOK_HEALTH_FIELDS = frozenset(
    {
        "checked_at",
        "covered_token_count",
        "dataset_id",
        "expected_token_count",
        "latest_books",
        "missing_market_ids",
        "missing_token_count",
        "missing_token_ids",
        "stale_after_seconds",
        "stale_market_ids",
        "stale_token_count",
        "stale_token_ids",
        "status",
    }
)
MAX_RESEARCH_BOOK_DEPTH = 100
MAX_RESEARCH_TRADE_LIMIT = 1000


class MarketHistoryReadStore(Protocol):
    def list_active_markets(
        self,
        filters: Optional[MarketFilter] = None,
        *,
        dataset_id: Optional[str] = None,
    ) -> list[dict[str, Any]]: ...

    def get_market_metadata(self, market_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]: ...

    def get_market_for_token(self, token_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]: ...

    def get_market_timeseries(
        self,
        market_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        fields: Optional[Iterable[str]] = None,
    ) -> list[dict[str, Any]]: ...

    def get_top_of_book(
        self,
        token_id: str,
        *,
        at: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> dict[str, Any]: ...

    def get_order_book_history(
        self,
        token_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> list[dict[str, Any]]: ...

    def get_trade_history(
        self,
        *,
        market_id: Optional[str] = None,
        token_id: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]: ...

    def get_book_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(minutes=5),
        dataset_id: Optional[str] = None,
    ) -> dict[str, Any]: ...

    def get_ingest_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(hours=1),
        dataset_id: Optional[str] = None,
    ) -> IngestHealthReport: ...


class ResearchMarketReader(Protocol):
    def list_active_markets(
        self,
        filters: Optional[MarketFilter] = None,
        *,
        dataset_id: Optional[str] = None,
    ) -> list[dict[str, Any]]: ...

    def get_market_metadata(self, market_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]: ...

    def get_market_for_token(self, token_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]: ...

    def get_market_timeseries(
        self,
        market_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        fields: Optional[Iterable[str]] = None,
    ) -> list[dict[str, Any]]: ...

    def get_top_of_book(
        self,
        token_id: str,
        *,
        at: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> dict[str, Any]: ...

    def get_order_book_history(
        self,
        token_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> list[dict[str, Any]]: ...

    def get_trade_history(
        self,
        *,
        market_id: Optional[str] = None,
        token_id: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]: ...

    def get_book_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(minutes=5),
        dataset_id: Optional[str] = None,
    ) -> dict[str, Any]: ...

    def get_ingest_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(hours=1),
        dataset_id: Optional[str] = None,
    ) -> IngestHealthReport: ...


class ResearchMarketReadPath:
    def __init__(self, history: MarketHistoryReadStore) -> None:
        self._history = history

    def list_active_markets(
        self,
        filters: Optional[MarketFilter] = None,
        *,
        dataset_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        rows = self._history.list_active_markets(filters, dataset_id=dataset_id)
        return [_project(row, RESEARCH_MARKET_SUMMARY_FIELDS) for row in rows]

    def get_market_metadata(self, market_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]:
        row = self._history.get_market_metadata(market_id, dataset_id=dataset_id)
        return _project(row, RESEARCH_MARKET_METADATA_FIELDS)

    def get_market_for_token(self, token_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]:
        row = self._history.get_market_for_token(token_id, dataset_id=dataset_id)
        return _project(row, RESEARCH_TOKEN_LOOKUP_FIELDS)

    def get_market_timeseries(
        self,
        market_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        fields: Optional[Iterable[str]] = None,
    ) -> list[dict[str, Any]]:
        selected_fields = _validate_timeseries_fields(fields)
        rows = self._history.get_market_timeseries(market_id, start=start, end=end, fields=selected_fields)
        return [_project_timeseries_point(row, selected_fields) for row in rows]

    def get_top_of_book(
        self,
        token_id: str,
        *,
        at: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> dict[str, Any]:
        _validate_depth(depth)
        row = self._history.get_top_of_book(token_id, at=at, depth=depth)
        return _project_book(row)

    def get_order_book_history(
        self,
        token_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        _validate_depth(depth)
        rows = self._history.get_order_book_history(token_id, start=start, end=end, depth=depth)
        return [_project_book(row) for row in rows]

    def get_trade_history(
        self,
        *,
        market_id: Optional[str] = None,
        token_id: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        _validate_trade_scope(market_id=market_id, token_id=token_id)
        _validate_limit(limit)
        rows = self._history.get_trade_history(
            market_id=market_id,
            token_id=token_id,
            start=start,
            end=end,
            limit=limit,
        )
        return [_project(row, RESEARCH_TRADE_FIELDS) for row in rows]

    def get_book_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(minutes=5),
        dataset_id: Optional[str] = None,
    ) -> dict[str, Any]:
        row = self._history.get_book_health(now=now, stale_after=stale_after, dataset_id=dataset_id)
        return _project(row, RESEARCH_BOOK_HEALTH_FIELDS)

    def get_ingest_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(hours=1),
        dataset_id: Optional[str] = None,
    ) -> IngestHealthReport:
        return self._history.get_ingest_health(now=now, stale_after=stale_after, dataset_id=dataset_id)


def _validate_timeseries_fields(fields: Optional[Iterable[str]]) -> Optional[tuple[str, ...]]:
    if fields is None:
        return None
    selected_fields = tuple(fields)
    unsupported = sorted(set(selected_fields) - RESEARCH_TIMESERIES_FIELDS)
    if unsupported:
        raise ValueError(f"unsupported research timeseries field(s): {', '.join(unsupported)}")
    return selected_fields


def _validate_depth(depth: Optional[int]) -> None:
    if depth is None:
        return
    if depth < 1 or depth > MAX_RESEARCH_BOOK_DEPTH:
        raise ValueError(f"depth must be between 1 and {MAX_RESEARCH_BOOK_DEPTH}")


def _validate_limit(limit: Optional[int]) -> None:
    if limit is None:
        return
    if limit < 1 or limit > MAX_RESEARCH_TRADE_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_RESEARCH_TRADE_LIMIT}")


def _validate_trade_scope(*, market_id: Optional[str], token_id: Optional[str]) -> None:
    if market_id is None and token_id is None:
        raise ValueError("market_id or token_id is required")


def _project(row: dict[str, Any], allowed_fields: frozenset[str]) -> dict[str, Any]:
    return {key: _copy_value(row[key]) for key in sorted(allowed_fields) if key in row}


def _project_book(row: dict[str, Any]) -> dict[str, Any]:
    return _project(row, RESEARCH_BOOK_FIELDS)


def _project_timeseries_point(row: dict[str, Any], fields: Optional[tuple[str, ...]]) -> dict[str, Any]:
    selected_fields = fields if fields is not None else tuple(RESEARCH_TIMESERIES_FIELDS)
    values = row.get("values", {})
    return {
        "dataset_id": row["dataset_id"],
        "market_id": row["market_id"],
        "recorded_at": row["recorded_at"],
        "values": {field: _copy_value(values[field]) for field in selected_fields if field in values},
    }


def _copy_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _copy_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_value(item) for item in value]
    return value
