from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import (
    IngestHealthReport,
    MarketDatasetManifest,
    MarketFilter,
    NormalizedMarketRecord,
    format_datetime,
    parse_datetime,
    utc_now,
)

GAMMA_API = "https://gamma-api.polymarket.com"


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text())


def _jsonl_dump(path: Path, payloads: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for payload in payloads:
            handle.write(json.dumps(payload, sort_keys=True))
            handle.write("\n")


def _jsonl_append(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _decimal_text(value: Any, *, field_name: str) -> str:
    if value in (None, ""):
        raise ValueError(f"{field_name} is required")
    try:
        return str(Decimal(str(value).strip()))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-like") from exc


def _optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_text(payload: dict[str, Any], *field_names: str) -> str:
    for field_name in field_names:
        text = _optional_text(payload.get(field_name))
        if text:
            return text
    raise ValueError(f"payload must contain one of: {', '.join(field_names)}")


def _normalized_time(payload: dict[str, Any], *, received_at: datetime, field_names: tuple[str, ...]) -> str:
    for field_name in field_names:
        parsed = parse_datetime(payload.get(field_name))
        if parsed is not None:
            return format_datetime(parsed) or ""
    return format_datetime(received_at) or ""


def _normalize_book_levels(levels: Any, *, side: str) -> list[dict[str, str]]:
    if levels is None:
        return []
    if not isinstance(levels, list):
        raise ValueError(f"{side} must be a list")

    normalized: list[dict[str, str]] = []
    for index, level in enumerate(levels):
        if isinstance(level, dict):
            price = level.get("price")
            size = level.get("size", level.get("amount"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = level[0]
            size = level[1]
        else:
            raise ValueError(f"{side}[{index}] must contain price and size")
        normalized.append(
            {
                "price": _decimal_text(price, field_name=f"{side}.price"),
                "size": _decimal_text(size, field_name=f"{side}.size"),
            }
        )

    reverse = side == "bids"
    return sorted(normalized, key=lambda item: Decimal(item["price"]), reverse=reverse)


def _sum_level_sizes(levels: list[dict[str, str]]) -> str:
    return str(sum((Decimal(level["size"]) for level in levels), Decimal("0")))


def _top_level(levels: list[dict[str, str]]) -> Optional[dict[str, str]]:
    return None if not levels else dict(levels[0])


def _build_book_snapshot_id(payload: dict[str, Any]) -> str:
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return f"book-{digest[:16]}"


def _build_trade_id(payload: dict[str, Any]) -> str:
    source_id = _optional_text(payload.get("id") or payload.get("trade_id"))
    if source_id:
        return source_id
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return f"trade-{digest[:16]}"


def _request_json(base_url: str, path: str, params: dict[str, Any]) -> Any:
    query = urlencode({key: value for key, value in params.items() if value is not None})
    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{query}"

    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "cashbox/0.1 (+https://github.com/erik-kroon/cashbox)",
        },
    )
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


@dataclass(frozen=True)
class MarketMetadataPoint:
    market_id: str
    dataset_id: str
    recorded_at: datetime
    record: NormalizedMarketRecord

    def values(self, fields: Iterable[str]) -> dict[str, Any]:
        return {field: getattr(self.record, field) for field in fields}


@dataclass
class FileSystemMarketHistoryPaths:
    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    @property
    def normalized_dir(self) -> Path:
        return self.root / "normalized"

    @property
    def manifests_dir(self) -> Path:
        return self.root / "manifests"

    @property
    def history_dir(self) -> Path:
        return self.root / "history"

    def latest_manifest_path(self) -> Path:
        return self.manifests_dir / "latest.json"

    def manifest_path(self, dataset_id: str) -> Path:
        return self.manifests_dir / f"{dataset_id}.json"

    def raw_path(self, dataset_id: str) -> Path:
        return self.raw_dir / f"{dataset_id}.jsonl"

    def normalized_path(self, dataset_id: str) -> Path:
        return self.normalized_dir / f"{dataset_id}.json"

    def history_path(self, market_id: str) -> Path:
        return self.history_dir / f"{market_id}.jsonl"

    @property
    def raw_order_books_dir(self) -> Path:
        return self.raw_dir / "clob" / "order-books"

    @property
    def normalized_order_books_dir(self) -> Path:
        return self.normalized_dir / "clob" / "order-books"

    @property
    def raw_trades_dir(self) -> Path:
        return self.raw_dir / "clob" / "trades"

    @property
    def normalized_trades_dir(self) -> Path:
        return self.normalized_dir / "clob" / "trades"

    def raw_order_book_path(self, token_id: str) -> Path:
        return self.raw_order_books_dir / f"{token_id}.jsonl"

    def normalized_order_book_path(self, token_id: str) -> Path:
        return self.normalized_order_books_dir / f"{token_id}.jsonl"

    def raw_trade_path(self, token_id: str) -> Path:
        return self.raw_trades_dir / f"{token_id}.jsonl"

    def normalized_trade_path(self, token_id: str) -> Path:
        return self.normalized_trades_dir / f"{token_id}.jsonl"


@dataclass
class MarketHistoryStorage:
    paths: FileSystemMarketHistoryPaths

    def write_market_dataset(
        self,
        *,
        dataset_id: str,
        source_name: str,
        received_at: str,
        raw_payloads: Iterable[dict[str, Any]],
        normalized_records: Iterable[NormalizedMarketRecord],
        manifest: MarketDatasetManifest,
    ) -> None:
        _jsonl_dump(
            self.paths.raw_path(dataset_id),
            (
                {
                    "dataset_id": dataset_id,
                    "source_name": source_name,
                    "received_at": received_at,
                    "payload": payload,
                }
                for payload in raw_payloads
            ),
        )
        records = list(normalized_records)
        _json_dump(self.paths.normalized_path(dataset_id), [record.to_dict() for record in records])
        _json_dump(self.paths.manifest_path(dataset_id), manifest.to_dict())
        _json_dump(self.paths.latest_manifest_path(), manifest.to_dict())

        for record in records:
            _jsonl_append(
                self.paths.history_path(record.market_id),
                {
                    "dataset_id": dataset_id,
                    "recorded_at": manifest.ingested_at,
                    "record": record.to_dict(),
                },
            )

    def load_manifest(self, dataset_id: Optional[str] = None) -> MarketDatasetManifest:
        path = self.paths.latest_manifest_path() if dataset_id is None else self.paths.manifest_path(dataset_id)
        return MarketDatasetManifest.from_dict(_json_load(path))

    def load_dataset(self, dataset_id: Optional[str] = None) -> list[NormalizedMarketRecord]:
        manifest = self.load_manifest(dataset_id)
        payload = _json_load(self.paths.normalized_path(manifest.dataset_id))
        return [NormalizedMarketRecord.from_dict(item) for item in payload]

    def load_market_history(self, market_id: str) -> list[dict[str, Any]]:
        return self._load_jsonl(self.paths.history_path(market_id))

    def append_raw_order_book(self, token_id: str, row: dict[str, Any]) -> None:
        _jsonl_append(self.paths.raw_order_book_path(token_id), row)

    def append_normalized_order_book(self, token_id: str, row: dict[str, Any]) -> None:
        _jsonl_append(self.paths.normalized_order_book_path(token_id), row)

    def append_raw_trade(self, token_id: str, row: dict[str, Any]) -> None:
        _jsonl_append(self.paths.raw_trade_path(token_id), row)

    def append_normalized_trade(self, token_id: str, row: dict[str, Any]) -> None:
        _jsonl_append(self.paths.normalized_trade_path(token_id), row)

    def load_order_book_history(self, token_id: str) -> list[dict[str, Any]]:
        return self._load_jsonl(self.paths.normalized_order_book_path(token_id))

    def load_trade_history(self, token_id: str) -> list[dict[str, Any]]:
        return self._load_jsonl(self.paths.normalized_trade_path(token_id))

    def _load_jsonl(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                rows.append(json.loads(line))
        return rows


@dataclass
class GammaMarketMetadataIngest:
    storage: MarketHistoryStorage

    def ingest_market_payloads(
        self,
        payloads: Iterable[dict[str, Any]],
        *,
        source_name: str = "polymarket-gamma",
        received_at: Optional[datetime] = None,
    ) -> MarketDatasetManifest:
        wall_clock = received_at or utc_now()
        raw_payloads = [dict(payload) for payload in payloads]
        normalized_records = sorted(
            (NormalizedMarketRecord.from_gamma_payload(payload, received_at=wall_clock) for payload in raw_payloads),
            key=lambda record: record.market_id,
        )

        raw_hash = hashlib.sha256(_canonical_json(raw_payloads).encode("utf-8")).hexdigest()
        dataset_id = f"{wall_clock.strftime('%Y%m%dT%H%M%SZ')}-{raw_hash[:12]}"
        manifest = MarketDatasetManifest(
            dataset_id=dataset_id,
            source_name=source_name,
            ingested_at=format_datetime(wall_clock) or "",
            created_at=format_datetime(utc_now()) or "",
            market_count=len(normalized_records),
            raw_payload_sha256=raw_hash,
        )

        self.storage.write_market_dataset(
            dataset_id=dataset_id,
            source_name=source_name,
            received_at=manifest.ingested_at,
            raw_payloads=raw_payloads,
            normalized_records=normalized_records,
            manifest=manifest,
        )

        return manifest


@dataclass
class MarketHistoryQueries:
    storage: MarketHistoryStorage

    def get_market_metadata(self, market_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]:
        manifest = self.storage.load_manifest(dataset_id)
        for record in self.storage.load_dataset(manifest.dataset_id):
            if record.market_id == market_id:
                payload = record.to_dict()
                payload["dataset_id"] = manifest.dataset_id
                payload["source_name"] = manifest.source_name
                return payload
        raise KeyError(f"unknown market_id: {market_id}")

    def get_market_metadata_history(
        self,
        market_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> list[MarketMetadataPoint]:
        points: list[MarketMetadataPoint] = []
        for row in self.storage.load_market_history(market_id):
            recorded_at = parse_datetime(row.get("recorded_at"))
            if recorded_at is None:
                continue
            if start is not None and recorded_at < start:
                continue
            if end is not None and recorded_at > end:
                continue
            points.append(
                MarketMetadataPoint(
                    market_id=market_id,
                    dataset_id=str(row["dataset_id"]),
                    recorded_at=recorded_at,
                    record=NormalizedMarketRecord.from_dict(row["record"]),
                )
            )
        return sorted(points, key=lambda item: (item.recorded_at, item.dataset_id))

    def list_active_markets(
        self,
        filters: Optional[MarketFilter] = None,
        *,
        dataset_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        manifest = self.storage.load_manifest(dataset_id)
        query = None if filters is None or filters.query is None else filters.query.strip().lower()
        category = None if filters is None or filters.category is None else filters.category.strip().lower()
        active_only = True if filters is None else filters.active_only
        limit = None if filters is None else filters.limit

        filtered: list[dict[str, Any]] = []
        for record in self.storage.load_dataset(manifest.dataset_id):
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

    def get_market_timeseries(
        self,
        market_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        fields: Optional[Iterable[str]] = None,
    ) -> list[dict[str, Any]]:
        selected_fields = tuple(fields or ("active", "closed", "category", "liquidity", "volume", "end_time"))
        return [
            {
                "market_id": market_id,
                "dataset_id": point.dataset_id,
                "recorded_at": format_datetime(point.recorded_at),
                "values": point.values(selected_fields),
            }
            for point in self.get_market_metadata_history(market_id, start=start, end=end)
        ]

    def get_market_for_token(self, token_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]:
        normalized_token_id = str(token_id).strip()
        manifest = self.storage.load_manifest(dataset_id)
        for record in self.storage.load_dataset(manifest.dataset_id):
            for outcome in record.outcomes:
                if outcome.token_id == normalized_token_id:
                    return {
                        "dataset_id": manifest.dataset_id,
                        "market_id": record.market_id,
                        "token_id": normalized_token_id,
                        "outcome": outcome.outcome,
                    }
        raise KeyError(f"unknown token_id: {normalized_token_id}")

    def get_top_of_book(
        self,
        token_id: str,
        *,
        at: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> dict[str, Any]:
        snapshots = self.get_order_book_history(token_id, end=at, depth=depth)
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
        for row in self.storage.load_order_book_history(token_id):
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
            point["best_bid"] = dict(point["bids"][0]) if point["bids"] else None
            point["best_ask"] = dict(point["asks"][0]) if point["asks"] else None
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

        token_ids = [token_id] if token_id is not None else self.token_ids_for_market(str(market_id))
        trades: list[dict[str, Any]] = []
        for selected_token_id in token_ids:
            if selected_token_id is None:
                continue
            for row in self.storage.load_trade_history(selected_token_id):
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
        manifest = self.storage.load_manifest(dataset_id)
        expected_tokens: list[str] = []
        token_to_market: dict[str, str] = {}
        for record in self.storage.load_dataset(manifest.dataset_id):
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
        manifest = self.storage.load_manifest(dataset_id)
        records = self.storage.load_dataset(manifest.dataset_id)
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

    def token_ids_for_market(self, market_id: str, *, dataset_id: Optional[str] = None) -> list[str]:
        metadata = self.get_market_metadata(market_id, dataset_id=dataset_id)
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


@dataclass
class ClobMarketDataIngest:
    storage: MarketHistoryStorage
    get_market_for_token: Callable[[str], dict[str, Any]]

    def ingest_order_book_snapshots(
        self,
        payloads: Iterable[dict[str, Any]],
        *,
        source_name: str = "polymarket-clob",
        received_at: Optional[datetime] = None,
    ) -> list[dict[str, Any]]:
        wall_clock = received_at or utc_now()
        normalized_rows: list[dict[str, Any]] = []
        for payload in payloads:
            raw_payload = dict(payload)
            token_id = _required_text(raw_payload, "token_id", "asset_id", "tokenId", "assetId")
            recorded_at = _normalized_time(
                raw_payload,
                received_at=wall_clock,
                field_names=("timestamp", "recorded_at", "created_at", "updated_at"),
            )
            bids = _normalize_book_levels(raw_payload.get("bids", []), side="bids")
            asks = _normalize_book_levels(raw_payload.get("asks", []), side="asks")
            market_id = _optional_text(raw_payload.get("market_id"))
            if market_id is None:
                try:
                    market_id = self.get_market_for_token(token_id)["market_id"]
                except (FileNotFoundError, KeyError):
                    market_id = None

            best_bid = _top_level(bids)
            best_ask = _top_level(asks)
            midpoint = None
            spread = None
            if best_bid is not None and best_ask is not None:
                bid_price = Decimal(best_bid["price"])
                ask_price = Decimal(best_ask["price"])
                midpoint = str((bid_price + ask_price) / Decimal("2"))
                spread = str(ask_price - bid_price)

            normalized = {
                "snapshot_id": _build_book_snapshot_id(
                    {
                        "asks": asks,
                        "bids": bids,
                        "recorded_at": recorded_at,
                        "source_name": source_name,
                        "token_id": token_id,
                    }
                ),
                "source_name": source_name,
                "market_id": market_id,
                "token_id": token_id,
                "recorded_at": recorded_at,
                "received_at": format_datetime(wall_clock) or "",
                "bids": bids,
                "asks": asks,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "midpoint": midpoint,
                "spread": spread,
                "bid_depth_size": _sum_level_sizes(bids),
                "ask_depth_size": _sum_level_sizes(asks),
            }
            self.storage.append_raw_order_book(
                token_id,
                {
                    "source_name": source_name,
                    "received_at": format_datetime(wall_clock) or "",
                    "payload": raw_payload,
                },
            )
            self.storage.append_normalized_order_book(token_id, normalized)
            normalized_rows.append(normalized)
        return normalized_rows

    def ingest_clob_trades(
        self,
        payloads: Iterable[dict[str, Any]],
        *,
        source_name: str = "polymarket-clob",
        received_at: Optional[datetime] = None,
    ) -> list[dict[str, Any]]:
        wall_clock = received_at or utc_now()
        normalized_rows: list[dict[str, Any]] = []
        for payload in payloads:
            raw_payload = dict(payload)
            token_id = _required_text(raw_payload, "token_id", "asset_id", "tokenId", "assetId")
            executed_at = _normalized_time(
                raw_payload,
                received_at=wall_clock,
                field_names=("timestamp", "executed_at", "created_at", "time"),
            )
            market_id = _optional_text(raw_payload.get("market_id"))
            if market_id is None:
                try:
                    market_id = self.get_market_for_token(token_id)["market_id"]
                except (FileNotFoundError, KeyError):
                    market_id = None
            side = _optional_text(raw_payload.get("side"))

            normalized = {
                "trade_id": _build_trade_id(raw_payload),
                "source_name": source_name,
                "market_id": market_id,
                "token_id": token_id,
                "executed_at": executed_at,
                "received_at": format_datetime(wall_clock) or "",
                "price": _decimal_text(raw_payload.get("price"), field_name="price"),
                "size": _decimal_text(raw_payload.get("size", raw_payload.get("amount")), field_name="size"),
                "side": None if side is None else side.upper(),
            }
            self.storage.append_raw_trade(
                token_id,
                {
                    "source_name": source_name,
                    "received_at": format_datetime(wall_clock) or "",
                    "payload": raw_payload,
                },
            )
            self.storage.append_normalized_trade(token_id, normalized)
            normalized_rows.append(normalized)
        return normalized_rows


@dataclass
class FileSystemMarketHistory:
    root: Path

    def __post_init__(self) -> None:
        self._paths = FileSystemMarketHistoryPaths(self.root)
        self.root = self._paths.root
        self._storage = MarketHistoryStorage(self._paths)
        self._queries = MarketHistoryQueries(self._storage)
        self._metadata_ingest = GammaMarketMetadataIngest(self._storage)
        self._clob_ingest = ClobMarketDataIngest(self._storage, self._queries.get_market_for_token)

    @property
    def raw_dir(self) -> Path:
        return self._paths.raw_dir

    @property
    def normalized_dir(self) -> Path:
        return self._paths.normalized_dir

    @property
    def manifests_dir(self) -> Path:
        return self._paths.manifests_dir

    @property
    def history_dir(self) -> Path:
        return self._paths.history_dir

    def latest_manifest_path(self) -> Path:
        return self._paths.latest_manifest_path()

    def manifest_path(self, dataset_id: str) -> Path:
        return self._paths.manifest_path(dataset_id)

    def raw_path(self, dataset_id: str) -> Path:
        return self._paths.raw_path(dataset_id)

    def normalized_path(self, dataset_id: str) -> Path:
        return self._paths.normalized_path(dataset_id)

    def history_path(self, market_id: str) -> Path:
        return self._paths.history_path(market_id)

    @property
    def raw_order_books_dir(self) -> Path:
        return self._paths.raw_order_books_dir

    @property
    def normalized_order_books_dir(self) -> Path:
        return self._paths.normalized_order_books_dir

    @property
    def raw_trades_dir(self) -> Path:
        return self._paths.raw_trades_dir

    @property
    def normalized_trades_dir(self) -> Path:
        return self._paths.normalized_trades_dir

    def raw_order_book_path(self, token_id: str) -> Path:
        return self._paths.raw_order_book_path(token_id)

    def normalized_order_book_path(self, token_id: str) -> Path:
        return self._paths.normalized_order_book_path(token_id)

    def raw_trade_path(self, token_id: str) -> Path:
        return self._paths.raw_trade_path(token_id)

    def normalized_trade_path(self, token_id: str) -> Path:
        return self._paths.normalized_trade_path(token_id)

    def ingest_market_payloads(
        self,
        payloads: Iterable[dict[str, Any]],
        *,
        source_name: str = "polymarket-gamma",
        received_at: Optional[datetime] = None,
    ) -> MarketDatasetManifest:
        return self._metadata_ingest.ingest_market_payloads(
            payloads,
            source_name=source_name,
            received_at=received_at,
        )

    def load_manifest(self, dataset_id: Optional[str] = None) -> MarketDatasetManifest:
        return self._storage.load_manifest(dataset_id)

    def load_dataset(self, dataset_id: Optional[str] = None) -> list[NormalizedMarketRecord]:
        return self._storage.load_dataset(dataset_id)

    def load_history(self, market_id: str) -> list[dict[str, Any]]:
        return self._storage.load_market_history(market_id)

    def get_market_metadata(self, market_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]:
        return self._queries.get_market_metadata(market_id, dataset_id=dataset_id)

    def get_market_metadata_history(
        self,
        market_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> list[MarketMetadataPoint]:
        return self._queries.get_market_metadata_history(market_id, start=start, end=end)

    def list_active_markets(
        self,
        filters: Optional[MarketFilter] = None,
        *,
        dataset_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        return self._queries.list_active_markets(filters, dataset_id=dataset_id)

    def get_market_timeseries(
        self,
        market_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        fields: Optional[Iterable[str]] = None,
    ) -> list[dict[str, Any]]:
        return self._queries.get_market_timeseries(market_id, start=start, end=end, fields=fields)

    def get_market_for_token(self, token_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]:
        return self._queries.get_market_for_token(token_id, dataset_id=dataset_id)

    def ingest_order_book_snapshots(
        self,
        payloads: Iterable[dict[str, Any]],
        *,
        source_name: str = "polymarket-clob",
        received_at: Optional[datetime] = None,
    ) -> list[dict[str, Any]]:
        return self._clob_ingest.ingest_order_book_snapshots(
            payloads,
            source_name=source_name,
            received_at=received_at,
        )

    def ingest_clob_trades(
        self,
        payloads: Iterable[dict[str, Any]],
        *,
        source_name: str = "polymarket-clob",
        received_at: Optional[datetime] = None,
    ) -> list[dict[str, Any]]:
        return self._clob_ingest.ingest_clob_trades(
            payloads,
            source_name=source_name,
            received_at=received_at,
        )

    def load_order_book_history(self, token_id: str) -> list[dict[str, Any]]:
        return self._storage.load_order_book_history(token_id)

    def load_trade_history(self, token_id: str) -> list[dict[str, Any]]:
        return self._storage.load_trade_history(token_id)

    def get_top_of_book(
        self,
        token_id: str,
        *,
        at: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> dict[str, Any]:
        return self._queries.get_top_of_book(token_id, at=at, depth=depth)

    def get_order_book_history(
        self,
        token_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        depth: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        return self._queries.get_order_book_history(token_id, start=start, end=end, depth=depth)

    def get_trade_history(
        self,
        *,
        market_id: Optional[str] = None,
        token_id: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        return self._queries.get_trade_history(
            market_id=market_id,
            token_id=token_id,
            start=start,
            end=end,
            limit=limit,
        )

    def get_book_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(minutes=5),
        dataset_id: Optional[str] = None,
    ) -> dict[str, Any]:
        return self._queries.get_book_health(now=now, stale_after=stale_after, dataset_id=dataset_id)

    def get_ingest_health(
        self,
        *,
        now: Optional[datetime] = None,
        stale_after: timedelta = timedelta(hours=1),
        dataset_id: Optional[str] = None,
    ) -> IngestHealthReport:
        return self._queries.get_ingest_health(now=now, stale_after=stale_after, dataset_id=dataset_id)

    def token_ids_for_market(self, market_id: str, *, dataset_id: Optional[str] = None) -> list[str]:
        return self._queries.token_ids_for_market(market_id, dataset_id=dataset_id)


def fetch_polymarket_markets(
    *,
    limit: int = 100,
    offset: int = 0,
    active: Optional[bool] = None,
) -> list[dict[str, Any]]:
    payload = _request_json(
        GAMMA_API,
        "/markets",
        {
            "limit": limit,
            "offset": offset,
            "active": None if active is None else str(active).lower(),
        },
    )
    if not isinstance(payload, list):
        raise ValueError("expected list payload from Polymarket Gamma markets endpoint")
    return [dict(item) for item in payload]


def ingest_polymarket_markets(
    store: FileSystemMarketHistory,
    *,
    limit: int = 100,
    offset: int = 0,
    active: Optional[bool] = None,
    received_at: Optional[datetime] = None,
) -> MarketDatasetManifest:
    payloads = fetch_polymarket_markets(limit=limit, offset=offset, active=active)
    return store.ingest_market_payloads(payloads, received_at=received_at)
