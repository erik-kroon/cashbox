from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import MarketDatasetManifest, NormalizedMarketRecord, format_datetime, parse_datetime, utc_now

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


@dataclass
class FileSystemMarketStore:
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

        _jsonl_dump(
            self.raw_path(dataset_id),
            (
                {
                    "dataset_id": dataset_id,
                    "source_name": source_name,
                    "received_at": manifest.ingested_at,
                    "payload": payload,
                }
                for payload in raw_payloads
            ),
        )
        _json_dump(self.normalized_path(dataset_id), [record.to_dict() for record in normalized_records])
        _json_dump(self.manifest_path(dataset_id), manifest.to_dict())
        _json_dump(self.latest_manifest_path(), manifest.to_dict())

        for record in normalized_records:
            _jsonl_append(
                self.history_path(record.market_id),
                {
                    "dataset_id": dataset_id,
                    "recorded_at": manifest.ingested_at,
                    "record": record.to_dict(),
                },
            )

        return manifest

    def load_manifest(self, dataset_id: Optional[str] = None) -> MarketDatasetManifest:
        path = self.latest_manifest_path() if dataset_id is None else self.manifest_path(dataset_id)
        return MarketDatasetManifest.from_dict(_json_load(path))

    def load_dataset(self, dataset_id: Optional[str] = None) -> list[NormalizedMarketRecord]:
        manifest = self.load_manifest(dataset_id)
        payload = _json_load(self.normalized_path(manifest.dataset_id))
        return [NormalizedMarketRecord.from_dict(item) for item in payload]

    def load_history(self, market_id: str) -> list[dict[str, Any]]:
        path = self.history_path(market_id)
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                rows.append(json.loads(line))
        return rows

    def get_market_for_token(self, token_id: str, *, dataset_id: Optional[str] = None) -> dict[str, Any]:
        normalized_token_id = str(token_id).strip()
        manifest = self.load_manifest(dataset_id)
        for record in self.load_dataset(manifest.dataset_id):
            for outcome in record.outcomes:
                if outcome.token_id == normalized_token_id:
                    return {
                        "dataset_id": manifest.dataset_id,
                        "market_id": record.market_id,
                        "token_id": normalized_token_id,
                        "outcome": outcome.outcome,
                    }
        raise KeyError(f"unknown token_id: {normalized_token_id}")

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
            _jsonl_append(
                self.raw_order_book_path(token_id),
                {
                    "source_name": source_name,
                    "received_at": format_datetime(wall_clock) or "",
                    "payload": raw_payload,
                },
            )
            _jsonl_append(self.normalized_order_book_path(token_id), normalized)
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
            _jsonl_append(
                self.raw_trade_path(token_id),
                {
                    "source_name": source_name,
                    "received_at": format_datetime(wall_clock) or "",
                    "payload": raw_payload,
                },
            )
            _jsonl_append(self.normalized_trade_path(token_id), normalized)
            normalized_rows.append(normalized)
        return normalized_rows

    def load_order_book_history(self, token_id: str) -> list[dict[str, Any]]:
        path = self.normalized_order_book_path(token_id)
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                rows.append(json.loads(line))
        return rows

    def load_trade_history(self, token_id: str) -> list[dict[str, Any]]:
        path = self.normalized_trade_path(token_id)
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                rows.append(json.loads(line))
        return rows


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
    store: FileSystemMarketStore,
    *,
    limit: int = 100,
    offset: int = 0,
    active: Optional[bool] = None,
    received_at: Optional[datetime] = None,
) -> MarketDatasetManifest:
    payloads = fetch_polymarket_markets(limit=limit, offset=offset, active=active)
    return store.ingest_market_payloads(payloads, received_at=received_at)
