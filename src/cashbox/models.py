from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any, Optional, Union


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_datetime(value: Optional[Union[str, datetime]]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized).astimezone(timezone.utc)


def format_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_category(value: Any) -> str:
    if value is None:
        return "unknown"
    normalized = str(value).strip().lower()
    return normalized or "unknown"


def normalize_text(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    normalized = str(value).strip()
    return normalized or default


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def parse_json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    raise ValueError(f"expected list-like value, got {value!r}")


def normalize_numeric_text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class MarketOutcome:
    outcome: str
    token_id: Optional[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome,
            "token_id": self.token_id,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MarketOutcome":
        return cls(
            outcome=str(payload["outcome"]),
            token_id=None if payload.get("token_id") is None else str(payload["token_id"]),
        )


@dataclass(frozen=True)
class NormalizedMarketRecord:
    market_id: str
    event_id: Optional[str]
    question: str
    category: str
    active: bool
    closed: bool
    archived: bool
    enable_order_book: bool
    outcomes: tuple[MarketOutcome, ...]
    resolution_source: Optional[str]
    end_time: Optional[str]
    liquidity: Optional[str]
    volume: Optional[str]
    source_received_at: str
    source_market_id: Optional[str]

    @classmethod
    def from_gamma_payload(cls, payload: dict[str, Any], *, received_at: datetime) -> "NormalizedMarketRecord":
        raw_outcomes = parse_json_list(payload.get("outcomes"))
        raw_token_ids = parse_json_list(payload.get("clobTokenIds"))
        if raw_token_ids and len(raw_token_ids) != len(raw_outcomes):
            raise ValueError("outcome and token counts must match")

        outcomes = tuple(
            MarketOutcome(
                outcome=normalize_text(outcome),
                token_id=raw_token_ids[index] if index < len(raw_token_ids) else None,
            )
            for index, outcome in enumerate(raw_outcomes)
        )

        market_id = normalize_text(payload.get("slug") or payload.get("conditionId") or payload.get("id"))
        if not market_id:
            raise ValueError("market payload must contain an id, slug, or conditionId")

        event_id = payload.get("eventSlug") or payload.get("eventId") or payload.get("seriesSlug")
        resolution_source = payload.get("resolutionSource")
        end_time = payload.get("endDate") or payload.get("endDateIso") or payload.get("expirationDate")

        return cls(
            market_id=market_id,
            event_id=None if event_id is None else normalize_text(event_id),
            question=normalize_text(payload.get("question") or payload.get("title") or market_id, default=market_id),
            category=normalize_category(payload.get("category")),
            active=parse_bool(payload.get("active")),
            closed=parse_bool(payload.get("closed")),
            archived=parse_bool(payload.get("archived")),
            enable_order_book=parse_bool(payload.get("enableOrderBook")),
            outcomes=outcomes,
            resolution_source=None if resolution_source is None else normalize_text(resolution_source),
            end_time=format_datetime(parse_datetime(end_time)),
            liquidity=normalize_numeric_text(payload.get("liquidity") or payload.get("liquidityNum")),
            volume=normalize_numeric_text(payload.get("volume") or payload.get("volumeNum")),
            source_received_at=format_datetime(received_at) or "",
            source_market_id=None if payload.get("id") is None else normalize_text(payload.get("id")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "event_id": self.event_id,
            "question": self.question,
            "category": self.category,
            "active": self.active,
            "closed": self.closed,
            "archived": self.archived,
            "enable_order_book": self.enable_order_book,
            "outcomes": [outcome.to_dict() for outcome in self.outcomes],
            "resolution_source": self.resolution_source,
            "end_time": self.end_time,
            "liquidity": self.liquidity,
            "volume": self.volume,
            "source_received_at": self.source_received_at,
            "source_market_id": self.source_market_id,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NormalizedMarketRecord":
        return cls(
            market_id=str(payload["market_id"]),
            event_id=None if payload.get("event_id") is None else str(payload["event_id"]),
            question=str(payload["question"]),
            category=str(payload["category"]),
            active=bool(payload["active"]),
            closed=bool(payload["closed"]),
            archived=bool(payload["archived"]),
            enable_order_book=bool(payload["enable_order_book"]),
            outcomes=tuple(MarketOutcome.from_dict(item) for item in payload.get("outcomes", [])),
            resolution_source=None if payload.get("resolution_source") is None else str(payload["resolution_source"]),
            end_time=None if payload.get("end_time") is None else str(payload["end_time"]),
            liquidity=None if payload.get("liquidity") is None else str(payload["liquidity"]),
            volume=None if payload.get("volume") is None else str(payload["volume"]),
            source_received_at=str(payload["source_received_at"]),
            source_market_id=None if payload.get("source_market_id") is None else str(payload["source_market_id"]),
        )


@dataclass(frozen=True)
class MarketDatasetManifest:
    dataset_id: str
    source_name: str
    ingested_at: str
    created_at: str
    market_count: int
    raw_payload_sha256: str
    normalized_schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "source_name": self.source_name,
            "ingested_at": self.ingested_at,
            "created_at": self.created_at,
            "market_count": self.market_count,
            "raw_payload_sha256": self.raw_payload_sha256,
            "normalized_schema_version": self.normalized_schema_version,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MarketDatasetManifest":
        return cls(
            dataset_id=str(payload["dataset_id"]),
            source_name=str(payload["source_name"]),
            ingested_at=str(payload["ingested_at"]),
            created_at=str(payload["created_at"]),
            market_count=int(payload["market_count"]),
            raw_payload_sha256=str(payload["raw_payload_sha256"]),
            normalized_schema_version=int(payload.get("normalized_schema_version", 1)),
        )


@dataclass(frozen=True)
class MarketFilter:
    category: Optional[str] = None
    query: Optional[str] = None
    active_only: bool = True
    limit: Optional[int] = None


@dataclass(frozen=True)
class IngestHealthReport:
    dataset_id: str
    source_name: str
    ingested_at: str
    market_count: int
    active_market_count: int
    stale_market_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "source_name": self.source_name,
            "ingested_at": self.ingested_at,
            "market_count": self.market_count,
            "active_market_count": self.active_market_count,
            "stale_market_ids": list(self.stale_market_ids),
        }
