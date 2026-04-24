from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import MarketDatasetManifest, NormalizedMarketRecord, format_datetime, utc_now

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
