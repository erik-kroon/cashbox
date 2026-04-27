from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .market_history import FileSystemMarketHistory, fetch_polymarket_markets, ingest_polymarket_markets


@dataclass
class FileSystemMarketStore(FileSystemMarketHistory):
    """Compatibility facade for market-history persistence and ingest."""

    root: Path


__all__ = [
    "FileSystemMarketStore",
    "fetch_polymarket_markets",
    "ingest_polymarket_markets",
]
