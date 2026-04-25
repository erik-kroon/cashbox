from __future__ import annotations

import json
from pathlib import Path

from .base import CLIContext, parse_datetime, register_command
from ..ingest import ingest_polymarket_markets


def register(subparsers: object) -> None:
    ingest_file = register_command(
        subparsers,
        name="ingest-file",
        help_text="Ingest Polymarket-style market payloads from a JSON file.",
        handler=_ingest_file,
    )
    ingest_file.add_argument("input", type=Path)
    ingest_file.add_argument("--source-name", default="polymarket-gamma")
    ingest_file.add_argument("--received-at", help="Override the ingest receive timestamp in ISO-8601.")

    ingest_live = register_command(
        subparsers,
        name="ingest-polymarket",
        help_text="Fetch and ingest markets from Polymarket Gamma.",
        handler=_ingest_polymarket,
    )
    ingest_live.add_argument("--limit", type=int, default=100)
    ingest_live.add_argument("--offset", type=int, default=0)
    ingest_live.add_argument("--active", choices=("true", "false"))
    ingest_live.add_argument("--received-at", help="Override the ingest receive timestamp in ISO-8601.")


def _ingest_file(context: CLIContext, args: object) -> int:
    try:
        payload = json.loads(args.input.read_text())
    except json.JSONDecodeError as exc:
        context.fail(f"invalid input file payload: {exc}")
    manifest = context.store.ingest_market_payloads(
        payload,
        source_name=args.source_name,
        received_at=parse_datetime(args.received_at),
    )
    return context.emit(manifest.to_dict())


def _ingest_polymarket(context: CLIContext, args: object) -> int:
    active = None if args.active is None else args.active == "true"
    manifest = ingest_polymarket_markets(
        context.store,
        limit=args.limit,
        offset=args.offset,
        active=active,
        received_at=parse_datetime(args.received_at),
    )
    return context.emit(manifest.to_dict())
