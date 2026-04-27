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

    ingest_books_file = register_command(
        subparsers,
        name="ingest-clob-books-file",
        help_text="Ingest local CLOB order-book snapshots from a JSON fixture file.",
        handler=_ingest_clob_books_file,
    )
    ingest_books_file.add_argument("input", type=Path)
    ingest_books_file.add_argument("--source-name", default="polymarket-clob")
    ingest_books_file.add_argument("--received-at", help="Override the ingest receive timestamp in ISO-8601.")

    ingest_trades_file = register_command(
        subparsers,
        name="ingest-clob-trades-file",
        help_text="Ingest local CLOB trades from a JSON fixture file.",
        handler=_ingest_clob_trades_file,
    )
    ingest_trades_file.add_argument("input", type=Path)
    ingest_trades_file.add_argument("--source-name", default="polymarket-clob")
    ingest_trades_file.add_argument("--received-at", help="Override the ingest receive timestamp in ISO-8601.")

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


def _load_json_array(context: CLIContext, path: Path) -> list[dict[str, object]]:
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        context.fail(f"invalid input file payload: {exc}")
    if not isinstance(payload, list):
        context.fail("input file must contain a JSON array")
    return [dict(item) for item in payload]


def _ingest_clob_books_file(context: CLIContext, args: object) -> int:
    rows = context.store.ingest_order_book_snapshots(
        _load_json_array(context, args.input),
        source_name=args.source_name,
        received_at=parse_datetime(args.received_at),
    )
    return context.emit({"snapshot_count": len(rows), "snapshots": rows})


def _ingest_clob_trades_file(context: CLIContext, args: object) -> int:
    rows = context.store.ingest_clob_trades(
        _load_json_array(context, args.input),
        source_name=args.source_name,
        received_at=parse_datetime(args.received_at),
    )
    return context.emit({"trade_count": len(rows), "trades": rows})


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
