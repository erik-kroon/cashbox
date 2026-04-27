from __future__ import annotations

from .base import CLIContext, parse_datetime, parse_duration_seconds, register_command
from ..models import MarketFilter


def register(subparsers: object) -> None:
    active = register_command(
        subparsers,
        name="list-active-markets",
        help_text="List sanitized active markets from the latest dataset.",
        handler=_list_active_markets,
    )
    active.add_argument("--category")
    active.add_argument("--query")
    active.add_argument("--limit", type=int)
    active.add_argument("--include-inactive", action="store_true")
    active.add_argument("--dataset-id")

    metadata = register_command(
        subparsers,
        name="get-market-metadata",
        help_text="Read sanitized market metadata.",
        handler=_get_market_metadata,
    )
    metadata.add_argument("market_id")
    metadata.add_argument("--dataset-id")

    timeseries = register_command(
        subparsers,
        name="get-market-timeseries",
        help_text="Read append-only market history.",
        handler=_get_market_timeseries,
    )
    timeseries.add_argument("market_id")
    timeseries.add_argument("--start")
    timeseries.add_argument("--end")
    timeseries.add_argument("--field", action="append", dest="fields")

    top_of_book = register_command(
        subparsers,
        name="get-top-of-book",
        help_text="Read the latest or point-in-time normalized top-of-book for a CLOB token.",
        handler=_get_top_of_book,
    )
    top_of_book.add_argument("token_id")
    top_of_book.add_argument("--at")
    top_of_book.add_argument("--depth", type=int)

    book_history = register_command(
        subparsers,
        name="get-order-book-history",
        help_text="Read normalized CLOB order-book snapshots by token and time window.",
        handler=_get_order_book_history,
    )
    book_history.add_argument("token_id")
    book_history.add_argument("--start")
    book_history.add_argument("--end")
    book_history.add_argument("--depth", type=int)

    trade_history = register_command(
        subparsers,
        name="get-trade-history",
        help_text="Read normalized CLOB trade history by market or token and time window.",
        handler=_get_trade_history,
    )
    trade_history.add_argument("--market-id")
    trade_history.add_argument("--token-id")
    trade_history.add_argument("--start")
    trade_history.add_argument("--end")
    trade_history.add_argument("--limit", type=int)

    book_health = register_command(
        subparsers,
        name="get-book-health",
        help_text="Summarize stale or missing CLOB book coverage separately from metadata ingest.",
        handler=_get_book_health,
    )
    book_health.add_argument("--dataset-id")
    book_health.add_argument("--stale-after-seconds", type=int, default=300)

    health = register_command(
        subparsers,
        name="get-ingest-health",
        help_text="Summarize dataset freshness.",
        handler=_get_ingest_health,
    )
    health.add_argument("--dataset-id")
    health.add_argument("--stale-after-seconds", type=int, default=3600)


def _list_active_markets(context: CLIContext, args: object) -> int:
    result = context.read_path.list_active_markets(
        MarketFilter(
            category=args.category,
            query=args.query,
            active_only=not args.include_inactive,
            limit=args.limit,
        ),
        dataset_id=args.dataset_id,
    )
    return context.emit(result)


def _get_market_metadata(context: CLIContext, args: object) -> int:
    result = context.read_path.get_market_metadata(args.market_id, dataset_id=args.dataset_id)
    return context.emit(result)


def _get_market_timeseries(context: CLIContext, args: object) -> int:
    result = context.read_path.get_market_timeseries(
        args.market_id,
        start=parse_datetime(args.start),
        end=parse_datetime(args.end),
        fields=args.fields,
    )
    return context.emit(result)


def _get_top_of_book(context: CLIContext, args: object) -> int:
    result = context.read_path.get_top_of_book(
        args.token_id,
        at=parse_datetime(args.at),
        depth=args.depth,
    )
    return context.emit(result)


def _get_order_book_history(context: CLIContext, args: object) -> int:
    result = context.read_path.get_order_book_history(
        args.token_id,
        start=parse_datetime(args.start),
        end=parse_datetime(args.end),
        depth=args.depth,
    )
    return context.emit(result)


def _get_trade_history(context: CLIContext, args: object) -> int:
    result = context.read_path.get_trade_history(
        market_id=args.market_id,
        token_id=args.token_id,
        start=parse_datetime(args.start),
        end=parse_datetime(args.end),
        limit=args.limit,
    )
    return context.emit(result)


def _get_book_health(context: CLIContext, args: object) -> int:
    result = context.read_path.get_book_health(
        dataset_id=args.dataset_id,
        stale_after=parse_duration_seconds(args.stale_after_seconds),
    )
    return context.emit(result)


def _get_ingest_health(context: CLIContext, args: object) -> int:
    result = context.read_path.get_ingest_health(
        dataset_id=args.dataset_id,
        stale_after=parse_duration_seconds(args.stale_after_seconds),
    )
    return context.emit(result.to_dict())
