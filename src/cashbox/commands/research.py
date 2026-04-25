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


def _get_ingest_health(context: CLIContext, args: object) -> int:
    result = context.read_path.get_ingest_health(
        dataset_id=args.dataset_id,
        stale_after=parse_duration_seconds(args.stale_after_seconds),
    )
    return context.emit(result.to_dict())
