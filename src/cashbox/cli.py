from __future__ import annotations

import argparse
from datetime import timedelta
import json
from pathlib import Path

from .gateway import AgentGatewayError, build_agent_gateway
from .ingest import FileSystemMarketStore, ingest_polymarket_markets
from .models import MarketFilter, parse_datetime
from .research import ResearchMarketReadPath


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cashbox market ingest and research read path.")
    parser.add_argument("--root", type=Path, default=Path(".cashbox/market-data"), help="Storage root.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_file = subparsers.add_parser("ingest-file", help="Ingest Polymarket-style market payloads from a JSON file.")
    ingest_file.add_argument("input", type=Path)
    ingest_file.add_argument("--source-name", default="polymarket-gamma")
    ingest_file.add_argument("--received-at", help="Override the ingest receive timestamp in ISO-8601.")

    ingest_live = subparsers.add_parser("ingest-polymarket", help="Fetch and ingest markets from Polymarket Gamma.")
    ingest_live.add_argument("--limit", type=int, default=100)
    ingest_live.add_argument("--offset", type=int, default=0)
    ingest_live.add_argument("--active", choices=("true", "false"))
    ingest_live.add_argument("--received-at", help="Override the ingest receive timestamp in ISO-8601.")

    active = subparsers.add_parser("list-active-markets", help="List sanitized active markets from the latest dataset.")
    active.add_argument("--category")
    active.add_argument("--query")
    active.add_argument("--limit", type=int)
    active.add_argument("--include-inactive", action="store_true")
    active.add_argument("--dataset-id")

    metadata = subparsers.add_parser("get-market-metadata", help="Read sanitized market metadata.")
    metadata.add_argument("market_id")
    metadata.add_argument("--dataset-id")

    timeseries = subparsers.add_parser("get-market-timeseries", help="Read append-only market history.")
    timeseries.add_argument("market_id")
    timeseries.add_argument("--start")
    timeseries.add_argument("--end")
    timeseries.add_argument("--field", action="append", dest="fields")

    health = subparsers.add_parser("get-ingest-health", help="Summarize dataset freshness.")
    health.add_argument("--dataset-id")
    health.add_argument("--stale-after-seconds", type=int, default=3600)

    credential = subparsers.add_parser(
        "issue-agent-credential",
        help="Issue a scoped credential for the read-only agent gateway.",
    )
    credential.add_argument("--subject", required=True)
    credential.add_argument("--allow-tool", action="append", dest="allowed_tools")
    credential.add_argument("--rate-limit-count", type=int, default=60)
    credential.add_argument("--rate-limit-window-seconds", type=int, default=60)
    credential.add_argument("--token", help="Optional fixed token for local development or tests.")

    gateway_call = subparsers.add_parser(
        "gateway-call",
        help="Call a read-only market tool through the scoped agent gateway.",
    )
    gateway_call.add_argument("tool_name")
    gateway_call.add_argument("--token", required=True)
    gateway_call.add_argument("--args-json", default="{}")
    gateway_call.add_argument("--user-id", required=True)
    gateway_call.add_argument("--session-id", required=True)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    store = FileSystemMarketStore(args.root)
    read_path = ResearchMarketReadPath(store)
    gateway = build_agent_gateway(args.root)

    if args.command == "ingest-file":
        payload = json.loads(args.input.read_text())
        manifest = store.ingest_market_payloads(
            payload,
            source_name=args.source_name,
            received_at=parse_datetime(args.received_at),
        )
        print(json.dumps(manifest.to_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "ingest-polymarket":
        active = None if args.active is None else args.active == "true"
        manifest = ingest_polymarket_markets(
            store,
            limit=args.limit,
            offset=args.offset,
            active=active,
            received_at=parse_datetime(args.received_at),
        )
        print(json.dumps(manifest.to_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "list-active-markets":
        result = read_path.list_active_markets(
            MarketFilter(
                category=args.category,
                query=args.query,
                active_only=not args.include_inactive,
                limit=args.limit,
            ),
            dataset_id=args.dataset_id,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "get-market-metadata":
        result = read_path.get_market_metadata(args.market_id, dataset_id=args.dataset_id)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "get-market-timeseries":
        result = read_path.get_market_timeseries(
            args.market_id,
            start=parse_datetime(args.start),
            end=parse_datetime(args.end),
            fields=args.fields,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "get-ingest-health":
        result = read_path.get_ingest_health(
            dataset_id=args.dataset_id,
            stale_after=timedelta(seconds=args.stale_after_seconds),
        )
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "issue-agent-credential":
        credential, raw_token = gateway.issue_read_only_credential(
            subject=args.subject,
            allowed_tools=None if args.allowed_tools is None else tuple(args.allowed_tools),
            rate_limit_count=args.rate_limit_count,
            rate_limit_window_seconds=args.rate_limit_window_seconds,
            token=args.token,
        )
        print(
            json.dumps(
                {
                    "allowed_tools": list(credential.allowed_tools),
                    "created_at": credential.created_at,
                    "credential_id": credential.credential_id,
                    "rate_limit_count": credential.rate_limit_count,
                    "rate_limit_window_seconds": credential.rate_limit_window_seconds,
                    "subject": credential.subject,
                    "token": raw_token,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "gateway-call":
        try:
            tool_arguments = json.loads(args.args_json)
            result = gateway.call_tool(
                args.tool_name,
                tool_arguments,
                token=args.token,
                user_id=args.user_id,
                session_id=args.session_id,
            )
        except json.JSONDecodeError as exc:
            parser.error(f"invalid --args-json payload: {exc}")
        except AgentGatewayError as exc:
            print(
                json.dumps(
                    {
                        "error": {
                            "code": exc.code,
                            "message": str(exc),
                        },
                        "ok": False,
                        "tool_name": args.tool_name,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
