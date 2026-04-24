from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable

from .models import BinaryMarketSnapshot, RiskBuffer
from .polymarket import load_live_neg_risk_events, load_live_snapshots
from .scanner import rank_opportunities, scan_neg_risk_events, scan_snapshots


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scan binary markets for full-set arbitrage.")
    parser.add_argument("input", nargs="?", type=Path, help="Path to a JSON file of market snapshots.")
    parser.add_argument(
        "--polymarket-live",
        action="store_true",
        help="Fetch active binary markets from Polymarket public APIs instead of reading a file.",
    )
    parser.add_argument("--limit", type=int, default=25, help="How many live markets to request.")
    parser.add_argument("--offset", type=int, default=0, help="Live-market pagination offset.")
    parser.add_argument("--category", help="Optional live-market category filter.")
    parser.add_argument(
        "--include-neg-risk-baskets",
        action="store_true",
        help="Also scan exhaustive negative-risk event baskets from the Polymarket events API.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.0,
        help="Seconds to wait between live polls. A positive value keeps scanning until interrupted.",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        help="Optional cap on live polls. Requires --poll-interval and is mainly useful for testing.",
    )
    parser.add_argument("--slippage", default="0", help="Per-share slippage buffer.")
    parser.add_argument("--precision-buffer", default="0", help="Per-share precision buffer.")
    parser.add_argument("--safety-margin", default="0", help="Per-share safety margin.")
    parser.add_argument("--min-edge", default="0", help="Additional required edge per share.")
    return parser


def format_decimal(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.000001")), "f")


def format_opportunity(opportunity, *, rank: int | None = None) -> str:
    prefix = f"{rank}. " if rank is not None else ""
    detail = f" {opportunity.detail}" if getattr(opportunity, "detail", None) else ""
    return (
        f"{prefix}{opportunity.market_id} {opportunity.side} "
        f"qty={opportunity.quantity} "
        f"gross={format_decimal(opportunity.gross_edge_per_share)} "
        f"net={format_decimal(opportunity.net_edge_per_share)} "
        f"pnl={format_decimal(opportunity.expected_pnl)}"
        f"{detail}"
    )


def print_opportunities(opportunities, *, output: Callable[[str], None] = print) -> None:
    if not opportunities:
        output("No opportunities found.")
        return

    for opportunity in opportunities:
        output(format_opportunity(opportunity))


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def emit_live_scan_results(
    opportunities,
    *,
    scan_number: int,
    scanned_at: datetime,
    output: Callable[[str], None] = print,
) -> None:
    output(f"scan={scan_number} at={_format_timestamp(scanned_at)} opportunities={len(opportunities)}")
    if not opportunities:
        output("No opportunities found.")
        return

    for rank, opportunity in enumerate(opportunities, start=1):
        output(format_opportunity(opportunity, rank=rank))


def emit_live_scan_error(
    error: Exception,
    *,
    scan_number: int,
    scanned_at: datetime,
    output: Callable[[str], None] = print,
) -> None:
    output(f"scan={scan_number} at={_format_timestamp(scanned_at)} status=error error={error}")


def run_live_scan_loop(
    *,
    risk: RiskBuffer,
    limit: int,
    offset: int,
    category: str | None,
    include_neg_risk_baskets: bool,
    poll_interval: float,
    max_iterations: int | None = None,
    snapshot_loader: Callable[..., list[BinaryMarketSnapshot]] = load_live_snapshots,
    neg_risk_loader: Callable[..., list] = load_live_neg_risk_events,
    output: Callable[[str], None] = print,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> int:
    iteration = 0
    while True:
        iteration += 1
        scanned_at = clock()
        try:
            opportunities = scan_snapshots(snapshot_loader(limit=limit, offset=offset, category=category), risk=risk)
            if include_neg_risk_baskets:
                opportunities = rank_opportunities(
                    opportunities
                    + scan_neg_risk_events(
                        neg_risk_loader(limit=limit, offset=offset, category=category),
                        risk=risk,
                    )
                )
            emit_live_scan_results(opportunities, scan_number=iteration, scanned_at=scanned_at, output=output)
        except Exception as error:
            emit_live_scan_error(error, scan_number=iteration, scanned_at=scanned_at, output=output)

        if max_iterations is not None and iteration >= max_iterations:
            return 0

        sleep(poll_interval)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not args.polymarket_live and args.input is None:
        parser.error("input is required unless --polymarket-live is set")
    if args.poll_interval < 0:
        parser.error("--poll-interval must be non-negative")
    if args.max_iterations is not None and args.max_iterations <= 0:
        parser.error("--max-iterations must be positive")
    if not args.polymarket_live and args.poll_interval > 0:
        parser.error("--poll-interval requires --polymarket-live")
    if not args.polymarket_live and args.include_neg_risk_baskets:
        parser.error("--include-neg-risk-baskets requires --polymarket-live")
    if args.max_iterations is not None and args.poll_interval <= 0:
        parser.error("--max-iterations requires a positive --poll-interval")

    risk = RiskBuffer.from_values(
        slippage=args.slippage,
        precision_buffer=args.precision_buffer,
        safety_margin=args.safety_margin,
        min_edge=args.min_edge,
    )

    if args.polymarket_live:
        if args.poll_interval > 0:
            return run_live_scan_loop(
                risk=risk,
                limit=args.limit,
                offset=args.offset,
                category=args.category,
                include_neg_risk_baskets=args.include_neg_risk_baskets,
                poll_interval=args.poll_interval,
                max_iterations=args.max_iterations,
            )
        opportunities = scan_snapshots(
            load_live_snapshots(limit=args.limit, offset=args.offset, category=args.category),
            risk=risk,
        )
        if args.include_neg_risk_baskets:
            opportunities = rank_opportunities(
                opportunities
                + scan_neg_risk_events(
                    load_live_neg_risk_events(limit=args.limit, offset=args.offset, category=args.category),
                    risk=risk,
                )
            )
        print_opportunities(opportunities)
        return 0
    else:
        payload = json.loads(args.input.read_text())
        snapshots = [BinaryMarketSnapshot.from_dict(item) for item in payload]

    print_opportunities(scan_snapshots(snapshots, risk=risk))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
