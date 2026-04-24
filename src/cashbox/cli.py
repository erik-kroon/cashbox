from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path

from .models import BinaryMarketSnapshot, RiskBuffer
from .polymarket import load_live_snapshots
from .scanner import scan_market


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
    parser.add_argument("--slippage", default="0", help="Per-share slippage buffer.")
    parser.add_argument("--precision-buffer", default="0", help="Per-share precision buffer.")
    parser.add_argument("--safety-margin", default="0", help="Per-share safety margin.")
    parser.add_argument("--min-edge", default="0", help="Additional required edge per share.")
    return parser


def format_decimal(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.000001")), "f")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not args.polymarket_live and args.input is None:
        parser.error("input is required unless --polymarket-live is set")

    risk = RiskBuffer.from_values(
        slippage=args.slippage,
        precision_buffer=args.precision_buffer,
        safety_margin=args.safety_margin,
        min_edge=args.min_edge,
    )

    if args.polymarket_live:
        snapshots = load_live_snapshots(limit=args.limit, offset=args.offset, category=args.category)
    else:
        payload = json.loads(args.input.read_text())
        snapshots = [BinaryMarketSnapshot.from_dict(item) for item in payload]

    total = 0
    for snapshot in snapshots:
        for opportunity in scan_market(snapshot, risk=risk):
            total += 1
            print(
                f"{opportunity.market_id} {opportunity.side} "
                f"qty={opportunity.quantity} "
                f"gross={format_decimal(opportunity.gross_edge_per_share)} "
                f"net={format_decimal(opportunity.net_edge_per_share)} "
                f"pnl={format_decimal(opportunity.expected_pnl)}"
            )

    if total == 0:
        print("No opportunities found.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
