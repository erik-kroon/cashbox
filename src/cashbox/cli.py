from __future__ import annotations

import argparse
from pathlib import Path

from .commands import register_all
from .commands.base import build_context


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cashbox market ingest and research read path.")
    parser.add_argument("--root", type=Path, default=Path(".cashbox/market-data"), help="Storage root.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    register_all(subparsers)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    handler = getattr(args, "_cashbox_handler", None)
    if handler is None:
        parser.error(f"unknown command: {getattr(args, 'command', None)}")
    context = build_context(root=args.root, parser=parser)
    return handler(context, args)
