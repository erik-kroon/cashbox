from __future__ import annotations

from .base import CLIContext, parse_duration_seconds, register_command


def register(subparsers: object) -> None:
    system_health = register_command(
        subparsers,
        name="get-system-health",
        help_text="Summarize operator-facing health across ingest, gateway, research, risk, execution, and governance.",
        handler=_get_system_health,
    )
    system_health.add_argument("--dataset-id")
    system_health.add_argument("--stale-after-seconds", type=int, default=3600)


def _get_system_health(context: CLIContext, args: object) -> int:
    result = context.health.get_system_health(
        dataset_id=args.dataset_id,
        stale_after=parse_duration_seconds(args.stale_after_seconds),
    )
    return context.emit(result)
