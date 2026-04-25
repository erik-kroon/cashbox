from __future__ import annotations

from .base import CLIContext, register_command
from ..backtests import BacktestServiceError
from ..experiments import ExperimentServiceError
from ..paper import PaperServiceError


def register(subparsers: object) -> None:
    start_paper = register_command(
        subparsers,
        name="start-paper-strategy",
        help_text="Start a paper-trading run from post-backtest market history and persist drift metrics.",
        handler=_start_paper_strategy,
    )
    start_paper.add_argument("experiment_id")
    start_paper.add_argument("--run-id")
    start_paper.add_argument("--started-by", default="paper-executor")

    stop_paper = register_command(
        subparsers,
        name="stop-paper-strategy",
        help_text="Stop the active paper run for an experiment and finalize paper-promotion state.",
        handler=_stop_paper_strategy,
    )
    stop_paper.add_argument("experiment_id")
    stop_paper.add_argument("--stopped-by", default="paper-executor")

    state = register_command(
        subparsers,
        name="get-paper-state",
        help_text="Read the latest paper-trading state for an experiment.",
        handler=_get_paper_state,
    )
    state.add_argument("experiment_id")

    results = register_command(
        subparsers,
        name="get-paper-results",
        help_text="Read persisted results for a paper run.",
        handler=_get_paper_results,
    )
    results.add_argument("paper_run_id")

    drift = register_command(
        subparsers,
        name="analyze-paper-vs-backtest-drift",
        help_text="Read the persisted paper-vs-backtest drift report for an experiment.",
        handler=_analyze_paper_vs_backtest_drift,
    )
    drift.add_argument("experiment_id")
    drift.add_argument("--paper-run-id")


def _start_paper_strategy(context: CLIContext, args: object) -> int:
    try:
        result = context.paper.start_paper_strategy(
            args.experiment_id,
            run_id=args.run_id,
            started_by=args.started_by,
        )
    except (BacktestServiceError, ExperimentServiceError, PaperServiceError) as exc:
        context.fail(str(exc))
    return context.emit(result)


def _stop_paper_strategy(context: CLIContext, args: object) -> int:
    try:
        result = context.paper.stop_paper_strategy(
            args.experiment_id,
            stopped_by=args.stopped_by,
        )
    except (ExperimentServiceError, PaperServiceError) as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_paper_state(context: CLIContext, args: object) -> int:
    try:
        result = context.paper.get_paper_state(args.experiment_id)
    except PaperServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_paper_results(context: CLIContext, args: object) -> int:
    try:
        result = context.paper.get_paper_results(args.paper_run_id)
    except PaperServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _analyze_paper_vs_backtest_drift(context: CLIContext, args: object) -> int:
    try:
        result = context.paper.analyze_paper_vs_backtest_drift(
            args.experiment_id,
            paper_run_id=args.paper_run_id,
        )
    except PaperServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)
