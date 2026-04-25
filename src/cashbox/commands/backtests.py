from __future__ import annotations

from .base import CLIContext, parse_json_argument, register_command
from ..backtests import BacktestServiceError
from ..experiments import ExperimentServiceError


def register(subparsers: object) -> None:
    run_backtest = register_command(
        subparsers,
        name="run-backtest",
        help_text="Execute a deterministic backtest for a validated experiment against its immutable dataset.",
        handler=_run_backtest,
    )
    run_backtest.add_argument("experiment_id")
    run_backtest.add_argument("--dataset-id")
    run_backtest.add_argument("--assumptions-json", required=True)

    artifacts = register_command(
        subparsers,
        name="get-backtest-artifacts",
        help_text="Read persisted artifacts for a backtest run.",
        handler=_get_backtest_artifacts,
    )
    artifacts.add_argument("run_id")

    failure = register_command(
        subparsers,
        name="explain-backtest-failure",
        help_text="Explain why a persisted backtest run failed.",
        handler=_explain_backtest_failure,
    )
    failure.add_argument("run_id")


def _run_backtest(context: CLIContext, args: object) -> int:
    assumptions = parse_json_argument(context, args.assumptions_json, flag_name="--assumptions-json")
    try:
        result = context.backtests.run_backtest(
            args.experiment_id,
            dataset_id=args.dataset_id,
            assumptions=assumptions,
        )
    except (BacktestServiceError, ExperimentServiceError) as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_backtest_artifacts(context: CLIContext, args: object) -> int:
    try:
        result = context.backtests.get_backtest_artifacts(args.run_id)
    except BacktestServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _explain_backtest_failure(context: CLIContext, args: object) -> int:
    try:
        result = context.backtests.explain_backtest_failure(args.run_id)
    except BacktestServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)
