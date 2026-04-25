from __future__ import annotations

from .base import CLIContext, register_command
from ..backtests import BacktestServiceError
from ..evaluator import EvaluationServiceError, PROMOTION_TARGET_STAGES
from ..experiments import ExperimentServiceError


def register(subparsers: object) -> None:
    score = register_command(
        subparsers,
        name="score-experiment",
        help_text="Compute and persist a deterministic evaluator score for a successful backtest.",
        handler=_score_experiment,
    )
    score.add_argument("experiment_id")
    score.add_argument("--run-id")

    promotion = register_command(
        subparsers,
        name="check-promotion-eligibility",
        help_text="Evaluate a promotion gate and optionally promote a strategy when the gate passes.",
        handler=_check_promotion_eligibility,
    )
    promotion.add_argument("experiment_id")
    promotion.add_argument("--target-stage", required=True, choices=PROMOTION_TARGET_STAGES)
    promotion.add_argument("--run-id")
    promotion.add_argument("--changed-by", default="evaluator")
    promotion.add_argument("--promote-if-eligible", action="store_true")
    promotion.add_argument("--min-out-of-sample-trades", type=int, default=250)
    promotion.add_argument("--min-distinct-markets", type=int, default=25)
    promotion.add_argument("--max-drawdown-limit-usd")


def _score_experiment(context: CLIContext, args: object) -> int:
    try:
        result = context.evaluator.score_experiment(
            args.experiment_id,
            run_id=args.run_id,
        )
    except (BacktestServiceError, EvaluationServiceError, ExperimentServiceError) as exc:
        context.fail(str(exc))
    return context.emit(result)


def _check_promotion_eligibility(context: CLIContext, args: object) -> int:
    try:
        result = context.evaluator.check_promotion_eligibility(
            args.experiment_id,
            args.target_stage,
            run_id=args.run_id,
            changed_by=args.changed_by,
            promote=args.promote_if_eligible,
            min_out_of_sample_trades=args.min_out_of_sample_trades,
            min_distinct_markets=args.min_distinct_markets,
            max_drawdown_limit_usd=args.max_drawdown_limit_usd,
        )
    except (BacktestServiceError, EvaluationServiceError, ExperimentServiceError) as exc:
        context.fail(str(exc))
    return context.emit(result)
