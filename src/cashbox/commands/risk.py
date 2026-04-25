from __future__ import annotations

from .base import CLIContext, parse_json_argument, register_command
from ..experiments import ExperimentServiceError
from ..risk import RiskNotFoundError, RiskServiceError


def register(subparsers: object) -> None:
    create_trade_intent = register_command(
        subparsers,
        name="create-trade-intent",
        help_text="Persist a structured live-adjacent trade intent for later risk evaluation.",
        handler=_create_trade_intent,
    )
    create_trade_intent.add_argument("experiment_id")
    create_trade_intent.add_argument("--submitted-by", required=True)
    create_trade_intent.add_argument("--order-json", required=True)
    create_trade_intent.add_argument("--rationale")

    get_trade_intent = register_command(
        subparsers,
        name="get-trade-intent",
        help_text="Read a persisted trade intent with its latest state, reviews, and decision.",
        handler=_get_trade_intent,
    )
    get_trade_intent.add_argument("intent_id")

    review_trade_intent = register_command(
        subparsers,
        name="review-trade-intent",
        help_text="Attach a human approve/reject review to a trade intent.",
        handler=_review_trade_intent,
    )
    review_trade_intent.add_argument("intent_id")
    review_trade_intent.add_argument("--reviewer", required=True)
    review_trade_intent.add_argument("--decision", required=True, choices=("approve", "reject"))
    review_trade_intent.add_argument("--reason", required=True)

    evaluate_trade_intent = register_command(
        subparsers,
        name="evaluate-trade-intent",
        help_text="Evaluate a trade intent against deterministic live risk checks and HITL approval status.",
        handler=_evaluate_trade_intent,
    )
    evaluate_trade_intent.add_argument("intent_id")
    evaluate_trade_intent.add_argument("--policy-json", default="{}")
    evaluate_trade_intent.add_argument("--decided-by", default="risk-gateway")

    risk_decision = register_command(
        subparsers,
        name="get-risk-decision",
        help_text="Read a persisted risk decision by decision id.",
        handler=_get_risk_decision,
    )
    risk_decision.add_argument("decision_id")


def _create_trade_intent(context: CLIContext, args: object) -> int:
    order_request = parse_json_argument(context, args.order_json, flag_name="--order-json")
    try:
        result = context.risk.create_trade_intent(
            args.experiment_id,
            order_request,
            submitted_by=args.submitted_by,
            rationale=args.rationale,
        )
    except (ExperimentServiceError, RiskServiceError) as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_trade_intent(context: CLIContext, args: object) -> int:
    try:
        result = context.risk.get_trade_intent(args.intent_id)
    except RiskNotFoundError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _review_trade_intent(context: CLIContext, args: object) -> int:
    try:
        result = context.risk.review_trade_intent(
            args.intent_id,
            reviewer=args.reviewer,
            decision=args.decision,
            reason=args.reason,
        )
    except RiskServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _evaluate_trade_intent(context: CLIContext, args: object) -> int:
    policy = parse_json_argument(context, args.policy_json, flag_name="--policy-json")
    try:
        result = context.risk.evaluate_trade_intent(
            args.intent_id,
            policy=policy,
            decided_by=args.decided_by,
        )
    except (ExperimentServiceError, RiskServiceError) as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_risk_decision(context: CLIContext, args: object) -> int:
    try:
        result = context.risk.get_risk_decision(args.decision_id)
    except RiskNotFoundError as exc:
        context.fail(str(exc))
    return context.emit(result)
