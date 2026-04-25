from __future__ import annotations

from .base import CLIContext, parse_json_argument, register_command
from ..execution import ExecutionNotFoundError, ExecutionServiceError


def register(subparsers: object) -> None:
    submit_approved_order = register_command(
        subparsers,
        name="submit-approved-order",
        help_text="Release an approved trade intent through signer-service and live-executor.",
        handler=_submit_approved_order,
    )
    submit_approved_order.add_argument("intent_id")
    submit_approved_order.add_argument("--approval-token", required=True)
    submit_approved_order.add_argument("--submitted-by", default="risk-gateway")
    submit_approved_order.add_argument("--policy-json", default="{}")

    execution_state = register_command(
        subparsers,
        name="get-execution-state",
        help_text="Read the live-execution state associated with a trade intent.",
        handler=_get_execution_state,
    )
    execution_state.add_argument("intent_id")

    execution_record = register_command(
        subparsers,
        name="get-execution-record",
        help_text="Read a submitted live execution record by execution id.",
        handler=_get_execution_record,
    )
    execution_record.add_argument("execution_id")


def _submit_approved_order(context: CLIContext, args: object) -> int:
    policy = parse_json_argument(context, args.policy_json, flag_name="--policy-json")
    try:
        result = context.execution.submit_approved_order(
            args.intent_id,
            approval_token=args.approval_token,
            submitted_by=args.submitted_by,
            policy=policy,
        )
    except ExecutionServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_execution_state(context: CLIContext, args: object) -> int:
    try:
        result = context.execution.get_execution_state(args.intent_id)
    except (ExecutionServiceError, ExecutionNotFoundError) as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_execution_record(context: CLIContext, args: object) -> int:
    try:
        result = context.execution.get_execution_record(args.execution_id)
    except ExecutionNotFoundError as exc:
        context.fail(str(exc))
    return context.emit(result)
