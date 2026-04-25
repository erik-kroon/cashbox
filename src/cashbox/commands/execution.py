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
    submit_approved_order.add_argument("--policy-json")

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

    record_live_fill = register_command(
        subparsers,
        name="record-live-fill",
        help_text="Record a live fill against an existing execution and update tracked positions.",
        handler=_record_live_fill,
    )
    record_live_fill.add_argument("execution_id")
    record_live_fill.add_argument("--filled-quantity", required=True)
    record_live_fill.add_argument("--fill-price", required=True)
    record_live_fill.add_argument("--recorded-by", default="live-executor")

    strategy_cancel_all = register_command(
        subparsers,
        name="request-strategy-cancel-all",
        help_text="Cancel all open live orders for one experiment.",
        handler=_request_strategy_cancel_all,
    )
    strategy_cancel_all.add_argument("experiment_id")
    strategy_cancel_all.add_argument("--reason", required=True)
    strategy_cancel_all.add_argument("--requested-by", default="ops-oncall")

    global_halt = register_command(
        subparsers,
        name="request-global-halt",
        help_text="Activate a global halt and cancel all reachable open live orders.",
        handler=_request_global_halt,
    )
    global_halt.add_argument("--reason", required=True)
    global_halt.add_argument("--requested-by", default="ops-oncall")

    live_controls = register_command(
        subparsers,
        name="get-live-controls",
        help_text="Read the persisted live control state, including any active global halt.",
        handler=_get_live_controls,
    )

    reconcile_live_state = register_command(
        subparsers,
        name="reconcile-live-state",
        help_text="Compare venue open orders and positions against locally tracked execution state.",
        handler=_reconcile_live_state,
    )
    reconcile_live_state.add_argument("--venue-orders-json", required=True)
    reconcile_live_state.add_argument("--venue-positions-json", required=True)
    reconcile_live_state.add_argument("--reconciled-by", default="ops-oncall")

    reconciliation_snapshot = register_command(
        subparsers,
        name="get-reconciliation-snapshot",
        help_text="Read a persisted reconciliation snapshot by snapshot id.",
        handler=_get_reconciliation_snapshot,
    )
    reconciliation_snapshot.add_argument("snapshot_id")


def _submit_approved_order(context: CLIContext, args: object) -> int:
    if args.policy_json is None:
        policy = context.governance.get_active_policy("execution")["policy"]
    else:
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


def _record_live_fill(context: CLIContext, args: object) -> int:
    try:
        result = context.execution.record_live_fill(
            args.execution_id,
            filled_quantity=args.filled_quantity,
            fill_price=args.fill_price,
            recorded_by=args.recorded_by,
        )
    except ExecutionServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _request_strategy_cancel_all(context: CLIContext, args: object) -> int:
    try:
        result = context.execution.request_strategy_cancel_all(
            args.experiment_id,
            reason=args.reason,
            requested_by=args.requested_by,
        )
    except ExecutionServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _request_global_halt(context: CLIContext, args: object) -> int:
    try:
        result = context.execution.request_global_halt(
            reason=args.reason,
            requested_by=args.requested_by,
        )
    except ExecutionServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_live_controls(context: CLIContext, _args: object) -> int:
    return context.emit(context.execution.get_live_controls())


def _reconcile_live_state(context: CLIContext, args: object) -> int:
    venue_orders = parse_json_argument(context, args.venue_orders_json, flag_name="--venue-orders-json")
    venue_positions = parse_json_argument(context, args.venue_positions_json, flag_name="--venue-positions-json")
    try:
        result = context.execution.reconcile_live_state(
            venue_orders=venue_orders,
            venue_positions=venue_positions,
            reconciled_by=args.reconciled_by,
        )
    except ExecutionServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_reconciliation_snapshot(context: CLIContext, args: object) -> int:
    try:
        result = context.execution.get_reconciliation_snapshot(args.snapshot_id)
    except ExecutionNotFoundError as exc:
        context.fail(str(exc))
    return context.emit(result)
