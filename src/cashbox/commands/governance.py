from __future__ import annotations

from .base import CLIContext, parse_json_argument, register_command
from ..audit import AuditTrailServiceError
from ..execution import ExecutionServiceError
from ..experiments import ExperimentLifecycleError, ExperimentNotFoundError
from ..governance import (
    GovernanceAuthorizationError,
    GovernanceNotFoundError,
    GovernanceServiceError,
)


def register(subparsers: object) -> None:
    bootstrap_subject = register_command(
        subparsers,
        name="bootstrap-governance-subject",
        help_text="Create the initial governance subject and roles for an empty workspace.",
        handler=_bootstrap_governance_subject,
    )
    bootstrap_subject.add_argument("--subject", required=True)
    bootstrap_subject.add_argument("--roles-json", required=True)
    bootstrap_subject.add_argument("--bootstrapped-by", default="system-bootstrap")

    assign_role = register_command(
        subparsers,
        name="assign-governance-role",
        help_text="Assign one governance role to a subject.",
        handler=_assign_governance_role,
    )
    assign_role.add_argument("--subject", required=True)
    assign_role.add_argument("--role", required=True)
    assign_role.add_argument("--granted-by", required=True)

    subject = register_command(
        subparsers,
        name="get-governance-subject",
        help_text="Read the current governance roles for one subject.",
        handler=_get_governance_subject,
    )
    subject.add_argument("subject")

    list_subjects = register_command(
        subparsers,
        name="list-governance-subjects",
        help_text="List all governance subjects and roles.",
        handler=_list_governance_subjects,
    )

    strategy_promotion = register_command(
        subparsers,
        name="request-strategy-promotion",
        help_text="Create a governance approval request to promote one strategy to production.",
        handler=_request_strategy_promotion,
    )
    strategy_promotion.add_argument("experiment_id")
    strategy_promotion.add_argument("--requested-by", required=True)
    strategy_promotion.add_argument("--reason", required=True)
    strategy_promotion.add_argument("--target-status", default="PRODUCTION_APPROVED")

    policy_change = register_command(
        subparsers,
        name="request-policy-change",
        help_text="Create a governance approval request for a risk or execution policy change.",
        handler=_request_policy_change,
    )
    policy_change.add_argument("--policy-type", required=True, choices=("risk", "execution"))
    policy_change.add_argument("--updates-json", required=True)
    policy_change.add_argument("--requested-by", required=True)
    policy_change.add_argument("--reason", required=True)

    governance_request = register_command(
        subparsers,
        name="get-governance-request",
        help_text="Read one governance approval request.",
        handler=_get_governance_request,
    )
    governance_request.add_argument("request_id")

    review_request = register_command(
        subparsers,
        name="review-governance-request",
        help_text="Approve or reject a governance approval request.",
        handler=_review_governance_request,
    )
    review_request.add_argument("request_id")
    review_request.add_argument("--reviewer", required=True)
    review_request.add_argument("--decision", required=True, choices=("approve", "reject"))
    review_request.add_argument("--reason", required=True)

    apply_request = register_command(
        subparsers,
        name="apply-governance-request",
        help_text="Apply an approved governance request.",
        handler=_apply_governance_request,
    )
    apply_request.add_argument("request_id")
    apply_request.add_argument("--applied-by", required=True)

    active_policy = register_command(
        subparsers,
        name="get-active-policy",
        help_text="Read the currently active governance-managed policy snapshot.",
        handler=_get_active_policy,
    )
    active_policy.add_argument("policy_type", choices=("risk", "execution"))

    policy_version = register_command(
        subparsers,
        name="get-policy-version",
        help_text="Read one policy version from the governance store.",
        handler=_get_policy_version,
    )
    policy_version.add_argument("policy_type", choices=("risk", "execution"))
    policy_version.add_argument("version", type=int)

    audit_console = register_command(
        subparsers,
        name="get-audit-console",
        help_text="Aggregate governance, risk, execution, and gateway audit events.",
        handler=_get_audit_console,
    )
    audit_console.add_argument("--service")
    audit_console.add_argument("--actor")
    audit_console.add_argument("--status")
    audit_console.add_argument("--limit", type=int)

    audit_timeline = register_command(
        subparsers,
        name="get-audit-timeline",
        help_text="Build a chronological domain timeline filtered by experiment, market, intent, decision, execution, or request id.",
        handler=_get_audit_timeline,
    )
    audit_timeline.add_argument("--experiment-id")
    audit_timeline.add_argument("--market-id")
    audit_timeline.add_argument("--intent-id")
    audit_timeline.add_argument("--decision-id")
    audit_timeline.add_argument("--execution-id")
    audit_timeline.add_argument("--request-id")
    audit_timeline.add_argument("--limit", type=int)

    emergency_halt = register_command(
        subparsers,
        name="request-emergency-halt",
        help_text="Trigger a governance-controlled global halt through the execution service.",
        handler=_request_emergency_halt,
    )
    emergency_halt.add_argument("--requested-by", required=True)
    emergency_halt.add_argument("--reason", required=True)


def _bootstrap_governance_subject(context: CLIContext, args: object) -> int:
    roles = parse_json_argument(context, args.roles_json, flag_name="--roles-json")
    try:
        result = context.governance.bootstrap_subject(
            args.subject,
            roles=roles,
            bootstrapped_by=args.bootstrapped_by,
        )
    except GovernanceServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _assign_governance_role(context: CLIContext, args: object) -> int:
    try:
        result = context.governance.assign_role(
            args.subject,
            role=args.role,
            granted_by=args.granted_by,
        )
    except GovernanceServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_governance_subject(context: CLIContext, args: object) -> int:
    try:
        result = context.governance.get_subject(args.subject)
    except GovernanceNotFoundError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _list_governance_subjects(context: CLIContext, _args: object) -> int:
    return context.emit(context.governance.list_subjects())


def _request_strategy_promotion(context: CLIContext, args: object) -> int:
    try:
        result = context.governance.request_strategy_promotion(
            args.experiment_id,
            requested_by=args.requested_by,
            reason=args.reason,
            target_status=args.target_status,
        )
    except (
        ExperimentLifecycleError,
        ExperimentNotFoundError,
        GovernanceAuthorizationError,
        GovernanceServiceError,
    ) as exc:
        context.fail(str(exc))
    return context.emit(result)


def _request_policy_change(context: CLIContext, args: object) -> int:
    updates = parse_json_argument(context, args.updates_json, flag_name="--updates-json")
    try:
        result = context.governance.request_policy_change(
            args.policy_type,
            updates,
            requested_by=args.requested_by,
            reason=args.reason,
        )
    except GovernanceServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_governance_request(context: CLIContext, args: object) -> int:
    try:
        result = context.governance.get_request(args.request_id)
    except GovernanceNotFoundError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _review_governance_request(context: CLIContext, args: object) -> int:
    try:
        result = context.governance.review_request(
            args.request_id,
            reviewer=args.reviewer,
            decision=args.decision,
            reason=args.reason,
        )
    except GovernanceServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _apply_governance_request(context: CLIContext, args: object) -> int:
    try:
        result = context.governance.apply_request(
            args.request_id,
            applied_by=args.applied_by,
        )
    except (ExperimentLifecycleError, GovernanceServiceError) as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_active_policy(context: CLIContext, args: object) -> int:
    try:
        result = context.governance.get_active_policy(args.policy_type)
    except GovernanceServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_policy_version(context: CLIContext, args: object) -> int:
    try:
        result = context.governance.get_policy_version(args.policy_type, args.version)
    except GovernanceNotFoundError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_audit_console(context: CLIContext, args: object) -> int:
    try:
        result = context.audit.list_audit_events(
            service=args.service,
            actor=args.actor,
            status=args.status,
            limit=args.limit,
        )
    except AuditTrailServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _get_audit_timeline(context: CLIContext, args: object) -> int:
    try:
        result = context.audit.get_audit_timeline(
            experiment_id=args.experiment_id,
            market_id=args.market_id,
            intent_id=args.intent_id,
            decision_id=args.decision_id,
            execution_id=args.execution_id,
            request_id=args.request_id,
            limit=args.limit,
        )
    except AuditTrailServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _request_emergency_halt(context: CLIContext, args: object) -> int:
    try:
        result = context.governance.request_emergency_halt(
            requested_by=args.requested_by,
            reason=args.reason,
        )
    except (ExecutionServiceError, GovernanceServiceError) as exc:
        context.fail(str(exc))
    return context.emit(result)
