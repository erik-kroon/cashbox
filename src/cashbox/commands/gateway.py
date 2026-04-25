from __future__ import annotations

from .base import CLIContext, parse_json_argument, register_command
from ..gateway import AgentGatewayError


def register(subparsers: object) -> None:
    credential = register_command(
        subparsers,
        name="issue-agent-credential",
        help_text="Issue a scoped credential for the read-only agent gateway.",
        handler=_issue_agent_credential,
    )
    credential.add_argument("--subject", required=True)
    credential.add_argument("--allow-tool", action="append", dest="allowed_tools")
    credential.add_argument("--rate-limit-count", type=int, default=60)
    credential.add_argument("--rate-limit-window-seconds", type=int, default=60)
    credential.add_argument("--token", help="Optional fixed token for local development or tests.")

    gateway_call = register_command(
        subparsers,
        name="gateway-call",
        help_text="Call a read-only market tool through the scoped agent gateway.",
        handler=_gateway_call,
    )
    gateway_call.add_argument("tool_name")
    gateway_call.add_argument("--token", required=True)
    gateway_call.add_argument("--args-json", default="{}")
    gateway_call.add_argument("--user-id", required=True)
    gateway_call.add_argument("--session-id", required=True)


def _issue_agent_credential(context: CLIContext, args: object) -> int:
    credential, raw_token = context.gateway.issue_read_only_credential(
        subject=args.subject,
        allowed_tools=None if args.allowed_tools is None else tuple(args.allowed_tools),
        rate_limit_count=args.rate_limit_count,
        rate_limit_window_seconds=args.rate_limit_window_seconds,
        token=args.token,
    )
    return context.emit(
        {
            "allowed_tools": list(credential.allowed_tools),
            "created_at": credential.created_at,
            "credential_id": credential.credential_id,
            "rate_limit_count": credential.rate_limit_count,
            "rate_limit_window_seconds": credential.rate_limit_window_seconds,
            "subject": credential.subject,
            "token": raw_token,
        }
    )


def _gateway_call(context: CLIContext, args: object) -> int:
    tool_arguments = parse_json_argument(context, args.args_json, flag_name="--args-json")
    try:
        result = context.gateway.call_tool(
            args.tool_name,
            tool_arguments,
            token=args.token,
            user_id=args.user_id,
            session_id=args.session_id,
        )
    except AgentGatewayError as exc:
        context.fail(str(exc))
    return context.emit(result)
