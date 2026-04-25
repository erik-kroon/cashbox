from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
from pathlib import Path
import secrets
from typing import Any, Optional

from .models import MarketFilter, format_datetime, parse_datetime, utc_now
from .persistence import append_jsonl, canonical_json, read_json, read_jsonl, write_json
from .research import ResearchMarketReadPath

READ_ONLY_TOOL_NAMES = (
    "get_ingest_health",
    "get_market_metadata",
    "get_market_timeseries",
    "list_active_markets",
)

_ALLOWED_TIMESERIES_FIELDS = {
    "active",
    "archived",
    "category",
    "closed",
    "enable_order_book",
    "end_time",
    "event_id",
    "liquidity",
    "question",
    "resolution_source",
    "source_market_id",
    "source_received_at",
    "volume",
}
_FORBIDDEN_ARGUMENT_SNIPPETS = (
    "$(",
    "../",
    "/etc/",
    "/bin/sh",
    "&&",
    ";/",
    ";",
    "<script",
    "bash ",
    "password",
    "private key",
    "risk-gateway",
    "secret",
    "signer-service",
    "ssh ",
    "vault",
    "zsh ",
    "|",
    "`",
)

def _sha256_json(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class AgentGatewayError(Exception):
    code = "agent_gateway_error"


class AgentAuthenticationError(AgentGatewayError):
    code = "authentication_failed"


class AgentAuthorizationError(AgentGatewayError):
    code = "authorization_failed"


class AgentInputError(AgentGatewayError):
    code = "invalid_arguments"


class AgentRateLimitError(AgentGatewayError):
    code = "rate_limited"


class AgentExecutionError(AgentGatewayError):
    code = "internal_error"


@dataclass(frozen=True)
class AgentGatewayCredential:
    credential_id: str
    subject: str
    token_sha256: str
    allowed_tools: tuple[str, ...]
    rate_limit_count: int
    rate_limit_window_seconds: int
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "credential_id": self.credential_id,
            "subject": self.subject,
            "token_sha256": self.token_sha256,
            "allowed_tools": list(self.allowed_tools),
            "rate_limit_count": self.rate_limit_count,
            "rate_limit_window_seconds": self.rate_limit_window_seconds,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "AgentGatewayCredential":
        return cls(
            credential_id=str(payload["credential_id"]),
            subject=str(payload["subject"]),
            token_sha256=str(payload["token_sha256"]),
            allowed_tools=tuple(sorted(str(item) for item in payload["allowed_tools"])),
            rate_limit_count=int(payload["rate_limit_count"]),
            rate_limit_window_seconds=int(payload["rate_limit_window_seconds"]),
            created_at=str(payload["created_at"]),
        )


@dataclass
class FileSystemAgentGatewayStore:
    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def gateway_dir(self) -> Path:
        return self.root / "gateway"

    @property
    def credentials_dir(self) -> Path:
        return self.gateway_dir / "credentials"

    @property
    def audit_path(self) -> Path:
        return self.gateway_dir / "audit.jsonl"

    def credential_path(self, token_sha256: str) -> Path:
        return self.credentials_dir / f"{token_sha256}.json"

    def issue_credential(
        self,
        *,
        subject: str,
        allowed_tools: tuple[str, ...],
        rate_limit_count: int,
        rate_limit_window_seconds: int,
        token: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> tuple[AgentGatewayCredential, str]:
        raw_token = token or secrets.token_urlsafe(24)
        token_sha256 = _sha256_text(raw_token)
        credential = AgentGatewayCredential(
            credential_id=token_sha256[:16],
            subject=subject,
            token_sha256=token_sha256,
            allowed_tools=tuple(sorted(allowed_tools)),
            rate_limit_count=rate_limit_count,
            rate_limit_window_seconds=rate_limit_window_seconds,
            created_at=format_datetime(now or utc_now()) or "",
        )
        path = self.credential_path(token_sha256)
        path.parent.mkdir(parents=True, exist_ok=True)
        write_json(path, credential.to_dict())
        return credential, raw_token

    def load_credential(self, token: str) -> AgentGatewayCredential:
        token_sha256 = _sha256_text(token)
        path = self.credential_path(token_sha256)
        if not path.exists():
            raise AgentAuthenticationError("unknown gateway credential")
        credential = AgentGatewayCredential.from_dict(read_json(path))
        if credential.token_sha256 != token_sha256:
            raise AgentAuthenticationError("invalid gateway credential")
        return credential

    def append_audit_record(self, payload: dict[str, Any]) -> None:
        append_jsonl(self.audit_path, payload)

    def load_audit_records(self) -> list[dict[str, Any]]:
        return read_jsonl(self.audit_path)

    def count_recent_calls(self, *, credential_id: str, since: datetime) -> int:
        threshold = format_datetime(since) or ""
        count = 0
        for row in self.load_audit_records():
            if row.get("credential_id") != credential_id:
                continue
            if str(row.get("called_at", "")) < threshold:
                continue
            count += 1
        return count


class AgentMarketGateway:
    def __init__(self, store: FileSystemAgentGatewayStore, read_path: ResearchMarketReadPath) -> None:
        self.store = store
        self.read_path = read_path

    def issue_read_only_credential(
        self,
        *,
        subject: str,
        allowed_tools: Optional[tuple[str, ...]] = None,
        rate_limit_count: int = 60,
        rate_limit_window_seconds: int = 60,
        token: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> tuple[AgentGatewayCredential, str]:
        tools = READ_ONLY_TOOL_NAMES if allowed_tools is None else tuple(sorted(set(allowed_tools)))
        invalid_tools = sorted(set(tools) - set(READ_ONLY_TOOL_NAMES))
        if invalid_tools:
            raise ValueError(f"unsupported gateway tools: {', '.join(invalid_tools)}")
        if rate_limit_count < 1:
            raise ValueError("rate_limit_count must be positive")
        if rate_limit_window_seconds < 1:
            raise ValueError("rate_limit_window_seconds must be positive")
        cleaned_subject = self._sanitize_text("subject", subject, max_length=120, allow_spaces=True)
        return self.store.issue_credential(
            subject=cleaned_subject,
            allowed_tools=tools,
            rate_limit_count=rate_limit_count,
            rate_limit_window_seconds=rate_limit_window_seconds,
            token=token,
            now=now,
        )

    def call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        token: str,
        user_id: str,
        session_id: str,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        called_at = now or utc_now()
        response_payload: dict[str, Any] = {
            "ok": False,
            "error": {
                "code": AgentExecutionError.code,
                "message": "gateway execution failed",
            },
            "tool_name": tool_name,
        }
        credential: Optional[AgentGatewayCredential] = None

        try:
            credential = self.store.load_credential(token)
            self._authorize(credential, tool_name)
            self._enforce_rate_limit(credential, called_at)
            sanitized_arguments = self._sanitize_arguments(tool_name, arguments)
            try:
                result = self._dispatch(tool_name, sanitized_arguments)
            except (FileNotFoundError, KeyError, ValueError) as exc:
                raise AgentInputError(str(exc)) from exc
            response_payload = {
                "ok": True,
                "result": result,
                "tool_name": tool_name,
            }
            return response_payload
        except AgentGatewayError as exc:
            response_payload = {
                "ok": False,
                "error": {
                    "code": exc.code,
                    "message": str(exc),
                },
                "tool_name": tool_name,
            }
            raise
        finally:
            self._audit(
                tool_name=tool_name,
                arguments=arguments,
                response_payload=response_payload,
                credential=credential,
                user_id=user_id,
                session_id=session_id,
                called_at=called_at,
            )

    def _authorize(self, credential: AgentGatewayCredential, tool_name: str) -> None:
        if tool_name not in READ_ONLY_TOOL_NAMES:
            raise AgentAuthorizationError(f"tool is not exposed by the gateway: {tool_name}")
        if tool_name not in credential.allowed_tools:
            raise AgentAuthorizationError(f"credential is not allowed to call tool: {tool_name}")

    def _enforce_rate_limit(self, credential: AgentGatewayCredential, called_at: datetime) -> None:
        since = called_at - timedelta(seconds=credential.rate_limit_window_seconds)
        recent_count = self.store.count_recent_calls(credential_id=credential.credential_id, since=since)
        if recent_count >= credential.rate_limit_count:
            raise AgentRateLimitError(
                f"rate limit exceeded for credential {credential.credential_id}: "
                f"{credential.rate_limit_count} calls per {credential.rate_limit_window_seconds}s"
            )

    def _dispatch(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        if tool_name == "list_active_markets":
            return self.read_path.list_active_markets(
                MarketFilter(
                    category=arguments.get("category"),
                    query=arguments.get("query"),
                    active_only=arguments.get("active_only", True),
                    limit=arguments.get("limit"),
                ),
                dataset_id=arguments.get("dataset_id"),
            )
        if tool_name == "get_market_metadata":
            return self.read_path.get_market_metadata(
                arguments["market_id"],
                dataset_id=arguments.get("dataset_id"),
            )
        if tool_name == "get_market_timeseries":
            return self.read_path.get_market_timeseries(
                arguments["market_id"],
                start=parse_datetime(arguments.get("start")),
                end=parse_datetime(arguments.get("end")),
                fields=arguments.get("fields"),
            )
        if tool_name == "get_ingest_health":
            return self.read_path.get_ingest_health(
                dataset_id=arguments.get("dataset_id"),
                stale_after=timedelta(seconds=arguments.get("stale_after_seconds", 3600)),
            ).to_dict()
        raise AgentAuthorizationError(f"tool is not exposed by the gateway: {tool_name}")

    def _sanitize_arguments(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(arguments, dict):
            raise AgentInputError("gateway arguments must be a JSON object")

        allowed_fields = {
            "list_active_markets": {"active_only", "category", "dataset_id", "limit", "query"},
            "get_market_metadata": {"dataset_id", "market_id"},
            "get_market_timeseries": {"end", "fields", "market_id", "start"},
            "get_ingest_health": {"dataset_id", "stale_after_seconds"},
        }
        if tool_name not in allowed_fields:
            raise AgentAuthorizationError(f"tool is not exposed by the gateway: {tool_name}")

        unexpected_fields = sorted(set(arguments) - allowed_fields[tool_name])
        if unexpected_fields:
            raise AgentInputError(f"unexpected gateway argument(s): {', '.join(unexpected_fields)}")

        if tool_name == "list_active_markets":
            payload: dict[str, Any] = {}
            if "category" in arguments:
                payload["category"] = self._sanitize_text("category", arguments["category"], max_length=80)
            if "query" in arguments:
                payload["query"] = self._sanitize_text("query", arguments["query"], max_length=120, allow_spaces=True)
            if "dataset_id" in arguments:
                payload["dataset_id"] = self._sanitize_text("dataset_id", arguments["dataset_id"], max_length=64)
            if "active_only" in arguments:
                if not isinstance(arguments["active_only"], bool):
                    raise AgentInputError("active_only must be a boolean")
                payload["active_only"] = arguments["active_only"]
            if "limit" in arguments:
                payload["limit"] = self._sanitize_int("limit", arguments["limit"], minimum=1, maximum=250)
            return payload

        if tool_name == "get_market_metadata":
            payload = {
                "market_id": self._sanitize_market_id(arguments.get("market_id")),
            }
            if "dataset_id" in arguments:
                payload["dataset_id"] = self._sanitize_text("dataset_id", arguments["dataset_id"], max_length=64)
            return payload

        if tool_name == "get_market_timeseries":
            payload = {
                "market_id": self._sanitize_market_id(arguments.get("market_id")),
            }
            if "start" in arguments:
                payload["start"] = self._sanitize_datetime("start", arguments["start"])
            if "end" in arguments:
                payload["end"] = self._sanitize_datetime("end", arguments["end"])
            if "fields" in arguments:
                payload["fields"] = self._sanitize_fields(arguments["fields"])
            return payload

        payload = {}
        if "dataset_id" in arguments:
            payload["dataset_id"] = self._sanitize_text("dataset_id", arguments["dataset_id"], max_length=64)
        if "stale_after_seconds" in arguments:
            payload["stale_after_seconds"] = self._sanitize_int(
                "stale_after_seconds",
                arguments["stale_after_seconds"],
                minimum=1,
                maximum=86400,
            )
        return payload

    def _sanitize_market_id(self, value: Any) -> str:
        cleaned = self._sanitize_text("market_id", value, max_length=120)
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:")
        if any(character not in allowed for character in cleaned):
            raise AgentInputError("market_id contains unsupported characters")
        return cleaned

    def _sanitize_datetime(self, field_name: str, value: Any) -> str:
        cleaned = self._sanitize_text(field_name, value, max_length=64)
        parsed = parse_datetime(cleaned)
        if parsed is None:
            raise AgentInputError(f"{field_name} must be an ISO-8601 datetime")
        return format_datetime(parsed) or cleaned

    def _sanitize_fields(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            raise AgentInputError("fields must be a list of strings")
        sanitized: list[str] = []
        for item in value:
            field_name = self._sanitize_text("fields", item, max_length=40)
            if field_name not in _ALLOWED_TIMESERIES_FIELDS:
                raise AgentInputError(f"unsupported timeseries field: {field_name}")
            sanitized.append(field_name)
        return sanitized

    def _sanitize_int(self, field_name: str, value: Any, *, minimum: int, maximum: int) -> int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise AgentInputError(f"{field_name} must be an integer")
        if value < minimum or value > maximum:
            raise AgentInputError(f"{field_name} must be between {minimum} and {maximum}")
        return value

    def _sanitize_text(
        self,
        field_name: str,
        value: Any,
        *,
        max_length: int,
        allow_spaces: bool = False,
    ) -> str:
        if not isinstance(value, str):
            raise AgentInputError(f"{field_name} must be a string")
        cleaned = value.strip()
        if not cleaned:
            raise AgentInputError(f"{field_name} must not be empty")
        if len(cleaned) > max_length:
            raise AgentInputError(f"{field_name} exceeds max length {max_length}")
        if any(ord(character) < 32 for character in cleaned):
            raise AgentInputError(f"{field_name} contains control characters")
        lowered = cleaned.lower()
        if cleaned.startswith("/") or cleaned.startswith("~/"):
            raise AgentInputError(f"{field_name} looks like a filesystem path")
        if any(snippet in lowered for snippet in _FORBIDDEN_ARGUMENT_SNIPPETS):
            raise AgentInputError(f"{field_name} contains forbidden content")
        if not allow_spaces and " " in cleaned:
            raise AgentInputError(f"{field_name} must not contain spaces")
        return cleaned

    def _audit(
        self,
        *,
        tool_name: str,
        arguments: dict[str, Any],
        response_payload: dict[str, Any],
        credential: Optional[AgentGatewayCredential],
        user_id: str,
        session_id: str,
        called_at: datetime,
    ) -> None:
        self.store.append_audit_record(
            {
                "arguments_sha256": _sha256_json(arguments),
                "called_at": format_datetime(called_at),
                "credential_id": None if credential is None else credential.credential_id,
                "response_sha256": _sha256_json(response_payload),
                "session_id": session_id,
                "status": "ok" if response_payload.get("ok") else response_payload["error"]["code"],
                "subject": None if credential is None else credential.subject,
                "tool_name": tool_name,
                "user_id": user_id,
            }
        )


def build_agent_gateway(root: Path) -> AgentMarketGateway:
    from .runtime import build_workspace

    return build_workspace(root).gateway
