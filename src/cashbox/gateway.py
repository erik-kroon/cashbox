from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
from pathlib import Path
import secrets
from typing import Any, Optional

from .gateway_contract import (
    READ_ONLY_GATEWAY_TOOL_CONTRACT,
    READ_ONLY_TOOL_NAMES,
    GatewayToolAuthorizationError,
    GatewayToolContract,
    GatewayToolInputError,
    sanitize_gateway_text,
)
from .models import format_datetime, utc_now
from .persistence import append_jsonl, canonical_json, read_json, read_jsonl, write_json
from .research import ResearchMarketReader


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
    def __init__(
        self,
        store: FileSystemAgentGatewayStore,
        read_path: ResearchMarketReader,
        *,
        tool_contract: GatewayToolContract = READ_ONLY_GATEWAY_TOOL_CONTRACT,
    ) -> None:
        self.store = store
        self.read_path = read_path
        self.tool_contract = tool_contract

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
        tools = self.tool_contract.tool_names if allowed_tools is None else tuple(sorted(set(allowed_tools)))
        invalid_tools = sorted(set(tools) - set(self.tool_contract.tool_names))
        if invalid_tools:
            raise ValueError(f"unsupported gateway tools: {', '.join(invalid_tools)}")
        if rate_limit_count < 1:
            raise ValueError("rate_limit_count must be positive")
        if rate_limit_window_seconds < 1:
            raise ValueError("rate_limit_window_seconds must be positive")
        try:
            cleaned_subject = sanitize_gateway_text("subject", subject, max_length=120, allow_spaces=True)
        except GatewayToolInputError as exc:
            raise AgentInputError(str(exc)) from exc
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
            try:
                sanitized_arguments = self.tool_contract.normalize_arguments(tool_name, arguments)
            except GatewayToolInputError as exc:
                raise AgentInputError(str(exc)) from exc
            except GatewayToolAuthorizationError as exc:
                raise AgentAuthorizationError(str(exc)) from exc
            try:
                result = self.tool_contract.dispatch(self.read_path, tool_name, sanitized_arguments)
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
        if not self.tool_contract.has_tool(tool_name):
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
                "tool_name": self.tool_contract.audit_name(tool_name),
                "user_id": user_id,
            }
        )


def build_agent_gateway(root: Path) -> AgentMarketGateway:
    from .runtime import build_workspace

    return build_workspace(root).gateway
