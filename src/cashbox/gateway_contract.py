from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

from .models import MarketFilter, format_datetime, parse_datetime
from .research import RESEARCH_TIMESERIES_FIELDS, ResearchMarketReader

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


class GatewayToolContractError(Exception):
    pass


class GatewayToolAuthorizationError(GatewayToolContractError):
    pass


class GatewayToolInputError(GatewayToolContractError):
    pass


class GatewayArgumentSanitizer:
    def text(
        self,
        field_name: str,
        value: Any,
        *,
        max_length: int,
        allow_spaces: bool = False,
    ) -> str:
        return sanitize_gateway_text(
            field_name,
            value,
            max_length=max_length,
            allow_spaces=allow_spaces,
        )

    def market_id(self, value: Any) -> str:
        cleaned = self.text("market_id", value, max_length=120)
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:")
        if any(character not in allowed for character in cleaned):
            raise GatewayToolInputError("market_id contains unsupported characters")
        return cleaned

    def token_id(self, value: Any) -> str:
        cleaned = self.text("token_id", value, max_length=160)
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:")
        if any(character not in allowed for character in cleaned):
            raise GatewayToolInputError("token_id contains unsupported characters")
        return cleaned

    def datetime_text(self, field_name: str, value: Any) -> str:
        cleaned = self.text(field_name, value, max_length=64)
        parsed = parse_datetime(cleaned)
        if parsed is None:
            raise GatewayToolInputError(f"{field_name} must be an ISO-8601 datetime")
        return format_datetime(parsed) or cleaned

    def int_value(self, field_name: str, value: Any, *, minimum: int, maximum: int) -> int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise GatewayToolInputError(f"{field_name} must be an integer")
        if value < minimum or value > maximum:
            raise GatewayToolInputError(f"{field_name} must be between {minimum} and {maximum}")
        return value

    def fields(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            raise GatewayToolInputError("fields must be a list of strings")
        sanitized: list[str] = []
        for item in value:
            field_name = self.text("fields", item, max_length=40)
            if field_name not in RESEARCH_TIMESERIES_FIELDS:
                raise GatewayToolInputError(f"unsupported timeseries field: {field_name}")
            sanitized.append(field_name)
        return sanitized


NormalizeToolArguments = Callable[[GatewayArgumentSanitizer, dict[str, Any]], dict[str, Any]]
DispatchTool = Callable[[ResearchMarketReader, dict[str, Any]], Any]


@dataclass(frozen=True)
class GatewayToolDefinition:
    name: str
    allowed_fields: frozenset[str]
    normalize: NormalizeToolArguments
    dispatch: DispatchTool
    audit_name: str


class GatewayToolContract:
    def __init__(self, definitions: tuple[GatewayToolDefinition, ...]) -> None:
        self._definitions = {definition.name: definition for definition in definitions}
        self._sanitizer = GatewayArgumentSanitizer()

    @property
    def tool_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._definitions))

    def has_tool(self, tool_name: str) -> bool:
        return tool_name in self._definitions

    def audit_name(self, tool_name: str) -> str:
        definition = self._definitions.get(tool_name)
        if definition is None:
            return tool_name
        return definition.audit_name

    def normalize_arguments(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(arguments, dict):
            raise GatewayToolInputError("gateway arguments must be a JSON object")
        definition = self._definition(tool_name)
        unexpected_fields = sorted(set(arguments) - definition.allowed_fields)
        if unexpected_fields:
            raise GatewayToolInputError(f"unexpected gateway argument(s): {', '.join(unexpected_fields)}")
        return definition.normalize(self._sanitizer, arguments)

    def dispatch(self, read_path: ResearchMarketReader, tool_name: str, arguments: dict[str, Any]) -> Any:
        return self._definition(tool_name).dispatch(read_path, arguments)

    def _definition(self, tool_name: str) -> GatewayToolDefinition:
        try:
            return self._definitions[tool_name]
        except KeyError as exc:
            raise GatewayToolAuthorizationError(f"tool is not exposed by the gateway: {tool_name}") from exc


def sanitize_gateway_text(
    field_name: str,
    value: Any,
    *,
    max_length: int,
    allow_spaces: bool = False,
) -> str:
    if not isinstance(value, str):
        raise GatewayToolInputError(f"{field_name} must be a string")
    cleaned = value.strip()
    if not cleaned:
        raise GatewayToolInputError(f"{field_name} must not be empty")
    if len(cleaned) > max_length:
        raise GatewayToolInputError(f"{field_name} exceeds max length {max_length}")
    if any(ord(character) < 32 for character in cleaned):
        raise GatewayToolInputError(f"{field_name} contains control characters")
    lowered = cleaned.lower()
    if cleaned.startswith("/") or cleaned.startswith("~/"):
        raise GatewayToolInputError(f"{field_name} looks like a filesystem path")
    if any(snippet in lowered for snippet in _FORBIDDEN_ARGUMENT_SNIPPETS):
        raise GatewayToolInputError(f"{field_name} contains forbidden content")
    if not allow_spaces and " " in cleaned:
        raise GatewayToolInputError(f"{field_name} must not contain spaces")
    return cleaned


def _normalize_list_active_markets(
    sanitizer: GatewayArgumentSanitizer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if "category" in arguments:
        payload["category"] = sanitizer.text("category", arguments["category"], max_length=80)
    if "query" in arguments:
        payload["query"] = sanitizer.text("query", arguments["query"], max_length=120, allow_spaces=True)
    if "dataset_id" in arguments:
        payload["dataset_id"] = sanitizer.text("dataset_id", arguments["dataset_id"], max_length=64)
    if "active_only" in arguments:
        if not isinstance(arguments["active_only"], bool):
            raise GatewayToolInputError("active_only must be a boolean")
        payload["active_only"] = arguments["active_only"]
    if "limit" in arguments:
        payload["limit"] = sanitizer.int_value("limit", arguments["limit"], minimum=1, maximum=250)
    return payload


def _dispatch_list_active_markets(read_path: ResearchMarketReader, arguments: dict[str, Any]) -> Any:
    return read_path.list_active_markets(
        MarketFilter(
            category=arguments.get("category"),
            query=arguments.get("query"),
            active_only=arguments.get("active_only", True),
            limit=arguments.get("limit"),
        ),
        dataset_id=arguments.get("dataset_id"),
    )


def _normalize_get_market_metadata(
    sanitizer: GatewayArgumentSanitizer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    payload = {"market_id": sanitizer.market_id(arguments.get("market_id"))}
    if "dataset_id" in arguments:
        payload["dataset_id"] = sanitizer.text("dataset_id", arguments["dataset_id"], max_length=64)
    return payload


def _dispatch_get_market_metadata(read_path: ResearchMarketReader, arguments: dict[str, Any]) -> Any:
    return read_path.get_market_metadata(
        arguments["market_id"],
        dataset_id=arguments.get("dataset_id"),
    )


def _normalize_get_market_timeseries(
    sanitizer: GatewayArgumentSanitizer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    payload = {"market_id": sanitizer.market_id(arguments.get("market_id"))}
    if "start" in arguments:
        payload["start"] = sanitizer.datetime_text("start", arguments["start"])
    if "end" in arguments:
        payload["end"] = sanitizer.datetime_text("end", arguments["end"])
    if "fields" in arguments:
        payload["fields"] = sanitizer.fields(arguments["fields"])
    return payload


def _dispatch_get_market_timeseries(read_path: ResearchMarketReader, arguments: dict[str, Any]) -> Any:
    return read_path.get_market_timeseries(
        arguments["market_id"],
        start=parse_datetime(arguments.get("start")),
        end=parse_datetime(arguments.get("end")),
        fields=arguments.get("fields"),
    )


def _normalize_get_top_of_book(
    sanitizer: GatewayArgumentSanitizer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    payload = {"token_id": sanitizer.token_id(arguments.get("token_id"))}
    if "at" in arguments:
        payload["at"] = sanitizer.datetime_text("at", arguments["at"])
    if "depth" in arguments:
        payload["depth"] = sanitizer.int_value("depth", arguments["depth"], minimum=1, maximum=100)
    return payload


def _dispatch_get_top_of_book(read_path: ResearchMarketReader, arguments: dict[str, Any]) -> Any:
    return read_path.get_top_of_book(
        arguments["token_id"],
        at=parse_datetime(arguments.get("at")),
        depth=arguments.get("depth"),
    )


def _normalize_get_order_book_history(
    sanitizer: GatewayArgumentSanitizer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    payload = {"token_id": sanitizer.token_id(arguments.get("token_id"))}
    if "start" in arguments:
        payload["start"] = sanitizer.datetime_text("start", arguments["start"])
    if "end" in arguments:
        payload["end"] = sanitizer.datetime_text("end", arguments["end"])
    if "depth" in arguments:
        payload["depth"] = sanitizer.int_value("depth", arguments["depth"], minimum=1, maximum=100)
    return payload


def _dispatch_get_order_book_history(read_path: ResearchMarketReader, arguments: dict[str, Any]) -> Any:
    return read_path.get_order_book_history(
        arguments["token_id"],
        start=parse_datetime(arguments.get("start")),
        end=parse_datetime(arguments.get("end")),
        depth=arguments.get("depth"),
    )


def _normalize_get_trade_history(
    sanitizer: GatewayArgumentSanitizer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    payload = {}
    if "market_id" in arguments:
        payload["market_id"] = sanitizer.market_id(arguments["market_id"])
    if "token_id" in arguments:
        payload["token_id"] = sanitizer.token_id(arguments["token_id"])
    if "market_id" not in payload and "token_id" not in payload:
        raise GatewayToolInputError("market_id or token_id is required")
    if "start" in arguments:
        payload["start"] = sanitizer.datetime_text("start", arguments["start"])
    if "end" in arguments:
        payload["end"] = sanitizer.datetime_text("end", arguments["end"])
    if "limit" in arguments:
        payload["limit"] = sanitizer.int_value("limit", arguments["limit"], minimum=1, maximum=1000)
    return payload


def _dispatch_get_trade_history(read_path: ResearchMarketReader, arguments: dict[str, Any]) -> Any:
    return read_path.get_trade_history(
        market_id=arguments.get("market_id"),
        token_id=arguments.get("token_id"),
        start=parse_datetime(arguments.get("start")),
        end=parse_datetime(arguments.get("end")),
        limit=arguments.get("limit"),
    )


def _normalize_health_arguments(
    sanitizer: GatewayArgumentSanitizer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    payload = {}
    if "dataset_id" in arguments:
        payload["dataset_id"] = sanitizer.text("dataset_id", arguments["dataset_id"], max_length=64)
    if "stale_after_seconds" in arguments:
        payload["stale_after_seconds"] = sanitizer.int_value(
            "stale_after_seconds",
            arguments["stale_after_seconds"],
            minimum=1,
            maximum=86400,
        )
    return payload


def _dispatch_get_book_health(read_path: ResearchMarketReader, arguments: dict[str, Any]) -> Any:
    return read_path.get_book_health(
        dataset_id=arguments.get("dataset_id"),
        stale_after=timedelta(seconds=arguments.get("stale_after_seconds", 300)),
    )


def _dispatch_get_ingest_health(read_path: ResearchMarketReader, arguments: dict[str, Any]) -> Any:
    return read_path.get_ingest_health(
        dataset_id=arguments.get("dataset_id"),
        stale_after=timedelta(seconds=arguments.get("stale_after_seconds", 3600)),
    ).to_dict()


READ_ONLY_GATEWAY_TOOL_CONTRACT = GatewayToolContract(
    (
        GatewayToolDefinition(
            name="get_book_health",
            allowed_fields=frozenset({"dataset_id", "stale_after_seconds"}),
            normalize=_normalize_health_arguments,
            dispatch=_dispatch_get_book_health,
            audit_name="get_book_health",
        ),
        GatewayToolDefinition(
            name="get_ingest_health",
            allowed_fields=frozenset({"dataset_id", "stale_after_seconds"}),
            normalize=_normalize_health_arguments,
            dispatch=_dispatch_get_ingest_health,
            audit_name="get_ingest_health",
        ),
        GatewayToolDefinition(
            name="get_market_metadata",
            allowed_fields=frozenset({"dataset_id", "market_id"}),
            normalize=_normalize_get_market_metadata,
            dispatch=_dispatch_get_market_metadata,
            audit_name="get_market_metadata",
        ),
        GatewayToolDefinition(
            name="get_market_timeseries",
            allowed_fields=frozenset({"end", "fields", "market_id", "start"}),
            normalize=_normalize_get_market_timeseries,
            dispatch=_dispatch_get_market_timeseries,
            audit_name="get_market_timeseries",
        ),
        GatewayToolDefinition(
            name="get_order_book_history",
            allowed_fields=frozenset({"depth", "end", "start", "token_id"}),
            normalize=_normalize_get_order_book_history,
            dispatch=_dispatch_get_order_book_history,
            audit_name="get_order_book_history",
        ),
        GatewayToolDefinition(
            name="get_top_of_book",
            allowed_fields=frozenset({"at", "depth", "token_id"}),
            normalize=_normalize_get_top_of_book,
            dispatch=_dispatch_get_top_of_book,
            audit_name="get_top_of_book",
        ),
        GatewayToolDefinition(
            name="get_trade_history",
            allowed_fields=frozenset({"end", "limit", "market_id", "start", "token_id"}),
            normalize=_normalize_get_trade_history,
            dispatch=_dispatch_get_trade_history,
            audit_name="get_trade_history",
        ),
        GatewayToolDefinition(
            name="list_active_markets",
            allowed_fields=frozenset({"active_only", "category", "dataset_id", "limit", "query"}),
            normalize=_normalize_list_active_markets,
            dispatch=_dispatch_list_active_markets,
            audit_name="list_active_markets",
        ),
    )
)
READ_ONLY_TOOL_NAMES = READ_ONLY_GATEWAY_TOOL_CONTRACT.tool_names
