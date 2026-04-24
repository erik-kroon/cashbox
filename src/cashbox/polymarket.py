from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import BinaryMarketSnapshot, TopOfBook

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"


@dataclass(frozen=True)
class PolymarketBinaryMarket:
    market_id: str
    question: str
    category: str
    yes_token_id: str
    no_token_id: str


def _request_json(base_url: str, path: str, params: dict[str, Any]) -> Any:
    query = urlencode({key: value for key, value in params.items() if value is not None})
    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{query}"

    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "cashbox/0.1 (+https://github.com/erik-kroon/cashbox)",
        },
    )

    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _parse_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    raise ValueError(f"Expected list-like value, got {value!r}")


def _normalize_outcome(outcome: str) -> str:
    return outcome.strip().lower()


def _parse_binary_market(payload: dict[str, Any]) -> PolymarketBinaryMarket | None:
    if not payload.get("enableOrderBook") or payload.get("closed") or not payload.get("active"):
        return None

    outcomes = _parse_json_list(payload["outcomes"])
    token_ids = _parse_json_list(payload["clobTokenIds"])
    if len(outcomes) != 2 or len(token_ids) != 2:
        return None

    outcome_map = {_normalize_outcome(outcome): token_id for outcome, token_id in zip(outcomes, token_ids)}
    if "yes" not in outcome_map or "no" not in outcome_map:
        return None

    market_id = str(payload.get("slug") or payload.get("conditionId") or payload.get("id"))
    return PolymarketBinaryMarket(
        market_id=market_id,
        question=str(payload.get("question", "")),
        category=str(payload.get("category", "unknown")),
        yes_token_id=outcome_map["yes"],
        no_token_id=outcome_map["no"],
    )


def list_binary_markets(*, limit: int = 50, offset: int = 0, category: str | None = None) -> list[PolymarketBinaryMarket]:
    payload = _request_json(
        GAMMA_API,
        "/markets",
        {
            "active": "true",
            "closed": "false",
            "limit": limit,
            "offset": offset,
        },
    )

    markets: list[PolymarketBinaryMarket] = []
    for item in payload:
        market = _parse_binary_market(item)
        if market is None:
            continue
        if category and market.category.strip().lower() != category.strip().lower():
            continue
        markets.append(market)
    return markets


def _best_level(levels: list[dict[str, Any]], *, side: str) -> tuple[Decimal, Decimal] | None:
    if not levels:
        return None

    normalized = [(Decimal(str(level["price"])), Decimal(str(level["size"]))) for level in levels]
    if side == "bid":
        return max(normalized, key=lambda level: level[0])
    if side == "ask":
        return min(normalized, key=lambda level: level[0])
    raise ValueError(f"Unsupported side: {side}")


def fetch_top_of_book(token_id: str) -> TopOfBook | None:
    payload = _request_json(CLOB_API, "/book", {"token_id": token_id})
    best_bid = _best_level(payload.get("bids", []), side="bid")
    best_ask = _best_level(payload.get("asks", []), side="ask")
    if best_bid is None or best_ask is None:
        return None

    bid_price, bid_size = best_bid
    ask_price, ask_size = best_ask
    return TopOfBook(bid=bid_price, ask=ask_price, bid_size=bid_size, ask_size=ask_size)


def snapshot_from_market(market: PolymarketBinaryMarket) -> BinaryMarketSnapshot | None:
    yes = fetch_top_of_book(market.yes_token_id)
    no = fetch_top_of_book(market.no_token_id)
    if yes is None or no is None:
        return None

    return BinaryMarketSnapshot(
        market_id=market.market_id,
        category=market.category,
        yes=yes,
        no=no,
    )


def load_live_snapshots(*, limit: int = 50, offset: int = 0, category: str | None = None) -> list[BinaryMarketSnapshot]:
    snapshots = []
    for market in list_binary_markets(limit=limit, offset=offset, category=category):
        snapshot = snapshot_from_market(market)
        if snapshot is not None:
            snapshots.append(snapshot)
    return snapshots
