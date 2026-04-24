from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import BasketLegSnapshot, BinaryMarketSnapshot, NegRiskEventSnapshot, TopOfBook

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"


@dataclass(frozen=True)
class PolymarketBinaryMarket:
    market_id: str
    question: str
    category: str
    yes_token_id: str
    no_token_id: str
    group_item_title: str
    group_item_threshold: int | None


@dataclass(frozen=True)
class PolymarketNegRiskEvent:
    event_id: str
    title: str
    category: str
    markets: tuple[PolymarketBinaryMarket, ...]


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


def _normalize_category(value: Any) -> str:
    if value is None:
        return "unknown"
    normalized = str(value).strip()
    return normalized or "unknown"


def _parse_threshold(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(str(value))


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
        category=_normalize_category(payload.get("category")),
        yes_token_id=outcome_map["yes"],
        no_token_id=outcome_map["no"],
        group_item_title=str(payload.get("groupItemTitle") or payload.get("question") or market_id),
        group_item_threshold=_parse_threshold(payload.get("groupItemThreshold")),
    )


def _parse_neg_risk_event(payload: dict[str, Any]) -> PolymarketNegRiskEvent | None:
    if payload.get("closed") or not payload.get("active") or not payload.get("negRisk"):
        return None

    raw_markets = payload.get("markets")
    if not isinstance(raw_markets, list) or len(raw_markets) < 2:
        return None

    markets: list[PolymarketBinaryMarket] = []
    for item in raw_markets:
        market = _parse_binary_market(item)
        if market is None or market.group_item_threshold is None:
            return None
        markets.append(market)

    sorted_markets = sorted(markets, key=lambda market: market.group_item_threshold or 0)
    thresholds = [market.group_item_threshold for market in sorted_markets]
    if thresholds != list(range(len(sorted_markets))):
        return None

    category = _normalize_category(payload.get("category"))
    if category == "unknown":
        category = sorted_markets[0].category

    event_id = str(payload.get("slug") or payload.get("id"))
    return PolymarketNegRiskEvent(
        event_id=event_id,
        title=str(payload.get("title") or event_id),
        category=category,
        markets=tuple(sorted_markets),
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


def list_neg_risk_events(*, limit: int = 25, offset: int = 0, category: str | None = None) -> list[PolymarketNegRiskEvent]:
    payload = _request_json(
        GAMMA_API,
        "/events",
        {
            "active": "true",
            "closed": "false",
            "negRisk": "true",
            "limit": limit,
            "offset": offset,
        },
    )

    events: list[PolymarketNegRiskEvent] = []
    for item in payload:
        event = _parse_neg_risk_event(item)
        if event is None:
            continue
        if category and event.category.strip().lower() != category.strip().lower():
            continue
        events.append(event)
    return events


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


def snapshot_from_neg_risk_event(event: PolymarketNegRiskEvent) -> NegRiskEventSnapshot | None:
    legs: list[BasketLegSnapshot] = []
    for market in event.markets:
        yes = fetch_top_of_book(market.yes_token_id)
        if yes is None:
            return None
        assert market.group_item_threshold is not None
        legs.append(
            BasketLegSnapshot(
                market_id=market.market_id,
                title=market.group_item_title,
                threshold=market.group_item_threshold,
                yes=yes,
            )
        )

    return NegRiskEventSnapshot(
        event_id=event.event_id,
        title=event.title,
        category=event.category,
        legs=tuple(legs),
    )


def load_live_snapshots(
    *,
    limit: int = 50,
    offset: int = 0,
    category: str | None = None,
    max_workers: int | None = None,
) -> list[BinaryMarketSnapshot]:
    markets = list_binary_markets(limit=limit, offset=offset, category=category)
    if not markets:
        return []

    worker_count = max_workers or min(16, len(markets))
    snapshots_by_market_id: dict[str, BinaryMarketSnapshot] = {}

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_market_id = {executor.submit(snapshot_from_market, market): market.market_id for market in markets}
        for future in as_completed(future_to_market_id):
            try:
                snapshot = future.result()
            except Exception:
                continue
            if snapshot is not None:
                snapshots_by_market_id[snapshot.market_id] = snapshot

    return [snapshots_by_market_id[market.market_id] for market in markets if market.market_id in snapshots_by_market_id]


def load_live_neg_risk_events(
    *,
    limit: int = 25,
    offset: int = 0,
    category: str | None = None,
    max_workers: int | None = None,
) -> list[NegRiskEventSnapshot]:
    events = list_neg_risk_events(limit=limit, offset=offset, category=category)
    if not events:
        return []

    worker_count = max_workers or min(16, len(events))
    snapshots_by_event_id: dict[str, NegRiskEventSnapshot] = {}

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_event_id = {executor.submit(snapshot_from_neg_risk_event, event): event.event_id for event in events}
        for future in as_completed(future_to_event_id):
            try:
                snapshot = future.result()
            except Exception:
                continue
            if snapshot is not None:
                snapshots_by_event_id[snapshot.event_id] = snapshot

    return [snapshots_by_event_id[event.event_id] for event in events if event.event_id in snapshots_by_event_id]
