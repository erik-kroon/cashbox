from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


def to_decimal(value: str | int | float | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


DEFAULT_FEE_RATES = {
    "crypto": Decimal("0.072"),
    "finance": Decimal("0.04"),
    "politics": Decimal("0.04"),
    "mentions": Decimal("0.04"),
    "tech": Decimal("0.04"),
    "geopolitics": Decimal("0.0"),
}


@dataclass(frozen=True)
class TopOfBook:
    bid: Decimal
    ask: Decimal
    bid_size: Decimal
    ask_size: Decimal

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "TopOfBook":
        return cls(
            bid=to_decimal(payload["bid"]),
            ask=to_decimal(payload["ask"]),
            bid_size=to_decimal(payload["bid_size"]),
            ask_size=to_decimal(payload["ask_size"]),
        )


@dataclass(frozen=True)
class BinaryMarketSnapshot:
    market_id: str
    category: str
    yes: TopOfBook
    no: TopOfBook

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "BinaryMarketSnapshot":
        return cls(
            market_id=str(payload["market_id"]),
            category=str(payload["category"]),
            yes=TopOfBook.from_dict(payload["yes"]),
            no=TopOfBook.from_dict(payload["no"]),
        )


@dataclass(frozen=True)
class BasketLegSnapshot:
    market_id: str
    title: str
    threshold: int
    yes: TopOfBook


@dataclass(frozen=True)
class NegRiskEventSnapshot:
    event_id: str
    title: str
    category: str
    legs: tuple[BasketLegSnapshot, ...]


@dataclass(frozen=True)
class FeeSchedule:
    taker_fee_rate: Decimal

    @classmethod
    def for_category(cls, category: str) -> "FeeSchedule":
        normalized = category.strip().lower()
        rate = DEFAULT_FEE_RATES.get(normalized, Decimal("0.04"))
        return cls(taker_fee_rate=rate)

    def taker_fee(self, *, shares: Decimal, price: Decimal) -> Decimal:
        return shares * self.taker_fee_rate * price * (Decimal("1") - price)


@dataclass(frozen=True)
class RiskBuffer:
    slippage: Decimal = Decimal("0")
    precision_buffer: Decimal = Decimal("0")
    safety_margin: Decimal = Decimal("0")
    min_edge: Decimal = Decimal("0")

    @classmethod
    def from_values(
        cls,
        *,
        slippage: str | int | float | Decimal = Decimal("0"),
        precision_buffer: str | int | float | Decimal = Decimal("0"),
        safety_margin: str | int | float | Decimal = Decimal("0"),
        min_edge: str | int | float | Decimal = Decimal("0"),
    ) -> "RiskBuffer":
        return cls(
            slippage=to_decimal(slippage),
            precision_buffer=to_decimal(precision_buffer),
            safety_margin=to_decimal(safety_margin),
            min_edge=to_decimal(min_edge),
        )

    @property
    def total(self) -> Decimal:
        return self.slippage + self.precision_buffer + self.safety_margin + self.min_edge


@dataclass(frozen=True)
class Opportunity:
    market_id: str
    side: str
    quantity: Decimal
    gross_edge_per_share: Decimal
    net_edge_per_share: Decimal
    expected_pnl: Decimal
    detail: str | None = None
