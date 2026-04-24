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

DEFAULT_MAKER_REBATE_RATES = {category: Decimal("0") for category in DEFAULT_FEE_RATES}


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
    fair_yes: Decimal | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "BinaryMarketSnapshot":
        fair_yes = payload.get("fair_yes")
        return cls(
            market_id=str(payload["market_id"]),
            category=str(payload["category"]),
            yes=TopOfBook.from_dict(payload["yes"]),
            no=TopOfBook.from_dict(payload["no"]),
            fair_yes=None if fair_yes is None else to_decimal(fair_yes),
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
    maker_rebate_rate: Decimal = Decimal("0")

    @classmethod
    def for_category(cls, category: str) -> "FeeSchedule":
        normalized = category.strip().lower()
        taker_rate = DEFAULT_FEE_RATES.get(normalized, Decimal("0.04"))
        maker_rate = DEFAULT_MAKER_REBATE_RATES.get(normalized, Decimal("0"))
        return cls(taker_fee_rate=taker_rate, maker_rebate_rate=maker_rate)

    def taker_fee(self, *, shares: Decimal, price: Decimal) -> Decimal:
        return shares * self.taker_fee_rate * price * (Decimal("1") - price)

    def maker_rebate(self, *, shares: Decimal, price: Decimal) -> Decimal:
        return shares * self.maker_rebate_rate * price * (Decimal("1") - price)


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
class MakerRiskBuffer:
    adverse_selection: Decimal = Decimal("0")
    inventory_penalty: Decimal = Decimal("0")
    operational_buffer: Decimal = Decimal("0")
    min_edge: Decimal = Decimal("0")

    @classmethod
    def from_values(
        cls,
        *,
        adverse_selection: str | int | float | Decimal = Decimal("0"),
        inventory_penalty: str | int | float | Decimal = Decimal("0"),
        operational_buffer: str | int | float | Decimal = Decimal("0"),
        min_edge: str | int | float | Decimal = Decimal("0"),
    ) -> "MakerRiskBuffer":
        return cls(
            adverse_selection=to_decimal(adverse_selection),
            inventory_penalty=to_decimal(inventory_penalty),
            operational_buffer=to_decimal(operational_buffer),
            min_edge=to_decimal(min_edge),
        )

    @property
    def total(self) -> Decimal:
        return self.adverse_selection + self.inventory_penalty + self.operational_buffer + self.min_edge


@dataclass(frozen=True)
class Opportunity:
    market_id: str
    side: str
    quantity: Decimal
    gross_edge_per_share: Decimal
    net_edge_per_share: Decimal
    expected_pnl: Decimal
    detail: str | None = None
