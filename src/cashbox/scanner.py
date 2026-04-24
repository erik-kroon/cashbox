from __future__ import annotations

from decimal import Decimal

from .models import BinaryMarketSnapshot, FeeSchedule, Opportunity, RiskBuffer


def _buy_full_set(snapshot: BinaryMarketSnapshot, fees: FeeSchedule, risk: RiskBuffer) -> Opportunity | None:
    quantity = min(snapshot.yes.ask_size, snapshot.no.ask_size)
    if quantity <= 0:
        return None

    gross_edge = Decimal("1") - snapshot.yes.ask - snapshot.no.ask
    fee_total = fees.taker_fee(shares=quantity, price=snapshot.yes.ask) + fees.taker_fee(
        shares=quantity,
        price=snapshot.no.ask,
    )
    net_edge = gross_edge - (fee_total / quantity) - risk.total
    if net_edge <= 0:
        return None

    return Opportunity(
        market_id=snapshot.market_id,
        side="buy_full_set",
        quantity=quantity,
        gross_edge_per_share=gross_edge,
        net_edge_per_share=net_edge,
        expected_pnl=net_edge * quantity,
    )


def _sell_full_set(snapshot: BinaryMarketSnapshot, fees: FeeSchedule, risk: RiskBuffer) -> Opportunity | None:
    quantity = min(snapshot.yes.bid_size, snapshot.no.bid_size)
    if quantity <= 0:
        return None

    gross_edge = snapshot.yes.bid + snapshot.no.bid - Decimal("1")
    fee_total = fees.taker_fee(shares=quantity, price=snapshot.yes.bid) + fees.taker_fee(
        shares=quantity,
        price=snapshot.no.bid,
    )
    net_edge = gross_edge - (fee_total / quantity) - risk.total
    if net_edge <= 0:
        return None

    return Opportunity(
        market_id=snapshot.market_id,
        side="sell_full_set",
        quantity=quantity,
        gross_edge_per_share=gross_edge,
        net_edge_per_share=net_edge,
        expected_pnl=net_edge * quantity,
    )


def scan_market(
    snapshot: BinaryMarketSnapshot,
    *,
    fees: FeeSchedule | None = None,
    risk: RiskBuffer | None = None,
) -> list[Opportunity]:
    fee_schedule = fees or FeeSchedule.for_category(snapshot.category)
    risk_buffer = risk or RiskBuffer()

    opportunities = []
    for evaluator in (_buy_full_set, _sell_full_set):
        opportunity = evaluator(snapshot, fee_schedule, risk_buffer)
        if opportunity is not None:
            opportunities.append(opportunity)

    return opportunities
