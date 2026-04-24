from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from .models import BinaryMarketSnapshot, FeeSchedule, NegRiskEventSnapshot, Opportunity, RiskBuffer


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


def _buy_neg_risk_basket(
    snapshot: NegRiskEventSnapshot,
    fees: FeeSchedule,
    risk: RiskBuffer,
) -> Opportunity | None:
    if not snapshot.legs:
        return None

    quantity = min(leg.yes.ask_size for leg in snapshot.legs)
    if quantity <= 0:
        return None

    gross_edge = Decimal("1") - sum((leg.yes.ask for leg in snapshot.legs), start=Decimal("0"))
    fee_total = sum((fees.taker_fee(shares=quantity, price=leg.yes.ask) for leg in snapshot.legs), start=Decimal("0"))
    net_edge = gross_edge - (fee_total / quantity) - risk.total
    if net_edge <= 0:
        return None

    return Opportunity(
        market_id=snapshot.event_id,
        side="buy_neg_risk_basket",
        quantity=quantity,
        gross_edge_per_share=gross_edge,
        net_edge_per_share=net_edge,
        expected_pnl=net_edge * quantity,
        detail=f"legs={len(snapshot.legs)}",
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


def scan_neg_risk_event(
    snapshot: NegRiskEventSnapshot,
    *,
    fees: FeeSchedule | None = None,
    risk: RiskBuffer | None = None,
) -> list[Opportunity]:
    fee_schedule = fees or FeeSchedule.for_category(snapshot.category)
    risk_buffer = risk or RiskBuffer()

    opportunity = _buy_neg_risk_basket(snapshot, fee_schedule, risk_buffer)
    if opportunity is None:
        return []
    return [opportunity]


def rank_opportunities(opportunities: Iterable[Opportunity]) -> list[Opportunity]:
    return sorted(
        opportunities,
        key=lambda opportunity: (
            -opportunity.expected_pnl,
            -opportunity.net_edge_per_share,
            -opportunity.gross_edge_per_share,
            opportunity.market_id,
            opportunity.side,
        ),
    )


def scan_snapshots(
    snapshots: Iterable[BinaryMarketSnapshot],
    *,
    fees: FeeSchedule | None = None,
    risk: RiskBuffer | None = None,
) -> list[Opportunity]:
    opportunities: list[Opportunity] = []
    for snapshot in snapshots:
        opportunities.extend(scan_market(snapshot, fees=fees, risk=risk))
    return rank_opportunities(opportunities)


def scan_neg_risk_events(
    snapshots: Iterable[NegRiskEventSnapshot],
    *,
    fees: FeeSchedule | None = None,
    risk: RiskBuffer | None = None,
) -> list[Opportunity]:
    opportunities: list[Opportunity] = []
    for snapshot in snapshots:
        opportunities.extend(scan_neg_risk_event(snapshot, fees=fees, risk=risk))
    return rank_opportunities(opportunities)
