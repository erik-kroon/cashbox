from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from .models import BinaryMarketSnapshot, FeeSchedule, MakerRiskBuffer, NegRiskEventSnapshot, Opportunity, RiskBuffer, TopOfBook


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


def _quote_is_passive(book: TopOfBook) -> bool:
    return Decimal("0") < book.bid < book.ask < Decimal("1")


def _passive_buy(
    *,
    market_id: str,
    side: str,
    price: Decimal,
    fair_value: Decimal,
    quantity: Decimal,
    fees: FeeSchedule,
    risk: MakerRiskBuffer,
) -> Opportunity | None:
    if quantity <= 0 or not (Decimal("0") < price < Decimal("1")):
        return None

    gross_edge = fair_value - price
    rebate_per_share = fees.maker_rebate(shares=quantity, price=price) / quantity
    net_edge = gross_edge + rebate_per_share - risk.total
    if net_edge <= 0:
        return None

    return Opportunity(
        market_id=market_id,
        side=side,
        quantity=quantity,
        gross_edge_per_share=gross_edge,
        net_edge_per_share=net_edge,
        expected_pnl=net_edge * quantity,
        detail=f"quote={price} fair={fair_value}",
    )


def _passive_sell(
    *,
    market_id: str,
    side: str,
    price: Decimal,
    fair_value: Decimal,
    quantity: Decimal,
    fees: FeeSchedule,
    risk: MakerRiskBuffer,
) -> Opportunity | None:
    if quantity <= 0 or not (Decimal("0") < price < Decimal("1")):
        return None

    gross_edge = price - fair_value
    rebate_per_share = fees.maker_rebate(shares=quantity, price=price) / quantity
    net_edge = gross_edge + rebate_per_share - risk.total
    if net_edge <= 0:
        return None

    return Opportunity(
        market_id=market_id,
        side=side,
        quantity=quantity,
        gross_edge_per_share=gross_edge,
        net_edge_per_share=net_edge,
        expected_pnl=net_edge * quantity,
        detail=f"quote={price} fair={fair_value}",
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


def scan_maker_quotes(
    snapshot: BinaryMarketSnapshot,
    *,
    fees: FeeSchedule | None = None,
    risk: MakerRiskBuffer | None = None,
    quantity: Decimal = Decimal("1"),
) -> list[Opportunity]:
    if snapshot.fair_yes is None or not (Decimal("0") < snapshot.fair_yes < Decimal("1")):
        return []

    fee_schedule = fees or FeeSchedule.for_category(snapshot.category)
    risk_buffer = risk or MakerRiskBuffer()
    fair_yes = snapshot.fair_yes
    fair_no = Decimal("1") - fair_yes

    opportunities: list[Opportunity] = []
    if _quote_is_passive(snapshot.yes):
        for evaluator in (
            lambda: _passive_buy(
                market_id=snapshot.market_id,
                side="make_yes_bid",
                price=snapshot.yes.bid,
                fair_value=fair_yes,
                quantity=quantity,
                fees=fee_schedule,
                risk=risk_buffer,
            ),
            lambda: _passive_sell(
                market_id=snapshot.market_id,
                side="make_yes_ask",
                price=snapshot.yes.ask,
                fair_value=fair_yes,
                quantity=quantity,
                fees=fee_schedule,
                risk=risk_buffer,
            ),
        ):
            opportunity = evaluator()
            if opportunity is not None:
                opportunities.append(opportunity)

    if _quote_is_passive(snapshot.no):
        for evaluator in (
            lambda: _passive_buy(
                market_id=snapshot.market_id,
                side="make_no_bid",
                price=snapshot.no.bid,
                fair_value=fair_no,
                quantity=quantity,
                fees=fee_schedule,
                risk=risk_buffer,
            ),
            lambda: _passive_sell(
                market_id=snapshot.market_id,
                side="make_no_ask",
                price=snapshot.no.ask,
                fair_value=fair_no,
                quantity=quantity,
                fees=fee_schedule,
                risk=risk_buffer,
            ),
        ):
            opportunity = evaluator()
            if opportunity is not None:
                opportunities.append(opportunity)

    return rank_opportunities(opportunities)


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


def scan_maker_snapshots(
    snapshots: Iterable[BinaryMarketSnapshot],
    *,
    risk: MakerRiskBuffer | None = None,
    quantity: Decimal = Decimal("1"),
    maker_rebate_rate: Decimal | None = None,
) -> list[Opportunity]:
    opportunities: list[Opportunity] = []
    for snapshot in snapshots:
        fee_schedule = FeeSchedule.for_category(snapshot.category)
        if maker_rebate_rate is not None:
            fee_schedule = FeeSchedule(
                taker_fee_rate=fee_schedule.taker_fee_rate,
                maker_rebate_rate=maker_rebate_rate,
            )
        opportunities.extend(scan_maker_quotes(snapshot, fees=fee_schedule, risk=risk, quantity=quantity))
    return rank_opportunities(opportunities)
