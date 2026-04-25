from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
import hashlib
import json
from math import ceil
from typing import Any, Callable, Optional

from .ingest import FileSystemMarketStore
from .models import NormalizedMarketRecord, format_datetime, parse_datetime


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _require_text(name: str, value: Any) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must be non-empty")
    return normalized


def _quantize_down(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    increments = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return increments * step


def _format_decimal(value: Decimal, *, places: str = "0.00000001") -> str:
    quantized = value.quantize(Decimal(places))
    return format(quantized.normalize(), "f")


def _safe_divide(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


def _split_name(index: int, total_points: int, assumptions: dict[str, Any]) -> str:
    train_ratio = Decimal(assumptions["train_ratio"])
    validation_ratio = Decimal(assumptions["validation_ratio"])
    train_cutoff = int((Decimal(total_points) * train_ratio).to_integral_value(rounding=ROUND_DOWN))
    validation_cutoff = train_cutoff + int(
        (Decimal(total_points) * validation_ratio).to_integral_value(rounding=ROUND_DOWN)
    )
    if index < train_cutoff:
        return "train"
    if index < validation_cutoff:
        return "validation"
    return "test"


@dataclass(frozen=True)
class HistoryPoint:
    market_id: str
    timestamp: datetime
    end_time: Optional[datetime]
    price_proxy: Decimal
    liquidity: Decimal
    volume: Decimal


@dataclass(frozen=True)
class LoadedHistoryBatch:
    histories: dict[str, list[HistoryPoint]]
    timeline_points: int
    history_sha256: str


@dataclass(frozen=True)
class LoadedHistoryWindow:
    histories: dict[str, list[HistoryPoint]]
    timeline_points: int
    history_sha256: str
    source_window: dict[str, Any]


@dataclass(frozen=True)
class StrategyReplayResult:
    trades: list[dict[str, Any]]
    rejections: list[dict[str, Any]]


class StrategyReplayService:
    def __init__(self, market_store: FileSystemMarketStore) -> None:
        self.market_store = market_store

    def load_backtest_histories(
        self,
        experiment: dict[str, Any],
        dataset_id: str,
        *,
        validation_error: Callable[..., Exception],
    ) -> LoadedHistoryBatch:
        dataset_manifest = self.market_store.load_manifest(dataset_id)
        dataset_time = parse_datetime(dataset_manifest.ingested_at)
        if dataset_time is None:
            raise validation_error(
                "dataset manifest is missing a versioned ingest timestamp",
                code="unversioned_dataset",
            )
        return self._load_histories(
            experiment,
            start_time=None,
            end_time=dataset_time,
            minimum_points=2,
            insufficient_history_message="insufficient history for market {market_id}; need at least 2 point-in-time records",
            post_resolution_message="history for {market_id} includes post-resolution data at {recorded_at}",
            validation_error=validation_error,
        )

    def load_paper_histories(
        self,
        experiment: dict[str, Any],
        *,
        start_dataset_id: str,
        end_dataset_id: str,
        validation_error: Callable[..., Exception],
    ) -> LoadedHistoryWindow:
        start_manifest = self.market_store.load_manifest(start_dataset_id)
        end_manifest = self.market_store.load_manifest(end_dataset_id)
        start_time = parse_datetime(start_manifest.ingested_at)
        end_time = parse_datetime(end_manifest.ingested_at)
        if start_time is None or end_time is None:
            raise validation_error("paper trading requires versioned dataset timestamps")
        if end_time <= start_time:
            raise validation_error("paper trading requires at least one newer dataset after the backtest dataset")

        loaded = self._load_histories(
            experiment,
            start_time=start_time,
            end_time=end_time,
            minimum_points=2,
            insufficient_history_message="insufficient future history for market {market_id}; need at least 2 post-backtest points",
            post_resolution_message="future history for {market_id} includes post-resolution data at {recorded_at}",
            validation_error=validation_error,
        )
        return LoadedHistoryWindow(
            histories=loaded.histories,
            timeline_points=loaded.timeline_points,
            history_sha256=loaded.history_sha256,
            source_window={
                "start_dataset_id": start_dataset_id,
                "end_dataset_id": end_dataset_id,
                "start_at": start_manifest.ingested_at,
                "end_at": end_manifest.ingested_at,
            },
        )

    def replay_strategy(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
        *,
        split_name_fn: Optional[Callable[[int, int], str]] = None,
        validation_error: Callable[..., Exception],
    ) -> StrategyReplayResult:
        resolved_split_name_fn = split_name_fn or (lambda index, total: _split_name(index, total, assumptions))
        if experiment["strategy_family"] == "midpoint_reversion":
            trades, rejections = self._simulate_midpoint_reversion(
                experiment,
                assumptions,
                histories,
                split_name_fn=resolved_split_name_fn,
                validation_error=validation_error,
            )
        elif experiment["strategy_family"] == "resolution_drift":
            trades, rejections = self._simulate_resolution_drift(
                experiment,
                assumptions,
                histories,
                split_name_fn=resolved_split_name_fn,
                validation_error=validation_error,
            )
        elif experiment["strategy_family"] == "cross_market_arbitrage":
            trades, rejections = self._simulate_cross_market_arbitrage(
                experiment,
                assumptions,
                histories,
                split_name_fn=resolved_split_name_fn,
                validation_error=validation_error,
            )
        else:
            raise validation_error(
                f"unsupported strategy_family: {experiment['strategy_family']}",
                code="unsupported_strategy_family",
            )

        return StrategyReplayResult(trades=trades, rejections=rejections)

    def _load_histories(
        self,
        experiment: dict[str, Any],
        *,
        start_time: Optional[datetime],
        end_time: datetime,
        minimum_points: int,
        insufficient_history_message: str,
        post_resolution_message: str,
        validation_error: Callable[..., Exception],
    ) -> LoadedHistoryBatch:
        histories: dict[str, list[HistoryPoint]] = {}
        history_fingerprint_rows: list[dict[str, Any]] = []
        for market_id in self._market_ids_for_experiment(experiment, validation_error=validation_error):
            points: list[HistoryPoint] = []
            for row in self.market_store.load_history(market_id):
                recorded_at = parse_datetime(row.get("recorded_at"))
                if recorded_at is None or recorded_at > end_time:
                    continue
                if start_time is not None and recorded_at <= start_time:
                    continue
                record = NormalizedMarketRecord.from_dict(row["record"])
                market_end_time = parse_datetime(record.end_time)
                if market_end_time is not None and recorded_at > market_end_time:
                    raise validation_error(
                        post_resolution_message.format(
                            market_id=market_id,
                            recorded_at=format_datetime(recorded_at),
                        ),
                        code="post_resolution_data",
                        violations=[
                            {
                                "market_id": market_id,
                                "recorded_at": format_datetime(recorded_at),
                                "end_time": format_datetime(market_end_time),
                                "issue": "post_resolution_data",
                            }
                        ],
                    )
                point = HistoryPoint(
                    market_id=market_id,
                    timestamp=recorded_at,
                    end_time=market_end_time,
                    price_proxy=self._price_proxy(record),
                    liquidity=self._decimal_text(record.liquidity),
                    volume=self._decimal_text(record.volume),
                )
                points.append(point)
                history_fingerprint_rows.append(
                    {
                        "market_id": market_id,
                        "timestamp": format_datetime(recorded_at),
                        "price_proxy": _format_decimal(point.price_proxy),
                        "liquidity": _format_decimal(point.liquidity),
                        "volume": _format_decimal(point.volume),
                    }
                )
            points.sort(key=lambda item: item.timestamp)
            if len(points) < minimum_points:
                raise validation_error(
                    insufficient_history_message.format(market_id=market_id),
                    code="insufficient_history",
                    violations=[{"market_id": market_id, "issue": "insufficient_history"}],
                )
            histories[market_id] = points

        timeline_points = min(len(points) for points in histories.values())
        history_sha256 = hashlib.sha256(_canonical_json(history_fingerprint_rows).encode("utf-8")).hexdigest()
        return LoadedHistoryBatch(
            histories=histories,
            timeline_points=timeline_points,
            history_sha256=history_sha256,
        )

    def _market_ids_for_experiment(
        self,
        experiment: dict[str, Any],
        *,
        validation_error: Callable[..., Exception],
    ) -> list[str]:
        config = experiment["config"]
        try:
            if experiment["strategy_family"] == "cross_market_arbitrage":
                market_ids = [_require_text("config.market_ids[]", item) for item in config["market_ids"]]
                return sorted(dict.fromkeys(market_ids))
            return [_require_text("config.market_id", config["market_id"])]
        except (KeyError, ValueError, TypeError) as exc:
            raise validation_error(str(exc), code="invalid_request") from exc

    def _price_proxy(self, record: NormalizedMarketRecord) -> Decimal:
        volume = self._decimal_text(record.volume)
        liquidity = self._decimal_text(record.liquidity)
        return (volume + Decimal("1")) / (volume + liquidity + Decimal("2"))

    def _decimal_text(self, value: Optional[str]) -> Decimal:
        if value in (None, ""):
            return Decimal("0")
        return Decimal(str(value))

    def _simulate_midpoint_reversion(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
        *,
        split_name_fn: Callable[[int, int], str],
        validation_error: Callable[..., Exception],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        points = histories[experiment["config"]["market_id"]]
        lookback = int(experiment["config"]["lookback_minutes"])
        entry_threshold = Decimal(str(experiment["config"]["entry_zscore"]))
        exit_threshold = Decimal(str(experiment["config"]["exit_zscore"]))
        latency_steps = self._latency_steps(points, assumptions)
        trades: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []
        zscores = self._zscores(points, lookback)

        for signal_index in range(lookback, len(points) - 1):
            zscore = zscores[signal_index]
            if zscore is None:
                continue
            direction = None
            if zscore <= -entry_threshold:
                direction = "LONG"
            elif zscore >= entry_threshold:
                direction = "SHORT"
            if direction is None:
                continue

            fill_index = signal_index + latency_steps
            if fill_index >= len(points) - 1:
                rejections.append(self._rejection(points[signal_index], "latency_overflow", signal_index))
                continue
            if points[fill_index].timestamp - points[signal_index].timestamp > timedelta(
                seconds=int(assumptions["stale_book_threshold_seconds"])
            ):
                rejections.append(self._rejection(points[signal_index], "stale_book", signal_index))
                continue

            exit_index = fill_index + 1
            for candidate_index in range(fill_index + 1, len(points)):
                candidate = zscores[candidate_index]
                if candidate is not None and abs(candidate) <= exit_threshold:
                    exit_index = candidate_index
                    break
            trades.append(
                self._completed_trade(
                    trade_number=len(trades) + 1,
                    split=split_name_fn(signal_index, len(points)),
                    market_id=points[signal_index].market_id,
                    signal_index=signal_index,
                    signal_time=points[signal_index].timestamp,
                    entry_time=points[fill_index].timestamp,
                    exit_time=points[exit_index].timestamp,
                    direction=direction,
                    signal_strength_bps=self._return_bps(
                        points[max(signal_index - 1, 0)].price_proxy,
                        points[signal_index].price_proxy,
                    ),
                    entry_price=points[fill_index].price_proxy,
                    exit_price=points[exit_index].price_proxy,
                    notional_cap=Decimal(str(experiment["config"]["max_position_usd"])),
                    assumptions=assumptions,
                    leg_count=1,
                    validation_error=validation_error,
                )
            )

        return trades, rejections

    def _simulate_resolution_drift(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
        *,
        split_name_fn: Callable[[int, int], str],
        validation_error: Callable[..., Exception],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        points = histories[experiment["config"]["market_id"]]
        signal_window = timedelta(minutes=int(experiment["config"]["signal_window_minutes"]))
        entry_edge_bps = Decimal(str(experiment["config"]["entry_edge_bps"]))
        max_holding = timedelta(minutes=int(experiment["config"]["max_holding_minutes"]))
        latency_steps = self._latency_steps(points, assumptions)
        trades: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []

        for signal_index in range(1, len(points) - 1):
            end_time = points[signal_index].end_time
            if end_time is None:
                continue
            time_to_end = end_time - points[signal_index].timestamp
            if time_to_end <= timedelta(0) or time_to_end > signal_window:
                continue

            move_bps = self._return_bps(points[signal_index - 1].price_proxy, points[signal_index].price_proxy).copy_abs()
            if move_bps < entry_edge_bps:
                continue

            fill_index = signal_index + latency_steps
            if fill_index >= len(points) - 1:
                rejections.append(self._rejection(points[signal_index], "latency_overflow", signal_index))
                continue
            if points[fill_index].timestamp - points[signal_index].timestamp > timedelta(
                seconds=int(assumptions["stale_book_threshold_seconds"])
            ):
                rejections.append(self._rejection(points[signal_index], "stale_book", signal_index))
                continue

            direction = "LONG" if points[signal_index].price_proxy >= points[signal_index - 1].price_proxy else "SHORT"
            exit_index = fill_index + 1
            for candidate_index in range(fill_index + 1, len(points)):
                if points[candidate_index].timestamp - points[fill_index].timestamp >= max_holding:
                    exit_index = candidate_index
                    break
            trades.append(
                self._completed_trade(
                    trade_number=len(trades) + 1,
                    split=split_name_fn(signal_index, len(points)),
                    market_id=points[signal_index].market_id,
                    signal_index=signal_index,
                    signal_time=points[signal_index].timestamp,
                    entry_time=points[fill_index].timestamp,
                    exit_time=points[exit_index].timestamp,
                    direction=direction,
                    signal_strength_bps=move_bps,
                    entry_price=points[fill_index].price_proxy,
                    exit_price=points[exit_index].price_proxy,
                    notional_cap=Decimal(str(experiment["config"]["max_position_usd"])),
                    assumptions=assumptions,
                    leg_count=1,
                    validation_error=validation_error,
                )
            )

        return trades, rejections

    def _simulate_cross_market_arbitrage(
        self,
        experiment: dict[str, Any],
        assumptions: dict[str, Any],
        histories: dict[str, list[HistoryPoint]],
        *,
        split_name_fn: Callable[[int, int], str],
        validation_error: Callable[..., Exception],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        market_ids = sorted(histories)
        min_points = min(len(points) for points in histories.values())
        base_points = histories[market_ids[0]]
        latency_steps = self._latency_steps(base_points, assumptions)
        max_spread_bps = Decimal(str(experiment["config"]["max_spread_bps"]))
        min_edge_bps = Decimal(str(experiment["config"]["min_edge_bps"]))
        rebalance_seconds = int(experiment["config"]["rebalance_interval_seconds"])
        rebalance_steps = max(1, latency_steps, ceil(rebalance_seconds / max(self._median_step_seconds(base_points), 1)))
        trades: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []

        for signal_index in range(min_points - 1):
            signal_points = {market_id: histories[market_id][signal_index] for market_id in market_ids}
            timestamps = [point.timestamp for point in signal_points.values()]
            if max(timestamps) - min(timestamps) > timedelta(seconds=int(assumptions["stale_book_threshold_seconds"])):
                rejections.append(self._rejection(base_points[signal_index], "stale_cross_market_snapshot", signal_index))
                continue

            sorted_by_price = sorted(
                ((point.price_proxy, market_id) for market_id, point in signal_points.items()),
                key=lambda item: (item[0], item[1]),
            )
            low_price, low_market_id = sorted_by_price[0]
            high_price, high_market_id = sorted_by_price[-1]
            spread_bps = self._return_bps(low_price, high_price)
            if spread_bps < min_edge_bps or spread_bps > max_spread_bps:
                continue

            fill_index = signal_index + latency_steps
            exit_index = fill_index + rebalance_steps
            if exit_index >= min_points:
                rejections.append(self._rejection(base_points[signal_index], "latency_overflow", signal_index))
                continue

            low_fill = histories[low_market_id][fill_index]
            high_fill = histories[high_market_id][fill_index]
            if max(low_fill.timestamp, high_fill.timestamp) - min(low_fill.timestamp, high_fill.timestamp) > timedelta(
                seconds=int(assumptions["stale_book_threshold_seconds"])
            ):
                rejections.append(self._rejection(base_points[signal_index], "stale_book", signal_index))
                continue

            low_exit = histories[low_market_id][exit_index]
            high_exit = histories[high_market_id][exit_index]
            trade = self._completed_trade(
                trade_number=len(trades) + 1,
                split=split_name_fn(signal_index, min_points),
                market_id=f"{low_market_id}|{high_market_id}",
                signal_index=signal_index,
                signal_time=max(signal_points[low_market_id].timestamp, signal_points[high_market_id].timestamp),
                entry_time=max(low_fill.timestamp, high_fill.timestamp),
                exit_time=max(low_exit.timestamp, high_exit.timestamp),
                direction="CONVERGENCE",
                signal_strength_bps=spread_bps,
                entry_price=(low_fill.price_proxy + high_fill.price_proxy) / 2,
                exit_price=(low_exit.price_proxy + high_exit.price_proxy) / 2,
                notional_cap=Decimal(str(experiment["config"]["max_position_usd"])),
                assumptions=assumptions,
                leg_count=2,
                extra_fields={
                    "low_market_id": low_market_id,
                    "high_market_id": high_market_id,
                    "low_entry_price": _format_decimal(low_fill.price_proxy),
                    "high_entry_price": _format_decimal(high_fill.price_proxy),
                    "low_exit_price": _format_decimal(low_exit.price_proxy),
                    "high_exit_price": _format_decimal(high_exit.price_proxy),
                },
                validation_error=validation_error,
            )
            gross_pnl = (
                Decimal(trade["quantity"]) * (low_exit.price_proxy - low_fill.price_proxy)
                + Decimal(trade["quantity"]) * (high_fill.price_proxy - high_exit.price_proxy)
            )
            trade["gross_pnl_usd"] = _format_decimal(gross_pnl)
            total_cost = Decimal(trade["fees_usd"]) + Decimal(trade["slippage_usd"])
            trade["net_pnl_usd"] = _format_decimal(gross_pnl - total_cost)
            trades.append(trade)

        return trades, rejections

    def _zscores(self, points: list[HistoryPoint], lookback: int) -> list[Optional[Decimal]]:
        zscores: list[Optional[Decimal]] = [None] * len(points)
        for index in range(lookback, len(points)):
            window = [point.price_proxy for point in points[index - lookback : index]]
            mean = sum(window) / Decimal(len(window))
            variance = sum((value - mean) ** 2 for value in window) / Decimal(len(window))
            if variance == 0:
                zscores[index] = Decimal("0")
                continue
            zscores[index] = (points[index].price_proxy - mean) / variance.sqrt()
        return zscores

    def _latency_steps(self, points: list[HistoryPoint], assumptions: dict[str, Any]) -> int:
        latency_seconds = int(assumptions["latency_seconds"])
        if latency_seconds <= 0:
            return 0
        median_step = max(self._median_step_seconds(points), 1)
        return max(1, ceil(latency_seconds / median_step))

    def _median_step_seconds(self, points: list[HistoryPoint]) -> int:
        if len(points) < 2:
            return 1
        deltas = sorted(
            max(1, int((points[index].timestamp - points[index - 1].timestamp).total_seconds()))
            for index in range(1, len(points))
        )
        return deltas[len(deltas) // 2]

    def _completed_trade(
        self,
        *,
        trade_number: int,
        split: str,
        market_id: str,
        signal_index: int,
        signal_time: datetime,
        entry_time: datetime,
        exit_time: datetime,
        direction: str,
        signal_strength_bps: Decimal,
        entry_price: Decimal,
        exit_price: Decimal,
        notional_cap: Decimal,
        assumptions: dict[str, Any],
        leg_count: int,
        extra_fields: Optional[dict[str, Any]] = None,
        validation_error: Callable[..., Exception],
    ) -> dict[str, Any]:
        tick_size = Decimal(assumptions["tick_size"])
        quantity_step = Decimal("1").scaleb(-int(assumptions["quantity_precision_dp"]))
        partial_fill_ratio = Decimal(assumptions["partial_fill_ratio"])
        effective_notional = notional_cap * partial_fill_ratio
        quantity = _quantize_down(_safe_divide(effective_notional, max(entry_price, tick_size)), quantity_step)
        if quantity <= 0:
            raise validation_error(
                "effective quantity rounded to zero under configured precision constraints",
                code="precision_rejection",
            )

        if direction == "LONG":
            gross_pnl = quantity * (exit_price - entry_price)
        elif direction == "SHORT":
            gross_pnl = quantity * (entry_price - exit_price)
        else:
            gross_pnl = quantity * (exit_price - entry_price)

        traded_notional = effective_notional * Decimal(leg_count)
        fees = traded_notional * Decimal(assumptions["fee_bps"]) / Decimal("10000")
        slippage = traded_notional * Decimal(assumptions["slippage_bps"]) / Decimal("10000")
        payload = {
            "trade_id": f"trade-{trade_number:04d}",
            "split": split,
            "market_id": market_id,
            "signal_index": signal_index,
            "signal_time": format_datetime(signal_time),
            "entry_time": format_datetime(entry_time),
            "exit_time": format_datetime(exit_time),
            "direction": direction,
            "signal_strength_bps": _format_decimal(signal_strength_bps),
            "entry_price": _format_decimal(entry_price),
            "exit_price": _format_decimal(exit_price),
            "quantity": _format_decimal(quantity),
            "filled_notional_usd": _format_decimal(traded_notional),
            "fees_usd": _format_decimal(fees),
            "slippage_usd": _format_decimal(slippage),
            "gross_pnl_usd": _format_decimal(gross_pnl),
            "net_pnl_usd": _format_decimal(gross_pnl - fees - slippage),
            "partial_fill_ratio": _format_decimal(partial_fill_ratio),
        }
        if extra_fields:
            payload.update(extra_fields)
        return payload

    def _rejection(self, point: HistoryPoint, reason: str, signal_index: int) -> dict[str, Any]:
        return {
            "market_id": point.market_id,
            "reason": reason,
            "signal_index": signal_index,
            "signal_time": format_datetime(point.timestamp),
        }

    def _return_bps(self, start: Decimal, end: Decimal) -> Decimal:
        if start <= 0:
            return Decimal("0")
        return ((end - start) / start) * Decimal("10000")
