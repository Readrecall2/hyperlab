from __future__ import annotations

import hashlib
import heapq
import math
from bisect import bisect_left, bisect_right
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Generic, Protocol, TypeVar

from hyperlab.analysis.lead_lag import LeadLagConfig, StrictInterval
from hyperlab.analysis.streaming_execution import BboSample, L2Snapshot, TradeSample
from hyperlab.analysis.streaming_store import ExactTimestampNs, timestamp_ns

EventRow = Mapping[str, object]
EventSink = Callable[[Sequence[EventRow]], None]
_T = TypeVar("_T")


class StreamingKernelError(ValueError):
    """Raised when the causal kernel cannot preserve a hard resource bound."""


class SourceBatchSource(Protocol):
    def iter_ordered_batches(
        self,
        *,
        asset: str,
        start_ns: int,
        end_ns: int,
        fetch_rows: int = 1_024,
    ) -> Iterator[tuple[int, tuple[tuple[str, Mapping[str, object]], ...]]]: ...


@dataclass(frozen=True, slots=True)
class StreamingKernelCounts:
    primary_signals: int
    reverse_signals: int
    information_rows: int
    control_rows: int
    execution_rows: int
    exclusions: tuple[tuple[str, int], ...]

    @property
    def total_rows(self) -> int:
        return self.information_rows + self.control_rows + self.execution_rows


@dataclass(frozen=True, slots=True)
class StreamingKernelHighWater:
    batches_processed: int
    rows_scanned: int
    simultaneous_batch_rows: int
    retained_source_rows: int
    bbo_history_rows: int
    public_trade_history_rows: int
    trade_window_batches: int
    l2_history_frames: int
    retained_l2_levels: int
    pending_response_states: int
    pending_execution_states: int
    completed_output_rows: int


@dataclass(frozen=True, slots=True)
class StreamingKernelResult:
    counts: StreamingKernelCounts
    high_water: StreamingKernelHighWater


@dataclass(frozen=True, slots=True)
class _Signal:
    signal_id: str
    venue: str
    asset: str
    family: str
    time_ns: int
    value: float
    direction: int


@dataclass(order=True, slots=True)
class _PendingResponse:
    finalize_ns: int
    order: int
    signal: _Signal = field(compare=False)
    horizon_ms: int = field(compare=False)
    response_venue: str = field(compare=False)
    role: str = field(compare=False)
    execution_state_count: int = field(compare=False)


class _Timeline(Generic[_T]):
    """Small received-time ordered state retained only for the causal window."""

    __slots__ = ("times", "values")

    def __init__(self) -> None:
        self.times: list[int] = []
        self.values: list[_T] = []

    def __len__(self) -> int:
        return len(self.times)

    def append(self, timestamp: int, value: _T) -> None:
        if self.times and timestamp <= self.times[-1]:
            raise StreamingKernelError("timeline timestamps must increase")
        self.times.append(timestamp)
        self.values.append(value)

    def at_or_before(self, timestamp: int) -> tuple[int, _T] | None:
        position = bisect_right(self.times, timestamp) - 1
        if position < 0:
            return None
        return position, self.values[position]

    def first_at_or_after(self, timestamp: int) -> tuple[int, _T] | None:
        position = bisect_left(self.times, timestamp)
        if position >= len(self.times):
            return None
        return position, self.values[position]

    def prune_before(self, timestamp: int) -> None:
        position = bisect_left(self.times, timestamp)
        if position:
            del self.times[:position]
            del self.values[:position]


@dataclass(slots=True)
class _TradeWindow:
    times: list[int] = field(default_factory=list)
    cumulative_signed: list[float] = field(default_factory=list)
    cumulative_total: list[float] = field(default_factory=list)
    signed_sum: float = 0.0
    total_sum: float = 0.0
    removed_signed: float = 0.0
    removed_total: float = 0.0

    def append(self, timestamp: int, signed: float, total: float) -> None:
        self.signed_sum += signed
        self.total_sum += total
        self.times.append(timestamp)
        self.cumulative_signed.append(self.signed_sum)
        self.cumulative_total.append(self.total_sum)

    def window_values(self, threshold: int) -> tuple[float, float]:
        position = bisect_left(self.times, threshold)
        before_signed = (
            self.removed_signed
            if position == 0
            else self.cumulative_signed[position - 1]
        )
        before_total = (
            self.removed_total
            if position == 0
            else self.cumulative_total[position - 1]
        )
        return self.signed_sum - before_signed, self.total_sum - before_total

    def prune_before(self, threshold: int) -> None:
        position = bisect_left(self.times, threshold)
        if not position:
            return
        self.removed_signed = self.cumulative_signed[position - 1]
        self.removed_total = self.cumulative_total[position - 1]
        del self.times[:position]
        del self.cumulative_signed[:position]
        del self.cumulative_total[:position]


def _positive_float(value: object, *, label: str, non_negative: bool = False) -> float:
    if isinstance(value, bool):
        raise StreamingKernelError(f"{label} must be numeric")
    try:
        result = float(str(value))
    except (TypeError, ValueError):
        raise StreamingKernelError(f"{label} must be numeric") from None
    if not math.isfinite(result):
        raise StreamingKernelError(f"{label} must be finite")
    if non_negative:
        if result < 0.0:
            raise StreamingKernelError(f"{label} must be non-negative")
    elif result <= 0.0:
        raise StreamingKernelError(f"{label} must be positive")
    return result


def _iso_timestamp(timestamp: int) -> str:
    """Format epoch nanoseconds exactly like ``pandas.Timestamp.isoformat``."""

    seconds, nanoseconds = divmod(timestamp, 1_000_000_000)
    prefix = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    if nanoseconds == 0:
        fraction = ""
    elif nanoseconds % 1_000 == 0:
        fraction = f".{nanoseconds // 1_000:06d}"
    else:
        fraction = f".{nanoseconds:09d}"
    return f"{prefix}{fraction}+00:00"


def _interval_id(interval: StrictInterval) -> str:
    payload = "|".join(
        (
            interval.tag,
            interval.start.isoformat(timespec="microseconds"),
            interval.end.isoformat(timespec="microseconds"),
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _signal_id(venue: str, asset: str, family: str, timestamp: int) -> str:
    payload = f"{venue.casefold()}|{asset}|{family}|{_iso_timestamp(timestamp)}".encode()
    return hashlib.sha256(payload).hexdigest()[:24]


class _StreamingKernel:
    def __init__(
        self,
        *,
        source: SourceBatchSource,
        asset: str,
        interval: StrictInterval,
        config: LeadLagConfig,
        sink: EventSink,
        include_execution: bool,
    ) -> None:
        self.source = source
        self.asset = asset
        self.interval = interval
        self.config = config
        self.sink = sink
        self.include_execution = include_execution
        self.exclusions: Counter[str] = Counter()
        self.primary_signals = 0
        self.reverse_signals = 0
        self.information_rows = 0
        self.control_rows = 0
        self.execution_rows = 0
        self.batches_processed = 0
        self.rows_scanned = 0
        self.peak_simultaneous = 0
        self.peak_retained_source = 0
        self.peak_bbo_history = 0
        self.peak_public_trade_history = 0
        self.peak_trade_window_batches = 0
        self.peak_l2_history_frames = 0
        self.peak_retained_l2 = 0
        self.peak_pending_responses = 0
        self.peak_pending_executions = 0
        self.peak_completed_output = 0
        self._bbo: dict[str, _Timeline[BboSample]] = {
            config.reference_venue: _Timeline(),
            config.execution_venue: _Timeline(),
        }
        self._l2: dict[str, _Timeline[L2Snapshot]] = {
            config.reference_venue: _Timeline(),
            config.execution_venue: _Timeline(),
        }
        self._trades: dict[str, _Timeline[TradeSample]] = {
            config.reference_venue: _Timeline(),
            config.execution_venue: _Timeline(),
        }
        self._trade_windows: dict[str, _TradeWindow] = {
            config.reference_venue: _TradeWindow(),
            config.execution_venue: _TradeWindow(),
        }
        self._pending: list[_PendingResponse] = []
        self._pending_execution_states = 0
        self._pending_order = 0
        self._output: list[EventRow] = []
        self._interval_id = _interval_id(interval)
        self._seen_bbo_venues: set[str] = set()
        max_horizon_ms = max(config.horizons_ms)
        maximum_exit_ms = max(
            scenario.exit_latency_ms for scenario in config.execution_scenarios
        )
        self._retention_ns = max(
            config.momentum_window_ms + config.max_book_age_ms,
            config.trade_window_ms,
            2 * max_horizon_ms
            + maximum_exit_ms
            + 2 * config.max_book_age_ms,
        ) * 1_000_000

    def _flush_output(self) -> None:
        if not self._output:
            return
        self.sink(tuple(self._output))
        self._output.clear()

    def _emit(self, row: dict[str, object]) -> None:
        row_kind = str(row["row_kind"])
        if row_kind == "information":
            self.information_rows += 1
            if not bool(row.get("evaluable")):
                self.exclusions[str(row.get("exclusion_reason") or "unspecified")] += 1
        elif row_kind == "control":
            self.control_rows += 1
        elif row_kind == "execution":
            self.execution_rows += 1
        else:
            raise StreamingKernelError(f"unknown output row_kind: {row_kind}")
        self._output.append(row)
        self.peak_completed_output = max(
            self.peak_completed_output, len(self._output)
        )
        if len(self._output) >= self.config.writer_buffer_rows:
            self._flush_output()

    def _normalize_bbo(self, timestamp: int, row: Mapping[str, object]) -> BboSample:
        bid = _positive_float(row.get("bid_price"), label="bbo.bid_price")
        ask = _positive_float(row.get("ask_price"), label="bbo.ask_price")
        bid_quantity = _positive_float(
            row.get("bid_quantity"), label="bbo.bid_quantity", non_negative=True
        )
        ask_quantity = _positive_float(
            row.get("ask_quantity"), label="bbo.ask_quantity", non_negative=True
        )
        if bid > ask:
            raise StreamingKernelError("bbo contains crossed prices")
        return BboSample(timestamp, bid, ask, bid_quantity, ask_quantity)

    def _normalize_trade(
        self, timestamp: int, row: Mapping[str, object]
    ) -> TradeSample:
        price = _positive_float(row.get("price"), label="trade.price")
        quantity = _positive_float(row.get("quantity"), label="trade.quantity")
        side = str(row.get("aggressor_side") or "").strip().casefold()
        if side not in {"buy", "sell"}:
            raise StreamingKernelError("trade.aggressor_side must be buy or sell")
        return TradeSample(timestamp, price, quantity, 1 if side == "buy" else -1)

    def _normalize_l2(
        self, timestamp: int, row: Mapping[str, object]
    ) -> L2Snapshot:
        normalized: dict[str, list[tuple[int, float, float]]] = {
            "bids": [],
            "asks": [],
        }
        for column in ("bids", "asks"):
            levels = row.get(column)
            if not isinstance(levels, (list, tuple)):
                raise StreamingKernelError(
                    f"l2.{column} must contain an atomic level sequence"
                )
            for fallback, raw_level in enumerate(levels):
                if isinstance(raw_level, Mapping):
                    level_value = raw_level.get("level", fallback)
                    price_value = raw_level.get("price")
                    quantity_value = raw_level.get("quantity")
                elif isinstance(raw_level, (list, tuple)) and len(raw_level) >= 3:
                    level_value, price_value, quantity_value = raw_level[:3]
                else:
                    raise StreamingKernelError(f"l2.{column} contains an invalid level")
                if isinstance(level_value, bool):
                    raise StreamingKernelError("l2.level must be a non-negative integer")
                try:
                    level = int(str(level_value))
                except (TypeError, ValueError):
                    raise StreamingKernelError(
                        "l2.level must be a non-negative integer"
                    ) from None
                if level < 0:
                    raise StreamingKernelError("l2.level must be non-negative")
                price = _positive_float(price_value, label="l2.price")
                quantity = _positive_float(
                    quantity_value, label="l2.quantity", non_negative=True
                )
                normalized[column].append((level, price, quantity))
        normalized["bids"].sort(key=lambda item: (item[0], -item[1]))
        normalized["asks"].sort(key=lambda item: (item[0], item[1]))
        snapshot = L2Snapshot(
            received_time_ns=timestamp,
            bids=tuple((price, quantity) for _level, price, quantity in normalized["bids"]),
            asks=tuple((price, quantity) for _level, price, quantity in normalized["asks"]),
        )
        if snapshot.level_count > self.config.max_l2_frame_levels:
            raise StreamingKernelError(
                "atomic L2 frame exceeds "
                f"max_l2_frame_levels={self.config.max_l2_frame_levels}"
            )
        return snapshot

    def _append_signal(
        self,
        signals: list[_Signal],
        *,
        venue: str,
        family: str,
        timestamp: int,
        value: float,
    ) -> None:
        if not math.isfinite(value) or abs(value) <= 1e-15:
            return
        signals.append(
            _Signal(
                signal_id=_signal_id(venue, self.asset, family, timestamp),
                venue=venue,
                asset=self.asset,
                family=family,
                time_ns=timestamp,
                value=value,
                direction=1 if value > 0.0 else -1,
            )
        )

    def _bbo_signals(
        self,
        *,
        venue: str,
        timestamp: int,
        previous: BboSample | None,
        current: BboSample | None,
    ) -> list[_Signal]:
        signals: list[_Signal] = []
        if previous is None or current is None:
            return signals
        maximum_age_ns = self.config.max_book_age_ms * 1_000_000
        if timestamp - previous.received_time_ns > maximum_age_ns:
            return signals
        self._append_signal(
            signals,
            venue=venue,
            family="mid_price_change",
            timestamp=timestamp,
            value=math.log(current.mid / previous.mid) * 10_000.0,
        )
        self._append_signal(
            signals,
            venue=venue,
            family="microprice_change",
            timestamp=timestamp,
            value=math.log(current.microprice / previous.microprice) * 10_000.0,
        )
        if current.bid_price > previous.bid_price:
            bid_flow = current.bid_quantity
        elif current.bid_price < previous.bid_price:
            bid_flow = -previous.bid_quantity
        else:
            bid_flow = current.bid_quantity - previous.bid_quantity
        if current.ask_price < previous.ask_price:
            ask_flow = -current.ask_quantity
        elif current.ask_price > previous.ask_price:
            ask_flow = previous.ask_quantity
        else:
            ask_flow = -(current.ask_quantity - previous.ask_quantity)
        scale = max(
            current.bid_quantity
            + current.ask_quantity
            + previous.bid_quantity
            + previous.ask_quantity,
            1e-12,
        )
        self._append_signal(
            signals,
            venue=venue,
            family="bbo_change",
            timestamp=timestamp,
            value=(bid_flow + ask_flow) / scale,
        )
        target = timestamp - self.config.momentum_window_ms * 1_000_000
        baseline = self._bbo[venue].at_or_before(target)
        if baseline is not None:
            past = baseline[1]
            if target - past.received_time_ns <= maximum_age_ns:
                self._append_signal(
                    signals,
                    venue=venue,
                    family="short_term_momentum",
                    timestamp=timestamp,
                    value=math.log(current.mid / past.mid) * 10_000.0,
                )
        return signals

    def _trade_signals(
        self,
        *,
        venue: str,
        timestamp: int,
        trades: Sequence[TradeSample],
        rows: Sequence[Mapping[str, object]],
    ) -> list[_Signal]:
        signals: list[_Signal] = []
        if not trades:
            return signals
        signed = 0.0
        total = 0.0
        for trade, row in zip(trades, rows, strict=True):
            quote_value = row.get("quote_quantity")
            try:
                quote = float(str(quote_value))
            except (TypeError, ValueError):
                quote = math.nan
            if not math.isfinite(quote) or quote <= 0.0:
                quote = trade.price * trade.quantity
            total += quote
            signed += trade.direction * quote
        window = self._trade_windows[venue]
        window.append(timestamp, signed, total)
        flow, window_total = window.window_values(
            timestamp - self.config.trade_window_ms * 1_000_000
        )
        self._append_signal(
            signals,
            venue=venue,
            family="agg_trade",
            timestamp=timestamp,
            value=signed,
        )
        self._append_signal(
            signals,
            venue=venue,
            family="signed_flow",
            timestamp=timestamp,
            value=flow,
        )
        if window_total > 0.0:
            self._append_signal(
                signals,
                venue=venue,
                family="trade_imbalance",
                timestamp=timestamp,
                value=flow / window_total,
            )
        return signals

    def _l2_signal(
        self, *, venue: str, timestamp: int, snapshot: L2Snapshot | None
    ) -> list[_Signal]:
        signals: list[_Signal] = []
        if snapshot is None or not snapshot.bids or not snapshot.asks:
            return signals
        bid_quantity = sum(
            quantity for _price, quantity in snapshot.bids[: self.config.l2_levels]
        )
        ask_quantity = sum(
            quantity for _price, quantity in snapshot.asks[: self.config.l2_levels]
        )
        total = bid_quantity + ask_quantity
        if total > 0.0:
            self._append_signal(
                signals,
                venue=venue,
                family="l2_imbalance",
                timestamp=timestamp,
                value=(bid_quantity - ask_quantity) / total,
            )
        return signals

    def _response_base(
        self, signal: _Signal, horizon_ms: int, role: str
    ) -> dict[str, object]:
        target_ns = signal.time_ns + horizon_ms * 1_000_000
        bucket_ns = self.config.bucket_minutes * 60 * 1_000_000_000
        return {
            "signal_id": signal.signal_id,
            "signal_venue": signal.venue,
            "asset": signal.asset,
            "signal_family": signal.family,
            "signal_time": ExactTimestampNs(signal.time_ns),
            "signal_value": signal.value,
            "signal_strength": abs(signal.value),
            "signal_direction": signal.direction,
            "signal_role": role,
            "time_axis": "received_time",
            "source_time_status": (
                "NOT_ADMISSIBLE_NO_SYMMETRIC_HL_CLOCK_CALIBRATION"
            ),
            "horizon_ms": horizon_ms,
            "target_time": ExactTimestampNs(target_ns),
            "time_bucket": ExactTimestampNs(
                (signal.time_ns // bucket_ns) * bucket_ns
            ),
            "interval_tag": self.interval.tag,
            "interval_id": self._interval_id,
            "interval_start": ExactTimestampNs(
                timestamp_ns(self.interval.start, label="strict interval start")
            ),
            "interval_end": ExactTimestampNs(
                timestamp_ns(self.interval.end, label="strict interval end")
            ),
            "evaluable": False,
            "exclusion_reason": None,
            "baseline_time": None,
            "response_state_time": None,
            "baseline_mid": math.nan,
            "response_mid": math.nan,
            "response_bps": math.nan,
            "negative_lag_response_bps": math.nan,
            "first_move_delay_ms": math.nan,
            "first_move_direction": "none",
            "classification": "not_evaluable",
        }

    def _output_response(self, event: dict[str, object], role: str) -> None:
        event["row_kind"] = "information" if role == "primary" else "control"
        event["execution_scenario"] = None
        event["execution_model"] = None
        event["execution_calibration_status"] = None
        event["execution_status"] = "NOT_APPLICABLE"
        self._emit(event)

    def _schedule_signal(self, signal: _Signal, role: str) -> None:
        if role == "primary":
            self.primary_signals += 1
            response_venue = self.config.execution_venue
        else:
            self.reverse_signals += 1
            response_venue = self.config.reference_venue
        interval_end_ns = timestamp_ns(self.interval.end, label="strict interval end")
        maximum_exit_ms = max(
            scenario.exit_latency_ms for scenario in self.config.execution_scenarios
        )
        for horizon_ms in self.config.horizons_ms:
            target_ns = signal.time_ns + horizon_ms * 1_000_000
            if target_ns >= interval_end_ns:
                event = self._response_base(signal, horizon_ms, role)
                event["exclusion_reason"] = "horizon_crosses_strict_interval"
                self._output_response(event, role)
                continue
            execution_states = (
                len(self.config.execution_scenarios) * 2
                if role == "primary" and self.include_execution
                else 0
            )
            finalize_ns = target_ns
            if execution_states:
                finalize_ns = max(
                    finalize_ns,
                    target_ns
                    + maximum_exit_ms * 1_000_000
                    + self.config.max_book_age_ms * 1_000_000,
                )
            self._pending_order += 1
            heapq.heappush(
                self._pending,
                _PendingResponse(
                    finalize_ns=finalize_ns,
                    order=self._pending_order,
                    signal=signal,
                    horizon_ms=horizon_ms,
                    response_venue=response_venue,
                    role=role,
                    execution_state_count=execution_states,
                ),
            )
            self._pending_execution_states += execution_states
            if len(self._pending) > self.config.max_pending_response_states:
                raise StreamingKernelError(
                    "pending response states exceed "
                    f"max_pending_response_states={self.config.max_pending_response_states}"
                )
            if (
                self._pending_execution_states
                > self.config.max_pending_execution_states
            ):
                raise StreamingKernelError(
                    "pending execution states exceed "
                    f"max_pending_execution_states={self.config.max_pending_execution_states}"
                )
            self.peak_pending_responses = max(
                self.peak_pending_responses, len(self._pending)
            )
            self.peak_pending_executions = max(
                self.peak_pending_executions, self._pending_execution_states
            )

    def _evaluate_response(self, pending: _PendingResponse) -> dict[str, object]:
        signal = pending.signal
        horizon_ms = pending.horizon_ms
        target_ns = signal.time_ns + horizon_ms * 1_000_000
        event = self._response_base(signal, horizon_ms, pending.role)
        timeline = self._bbo[pending.response_venue]
        if not timeline:
            event["exclusion_reason"] = "missing_response_bbo"
            return event
        baseline_item = timeline.at_or_before(signal.time_ns)
        if baseline_item is None:
            event["exclusion_reason"] = "missing_baseline_bbo"
            return event
        baseline_position, baseline = baseline_item
        interval_start_ns = timestamp_ns(
            self.interval.start, label="strict interval start"
        )
        if baseline.received_time_ns < interval_start_ns:
            event["exclusion_reason"] = "baseline_outside_strict_interval"
            return event
        maximum_age_ns = self.config.max_book_age_ms * 1_000_000
        if signal.time_ns - baseline.received_time_ns > maximum_age_ns:
            event["exclusion_reason"] = "stale_baseline_bbo"
            return event
        response_item = timeline.at_or_before(target_ns)
        if response_item is None or response_item[0] < baseline_position:
            event["exclusion_reason"] = "missing_response_state"
            return event
        response_position, response = response_item
        if target_ns - response.received_time_ns > maximum_age_ns:
            event["exclusion_reason"] = "stale_response_bbo"
            return event
        response_bps = (
            signal.direction * math.log(response.mid / baseline.mid) * 10_000.0
        )
        first_delay = math.nan
        first_direction = "none"
        for position in range(baseline_position + 1, response_position + 1):
            state = timeline.values[position]
            move_bps = math.log(state.mid / baseline.mid) * 10_000.0
            if abs(move_bps) > self.config.minimum_move_bps:
                first_delay = (
                    state.received_time_ns - signal.time_ns
                ) / 1_000_000.0
                first_direction = (
                    "same" if signal.direction * move_bps > 0.0 else "opposite"
                )
                break
        negative_response = math.nan
        past_target_ns = signal.time_ns - horizon_ms * 1_000_000
        if past_target_ns >= interval_start_ns:
            past_item = timeline.at_or_before(past_target_ns)
            if past_item is not None:
                past = past_item[1]
                if (
                    past.received_time_ns >= interval_start_ns
                    and past_target_ns - past.received_time_ns <= maximum_age_ns
                ):
                    negative_response = (
                        signal.direction
                        * math.log(baseline.mid / past.mid)
                        * 10_000.0
                    )
        if response_bps > self.config.minimum_move_bps:
            classification = "same_direction"
        elif response_bps < -self.config.minimum_move_bps:
            classification = "adverse"
        else:
            classification = "neutral"
        block_number = (
            signal.time_ns - interval_start_ns
        ) // (self.config.randomization_block_ms * 1_000_000)
        event.update(
            {
                "evaluable": True,
                "baseline_time": ExactTimestampNs(baseline.received_time_ns),
                "response_state_time": ExactTimestampNs(
                    response.received_time_ns
                ),
                "baseline_mid": baseline.mid,
                "baseline_bid": baseline.bid_price,
                "baseline_ask": baseline.ask_price,
                "baseline_bid_quantity": baseline.bid_quantity,
                "baseline_ask_quantity": baseline.ask_quantity,
                "response_mid": response.mid,
                "response_bid": response.bid_price,
                "response_ask": response.ask_price,
                "response_bps": response_bps,
                "negative_lag_response_bps": negative_response,
                "first_move_delay_ms": first_delay,
                "first_move_direction": first_direction,
                "classification": classification,
                "randomization_block": f"{self._interval_id}|{block_number:012d}",
            }
        )
        return event

    def _finalize_ready(self, watermark_ns: int, *, force: bool = False) -> None:
        while self._pending and (force or self._pending[0].finalize_ns <= watermark_ns):
            pending = heapq.heappop(self._pending)
            self._pending_execution_states -= pending.execution_state_count
            event = self._evaluate_response(pending)
            self._output_response(event, pending.role)
            if pending.execution_state_count:
                self._emit_execution(event)

    def _emit_execution(self, event: Mapping[str, object]) -> None:
        # Populated by the separate scalar execution component. Keeping this
        # call isolated ensures the information/control state machine remains
        # independently testable and never falls back to pandas.
        from hyperlab.analysis.streaming_execution import model_execution_events

        interval_start_ns = timestamp_ns(
            self.interval.start, label="strict interval start"
        )
        interval_end_ns = timestamp_ns(self.interval.end, label="strict interval end")
        for execution in model_execution_events(
            event,
            self,
            interval_start_ns,
            interval_end_ns,
            self.config,
        ):
            self._emit(execution)

    def first_bbo_at_or_after(
        self, timestamp: int, maximum_age_ns: int, interval_end_ns: int
    ) -> BboSample | None:
        item = self._bbo[self.config.execution_venue].first_at_or_after(timestamp)
        if item is None:
            return None
        state = item[1]
        if (
            state.received_time_ns - timestamp > maximum_age_ns
            or state.received_time_ns >= interval_end_ns
        ):
            return None
        return state

    def l2_at_or_before(
        self, timestamp: int, maximum_age_ns: int
    ) -> L2Snapshot | None:
        item = self._l2[self.config.execution_venue].at_or_before(timestamp)
        if item is None:
            return None
        snapshot = item[1]
        if timestamp - snapshot.received_time_ns > maximum_age_ns:
            return None
        return snapshot

    def iter_trades(
        self, start_exclusive_ns: int, end_inclusive_ns: int
    ) -> Iterator[TradeSample]:
        timeline = self._trades[self.config.execution_venue]
        left = bisect_right(timeline.times, start_exclusive_ns)
        right = bisect_right(timeline.times, end_inclusive_ns)
        yield from timeline.values[left:right]

    def _prune(self, watermark_ns: int) -> None:
        threshold = watermark_ns - self._retention_ns
        for bbo_timeline in self._bbo.values():
            bbo_timeline.prune_before(threshold)
        for l2_timeline in self._l2.values():
            l2_timeline.prune_before(threshold)
        for trade_timeline in self._trades.values():
            trade_timeline.prune_before(threshold)
        trade_threshold = watermark_ns - self.config.trade_window_ms * 1_000_000
        for window in self._trade_windows.values():
            window.prune_before(trade_threshold)

    def _observe_retained_state(self, batch_rows: int) -> None:
        bbo_rows = sum(len(timeline) for timeline in self._bbo.values())
        trade_rows = sum(len(timeline) for timeline in self._trades.values())
        l2_frames = sum(len(timeline) for timeline in self._l2.values())
        trade_window_batches = sum(
            len(window.times) for window in self._trade_windows.values()
        )
        retained_source = (
            batch_rows
            + bbo_rows
            + trade_rows
            + l2_frames
            + trade_window_batches
        )
        retained_l2 = sum(
            snapshot.level_count
            for timeline in self._l2.values()
            for snapshot in timeline.values
        )
        if retained_source > self.config.max_source_rows_per_chunk:
            raise StreamingKernelError(
                "retained rolling source state exceeds "
                f"max_source_rows_per_chunk={self.config.max_source_rows_per_chunk}"
            )
        if retained_l2 > self.config.max_l2_levels_per_chunk:
            raise StreamingKernelError(
                "retained rolling L2 levels exceed "
                f"max_l2_levels_per_chunk={self.config.max_l2_levels_per_chunk}"
            )
        self.peak_retained_source = max(
            self.peak_retained_source, retained_source
        )
        self.peak_bbo_history = max(self.peak_bbo_history, bbo_rows)
        self.peak_public_trade_history = max(
            self.peak_public_trade_history, trade_rows
        )
        self.peak_trade_window_batches = max(
            self.peak_trade_window_batches, trade_window_batches
        )
        self.peak_l2_history_frames = max(
            self.peak_l2_history_frames, l2_frames
        )
        self.peak_retained_l2 = max(self.peak_retained_l2, retained_l2)

    def _process_batch(
        self,
        timestamp: int,
        batch: Sequence[tuple[str, Mapping[str, object]]],
    ) -> None:
        terminal_bbo: dict[str, BboSample] = {}
        terminal_l2: dict[str, L2Snapshot] = {}
        trades_by_venue: dict[str, list[TradeSample]] = {
            self.config.reference_venue: [],
            self.config.execution_venue: [],
        }
        trade_rows_by_venue: dict[str, list[Mapping[str, object]]] = {
            self.config.reference_venue: [],
            self.config.execution_venue: [],
        }
        for kind, row in batch:
            if timestamp_ns(row.get("received_time"), label="source received_time") != timestamp:
                raise StreamingKernelError("source batch timestamp does not match payload")
            row_asset = str(row.get("asset") or "").strip().upper()
            if row_asset != self.asset:
                raise StreamingKernelError("source batch asset does not match query")
            venue = str(row.get("venue") or "").strip().casefold()
            if venue not in self._bbo:
                continue
            if kind == "bbo":
                terminal_bbo[venue] = self._normalize_bbo(timestamp, row)
            elif kind == "trade":
                trade = self._normalize_trade(timestamp, row)
                trades_by_venue[venue].append(trade)
                trade_rows_by_venue[venue].append(row)
            elif kind == "l2":
                terminal_l2[venue] = self._normalize_l2(timestamp, row)
            else:
                raise StreamingKernelError(f"unknown projected source kind: {kind}")

        previous_bbo: dict[str, BboSample | None] = {}
        for venue, state in terminal_bbo.items():
            self._seen_bbo_venues.add(venue)
            bbo_timeline = self._bbo[venue]
            previous_bbo[venue] = (
                bbo_timeline.values[-1] if bbo_timeline.values else None
            )
            bbo_timeline.append(timestamp, state)
        for venue, snapshots in terminal_l2.items():
            self._l2[venue].append(timestamp, snapshots)
        for venue, trades in trades_by_venue.items():
            trade_timeline = self._trades[venue]
            for trade in trades:
                # Multiple public trades at one received timestamp are retained
                # in deterministic persisted order.
                trade_timeline.times.append(timestamp)
                trade_timeline.values.append(trade)

        signals_by_venue: dict[str, list[_Signal]] = {
            self.config.reference_venue: [],
            self.config.execution_venue: [],
        }
        for venue in signals_by_venue:
            signals_by_venue[venue].extend(
                self._bbo_signals(
                    venue=venue,
                    timestamp=timestamp,
                    previous=previous_bbo.get(venue),
                    current=terminal_bbo.get(venue),
                )
            )
            signals_by_venue[venue].extend(
                self._trade_signals(
                    venue=venue,
                    timestamp=timestamp,
                    trades=trades_by_venue[venue],
                    rows=trade_rows_by_venue[venue],
                )
            )
            signals_by_venue[venue].extend(
                self._l2_signal(
                    venue=venue,
                    timestamp=timestamp,
                    snapshot=terminal_l2.get(venue),
                )
            )
        for signal in signals_by_venue[self.config.reference_venue]:
            self._schedule_signal(signal, "primary")
        for signal in signals_by_venue[self.config.execution_venue]:
            self._schedule_signal(signal, "reverse")

    def run(self) -> StreamingKernelResult:
        start_ns = timestamp_ns(self.interval.start, label="strict interval start")
        end_ns = timestamp_ns(self.interval.end, label="strict interval end")
        for received_ns, batch in self.source.iter_ordered_batches(
            asset=self.asset,
            start_ns=start_ns,
            end_ns=end_ns,
        ):
            size = len(batch)
            if size > self.config.max_simultaneous_batch_rows:
                raise StreamingKernelError(
                    "complete received-time batch exceeds "
                    f"max_simultaneous_batch_rows={self.config.max_simultaneous_batch_rows}"
                )
            self.batches_processed += 1
            self.rows_scanned += size
            self.peak_simultaneous = max(self.peak_simultaneous, size)
            self._finalize_ready(received_ns - 1)
            self._prune(received_ns)
            self._process_batch(received_ns, batch)
            # Complete equal-time state is applied before target deadlines and
            # same-time signal baselines are observed.
            self._finalize_ready(received_ns)
            self._observe_retained_state(size)
        missing_bbo = sorted(set(self._bbo) - self._seen_bbo_venues)
        if missing_bbo:
            raise StreamingKernelError(
                "missing required BBO venue/assets for interval: "
                + ", ".join(missing_bbo)
            )
        self._finalize_ready(end_ns, force=True)
        self._flush_output()
        return StreamingKernelResult(
            counts=StreamingKernelCounts(
                primary_signals=self.primary_signals,
                reverse_signals=self.reverse_signals,
                information_rows=self.information_rows,
                control_rows=self.control_rows,
                execution_rows=self.execution_rows,
                exclusions=tuple(sorted(self.exclusions.items())),
            ),
            high_water=StreamingKernelHighWater(
                batches_processed=self.batches_processed,
                rows_scanned=self.rows_scanned,
                simultaneous_batch_rows=self.peak_simultaneous,
                retained_source_rows=self.peak_retained_source,
                bbo_history_rows=self.peak_bbo_history,
                public_trade_history_rows=self.peak_public_trade_history,
                trade_window_batches=self.peak_trade_window_batches,
                l2_history_frames=self.peak_l2_history_frames,
                retained_l2_levels=self.peak_retained_l2,
                pending_response_states=self.peak_pending_responses,
                pending_execution_states=self.peak_pending_executions,
                completed_output_rows=self.peak_completed_output,
            ),
        )


def run_streaming_kernel(
    source: SourceBatchSource,
    *,
    asset: str,
    interval: StrictInterval,
    config: LeadLagConfig,
    sink: EventSink,
    include_execution: bool = True,
) -> StreamingKernelResult:
    """Run one asset/strict-interval causal received-time state machine."""

    normalized_asset = asset.strip().upper()
    if normalized_asset not in config.assets:
        raise StreamingKernelError("kernel asset must be configured")
    return _StreamingKernel(
        source=source,
        asset=normalized_asset,
        interval=interval,
        config=config,
        sink=sink,
        include_execution=include_execution,
    ).run()


__all__ = [
    "BboSample",
    "EventRow",
    "EventSink",
    "L2Snapshot",
    "SourceBatchSource",
    "StreamingKernelCounts",
    "StreamingKernelError",
    "StreamingKernelHighWater",
    "StreamingKernelResult",
    "TradeSample",
    "run_streaming_kernel",
]
