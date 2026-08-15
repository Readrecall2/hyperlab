from __future__ import annotations

import math
import re
from bisect import bisect_left, bisect_right
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from numbers import Integral, Real
from typing import Literal, Protocol

from hyperlab.analysis.lead_lag import (
    ExecutionAssumptions,
    LeadLagConfig,
)
from hyperlab.analysis.streaming_store import ExactTimestampNs

_ISO_TIMESTAMP_RE = re.compile(
    r"^(?P<prefix>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?P<timezone>Z|[+-]\d{2}:\d{2})$"
)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class StreamingExecutionError(ValueError):
    """Raised when scalar execution inputs violate the causal contract."""


def _finite(value: object, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (Real, str)):
        raise StreamingExecutionError(f"{label} must be finite")
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise StreamingExecutionError(f"{label} must be finite") from None
    if not math.isfinite(result):
        raise StreamingExecutionError(f"{label} must be finite")
    return result


def _positive(value: object, *, label: str) -> float:
    result = _finite(value, label=label)
    if result <= 0.0:
        raise StreamingExecutionError(f"{label} must be positive")
    return result


def _non_negative(value: object, *, label: str) -> float:
    result = _finite(value, label=label)
    if result < 0.0:
        raise StreamingExecutionError(f"{label} must be non-negative")
    return result


def _integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (Integral, str)):
        raise StreamingExecutionError(f"{label} must be integer-like")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        raise StreamingExecutionError(f"{label} must be integer-like") from None
    return result


def _timestamp_ns(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise StreamingExecutionError(f"{label} must be a timezone-aware timestamp")
    if isinstance(value, ExactTimestampNs):
        return value.value
    # The scalar state machine uses exact epoch nanoseconds internally.  Treat
    # integral values as that canonical representation; converting them through
    # ``Timestamp`` first would incorrectly make them timezone-naive.
    if isinstance(value, Integral):
        return int(value)
    exact_value = getattr(value, "value", None)
    if isinstance(exact_value, Integral) and getattr(value, "tzinfo", None) is not None:
        return int(exact_value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise StreamingExecutionError(
                f"{label} must be a timezone-aware timestamp"
            )
        delta = value.astimezone(UTC) - _EPOCH
        return (
            (delta.days * 86_400 + delta.seconds) * 1_000_000_000
            + delta.microseconds * 1_000
        )
    if not isinstance(value, str):
        raise StreamingExecutionError(f"{label} must be a timezone-aware timestamp")
    match = _ISO_TIMESTAMP_RE.fullmatch(value.strip())
    if match is None:
        raise StreamingExecutionError(
            f"{label} must be a timezone-aware timestamp"
        )
    timezone_text = match.group("timezone")
    if timezone_text == "Z":
        timezone_text = "+00:00"
    try:
        timestamp = datetime.fromisoformat(match.group("prefix") + timezone_text)
    except ValueError:
        raise StreamingExecutionError(
            f"{label} must be a timezone-aware timestamp"
        ) from None
    delta = timestamp.astimezone(UTC) - _EPOCH
    whole_seconds_ns = (delta.days * 86_400 + delta.seconds) * 1_000_000_000
    fraction = match.group("fraction") or ""
    fraction_ns = int(fraction.ljust(9, "0")) if fraction else 0
    return whole_seconds_ns + fraction_ns


def utc_iso_from_ns(value_ns: int) -> str:
    """Format exact epoch nanoseconds without truncating below microseconds."""

    seconds, nanoseconds = divmod(value_ns, 1_000_000_000)
    prefix = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    if nanoseconds == 0:
        fraction = ""
    elif nanoseconds % 1_000 == 0:
        fraction = f".{nanoseconds // 1_000:06d}"
    else:
        fraction = f".{nanoseconds:09d}"
    return f"{prefix}{fraction}+00:00"


def _time_ns(value: int, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StreamingExecutionError(f"{label} must be an integer nanosecond timestamp")
    return value


@dataclass(frozen=True, slots=True)
class BboSample:
    """Terminal Hyperliquid BBO state for one received-time batch."""

    received_time_ns: int
    bid_price: float
    ask_price: float
    bid_quantity: float
    ask_quantity: float

    def __post_init__(self) -> None:
        _time_ns(self.received_time_ns, label="BBO received_time_ns")
        bid = _positive(self.bid_price, label="BBO bid_price")
        ask = _positive(self.ask_price, label="BBO ask_price")
        bid_quantity = _non_negative(self.bid_quantity, label="BBO bid_quantity")
        ask_quantity = _non_negative(self.ask_quantity, label="BBO ask_quantity")
        if bid > ask:
            raise StreamingExecutionError("BBO prices cannot be crossed")
        object.__setattr__(self, "bid_price", bid)
        object.__setattr__(self, "ask_price", ask)
        object.__setattr__(self, "bid_quantity", bid_quantity)
        object.__setattr__(self, "ask_quantity", ask_quantity)

    @property
    def mid(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0

    @property
    def microprice(self) -> float:
        total = self.bid_quantity + self.ask_quantity
        if total <= 0.0:
            return self.mid
        return (
            self.ask_price * self.bid_quantity
            + self.bid_price * self.ask_quantity
        ) / total


@dataclass(frozen=True, slots=True)
class L2Snapshot:
    """One complete terminal L2 frame reconstructed at received time."""

    received_time_ns: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]

    def __post_init__(self) -> None:
        _time_ns(self.received_time_ns, label="L2 received_time_ns")
        for side_name, levels in (("bids", self.bids), ("asks", self.asks)):
            for position, (price, quantity) in enumerate(levels):
                _positive(price, label=f"L2 {side_name}[{position}] price")
                _non_negative(quantity, label=f"L2 {side_name}[{position}] quantity")

    @property
    def level_count(self) -> int:
        return len(self.bids) + len(self.asks)


@dataclass(frozen=True, slots=True)
class TradeSample:
    """One public Hyperliquid trade in deterministic received-time order."""

    received_time_ns: int
    price: float
    quantity: float
    direction: int

    def __post_init__(self) -> None:
        _time_ns(self.received_time_ns, label="trade received_time_ns")
        object.__setattr__(self, "price", _positive(self.price, label="trade price"))
        object.__setattr__(
            self,
            "quantity",
            _positive(self.quantity, label="trade quantity"),
        )
        if isinstance(self.direction, bool) or self.direction not in {-1, 1}:
            raise StreamingExecutionError("trade direction must be -1 or 1")


class ExecutionTimelines(Protocol):
    """Bounded, interval-local received-time lookup surface for execution."""

    def first_bbo_at_or_after(
        self,
        received_time_ns: int,
        max_age_ns: int,
        interval_end_ns: int,
        /,
    ) -> BboSample | None:
        """Return the first BBO at/after the time, subject to age and interval end."""

    def l2_at_or_before(
        self,
        received_time_ns: int,
        max_age_ns: int,
        /,
    ) -> L2Snapshot | None:
        """Return the latest complete L2 frame at/before the time if fresh."""

    def iter_trades(
        self,
        start_exclusive_ns: int,
        end_inclusive_ns: int,
        /,
    ) -> Iterator[TradeSample]:
        """Yield public trades in deterministic order over ``(start, end]``."""


@dataclass(frozen=True, slots=True)
class TupleExecutionTimelines:
    """Small-fixture implementation of :class:`ExecutionTimelines`.

    The production state machine may conform structurally without using this
    tuple-backed helper.  These tuples are required to be interval-local and
    bounded; they are never populated from a complete multi-hour capture.
    """

    bbo: tuple[BboSample, ...] = ()
    l2: tuple[L2Snapshot, ...] = ()
    trades: tuple[TradeSample, ...] = ()

    def __post_init__(self) -> None:
        _require_strictly_increasing(
            (item.received_time_ns for item in self.bbo), label="BBO"
        )
        _require_strictly_increasing(
            (item.received_time_ns for item in self.l2), label="L2"
        )
        _require_non_decreasing(
            (item.received_time_ns for item in self.trades), label="trades"
        )

    def first_bbo_at_or_after(
        self,
        received_time_ns: int,
        max_age_ns: int,
        interval_end_ns: int,
    ) -> BboSample | None:
        position = bisect_left(
            self.bbo,
            received_time_ns,
            key=lambda item: item.received_time_ns,
        )
        if position >= len(self.bbo):
            return None
        sample = self.bbo[position]
        if (
            sample.received_time_ns >= interval_end_ns
            or sample.received_time_ns - received_time_ns > max_age_ns
        ):
            return None
        return sample

    def l2_at_or_before(
        self,
        received_time_ns: int,
        max_age_ns: int,
    ) -> L2Snapshot | None:
        position = bisect_right(
            self.l2,
            received_time_ns,
            key=lambda item: item.received_time_ns,
        ) - 1
        if position < 0:
            return None
        snapshot = self.l2[position]
        if received_time_ns - snapshot.received_time_ns > max_age_ns:
            return None
        return snapshot

    def iter_trades(
        self,
        start_exclusive_ns: int,
        end_inclusive_ns: int,
    ) -> Iterator[TradeSample]:
        left = bisect_right(
            self.trades,
            start_exclusive_ns,
            key=lambda item: item.received_time_ns,
        )
        right = bisect_right(
            self.trades,
            end_inclusive_ns,
            key=lambda item: item.received_time_ns,
        )
        yield from self.trades[left:right]


def _require_strictly_increasing(values: Iterator[int], *, label: str) -> None:
    previous: int | None = None
    for value in values:
        if previous is not None and value <= previous:
            raise StreamingExecutionError(
                f"{label} received times must be strictly increasing"
            )
        previous = value


def _require_non_decreasing(values: Iterator[int], *, label: str) -> None:
    previous: int | None = None
    for value in values:
        if previous is not None and value < previous:
            raise StreamingExecutionError(
                f"{label} received times must be non-decreasing"
            )
        previous = value


@dataclass(frozen=True, slots=True)
class _Fill:
    average_price: float
    base_quantity: float
    fraction: float


def _execution_base(
    event: Mapping[str, object],
    scenario: ExecutionAssumptions,
    model: Literal["taker", "maker"],
) -> dict[str, object]:
    result: dict[str, object] = {
        **event,
        "row_kind": "execution",
        "execution_scenario": scenario.name,
        "execution_model": model,
        "execution_calibration_status": scenario.calibration_status,
        "execution_source": scenario.source,
        "execution_status": "NOT_ATTEMPTED",
        "economic_scope": "BEFORE_FUNDING",
        "funding_status": "NOT_EVALUATED",
        "economic_admissibility": "NOT_ADMISSIBLE_FUNDING_NOT_EVALUATED",
        "net_execution_scope": "FEES_SPREAD_SLIPPAGE_BEFORE_FUNDING",
        "latency_ms_assumption": scenario.latency_ms,
        "exit_latency_ms_assumption": scenario.exit_latency_ms,
        "maker_timeout_ms_assumption": scenario.maker_timeout_ms,
        "maker_fee_bps_assumption": scenario.maker_fee_bps,
        "taker_fee_bps_assumption": scenario.taker_fee_bps,
        "slippage_bps_assumption": scenario.slippage_bps,
        "adverse_exit_bps_assumption": scenario.adverse_exit_bps,
        "queue_ahead_multiplier_assumption": scenario.queue_ahead_multiplier,
        "max_participation_assumption": scenario.max_participation,
        "spread_source": "OBSERVED_HYPERLIQUID_BOOK_AT_FILL_EVENT",
        "entry_time": None,
        "exit_time": None,
        "entry_price": math.nan,
        "exit_price": math.nan,
        "requested_notional_usd": scenario.notional_usd,
        "entry_fill_fraction": 0.0,
        "exit_fill_fraction": 0.0,
        "matched_fill_fraction": 0.0,
        "unclosed_exposure_fraction": 0.0,
        "fill_fraction": 0.0,
        "gross_execution_bps": math.nan,
        "net_execution_bps": math.nan,
        "before_funding_execution_bps": math.nan,
        "before_cost_mid_move_bps": math.nan,
        "entry_fee_bps_applied": math.nan,
        "exit_fee_bps_applied": math.nan,
        "entry_spread_cost_bps": math.nan,
        "exit_spread_cost_bps": math.nan,
        "entry_slippage_cost_bps": math.nan,
        "exit_slippage_cost_bps": math.nan,
        "adverse_exit_cost_bps": math.nan,
        "fill_adjusted_gross_bps": math.nan,
        "fill_adjusted_net_bps": math.nan,
        "break_even_move_bps": math.nan,
    }
    # Every execution row retains the fixed information-event schema even when
    # an unevaluable information response had no observable baseline.
    result.setdefault("baseline_mid", math.nan)
    return result


def _first_bbo(
    timelines: ExecutionTimelines,
    due_ns: int,
    *,
    maximum_age_ns: int,
    interval_start_ns: int,
    interval_end_ns: int,
) -> BboSample | None:
    sample = timelines.first_bbo_at_or_after(
        due_ns,
        maximum_age_ns,
        interval_end_ns,
    )
    if sample is None:
        return None
    if sample.received_time_ns < due_ns:
        raise StreamingExecutionError("execution timeline returned a BBO before its query")
    if sample.received_time_ns - due_ns > maximum_age_ns:
        raise StreamingExecutionError("execution timeline returned a stale BBO")
    if not interval_start_ns <= sample.received_time_ns < interval_end_ns:
        raise StreamingExecutionError("execution timeline returned a BBO outside the interval")
    return sample


def _l2_for_bbo(
    timelines: ExecutionTimelines,
    bbo: BboSample,
    *,
    maximum_age_ns: int,
) -> L2Snapshot | None:
    snapshot = timelines.l2_at_or_before(bbo.received_time_ns, maximum_age_ns)
    if snapshot is None:
        return None
    if snapshot.received_time_ns > bbo.received_time_ns:
        raise StreamingExecutionError("execution timeline returned look-ahead L2 state")
    if bbo.received_time_ns - snapshot.received_time_ns > maximum_age_ns:
        raise StreamingExecutionError("execution timeline returned stale L2 state")
    return snapshot


def _book_fill(
    *,
    side: Literal["buy", "sell"],
    requested_base_quantity: float,
    bbo: BboSample,
    snapshot: L2Snapshot | None,
    max_participation: float,
) -> _Fill:
    if requested_base_quantity <= 0.0:
        return _Fill(math.nan, 0.0, 0.0)
    price_quantity: Iterator[tuple[float, float]]
    if snapshot is not None:
        consistent = bool(snapshot.bids and snapshot.asks)
        if consistent:
            top_bid = snapshot.bids[0][0]
            top_ask = snapshot.asks[0][0]
            consistent = (
                snapshot.received_time_ns == bbo.received_time_ns
                and top_bid <= top_ask
                and math.isclose(top_bid, bbo.bid_price, rel_tol=0.0, abs_tol=1e-12)
                and math.isclose(top_ask, bbo.ask_price, rel_tol=0.0, abs_tol=1e-12)
            )
        if not consistent:
            snapshot = None
    if snapshot is not None:
        levels = snapshot.asks if side == "buy" else snapshot.bids
        price_quantity = (
            (price, quantity * max_participation) for price, quantity in levels
        )
    else:
        price_quantity = iter(
            (
                (
                    bbo.ask_price if side == "buy" else bbo.bid_price,
                    (
                        bbo.ask_quantity if side == "buy" else bbo.bid_quantity
                    )
                    * max_participation,
                ),
            )
        )
    remaining = requested_base_quantity
    filled = 0.0
    quote = 0.0
    for price, available in price_quantity:
        if price <= 0.0 or available <= 0.0:
            continue
        quantity = min(remaining, available)
        filled += quantity
        quote += quantity * price
        remaining -= quantity
        if remaining <= 1e-15:
            break
    if filled <= 0.0:
        return _Fill(math.nan, 0.0, 0.0)
    return _Fill(
        average_price=quote / filled,
        base_quantity=filled,
        fraction=min(1.0, filled / requested_base_quantity),
    )


def _adjust_execution_price(
    price: float,
    *,
    side: Literal["buy", "sell"],
    slippage_bps: float,
    adverse_bps: float = 0.0,
) -> float:
    adjustment = (slippage_bps + adverse_bps) / 10_000.0
    return price * (1.0 + adjustment if side == "buy" else 1.0 - adjustment)


def _economic_values(
    *,
    direction: int,
    baseline_mid: float,
    entry_price: float,
    exit_price: float,
    entry_fee_bps: float,
    exit_fee_bps: float,
    exit_slippage_bps: float,
    adverse_exit_bps: float,
) -> tuple[float, float, float]:
    entry_fee = entry_fee_bps / 10_000.0
    exit_fee = exit_fee_bps / 10_000.0
    if direction > 0:
        gross = exit_price / entry_price - 1.0
        net = (
            exit_price * (1.0 - exit_fee) - entry_price * (1.0 + entry_fee)
        ) / entry_price
        required_adjusted_exit = entry_price * (1.0 + entry_fee) / (1.0 - exit_fee)
        exit_factor = 1.0 - (exit_slippage_bps + adverse_exit_bps) / 10_000.0
        required_raw_exit = required_adjusted_exit / exit_factor
        break_even = (required_raw_exit / baseline_mid - 1.0) * 10_000.0
    else:
        gross = 1.0 - exit_price / entry_price
        net = (
            entry_price * (1.0 - entry_fee) - exit_price * (1.0 + exit_fee)
        ) / entry_price
        required_adjusted_exit = entry_price * (1.0 - entry_fee) / (1.0 + exit_fee)
        exit_factor = 1.0 + (exit_slippage_bps + adverse_exit_bps) / 10_000.0
        required_raw_exit = required_adjusted_exit / exit_factor
        break_even = (1.0 - required_raw_exit / baseline_mid) * 10_000.0
    return gross * 10_000.0, net * 10_000.0, break_even


def _complete_execution(
    result: dict[str, object],
    *,
    direction: int,
    baseline_mid: float,
    entry_reference_mid: float,
    exit_reference_mid: float,
    entry_time_ns: int,
    exit_time_ns: int,
    entry_fill: _Fill,
    exit_fill: _Fill,
    entry_fee_bps: float,
    exit_fee_bps: float,
    scenario: ExecutionAssumptions,
    entry_side: Literal["buy", "sell"],
    entry_has_slippage: bool,
) -> dict[str, object]:
    exit_side: Literal["buy", "sell"] = "sell" if direction > 0 else "buy"
    entry_price = _adjust_execution_price(
        entry_fill.average_price,
        side=entry_side,
        slippage_bps=scenario.slippage_bps if entry_has_slippage else 0.0,
    )
    exit_price = _adjust_execution_price(
        exit_fill.average_price,
        side=exit_side,
        slippage_bps=scenario.slippage_bps,
        adverse_bps=scenario.adverse_exit_bps,
    )
    gross, net, break_even = _economic_values(
        direction=direction,
        baseline_mid=baseline_mid,
        entry_price=entry_price,
        exit_price=exit_price,
        entry_fee_bps=entry_fee_bps,
        exit_fee_bps=exit_fee_bps,
        exit_slippage_bps=scenario.slippage_bps,
        adverse_exit_bps=scenario.adverse_exit_bps,
    )
    matched_fraction = entry_fill.fraction * exit_fill.fraction
    unclosed_fraction = entry_fill.fraction * (1.0 - exit_fill.fraction)
    status = "FILLED" if matched_fraction >= 1.0 - 1e-12 else "PARTIAL"
    entry_spread = (
        direction * (entry_fill.average_price / entry_reference_mid - 1.0) * 10_000.0
    )
    exit_spread = (
        -direction * (exit_fill.average_price / exit_reference_mid - 1.0) * 10_000.0
    )
    before_cost_mid_move = (
        direction * math.log(exit_reference_mid / entry_reference_mid) * 10_000.0
    )
    result.update(
        {
            "execution_status": status,
            "entry_time": ExactTimestampNs(entry_time_ns),
            "exit_time": ExactTimestampNs(exit_time_ns),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "entry_fill_fraction": entry_fill.fraction,
            "exit_fill_fraction": exit_fill.fraction,
            "matched_fill_fraction": matched_fraction,
            "unclosed_exposure_fraction": unclosed_fraction,
            "fill_fraction": matched_fraction,
            "gross_execution_bps": gross,
            "net_execution_bps": net,
            "before_funding_execution_bps": net,
            "before_cost_mid_move_bps": before_cost_mid_move,
            "entry_fee_bps_applied": entry_fee_bps,
            "exit_fee_bps_applied": exit_fee_bps,
            "entry_spread_cost_bps": entry_spread,
            "exit_spread_cost_bps": exit_spread,
            "entry_slippage_cost_bps": scenario.slippage_bps
            if entry_has_slippage
            else 0.0,
            "exit_slippage_cost_bps": scenario.slippage_bps,
            "adverse_exit_cost_bps": scenario.adverse_exit_bps,
            "fill_adjusted_gross_bps": gross * matched_fraction,
            "fill_adjusted_net_bps": net * matched_fraction,
            "break_even_move_bps": break_even,
        }
    )
    return result


def model_taker_execution(
    information_event: Mapping[str, object],
    timelines: ExecutionTimelines,
    interval_start_ns: int,
    interval_end_ns: int,
    scenario: ExecutionAssumptions,
    *,
    maximum_age_ns: int,
) -> dict[str, object]:
    """Model one causal Hyperliquid taker attempt using scalar state only."""

    result = _execution_base(information_event, scenario, "taker")
    signal_time_ns, target_time_ns, direction = _event_inputs(
        information_event,
        interval_start_ns=interval_start_ns,
        interval_end_ns=interval_end_ns,
    )
    entry_due_ns = signal_time_ns + scenario.latency_ms * 1_000_000
    if entry_due_ns >= target_time_ns:
        result["execution_status"] = "MISSED_LATENCY"
        return result
    entry_bbo = _first_bbo(
        timelines,
        entry_due_ns,
        maximum_age_ns=maximum_age_ns,
        interval_start_ns=interval_start_ns,
        interval_end_ns=interval_end_ns,
    )
    if entry_bbo is None:
        result["execution_status"] = "MISSED_ENTRY_BOOK"
        return result
    if entry_bbo.received_time_ns >= target_time_ns:
        result["execution_status"] = "MISSED_ENTRY_AFTER_HORIZON"
        return result
    entry_side: Literal["buy", "sell"] = "buy" if direction > 0 else "sell"
    reference_price = entry_bbo.ask_price if direction > 0 else entry_bbo.bid_price
    requested_base = scenario.notional_usd / reference_price
    entry_fill = _book_fill(
        side=entry_side,
        requested_base_quantity=requested_base,
        bbo=entry_bbo,
        snapshot=_l2_for_bbo(
            timelines,
            entry_bbo,
            maximum_age_ns=maximum_age_ns,
        ),
        max_participation=scenario.max_participation,
    )
    if entry_fill.fraction <= 0.0:
        result["execution_status"] = "MISSED_ENTRY_LIQUIDITY"
        return result

    exit_due_ns = target_time_ns + scenario.exit_latency_ms * 1_000_000
    exit_bbo = _first_bbo(
        timelines,
        exit_due_ns,
        maximum_age_ns=maximum_age_ns,
        interval_start_ns=interval_start_ns,
        interval_end_ns=interval_end_ns,
    )
    if exit_bbo is None:
        result.update(
            execution_status="UNRESOLVED_EXIT_BOOK",
            entry_time=ExactTimestampNs(entry_bbo.received_time_ns),
            entry_price=_adjust_execution_price(
                entry_fill.average_price,
                side=entry_side,
                slippage_bps=scenario.slippage_bps,
            ),
            entry_fill_fraction=entry_fill.fraction,
            unclosed_exposure_fraction=entry_fill.fraction,
        )
        return result
    exit_side: Literal["buy", "sell"] = "sell" if direction > 0 else "buy"
    exit_fill = _book_fill(
        side=exit_side,
        requested_base_quantity=entry_fill.base_quantity,
        bbo=exit_bbo,
        snapshot=_l2_for_bbo(
            timelines,
            exit_bbo,
            maximum_age_ns=maximum_age_ns,
        ),
        max_participation=scenario.max_participation,
    )
    if exit_fill.fraction <= 0.0:
        result.update(
            execution_status="UNRESOLVED_EXIT_LIQUIDITY",
            entry_time=ExactTimestampNs(entry_bbo.received_time_ns),
            entry_price=_adjust_execution_price(
                entry_fill.average_price,
                side=entry_side,
                slippage_bps=scenario.slippage_bps,
            ),
            entry_fill_fraction=entry_fill.fraction,
            unclosed_exposure_fraction=entry_fill.fraction,
        )
        return result
    return _complete_execution(
        result,
        direction=direction,
        baseline_mid=_nullable_baseline_mid(information_event),
        entry_reference_mid=entry_bbo.mid,
        exit_reference_mid=exit_bbo.mid,
        entry_time_ns=entry_bbo.received_time_ns,
        exit_time_ns=exit_bbo.received_time_ns,
        entry_fill=entry_fill,
        exit_fill=exit_fill,
        entry_fee_bps=scenario.taker_fee_bps,
        exit_fee_bps=scenario.taker_fee_bps,
        scenario=scenario,
        entry_side=entry_side,
        entry_has_slippage=True,
    )


def model_maker_execution(
    information_event: Mapping[str, object],
    timelines: ExecutionTimelines,
    interval_start_ns: int,
    interval_end_ns: int,
    scenario: ExecutionAssumptions,
    *,
    maximum_age_ns: int,
) -> dict[str, object]:
    """Model one public-trade-evidenced Hyperliquid maker attempt."""

    result = _execution_base(information_event, scenario, "maker")
    signal_time_ns, target_time_ns, direction = _event_inputs(
        information_event,
        interval_start_ns=interval_start_ns,
        interval_end_ns=interval_end_ns,
    )
    entry_due_ns = signal_time_ns + scenario.latency_ms * 1_000_000
    if entry_due_ns >= target_time_ns:
        result["execution_status"] = "MISSED_LATENCY"
        return result
    entry_bbo = _first_bbo(
        timelines,
        entry_due_ns,
        maximum_age_ns=maximum_age_ns,
        interval_start_ns=interval_start_ns,
        interval_end_ns=interval_end_ns,
    )
    if entry_bbo is None:
        result["execution_status"] = "MISSED_ENTRY_BOOK"
        return result
    if entry_bbo.received_time_ns >= target_time_ns:
        result["execution_status"] = "MISSED_ENTRY_AFTER_HORIZON"
        return result
    entry_side: Literal["buy", "sell"] = "buy" if direction > 0 else "sell"
    maker_price = entry_bbo.bid_price if direction > 0 else entry_bbo.ask_price
    displayed_quantity = (
        entry_bbo.bid_quantity if direction > 0 else entry_bbo.ask_quantity
    )
    requested_base = scenario.notional_usd / maker_price
    queue_ahead = displayed_quantity * scenario.queue_ahead_multiplier
    deadline_ns = min(
        target_time_ns,
        entry_due_ns + scenario.maker_timeout_ms * 1_000_000,
    )
    remaining = requested_base
    filled = 0.0
    fill_time_ns: int | None = None
    eligible_found = False
    required_trade_direction = -1 if direction > 0 else 1
    previous_trade_time_ns: int | None = None
    for trade in timelines.iter_trades(entry_bbo.received_time_ns, deadline_ns):
        if not entry_bbo.received_time_ns < trade.received_time_ns <= deadline_ns:
            raise StreamingExecutionError(
                "execution timeline returned a trade outside the requested causal window"
            )
        if (
            previous_trade_time_ns is not None
            and trade.received_time_ns < previous_trade_time_ns
        ):
            raise StreamingExecutionError(
                "execution timeline returned trades out of received-time order"
            )
        previous_trade_time_ns = trade.received_time_ns
        if trade.direction != required_trade_direction:
            continue
        if direction > 0 and trade.price > maker_price:
            continue
        if direction < 0 and trade.price < maker_price:
            continue
        eligible_found = True
        quantity = trade.quantity
        if queue_ahead > 0.0:
            consumed = min(queue_ahead, quantity)
            queue_ahead -= consumed
            quantity -= consumed
        if quantity <= 0.0:
            continue
        executed = min(remaining, quantity)
        filled += executed
        remaining -= executed
        fill_time_ns = trade.received_time_ns
        if remaining <= 1e-15:
            break
    if filled <= 0.0:
        result["execution_status"] = (
            "MISSED_QUEUE" if eligible_found else "MISSED_NO_PUBLIC_TRADE"
        )
        result["entry_time"] = ExactTimestampNs(entry_bbo.received_time_ns)
        return result

    entry_fill = _Fill(
        average_price=maker_price,
        base_quantity=filled,
        fraction=min(1.0, filled / requested_base),
    )
    exit_due_ns = target_time_ns + scenario.exit_latency_ms * 1_000_000
    exit_bbo = _first_bbo(
        timelines,
        exit_due_ns,
        maximum_age_ns=maximum_age_ns,
        interval_start_ns=interval_start_ns,
        interval_end_ns=interval_end_ns,
    )
    if exit_bbo is None:
        result.update(
            execution_status="UNRESOLVED_EXIT_BOOK",
            entry_time=ExactTimestampNs(fill_time_ns)
            if fill_time_ns is not None
            else None,
            entry_price=maker_price,
            entry_fill_fraction=entry_fill.fraction,
            unclosed_exposure_fraction=entry_fill.fraction,
        )
        return result
    exit_side: Literal["buy", "sell"] = "sell" if direction > 0 else "buy"
    exit_fill = _book_fill(
        side=exit_side,
        requested_base_quantity=entry_fill.base_quantity,
        bbo=exit_bbo,
        snapshot=_l2_for_bbo(
            timelines,
            exit_bbo,
            maximum_age_ns=maximum_age_ns,
        ),
        max_participation=scenario.max_participation,
    )
    if exit_fill.fraction <= 0.0:
        result.update(
            execution_status="UNRESOLVED_EXIT_LIQUIDITY",
            entry_time=ExactTimestampNs(fill_time_ns)
            if fill_time_ns is not None
            else None,
            entry_price=maker_price,
            entry_fill_fraction=entry_fill.fraction,
            unclosed_exposure_fraction=entry_fill.fraction,
        )
        return result
    if fill_time_ns is None:
        raise StreamingExecutionError("maker fill is missing its causal public-trade time")
    return _complete_execution(
        result,
        direction=direction,
        baseline_mid=_nullable_baseline_mid(information_event),
        entry_reference_mid=entry_bbo.mid,
        exit_reference_mid=exit_bbo.mid,
        entry_time_ns=fill_time_ns,
        exit_time_ns=exit_bbo.received_time_ns,
        entry_fill=entry_fill,
        exit_fill=exit_fill,
        entry_fee_bps=scenario.maker_fee_bps,
        exit_fee_bps=scenario.taker_fee_bps,
        scenario=scenario,
        entry_side=entry_side,
        entry_has_slippage=False,
    )


def _event_inputs(
    information_event: Mapping[str, object],
    *,
    interval_start_ns: int,
    interval_end_ns: int,
) -> tuple[int, int, int]:
    interval_start_ns = _time_ns(interval_start_ns, label="interval_start_ns")
    interval_end_ns = _time_ns(interval_end_ns, label="interval_end_ns")
    if interval_end_ns <= interval_start_ns:
        raise StreamingExecutionError("strict interval end must be after its start")
    try:
        signal_raw = information_event["signal_time"]
        target_raw = information_event["target_time"]
        direction_raw = information_event["signal_direction"]
    except KeyError as exc:
        raise StreamingExecutionError(
            f"information event is missing {exc.args[0]}"
        ) from None
    signal_time_ns = _timestamp_ns(signal_raw, label="signal_time")
    target_time_ns = _timestamp_ns(target_raw, label="target_time")
    direction = _integer(direction_raw, label="signal_direction")
    if not interval_start_ns <= signal_time_ns < target_time_ns < interval_end_ns:
        raise StreamingExecutionError(
            "execution event lifecycle must fit its half-open strict interval"
        )
    return signal_time_ns, target_time_ns, direction


def _nullable_baseline_mid(information_event: Mapping[str, object]) -> float:
    """Return a valid baseline or NaN for an unevaluable information response.

    The baseline is not an entry precondition.  The pandas oracle still models
    execution when its response baseline is missing or stale; only break-even,
    which divides by that baseline, becomes unavailable.
    """

    raw = information_event.get("baseline_mid")
    if raw is None:
        return math.nan
    if isinstance(raw, bool) or not isinstance(raw, (Real, str)):
        raise StreamingExecutionError("baseline_mid must be numeric or missing")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise StreamingExecutionError("baseline_mid must be numeric or missing") from None
    if not math.isfinite(value):
        return math.nan
    if value <= 0.0:
        raise StreamingExecutionError("baseline_mid must be positive when present")
    return value


def model_scenario_execution_events(
    information_event: Mapping[str, object],
    timelines: ExecutionTimelines,
    interval_start_ns: int,
    interval_end_ns: int,
    scenario: ExecutionAssumptions,
    *,
    maximum_age_ns: int,
) -> tuple[dict[str, object], dict[str, object]]:
    """Return one scenario's taker row followed by its maker row."""

    if isinstance(maximum_age_ns, bool) or not isinstance(maximum_age_ns, int):
        raise StreamingExecutionError("maximum_age_ns must be a positive integer")
    if maximum_age_ns <= 0:
        raise StreamingExecutionError("maximum_age_ns must be a positive integer")
    return (
        model_taker_execution(
            information_event,
            timelines,
            interval_start_ns,
            interval_end_ns,
            scenario,
            maximum_age_ns=maximum_age_ns,
        ),
        model_maker_execution(
            information_event,
            timelines,
            interval_start_ns,
            interval_end_ns,
            scenario,
            maximum_age_ns=maximum_age_ns,
        ),
    )


def model_execution_events(
    information_event: Mapping[str, object],
    timelines: ExecutionTimelines,
    interval_start_ns: int,
    interval_end_ns: int,
    config: LeadLagConfig,
) -> tuple[dict[str, object], ...]:
    """Return every configured Hyperliquid attempt without population materialization.

    Rows retain the pandas oracle's deterministic order: for each configured
    scenario, taker precedes maker.  All lookups are on received time and are
    bounded to the supplied strict interval.
    """

    maximum_age_ns = config.max_book_age_ms * 1_000_000
    rows: list[dict[str, object]] = []
    for scenario in config.execution_scenarios:
        rows.extend(
            model_scenario_execution_events(
                information_event,
                timelines,
                interval_start_ns,
                interval_end_ns,
                scenario,
                maximum_age_ns=maximum_age_ns,
            )
        )
    return tuple(rows)


__all__ = [
    "BboSample",
    "ExecutionTimelines",
    "L2Snapshot",
    "StreamingExecutionError",
    "TradeSample",
    "TupleExecutionTimelines",
    "model_execution_events",
    "model_maker_execution",
    "model_scenario_execution_events",
    "model_taker_execution",
    "utc_iso_from_ns",
]
