from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


class H1Action(StrEnum):
    BID_ONLY = "BID_ONLY"
    ASK_ONLY = "ASK_ONLY"
    NO_QUOTE = "NO_QUOTE"


class ResearchState(StrEnum):
    ACTION = "ACTION"
    NO_TRADE = "NO_TRADE"


MARKOUT_HORIZONS_MS = (100, 500, 1_000, 5_000, 30_000)


@dataclass(frozen=True, slots=True)
class ActionDelayBand:
    minimum_ms: int
    maximum_ms: int

    def __post_init__(self) -> None:
        if (
            type(self.minimum_ms) is not int
            or type(self.maximum_ms) is not int
            or self.minimum_ms < 0
            or self.maximum_ms < self.minimum_ms
        ):
            raise ValueError("action delay band is invalid")


@dataclass(frozen=True, slots=True)
class MarkoutObservation:
    horizon_ms: int
    observed_at_ns: int
    markout: Decimal | None

    def __post_init__(self) -> None:
        if type(self.horizon_ms) is not int or self.horizon_ms not in MARKOUT_HORIZONS_MS:
            raise ValueError("markout horizon is not part of the frozen H1 contract")
        if type(self.observed_at_ns) is not int or self.observed_at_ns < 0:
            raise ValueError("markout observation time cannot be negative")
        if self.markout is not None and not self.markout.is_finite():
            raise ValueError("markout must be absent or finite")


@dataclass(frozen=True, slots=True)
class H1DatasetRow:
    observation_id: str
    instrument_id: str
    decision_time_ns: int
    action: H1Action
    state: ResearchState
    action_delay_band: ActionDelayBand
    markouts: tuple[MarkoutObservation, ...]
    fill_to_close_markout: Decimal | None
    no_trade_reason: str | None
    fill_to_close_observed_at_ns: int | None = None

    def __post_init__(self) -> None:
        if (
            not self.observation_id
            or not self.instrument_id
            or type(self.decision_time_ns) is not int
            or self.decision_time_ns < 0
        ):
            raise ValueError("H1 dataset identity or decision time is invalid")
        if self.fill_to_close_markout is not None and not self.fill_to_close_markout.is_finite():
            raise ValueError("fill-to-close markout must be absent or finite")
        horizons = tuple(item.horizon_ms for item in self.markouts)
        if horizons != MARKOUT_HORIZONS_MS:
            raise ValueError("H1 row must expose all frozen markout horizons in order")
        for markout in self.markouts:
            minimum_observed_at = self.decision_time_ns + markout.horizon_ms * 1_000_000
            if markout.observed_at_ns < minimum_observed_at:
                raise ValueError("markout leaks information from before its causal horizon")
        if self.action is H1Action.NO_QUOTE:
            if self.state is not ResearchState.NO_TRADE or not self.no_trade_reason:
                raise ValueError("NO_QUOTE requires an explicit no-trade reason")
            if self.fill_to_close_markout is not None:
                raise ValueError("NO_QUOTE cannot have a fill-to-close markout")
        elif self.state is not ResearchState.ACTION or self.no_trade_reason is not None:
            raise ValueError("quoted actions must use ACTION state without a no-trade reason")
        if (self.fill_to_close_markout is None) != (
            self.fill_to_close_observed_at_ns is None
        ):
            raise ValueError("fill-to-close markout and observation time must be present together")
        if (
            self.fill_to_close_observed_at_ns is not None
            and self.fill_to_close_observed_at_ns < self.decision_time_ns
        ):
            raise ValueError("fill-to-close markout leaks information before the decision")


def build_h1_row(
    *,
    observation_id: str,
    instrument_id: str,
    decision_time_ns: int,
    action: H1Action,
    state: ResearchState,
    action_delay_band: ActionDelayBand,
    markout_observations: Mapping[int, tuple[int, Decimal | None]],
    fill_to_close_markout: Decimal | None,
    fill_to_close_observed_at_ns: int | None,
    no_trade_reason: str | None,
) -> H1DatasetRow:
    if set(markout_observations) != set(MARKOUT_HORIZONS_MS):
        raise ValueError("H1 builder requires exactly the frozen markout horizons")
    markouts = tuple(
        MarkoutObservation(
            horizon_ms=horizon,
            observed_at_ns=markout_observations[horizon][0],
            markout=markout_observations[horizon][1],
        )
        for horizon in MARKOUT_HORIZONS_MS
    )
    return H1DatasetRow(
        observation_id=observation_id,
        instrument_id=instrument_id,
        decision_time_ns=decision_time_ns,
        action=action,
        state=state,
        action_delay_band=action_delay_band,
        markouts=markouts,
        fill_to_close_markout=fill_to_close_markout,
        no_trade_reason=no_trade_reason,
        fill_to_close_observed_at_ns=fill_to_close_observed_at_ns,
    )


class EventLabelType(StrEnum):
    TWAP = "TWAP"
    LIQUIDATION = "LIQUIDATION"
    FORCED_FLOW = "FORCED_FLOW"


@dataclass(frozen=True, slots=True)
class CausalEventLabel:
    label_type: EventLabelType
    source_event_id: str
    source_event_time_ns: int
    observed_at_ns: int
    source_metadata_version: str
    official_public_source: bool
    verified_causal: bool

    def __post_init__(self) -> None:
        if not self.source_event_id or not self.source_metadata_version:
            raise ValueError("event label provenance is incomplete")
        if self.source_event_time_ns < 0 or self.observed_at_ns < self.source_event_time_ns:
            raise ValueError("event label was not available causally")
        if not self.official_public_source or not self.verified_causal:
            raise ValueError("event labels require a verified official public causal source")


@dataclass(frozen=True, slots=True)
class MatchedControlKey:
    instrument_id: str
    utc_bucket: str
    volatility_bucket: str
    spread_bucket: str
    liquidity_bucket: str

    def __post_init__(self) -> None:
        if not all(
            (
                self.instrument_id,
                self.utc_bucket,
                self.volatility_bucket,
                self.spread_bucket,
                self.liquidity_bucket,
            )
        ):
            raise ValueError("matched-control key is incomplete")


@dataclass(frozen=True, slots=True)
class EventWindowRow:
    event: CausalEventLabel
    instrument_id: str
    window_start_ns: int
    window_end_ns: int
    matched_control_key: MatchedControlKey
    action_delay_band: ActionDelayBand
    state: ResearchState

    def __post_init__(self) -> None:
        if self.window_start_ns < 0:
            raise ValueError("event window cannot start before the UTC epoch")
        if self.window_start_ns > self.event.source_event_time_ns:
            raise ValueError("event window starts after the causal event")
        if self.window_end_ns <= self.event.source_event_time_ns:
            raise ValueError("event window must include a post-event interval")
        if self.instrument_id != self.matched_control_key.instrument_id:
            raise ValueError("event and matched-control instruments differ")


def build_event_window(
    *,
    event: CausalEventLabel,
    instrument_id: str,
    pre_event_ms: int,
    post_event_ms: int,
    matched_control_key: MatchedControlKey,
    action_delay_band: ActionDelayBand,
    state: ResearchState = ResearchState.NO_TRADE,
) -> EventWindowRow:
    if pre_event_ms < 0 or post_event_ms <= 0:
        raise ValueError("event window bounds must be non-negative/positive")
    return EventWindowRow(
        event=event,
        instrument_id=instrument_id,
        window_start_ns=event.source_event_time_ns - pre_event_ms * 1_000_000,
        window_end_ns=event.source_event_time_ns + post_event_ms * 1_000_000,
        matched_control_key=matched_control_key,
        action_delay_band=action_delay_band,
        state=state,
    )


__all__ = [
    "MARKOUT_HORIZONS_MS",
    "ActionDelayBand",
    "CausalEventLabel",
    "EventLabelType",
    "EventWindowRow",
    "H1Action",
    "H1DatasetRow",
    "MarkoutObservation",
    "MatchedControlKey",
    "ResearchState",
    "build_event_window",
    "build_h1_row",
]
