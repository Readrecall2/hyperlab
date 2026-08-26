from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from hyperlab.research_data.canonical import (
    CanonicalValue,
    canonical_json_bytes,
    decode_canonical_json,
)
from hyperlab.research_data.envelope import SYNTHETIC_FIXTURE_LABEL
from hyperlab.research_data.segments import ResearchSegmentReader

from .adapters import (
    CanonicalGhostFixtureEnvelopeAdapter,
    GhostEnvelopeAdapter,
)
from .models import (
    BOUNDARY,
    MODEL_VERSION,
    BookLevel,
    ClockedObservation,
    CostScheduleVersion,
    DepthConsumption,
    ExecutableBook,
    ExecutionMechanismVersion,
    ExposureReport,
    GhostReport,
    GroupReport,
    InstrumentGridVersion,
    LatencyModelVersion,
    OrderReport,
    PnlReport,
    QueueModelVersion,
    QueueScenario,
    ReplayProvenance,
    Side,
    TimeInForce,
    VenueHealth,
    exact_decimal,
)

_ZERO = Decimal("0")
_BPS = Decimal("10000")


def _mapping(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return cast(dict[str, Any], value)


def _sequence(value: object, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be an array")
    return value


def _integer(value: object, *, label: str, optional: bool = False) -> int | None:
    if value is None and optional:
        return None
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _text(value: object, *, label: str, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be non-empty text")
    return value


def _event_time(event: Mapping[str, object]) -> int:
    kind = event.get("kind")
    field = "decision_ns" if kind == "ORDER" else "receive_ns"
    value = event.get(field)
    if type(value) is not int:
        raise ValueError(f"{kind} event requires integer {field}")
    return value


@dataclass(frozen=True, slots=True)
class GhostFixture:
    body: dict[str, CanonicalValue]
    fixture_sha256: str

    @classmethod
    def from_bytes(cls, value: bytes) -> GhostFixture:
        stripped = value.rstrip(b"\r\n")
        if value[len(stripped) :] not in {b"", b"\n", b"\r\n"}:
            raise ValueError("fixture may contain at most one terminal newline")
        decoded = decode_canonical_json(stripped, require_canonical=True)
        if not isinstance(decoded, dict):
            raise ValueError("ghost fixture must be a canonical JSON object")
        expected = {
            "boundary",
            "events",
            "fixture_label",
            "model",
            "scenario_id",
            "schema_version",
        }
        if set(decoded) != expected:
            raise ValueError("ghost fixture fields differ from schema v1")
        if decoded["schema_version"] != 1:
            raise ValueError("unsupported ghost fixture schema")
        if decoded["boundary"] != BOUNDARY:
            raise ValueError("ghost fixture boundary is not research-only")
        if decoded["fixture_label"] != SYNTHETIC_FIXTURE_LABEL:
            raise ValueError("synthetic fixtures require visible SYNTHETIC/FIXTURE provenance")
        events = _sequence(decoded["events"], label="events")
        if not events:
            raise ValueError("ghost fixture requires events")
        times = [_event_time(_mapping(event, label="event")) for event in events]
        if times != sorted(times):
            raise ValueError("ghost fixture events must be causally ordered")
        return cls(
            body=decoded,
            fixture_sha256=hashlib.sha256(stripped).hexdigest(),
        )

    @property
    def scenario_id(self) -> str:
        return cast(str, self.body["scenario_id"])

    @property
    def fixture_label(self) -> str:
        return cast(str, self.body["fixture_label"])

    @property
    def model(self) -> dict[str, Any]:
        return _mapping(self.body["model"], label="model")

    @property
    def events(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            _mapping(item, label="event") for item in _sequence(self.body["events"], label="events")
        )


@dataclass(frozen=True, slots=True)
class _Config:
    latency: LatencyModelVersion
    queue: QueueModelVersion
    grids: tuple[InstrumentGridVersion, ...]
    costs: tuple[CostScheduleVersion, ...]
    mechanisms: tuple[ExecutionMechanismVersion, ...]
    stale_after_ns: int
    multi_leg_timeout_ns: int
    closeout_model_id: str
    config_sha256: str

    @classmethod
    def parse(cls, value: Mapping[str, object]) -> _Config:
        expected = {
            "closeout",
            "cost_schedules",
            "grids",
            "latency",
            "model_version",
            "multi_leg_timeout_ns",
            "queue",
            "mechanisms",
            "stale_after_ns",
        }
        if set(value) != expected or value["model_version"] != MODEL_VERSION:
            raise ValueError("ghost model fields or model version are invalid")
        latency = _mapping(value["latency"], label="latency")
        queue = _mapping(value["queue"], label="queue")
        closeout = _mapping(value["closeout"], label="closeout")
        if closeout.get("required") is not True:
            raise ValueError("PESSIMISTIC_CLOSEOUT_MODEL_IS_REQUIRED")
        latency_model = LatencyModelVersion(
            model_id=cast(str, _text(latency.get("model_id"), label="latency model id")),
            decision_ns=cast(int, _integer(latency.get("decision_ns"), label="decision latency")),
            transit_ns=cast(int, _integer(latency.get("transit_ns"), label="transit latency")),
            admission_ns=cast(int, _integer(latency.get("admission_ns"), label="admission latency")),
            ack_ns=cast(int, _integer(latency.get("ack_ns"), label="ack latency")),
            cancel_ns=cast(int, _integer(latency.get("cancel_ns"), label="cancel latency")),
            clock_uncertainty_ns=cast(
                int, _integer(latency.get("clock_uncertainty_ns"), label="clock uncertainty")
            ),
        )
        queue_model = QueueModelVersion(
            model_id=cast(str, _text(queue.get("model_id"), label="queue model id")),
            primary=QueueScenario(cast(str, queue.get("primary"))),
            pessimistic_ahead_multiplier=exact_decimal(
                queue.get("pessimistic_ahead_multiplier"), label="pessimistic queue multiplier"
            ),
            conservative_ahead_multiplier=exact_decimal(
                queue.get("conservative_ahead_multiplier"), label="conservative queue multiplier"
            ),
            sensitivity_ahead_multiplier=exact_decimal(
                queue.get("sensitivity_ahead_multiplier"), label="sensitivity queue multiplier"
            ),
        )
        grids = tuple(cls._grid(_mapping(item, label="grid")) for item in _sequence(value["grids"], label="grids"))
        costs = tuple(
            cls._cost(_mapping(item, label="cost schedule"))
            for item in _sequence(value["cost_schedules"], label="cost schedules")
        )
        mechanisms = tuple(
            cls._mechanism(_mapping(item, label="execution mechanism"))
            for item in _sequence(value["mechanisms"], label="execution mechanisms")
        )
        if not grids or not costs or not mechanisms:
            raise ValueError("grid, cost, and mechanism versions are mandatory")
        return cls(
            latency=latency_model,
            queue=queue_model,
            grids=grids,
            costs=costs,
            mechanisms=mechanisms,
            stale_after_ns=cast(int, _integer(value["stale_after_ns"], label="stale threshold")),
            multi_leg_timeout_ns=cast(
                int, _integer(value["multi_leg_timeout_ns"], label="multi-leg timeout")
            ),
            closeout_model_id=cast(
                str, _text(closeout.get("model_id"), label="closeout model id")
            ),
            config_sha256=hashlib.sha256(canonical_json_bytes(value)).hexdigest(),
        )

    @staticmethod
    def _grid(value: Mapping[str, object]) -> InstrumentGridVersion:
        return InstrumentGridVersion(
            grid_id=cast(str, _text(value.get("grid_id"), label="grid id")),
            venue=cast(str, _text(value.get("venue"), label="grid venue")),
            instrument_id=cast(str, _text(value.get("instrument_id"), label="grid instrument")),
            effective_from_ns=cast(
                int, _integer(value.get("effective_from_ns"), label="grid effective from")
            ),
            effective_to_ns=_integer(
                value.get("effective_to_ns"), label="grid effective to", optional=True
            ),
            tick_size=exact_decimal(value.get("tick_size"), label="tick size", positive=True),
            lot_size=exact_decimal(value.get("lot_size"), label="lot size", positive=True),
        )

    @staticmethod
    def _mechanism(value: Mapping[str, object]) -> ExecutionMechanismVersion:
        supported = tuple(
            TimeInForce(cast(str, item))
            for item in _sequence(
                value.get("supported_time_in_force"),
                label="supported time in force",
            )
        )
        return ExecutionMechanismVersion(
            mechanism_id=cast(
                str, _text(value.get("mechanism_id"), label="mechanism id")
            ),
            venue=cast(str, _text(value.get("venue"), label="mechanism venue")),
            instrument_id=cast(
                str, _text(value.get("instrument_id"), label="mechanism instrument")
            ),
            effective_from_ns=cast(
                int,
                _integer(value.get("effective_from_ns"), label="mechanism effective from"),
            ),
            effective_to_ns=_integer(
                value.get("effective_to_ns"),
                label="mechanism effective to",
                optional=True,
            ),
            supported_time_in_force=supported,
            maker_fill_requires_aggressor_flow=value.get(
                "maker_fill_requires_aggressor_flow"
            ) is True,
            supports_partial_fills=value.get("supports_partial_fills") is True,
            cancel_replace_loses_priority=value.get("cancel_replace_loses_priority") is True,
        )


    @staticmethod
    def _cost(value: Mapping[str, object]) -> CostScheduleVersion:
        return CostScheduleVersion(
            schedule_id=cast(str, _text(value.get("schedule_id"), label="schedule id")),
            venue=cast(str, _text(value.get("venue"), label="cost venue")),
            instrument_id=cast(str, _text(value.get("instrument_id"), label="cost instrument")),
            effective_from_ns=cast(
                int, _integer(value.get("effective_from_ns"), label="cost effective from")
            ),
            effective_to_ns=_integer(
                value.get("effective_to_ns"), label="cost effective to", optional=True
            ),
            maker_fee_bps=exact_decimal(value.get("maker_fee_bps"), label="maker fee"),
            taker_fee_bps=exact_decimal(value.get("taker_fee_bps"), label="taker fee"),
            funding_bps=exact_decimal(value.get("funding_bps"), label="funding"),
            hedge_fee_bps=exact_decimal(value.get("hedge_fee_bps"), label="hedge fee"),
            opportunity_cost_bps_per_second=exact_decimal(
                value.get("opportunity_cost_bps_per_second"), label="opportunity cost"
            ),
        )

    def grid(self, venue: str, instrument: str, timestamp_ns: int) -> InstrumentGridVersion:
        matches = [
            item
            for item in self.grids
            if item.venue == venue
            and item.instrument_id == instrument
            and item.effective_from_ns <= timestamp_ns
            and (item.effective_to_ns is None or timestamp_ns < item.effective_to_ns)
        ]
        if len(matches) != 1:
            raise ValueError("EXACTLY_ONE_GRID_VERSION_REQUIRED")
        return matches[0]

    def cost(self, venue: str, instrument: str, timestamp_ns: int) -> CostScheduleVersion:
        matches = [
            item
            for item in self.costs
            if item.venue == venue
            and item.instrument_id == instrument
            and item.effective_from_ns <= timestamp_ns
            and (item.effective_to_ns is None or timestamp_ns < item.effective_to_ns)
        ]
        if len(matches) != 1:
            raise ValueError("EXACTLY_ONE_COST_SCHEDULE_REQUIRED")
        return matches[0]

    def mechanism(
        self, venue: str, instrument: str, timestamp_ns: int
    ) -> ExecutionMechanismVersion:
        matches = [
            item
            for item in self.mechanisms
            if item.venue == venue
            and item.instrument_id == instrument
            and item.effective_from_ns <= timestamp_ns
            and (item.effective_to_ns is None or timestamp_ns < item.effective_to_ns)
        ]
        if len(matches) != 1:
            raise ValueError("EXACTLY_ONE_EXECUTION_MECHANISM_REQUIRED")
        return matches[0]


@dataclass(frozen=True, slots=True)
class _BookEvent:
    event_id: str
    book: ExecutableBook
    resync_complete: bool


@dataclass(frozen=True, slots=True)
class _TradeEvent:
    event_id: str
    venue: str
    instrument_id: str
    source_ns: int | None
    receive_ns: int
    price: Decimal
    quantity: Decimal
    aggressor_side: Side


@dataclass(frozen=True, slots=True)
class _HealthEvent:
    event_id: str

    venue: str
    instrument_id: str
    receive_ns: int
    health: VenueHealth
    reason: str


@dataclass(frozen=True, slots=True)
class _Order:
    order_id: str
    venue: str
    instrument_id: str
    decision_ns: int
    side: Side
    quantity: Decimal
    limit_price: Decimal
    time_in_force: TimeInForce
    cancel_request_ns: int | None
    group_id: str | None
    leg_index: int
    role: str
    depends_on_order_id: str | None


@dataclass(frozen=True, slots=True)
class _Fill:
    order_id: str
    venue: str
    instrument_id: str
    side: Side
    quantity: Decimal
    notional: Decimal
    fee: Decimal
    timestamp_ns: int
    role: str
    forced: bool = False


class GhostReplay:
    def __init__(
        self,
        fixture: GhostFixture,
        *,
        input_adapter_id: str = "canonical-direct-fixture-v1",
        raw_manifest_sha256: str | None = None,
        raw_root_sha256: str | None = None,
        segment_sha256s: tuple[str, ...] = (),
    ) -> None:
        self.fixture = fixture
        self.config = _Config.parse(fixture.model)
        self.raw_manifest_sha256 = raw_manifest_sha256
        self.raw_root_sha256 = raw_root_sha256
        self.segment_sha256s = segment_sha256s
        self.input_adapter_id = input_adapter_id
        self._books: list[_BookEvent] = []
        self._trades: list[_TradeEvent] = []
        self._health: list[_HealthEvent] = []
        self._orders: list[_Order] = []
        self._funding: list[dict[str, Any]] = []
        self._parse_events()

    def _parse_events(self) -> None:
        ids: set[str] = set()
        for event in self.fixture.events:
            kind = cast(str, _text(event.get("kind"), label="event kind"))
            if kind == "BOOK":
                parsed = self._parse_book(event)
                event_id = parsed.event_id
                self._books.append(parsed)
            elif kind == "TRADE":
                parsed_trade = self._parse_trade(event)
                event_id = parsed_trade.event_id
                self._trades.append(parsed_trade)
            elif kind in {"GAP", "RECONNECT", "OUTAGE"}:
                parsed_health = self._parse_health(event, VenueHealth(kind))
                event_id = parsed_health.event_id
                self._health.append(parsed_health)
            elif kind == "ORDER":
                parsed_order = self._parse_order(event)
                event_id = parsed_order.order_id
                self._orders.append(parsed_order)
            elif kind == "FUNDING":
                event_id = cast(str, _text(event.get("event_id"), label="funding event id"))
                self._funding.append(event)
            else:
                raise ValueError(f"unsupported ghost event kind {kind!r}")
            if event_id in ids:
                raise ValueError("event and order ids must be unique")
            ids.add(event_id)

    def _parse_book(self, event: Mapping[str, object]) -> _BookEvent:
        venue = cast(str, _text(event.get("venue"), label="book venue"))
        instrument = cast(str, _text(event.get("instrument_id"), label="book instrument"))
        receive = cast(int, _integer(event.get("receive_ns"), label="book receive time"))
        grid = self.config.grid(venue, instrument, receive)

        def levels(name: str) -> tuple[BookLevel, ...]:
            result = []
            for raw in _sequence(event.get(name), label=name):
                pair = _sequence(raw, label="book level")
                if len(pair) != 2:
                    raise ValueError("book level must contain price and quantity")
                result.append(
                    BookLevel(
                        exact_decimal(pair[0], label="book price", positive=True),
                        exact_decimal(pair[1], label="book quantity", positive=True),
                    )
                )
            return tuple(result)

        return _BookEvent(
            event_id=cast(str, _text(event.get("event_id"), label="book event id")),
            book=ExecutableBook(
                venue=venue,
                instrument_id=instrument,
                observation=ClockedObservation(
                    source_ns=_integer(
                        event.get("source_ns"), label="book source time", optional=True
                    ),
                    receive_ns=receive,
                    clock_uncertainty_ns=cast(
                        int,
                        _integer(
                            event.get("clock_uncertainty_ns"),
                            label="book clock uncertainty",
                        ),
                    ),
                ),
                health=VenueHealth.FRESH,
                grid=grid,
                bids=levels("bids"),
                asks=levels("asks"),
            ),
            resync_complete=event.get("resync_complete") is True,
        )

    def _parse_trade(self, event: Mapping[str, object]) -> _TradeEvent:
        return _TradeEvent(
            event_id=cast(str, _text(event.get("event_id"), label="trade event id")),
            venue=cast(str, _text(event.get("venue"), label="trade venue")),
            instrument_id=cast(
                str, _text(event.get("instrument_id"), label="trade instrument")
            ),
            source_ns=_integer(event.get("source_ns"), label="trade source time", optional=True),
            receive_ns=cast(int, _integer(event.get("receive_ns"), label="trade receive time")),
            price=exact_decimal(event.get("price"), label="trade price", positive=True),
            quantity=exact_decimal(event.get("quantity"), label="trade quantity", positive=True),
            aggressor_side=Side(
                cast(str, _text(event.get("aggressor_side"), label="aggressor side"))
            ),
        )

    def _parse_health(self, event: Mapping[str, object], health: VenueHealth) -> _HealthEvent:
        return _HealthEvent(
            event_id=cast(str, _text(event.get("event_id"), label="health event id")),
            venue=cast(str, _text(event.get("venue"), label="health venue")),
            instrument_id=cast(
                str, _text(event.get("instrument_id"), label="health instrument")
            ),
            receive_ns=cast(int, _integer(event.get("receive_ns"), label="health receive time")),
            health=health,
            reason=cast(str, _text(event.get("reason"), label="health reason")),
        )

    def _parse_order(self, event: Mapping[str, object]) -> _Order:
        role = cast(str, _text(event.get("role"), label="order role"))
        if role not in {"PRIMARY", "HEDGE", "REPAIR"}:
            raise ValueError("order role must be PRIMARY, HEDGE, or REPAIR")
        return _Order(
            order_id=cast(str, _text(event.get("order_id"), label="order id")),
            venue=cast(str, _text(event.get("venue"), label="order venue")),
            instrument_id=cast(
                str, _text(event.get("instrument_id"), label="order instrument")
            ),
            decision_ns=cast(int, _integer(event.get("decision_ns"), label="decision time")),
            side=Side(cast(str, _text(event.get("side"), label="order side"))),
            quantity=exact_decimal(event.get("quantity"), label="order quantity", positive=True),
            limit_price=exact_decimal(
                event.get("limit_price"), label="order limit price", positive=True
            ),
            time_in_force=TimeInForce(
                cast(str, _text(event.get("time_in_force"), label="time in force"))
            ),
            cancel_request_ns=_integer(
                event.get("cancel_request_ns"), label="cancel request", optional=True
            ),
            group_id=_text(event.get("group_id"), label="group id", optional=True),
            leg_index=cast(int, _integer(event.get("leg_index"), label="leg index")),
            role=role,
            depends_on_order_id=_text(
                event.get("depends_on_order_id"),
                label="dependency order id",
                optional=True,
            ),
        )

    def _book_at(self, venue: str, instrument: str, timestamp_ns: int) -> _BookEvent | None:
        matches = [
            item
            for item in self._books
            if item.book.venue == venue
            and item.book.instrument_id == instrument
            and item.book.observation.receive_ns <= timestamp_ns
        ]
        return None if not matches else matches[-1]

    def _health_at(self, venue: str, instrument: str, timestamp_ns: int) -> tuple[VenueHealth, str]:
        relevant: list[tuple[int, VenueHealth, str]] = []
        for book_event in self._books:
            if (
                book_event.book.venue == venue
                and book_event.book.instrument_id == instrument
                and book_event.book.observation.receive_ns <= timestamp_ns
                and book_event.resync_complete
            ):
                relevant.append((book_event.book.observation.receive_ns, VenueHealth.FRESH, "FRESH_BOOK"))
        for health_event in self._health:
            if (
                health_event.venue == venue
                and health_event.instrument_id == instrument
                and health_event.receive_ns <= timestamp_ns
            ):
                relevant.append((health_event.receive_ns, health_event.health, health_event.reason))
        if not relevant:
            return VenueHealth.OUTAGE, "NO_EXECUTABLE_BOOK"
        _, health, reason = sorted(relevant, key=lambda item: item[0])[-1]
        if health is VenueHealth.FRESH:
            book = self._book_at(venue, instrument, timestamp_ns)
            if book is None or timestamp_ns - book.book.observation.receive_ns > self.config.stale_after_ns:
                return VenueHealth.STALE, "STALE_AFTER_THRESHOLD"
        return health, reason

    def _no_trade(
        self,
        order: _Order,
        *,
        reason: str,
        timeline: dict[str, int | None],
        priority_generation: int,
        dependency_fill: Decimal | None,
    ) -> OrderReport:
        return OrderReport(
            order_id=order.order_id,
            venue=order.venue,
            instrument_id=order.instrument_id,
            side=order.side,
            time_in_force=order.time_in_force,
            role=order.role,
            status="NO_TRADE",
            reason=reason,
            requested_quantity=order.quantity,
            admitted_quantity=_ZERO,
            filled_quantity=_ZERO,
            unfilled_quantity=order.quantity,
            notional=_ZERO,
            average_price=None,
            fee=_ZERO,
            fill_timestamp_ns=None,
            level_count=0,
            timeline=timeline,
            queue_sensitivity={},
            cancel_fill_race_observed=False,
            priority_generation=priority_generation,
            depends_on_order_id=order.depends_on_order_id,
            depends_on_filled_quantity=dependency_fill,
            group_id=order.group_id,
            leg_index=order.leg_index,
        )

    def _maker_fill(
        self,
        order: _Order,
        *,
        event_id: str,
        book: ExecutableBook,
        quantity: Decimal,
        timeline: dict[str, int | None],
        depth_state: dict[tuple[str, Side], tuple[BookLevel, ...]],
    ) -> tuple[dict[str, Decimal], int, bool, int | None]:
        book_quantity = _ZERO
        taker_side = Side.SELL if order.side is Side.BUY else Side.BUY
        levels = depth_state.get(
            (event_id, taker_side),
            book.bids if order.side is Side.BUY else book.asks,
        )
        for level in levels:
            if level.price == order.limit_price:
                book_quantity = level.quantity
                break
        cancel_ack = timeline["cancel_ack_ns"]
        end = cancel_ack if cancel_ack is not None else 2**63 - 1
        ack = cast(int, timeline["ack_ns"])
        cancel_request = timeline["cancel_request_ns"]
        end = min(
            end,
            book.observation.receive_ns + self.config.stale_after_ns,
        )
        health_cutoffs = [
            event.receive_ns
            for event in self._health
            if event.venue == order.venue
            and event.instrument_id == order.instrument_id
            and ack <= event.receive_ns <= end
        ]
        if health_cutoffs:
            end = min(end, min(health_cutoffs) - 1)
        trade_quantity = _ZERO
        race = False
        matching_trades: list[_TradeEvent] = []
        for trade in self._trades:
            if (
                trade.venue != order.venue
                or trade.instrument_id != order.instrument_id
                or trade.receive_ns < ack
                or trade.receive_ns > end
            ):
                continue
            matches = (
                order.side is Side.BUY
                and trade.aggressor_side is Side.SELL
                and trade.price <= order.limit_price
            ) or (
                order.side is Side.SELL
                and trade.aggressor_side is Side.BUY
                and trade.price >= order.limit_price
            )
            if matches:
                matching_trades.append(trade)
                trade_quantity += trade.quantity
                if cancel_request is not None and trade.receive_ns >= cancel_request:
                    race = True
        scenario_fills: dict[str, Decimal] = {}
        for scenario, multiplier in self.config.queue.multipliers().items():
            queue_ahead = book_quantity * multiplier
            scenario_fills[scenario.value] = min(quantity, max(_ZERO, trade_quantity - queue_ahead))
        primary_queue = book_quantity * self.config.queue.pessimistic_ahead_multiplier
        cumulative = _ZERO
        previous_fill = _ZERO
        fill_time: int | None = None
        for trade in matching_trades:
            cumulative += trade.quantity
            current_fill = min(quantity, max(_ZERO, cumulative - primary_queue))
            if current_fill > previous_fill:
                fill_time = trade.receive_ns
                previous_fill = current_fill
        primary_fill = scenario_fills[QueueScenario.PESSIMISTIC.value]
        return scenario_fills, 1 if primary_fill > 0 else 0, race, fill_time

    def _execute_order(
        self,
        order: _Order,
        *,
        priority_generation: int,
        prior: Mapping[str, OrderReport],
        depth_state: dict[tuple[str, Side], tuple[BookLevel, ...]],
    ) -> tuple[OrderReport, _Fill | None]:
        timeline = self.config.latency.timeline(order.decision_ns, order.cancel_request_ns)
        dependency_fill: Decimal | None = None
        quantity = order.quantity
        if order.depends_on_order_id is not None:
            dependency = prior.get(order.depends_on_order_id)
            if dependency is None:
                return (
                    self._no_trade(
                        order,
                        reason="DEPENDENCY_ORDER_NOT_AVAILABLE",
                        timeline=timeline,
                        priority_generation=priority_generation,
                        dependency_fill=None,
                    ),
                    None,
                )
            dependency_fill = dependency.filled_quantity
            if dependency_fill == 0:
                return (
                    self._no_trade(
                        order,
                        reason="DEPENDENCY_NOT_FILLED",
                        timeline=timeline,
                        priority_generation=priority_generation,
                        dependency_fill=dependency_fill,
                    ),
                    None,
                )
            quantity = min(quantity, dependency_fill)
            dependency_fill_time = dependency.fill_timestamp_ns
            uncertainty = self.config.latency.clock_uncertainty_ns
            if (
                dependency_fill_time is None
                or dependency_fill_time + uncertainty
                > order.decision_ns - uncertainty
            ):
                return (
                    self._no_trade(
                        order,
                        reason="DEPENDENCY_FILL_NOT_KNOWN_AT_DECISION",
                        timeline=timeline,
                        priority_generation=priority_generation,
                        dependency_fill=dependency_fill,
                    ),
                    None,
                )
        decision_health, _ = self._health_at(
            order.venue, order.instrument_id, order.decision_ns
        )
        if decision_health is not VenueHealth.FRESH:
            return (
                self._no_trade(
                    order,
                    reason=f"VENUE_HEALTH_{decision_health.value}",
                    timeline=timeline,
                    priority_generation=priority_generation,
                    dependency_fill=dependency_fill,
                ),
                None,
            )
        decision_book_event = self._book_at(order.venue, order.instrument_id, order.decision_ns)
        if decision_book_event is None:
            return (
                self._no_trade(
                    order,
                    reason="NO_POINT_IN_TIME_BOOK",
                    timeline=timeline,
                    priority_generation=priority_generation,
                    dependency_fill=dependency_fill,
                ),
                None,
            )
        try:
            decision_book_event.book.observation.assert_known_at(
                order.decision_ns,
                decision_clock_uncertainty_ns=self.config.latency.clock_uncertainty_ns,
            )
        except ValueError as error:
            return (
                self._no_trade(
                    order,
                    reason=str(error),
                    timeline=timeline,
                    priority_generation=priority_generation,
                    dependency_fill=dependency_fill,
                ),
                None,
            )
        admission = cast(int, timeline["admission_ns"])
        health, _ = self._health_at(order.venue, order.instrument_id, admission)
        if health is not VenueHealth.FRESH:
            return (
                self._no_trade(
                    order,
                    reason=f"VENUE_HEALTH_{health.value}",
                    timeline=timeline,
                    priority_generation=priority_generation,
                    dependency_fill=dependency_fill,
                ),
                None,
            )
        admission_event = self._book_at(order.venue, order.instrument_id, admission)
        if admission_event is None:
            raise AssertionError("fresh health requires a book")
        key = admission_event.event_id
        book = admission_event.book
        current_grid = self.config.grid(order.venue, order.instrument_id, admission)
        if current_grid.grid_id != book.grid.grid_id:
            return (
                self._no_trade(
                    order,
                    reason="GRID_VERSION_CHANGED_WITHOUT_FRESH_BOOK",
                    timeline=timeline,
                    priority_generation=priority_generation,
                    dependency_fill=dependency_fill,
                ),
                None,
            )
        try:
            book.grid.assert_quantity(quantity)
            book.grid.assert_price(order.limit_price)
        except ValueError as error:
            return (
                self._no_trade(
                    order,
                    reason=str(error),
                    timeline=timeline,
                    priority_generation=priority_generation,
                    dependency_fill=dependency_fill,
                ),
                None,
            )
        cost = self.config.cost(order.venue, order.instrument_id, admission)
        mechanism = self.config.mechanism(order.venue, order.instrument_id, admission)
        if order.time_in_force not in mechanism.supported_time_in_force:
            return (
                self._no_trade(
                    order,
                    reason="TIME_IN_FORCE_UNSUPPORTED_BY_MECHANISM",
                    timeline=timeline,
                    priority_generation=priority_generation,
                    dependency_fill=dependency_fill,
                ),
                None,
            )
        if order.time_in_force.is_maker:
            bid_levels = depth_state.get((key, Side.SELL), book.bids)
            ask_levels = depth_state.get((key, Side.BUY), book.asks)
            if not bid_levels or not ask_levels:
                return (
                    self._no_trade(
                        order,
                        reason="EXECUTABLE_BBO_DEPTH_EXHAUSTED",
                        timeline=timeline,
                        priority_generation=priority_generation,
                        dependency_fill=dependency_fill,
                    ),
                    None,
                )
            would_take = (
                order.side is Side.BUY and order.limit_price >= ask_levels[0].price
            ) or (order.side is Side.SELL and order.limit_price <= bid_levels[0].price)
            if would_take:
                report = self._no_trade(
                    order,
                    reason="POST_ONLY_WOULD_TAKE",
                    timeline=timeline,
                    priority_generation=priority_generation,
                    dependency_fill=dependency_fill,
                )
                return replace(report, status="REJECTED"), None
            scenario_fills, level_count, race, fill_time = self._maker_fill(
                order,
                event_id=key,
                book=book,
                quantity=quantity,
                timeline=timeline,
                depth_state=depth_state,
            )
            filled = scenario_fills[QueueScenario.PESSIMISTIC.value]
            notional = filled * order.limit_price
            fee = cost.fee(notional=notional, maker=True, hedge=order.role != "PRIMARY")
            status = "FILLED" if filled == quantity else "PARTIAL" if filled > 0 else "MISSED"
            reason = (
                "FILLED_AFTER_QUEUE_DEPLETION"
                if status == "FILLED"
                else "PARTIAL_AFTER_QUEUE_DEPLETION"
                if status == "PARTIAL"
                else "CONTACT_WITHOUT_PESSIMISTIC_QUEUE_DEPLETION"
            )
            report = OrderReport(
                order.order_id,
                order.venue,
                order.instrument_id,
                order.side,
                order.time_in_force,
                order.role,
                status,
                reason,
                order.quantity,
                quantity,
                filled,
                quantity - filled,
                notional,
                None if filled == 0 else order.limit_price,
                fee,
                fill_time if filled > 0 else None,
                level_count,
                timeline,
                {name: format(value, "f") for name, value in sorted(scenario_fills.items())},
                race,
                priority_generation,
                order.depends_on_order_id,
                dependency_fill,
                order.group_id,
                order.leg_index,
            )
            fill = None
            if filled > 0:
                fill = _Fill(
                    order.order_id,
                    order.venue,
                    order.instrument_id,
                    order.side,
                    filled,
                    notional,
                    fee,
                    cast(int, fill_time),
                    order.role,
                )
            return report, fill

        consumption = self._consume_depth(
            key, book, order.side, quantity, order.limit_price, depth_state
        )
        fee = cost.fee(
            notional=consumption.notional, maker=False, hedge=order.role != "PRIMARY"
        )
        status = (
            "FILLED"
            if consumption.filled_quantity == quantity
            else "PARTIAL"
            if consumption.filled_quantity > 0
            else "MISSED"
        )
        reason = (
            "IOC_EXECUTED_AT_OBSERVED_DEPTH"
            if status == "FILLED"
            else "FINITE_DEPTH_EXHAUSTED"
            if status == "PARTIAL"
            else "IOC_LIMIT_NOT_EXECUTABLE"
        )
        report = OrderReport(
            order.order_id,
            order.venue,
            order.instrument_id,
            order.side,
            order.time_in_force,
            order.role,
            status,
            reason,
            order.quantity,
            quantity,
            consumption.filled_quantity,
            consumption.unfilled_quantity,
            consumption.notional,
            consumption.average_price,
            fee,
            admission if consumption.filled_quantity > 0 else None,
            consumption.levels,
            timeline,
            {},
            False,
            priority_generation,
            order.depends_on_order_id,
            dependency_fill,
            order.group_id,
            order.leg_index,
        )
        fill = None
        if consumption.filled_quantity > 0:
            fill = _Fill(
                order.order_id,
                order.venue,
                order.instrument_id,
                order.side,
                consumption.filled_quantity,
                consumption.notional,
                fee,
                admission,
                order.role,
            )
        return report, fill

    def _forced_close(
        self,
        positions: Mapping[str, Decimal],
        *,
        depth_state: dict[tuple[str, Side], tuple[BookLevel, ...]],
        after_ns: int,
    ) -> tuple[list[_Fill], dict[str, Decimal]]:
        fills: list[_Fill] = []
        unresolved: dict[str, Decimal] = {}
        for key, position in sorted(positions.items()):
            if position == 0:
                continue
            venue, instrument = key.split("|", 1)
            health, _ = self._health_at(venue, instrument, after_ns)
            event = self._book_at(venue, instrument, after_ns)
            if health is not VenueHealth.FRESH or event is None:
                unresolved[key] = position
                continue
            book = event.book
            side = Side.SELL if position > 0 else Side.BUY
            quantity = abs(position)
            limit = book.bids[-1].price if side is Side.SELL else book.asks[-1].price
            consumption = self._consume_depth(
                event.event_id, book, side, quantity, limit, depth_state
            )
            if consumption.filled_quantity > 0:
                cost = self.config.cost(venue, instrument, after_ns)
                fee = cost.fee(notional=consumption.notional, maker=False, hedge=True)
                fills.append(
                    _Fill(
                        f"FORCED_CLOSE:{key}",
                        venue,
                        instrument,
                        side,
                        consumption.filled_quantity,
                        consumption.notional,
                        fee,
                        after_ns,
                        "FORCED_CLOSE",
                        True,
                    )
                )
            residual = quantity - consumption.filled_quantity
            if residual > 0:
                unresolved[key] = residual if position > 0 else -residual
        return fills, unresolved

    def _groups(self, reports: Sequence[OrderReport]) -> tuple[GroupReport, ...]:
        grouped: dict[str, list[OrderReport]] = {}
        for report in reports:
            if report.group_id is not None:
                grouped.setdefault(report.group_id, []).append(report)
        results = []
        for group_id, legs in sorted(grouped.items()):
            ordered = sorted(legs, key=lambda item: item.leg_index)
            ratios = [
                _ZERO
                if leg.admitted_quantity == 0
                else leg.filled_quantity / leg.admitted_quantity
                for leg in ordered
            ]
            first_decision = ordered[0].timeline["decision_ns"]
            last_decision = ordered[-1].timeline["decision_ns"]
            if first_decision is None or last_decision is None:
                raise AssertionError("order report decision time is mandatory")
            timed_out = last_decision - first_decision
            repair = any(item.role == "REPAIR" for item in ordered)
            if all(item.status == "FILLED" for item in ordered):
                status = "REPAIRED" if repair else "COMPLETE"
            elif timed_out > self.config.multi_leg_timeout_ns:
                status = "TIMED_OUT"
            elif ordered[0].filled_quantity > 0:
                status = "HEDGE_PENDING"
            else:
                status = "NO_FILL"
            residual_totals: dict[str, Decimal] = {}
            for leg in ordered:
                key = f"{leg.venue}|{leg.instrument_id}"
                residual_totals[key] = (
                    residual_totals.get(key, _ZERO)
                    + leg.side.sign * leg.filled_quantity
                )
            residual = {
                key: value for key, value in sorted(residual_totals.items()) if value != 0
            }
            results.append(
                GroupReport(
                    group_id,
                    status,
                    tuple(item.order_id for item in ordered),
                    min(ratios, default=_ZERO),
                    residual,
                    self.config.multi_leg_timeout_ns,
                    repair,
                )
            )
        return tuple(results)

    @staticmethod
    def _consume_depth(
        event_id: str,
        book: ExecutableBook,
        side: Side,
        quantity: Decimal,
        limit_price: Decimal,
        depth_state: dict[tuple[str, Side], tuple[BookLevel, ...]],
    ) -> DepthConsumption:
        book.grid.assert_quantity(quantity)
        book.grid.assert_price(limit_price)
        state_key = (event_id, side)
        selected = depth_state.get(
            state_key, book.asks if side is Side.BUY else book.bids
        )
        remaining = quantity
        fills: list[BookLevel] = []
        untouched: list[BookLevel] = []
        consuming = True
        for level in selected:
            allowed = (
                level.price <= limit_price
                if side is Side.BUY
                else level.price >= limit_price
            )
            if not consuming or not allowed or remaining == 0:
                consuming = False
                untouched.append(level)
                continue
            filled = min(remaining, level.quantity)
            fills.append(BookLevel(level.price, filled))
            remaining -= filled
            if filled < level.quantity:
                untouched.append(BookLevel(level.price, level.quantity - filled))
        depth_state[state_key] = tuple(untouched)
        return DepthConsumption(
            requested_quantity=quantity,
            filled_quantity=quantity - remaining,
            unfilled_quantity=remaining,
            notional=sum((item.price * item.quantity for item in fills), _ZERO),
            levels=len(fills),
            fills=tuple(fills),
            midpoint_used=False,
        )

    def _opportunity_cost_interval(
        self,
        venue: str,
        instrument: str,
        capital: Decimal,
        start_ns: int,
        end_ns: int,
    ) -> Decimal:
        if capital == 0 or end_ns <= start_ns:
            return _ZERO
        schedules = sorted(
            (
                item
                for item in self.config.costs
                if item.venue == venue and item.instrument_id == instrument
            ),
            key=lambda item: item.effective_from_ns,
        )
        covered_ns = 0
        cost = _ZERO
        for schedule in schedules:
            overlap_start = max(start_ns, schedule.effective_from_ns)
            overlap_end = min(
                end_ns,
                schedule.effective_to_ns
                if schedule.effective_to_ns is not None
                else end_ns,
            )
            if overlap_end <= overlap_start:
                continue
            duration_ns = overlap_end - overlap_start
            covered_ns += duration_ns
            cost += schedule.opportunity_cost(
                capital_notional_ns=capital * Decimal(duration_ns)
            )
        if covered_ns != end_ns - start_ns:
            raise ValueError("OPPORTUNITY_COST_SCHEDULE_GAP")
        return cost

    def run(self) -> GhostReport:
        reports: list[OrderReport] = []
        fills: list[_Fill] = []
        prior: dict[str, OrderReport] = {}
        depth_state: dict[tuple[str, Side], tuple[BookLevel, ...]] = {}
        for priority, order in enumerate(self._orders, start=1):
            report, fill = self._execute_order(
                order,
                priority_generation=priority,
                prior=prior,
                depth_state=depth_state,
            )
            reports.append(report)
            prior[order.order_id] = report
            if fill is not None:
                fills.append(fill)

        positions: dict[str, Decimal] = {}
        for fill in sorted(fills, key=lambda item: (item.timestamp_ns, item.order_id)):
            key = f"{fill.venue}|{fill.instrument_id}"
            positions[key] = positions.get(key, _ZERO) + fill.side.sign * fill.quantity
        timeline_times = [
            value
            for report in reports
            for value in report.timeline.values()
            if value is not None
        ]
        final_time = max(
            max((_event_time(event) for event in self.fixture.events), default=0),
            max(timeline_times, default=0),
        )
        forced, unresolved = self._forced_close(
            positions, depth_state=depth_state, after_ns=final_time
        )
        fills.extend(forced)

        positions = {}
        inventory_cash = _ZERO
        hedge_cash = _ZERO
        forced_cash = _ZERO
        fees = _ZERO
        gross_notional = _ZERO
        capital_notional_ns = _ZERO
        opportunity = _ZERO
        last_time: dict[str, int] = {}
        last_price: dict[str, Decimal] = {}
        ordered_fills = sorted(fills, key=lambda item: (item.timestamp_ns, item.order_id))
        for fill in ordered_fills:
            key = f"{fill.venue}|{fill.instrument_id}"
            if key in last_time and fill.timestamp_ns > last_time[key]:
                capital = abs(positions.get(key, _ZERO)) * last_price[key]
                elapsed_ns = fill.timestamp_ns - last_time[key]
                capital_notional_ns += capital * Decimal(elapsed_ns)
                opportunity -= self._opportunity_cost_interval(
                    fill.venue,
                    fill.instrument_id,
                    capital,
                    last_time[key],
                    fill.timestamp_ns,
                )
            positions[key] = positions.get(key, _ZERO) + fill.side.sign * fill.quantity
            last_price[key] = fill.notional / fill.quantity
            cashflow = fill.notional if fill.side is Side.SELL else -fill.notional
            if fill.forced:
                forced_cash += cashflow
            elif fill.role in {"HEDGE", "REPAIR"}:
                hedge_cash += cashflow
            else:
                inventory_cash += cashflow
            fees -= fill.fee
            gross_notional += fill.notional
            last_time[key] = fill.timestamp_ns

        for key, position in positions.items():
            if position == 0 or key not in last_time or last_time[key] >= final_time:
                continue
            venue, instrument = key.split("|", 1)
            capital = abs(position) * last_price[key]
            elapsed_ns = final_time - last_time[key]
            capital_notional_ns += capital * Decimal(elapsed_ns)
            opportunity -= self._opportunity_cost_interval(
                venue,
                instrument,
                capital,
                last_time[key],
                final_time,
            )

        funding = _ZERO
        for event in self._funding:
            venue = cast(str, _text(event.get("venue"), label="funding venue"))
            instrument = cast(
                str, _text(event.get("instrument_id"), label="funding instrument")
            )
            rate = exact_decimal(event.get("rate_bps"), label="funding rate")
            reference = exact_decimal(
                event.get("reference_price"), label="funding reference", positive=True
            )
            funding_time = cast(
                int, _integer(event.get("receive_ns"), label="funding receive time")
            )
            position_at_funding = sum(
                (
                    fill.side.sign * fill.quantity
                    for fill in ordered_fills
                    if fill.venue == venue
                    and fill.instrument_id == instrument
                    and (
                        fill.timestamp_ns < funding_time
                        or (fill.timestamp_ns == funding_time and not fill.forced)
                    )
                ),
                _ZERO,
            )
            funding -= position_at_funding * reference * rate / _BPS

        components = {
            "spread": _ZERO,
            "signal": _ZERO,
            "fees": fees,
            "adverse_selection": _ZERO,
            "inventory": inventory_cash,
            "hedge": hedge_cash,
            "funding": funding,
            "opportunity_cost": opportunity,
            "forced_close": forced_cash,
            "reward": _ZERO,
            "rebate": _ZERO,
        }
        net = sum(components.values(), _ZERO)
        ledger_net = (
            sum(
                (
                    fill.notional if fill.side is Side.SELL else -fill.notional
                    for fill in ordered_fills
                ),
                _ZERO,
            )
            + fees
            + funding
            + opportunity
        )
        pnl = PnlReport(
            **components,
            net=net,
            reconciliation_difference=net - ledger_net,
        )
        final_positions = {
            key: value for key, value in sorted(positions.items()) if value != 0
        }
        exposure_keys = set(final_positions) | set(unresolved)
        exposure_difference = sum(
            (
                abs(final_positions.get(key, _ZERO) - unresolved.get(key, _ZERO))
                for key in exposure_keys
            ),
            _ZERO,
        )
        exposure = ExposureReport(
            positions=final_positions,
            gross_filled_notional=gross_notional,
            capital_immobilized_notional_ns=capital_notional_ns,
            unresolved_closeout=unresolved,
            reconciliation_difference=exposure_difference,
        )
        groups = self._groups(reports)
        no_trade = tuple(
            report.reason
            for report in reports
            if report.status in {"NO_TRADE", "REJECTED", "MISSED"}
        )
        observability_items: list[dict[str, Any]] = [
            {
                "admission_ns": report.timeline["admission_ns"],
                "filled_quantity": report.filled_quantity,
                "kind": "GHOST_ORDER_OUTCOME",
                "order_id": report.order_id,
                "reason": report.reason,
                "status": report.status,
            }
            for report in reports
        ]
        observability_items.append(
            {
                "capital_immobilized_notional_ns": capital_notional_ns,
                "kind": "GHOST_RECONCILIATION",
                "net_pnl": net,
                "pnl_reconciliation_difference": pnl.reconciliation_difference,
                "positions": exposure.positions,
            }
        )
        observability = tuple(observability_items)
        return GhostReport(
            scenario_id=self.fixture.scenario_id,
            fixture_label=self.fixture.fixture_label,
            provenance=ReplayProvenance(
                adapter_id=self.input_adapter_id,
                fixture_sha256=self.fixture.fixture_sha256,
                configuration_sha256=self.config.config_sha256,
                raw_manifest_sha256=self.raw_manifest_sha256,
                raw_root_sha256=self.raw_root_sha256,
                segment_sha256s=self.segment_sha256s,
                synthetic=True,
            ),
            model_version=MODEL_VERSION,
            latency_model_id=self.config.latency.model_id,
            queue_model_id=self.config.queue.model_id,
            grid_version_ids=tuple(item.grid_id for item in self.config.grids),
            cost_schedule_ids=tuple(item.schedule_id for item in self.config.costs),
            orders=tuple(reports),
            groups=groups,
            pnl=pnl,
            mechanism_version_ids=tuple(item.mechanism_id for item in self.config.mechanisms),
            closeout_model_id=self.config.closeout_model_id,
            exposure=exposure,
            no_trade_reasons=no_trade,
            observability=observability,
            limitations=(
                "PNL_IS_REALIZED_GHOST_CASHFLOW_WITH_UNRESOLVED_EXPOSURE_SEPARATE",
                "NO_STRATEGY_OR_EDGE_SELECTED",
                "FIXTURE_MECHANISM_TEST_NOT_ECONOMIC_EVIDENCE",
                "REBATE_AND_REWARD_ZERO_IN_PRIMARY_ECONOMICS",
                "NO_PRIVATE_DATA_NO_ORDER_ROUTE",
                f"CLOSEOUT_MODEL:{self.config.closeout_model_id}",
            ),
        )


def replay_research_manifest(
    root: Path,
    manifest_sha256: str,
    *,
    adapter: GhostEnvelopeAdapter | None = None,
) -> GhostReport:
    reader = ResearchSegmentReader(root, manifest_sha256=manifest_sha256)
    envelopes = reader.replay()
    selected = adapter or CanonicalGhostFixtureEnvelopeAdapter()
    fixture = GhostFixture.from_bytes(selected.fixture_bytes(envelopes))
    return GhostReplay(
        fixture,
        input_adapter_id=selected.adapter_id,
        raw_manifest_sha256=reader.manifest.manifest_sha256,
        raw_root_sha256=reader.manifest.root_sha256,
        segment_sha256s=tuple(item.physical_sha256 for item in reader.manifest.segments),
    ).run()


__all__ = ["GhostFixture", "GhostReplay", "replay_research_manifest"]
