from __future__ import annotations

import math
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from threading import Event
from types import TracebackType
from typing import Protocol, Self

from hyperlab.backtest.protocol import canonical_sha256
from hyperlab.paper.engine import PaperCommandResult, PaperEngine
from hyperlab.paper.models import (
    MarketEvent,
    PaperProjection,
    PaperRunConfig,
    PaperState,
    deterministic_id,
)
from hyperlab.paper.reducer import replay_projection
from hyperlab.paper.runner import FrozenPaperStrategy, PaperRunner, PaperRunnerResult
from hyperlab.paper.store import PaperStore, RunNotFoundError

PUBLIC_MARKET_SCHEMA_VERSION = 1
PUBLIC_MARKET_SOURCE_KIND = "PUBLIC_NORMALIZED"


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _utc_clock_value(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("paper runtime clock must return a datetime")
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("paper runtime clock must return an explicit UTC timestamp")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class PublicSourceDescriptor:
    """Frozen identity of a credential-free normalized public market source."""

    source: str
    data_hash: str
    schema_version: int = PUBLIC_MARKET_SCHEMA_VERSION
    source_kind: str = PUBLIC_MARKET_SOURCE_KIND
    public_only: bool = True

    def __post_init__(self) -> None:
        normalized_source = self.source.strip()
        if not normalized_source:
            raise ValueError("public paper source cannot be empty")
        object.__setattr__(self, "source", normalized_source)
        normalized_hash = self.data_hash.strip().lower()
        if len(normalized_hash) != 64 or any(
            character not in "0123456789abcdef" for character in normalized_hash
        ):
            raise ValueError("public paper source data_hash must be a SHA-256 digest")
        object.__setattr__(self, "data_hash", normalized_hash)
        if self.schema_version != PUBLIC_MARKET_SCHEMA_VERSION:
            raise ValueError("unsupported normalized public market schema")
        if self.source_kind != PUBLIC_MARKET_SOURCE_KIND or self.public_only is not True:
            raise ValueError("paper runtime accepts normalized public-only sources")


class NormalizedPublicMarketSource(Protocol):
    """The only input boundary used by the paper runtime.

    Implementations may consume a public collector, an IPC fan-out, or a test
    fixture, but they must expose only validated ``MarketEvent`` objects.  This
    package deliberately knows nothing about venue clients or network transports.
    """

    @property
    def descriptor(self) -> PublicSourceDescriptor: ...

    def poll(self, *, timeout_seconds: float) -> Mapping[str, MarketEvent] | None: ...

    def stop(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PaperRuntimeConfig:
    timer_interval_seconds: float = 1.0
    source_poll_timeout_seconds: float = 0.25
    mode: str = "readonly"

    def __post_init__(self) -> None:
        for name in ("timer_interval_seconds", "source_poll_timeout_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError(f"{name} must be a finite positive number")
            if not math.isfinite(float(value)) or value <= 0:
                raise ValueError(f"{name} must be a finite positive number")
            object.__setattr__(self, name, float(value))
        normalized_mode = self.mode.strip().lower()
        if normalized_mode not in {"readonly", "research"}:
            raise ValueError("paper runtime only permits readonly/research HYPERLAB_MODE")
        object.__setattr__(self, "mode", normalized_mode)


class PaperRuntimeError(RuntimeError):
    """Base class for fail-closed paper runtime errors."""


class PaperAdmissionError(PaperRuntimeError):
    """A frozen binding, clock, source, or reconciliation gate failed."""


class PaperRuntimeStepKind(StrEnum):
    MARKET = "MARKET"
    TIMER = "TIMER"
    DUPLICATE = "DUPLICATE"
    IDLE = "IDLE"


@dataclass(frozen=True, slots=True)
class PaperRuntimeStartup:
    started: PaperCommandResult
    reconciled: PaperCommandResult
    restart_exercise: PaperCommandResult | None = None

    @property
    def projection(self) -> PaperProjection:
        if self.restart_exercise is not None:
            return self.restart_exercise.projection
        return self.reconciled.projection


@dataclass(frozen=True, slots=True)
class PaperRuntimeStep:
    kind: PaperRuntimeStepKind
    projection: PaperProjection
    market_event_ids: tuple[str, ...] = ()
    duplicate_event_ids: tuple[str, ...] = ()
    timer_result: PaperCommandResult | None = None
    runner_result: PaperRunnerResult | None = None


@dataclass(frozen=True, slots=True)
class PaperReplayVerification:
    run_id: str
    config_hash: str
    event_count: int
    event_head_hash: str
    projection_hash: str

    def to_dict(self) -> dict[str, object]:
        return {
            "config_hash": self.config_hash,
            "event_count": self.event_count,
            "event_head_hash": self.event_head_hash,
            "mode": "PAPER_ONLY",
            "orders_enabled": False,
            "projection_hash": self.projection_hash,
            "run_id": self.run_id,
            "status": "REPLAY_EXACT",
        }


def replay_paper_run(store: PaperStore, run_id: str) -> PaperReplayVerification:
    """Re-run the canonical inbox in an isolated store; source stays read-only."""

    run = store.get_run(run_id)
    config = PaperRunConfig.from_dict(run.config_snapshot)
    if config.config_hash != run.config_hash or config.run_id != run.run_id:
        raise PaperAdmissionError("stored paper config is not bound to its durable run identity")
    events = store.get_events(run.run_id)
    replayed = replay_projection(
        run_id=run.run_id,
        config_hash=config.config_hash,
        initial_cash=config.initial_cash,
        events=events,
    )
    durable = store.get_projection(run.run_id)
    if replayed.to_dict() != durable.to_dict():
        raise PaperAdmissionError("event replay differs from the durable paper projection")
    if len(events) != run.event_sequence or replayed.last_event_hash != run.event_head_hash:
        raise PaperAdmissionError("event replay differs from the durable hash-chain head")
    input_replayed = PaperEngine(store, config).verify_input_replay()
    if input_replayed.to_dict() != replayed.to_dict():
        raise PaperAdmissionError("canonical input replay differs from event replay")
    return PaperReplayVerification(
        run_id=run.run_id,
        config_hash=run.config_hash,
        event_count=len(events),
        event_head_hash=run.event_head_hash,
        projection_hash=replayed.canonical_hash,
    )


class PaperRuntime:
    """Restart-safe supervisor around one frozen paper engine and strategy.

    ``start`` performs durable restore and exact reconciliation before ``poll``
    may be called.  ``run_once`` is deterministic under an injected clock and
    source; ``run_forever`` adds cooperative shutdown and resource cleanup.
    """

    def __init__(
        self,
        engine: PaperEngine,
        strategy: FrozenPaperStrategy,
        source: NormalizedPublicMarketSource,
        *,
        config: PaperRuntimeConfig | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not isinstance(engine, PaperEngine):
            raise TypeError("engine must be a PaperEngine")
        descriptor = source.descriptor
        if not isinstance(descriptor, PublicSourceDescriptor):
            raise TypeError("source descriptor must be a PublicSourceDescriptor")
        self.engine = engine
        self._strategy = strategy
        self._source = source
        self.config = config or PaperRuntimeConfig()
        self._clock = clock
        self._expected_config_hash = engine.config.config_hash
        self._expected_strategy_name = strategy.strategy_name
        self._expected_strategy_hash = strategy.strategy_hash
        self._expected_source = descriptor
        self._stop_requested = Event()
        self._stop_notified = False
        self._closed = False
        self._startup: PaperRuntimeStartup | None = None
        self._last_clock: datetime | None = None
        self._next_timer_at: datetime | None = None
        self._pending_frame: dict[str, MarketEvent] | None = None
        self._verify_frozen_bindings()
        self._runner = PaperRunner(engine, strategy)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.close()

    @property
    def stopped(self) -> bool:
        return self._stop_requested.is_set()

    @property
    def started(self) -> bool:
        return self._startup is not None

    @property
    def orders_enabled(self) -> bool:
        return False

    def _verify_frozen_bindings(self) -> None:
        if self.engine.config.config_hash != self._expected_config_hash:
            raise PaperAdmissionError("paper run configuration changed after runtime construction")
        if (
            self._strategy.strategy_name != self._expected_strategy_name
            or self._strategy.strategy_hash != self._expected_strategy_hash
        ):
            raise PaperAdmissionError("frozen paper strategy identity changed during the run")
        if (
            self.engine.config.strategy_name != self._expected_strategy_name
            or self.engine.config.strategy_hash != self._expected_strategy_hash
        ):
            raise PaperAdmissionError("paper strategy differs from the frozen paper configuration")
        descriptor = self._source.descriptor
        if not isinstance(descriptor, PublicSourceDescriptor) or descriptor != self._expected_source:
            raise PaperAdmissionError("normalized public source identity changed during the run")
        if descriptor.source != self.engine.config.data_source:
            raise PaperAdmissionError("public source differs from the frozen paper data_source")
        if descriptor.data_hash != self.engine.config.data_hash:
            raise PaperAdmissionError("public source data hash differs from the frozen paper config")

    def _now(self) -> datetime:
        current = _utc_clock_value(self._clock())
        if self._last_clock is not None and current < self._last_clock:
            raise PaperAdmissionError("paper runtime clock moved backwards")
        self._last_clock = current
        return current

    def start(self) -> PaperRuntimeStartup:
        if self._closed:
            raise PaperRuntimeError("closed paper runtime cannot be restarted")
        if self._startup is not None:
            return self._startup
        self._verify_frozen_bindings()
        as_of = self._now()
        if as_of < self.engine.config.validation_started_at:
            raise PaperAdmissionError("paper runtime clock precedes validation_started_at")
        try:
            durable_before_start = self.engine.store.get_run(self.engine.run_id)
        except RunNotFoundError:
            durable_before_start = None
        if durable_before_start is not None:
            durable_projection = self.engine.store.get_projection(self.engine.run_id)
            if (
                durable_projection.last_received_at is not None
                and as_of < durable_projection.last_received_at
            ):
                raise PaperAdmissionError("paper runtime clock precedes durable paper state")
        started = self.engine.start()
        if (
            started.projection.last_received_at is not None
            and as_of < started.projection.last_received_at
        ):
            raise PaperAdmissionError("paper runtime clock precedes durable paper state")
        reconciled = self.engine.reconcile(as_of=as_of)
        projection = reconciled.projection
        if not projection.reconciled or projection.state is PaperState.MANUAL_REVIEW:
            raise PaperAdmissionError("paper run did not reconcile cleanly before admission")
        self._verify_frozen_bindings()
        restart_exercise: PaperCommandResult | None = None
        if durable_before_start is not None:
            artifact_hash = deterministic_id(
                "paper_runtime_restart_artifact",
                self.engine.run_id,
                durable_before_start.config_hash,
                durable_before_start.event_head_hash,
                durable_before_start.commit_head_hash,
            )
            restart_exercise = self.engine.record_resilience_exercise(
                exercise="RESTART",
                artifact_hash=artifact_hash,
                exercised_at=as_of,
            )
        self._startup = PaperRuntimeStartup(
            started=started,
            reconciled=reconciled,
            restart_exercise=restart_exercise,
        )
        self._next_timer_at = as_of + timedelta(seconds=self.config.timer_interval_seconds)
        return self._startup

    def _ready_projection(self) -> PaperProjection:
        self.start()
        self._verify_frozen_bindings()
        projection = self.engine.projection()
        if not projection.reconciled or projection.state is PaperState.MANUAL_REVIEW:
            raise PaperAdmissionError("paper admission is closed until reconciliation succeeds")
        return projection

    def _timer_step(self, as_of: datetime) -> PaperRuntimeStep:
        projection = self._ready_projection()
        if projection.last_received_at is not None and as_of < projection.last_received_at:
            raise PaperAdmissionError("paper timer would precede durable paper state")
        result = self.engine.process_timer(as_of=as_of)
        interval = timedelta(seconds=self.config.timer_interval_seconds)
        if self._next_timer_at is not None and as_of == self._next_timer_at:
            self._next_timer_at += interval
        else:
            self._next_timer_at = as_of + interval
        return PaperRuntimeStep(
            kind=PaperRuntimeStepKind.TIMER,
            projection=result.projection,
            timer_result=result,
        )

    @staticmethod
    def _normalize_frame(value: object) -> dict[str, MarketEvent]:
        if not isinstance(value, Mapping) or not value:
            raise PaperAdmissionError("public source must return a non-empty normalized market frame")
        frame: dict[str, MarketEvent] = {}
        for key, event in value.items():
            if not isinstance(key, str) or not isinstance(event, MarketEvent):
                raise PaperAdmissionError("public source returned a non-MarketEvent value")
            if key != event.instrument or key in frame:
                raise PaperAdmissionError("public source frame is not keyed by canonical instrument")
            frame[key] = event
        return frame

    def _filter_durable_duplicates(
        self,
        frame: Mapping[str, MarketEvent],
        projection: PaperProjection,
    ) -> tuple[dict[str, MarketEvent], tuple[str, ...]]:
        fresh: dict[str, MarketEvent] = {}
        duplicate_ids: list[str] = []
        for instrument, event in sorted(frame.items()):
            durable = self.engine.store.get_input(self.engine.run_id, event.event_id)
            if durable is not None:
                candidate_payload = {
                    "input_type": "PUBLIC_MARKET_EVENT",
                    "market": event.to_dict(),
                }
                if durable.payload_hash != canonical_sha256(candidate_payload):
                    # Route the conflict through the engine/store so the durable
                    # MANUAL_REVIEW latch and critical guard alert are persisted.
                    self.engine.process_market(event)
                    raise PaperAdmissionError(
                        "public market event identity was redelivered with divergent payload"
                    )
                duplicate_ids.append(event.event_id)
                continue
            if (
                projection.last_received_at is not None
                and event.received_at < projection.last_received_at
            ):
                raise PaperAdmissionError("unseen public market event precedes durable paper state")
            fresh[instrument] = event
        return fresh, tuple(duplicate_ids)

    def run_once(self) -> PaperRuntimeStep:
        projection = self._ready_projection()
        if self.stopped:
            return PaperRuntimeStep(PaperRuntimeStepKind.IDLE, projection)
        now = self._now()
        if projection.last_received_at is not None and now < projection.last_received_at:
            raise PaperAdmissionError("paper runtime clock precedes durable paper state")
        if self._next_timer_at is None:
            raise PaperRuntimeError("paper runtime timer was not initialized")

        if self._pending_frame is None and now >= self._next_timer_at:
            return self._timer_step(now)

        frame = self._pending_frame
        if frame is None:
            timeout = min(
                self.config.source_poll_timeout_seconds,
                max((self._next_timer_at - now).total_seconds(), 0.0),
            )
            value = self._source.poll(timeout_seconds=timeout)
            self._verify_frozen_bindings()
            observed_at = self._now()
            if value is None:
                if observed_at >= self._next_timer_at:
                    return self._timer_step(observed_at)
                return PaperRuntimeStep(PaperRuntimeStepKind.IDLE, self.engine.projection())
            frame = self._normalize_frame(value)
        else:
            self._pending_frame = None
            observed_at = now

        if any(event.received_at > observed_at for event in frame.values()):
            raise PaperAdmissionError("public market event received_at is ahead of the runtime clock")
        if any(
            observed_at - event.received_at
            > timedelta(seconds=self.engine.config.risk.stale_after_seconds)
            for event in frame.values()
        ):
            # Persist the watchdog incident and protective state before
            # terminating this runtime step.  The rejected frame itself remains
            # outside the canonical market inbox.
            self.engine.process_timer(as_of=observed_at)
            raise PaperAdmissionError(
                "public market event is older than the frozen stale_after_seconds limit"
            )
        projection = self._ready_projection()
        fresh, duplicate_ids = self._filter_durable_duplicates(frame, projection)
        if not fresh:
            return PaperRuntimeStep(
                kind=PaperRuntimeStepKind.DUPLICATE,
                projection=projection,
                duplicate_event_ids=duplicate_ids,
            )

        earliest_received = min(event.received_at for event in fresh.values())
        if self._next_timer_at <= observed_at and self._next_timer_at <= earliest_received:
            self._pending_frame = fresh
            timer_at = self._next_timer_at
            projection = self.engine.projection()
            if projection.last_received_at is not None:
                timer_at = max(timer_at, projection.last_received_at)
            return self._timer_step(timer_at)

        result = self._runner.process_frame(fresh)
        self._verify_frozen_bindings()
        return PaperRuntimeStep(
            kind=PaperRuntimeStepKind.MARKET,
            projection=result.projection,
            market_event_ids=tuple(event.event_id for event in fresh.values()),
            duplicate_event_ids=duplicate_ids,
            runner_result=result,
        )

    def run_forever(self, *, max_steps: int | None = None) -> PaperProjection:
        if max_steps is not None and (
            isinstance(max_steps, bool) or not isinstance(max_steps, int) or max_steps <= 0
        ):
            raise ValueError("max_steps must be a positive integer or None")
        try:
            self.start()
            steps = 0
            while not self.stopped and (max_steps is None or steps < max_steps):
                self.run_once()
                steps += 1
            return self.engine.projection()
        finally:
            active_error = sys.exception()
            try:
                self.close()
            except BaseException as cleanup_error:
                if active_error is None:
                    raise
                active_error.add_note(
                    f"paper runtime cleanup also failed: {type(cleanup_error).__name__}: {cleanup_error}"
                )

    def stop(self) -> None:
        self._stop_requested.set()
        if self._stop_notified:
            return
        self._stop_notified = True
        self._source.stop()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        errors: list[BaseException] = []
        try:
            self.stop()
        except BaseException as error:
            errors.append(error)
        try:
            self._source.close()
        except BaseException as error:
            errors.append(error)
        if errors:
            primary = errors[0]
            for secondary in errors[1:]:
                primary.add_note(
                    f"source close also failed: {type(secondary).__name__}: {secondary}"
                )
            raise primary


__all__ = [
    "PUBLIC_MARKET_SCHEMA_VERSION",
    "PUBLIC_MARKET_SOURCE_KIND",
    "NormalizedPublicMarketSource",
    "PaperAdmissionError",
    "PaperReplayVerification",
    "PaperRuntime",
    "PaperRuntimeConfig",
    "PaperRuntimeError",
    "PaperRuntimeStartup",
    "PaperRuntimeStep",
    "PaperRuntimeStepKind",
    "PublicSourceDescriptor",
    "replay_paper_run",
]
