from __future__ import annotations

import math
import os
import sys
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from threading import Event
from types import TracebackType
from typing import BinaryIO, Protocol, Self, cast

from hyperlab.backtest.protocol import canonical_sha256
from hyperlab.paper.engine import PaperCommandResult, PaperEngine
from hyperlab.paper.models import (
    MarketEvent,
    MarketExecutionPolicy,
    PaperProjection,
    PaperRunConfig,
    PaperState,
    PaperStrategyConfig,
    decimal_text,
    deterministic_id,
    parse_utc,
    utc_text,
)
from hyperlab.paper.reducer import replay_projection
from hyperlab.paper.runner import (
    FrozenPaperStrategy,
    PaperRunner,
    PaperRunnerResult,
    PaperStrategyView,
    PortfolioRunner,
)
from hyperlab.paper.store import PaperStore, RunNotFoundError

PUBLIC_MARKET_SCHEMA_VERSION = 1
PUBLIC_MARKET_SOURCE_KIND = "PUBLIC_NORMALIZED"
DEFAULT_PUBLIC_SOURCE_BOOTSTRAP_TIMEOUT_SECONDS = 120.0
PAPER_RUNTIME_LEASE_SCHEMA = "paper-runtime-exclusive-os-lock-v1"
_AUTOMATIC_EMERGENCY_FLATTEN_REASON = "runtime automatic unhedged-timeout emergency flatten"
_AUTOMATIC_PROTECTIVE_FLATTEN_REASON = "runtime automatic protective risk flatten"


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _utc_clock_value(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("paper runtime clock must return a datetime")
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("paper runtime clock must return an explicit UTC timestamp")
    return value.astimezone(UTC)


class PaperRuntimeLease:
    """Exclusive OS-held guard keyed by canonical database path and paper run.

    Runtime admission and stopped-runtime mutators may share this context manager.
    """

    def __init__(self, database: Path, run_id: str) -> None:
        resolved_database = database.resolve(strict=True)
        canonical_database = os.path.normcase(str(resolved_database))
        self.identity = canonical_sha256(
            {
                "database": canonical_database,
                "run_id": run_id,
                "schema": PAPER_RUNTIME_LEASE_SCHEMA,
            }
        )
        self.database = resolved_database
        self.run_id = run_id
        self.path = resolved_database.parent / (
            f".{resolved_database.name}.paper-runtime-{self.identity}.lock"
        )
        stream = self.path.open("a+b")
        try:
            self._lock(stream)
        except OSError:
            stream.close()
            raise PaperAdmissionError("paper runtime already active for the exact database and run") from None
        self._stream: BinaryIO | None = stream

    @staticmethod
    def _lock(stream: BinaryIO) -> None:
        stream.seek(0)
        if stream.read(1) == b"":
            stream.write(b"\0")
            stream.flush()
            os.fsync(stream.fileno())
        stream.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            return
        import fcntl

        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[attr-defined]

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        stream = self._stream
        if stream is None:
            return
        self._stream = None
        stream.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]
        stream.close()


@dataclass(frozen=True, slots=True)
class PublicSourceDescriptor:
    """Frozen identity of a credential-free normalized public market source."""

    source: str
    data_hash: str
    schema_version: int = PUBLIC_MARKET_SCHEMA_VERSION
    source_kind: str = PUBLIC_MARKET_SOURCE_KIND
    public_only: bool = True
    bootstrap_timeout_seconds: float = DEFAULT_PUBLIC_SOURCE_BOOTSTRAP_TIMEOUT_SECONDS

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
        bootstrap_timeout = self.bootstrap_timeout_seconds
        if (
            isinstance(bootstrap_timeout, bool)
            or not isinstance(bootstrap_timeout, (int, float))
            or not math.isfinite(float(bootstrap_timeout))
            or bootstrap_timeout <= 0
        ):
            raise ValueError("bootstrap_timeout_seconds must be a finite positive number")
        object.__setattr__(self, "bootstrap_timeout_seconds", float(bootstrap_timeout))


class NormalizedPublicMarketSource(Protocol):
    """The only input boundary used by the paper runtime.

    Implementations may consume a public collector, an IPC fan-out, or a test
    fixture, but they must expose only validated ``MarketEvent`` objects.  This
    package deliberately knows nothing about venue clients or network transports.
    """

    @property
    def descriptor(self) -> PublicSourceDescriptor: ...

    def poll(self, *, timeout_seconds: float) -> object | None: ...

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


class PaperStartupInterrupted(PaperRuntimeError):
    """A cooperative stop interrupted restoration before session admission."""


class PaperRuntimeStepKind(StrEnum):
    MARKET = "MARKET"
    FUNDING = "FUNDING"
    TIMER = "TIMER"
    DUPLICATE = "DUPLICATE"
    IDLE = "IDLE"


@dataclass(frozen=True, slots=True)
class PaperRuntimeStartup:
    started: PaperCommandResult
    reconciled: PaperCommandResult
    runtime_session_started: PaperCommandResult
    restart_exercise: PaperCommandResult | None = None

    @property
    def projection(self) -> PaperProjection:
        if self.restart_exercise is not None:
            return self.restart_exercise.projection
        return self.runtime_session_started.projection


@dataclass(frozen=True, slots=True)
class PaperRuntimeStep:
    kind: PaperRuntimeStepKind
    projection: PaperProjection
    market_event_ids: tuple[str, ...] = ()
    duplicate_event_ids: tuple[str, ...] = ()
    funding_event_id: str | None = None
    funding_result: PaperCommandResult | None = None
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

    integrity = store.inspect_integrity_readonly(run_id)
    if not integrity.ok:
        codes = ", ".join(issue.code for issue in integrity.issues)
        raise PaperAdmissionError(f"paper replay requires full readonly integrity: {codes}")
    run = store.get_run(run_id)
    config = PaperRunConfig.from_dict(run.config_snapshot)
    if config.config_hash != run.config_hash or config.run_id != run.run_id:
        raise PaperAdmissionError("stored paper config is not bound to its durable run identity")
    run_start_payload = {
        "config_hash": config.config_hash,
        "input_type": "RUN_START",
        "run_id": run.run_id,
    }
    run_start = store.get_input(
        run.run_id,
        deterministic_id("paper_input_run_started", run.run_id),
    )
    if (
        run_start is None
        or run_start.payload != run_start_payload
        or run_start.payload_hash != canonical_sha256(run_start_payload)
    ):
        raise PaperAdmissionError("paper replay requires full readonly integrity: RUN_START_INPUT_MISMATCH")
    events = store.iter_events(run.run_id)
    replayed = replay_projection(
        run_id=run.run_id,
        config_hash=config.config_hash,
        initial_cash=config.initial_cash,
        events=events,
    )
    durable = store.get_projection(run.run_id)
    if replayed.to_dict() != durable.to_dict():
        raise PaperAdmissionError("event replay differs from the durable paper projection")
    if replayed.last_sequence != run.event_sequence or replayed.last_event_hash != run.event_head_hash:
        raise PaperAdmissionError("event replay differs from the durable hash-chain head")
    input_replayed = PaperEngine(store, config).verify_input_replay()
    if input_replayed.to_dict() != replayed.to_dict():
        raise PaperAdmissionError("canonical input replay differs from event replay")
    return PaperReplayVerification(
        run_id=run.run_id,
        config_hash=run.config_hash,
        event_count=run.event_sequence,
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
        strategy: FrozenPaperStrategy | Iterable[FrozenPaperStrategy],
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
        if engine.config.strategies:
            strategy_adapters: tuple[FrozenPaperStrategy, ...]
            if hasattr(strategy, "decide"):
                strategy_adapters = (cast(FrozenPaperStrategy, strategy),)
            else:
                strategy_adapters = tuple(strategy)
            try:
                self._runner: PaperRunner | PortfolioRunner = PortfolioRunner(
                    engine,
                    strategy_adapters,
                )
            except ValueError as error:
                raise PaperAdmissionError(
                    "paper strategy differs from the frozen paper configuration"
                ) from error
            self._strategy = strategy_adapters[0]
        else:
            if not hasattr(strategy, "decide"):
                raise TypeError("legacy paper runtime requires exactly one strategy adapter")
            self._strategy = cast(FrozenPaperStrategy, strategy)
            try:
                self._runner = PaperRunner(engine, self._strategy)
            except ValueError as error:
                raise PaperAdmissionError(
                    "paper strategy differs from the frozen paper configuration"
                ) from error
        self._source = source
        self.config = config or PaperRuntimeConfig()
        self._clock = clock
        self._expected_config_hash = engine.config.config_hash
        self._expected_strategy_name = self._strategy.strategy_name
        self._expected_strategy_hash = self._strategy.strategy_hash
        self._expected_source = descriptor
        self._stop_requested = Event()
        self._stop_notified = False
        self._closed = False
        self._startup: PaperRuntimeStartup | None = None
        self._runtime_lease: PaperRuntimeLease | None = None
        self._runtime_session_id: str | None = None
        self._runtime_session_generation: int | None = None
        self._faulted = False
        self._step_failure_persisted = False
        self._evaluation_in_progress = False
        self._source_started = False
        self._source_closed = False
        self._last_clock: datetime | None = None
        self._next_timer_at: datetime | None = None
        self._pending_item: object | None = None
        self._latest_markets: dict[str, MarketEvent] = {}
        self._bootstrap_connect_health: dict[str, MarketEvent] = {}
        self._source_activation_cutoff: datetime | None = None
        self._bootstrap_started_at: datetime | None = None
        self._bootstrap_deadline_at: datetime | None = None
        self._steady_state_armed = False
        self._strategy_restored = False
        self._strategy_restore_commit_sequence = 0
        self._verify_release_code()
        self._verify_frozen_bindings()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_value, traceback
        if exc_type is not None and not self._step_failure_persisted:
            self._faulted = True
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
            self.config.timer_interval_seconds != self.engine.config.runtime_timer_interval_seconds
            or self.config.source_poll_timeout_seconds
            != self.engine.config.runtime_source_poll_timeout_seconds
        ):
            raise PaperAdmissionError("paper runtime cadence differs from the frozen run configuration")
        if isinstance(self._runner, PortfolioRunner):
            for strategy_config, adapter in self._runner.strategies:
                if (
                    getattr(adapter, "strategy_id", None) != strategy_config.strategy_id
                    or adapter.strategy_name != strategy_config.strategy_name
                    or adapter.strategy_hash != strategy_config.strategy_hash
                    or getattr(adapter, "strategy_config_hash", None) != strategy_config.strategy_config_hash
                ):
                    raise PaperAdmissionError("frozen portfolio strategy identity changed during the run")
        else:
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

    def _verify_release_code(self) -> None:
        try:
            from hyperlab.environment_authorization import (
                current_paper_release_code_sha256,
            )

            current = current_paper_release_code_sha256()
        except Exception as error:
            raise PaperAdmissionError("current paper release code digest could not be verified") from error
        if current != self.engine.config.release_code_sha256:
            raise PaperAdmissionError("paper release code differs from frozen run identity")

    def _verify_runtime_environment(self) -> None:
        try:
            from hyperlab.environment_authorization import (
                current_paper_runtime_environment_sha256,
            )

            current = current_paper_runtime_environment_sha256()
        except Exception as error:
            raise PaperAdmissionError(
                "current paper runtime environment digest could not be verified"
            ) from error
        if current != self.engine.config.runtime_environment_sha256:
            raise PaperAdmissionError("paper runtime environment differs from frozen run identity")

    def _now(self) -> datetime:
        current = _utc_clock_value(self._clock())
        if self._last_clock is not None and current < self._last_clock:
            raise PaperAdmissionError("paper runtime clock moved backwards")
        self._last_clock = current
        return current

    def _persist_source_failure(self, *, as_of: datetime, error: Exception) -> None:
        projection = self.engine.projection()
        # The store's terminal MANUAL_REVIEW latch intentionally rejects every append.
        if projection.state is PaperState.MANUAL_REVIEW:
            return
        error_type = type(error).__name__
        artifact_hash = deterministic_id(
            "paper_public_source_failure",
            self.engine.run_id,
            self._expected_source.source,
            self._expected_source.data_hash,
            self._expected_source.bootstrap_timeout_seconds,
            error_type,
            utc_text(as_of),
        )
        self.engine.pause(
            as_of=as_of,
            reason=f"terminal public source failure: {error_type}",
            operator_artifact_hash=artifact_hash,
            origin="PUBLIC_SOURCE_FAILURE",
        )
        self._step_failure_persisted = True

    def _runtime_failure_identity(
        self,
        *,
        phase: str,
        error_type: str,
        failure_key: str,
    ) -> tuple[str, str]:
        normalized_phase = phase.strip().upper()
        normalized_error_type = error_type.strip()
        if not normalized_phase or any(
            character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for character in normalized_phase
        ):
            raise ValueError("paper runtime failure phase is malformed")
        if not normalized_error_type or any(
            not (character.isalnum() or character in "._") for character in normalized_error_type
        ):
            raise ValueError("paper runtime failure type is malformed")
        artifact_hash = deterministic_id(
            "paper_runtime_failure_v1",
            self.engine.run_id,
            self.engine.config.config_hash,
            normalized_phase,
            normalized_error_type,
            failure_key,
        )
        reason = f"terminal paper runtime failure: {normalized_phase}: {normalized_error_type}"
        return artifact_hash, reason

    def _persist_runtime_failure(
        self,
        *,
        as_of: datetime,
        phase: str,
        error_type: str,
        failure_key: str,
    ) -> PaperCommandResult | None:
        projection = self.engine.projection()
        if projection.state is PaperState.MANUAL_REVIEW:
            return None
        artifact_hash, reason = self._runtime_failure_identity(
            phase=phase,
            error_type=error_type,
            failure_key=failure_key,
        )
        effective_at = as_of
        if projection.last_received_at is not None and effective_at < projection.last_received_at:
            effective_at = projection.last_received_at
        result = self.engine.pause(
            as_of=effective_at,
            reason=reason,
            operator_artifact_hash=artifact_hash,
            origin="PAPER_RUNTIME_FAILURE",
        )
        self._step_failure_persisted = True
        return result

    def _unclosed_session_replacement(
        self,
        projection: PaperProjection,
        *,
        as_of: datetime,
    ) -> str | None:
        if not projection.runtime_session_active:
            return None
        session_id = projection.runtime_session_id
        if session_id is None:
            raise PaperAdmissionError("active runtime session has no durable identity")
        artifact_hash, reason = self._runtime_failure_identity(
            phase="UNCLOSED_RUNTIME_SESSION",
            error_type="UnclosedRuntimeSessionError",
            failure_key=session_id,
        )
        failure_input_id = deterministic_id(
            "paper_runtime_failure_input",
            self.engine.run_id,
            artifact_hash,
        )
        failure = self.engine.store.get_input(self.engine.run_id, failure_input_id)
        if failure is None:
            self._persist_runtime_failure(
                as_of=as_of,
                phase="UNCLOSED_RUNTIME_SESSION",
                error_type="UnclosedRuntimeSessionError",
                failure_key=session_id,
            )
            raise PaperAdmissionError("unclosed prior runtime session requires explicit reviewed recovery")
        if (
            failure.payload.get("input_type") != "PAPER_RUNTIME_FAILURE"
            or failure.payload.get("operator_artifact_hash") != artifact_hash
            or failure.payload.get("reason") != reason
        ):
            raise PaperAdmissionError("durable runtime-session failure evidence differs")
        if projection.state is PaperState.PAUSED:
            raise PaperAdmissionError("unclosed prior runtime session requires explicit reviewed recovery")
        raw_failure_at = failure.payload.get("as_of")
        if not isinstance(raw_failure_at, str):
            raise PaperAdmissionError("durable runtime-session failure time is malformed")
        failure_at = parse_utc(raw_failure_at)
        for resumed in self.engine.store.iter_inputs(
            self.engine.run_id,
            input_type="RESUME_AFTER_REVIEW",
        ):
            if resumed.commit_sequence <= failure.commit_sequence:
                continue
            raw_count = resumed.payload.get("reviewed_critical_incident_count")
            raw_reviewed_at = resumed.payload.get("reviewed_last_critical_incident_at")
            if (
                resumed.payload.get("recovery_mode") != "OFFLINE_UNCLOSED_SESSION"
                or isinstance(raw_count, bool)
                or not isinstance(raw_count, int)
                or raw_count <= 0
                or not isinstance(raw_reviewed_at, str)
            ):
                continue
            if parse_utc(raw_reviewed_at) >= failure_at:
                return session_id
        raise PaperAdmissionError("unclosed prior runtime session was not explicitly reviewed")

    def _start_runtime_session(
        self,
        projection: PaperProjection,
        *,
        as_of: datetime,
    ) -> PaperCommandResult:
        replacement = self._unclosed_session_replacement(projection, as_of=as_of)
        generation = projection.runtime_session_generation + 1
        session_id = deterministic_id(
            "paper_runtime_session_v1",
            self.engine.run_id,
            self.engine.config.config_hash,
            generation,
            utc_text(as_of),
            projection.last_event_hash,
        )
        result = self.engine.start_runtime_session(
            as_of=as_of,
            session_id=session_id,
            generation=generation,
            replaces_unclosed_session_id=replacement,
        )
        self._runtime_session_id = session_id
        self._runtime_session_generation = generation
        return result

    def _shutdown_source(self) -> list[BaseException]:
        errors: list[BaseException] = []
        try:
            self.stop()
        except BaseException as error:
            errors.append(error)
        if not self._source_closed:
            try:
                self._source.close()
            except BaseException as error:
                errors.append(error)
            else:
                self._source_closed = True
        return errors

    def _stop_runtime_session_if_clean(self, *, reason: str) -> None:
        session_id = self._runtime_session_id
        generation = self._runtime_session_generation
        if session_id is None or generation is None:
            return
        if self._faulted or self._evaluation_in_progress:
            return
        projection = self.engine.projection()
        if not projection.runtime_session_active:
            return
        as_of = self._now()
        if projection.last_received_at is not None and as_of < projection.last_received_at:
            as_of = projection.last_received_at
        self.engine.stop_runtime_session(
            as_of=as_of,
            session_id=session_id,
            generation=generation,
            reason=reason,
        )

    def start(self) -> PaperRuntimeStartup:
        if self._closed:
            raise PaperRuntimeError("closed paper runtime cannot be restarted")
        if self._startup is not None:
            return self._startup
        self._verify_runtime_environment()
        self._verify_release_code()
        self._verify_frozen_bindings()
        if self._runtime_lease is not None:
            raise PaperRuntimeError("paper runtime lease state is inconsistent")
        lease = PaperRuntimeLease(self.engine.store.path, self.engine.run_id)
        self._runtime_lease = lease
        try:
            return self._start_with_lease()
        except BaseException as active_error:
            if self._runtime_session_id is not None and not self._step_failure_persisted:
                self._faulted = True
            self._closed = True
            cleanup_errors = self._shutdown_source()
            if cleanup_errors and self._runtime_session_id is not None:
                self._faulted = True
            if not cleanup_errors and self._step_failure_persisted:
                try:
                    self._stop_runtime_session_if_clean(reason="NORMAL_COMPLETION")
                except BaseException as cleanup_error:
                    self._faulted = True
                    cleanup_errors.append(cleanup_error)
            self._runtime_lease = None
            try:
                lease.close()
            except BaseException as cleanup_error:
                cleanup_errors.append(cleanup_error)
            for recorded_cleanup_error in cleanup_errors:
                active_error.add_note(
                    "paper runtime admission cleanup also failed: "
                    f"{type(recorded_cleanup_error).__name__}: {recorded_cleanup_error}"
                )
            raise

    def _check_startup_interrupted(self) -> None:
        if self.stopped:
            raise PaperStartupInterrupted("paper startup interrupted by cooperative stop")

    def _start_with_lease(self) -> PaperRuntimeStartup:
        if self._closed:
            raise PaperRuntimeError("closed paper runtime cannot be restarted")
        if self._startup is not None:
            return self._startup
        self._verify_frozen_bindings()
        as_of = self._now()
        if as_of < self.engine.config.validation_started_at:
            raise PaperAdmissionError("paper runtime clock precedes validation_started_at")
        runtime_restart = False
        try:
            durable_before_start = self.engine.store.get_run(self.engine.run_id)
        except RunNotFoundError:
            durable_before_start = None
        if durable_before_start is not None:
            durable_projection = self.engine.store.get_projection(self.engine.run_id)
            runtime_restart = durable_projection.runtime_session_generation > 0
            if (
                durable_projection.last_received_at is not None
                and as_of < durable_projection.last_received_at
            ):
                raise PaperAdmissionError("paper runtime clock precedes durable paper state")
        try:
            preparation = self.engine.prepare_startup(
                should_stop=self._stop_requested.is_set,
            )
        except InterruptedError as error:
            if self.stopped:
                raise PaperStartupInterrupted("paper startup interrupted by cooperative stop") from error
            raise
        started = preparation.started
        if started.projection.last_received_at is not None and as_of < started.projection.last_received_at:
            raise PaperAdmissionError("paper runtime clock precedes durable paper state")
        self._verify_frozen_bindings()
        if runtime_restart or not self.engine.config.required_instruments:
            self._restore_strategy(self.engine.projection())
            self._strategy_restored = True
        self._check_startup_interrupted()
        try:
            reconciled = self.engine.reconcile_prepared(
                preparation,
                as_of=as_of,
                should_stop=self._stop_requested.is_set,
            )
        except InterruptedError as error:
            if self.stopped:
                raise PaperStartupInterrupted("paper startup interrupted by cooperative stop") from error
            raise
        projection = reconciled.projection
        if not projection.reconciled or projection.state is PaperState.MANUAL_REVIEW:
            raise PaperAdmissionError("paper run did not reconcile cleanly before admission")
        self._source_activation_cutoff = self._load_source_activation_cutoff()
        self._check_startup_interrupted()
        runtime_session_started = self._start_runtime_session(projection, as_of=as_of)
        projection = runtime_session_started.projection
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
        self._verify_runtime_environment()
        source_start = getattr(self._source, "start", None)
        if source_start is not None:
            if not callable(source_start):
                raise PaperAdmissionError("public source start attribute must be callable")
            try:
                source_start()
            except Exception as error:
                self._persist_source_failure(as_of=as_of, error=error)
                raise PaperAdmissionError("public source failed during admission") from error
        self._source_started = True
        self._startup = PaperRuntimeStartup(
            started=started,
            reconciled=reconciled,
            runtime_session_started=runtime_session_started,
            restart_exercise=restart_exercise,
        )
        self._bootstrap_started_at = as_of
        self._bootstrap_deadline_at = as_of + timedelta(
            seconds=self._expected_source.bootstrap_timeout_seconds
        )
        self._steady_state_armed = not bool(self.engine.config.required_instruments)
        self._next_timer_at = (
            as_of + timedelta(seconds=self.config.timer_interval_seconds)
            if self._steady_state_armed
            else None
        )
        return self._startup

    def _load_source_activation_cutoff(self) -> datetime:
        for record in self.engine.store.iter_inputs(
            self.engine.run_id,
            input_type="RECONCILE",
        ):
            raw_as_of = record.payload.get("as_of")
            if not isinstance(raw_as_of, str):
                raise PaperAdmissionError("durable reconciliation cutoff is malformed")
            try:
                cutoff = parse_utc(raw_as_of)
            except (TypeError, ValueError) as error:
                raise PaperAdmissionError("durable reconciliation cutoff cannot be reconstructed") from error
            if cutoff < self.engine.config.validation_started_at:
                raise PaperAdmissionError("durable source activation precedes validation_started_at")
            return cutoff
        raise PaperAdmissionError("paper source activation lacks durable reconciliation")

    def _observe_connection_health(
        self,
        markets: Iterable[MarketEvent],
    ) -> None:
        for market in markets:
            if market.source_event_kind == "connect":
                self._bootstrap_connect_health[market.instrument] = market

    def _durable_markets(
        self,
        *,
        after_commit_sequence: int = 0,
    ) -> Iterable[MarketEvent]:
        for record in self.engine.store.iter_inputs(
            self.engine.run_id,
            input_type="PUBLIC_MARKET_EVENT",
            after_commit_sequence=after_commit_sequence,
        ):
            self._check_startup_interrupted()
            raw_market = record.payload.get("market")
            if not isinstance(raw_market, Mapping):
                raise PaperAdmissionError("durable public market input is malformed")
            try:
                market = MarketEvent.from_dict(cast(Mapping[str, object], raw_market))
            except (KeyError, TypeError, ValueError) as error:
                raise PaperAdmissionError("durable public market input cannot be reconstructed") from error
            self._latest_markets[market.instrument] = market
            self._observe_connection_health((market,))
            yield market

    def _restore_strategy(
        self,
        projection: PaperProjection,
        *,
        incremental: bool = False,
    ) -> None:
        self._check_startup_interrupted()
        if not incremental:
            self._latest_markets.clear()
            self._bootstrap_connect_health.clear()
        bindings: Iterable[tuple[PaperStrategyConfig | None, FrozenPaperStrategy]]
        if isinstance(self._runner, PortfolioRunner):
            bindings = self._runner.strategies
        else:
            bindings = ((None, self._strategy),)
        for strategy_config, adapter in bindings:
            after_commit_sequence = self._strategy_restore_commit_sequence if incremental else 0
            restore = (
                getattr(adapter, "restore_incremental", None)
                if incremental
                else getattr(adapter, "restore", None)
            )
            if incremental and restore is None:
                after_commit_sequence = 0
                restore = getattr(adapter, "restore", None)
            markets = self._durable_markets(
                after_commit_sequence=after_commit_sequence,
            )
            try:
                if restore is None:
                    for _market in markets:
                        pass
                else:
                    if not callable(restore):
                        raise PaperAdmissionError("paper strategy restore attribute must be callable")
                    restore(
                        markets,
                        PaperStrategyView.from_projection(
                            projection,
                            strategy_config,
                        ),
                    )
            except PaperStartupInterrupted:
                raise
            except (KeyError, TypeError, ValueError) as error:
                strategy_label = (
                    strategy_config.strategy_id
                    if strategy_config is not None
                    else self._expected_strategy_name
                )
                raise PaperAdmissionError(
                    f"paper strategy state restoration failed closed: {strategy_label}"
                ) from error
        self._check_startup_interrupted()
        self._strategy_restore_commit_sequence = self.engine.store.get_run(self.engine.run_id).commit_sequence

    def _bootstrap_complete(self, *, as_of: datetime) -> bool:
        if self._steady_state_armed:
            return True
        started_at = self._bootstrap_started_at
        if started_at is None:
            raise PaperRuntimeError("public source bootstrap was not initialized")
        stale_after = timedelta(seconds=self.engine.config.risk.stale_after_seconds)
        shared_lineage: tuple[str, int] | None = None
        for instrument in self.engine.config.required_instruments:
            health = self._bootstrap_connect_health.get(instrument)
            market = self._latest_markets.get(instrument)
            if health is None or market is None:
                return False
            connection_id = health.source_connection_id
            connection_epoch = health.source_connection_epoch
            if (
                health.received_at < started_at
                or health.source_event_kind != "connect"
                or not connection_id
                or isinstance(connection_epoch, bool)
                or not isinstance(connection_epoch, int)
                or connection_epoch <= 0
                or health.tradable
                or health.stale
            ):
                return False
            if (
                market.received_at <= health.received_at
                or market.received_at > as_of
                or as_of - market.received_at > stale_after
                or market.source_event_kind != "bbo"
                or market.source_connection_id != connection_id
                or market.source_connection_epoch != connection_epoch
                or market.stale
                or market.gap
                or not market.tradable
            ):
                return False
            lineage = (connection_id, connection_epoch)
            if shared_lineage is None:
                shared_lineage = lineage
            elif lineage != shared_lineage:
                return False
        return shared_lineage is not None

    def _arm_steady_state(self, *, as_of: datetime) -> None:
        if self._steady_state_armed:
            return
        self._steady_state_armed = True
        self._next_timer_at = as_of + timedelta(seconds=self.config.timer_interval_seconds)

    def _expire_bootstrap(self, *, as_of: datetime) -> None:
        error = TimeoutError("normalized public BBO bootstrap deadline expired")
        self._persist_source_failure(as_of=as_of, error=error)
        raise PaperAdmissionError(
            "public source bootstrap deadline expired before every required BBO was fresh"
        ) from error

    @staticmethod
    def _ordered_markets(
        markets: Mapping[str, MarketEvent],
    ) -> tuple[MarketEvent, ...]:
        ordered = tuple(
            sorted(
                markets.values(),
                key=lambda item: (
                    item.received_at,
                    item.capture_ordinal,
                    item.event_id,
                ),
            )
        )
        if len({(item.received_at, item.capture_ordinal) for item in ordered}) != len(ordered):
            raise PaperAdmissionError("equal-time paper frames require unique capture ordinals")
        return ordered

    def _process_bootstrap_frame(
        self,
        markets: Mapping[str, MarketEvent],
        *,
        processed_at: datetime,
    ) -> PaperRunnerResult:
        results = tuple(
            self.engine.process_market(
                market,
                processed_at=processed_at,
                execution_policy=MarketExecutionPolicy.BOOTSTRAP_OBSERVE_ONLY,
            )
            for market in self._ordered_markets(markets)
        )
        return PaperRunnerResult(results, None)

    def _bootstrap_unhedged_timeout_at(
        self,
        projection: PaperProjection,
    ) -> datetime | None:
        pending_states = {
            PaperState.LEG_1_PENDING,
            PaperState.HEDGE_PENDING,
            PaperState.EXIT_PENDING,
        }
        if projection.strategy_projections:
            deadlines = tuple(
                strategy.state_since
                + timedelta(
                    seconds=self.engine.config.strategy_config(strategy_id).risk.unhedged_timeout_seconds
                )
                for strategy_id, strategy in projection.strategy_projections.items()
                if strategy.positions
                and strategy.state_since is not None
                and (
                    strategy.state in pending_states
                    or (strategy.state is PaperState.PAUSED and strategy.suspended_from in pending_states)
                )
            )
            return min(deadlines, default=None)
        state = projection.state
        if state is PaperState.PAUSED and projection.suspended_from in pending_states:
            state = projection.suspended_from
        if state not in pending_states or not projection.positions or projection.state_since is None:
            return None
        return projection.state_since + timedelta(seconds=self.engine.config.risk.unhedged_timeout_seconds)

    def _bootstrap_safety_timer_step(
        self,
        *,
        as_of: datetime,
    ) -> PaperRuntimeStep:
        before = self._ready_projection()
        timeout_at = self._bootstrap_unhedged_timeout_at(before)
        if timeout_at is None or as_of < timeout_at:
            raise PaperRuntimeError("bootstrap safety timer is not due")
        result = self.engine.process_timer(as_of=as_of)
        if result.projection.state is not PaperState.EMERGENCY_FLATTEN:
            raise PaperAdmissionError("unhedged bootstrap timeout did not preserve EMERGENCY_FLATTEN")
        emergency = self._ensure_automatic_emergency_flatten(
            result.projection,
            as_of=as_of,
        )
        return PaperRuntimeStep(
            kind=PaperRuntimeStepKind.TIMER,
            projection=(emergency.projection if emergency is not None else result.projection),
            timer_result=result,
        )

    def _runtime_emergency_exit_is_active(
        self,
        projection: PaperProjection,
    ) -> bool:
        if projection.strategy_projections:
            decision_ids = tuple(
                strategy.current_exit_decision_id
                for strategy in projection.strategy_projections.values()
                if strategy.current_exit_decision_id is not None
            )
            for strategy_decision_id in decision_ids:
                record = self.engine.store.get_input(
                    self.engine.run_id,
                    strategy_decision_id,
                )
                if record is None:
                    raise PaperAdmissionError(
                        "current strategy emergency exit lacks a durable canonical input"
                    )
                raw_decision = record.payload.get("decision")
                if not isinstance(raw_decision, Mapping):
                    raise PaperAdmissionError("current strategy emergency exit input is malformed")
                orders = tuple(
                    order
                    for order in projection.orders.values()
                    if order.intent.decision_id == strategy_decision_id
                )
                if raw_decision.get("action") == "EXIT" and any(order.status.active for order in orders):
                    return True
            return False
        decision_id = projection.current_exit_decision_id
        if decision_id is None:
            return False
        record = self.engine.store.get_input(self.engine.run_id, decision_id)
        if record is None:
            raise PaperAdmissionError("current emergency exit decision lacks a durable canonical input")
        raw_decision = record.payload.get("decision")
        if not isinstance(raw_decision, Mapping):
            raise PaperAdmissionError("current emergency exit decision input is malformed")
        if raw_decision.get("action") != "EXIT":
            return False
        orders = tuple(
            order for order in projection.orders.values() if order.intent.decision_id == decision_id
        )
        if not orders:
            raise PaperAdmissionError("automatic emergency exit decision lacks durable orders")
        return any(order.status.active for order in orders)

    def _durable_emergency_markets(
        self,
        projection: PaperProjection,
        *,
        as_of: datetime,
    ) -> dict[str, MarketEvent] | None:
        if projection.last_received_at is not None and as_of < projection.last_received_at:
            raise PaperAdmissionError("automatic emergency flatten clock precedes durable state")
        markets: dict[str, MarketEvent] = {}
        stale_after = timedelta(seconds=self.engine.config.risk.stale_after_seconds)
        instruments = (
            {
                instrument
                for strategy in projection.strategy_projections.values()
                for instrument in strategy.positions
            }
            if projection.strategy_projections
            else set(projection.positions)
        )
        for instrument in sorted(instruments):
            market = self._latest_markets.get(instrument)
            if market is None:
                return None
            record = self.engine.store.get_input(self.engine.run_id, market.event_id)
            if (
                record is None
                or record.payload.get("input_type") != "PUBLIC_MARKET_EVENT"
                or record.payload.get("market") != market.to_dict()
            ):
                raise PaperAdmissionError("automatic emergency flatten market is not durably canonical")
            if market.received_at > as_of:
                raise PaperAdmissionError("automatic emergency flatten market is ahead of the runtime clock")
            if as_of - market.received_at > stale_after or market.stale or market.gap or not market.tradable:
                return None
            markets[instrument] = market
        return markets

    def _ensure_automatic_emergency_flatten(
        self,
        projection: PaperProjection,
        *,
        as_of: datetime,
    ) -> PaperCommandResult | None:
        has_attributed_position = (
            any(strategy.positions for strategy in projection.strategy_projections.values())
            if projection.strategy_projections
            else bool(projection.positions)
        )
        if (
            projection.state
            not in {
                PaperState.EMERGENCY_FLATTEN,
                PaperState.REDUCE_ONLY,
            }
            or not has_attributed_position
            or self._runtime_emergency_exit_is_active(projection)
        ):
            return None
        markets = self._durable_emergency_markets(projection, as_of=as_of)
        if markets is None:
            return None
        try:
            return self.engine.emergency_flatten(
                markets,
                decided_at=as_of,
                reason=(
                    _AUTOMATIC_EMERGENCY_FLATTEN_REASON
                    if projection.state is PaperState.EMERGENCY_FLATTEN
                    else _AUTOMATIC_PROTECTIVE_FLATTEN_REASON
                ),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise PaperAdmissionError("automatic emergency flatten failed closed") from error

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
        emergency = self._ensure_automatic_emergency_flatten(
            result.projection,
            as_of=as_of,
        )
        return PaperRuntimeStep(
            kind=PaperRuntimeStepKind.TIMER,
            projection=(emergency.projection if emergency is not None else result.projection),
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
        *,
        processed_at: datetime,
    ) -> tuple[dict[str, MarketEvent], tuple[str, ...]]:
        fresh: dict[str, MarketEvent] = {}
        duplicate_ids: list[str] = []
        for instrument, event in sorted(frame.items()):
            durable = self.engine.store.get_input(self.engine.run_id, event.event_id)
            if durable is not None:
                if (
                    durable.payload.get("input_type") != "PUBLIC_MARKET_EVENT"
                    or durable.payload.get("market") != event.to_dict()
                ):
                    # Route the conflict through the engine/store so the durable
                    # MANUAL_REVIEW latch and critical guard alert are persisted.
                    self.engine.process_market(event, processed_at=processed_at)
                    raise PaperAdmissionError(
                        "public market event identity was redelivered with divergent payload"
                    )
                duplicate_ids.append(event.event_id)
                continue
            previous = projection.last_market_received_at_by_instrument.get(instrument)
            if previous is not None and event.received_at < previous:
                raise PaperAdmissionError(
                    "unseen public market event regresses its instrument source chronology"
                )
            fresh[instrument] = event
        return fresh, tuple(duplicate_ids)

    def _market_execution_policies(
        self,
        frame: Mapping[str, MarketEvent],
        projection: PaperProjection,
    ) -> dict[str, MarketExecutionPolicy]:
        watermarks = tuple(
            value
            for value in (
                projection.last_public_source_received_at,
                self._source_activation_cutoff,
            )
            if value is not None
        )
        watermark = max(watermarks) if watermarks else None
        return {
            instrument: (
                MarketExecutionPolicy.SOURCE_CHRONOLOGY_OBSERVE_ONLY
                if watermark is not None and event.received_at < watermark
                else MarketExecutionPolicy.EXECUTE
            )
            for instrument, event in frame.items()
        }

    @staticmethod
    def _item_received_at(value: object) -> datetime:
        if isinstance(value, Mapping):
            frame = PaperRuntime._normalize_frame(value)
            return min(event.received_at for event in frame.values())
        from hyperlab.paper.public_source import PublicFundingSettlement

        if not isinstance(value, PublicFundingSettlement):
            raise PaperAdmissionError("public source returned an unsupported item")
        return value.received_at

    def _funding_step(self, value: object, observed_at: datetime) -> PaperRuntimeStep:
        from hyperlab.paper.public_source import PublicFundingSettlement

        if not isinstance(value, PublicFundingSettlement):
            raise PaperAdmissionError("public source returned an unsupported item")
        settlement = value
        if settlement.instrument not in self.engine.config.required_instruments:
            raise PaperAdmissionError("public funding instrument is outside the frozen run")
        if settlement.received_at > observed_at:
            raise PaperAdmissionError("public funding received_at is ahead of the runtime clock")

        projection = self._ready_projection()
        input_id = deterministic_id(
            "paper_funding_input",
            self.engine.run_id,
            settlement.event_id,
        )
        durable = self.engine.store.get_input(self.engine.run_id, input_id)
        if durable is not None:
            payload = durable.payload
            durable_source_mark = payload.get("source_mark_price")
            if durable_source_mark is None and payload.get("mark_source") == "PUBLIC_SETTLEMENT_MARK":
                durable_source_mark = payload.get("mark_price")
            durable_signature = {
                "funding_interval_seconds": payload.get("funding_interval_seconds"),
                "funding_rate": payload.get("funding_rate"),
                "instrument": payload.get("instrument"),
                "occurred_at": payload.get("occurred_at"),
                "oracle_price": payload.get("oracle_price"),
                "rate_kind": payload.get("rate_kind"),
                "source_event_id": payload.get("source_event_id"),
                "source_mark_price": durable_source_mark,
            }
            source_signature = {
                "funding_interval_seconds": settlement.funding_interval_seconds,
                "funding_rate": decimal_text(settlement.funding_rate),
                "instrument": settlement.instrument,
                "occurred_at": utc_text(settlement.funding_time),
                "oracle_price": (
                    decimal_text(settlement.oracle_price) if settlement.oracle_price is not None else None
                ),
                "rate_kind": settlement.rate_kind,
                "source_event_id": settlement.event_id,
                "source_mark_price": (
                    decimal_text(settlement.mark_price) if settlement.mark_price is not None else None
                ),
            }
            if durable_signature == source_signature:
                return PaperRuntimeStep(
                    kind=PaperRuntimeStepKind.DUPLICATE,
                    projection=projection,
                    duplicate_event_ids=(settlement.event_id,),
                    funding_event_id=settlement.event_id,
                )

        if observed_at - settlement.received_at > timedelta(
            seconds=self.engine.config.risk.stale_after_seconds
        ):
            self.engine.process_timer(as_of=observed_at)
            raise PaperAdmissionError(
                "public funding settlement is older than the frozen stale_after_seconds limit"
            )
        cutoff = self._source_activation_cutoff
        if cutoff is None:
            raise PaperAdmissionError("paper source activation cutoff is not initialized")
        pre_activation = settlement.funding_time < cutoff
        historical = self.engine.store.get_projection_before_received_at(
            self.engine.run_id,
            before=settlement.funding_time,
        )
        quantity = Decimal(0)
        if not pre_activation and historical is not None:
            quantity = historical.positions.get(settlement.instrument, Decimal(0))

        mark = settlement.mark_price
        mark_source = "PUBLIC_SETTLEMENT_MARK"
        if mark is None:
            mark = settlement.oracle_price
            mark_source = "PUBLIC_SETTLEMENT_ORACLE"
        if mark is None and not pre_activation and historical is not None:
            historical_mark = historical.public_bbo_mids.get(settlement.instrument)
            historical_mark_at = historical.public_bbo_received_at_by_instrument.get(settlement.instrument)
            if (
                historical_mark is not None
                and historical_mark_at is not None
                and historical_mark_at <= settlement.funding_time
                and settlement.funding_time - historical_mark_at
                <= timedelta(seconds=self.engine.config.risk.stale_after_seconds)
            ):
                mark = historical_mark
                mark_source = "DURABLE_PUBLIC_BBO_MID"
        if mark is None:
            mark_source = "FLAT_NO_MARK"
        if mark is None and quantity != 0:
            raise PaperAdmissionError("non-flat funding settlement lacks a public or durable historical mark")
        if pre_activation or quantity == 0:
            amount = Decimal(0)
        else:
            assert mark is not None
            amount = -(quantity * mark * settlement.funding_rate)
        result = self.engine.post_funding(
            instrument=settlement.instrument,
            amount=amount,
            occurred_at=settlement.funding_time,
            source_event_id=settlement.event_id,
            funding_rate=settlement.funding_rate,
            funding_interval_seconds=settlement.funding_interval_seconds,
            rate_kind=settlement.rate_kind,
            mark_price=mark,
            source_mark_price=settlement.mark_price,
            oracle_price=settlement.oracle_price,
            position_quantity=quantity,
            mark_source=mark_source,
            source_observation_id=settlement.source_observation_id,
            received_at=settlement.received_at,
            processed_at=observed_at,
            applicability=("PRE_ACTIVATION_IGNORED" if pre_activation else "APPLIED"),
            source_activation_cutoff=cutoff,
        )
        protective = self._ensure_automatic_emergency_flatten(
            result.projection,
            as_of=observed_at,
        )
        final_projection = protective.projection if protective is not None else result.projection
        return PaperRuntimeStep(
            kind=PaperRuntimeStepKind.FUNDING,
            projection=final_projection,
            funding_event_id=settlement.event_id,
            funding_result=result,
        )

    def run_once(self) -> PaperRuntimeStep:
        try:
            return self._run_once()
        except BaseException:
            if self._evaluation_in_progress and not self._step_failure_persisted:
                self._faulted = True
            raise

    def _run_once(self) -> PaperRuntimeStep:
        self._step_failure_persisted = False
        projection = self._ready_projection()
        if self.stopped:
            return PaperRuntimeStep(PaperRuntimeStepKind.IDLE, projection)
        now = self._now()
        if projection.last_received_at is not None and now < projection.last_received_at:
            raise PaperAdmissionError("paper runtime clock precedes durable paper state")

        deadline = self._bootstrap_deadline_at
        bootstrap_safety_at: datetime | None = None
        if not self._steady_state_armed:
            if deadline is None:
                raise PaperRuntimeError("public source bootstrap deadline was not initialized")
            bootstrap_safety_at = self._bootstrap_unhedged_timeout_at(projection)
            if bootstrap_safety_at is not None and now >= bootstrap_safety_at:
                return self._bootstrap_safety_timer_step(as_of=now)
            if now >= deadline:
                self._expire_bootstrap(as_of=now)
        elif self._next_timer_at is None:
            raise PaperRuntimeError("paper runtime timer was not initialized")

        next_timer_at = self._next_timer_at
        if (
            self._steady_state_armed
            and next_timer_at is not None
            and self._pending_item is None
            and now >= next_timer_at
        ):
            return self._timer_step(now)

        item = self._pending_item
        if item is None:
            if self._steady_state_armed:
                assert next_timer_at is not None
                remaining = max((next_timer_at - now).total_seconds(), 0.0)
            else:
                assert deadline is not None
                wake_at = min(
                    deadline,
                    bootstrap_safety_at or deadline,
                )
                remaining = max((wake_at - now).total_seconds(), 0.0)
            timeout = min(self.config.source_poll_timeout_seconds, remaining)
            try:
                value = self._source.poll(timeout_seconds=timeout)
            except Exception as error:
                self._persist_source_failure(as_of=now, error=error)
                raise PaperAdmissionError("public source failed closed") from error
            self._verify_frozen_bindings()
            observed_at = self._now()
            if not self._steady_state_armed and observed_at > cast(datetime, deadline):
                self._expire_bootstrap(as_of=observed_at)
            if value is None:
                if not self._steady_state_armed:
                    current = self.engine.projection()
                    safety_at = self._bootstrap_unhedged_timeout_at(current)
                    if safety_at is not None and observed_at >= safety_at:
                        return self._bootstrap_safety_timer_step(as_of=observed_at)
                    if observed_at >= cast(datetime, deadline):
                        self._expire_bootstrap(as_of=observed_at)
                    return PaperRuntimeStep(
                        PaperRuntimeStepKind.IDLE,
                        current,
                    )
                assert next_timer_at is not None
                if observed_at >= next_timer_at:
                    return self._timer_step(observed_at)
                return PaperRuntimeStep(
                    PaperRuntimeStepKind.IDLE,
                    self.engine.projection(),
                )
            item = value
        else:
            self._pending_item = None
            observed_at = now

        try:
            item_received_at = self._item_received_at(item)
        except PaperAdmissionError as error:
            self._persist_source_failure(as_of=observed_at, error=error)
            raise
        if not self._steady_state_armed:
            current = self.engine.projection()
            safety_at = self._bootstrap_unhedged_timeout_at(current)
            if safety_at is not None and safety_at <= observed_at and safety_at <= item_received_at:
                self._pending_item = item
                timer_at = safety_at
                if current.last_received_at is not None:
                    timer_at = max(timer_at, current.last_received_at)
                return self._bootstrap_safety_timer_step(as_of=timer_at)
        next_timer_at = self._next_timer_at
        if (
            self._steady_state_armed
            and next_timer_at is not None
            and next_timer_at <= observed_at
            and next_timer_at <= item_received_at
        ):
            self._pending_item = item
            timer_at = next_timer_at
            projection = self.engine.projection()
            if projection.last_received_at is not None:
                timer_at = max(timer_at, projection.last_received_at)
            return self._timer_step(timer_at)

        if not isinstance(item, Mapping):
            if not self._steady_state_armed and deadline is not None and observed_at >= deadline:
                self._expire_bootstrap(as_of=observed_at)
            try:
                return self._funding_step(item, observed_at)
            except PaperAdmissionError as error:
                self._persist_source_failure(as_of=observed_at, error=error)
                raise
        try:
            frame = self._normalize_frame(item)
        except PaperAdmissionError as error:
            self._persist_source_failure(as_of=observed_at, error=error)
            raise
        if any(event.received_at > observed_at for event in frame.values()):
            future_error = PaperAdmissionError(
                "public market event received_at is ahead of the runtime clock"
            )
            self._persist_source_failure(as_of=observed_at, error=future_error)
            raise future_error
        stale_after = timedelta(seconds=self.engine.config.risk.stale_after_seconds)
        too_old = any(observed_at - event.received_at > stale_after for event in frame.values())
        if self._steady_state_armed and too_old:
            # Persist the watchdog incident and protective state before
            # terminating this runtime step.  The rejected frame itself remains
            # outside the canonical market inbox.
            self.engine.process_timer(as_of=observed_at)
            stale_error = PaperAdmissionError(
                "public market event is older than the frozen stale_after_seconds limit"
            )
            self._persist_source_failure(as_of=observed_at, error=stale_error)
            raise stale_error
        projection = self._ready_projection()
        try:
            fresh, duplicate_ids = self._filter_durable_duplicates(
                frame,
                projection,
                processed_at=observed_at,
            )
        except PaperAdmissionError as error:
            self._persist_source_failure(as_of=observed_at, error=error)
            raise
        if not fresh:
            if not self._steady_state_armed and deadline is not None and observed_at >= deadline:
                self._expire_bootstrap(as_of=observed_at)
            return PaperRuntimeStep(
                kind=PaperRuntimeStepKind.DUPLICATE,
                projection=projection,
                duplicate_event_ids=duplicate_ids,
            )

        decision_markets = dict(self._latest_markets)
        decision_markets.update(fresh)
        evaluation_key = deterministic_id(
            "paper_runtime_market_evaluation_v1",
            self.engine.run_id,
            utc_text(observed_at),
            tuple(sorted(event.event_id for event in fresh.values())),
        )
        self._evaluation_in_progress = True
        try:
            if self._steady_state_armed:
                if not self._strategy_restored:
                    raise PaperRuntimeError("paper strategy was not restored before steady-state decisions")
                policies = self._market_execution_policies(fresh, projection)
                result = self._runner.process_frame(
                    fresh,
                    decision_markets=decision_markets,
                    processed_at=observed_at,
                    execution_policies=policies,
                )
            else:
                result = self._process_bootstrap_frame(
                    fresh,
                    processed_at=observed_at,
                )
            # Cache/health state changes only after every fresh market input
            # committed successfully.
            self._latest_markets.update(fresh)
            self._observe_connection_health(fresh.values())
            if not self._steady_state_armed:
                if self._bootstrap_complete(as_of=observed_at):
                    self._restore_strategy(result.projection, incremental=True)
                    self._strategy_restored = True
                    self._arm_steady_state(as_of=observed_at)
                elif deadline is not None and observed_at >= deadline:
                    self._expire_bootstrap(as_of=observed_at)

            emergency = self._ensure_automatic_emergency_flatten(
                result.projection,
                as_of=observed_at,
            )
            self._verify_frozen_bindings()
        except Exception as error:
            try:
                self._persist_runtime_failure(
                    as_of=observed_at,
                    phase="MARKET_EVALUATION",
                    error_type=type(error).__name__,
                    failure_key=evaluation_key,
                )
            except Exception as persistence_error:
                error.add_note(
                    "paper runtime failure persistence also failed: "
                    f"{type(persistence_error).__name__}: {persistence_error}"
                )
            if self._step_failure_persisted:
                self._evaluation_in_progress = False
            raise
        self._evaluation_in_progress = False
        return PaperRuntimeStep(
            kind=PaperRuntimeStepKind.MARKET,
            projection=(emergency.projection if emergency is not None else result.projection),
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
        return self.engine.projection()

    def stop(self) -> None:
        self._stop_requested.set()
        if self._stop_notified:
            return
        self._stop_notified = True
        self._source.stop()

    def close(self) -> None:
        if self._closed:
            return
        cooperative_requested = self.stopped
        self._closed = True
        errors = self._shutdown_source()
        if errors and self._runtime_session_id is not None:
            self._faulted = True
        if not errors:
            stop_reason = "COOPERATIVE_STOP" if cooperative_requested else "NORMAL_COMPLETION"
            try:
                self._stop_runtime_session_if_clean(reason=stop_reason)
            except BaseException as error:
                self._faulted = True
                errors.append(error)
        lease = self._runtime_lease
        self._runtime_lease = None
        if lease is not None:
            try:
                lease.close()
            except BaseException as error:
                errors.append(error)
        if errors:
            primary = errors[0]
            for secondary in errors[1:]:
                primary.add_note(
                    f"paper runtime cleanup also failed: {type(secondary).__name__}: {secondary}"
                )
            raise primary


__all__ = [
    "PAPER_RUNTIME_LEASE_SCHEMA",
    "PUBLIC_MARKET_SCHEMA_VERSION",
    "PUBLIC_MARKET_SOURCE_KIND",
    "NormalizedPublicMarketSource",
    "PaperAdmissionError",
    "PaperReplayVerification",
    "PaperRuntime",
    "PaperRuntimeConfig",
    "PaperRuntimeError",
    "PaperRuntimeLease",
    "PaperRuntimeStartup",
    "PaperRuntimeStep",
    "PaperRuntimeStepKind",
    "PaperStartupInterrupted",
    "PublicSourceDescriptor",
    "replay_paper_run",
]
