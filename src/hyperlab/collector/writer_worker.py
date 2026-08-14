from __future__ import annotations

import math
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from hyperlab.collector.models import ParsedRecord
from hyperlab.collector.storage import (
    CoordinatedLakeSink,
    CoordinatedLakeWriter,
    CoordinatedWriterError,
    FlushResult,
)
from hyperlab.data.lake import PartitionManifest

_TIMING_WINDOW = 4_096


class WriterWorkerError(CoordinatedWriterError):
    """Fatal failure raised by the isolated coordinated-writer worker."""


class WriterQueueCapacityError(WriterWorkerError):
    """Fail-closed rejection of a complete frame at the exact row bound."""


@dataclass(frozen=True, slots=True)
class DurationSummary:
    """Bounded timing summary; quantiles cover the most recent sample window."""

    count: int
    window_count: int
    min_ms: float | None
    mean_ms: float | None
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    max_ms: float | None

    def as_dict(self) -> dict[str, int | float | None]:
        return {
            "count": self.count,
            "window_count": self.window_count,
            "min_ms": self.min_ms,
            "mean_ms": self.mean_ms,
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
            "max_ms": self.max_ms,
        }


@dataclass(slots=True)
class _Durations:
    count: int = 0
    total_ms: float = 0.0
    min_ms: float | None = None
    max_ms: float | None = None
    samples: deque[float] = field(default_factory=lambda: deque(maxlen=_TIMING_WINDOW))

    def add(self, value_ms: float) -> None:
        value = max(value_ms, 0.0)
        self.count += 1
        self.total_ms += value
        self.min_ms = value if self.min_ms is None else min(self.min_ms, value)
        self.max_ms = value if self.max_ms is None else max(self.max_ms, value)
        self.samples.append(value)

    def summary(self) -> DurationSummary:
        if not self.samples:
            return DurationSummary(self.count, 0, self.min_ms, None, None, None, None, self.max_ms)
        ordered = sorted(self.samples)

        def percentile(ratio: float) -> float:
            index = max(math.ceil(ratio * len(ordered)) - 1, 0)
            return ordered[index]

        return DurationSummary(
            count=self.count,
            window_count=len(ordered),
            min_ms=self.min_ms,
            mean_ms=self.total_ms / self.count,
            p50_ms=percentile(0.50),
            p95_ms=percentile(0.95),
            p99_ms=percentile(0.99),
            max_ms=self.max_ms,
        )


@dataclass(slots=True)
class _Credits:
    manifests: list[PartitionManifest] = field(default_factory=list)
    row_count: int = 0
    duplicate_count: int = 0

    def add(self, result: FlushResult) -> None:
        self.manifests.extend(result.manifests)
        self.row_count += result.row_count
        self.duplicate_count += result.duplicate_count

    def take(self) -> FlushResult:
        result = FlushResult(tuple(self.manifests), self.row_count, self.duplicate_count)
        self.manifests.clear()
        self.row_count = 0
        self.duplicate_count = 0
        return result


@dataclass(frozen=True, slots=True)
class _Frame:
    venue: str
    records: tuple[ParsedRecord, ...]
    enqueued_ns: int
    sequence: int


@dataclass(slots=True)
class _Barrier:
    completed: threading.Event = field(default_factory=threading.Event)
    covers_frame_sequence: int | None = None


@dataclass(slots=True)
class _Stop:
    completed: threading.Event = field(default_factory=threading.Event)


_Command = _Frame | _Barrier | _Stop


class WriterWorkerSink:
    """Venue-scoped nonblocking producer view over one background writer."""

    def __init__(self, owner: CoordinatedWriterWorker, venue: str) -> None:
        self._owner = owner
        self.venue = venue
        self._closed = False
        self._credit_lock = threading.Lock()

    @property
    def pending_count(self) -> int:
        return self._owner._client_pending(self)

    @property
    def high_water(self) -> int:
        return self._owner._client_high_water(self)

    @property
    def should_flush(self) -> bool:
        # Threshold flushes belong to the worker and must never block a network
        # supervisor merely because a batch boundary was crossed.
        return False

    def add(self, record: ParsedRecord) -> bool:
        """Admit one immutable record snapshot; deduplication completes asynchronously."""

        return self._owner._client_add_many(self, (record,)) == 1

    def add_many(self, records: Iterable[ParsedRecord]) -> int:
        """Atomically and nonblockingly admit one complete logical source frame."""

        return self._owner._client_add_many(self, records)

    def collect_completed(self) -> FlushResult:
        """Return already-durable credits without waiting for worker or disk."""

        with self._credit_lock:
            return self._owner._client_collect_completed(self)

    def request_flush(self) -> bool:
        """Enqueue a FIFO durability barrier without waiting for storage."""

        return self._owner._client_request_flush(self)

    def flush(self) -> FlushResult:
        """Wait until every frame admitted before this call is durable."""

        with self._credit_lock:
            return self._owner._client_flush(self)

    def close(self) -> None:
        self._owner._close_client(self)

    def metrics_snapshot(self) -> dict[str, object]:
        return self._owner.metrics_snapshot()


def _resolve_venue_capacities(
    venues: tuple[str, ...],
    queue_capacity: int,
    configured: Mapping[str, int] | None,
) -> dict[str, int]:
    if configured is None:
        base, remainder = divmod(queue_capacity, len(venues))
        if base == 0:
            raise ValueError("writer worker queue capacity must reserve at least one row for every venue")
        return {venue: base + int(index < remainder) for index, venue in enumerate(venues)}

    if frozenset(configured) != frozenset(venues):
        raise ValueError("writer worker venue capacities must contain exactly the configured venues")
    capacities: dict[str, int] = {}
    for venue in venues:
        capacity = configured[venue]
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("writer worker venue capacities must be positive integers")
        capacities[venue] = capacity
    if sum(capacities.values()) != queue_capacity:
        raise ValueError("writer worker venue capacities must sum exactly to queue_capacity")
    return capacities


class CoordinatedWriterWorker:
    """One bounded producer queue around exactly one coordinated lake writer.

    Capacity counts rows admitted but not yet durably flushed: queued/in-flight
    rows plus rows accepted by the physical sink. Complete logical frames are
    reserved and enqueued atomically; the worker is the only thread that calls
    the underlying coordinated writer.
    """

    def __init__(
        self,
        root: Path,
        *,
        venues: tuple[str, ...],
        batch_size: int = 500,
        queue_capacity: int = 10_000,
        venue_capacity_rows: Mapping[str, int] | None = None,
        recent_key_capacity: int = 100_000,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if not venues or len(venues) != len(set(venues)) or any(not venue for venue in venues):
            raise ValueError("writer worker venues must be non-empty and unique")
        if batch_size <= 0 or queue_capacity < batch_size:
            raise ValueError("writer worker capacity cannot be smaller than its batch size")
        self.root = root
        self.batch_size = batch_size
        self.queue_capacity = queue_capacity
        self._venues = venues
        self._venue_set = frozenset(venues)
        self._venue_capacity_rows = _resolve_venue_capacities(
            venues,
            queue_capacity,
            venue_capacity_rows,
        )
        self._monotonic_ns = monotonic_ns
        self._condition = threading.Condition(threading.RLock())
        self._close_lock = threading.Lock()
        self._commands: deque[_Command] = deque()
        self._clients: dict[str, WriterWorkerSink] = {}
        self._accepting = True
        self._closed = False
        self._failure: BaseException | None = None
        self._failure_phase: str | None = None
        self._active_phase = "starting"
        self._pending_nonblocking_flush_sequence: int | None = None
        self._writer_closed = False
        self._next_frame_sequence = 1
        self._last_processed_frame_sequence = 0
        self._last_flushed_frame_sequence = 0

        self._outstanding_rows = 0
        self._outstanding_high_water_rows = 0
        self._queued_by_venue = {venue: 0 for venue in venues}
        self._accepted_pending_by_venue = {venue: 0 for venue in venues}
        self._high_water_by_venue = {venue: 0 for venue in venues}
        self._capacity_rejections_by_venue = {venue: 0 for venue in venues}
        self._frames_enqueued_by_venue = {venue: 0 for venue in venues}
        self._frames_processed_by_venue = {venue: 0 for venue in venues}
        self._durable_rows_by_venue = {venue: 0 for venue in venues}
        self._completed = {venue: _Credits() for venue in venues}
        self._enqueue_delay = {venue: _Durations() for venue in venues}
        self._queue_residence = {venue: _Durations() for venue in venues}
        self._add_timing = {venue: _Durations() for venue in venues}
        self._write_timing = {venue: _Durations() for venue in venues}
        self._flush_timing = _Durations()

        self._writer = CoordinatedLakeWriter(
            root,
            venues=venues,
            batch_size=batch_size,
            queue_capacity=queue_capacity,
            recent_key_capacity=recent_key_capacity,
            monotonic_ns=monotonic_ns,
        )
        try:
            self._physical_clients: dict[str, CoordinatedLakeSink] = {
                venue: self._writer.client(venue) for venue in venues
            }
            self._thread = threading.Thread(
                target=self._run,
                name="hyperlab-coordinated-writer",
                daemon=False,
            )
            self._thread.start()
        except BaseException:
            self._writer.close()
            raise

    @property
    def pending_count(self) -> int:
        with self._condition:
            return self._outstanding_rows

    def client(self, venue: str) -> WriterWorkerSink:
        with self._condition:
            self._raise_if_failed_locked()
            if not self._accepting or self._closed:
                raise RuntimeError("coordinated writer worker is closed")
            if venue not in self._venue_set:
                raise ValueError(f"venue {venue!r} is not configured for this writer worker")
            if venue in self._clients:
                raise RuntimeError(f"writer worker already has a client for venue {venue!r}")
            client = WriterWorkerSink(self, venue)
            self._clients[venue] = client
            return client

    def metrics_snapshot(self) -> dict[str, object]:
        storage = self._writer.metrics_snapshot()
        with self._condition:
            venues: dict[str, object] = {}
            for venue in self._venues:
                queued = self._queued_by_venue[venue]
                accepted = self._accepted_pending_by_venue[venue]
                venues[venue] = {
                    "capacity_rows": self._venue_capacity_rows[venue],
                    "pending_rows": queued + accepted,
                    "queued_or_inflight_rows": queued,
                    "accepted_pending_rows": accepted,
                    "high_water_rows": self._high_water_by_venue[venue],
                    "capacity_rejections": self._capacity_rejections_by_venue[venue],
                    "frames_enqueued": self._frames_enqueued_by_venue[venue],
                    "frames_processed": self._frames_processed_by_venue[venue],
                    "durable_rows": self._durable_rows_by_venue[venue],
                    "enqueue_delay_ms": self._enqueue_delay[venue].summary().as_dict(),
                    "queue_residence_ms": self._queue_residence[venue].summary().as_dict(),
                    "add_ms": self._add_timing[venue].summary().as_dict(),
                    "write_ms": self._write_timing[venue].summary().as_dict(),
                }
            failure: dict[str, str] | None = None
            if self._failure is not None:
                failure = {
                    "phase": self._failure_phase or "unknown",
                    "type": type(self._failure).__name__,
                    "message": str(self._failure),
                }
            return {
                "queue_capacity_rows": self.queue_capacity,
                "batch_size_rows": self.batch_size,
                "outstanding_rows": self._outstanding_rows,
                "outstanding_high_water_rows": self._outstanding_high_water_rows,
                "active_phase": self._active_phase,
                "accepting": self._accepting,
                "closed": self._closed,
                "failure": failure,
                "flush_ms": self._flush_timing.summary().as_dict(),
                "storage": storage,
                "venues": venues,
            }

    def close(self) -> None:
        """Drain queued frames, flush durably, close the sole writer, and join."""

        with self._close_lock:
            stop: _Stop | None = None
            with self._condition:
                if self._failure is None and not self._closed:
                    self._accepting = False
                    stop = _Stop()
                    self._commands.append(stop)
                    self._condition.notify()
                elif self._failure is None and self._closed:
                    return
            if stop is not None:
                stop.completed.wait()
            self._thread.join()
            with self._condition:
                self._raise_if_failed_locked()
                if not self._closed:
                    raise WriterWorkerError("coordinated writer worker terminated without closing")

    def __enter__(self) -> CoordinatedWriterWorker:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _require_active_locked(self, client: WriterWorkerSink) -> None:
        self._raise_if_failed_locked()
        if not self._accepting or self._closed:
            raise WriterWorkerError("coordinated writer worker is closed")
        if client._closed or self._clients.get(client.venue) is not client:
            raise WriterWorkerError(f"writer worker client for {client.venue!r} is closed")

    def _raise_if_failed_locked(self) -> None:
        failure = self._failure
        if failure is None:
            return
        phase = self._failure_phase or "unknown"
        raise WriterWorkerError(
            f"coordinated writer worker failed during {phase}: {type(failure).__name__}: {failure}"
        ) from failure

    def _client_pending(self, client: WriterWorkerSink) -> int:
        with self._condition:
            if client._closed or self._clients.get(client.venue) is not client:
                raise WriterWorkerError(f"writer worker client for {client.venue!r} is closed")
            return self._queued_by_venue[client.venue] + self._accepted_pending_by_venue[client.venue]

    def _client_high_water(self, client: WriterWorkerSink) -> int:
        with self._condition:
            if client._closed or self._clients.get(client.venue) is not client:
                raise WriterWorkerError(f"writer worker client for {client.venue!r} is closed")
            return self._high_water_by_venue[client.venue]

    @staticmethod
    def _freeze_frame(records: Iterable[ParsedRecord]) -> tuple[ParsedRecord, ...]:
        return tuple(ParsedRecord(record.record_type, record.asset, dict(record.row)) for record in records)

    def _client_add_many(
        self,
        client: WriterWorkerSink,
        records: Iterable[ParsedRecord],
    ) -> int:
        started_ns = self._monotonic_ns()
        with self._condition:
            self._require_active_locked(client)
        batch = self._freeze_frame(records)
        with self._condition:
            # A failure may have landed while caller-owned records were frozen.
            self._require_active_locked(client)
        mismatched = sorted(
            {str(record.row.get("venue")) for record in batch if record.row.get("venue") != client.venue}
        )
        if mismatched:
            raise CoordinatedWriterError(
                f"writer worker client venue mismatch: expected {client.venue!r}, observed {mismatched!r}"
            )

        with self._condition:
            self._require_active_locked(client)
            observed_ns = self._monotonic_ns()
            self._enqueue_delay[client.venue].add(self._milliseconds(observed_ns - started_ns))
            row_count = len(batch)
            venue_outstanding = (
                self._queued_by_venue[client.venue] + self._accepted_pending_by_venue[client.venue]
            )
            venue_capacity = self._venue_capacity_rows[client.venue]
            if row_count > venue_capacity - venue_outstanding:
                self._capacity_rejections_by_venue[client.venue] += 1
                raise WriterQueueCapacityError(
                    "coordinated writer venue outstanding-row capacity exceeded "
                    f"for {client.venue!r}: capacity={venue_capacity}; "
                    "no record was admitted"
                )
            if row_count > self.queue_capacity - self._outstanding_rows:
                self._capacity_rejections_by_venue[client.venue] += 1
                raise WriterQueueCapacityError(
                    "coordinated writer outstanding-row capacity exceeded before "
                    "atomic frame enqueue; no record was admitted"
                )
            if not batch:
                return 0
            frame = _Frame(
                venue=client.venue,
                records=batch,
                enqueued_ns=observed_ns,
                sequence=self._next_frame_sequence,
            )
            self._next_frame_sequence += 1
            self._commands.append(frame)
            self._outstanding_rows += row_count
            self._outstanding_high_water_rows = max(
                self._outstanding_high_water_rows,
                self._outstanding_rows,
            )
            self._queued_by_venue[client.venue] += row_count
            venue_pending = venue_outstanding + row_count
            self._high_water_by_venue[client.venue] = max(
                self._high_water_by_venue[client.venue],
                venue_pending,
            )
            self._frames_enqueued_by_venue[client.venue] += 1
            self._check_accounting_locked()
            self._condition.notify()
            return row_count

    def _client_collect_completed(self, client: WriterWorkerSink) -> FlushResult:
        with self._condition:
            self._raise_if_failed_locked()
            if client._closed or self._clients.get(client.venue) is not client:
                raise WriterWorkerError(f"writer worker client for {client.venue!r} is closed")
            return self._completed[client.venue].take()

    def _client_request_flush(self, client: WriterWorkerSink) -> bool:
        with self._condition:
            self._require_active_locked(client)
            covered_sequence = self._next_frame_sequence - 1
            pending_sequence = self._pending_nonblocking_flush_sequence
            if pending_sequence is not None and pending_sequence >= covered_sequence:
                return False
            if covered_sequence <= self._last_flushed_frame_sequence:
                return False
            barrier = _Barrier(covers_frame_sequence=covered_sequence)
            self._pending_nonblocking_flush_sequence = covered_sequence
            self._commands.append(barrier)
            self._condition.notify()
            return True

    def _client_flush(self, client: WriterWorkerSink) -> FlushResult:
        barrier = _Barrier()
        with self._condition:
            self._require_active_locked(client)
            self._commands.append(barrier)
            self._condition.notify()
        barrier.completed.wait()
        with self._condition:
            self._raise_if_failed_locked()
            if client._closed or self._clients.get(client.venue) is not client:
                raise WriterWorkerError(f"writer worker client for {client.venue!r} is closed")
            return self._completed[client.venue].take()

    def _close_client(self, client: WriterWorkerSink) -> None:
        with self._condition:
            self._raise_if_failed_locked()
            if client._closed:
                return
            if self._clients.get(client.venue) is not client:
                raise RuntimeError("writer worker client does not belong to this writer")
            client._closed = True

    def _run(self) -> None:
        current: _Command | None = None
        try:
            while True:
                with self._condition:
                    while not self._commands:
                        self._active_phase = "waiting"
                        self._condition.wait()
                    current = self._commands.popleft()
                    self._active_phase = "dequeue"
                if isinstance(current, _Frame):
                    self._process_frame(current)
                    current = None
                    continue
                if isinstance(current, _Barrier):
                    self._set_phase("barrier_flush")
                    self._flush_all()
                    if current.covers_frame_sequence is not None:
                        with self._condition:
                            if self._pending_nonblocking_flush_sequence == current.covers_frame_sequence:
                                self._pending_nonblocking_flush_sequence = None
                            self._condition.notify_all()
                    current.completed.set()
                    current = None
                    continue
                self._set_phase("close_flush")
                self._flush_all()
                with self._condition:
                    if self._outstanding_rows:
                        raise CoordinatedWriterError(
                            "writer worker close reached a non-zero outstanding-row count"
                        )
                self._set_phase("close_writer")
                self._close_underlying()
                current.completed.set()
                current = None
                return
        except BaseException as exc:
            self._record_failure(exc, current)
        finally:
            if not self._writer_closed:
                try:
                    self._set_phase("failure_close")
                    self._close_underlying()
                except BaseException as cleanup_error:
                    failure = self._failure
                    if failure is None:
                        self._record_failure(cleanup_error, current)
                    else:
                        failure.add_note(
                            "underlying writer close also failed: "
                            f"{type(cleanup_error).__name__}: {cleanup_error}"
                        )
            with self._condition:
                self._accepting = False
                self._closed = True
                self._active_phase = "failed" if self._failure is not None else "closed"
                self._signal_waiters_locked(current)
                self._condition.notify_all()

    def _process_frame(self, frame: _Frame) -> None:
        frame_started_ns = self._monotonic_ns()
        with self._condition:
            self._queue_residence[frame.venue].add(self._milliseconds(frame_started_ns - frame.enqueued_ns))
        add_started_ns = self._monotonic_ns()
        self._set_phase("add")
        try:
            accepted = self._physical_clients[frame.venue].add_many(frame.records)
        except BaseException:
            with self._condition:
                row_count = len(frame.records)
                self._queued_by_venue[frame.venue] -= row_count
                self._outstanding_rows -= row_count
                self._check_accounting_locked()
            raise
        finally:
            add_ended_ns = self._monotonic_ns()
            with self._condition:
                self._add_timing[frame.venue].add(self._milliseconds(add_ended_ns - add_started_ns))

        with self._condition:
            row_count = len(frame.records)
            duplicates = row_count - accepted
            self._queued_by_venue[frame.venue] -= row_count
            self._accepted_pending_by_venue[frame.venue] += accepted
            self._outstanding_rows -= duplicates
            self._frames_processed_by_venue[frame.venue] += 1
            self._last_processed_frame_sequence = frame.sequence
            should_flush = sum(self._accepted_pending_by_venue.values()) >= self.batch_size
            self._check_accounting_locked()
        try:
            if should_flush:
                self._set_phase("auto_flush")
                self._flush_all()
        finally:
            frame_ended_ns = self._monotonic_ns()
            with self._condition:
                self._write_timing[frame.venue].add(self._milliseconds(frame_ended_ns - frame_started_ns))

    def _flush_all(self) -> None:
        started_ns = self._monotonic_ns()
        try:
            results = self._writer.flush_all()
        finally:
            ended_ns = self._monotonic_ns()
            with self._condition:
                self._flush_timing.add(self._milliseconds(ended_ns - started_ns))

        with self._condition:
            if frozenset(results) != self._venue_set:
                raise CoordinatedWriterError("writer worker coordinated flush returned incompatible venues")
            for venue in self._venues:
                result = results[venue]
                pending = self._accepted_pending_by_venue[venue]
                if result.row_count != pending:
                    raise CoordinatedWriterError(
                        "writer worker durable-row accounting did not match accepted pending rows "
                        f"for {venue}: durable={result.row_count}, pending={pending}"
                    )

            for venue in self._venues:
                result = results[venue]
                self._accepted_pending_by_venue[venue] -= result.row_count
                self._outstanding_rows -= result.row_count
                self._durable_rows_by_venue[venue] += result.row_count
                self._completed[venue].add(result)
            self._last_flushed_frame_sequence = self._last_processed_frame_sequence
            self._check_accounting_locked()

    def _close_underlying(self) -> None:
        if self._writer_closed:
            return
        try:
            for client in self._physical_clients.values():
                client.close()
        finally:
            try:
                self._writer.close()
            finally:
                self._writer_closed = True

    def _record_failure(
        self,
        error: BaseException,
        current: _Command | None,
    ) -> None:
        with self._condition:
            if self._failure is None:
                self._failure = error
                self._failure_phase = self._active_phase
            self._accepting = False
            self._signal_waiters_locked(current)
            while self._commands:
                command = self._commands.popleft()
                if isinstance(command, _Frame):
                    row_count = len(command.records)
                    self._queued_by_venue[command.venue] -= row_count
                    self._outstanding_rows -= row_count
                else:
                    command.completed.set()
            self._check_accounting_locked()
            self._condition.notify_all()

    @staticmethod
    def _signal_waiters_locked(command: _Command | None) -> None:
        if isinstance(command, (_Barrier, _Stop)):
            command.completed.set()

    def _set_phase(self, phase: str) -> None:
        with self._condition:
            self._active_phase = phase

    def _check_accounting_locked(self) -> None:
        expected = sum(self._queued_by_venue.values()) + sum(self._accepted_pending_by_venue.values())
        if expected != self._outstanding_rows:
            raise CoordinatedWriterError(
                "writer worker outstanding-row accounting mismatch: "
                f"counter={self._outstanding_rows}, components={expected}"
            )
        if self._outstanding_rows < 0 or self._outstanding_rows > self.queue_capacity:
            raise CoordinatedWriterError(
                "writer worker outstanding-row counter is outside its exact capacity"
            )
        for venue in self._venues:
            venue_outstanding = self._queued_by_venue[venue] + self._accepted_pending_by_venue[venue]
            if venue_outstanding < 0 or venue_outstanding > self._venue_capacity_rows[venue]:
                raise CoordinatedWriterError(
                    f"writer worker {venue!r} outstanding-row counter is outside its reserved capacity"
                )

    @staticmethod
    def _milliseconds(nanoseconds: int) -> float:
        return max(nanoseconds, 0) / 1_000_000
