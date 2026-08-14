from __future__ import annotations

import contextlib
import copy
import multiprocessing as mp
import os
import pickle
import queue
import signal
import threading
import time
import traceback
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from hyperlab.collector.models import ParsedRecord
from hyperlab.collector.storage import (
    CoordinatedLakeWriter,
    CoordinatedWriterError,
    FlushResult,
)
from hyperlab.collector.telemetry import ProcessRuntimeTelemetry
from hyperlab.collector.writer_worker import (
    WriterQueueCapacityError,
    WriterWorkerError,
    _Durations,
    _resolve_venue_capacities,
)
from hyperlab.data.lake import PartitionManifest

_CHILD_SNAPSHOT_INTERVAL_NS = 1_000_000_000
_CHILD_EXIT_EVENT_GRACE_NS = 1_000_000_000
_CHILD_CACHE_CURRENT_MAX_AGE_MS = 2_500.0
_PROCESS_JOIN_TIMEOUT_SECONDS = 5.0


class ProcessWriterError(WriterWorkerError):
    """Fatal failure raised by the process-isolated writer."""


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
        result = FlushResult(
            tuple(self.manifests),
            self.row_count,
            self.duplicate_count,
        )
        self.manifests.clear()
        self.row_count = 0
        self.duplicate_count = 0
        return result


@dataclass(slots=True)
class _PendingCommand:
    kind: str
    venue: str | None = None
    row_count: int = 0
    event: threading.Event | None = None


def _child_snapshot(
    writer: CoordinatedLakeWriter | None,
    telemetry: ProcessRuntimeTelemetry,
    *,
    phase: str,
) -> dict[str, object]:
    storage: dict[str, object] | None = None
    if writer is not None:
        try:
            storage = writer.metrics_snapshot()
        except Exception as exc:
            storage = {
                "telemetry_error": f"{type(exc).__name__}: {exc}",
            }
    return {
        "pid": os.getpid(),
        "captured_monotonic_ns": time.monotonic_ns(),
        "phase": phase,
        "process": telemetry.snapshot(),
        "storage": storage,
    }


def _validate_child_flush(
    results: Mapping[str, FlushResult],
    *,
    venues: tuple[str, ...],
    accepted_pending: Mapping[str, int],
    duplicates_pending: Mapping[str, int],
) -> None:
    if frozenset(results) != frozenset(venues):
        raise CoordinatedWriterError("process writer coordinated flush returned incompatible venues")
    for venue in venues:
        result = results[venue]
        if result.row_count != accepted_pending[venue]:
            raise CoordinatedWriterError(
                "process writer durable-row accounting did not match accepted "
                f"pending rows for {venue}: durable={result.row_count}, "
                f"pending={accepted_pending[venue]}"
            )
        if result.duplicate_count != duplicates_pending[venue]:
            raise CoordinatedWriterError(
                "process writer duplicate accounting did not match pending "
                f"duplicates for {venue}: durable={result.duplicate_count}, "
                f"pending={duplicates_pending[venue]}"
            )


def _writer_process_main(
    root: str,
    venues: tuple[str, ...],
    batch_size: int,
    queue_capacity: int,
    recent_key_capacity: int,
    command_queue: Any,
    result_queue: Any,
) -> None:
    for signal_name in ("SIGINT", "SIGTERM"):
        managed_signal = getattr(signal, signal_name, None)
        if managed_signal is not None:
            signal.signal(managed_signal, signal.SIG_IGN)

    telemetry = ProcessRuntimeTelemetry()
    writer: CoordinatedLakeWriter | None = None
    clients: dict[str, Any] = {}
    accepted_pending = {venue: 0 for venue in venues}
    duplicates_pending = {venue: 0 for venue in venues}
    event_sequence = 0
    phase = "initializing"
    current_command_id: int | None = None

    last_snapshot_ns = 0
    resources_closed = False
    parent_process = mp.parent_process()

    def parent_is_alive() -> bool:
        if parent_process is None:
            return True
        try:
            return parent_process.is_alive()
        except (AssertionError, OSError, ValueError):
            return False

    def snapshot_if_due(
        *,
        snapshot_phase: str,
        force: bool = False,
    ) -> dict[str, object] | None:
        nonlocal last_snapshot_ns
        observed_ns = time.monotonic_ns()
        if (
            not force
            and last_snapshot_ns > 0
            and observed_ns - last_snapshot_ns < _CHILD_SNAPSHOT_INTERVAL_NS
        ):
            return None
        snapshot = _child_snapshot(
            writer,
            telemetry,
            phase=snapshot_phase,
        )
        captured_ns = snapshot.get("captured_monotonic_ns")
        last_snapshot_ns = captured_ns if isinstance(captured_ns, int) else observed_ns
        return snapshot

    def close_owned_resources() -> None:
        nonlocal resources_closed
        if resources_closed:
            return
        for client in clients.values():
            with contextlib.suppress(BaseException):
                client.close()
        if writer is not None:
            with contextlib.suppress(BaseException):
                writer.close()
        telemetry.close()
        resources_closed = True

    def abandon_transports() -> None:
        for transport in (command_queue, result_queue):
            with contextlib.suppress(AttributeError, OSError, ValueError):
                transport.cancel_join_thread()

    def emit(kind: str, payload: dict[str, object]) -> None:
        nonlocal event_sequence
        event_sequence += 1
        result_queue.put((event_sequence, kind, payload))

    def emit_phase(value: str) -> None:
        nonlocal phase
        phase = value
        emit("phase", {"phase": phase})

    def flush_payload(command_id: int | None, reason: str) -> dict[str, object]:
        nonlocal phase
        phase = f"{reason}_flush"
        emit_phase(phase)
        started_ns = time.monotonic_ns()
        assert writer is not None
        results = writer.flush_all()
        ended_ns = time.monotonic_ns()
        _validate_child_flush(
            results,
            venues=venues,
            accepted_pending=accepted_pending,
            duplicates_pending=duplicates_pending,
        )
        for venue in venues:
            accepted_pending[venue] = 0
            duplicates_pending[venue] = 0
        return {
            "command_id": command_id,
            "reason": reason,
            "results": results,
            "flush_duration_ns": max(ended_ns - started_ns, 0),
            "snapshot": snapshot_if_due(
                snapshot_phase=f"{reason}_flush_complete",
                force=True,
            ),
        }

    try:
        writer = CoordinatedLakeWriter(
            Path(root),
            venues=venues,
            batch_size=batch_size,
            queue_capacity=queue_capacity,
            recent_key_capacity=recent_key_capacity,
        )
        clients = {venue: writer.client(venue) for venue in venues}
        phase = "ready"
        emit(
            "ready",
            {
                "phase": phase,
                "snapshot": snapshot_if_due(
                    snapshot_phase=phase,
                    force=True,
                ),
            },
        )

        while True:
            if not parent_is_alive():
                phase = "parent_exit"
                close_owned_resources()
                abandon_transports()
                return
            try:
                command = command_queue.get(timeout=0.5)
            except queue.Empty:
                if not parent_is_alive():
                    phase = "parent_exit"
                    close_owned_resources()
                    abandon_transports()
                    return
                phase = "waiting"
                snapshot = snapshot_if_due(snapshot_phase=phase)
                if snapshot is not None:
                    emit(
                        "heartbeat",
                        {
                            "phase": phase,
                            "snapshot": snapshot,
                        },
                    )
                continue
            dequeued_ns = time.monotonic_ns()
            if not parent_is_alive():
                phase = "parent_exit"
                close_owned_resources()
                abandon_transports()
                return
            if (
                not isinstance(command, tuple)
                or len(command) != 6
                or not isinstance(command[0], int)
                or not isinstance(command[1], str)
            ):
                raise CoordinatedWriterError("process writer received an invalid command envelope")
            (
                current_command_id,
                kind,
                venue,
                serialized_records,
                submitted_rows,
                enqueued_ns,
            ) = command

            if kind == "frame":
                if (
                    venue not in clients
                    or not isinstance(serialized_records, bytes)
                    or not isinstance(submitted_rows, int)
                    or submitted_rows <= 0
                    or not isinstance(enqueued_ns, int)
                ):
                    raise CoordinatedWriterError("process writer received an invalid frame command")
                frame_started_ns = dequeued_ns
                emit_phase("deserialize_add")
                records = pickle.loads(serialized_records)
                if not isinstance(records, tuple) or len(records) != submitted_rows:
                    raise CoordinatedWriterError("process writer frame serialization row count mismatch")
                add_started_ns = time.monotonic_ns()
                accepted = clients[venue].add_many(records)
                add_ended_ns = time.monotonic_ns()
                duplicates = submitted_rows - accepted
                if accepted < 0 or duplicates < 0 or accepted + duplicates != submitted_rows:
                    raise CoordinatedWriterError("process writer frame acknowledgement accounting mismatch")
                accepted_pending[venue] += accepted
                duplicates_pending[venue] += duplicates
                phase = "frame_complete"
                emit(
                    "frame",
                    {
                        "command_id": current_command_id,
                        "venue": venue,
                        "submitted_rows": submitted_rows,
                        "accepted_rows": accepted,
                        "duplicate_rows": duplicates,
                        "queue_residence_ns": max(frame_started_ns - enqueued_ns, 0),
                        "add_duration_ns": max(add_ended_ns - add_started_ns, 0),
                        "write_duration_ns": max(add_ended_ns - frame_started_ns, 0),
                        "phase": phase,
                        "snapshot": snapshot_if_due(snapshot_phase=phase),
                    },
                )
                if sum(accepted_pending.values()) >= batch_size:
                    emit("flush", flush_payload(None, "auto"))
                current_command_id = None
                continue

            if kind == "barrier":
                emit("flush", flush_payload(current_command_id, "barrier"))
                current_command_id = None
                continue

            if kind != "stop":
                raise CoordinatedWriterError(f"process writer received unknown command kind {kind!r}")

            stop_payload = flush_payload(current_command_id, "stop")
            emit_phase("closing")
            for client in clients.values():
                client.close()
            writer.close()
            phase = "closed"
            final_snapshot = snapshot_if_due(
                snapshot_phase=phase,
                force=True,
            )
            telemetry.close()
            resources_closed = True
            stop_payload["phase"] = phase
            stop_payload["snapshot"] = final_snapshot
            emit("stopped", stop_payload)
            return
    except BaseException as exc:
        failure_payload: dict[str, object] = {
            "phase": phase,
            "command_id": current_command_id,
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        }
        try:
            failure_payload["snapshot"] = _child_snapshot(
                writer,
                telemetry,
                phase="failed",
            )
            emit("failure", failure_payload)
        except BaseException:
            pass
        close_owned_resources()


class ProcessWriterSink:
    """Venue-scoped producer view over a process-isolated writer."""

    def __init__(self, owner: CoordinatedWriterProcess, venue: str) -> None:
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
        return False

    def add(self, record: ParsedRecord) -> bool:
        return self._owner._client_add_many(self, (record,)) == 1

    def add_many(self, records: Iterable[ParsedRecord]) -> int:
        return self._owner._client_add_many(self, records)

    def collect_completed(self) -> FlushResult:
        with self._credit_lock:
            return self._owner._client_collect_completed(self)

    def request_flush(self) -> bool:
        """Enqueue a FIFO durability barrier without waiting for storage."""

        return self._owner._client_request_flush(self)

    def flush(self) -> FlushResult:
        with self._credit_lock:
            return self._owner._client_flush(self)

    def close(self) -> None:
        self._owner._close_client(self)

    def metrics_snapshot(self) -> dict[str, object]:
        return self._owner.metrics_snapshot()


class CoordinatedWriterProcess:
    """Bounded multi-venue writer whose root lock and storage live in one child."""

    def __init__(
        self,
        root: Path,
        *,
        venues: tuple[str, ...],
        batch_size: int = 500,
        queue_capacity: int = 10_000,
        venue_capacity_rows: Mapping[str, int] | None = None,
        recent_key_capacity: int = 100_000,
        start_method: str = "spawn",
        startup_timeout_seconds: float = 30.0,
        monotonic_ns: Any = time.monotonic_ns,
    ) -> None:
        if not venues or len(venues) != len(set(venues)) or any(not venue for venue in venues):
            raise ValueError("process writer venues must be non-empty and unique")
        if batch_size <= 0 or queue_capacity < batch_size:
            raise ValueError("process writer capacity cannot be smaller than its batch size")
        if recent_key_capacity <= 0:
            raise ValueError("process writer recent key capacity must be positive")
        if startup_timeout_seconds <= 0:
            raise ValueError("process writer startup timeout must be positive")

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
        self._clients: dict[str, ProcessWriterSink] = {}
        self._accepting = True
        self._closed = False
        self._ready = False
        self._expected_stop = False
        self._stop_acknowledged = False
        self._accounting_status = "exact"
        self._failure: dict[str, object] | None = None
        self._next_command_id = 1
        self._expected_event_sequence = 1
        self._pending_commands: dict[int, _PendingCommand] = {}

        self._outstanding_rows = 0
        self._outstanding_high_water_rows = 0
        self._queued_by_venue = {venue: 0 for venue in venues}
        self._accepted_pending_by_venue = {venue: 0 for venue in venues}
        self._duplicates_pending_by_venue = {venue: 0 for venue in venues}
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
        self._child_cache: dict[str, object] | None = None
        self._child_cache_received_ns: int | None = None
        self._last_event_sequence = 0
        self._child_phase = "starting"
        self._dead_process_observed_ns: int | None = None

        self._context: Any = mp.get_context(start_method)
        transport_capacity = max(queue_capacity + len(venues) + 4, 64)
        result_capacity = max(queue_capacity * 4 + 64, 256)
        self._command_queue = self._context.Queue(maxsize=transport_capacity)
        self._result_queue = self._context.Queue(maxsize=result_capacity)
        self._ready_event = threading.Event()
        self._listener_stop = threading.Event()
        process_factory = cast(Any, self._context).Process
        self._process = process_factory(
            target=_writer_process_main,
            name="hyperlab-coordinated-writer-process",
            args=(
                str(root),
                venues,
                batch_size,
                queue_capacity,
                recent_key_capacity,
                self._command_queue,
                self._result_queue,
            ),
            daemon=False,
        )
        self._listener = threading.Thread(
            target=self._listen,
            name="hyperlab-writer-process-monitor",
            daemon=True,
        )
        try:
            self._process.start()
            self._listener.start()
            self._wait_for_ready(startup_timeout_seconds)
        except BaseException:
            self._cleanup_failed_process()
            raise

    @property
    def pending_count(self) -> int:
        with self._condition:
            return self._outstanding_rows

    def client(self, venue: str) -> ProcessWriterSink:
        with self._condition:
            self._raise_if_failed_locked()
            if not self._accepting or self._closed:
                raise ProcessWriterError("coordinated writer process is closed")
            if venue not in self._venue_set:
                raise ValueError(f"venue {venue!r} is not configured for this writer process")
            if venue in self._clients:
                raise RuntimeError(f"process writer already has a client for venue {venue!r}")
            client = ProcessWriterSink(self, venue)
            self._clients[venue] = client
            return client

    def metrics_snapshot(self) -> dict[str, object]:
        with self._condition:
            now_ns = self._monotonic_ns()
            cache_age_ms = (
                None
                if self._child_cache_received_ns is None
                else max(now_ns - self._child_cache_received_ns, 0) / 1_000_000
            )
            child_cache = None if self._child_cache is None else copy.deepcopy(self._child_cache)
            process_alive = self._process.is_alive()
            venues: dict[str, object] = {}
            for venue in self._venues:
                queued = self._queued_by_venue[venue]
                accepted = self._accepted_pending_by_venue[venue]
                venues[venue] = {
                    "capacity_rows": self._venue_capacity_rows[venue],
                    "pending_rows": queued + accepted,
                    "queued_or_inflight_rows": queued,
                    "accepted_pending_rows": accepted,
                    "duplicates_pending_credit": self._duplicates_pending_by_venue[venue],
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
            storage = None if child_cache is None else child_cache.get("storage")
            return {
                "isolation": "spawned_process",
                "queue_capacity_rows": self.queue_capacity,
                "batch_size_rows": self.batch_size,
                "outstanding_rows": self._outstanding_rows,
                "outstanding_high_water_rows": self._outstanding_high_water_rows,
                "accounting_status": self._accounting_status,
                "active_phase": self._child_phase,
                "accepting": self._accepting,
                "closed": self._closed,
                "failure": (None if self._failure is None else dict(self._failure)),
                "flush_ms": self._flush_timing.summary().as_dict(),
                "storage": storage,
                "child_process": {
                    "pid": self._process.pid,
                    "start_method": self._context.get_start_method(),
                    "alive": process_alive,
                    "exitcode": self._process.exitcode,
                    "last_event_sequence": self._last_event_sequence,
                    "cache_age_ms": cache_age_ms,
                    "cache_current": (
                        process_alive
                        and cache_age_ms is not None
                        and cache_age_ms <= _CHILD_CACHE_CURRENT_MAX_AGE_MS
                    ),
                    "cache_stale": (cache_age_ms is None or cache_age_ms > _CHILD_CACHE_CURRENT_MAX_AGE_MS),
                    "telemetry": (None if child_cache is None else child_cache.get("process")),
                },
                "venues": venues,
            }

    def close(self) -> None:
        with self._close_lock:
            stop_event: threading.Event | None = None
            with self._condition:
                if self._failure is None and self._closed:
                    return
                if self._failure is None:
                    self._accepting = False
                    self._expected_stop = True
                    stop_event = threading.Event()
                    command_id = self._next_command_id
                    self._next_command_id += 1
                    self._put_command_locked(
                        (
                            command_id,
                            "stop",
                            None,
                            None,
                            0,
                            self._monotonic_ns(),
                        )
                    )
                    self._pending_commands[command_id] = _PendingCommand(
                        kind="stop",
                        event=stop_event,
                    )
            if stop_event is not None:
                self._wait_for_event(stop_event)
            if self._failure is not None:
                self._cleanup_failed_process()
                with self._condition:
                    self._raise_if_failed_locked()
            self._process.join(timeout=_PROCESS_JOIN_TIMEOUT_SECONDS)
            if self._process.is_alive():
                with self._condition:
                    self._record_failure_locked(
                        {
                            "phase": "child_shutdown",
                            "type": "TimeoutError",
                            "message": ("writer child did not exit after its durable stop acknowledgement"),
                        },
                        accounting_indeterminate=False,
                    )
                self._force_stop_process()
            self._listener_stop.set()
            self._listener.join(timeout=5)
            self._close_queues()
            with self._condition:
                self._raise_if_failed_locked()
                if not self._closed or not self._stop_acknowledged:
                    raise ProcessWriterError("coordinated writer process terminated without a durable stop")

    def __enter__(self) -> CoordinatedWriterProcess:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _wait_for_ready(self, timeout_seconds: float) -> None:
        deadline = time.monotonic() + timeout_seconds
        while not self._ready_event.wait(timeout=0.05):
            with self._condition:
                self._audit_liveness_locked()
                self._raise_if_failed_locked()
            if time.monotonic() >= deadline:
                with self._condition:
                    self._record_failure_locked(
                        {
                            "phase": "startup",
                            "type": "TimeoutError",
                            "message": (
                                f"process writer child did not become ready within {timeout_seconds:.3f}s"
                            ),
                        }
                    )
                break
        with self._condition:
            self._raise_if_failed_locked()
            if not self._ready:
                raise ProcessWriterError("process writer ready event was signalled without readiness")

    def _wait_for_event(self, event: threading.Event) -> None:
        while not event.wait(timeout=0.1):
            with self._condition:
                self._audit_liveness_locked()
                self._raise_if_failed_locked()
        with self._condition:
            self._raise_if_failed_locked()

    def _audit_liveness_locked(self) -> None:
        if self._failure is not None:
            return
        if not self._listener.is_alive():
            self._record_failure_locked(
                {
                    "phase": "result_monitor",
                    "type": "ListenerThreadExit",
                    "message": "writer result listener exited unexpectedly",
                }
            )
            return
        exitcode = self._process.exitcode
        if exitcode is None or self._stop_acknowledged:
            self._dead_process_observed_ns = None
            return
        observed_ns = time.monotonic_ns()
        if self._dead_process_observed_ns is None:
            self._dead_process_observed_ns = observed_ns
            return
        if observed_ns - self._dead_process_observed_ns < _CHILD_EXIT_EVENT_GRACE_NS:
            return
        self._record_failure_locked(
            {
                "phase": "child_exit",
                "type": "ChildProcessExit",
                "message": (
                    f"writer child exited before its terminal event was observed; exitcode={exitcode}"
                ),
                "exitcode": exitcode,
            }
        )

    def _raise_if_failed_locked(self) -> None:
        if self._failure is None:
            return
        phase = str(self._failure.get("phase", "unknown"))
        error_type = str(self._failure.get("type", "ProcessWriterFailure"))
        message = str(self._failure.get("message", ""))
        raise ProcessWriterError(f"coordinated writer process failed during {phase}: {error_type}: {message}")

    def _require_active_locked(self, client: ProcessWriterSink) -> None:
        self._raise_if_failed_locked()
        if not self._accepting or self._closed:
            raise ProcessWriterError("coordinated writer process is closed")
        if client._closed or self._clients.get(client.venue) is not client:
            raise ProcessWriterError(f"process writer client for {client.venue!r} is closed")

    def _client_pending(self, client: ProcessWriterSink) -> int:
        with self._condition:
            if client._closed or self._clients.get(client.venue) is not client:
                raise ProcessWriterError(f"process writer client for {client.venue!r} is closed")
            return self._queued_by_venue[client.venue] + self._accepted_pending_by_venue[client.venue]

    def _client_high_water(self, client: ProcessWriterSink) -> int:
        with self._condition:
            if client._closed or self._clients.get(client.venue) is not client:
                raise ProcessWriterError(f"process writer client for {client.venue!r} is closed")
            return self._high_water_by_venue[client.venue]

    @staticmethod
    def _freeze_and_serialize(
        records: Iterable[ParsedRecord],
    ) -> tuple[bytes, tuple[ParsedRecord, ...]]:
        frozen = tuple(
            ParsedRecord(
                record.record_type,
                record.asset,
                dict(record.row),
            )
            for record in records
        )
        return pickle.dumps(frozen, protocol=pickle.HIGHEST_PROTOCOL), frozen

    def _client_add_many(
        self,
        client: ProcessWriterSink,
        records: Iterable[ParsedRecord],
    ) -> int:
        started_ns = self._monotonic_ns()
        with self._condition:
            self._require_active_locked(client)
        serialized, frozen = self._freeze_and_serialize(records)
        with self._condition:
            self._require_active_locked(client)

        mismatched = sorted(
            {str(record.row.get("venue")) for record in frozen if record.row.get("venue") != client.venue}
        )
        if mismatched:
            raise CoordinatedWriterError(
                f"process writer client venue mismatch: expected {client.venue!r}, observed {mismatched!r}"
            )
        row_count = len(frozen)
        if row_count == 0:
            return 0

        with self._condition:
            self._require_active_locked(client)
            observed_ns = self._monotonic_ns()
            self._enqueue_delay[client.venue].add(max(observed_ns - started_ns, 0) / 1_000_000)
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

            command_id = self._next_command_id
            self._next_command_id += 1
            try:
                self._put_command_locked(
                    (
                        command_id,
                        "frame",
                        client.venue,
                        serialized,
                        row_count,
                        observed_ns,
                    )
                )
            except ProcessWriterError:
                self._capacity_rejections_by_venue[client.venue] += 1
                raise
            self._pending_commands[command_id] = _PendingCommand(
                kind="frame",
                venue=client.venue,
                row_count=row_count,
            )
            self._queued_by_venue[client.venue] += row_count
            self._outstanding_rows += row_count
            self._outstanding_high_water_rows = max(
                self._outstanding_high_water_rows,
                self._outstanding_rows,
            )
            venue_pending = venue_outstanding + row_count
            self._high_water_by_venue[client.venue] = max(
                self._high_water_by_venue[client.venue],
                venue_pending,
            )
            self._frames_enqueued_by_venue[client.venue] += 1
            self._check_accounting_locked()
            return row_count

    def _client_collect_completed(
        self,
        client: ProcessWriterSink,
    ) -> FlushResult:
        with self._condition:
            self._raise_if_failed_locked()
            if client._closed or self._clients.get(client.venue) is not client:
                raise ProcessWriterError(f"process writer client for {client.venue!r} is closed")
            return self._completed[client.venue].take()

    def _client_request_flush(self, client: ProcessWriterSink) -> bool:
        with self._condition:
            self._require_active_locked(client)
            latest_frame_command_id = max(
                (
                    command_id
                    for command_id, pending in self._pending_commands.items()
                    if pending.kind == "frame"
                ),
                default=0,
            )
            if any(
                command_id > latest_frame_command_id and pending.kind == "barrier"
                for command_id, pending in self._pending_commands.items()
            ):
                return False
            if self._outstanding_rows == 0 and not any(self._duplicates_pending_by_venue.values()):
                return False

            command_id = self._next_command_id
            self._next_command_id += 1
            self._put_command_locked(
                (
                    command_id,
                    "barrier",
                    None,
                    None,
                    0,
                    self._monotonic_ns(),
                )
            )
            self._pending_commands[command_id] = _PendingCommand(
                kind="barrier",
                venue=client.venue,
                event=threading.Event(),
            )
            return True

    def _client_flush(self, client: ProcessWriterSink) -> FlushResult:
        event = threading.Event()
        with self._condition:
            self._require_active_locked(client)
            command_id = self._next_command_id
            self._next_command_id += 1
            self._put_command_locked(
                (
                    command_id,
                    "barrier",
                    None,
                    None,
                    0,
                    self._monotonic_ns(),
                )
            )
            self._pending_commands[command_id] = _PendingCommand(
                kind="barrier",
                venue=client.venue,
                event=event,
            )
        self._wait_for_event(event)
        with self._condition:
            self._raise_if_failed_locked()
            return self._completed[client.venue].take()

    def _close_client(self, client: ProcessWriterSink) -> None:
        with self._condition:
            self._raise_if_failed_locked()
            if client._closed:
                return
            if self._clients.get(client.venue) is not client:
                raise RuntimeError("process writer client does not belong to this writer")
            client._closed = True

    def _put_command_locked(self, command: tuple[object, ...]) -> None:
        try:
            self._command_queue.put_nowait(command)
        except queue.Full as exc:
            raise ProcessWriterError(
                "process writer IPC command capacity exhausted; no command was admitted"
            ) from exc
        except (BrokenPipeError, EOFError, OSError, ValueError) as exc:
            self._record_failure_locked(
                {
                    "phase": "command_enqueue",
                    "type": type(exc).__name__,
                    "message": str(exc),
                }
            )
            self._raise_if_failed_locked()

    def _listen(self) -> None:
        dead_empty_polls = 0
        while not self._listener_stop.is_set():
            try:
                event = self._result_queue.get(timeout=0.1)
            except queue.Empty:
                if self._process.exitcode is None:
                    dead_empty_polls = 0
                    continue
                with self._condition:
                    if self._stop_acknowledged:
                        return
                dead_empty_polls += 1
                if dead_empty_polls < 3:
                    continue
                with self._condition:
                    self._record_failure_locked(
                        {
                            "phase": "child_exit",
                            "type": "ChildProcessExit",
                            "message": (
                                "writer child exited without a terminal event; "
                                f"exitcode={self._process.exitcode}"
                            ),
                            "exitcode": self._process.exitcode,
                        }
                    )
                return
            except (EOFError, OSError, ValueError) as exc:
                if self._listener_stop.is_set():
                    return
                with self._condition:
                    self._record_failure_locked(
                        {
                            "phase": "result_transport",
                            "type": type(exc).__name__,
                            "message": str(exc) or "writer result transport closed",
                        }
                    )
                return

            dead_empty_polls = 0
            try:
                self._handle_child_event(event)
            except BaseException as exc:
                with self._condition:
                    self._record_failure_locked(
                        {
                            "phase": "protocol",
                            "type": type(exc).__name__,
                            "message": str(exc),
                            "traceback": "".join(
                                traceback.format_exception(
                                    type(exc),
                                    exc,
                                    exc.__traceback__,
                                )
                            ),
                        }
                    )
                self._force_stop_process()
                return

    def _handle_child_event(self, event: object) -> None:
        if (
            not isinstance(event, tuple)
            or len(event) != 3
            or not isinstance(event[0], int)
            or not isinstance(event[1], str)
            or not isinstance(event[2], dict)
        ):
            raise CoordinatedWriterError("process writer received an invalid child event envelope")
        event_sequence, kind, payload = event
        with self._condition:
            if event_sequence != self._expected_event_sequence:
                raise CoordinatedWriterError(
                    "process writer child event sequence discontinuity: "
                    f"expected={self._expected_event_sequence}, "
                    f"observed={event_sequence}"
                )
            self._expected_event_sequence += 1
            self._last_event_sequence = event_sequence
            phase_value = payload.get("phase")
            if phase_value is not None:
                if not isinstance(phase_value, str) or not phase_value:
                    raise CoordinatedWriterError("process writer child phase is invalid")
                self._child_phase = phase_value
            self._update_child_cache_locked(payload.get("snapshot"))

            if kind == "ready":
                if self._ready:
                    raise CoordinatedWriterError("process writer emitted duplicate readiness")
                self._ready = True
                self._ready_event.set()
            elif kind in {"phase", "heartbeat"}:
                pass
            elif kind == "frame":
                self._apply_frame_locked(payload)
            elif kind == "flush":
                self._apply_flush_locked(payload, expected_kind="barrier")
            elif kind == "stopped":
                self._apply_flush_locked(payload, expected_kind="stop")
                self._stop_acknowledged = True
                self._closed = True
                self._accepting = False
            elif kind == "failure":
                self._record_failure_locked(payload)
            else:
                raise CoordinatedWriterError(f"process writer received unknown child event kind {kind!r}")
            self._condition.notify_all()

    def _update_child_cache_locked(self, snapshot: object) -> None:
        if snapshot is None:
            return
        if not isinstance(snapshot, dict):
            raise CoordinatedWriterError("process writer child snapshot is not a dictionary")
        snapshot_phase = snapshot.get("phase")
        if snapshot_phase is not None:
            if not isinstance(snapshot_phase, str) or not snapshot_phase:
                raise CoordinatedWriterError("process writer child snapshot phase is invalid")
            self._child_phase = snapshot_phase
        self._child_cache = copy.deepcopy(snapshot)
        self._child_cache_received_ns = self._monotonic_ns()

    def _apply_frame_locked(self, payload: Mapping[str, object]) -> None:
        command_id = payload.get("command_id")
        venue = payload.get("venue")
        submitted = payload.get("submitted_rows")
        accepted = payload.get("accepted_rows")
        duplicates = payload.get("duplicate_rows")
        if (
            isinstance(command_id, bool)
            or not isinstance(command_id, int)
            or venue not in self._venue_set
            or isinstance(submitted, bool)
            or not isinstance(submitted, int)
            or isinstance(accepted, bool)
            or not isinstance(accepted, int)
            or isinstance(duplicates, bool)
            or not isinstance(duplicates, int)
            or accepted < 0
            or duplicates < 0
            or accepted + duplicates != submitted
        ):
            raise CoordinatedWriterError("process writer received an invalid frame acknowledgement")
        pending = self._pending_commands.get(command_id)
        if (
            pending is None
            or pending.kind != "frame"
            or pending.venue != venue
            or pending.row_count != submitted
        ):
            raise CoordinatedWriterError("process writer frame acknowledgement did not match admission")
        if self._queued_by_venue[str(venue)] < submitted:
            raise CoordinatedWriterError("process writer frame acknowledgement exceeds queued rows")

        del self._pending_commands[command_id]
        venue_key = str(venue)
        self._queued_by_venue[venue_key] -= submitted
        self._accepted_pending_by_venue[venue_key] += accepted
        self._duplicates_pending_by_venue[venue_key] += duplicates
        self._outstanding_rows -= duplicates
        self._frames_processed_by_venue[venue_key] += 1
        for timing_field, timings in (
            ("queue_residence_ns", self._queue_residence[venue_key]),
            ("add_duration_ns", self._add_timing[venue_key]),
            ("write_duration_ns", self._write_timing[venue_key]),
        ):
            value = payload.get(timing_field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise CoordinatedWriterError(f"process writer frame timing {timing_field!r} is invalid")
            timings.add(value / 1_000_000)
        self._check_accounting_locked()

    def _apply_flush_locked(
        self,
        payload: Mapping[str, object],
        *,
        expected_kind: str,
    ) -> None:
        command_id = payload.get("command_id")
        results_value = payload.get("results")
        duration_ns = payload.get("flush_duration_ns")
        if command_id is not None and (isinstance(command_id, bool) or not isinstance(command_id, int)):
            raise CoordinatedWriterError("process writer flush command id is invalid")
        if not isinstance(results_value, Mapping):
            raise CoordinatedWriterError("process writer flush results are not a mapping")
        if isinstance(duration_ns, bool) or not isinstance(duration_ns, int) or duration_ns < 0:
            raise CoordinatedWriterError("process writer flush duration is invalid")

        results: dict[str, FlushResult] = {}
        if frozenset(results_value) != self._venue_set:
            raise CoordinatedWriterError("process writer flush results contain incompatible venues")
        for venue in self._venues:
            result = results_value[venue]
            if not isinstance(result, FlushResult):
                raise CoordinatedWriterError("process writer flush result has an invalid type")
            if result.row_count != self._accepted_pending_by_venue[venue]:
                raise CoordinatedWriterError(
                    f"process writer durable-row accounting did not match accepted pending rows for {venue}"
                )
            if result.duplicate_count != self._duplicates_pending_by_venue[venue]:
                raise CoordinatedWriterError(
                    f"process writer duplicate accounting did not match pending duplicates for {venue}"
                )
            results[venue] = result

        pending: _PendingCommand | None = None
        if command_id is not None:
            pending = self._pending_commands.get(command_id)
            if pending is None or pending.kind != expected_kind:
                raise CoordinatedWriterError(
                    f"process writer flush acknowledgement did not match a pending {expected_kind} command"
                )

        for venue in self._venues:
            result = results[venue]
            self._accepted_pending_by_venue[venue] = 0
            self._duplicates_pending_by_venue[venue] = 0
            self._outstanding_rows -= result.row_count
            self._durable_rows_by_venue[venue] += result.row_count
            self._completed[venue].add(result)
        self._flush_timing.add(duration_ns / 1_000_000)
        self._check_accounting_locked()

        if command_id is not None:
            del self._pending_commands[command_id]
            assert pending is not None
            if pending.event is None:
                raise CoordinatedWriterError("process writer durable command is missing its waiter")
            pending.event.set()

    def _record_failure_locked(
        self,
        payload: Mapping[str, object],
        *,
        accounting_indeterminate: bool = True,
    ) -> None:
        if self._failure is None:
            self._failure = {
                "phase": str(payload.get("phase", "unknown")),
                "type": str(payload.get("type", "ProcessWriterFailure")),
                "message": str(payload.get("message", "")),
                "traceback": (None if payload.get("traceback") is None else str(payload.get("traceback"))),
                "exitcode": payload.get("exitcode"),
            }
        self._accepting = False
        if accounting_indeterminate:
            self._accounting_status = "indeterminate"
        for pending in self._pending_commands.values():
            if pending.event is not None:
                pending.event.set()
        self._ready_event.set()
        self._condition.notify_all()

    def _check_accounting_locked(self) -> None:
        expected = sum(self._queued_by_venue.values()) + sum(self._accepted_pending_by_venue.values())
        if expected != self._outstanding_rows:
            raise CoordinatedWriterError(
                "process writer outstanding-row accounting mismatch: "
                f"counter={self._outstanding_rows}, components={expected}"
            )
        if self._outstanding_rows < 0 or self._outstanding_rows > self.queue_capacity:
            raise CoordinatedWriterError(
                "process writer outstanding-row counter is outside its exact capacity"
            )
        for venue in self._venues:
            outstanding = self._queued_by_venue[venue] + self._accepted_pending_by_venue[venue]
            if outstanding < 0 or outstanding > self._venue_capacity_rows[venue]:
                raise CoordinatedWriterError(
                    f"process writer {venue!r} outstanding rows are outside their reserved capacity"
                )
            if self._duplicates_pending_by_venue[venue] < 0:
                raise CoordinatedWriterError(f"process writer {venue!r} duplicate credit is negative")

    def _force_stop_process(self) -> None:
        try:
            if self._process.is_alive():
                self._process.join(timeout=1)
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=1)
            if self._process.is_alive():
                self._process.kill()
                self._process.join(timeout=_PROCESS_JOIN_TIMEOUT_SECONDS)
        except (AssertionError, OSError, ValueError):
            pass

    def _cleanup_failed_process(self) -> None:
        self._force_stop_process()
        self._listener_stop.set()
        if self._listener.ident is not None:
            self._listener.join(timeout=5)
        self._close_queues()
        with self._condition:
            self._closed = True
            self._accepting = False

    def _close_queues(self) -> None:
        for transport in (self._command_queue, self._result_queue):
            with contextlib.suppress(AttributeError, OSError, ValueError):
                transport.cancel_join_thread()
            with contextlib.suppress(AttributeError, OSError, ValueError):
                transport.close()
