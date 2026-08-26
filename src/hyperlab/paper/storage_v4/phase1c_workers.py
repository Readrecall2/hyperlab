"""Fresh-process wrappers for offline Storage V4 Phase 1C runners.

The parent process owns orchestration only. Each child receives one frozen,
typed request, streams progress over a multiprocessing queue, and returns one
typed terminal result. The wrappers do not publish COMPLETE and do not remove
a partial candidate after interruption.
"""

from __future__ import annotations

import math
import multiprocessing
import pickle
import queue as queue_module
import time
import traceback
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Protocol, TypeVar, cast

from hyperlab.paper.golden_v3 import GoldenVerification

from .capacity import (
    CapacityMeasurement,
    CapacityWorkloadManifest,
    iter_capacity_commits,
)
from .capacity_runner import (
    CumulativeCapacityRunResult,
    OfflineCapacityRunEvidence,
    OfflinePhase1CCapacityRunner,
    current_process_cumulative_write_bytes,
)
from .golden_runner import GoldenNativeRunResult, OfflineGoldenNativeRunner
from .phase1c_progress import Phase1CHeartbeatWindow
from .raw_segment import RawSegmentThresholds
from .types import Hash32

ProgressCallback = Callable[[Mapping[str, object]], None]

_MIN_HEARTBEAT_SECONDS = 30.0
_MAX_HEARTBEAT_SECONDS = 60.0
_DEFAULT_PROCESS_POLL_SECONDS = 0.25


class Phase1CWorkerKind(StrEnum):
    GOLDEN_NATIVE = "GOLDEN_NATIVE"
    CAPACITY = "CAPACITY"
    CAPACITY_CUMULATIVE = "CAPACITY_CUMULATIVE"


class Phase1CWorkerErrorCode(StrEnum):
    INPUT_INVALID = "PHASE1C_WORKER_INPUT_INVALID"
    REMOTE_FAILURE = "PHASE1C_WORKER_REMOTE_FAILURE"
    PROCESS_EXIT_NONZERO = "PHASE1C_WORKER_PROCESS_EXIT_NONZERO"
    PROTOCOL_INVALID = "PHASE1C_WORKER_PROTOCOL_INVALID"


class Phase1CWorkerError(RuntimeError):
    """Stable fail-closed error raised by the isolated worker boundary."""

    def __init__(self, code: Phase1CWorkerErrorCode, message: str) -> None:
        if type(code) is not Phase1CWorkerErrorCode:
            raise TypeError("phase1c worker error code is invalid")
        self.code = code
        super().__init__(f"{code.value}: {message}")


@dataclass(frozen=True, slots=True)
class Phase1CWorkerFailure:
    """Picklable exception evidence returned by a failed child."""

    worker_kind: Phase1CWorkerKind
    exception_module: str
    exception_qualname: str
    message: str
    traceback_text: str

    @classmethod
    def capture(
        cls,
        worker_kind: Phase1CWorkerKind,
        error: BaseException,
    ) -> Phase1CWorkerFailure:
        return cls(
            worker_kind=worker_kind,
            exception_module=type(error).__module__,
            exception_qualname=type(error).__qualname__,
            message=str(error),
            traceback_text="".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            ),
        )


class Phase1CWorkerRemoteError(Phase1CWorkerError):
    """A child caught an exception and returned structured failure evidence."""

    def __init__(self, failure: Phase1CWorkerFailure, *, exit_code: int) -> None:
        if not isinstance(failure, Phase1CWorkerFailure):
            raise TypeError("failure must be Phase1CWorkerFailure")
        self.failure = failure
        self.exit_code = exit_code
        super().__init__(
            Phase1CWorkerErrorCode.REMOTE_FAILURE,
            (
                f"{failure.worker_kind.value} child raised "
                f"{failure.exception_module}.{failure.exception_qualname}: "
                f"{failure.message} (exit_code={exit_code})\n"
                f"remote traceback:\n{failure.traceback_text}"
            ),
        )


class Phase1CWorkerProcessError(Phase1CWorkerError):
    """A child exited unsuccessfully without a valid result."""

    def __init__(self, worker_kind: Phase1CWorkerKind, *, exit_code: int) -> None:
        self.worker_kind = worker_kind
        self.exit_code = exit_code
        super().__init__(
            Phase1CWorkerErrorCode.PROCESS_EXIT_NONZERO,
            f"{worker_kind.value} child exited with code {exit_code}",
        )


def _input_error(message: str) -> Phase1CWorkerError:
    return Phase1CWorkerError(Phase1CWorkerErrorCode.INPUT_INVALID, message)


def _validate_fresh_candidate(candidate_root: Path) -> None:
    if not isinstance(candidate_root, Path) or not candidate_root.is_absolute():
        raise _input_error("candidate_root must be an absolute pathlib.Path")
    if candidate_root.exists() or candidate_root.is_symlink():
        raise _input_error("candidate_root must not exist when the request is frozen")


def _validate_existing_candidate(candidate_root: Path) -> None:
    if not isinstance(candidate_root, Path) or not candidate_root.is_absolute():
        raise _input_error("candidate_root must be an absolute pathlib.Path")
    if candidate_root.is_symlink() or not candidate_root.is_dir():
        raise _input_error("resume candidate_root must be an existing regular directory")


def _positive_exact(value: int, *, label: str) -> None:
    if type(value) is not int or value < 1:
        raise _input_error(f"{label} must be a positive exact integer")


@dataclass(frozen=True, slots=True)
class GoldenNativeWorkerRequest:
    verification: GoldenVerification
    candidate_root: Path
    code_identity: Hash32
    runtime_identity: Hash32 | None = None
    batch_size: int = 12_000
    expected_commits: int = 252_262
    expected_rows: int = 1_011_362
    expected_streams: int = 13
    expected_market_gaps: int = 1

    def __post_init__(self) -> None:
        if type(self.verification) is not GoldenVerification:
            raise _input_error("verification must be a verified GoldenVerification")
        _validate_fresh_candidate(self.candidate_root)
        if type(self.code_identity) is not Hash32:
            raise _input_error("code_identity must be Hash32")
        if self.runtime_identity is not None and type(self.runtime_identity) is not Hash32:
            raise _input_error("runtime_identity must be Hash32 or None")
        for label, value in (
            ("batch_size", self.batch_size),
            ("expected_commits", self.expected_commits),
            ("expected_rows", self.expected_rows),
            ("expected_streams", self.expected_streams),
            ("expected_market_gaps", self.expected_market_gaps),
        ):
            _positive_exact(value, label=label)
        if self.batch_size > 50_000:
            raise _input_error("batch_size exceeds the Golden raw segment bound")


@dataclass(frozen=True, slots=True)
class Phase1CCapacityWorkerRequest:
    manifest: CapacityWorkloadManifest
    candidate_root: Path
    code_identity: Hash32
    runtime_identity: Hash32
    batch_size: int = 10_000
    checkpoint_every_batches: int = 1
    raw_thresholds: RawSegmentThresholds | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, CapacityWorkloadManifest):
            raise _input_error("manifest must be CapacityWorkloadManifest")
        _validate_fresh_candidate(self.candidate_root)
        if type(self.code_identity) is not Hash32:
            raise _input_error("code_identity must be Hash32")
        if type(self.runtime_identity) is not Hash32:
            raise _input_error("runtime_identity must be Hash32")
        _positive_exact(self.batch_size, label="batch_size")
        _positive_exact(
            self.checkpoint_every_batches,
            label="checkpoint_every_batches",
        )
        if self.batch_size > 10_000:
            raise _input_error("batch_size exceeds the synthetic adapter bound")
        if (
            self.raw_thresholds is not None
            and type(self.raw_thresholds) is not RawSegmentThresholds
        ):
            raise _input_error("raw_thresholds must be RawSegmentThresholds or None")


@dataclass(frozen=True, slots=True)
class Phase1CCumulativeCapacityWorkerRequest:
    manifests: tuple[CapacityWorkloadManifest, ...]
    candidate_root: Path
    code_identity: Hash32
    runtime_identity: Hash32
    batch_size: int = 10_000
    raw_thresholds: RawSegmentThresholds | None = None
    resume_existing: bool = False

    def __post_init__(self) -> None:
        if (
            type(self.manifests) is not tuple
            or len(self.manifests) < 2
            or any(
                not isinstance(manifest, CapacityWorkloadManifest)
                for manifest in self.manifests
            )
        ):
            raise _input_error(
                "manifests must be an exact tuple of cumulative capacity boundaries"
            )
        counts = tuple(manifest.commit_count for manifest in self.manifests)
        if counts != tuple(sorted(set(counts))):
            raise _input_error("cumulative manifest counts must be strictly increasing")
        terminal = self.manifests[-1]
        if any(
            replace(
                manifest.config,
                commit_count=terminal.config.commit_count,
                market_gap_count=terminal.config.market_gap_count,
            )
            != terminal.config
            for manifest in self.manifests[:-1]
        ):
            raise _input_error(
                "cumulative manifests differ outside exact prefix counts"
            )
        if type(self.resume_existing) is not bool:
            raise _input_error("resume_existing must be an exact bool")
        if self.resume_existing:
            _validate_existing_candidate(self.candidate_root)
        else:
            _validate_fresh_candidate(self.candidate_root)
        if type(self.code_identity) is not Hash32:
            raise _input_error("code_identity must be Hash32")
        if type(self.runtime_identity) is not Hash32:
            raise _input_error("runtime_identity must be Hash32")
        _positive_exact(self.batch_size, label="batch_size")
        if self.batch_size > 10_000:
            raise _input_error("batch_size exceeds the synthetic adapter bound")
        if (
            self.raw_thresholds is not None
            and type(self.raw_thresholds) is not RawSegmentThresholds
        ):
            raise _input_error("raw_thresholds must be RawSegmentThresholds or None")


class _MessageKind(StrEnum):
    PROGRESS = "PROGRESS"
    RESULT = "RESULT"
    ERROR = "ERROR"


@dataclass(frozen=True, slots=True)
class _WorkerMessage:
    kind: _MessageKind
    worker_kind: Phase1CWorkerKind
    payload: object


class _QueueLike(Protocol):
    def put(self, value: object) -> None: ...

    def get(self, block: bool = True, timeout: float | None = None) -> object: ...

    def get_nowait(self) -> object: ...

    def close(self) -> None: ...

    def join_thread(self) -> None: ...


class _ProcessLike(Protocol):
    pid: int | None
    exitcode: int | None

    def start(self) -> None: ...

    def is_alive(self) -> bool: ...

    def terminate(self) -> None: ...

    def join(self, timeout: float | None = None) -> None: ...

    def close(self) -> None: ...


class _SpawnContext(Protocol):
    def Queue(self) -> _QueueLike: ...

    def Process(
        self,
        *,
        target: Callable[..., object],
        args: tuple[object, ...],
        daemon: bool,
        name: str,
    ) -> _ProcessLike: ...


def _send(queue: _QueueLike, message: _WorkerMessage) -> None:
    # Queue feeder threads can otherwise report a pickle failure asynchronously
    # after put has returned. Preflight serialization makes that failure a
    # normal child exception that can itself be returned structurally.
    pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
    queue.put(message)


def _send_progress(
    queue: _QueueLike,
    worker_kind: Phase1CWorkerKind,
    payload: Mapping[str, object],
) -> None:
    _send(
        queue,
        _WorkerMessage(
            kind=_MessageKind.PROGRESS,
            worker_kind=worker_kind,
            payload=dict(payload),
        ),
    )


def _run_child(
    worker_kind: Phase1CWorkerKind,
    queue: _QueueLike,
    operation: Callable[[], object],
) -> None:
    try:
        result = operation()
        _send(
            queue,
            _WorkerMessage(
                kind=_MessageKind.RESULT,
                worker_kind=worker_kind,
                payload=result,
            ),
        )
    except BaseException as error:
        _send(
            queue,
            _WorkerMessage(
                kind=_MessageKind.ERROR,
                worker_kind=worker_kind,
                payload=Phase1CWorkerFailure.capture(worker_kind, error),
            ),
        )
    finally:
        # Waiting for the feeder guarantees that the terminal envelope is in
        # the pipe before the process exits. The parent drains while the child
        # is alive, avoiding the documented Queue/join deadlock.
        queue.close()
        queue.join_thread()


def _golden_worker_entry(
    request: GoldenNativeWorkerRequest,
    queue: _QueueLike,
) -> None:
    def operation() -> GoldenNativeRunResult:
        runner = OfflineGoldenNativeRunner(
            candidate_root=request.candidate_root,
            code_identity=request.code_identity,
            runtime_identity=request.runtime_identity,
            batch_size=request.batch_size,
            expected_commits=request.expected_commits,
            expected_rows=request.expected_rows,
            expected_streams=request.expected_streams,
            expected_market_gaps=request.expected_market_gaps,
            write_bytes_probe=current_process_cumulative_write_bytes,
            progress=lambda payload: _send_progress(
                queue,
                Phase1CWorkerKind.GOLDEN_NATIVE,
                payload,
            ),
        )
        return runner.run(request.verification)

    _run_child(Phase1CWorkerKind.GOLDEN_NATIVE, queue, operation)


def _capacity_worker_entry(
    request: Phase1CCapacityWorkerRequest,
    queue: _QueueLike,
) -> None:
    def operation() -> tuple[CapacityMeasurement, OfflineCapacityRunEvidence]:
        runner = OfflinePhase1CCapacityRunner(
            candidate_root=request.candidate_root,
            code_identity=request.code_identity,
            runtime_identity=request.runtime_identity,
            batch_size=request.batch_size,
            checkpoint_every_batches=request.checkpoint_every_batches,
            raw_thresholds=request.raw_thresholds,
            write_bytes_probe=current_process_cumulative_write_bytes,
            progress=lambda payload: _send_progress(
                queue,
                Phase1CWorkerKind.CAPACITY,
                payload,
            ),
        )
        measurement = runner.run_capacity_workload(
            manifest=request.manifest,
            commits=iter_capacity_commits(request.manifest.config),
        )
        evidence = runner.last_evidence
        if evidence is None:
            raise Phase1CWorkerError(
                Phase1CWorkerErrorCode.PROTOCOL_INVALID,
                "capacity runner completed without terminal evidence",
            )
        return measurement, evidence

    _run_child(Phase1CWorkerKind.CAPACITY, queue, operation)


def _cumulative_capacity_worker_entry(
    request: Phase1CCumulativeCapacityWorkerRequest,
    queue: _QueueLike,
) -> None:
    def operation() -> CumulativeCapacityRunResult:
        runner = OfflinePhase1CCapacityRunner(
            candidate_root=request.candidate_root,
            code_identity=request.code_identity,
            runtime_identity=request.runtime_identity,
            batch_size=request.batch_size,
            checkpoint_every_batches=1,
            raw_thresholds=request.raw_thresholds,
            write_bytes_probe=current_process_cumulative_write_bytes,
            progress=lambda payload: _send_progress(
                queue,
                Phase1CWorkerKind.CAPACITY_CUMULATIVE,
                payload,
            ),
        )
        if request.resume_existing:
            return runner.resume_cumulative_capacity_workload(
                manifests=request.manifests,
            )
        terminal = request.manifests[-1]
        return runner.run_cumulative_capacity_workload(
            manifests=request.manifests,
            commits=iter_capacity_commits(terminal.config),
        )

    _run_child(Phase1CWorkerKind.CAPACITY_CUMULATIVE, queue, operation)


ResultT = TypeVar("ResultT")
RequestT = TypeVar("RequestT")


def _validate_runtime_options(
    *,
    progress: ProgressCallback | None,
    heartbeat_interval_seconds: float,
    process_poll_seconds: float,
) -> None:
    if progress is not None and not callable(progress):
        raise _input_error("progress must be callable or None")
    if (
        type(heartbeat_interval_seconds) not in (int, float)
        or not math.isfinite(float(heartbeat_interval_seconds))
        or not _MIN_HEARTBEAT_SECONDS
        <= float(heartbeat_interval_seconds)
        <= _MAX_HEARTBEAT_SECONDS
    ):
        raise _input_error("heartbeat_interval_seconds must be between 30 and 60")
    if (
        type(process_poll_seconds) not in (int, float)
        or not math.isfinite(float(process_poll_seconds))
        or not 0 < float(process_poll_seconds) <= float(heartbeat_interval_seconds)
    ):
        raise _input_error("private process poll interval is invalid")


def _heartbeat_payload(
    *,
    candidate_root: Path,
    child_pid: int | None,
    elapsed_ns: int,
    last_progress: Mapping[str, object] | None,
    worker_kind: Phase1CWorkerKind,
    window: Phase1CHeartbeatWindow,
) -> dict[str, object]:
    normalized = window.render(
        last_progress,
        observed_elapsed_ns=elapsed_ns,
    )
    candidate_phase = None if last_progress is None else last_progress.get("phase")
    active_phase = (
        candidate_phase
        if type(candidate_phase) is str and candidate_phase
        else "phase1c_isolated_worker"
    )
    return {
        **normalized,
        "candidate_root": str(candidate_root),
        "child_pid": child_pid,
        "descendant_process_visibility_scope": (
            "LAST_CHILD_SELF_REPORTED_PROGRESS_SNAPSHOT_ONLY; "
            "DESCENDANT_PROCESS_TREE_NOT_DIRECTLY_OBSERVED"
            if last_progress is not None
            else "DESCENDANT_PROCESS_NOT_YET_SELF_REPORTED; "
            "DESCENDANT_PROCESS_TREE_NOT_DIRECTLY_OBSERVED"
        ),
        "elapsed_ns": elapsed_ns,
        "event": "heartbeat",
        "heartbeat_scope": "PHASE1C_ISOLATED_WORKER",
        "last_progress": None if last_progress is None else dict(last_progress),
        "phase": active_phase,
        "status": "RUNNING",
        "worker_kind": worker_kind.value,
    }


def _run_isolated_worker(
    request: RequestT,
    *,
    candidate_root: Path,
    worker_kind: Phase1CWorkerKind,
    target: Callable[[RequestT, _QueueLike], None],
    result_parser: Callable[[object], ResultT],
    progress: ProgressCallback | None,
    heartbeat_interval_seconds: float,
    context: _SpawnContext | None,
    monotonic_ns: Callable[[], int],
    process_poll_seconds: float,
) -> ResultT:
    _validate_runtime_options(
        progress=progress,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        process_poll_seconds=process_poll_seconds,
    )
    if not callable(monotonic_ns):
        raise _input_error("private monotonic clock must be callable")
    active_context = context or cast(
        _SpawnContext,
        multiprocessing.get_context("spawn"),
    )
    queue = active_context.Queue()
    process = active_context.Process(
        target=target,
        args=(request, queue),
        daemon=False,
        name=f"hyperlab-phase1c-{worker_kind.value.lower()}",
    )
    started = False
    terminal: _WorkerMessage | None = None
    protocol_failure: str | None = None
    last_progress: dict[str, object] | None = None
    started_ns = monotonic_ns()
    heartbeat_interval_ns = int(float(heartbeat_interval_seconds) * 1_000_000_000)
    next_heartbeat_ns = started_ns + heartbeat_interval_ns
    heartbeat_window = Phase1CHeartbeatWindow()

    def consume(value: object) -> None:
        nonlocal last_progress, protocol_failure, terminal
        if not isinstance(value, _WorkerMessage):
            protocol_failure = "child returned a non-envelope queue value"
            return
        if value.worker_kind is not worker_kind:
            protocol_failure = "child returned an envelope for the wrong worker kind"
            return
        if value.kind is _MessageKind.PROGRESS:
            if terminal is not None:
                protocol_failure = "child returned progress after a terminal envelope"
                return
            if not isinstance(value.payload, dict) or any(
                not isinstance(key, str) for key in value.payload
            ):
                protocol_failure = "child returned an invalid progress payload"
                return
            last_progress = dict(value.payload)
            if progress is not None:
                progress(last_progress)
            return
        if value.kind not in {_MessageKind.RESULT, _MessageKind.ERROR}:
            protocol_failure = "child returned an unknown envelope kind"
            return
        if terminal is not None:
            protocol_failure = "child returned more than one terminal envelope"
            return
        terminal = value

    try:
        process.start()
        started = True
        while process.is_alive():
            now_ns = monotonic_ns()
            until_heartbeat_seconds = max(
                0.0,
                (next_heartbeat_ns - now_ns) / 1_000_000_000,
            )
            wait_seconds = min(float(process_poll_seconds), until_heartbeat_seconds)
            with suppress(queue_module.Empty):
                consume(queue.get(timeout=wait_seconds))
            now_ns = monotonic_ns()
            if now_ns >= next_heartbeat_ns and process.is_alive():
                if progress is not None:
                    progress(
                        _heartbeat_payload(
                            candidate_root=candidate_root,
                            child_pid=process.pid,
                            elapsed_ns=max(0, now_ns - started_ns),
                            last_progress=last_progress,
                            worker_kind=worker_kind,
                            window=heartbeat_window,
                        )
                    )
                periods = ((now_ns - next_heartbeat_ns) // heartbeat_interval_ns) + 1
                next_heartbeat_ns += periods * heartbeat_interval_ns

        # The child closes and joins its feeder before exit. Only after it is
        # observed dead is a nonblocking drain complete and trustworthy.
        while True:
            try:
                consume(queue.get_nowait())
            except queue_module.Empty:
                break
        process.join()
        exit_code = process.exitcode
        if exit_code is None:
            raise Phase1CWorkerError(
                Phase1CWorkerErrorCode.PROTOCOL_INVALID,
                f"{worker_kind.value} child has no exit code after join",
            )
        if protocol_failure is not None:
            raise Phase1CWorkerError(
                Phase1CWorkerErrorCode.PROTOCOL_INVALID,
                f"{worker_kind.value} queue protocol failed: {protocol_failure}",
            )
        if terminal is not None and terminal.kind is _MessageKind.ERROR:
            if not isinstance(terminal.payload, Phase1CWorkerFailure):
                raise Phase1CWorkerError(
                    Phase1CWorkerErrorCode.PROTOCOL_INVALID,
                    f"{worker_kind.value} child returned malformed failure evidence",
                )
            raise Phase1CWorkerRemoteError(terminal.payload, exit_code=exit_code)
        if exit_code != 0:
            raise Phase1CWorkerProcessError(worker_kind, exit_code=exit_code)
        if terminal is None or terminal.kind is not _MessageKind.RESULT:
            raise Phase1CWorkerError(
                Phase1CWorkerErrorCode.PROTOCOL_INVALID,
                f"{worker_kind.value} child exited without a result envelope",
            )
        return result_parser(terminal.payload)
    except KeyboardInterrupt:
        if started and process.is_alive():
            process.terminate()
        if started:
            process.join()
        raise
    except BaseException:
        if started and process.is_alive():
            process.terminate()
            process.join()
        raise
    finally:
        queue.close()
        queue.join_thread()
        if started and not process.is_alive():
            process.close()


def _parse_golden_result(value: object) -> GoldenNativeRunResult:
    if not isinstance(value, GoldenNativeRunResult):
        raise Phase1CWorkerError(
            Phase1CWorkerErrorCode.PROTOCOL_INVALID,
            "GOLDEN_NATIVE child returned a result of the wrong type",
        )
    return value


def _parse_capacity_result(
    value: object,
) -> tuple[CapacityMeasurement, OfflineCapacityRunEvidence]:
    if not (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[0], CapacityMeasurement)
        and isinstance(value[1], OfflineCapacityRunEvidence)
    ):
        raise Phase1CWorkerError(
            Phase1CWorkerErrorCode.PROTOCOL_INVALID,
            "CAPACITY child returned a result of the wrong type",
        )
    return value


def _parse_cumulative_capacity_result(value: object) -> CumulativeCapacityRunResult:
    if not isinstance(value, CumulativeCapacityRunResult):
        raise Phase1CWorkerError(
            Phase1CWorkerErrorCode.PROTOCOL_INVALID,
            "CAPACITY_CUMULATIVE child returned a result of the wrong type",
        )
    return value


def run_golden_native_worker(
    request: GoldenNativeWorkerRequest,
    *,
    progress: ProgressCallback | None = None,
    heartbeat_interval_seconds: float = 30.0,
    _context: _SpawnContext | None = None,
    _target: Callable[
        [GoldenNativeWorkerRequest, _QueueLike],
        None,
    ] = _golden_worker_entry,
    _monotonic_ns: Callable[[], int] = time.monotonic_ns,
    _process_poll_seconds: float = _DEFAULT_PROCESS_POLL_SECONDS,
) -> GoldenNativeRunResult:
    """Run one verified Golden request in a fresh spawn process."""

    if not isinstance(request, GoldenNativeWorkerRequest):
        raise _input_error("request must be GoldenNativeWorkerRequest")
    return _run_isolated_worker(
        request,
        candidate_root=request.candidate_root,
        worker_kind=Phase1CWorkerKind.GOLDEN_NATIVE,
        target=_target,
        result_parser=_parse_golden_result,
        progress=progress,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        context=_context,
        monotonic_ns=_monotonic_ns,
        process_poll_seconds=_process_poll_seconds,
    )


def run_phase1c_capacity_worker(
    request: Phase1CCapacityWorkerRequest,
    *,
    progress: ProgressCallback | None = None,
    heartbeat_interval_seconds: float = 30.0,
    _context: _SpawnContext | None = None,
    _target: Callable[
        [Phase1CCapacityWorkerRequest, _QueueLike],
        None,
    ] = _capacity_worker_entry,
    _monotonic_ns: Callable[[], int] = time.monotonic_ns,
    _process_poll_seconds: float = _DEFAULT_PROCESS_POLL_SECONDS,
) -> tuple[CapacityMeasurement, OfflineCapacityRunEvidence]:
    """Run one frozen capacity manifest in a fresh spawn process."""

    if not isinstance(request, Phase1CCapacityWorkerRequest):
        raise _input_error("request must be Phase1CCapacityWorkerRequest")
    return _run_isolated_worker(
        request,
        candidate_root=request.candidate_root,
        worker_kind=Phase1CWorkerKind.CAPACITY,
        target=_target,
        result_parser=_parse_capacity_result,
        progress=progress,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        context=_context,
        monotonic_ns=_monotonic_ns,
        process_poll_seconds=_process_poll_seconds,
    )


def run_phase1c_cumulative_capacity_worker(
    request: Phase1CCumulativeCapacityWorkerRequest,
    *,
    progress: ProgressCallback | None = None,
    heartbeat_interval_seconds: float = 30.0,
    _context: _SpawnContext | None = None,
    _target: Callable[
        [Phase1CCumulativeCapacityWorkerRequest, _QueueLike],
        None,
    ] = _cumulative_capacity_worker_entry,
    _monotonic_ns: Callable[[], int] = time.monotonic_ns,
    _process_poll_seconds: float = _DEFAULT_PROCESS_POLL_SECONDS,
) -> CumulativeCapacityRunResult:
    """Run or resume one cumulative terminal stream in one isolated child."""

    if not isinstance(request, Phase1CCumulativeCapacityWorkerRequest):
        raise _input_error(
            "request must be Phase1CCumulativeCapacityWorkerRequest"
        )
    return _run_isolated_worker(
        request,
        candidate_root=request.candidate_root,
        worker_kind=Phase1CWorkerKind.CAPACITY_CUMULATIVE,
        target=_target,
        result_parser=_parse_cumulative_capacity_result,
        progress=progress,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        context=_context,
        monotonic_ns=_monotonic_ns,
        process_poll_seconds=_process_poll_seconds,
    )


__all__ = [
    "GoldenNativeWorkerRequest",
    "Phase1CCapacityWorkerRequest",
    "Phase1CCumulativeCapacityWorkerRequest",
    "Phase1CWorkerError",
    "Phase1CWorkerErrorCode",
    "Phase1CWorkerFailure",
    "Phase1CWorkerKind",
    "Phase1CWorkerProcessError",
    "Phase1CWorkerRemoteError",
    "ProgressCallback",
    "run_golden_native_worker",
    "run_phase1c_capacity_worker",
    "run_phase1c_cumulative_capacity_worker",
]
