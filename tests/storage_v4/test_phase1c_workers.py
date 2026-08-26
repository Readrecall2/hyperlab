from __future__ import annotations

import hashlib
import os
import queue as queue_module
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from multiprocessing import get_context
from pathlib import Path

import pytest

from hyperlab.paper.golden_v3 import GoldenVerification
from hyperlab.paper.storage_v4.capacity import (
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
    CapacityWorkloadHasher,
    CapacityWorkloadManifest,
    SyntheticCapacityCommit,
    build_capacity_workload_manifest,
    iter_capacity_commits,
)
from hyperlab.paper.storage_v4.capacity_runner import OfflinePhase1CCapacityRunner
from hyperlab.paper.storage_v4.faults import FaultPoint
from hyperlab.paper.storage_v4.golden_runner import GoldenNativeRunResult
from hyperlab.paper.storage_v4.phase1c_progress import Phase1CHeartbeatWindow
from hyperlab.paper.storage_v4.phase1c_workers import (
    GoldenNativeWorkerRequest,
    Phase1CCapacityWorkerRequest,
    Phase1CCumulativeCapacityWorkerRequest,
    Phase1CWorkerError,
    Phase1CWorkerErrorCode,
    Phase1CWorkerFailure,
    Phase1CWorkerKind,
    Phase1CWorkerProcessError,
    Phase1CWorkerRemoteError,
    _heartbeat_payload,
    _MessageKind,
    _QueueLike,
    _WorkerMessage,
    run_golden_native_worker,
    run_phase1c_capacity_worker,
    run_phase1c_cumulative_capacity_worker,
)
from hyperlab.paper.storage_v4.types import Hash32

SYNTHETIC_STORAGE_V4_WORKLOAD = True


def _config(*, commit_count: int = 1) -> CapacityWorkloadConfig:
    return CapacityWorkloadConfig(
        profile=CapacityProfile.GOLDEN_SHAPED,
        seed=731,
        commit_count=commit_count,
        start_time_ns=1_700_000_000_000_000_000,
        cadence_ns=250_000_000,
        type_distribution=(
            CapacityTypeSpec(
                record_type="PUBLIC_BBO",
                stream="inbox",
                weight=1,
                payload_min_bytes=8,
                payload_max_bytes=16,
                payload_cardinality=2,
            ),
        ),
        strategies=("phase05_cash_and_carry", "phase08_cross_venue"),
        alert_every_commits=None,
        incident_every_commits=None,
        ledger_every_commits=None,
        market_gap_count=1,
        alert_payload_bytes=5,
        incident_payload_bytes=6,
        ledger_payload_bytes=7,
        market_gap_payload_bytes=8,
        golden_census_sha256="a" * 64,
    )


def _capacity_request(
    tmp_path: Path,
    *,
    name: str = "candidate",
) -> Phase1CCapacityWorkerRequest:
    return Phase1CCapacityWorkerRequest(
        manifest=build_capacity_workload_manifest(_config()),
        candidate_root=(tmp_path / name).absolute(),
        code_identity=Hash32(b"\x91" * 32),
        runtime_identity=Hash32(b"\x92" * 32),
        batch_size=1,
        checkpoint_every_batches=1,
    )


def _cumulative_manifests(
    levels: tuple[int, ...] = (2, 4, 6),
) -> tuple[CapacityWorkloadManifest, ...]:
    terminal = replace(
        _config(commit_count=levels[-1]),
        market_gap_count=0,
    )
    configs = {level: replace(terminal, commit_count=level) for level in levels}
    hasher = CapacityWorkloadHasher()
    manifests: list[CapacityWorkloadManifest] = []
    for commit in iter_capacity_commits(terminal):
        hasher.update(commit)
        if commit.sequence in configs:
            manifests.append(
                CapacityWorkloadManifest(
                    config=configs[commit.sequence],
                    digest=hasher.snapshot(),
                )
            )
    return tuple(manifests)


def _crash_after_raw_suffix(
    candidate_root: Path,
    manifests: tuple[CapacityWorkloadManifest, ...],
    crash_occurrence: int = 3,
) -> None:
    raw_before_paper = 0

    def fault_hook(point: FaultPoint, /) -> None:
        nonlocal raw_before_paper
        if point is not FaultPoint.AFTER_RAW_BEFORE_PAPER_APPEND:
            return
        raw_before_paper += 1
        if raw_before_paper == crash_occurrence:
            os._exit(73)

    runner = OfflinePhase1CCapacityRunner(
        candidate_root=candidate_root,
        code_identity=Hash32(b"\x91" * 32),
        runtime_identity=Hash32(b"\x92" * 32),
        batch_size=2,
        checkpoint_every_batches=1,
        rss_probe=lambda: None,
        fault_hook=fault_hook,
    )
    runner.run_cumulative_capacity_workload(
        manifests=manifests,
        commits=iter_capacity_commits(manifests[-1].config),
    )


def _golden_request(tmp_path: Path) -> GoldenNativeWorkerRequest:
    verification = GoldenVerification(
        export_root=(tmp_path / "verified-export").absolute(),
        root_hash="9" * 64,
        manifest={"run_id": "SYNTHETIC/phase1c-worker", "streams": {}},
    )
    return GoldenNativeWorkerRequest(
        verification=verification,
        candidate_root=(tmp_path / "golden-candidate").absolute(),
        code_identity=Hash32(b"\x91" * 32),
        batch_size=1,
        expected_commits=1,
        expected_rows=1,
        expected_streams=1,
        expected_market_gaps=1,
    )


def test_capacity_worker_real_spawn_roundtrip_is_exact_and_forwards_progress(
    tmp_path: Path,
) -> None:
    request = _capacity_request(tmp_path)
    progress: list[dict[str, object]] = []

    measurement, evidence = run_phase1c_capacity_worker(
        request,
        progress=lambda payload: progress.append(dict(payload)),
    )

    assert measurement.commit_count == 1
    assert measurement.workload_manifest_sha256 == request.manifest.sha256
    assert evidence.oracle.commit_count == 1
    assert evidence.audited_candidate_tree.root == request.candidate_root
    integrity = evidence.payload()["integrity"]
    assert isinstance(integrity, dict)
    assert integrity["audited_candidate_tree"] == (
        evidence.audited_candidate_tree.payload()
    )
    assert request.candidate_root.is_dir()
    assert not (request.candidate_root / "COMPLETE").exists()
    assert progress[-1]["phase"] == "capacity_complete"
    if os.name == "nt":
        assert measurement.cumulative_bytes_written is not None


def test_cumulative_capacity_worker_runs_one_terminal_stream_in_one_spawn(
    tmp_path: Path,
) -> None:
    manifests = _cumulative_manifests((2, 4))
    request = Phase1CCumulativeCapacityWorkerRequest(
        manifests=manifests,
        candidate_root=(tmp_path / "cumulative-worker").absolute(),
        code_identity=Hash32(b"\x91" * 32),
        runtime_identity=Hash32(b"\x92" * 32),
        batch_size=2,
    )
    progress: list[dict[str, object]] = []

    result = run_phase1c_cumulative_capacity_worker(
        request,
        progress=lambda payload: progress.append(dict(payload)),
    )

    assert [item.commit_count for item in result.boundaries] == [2, 4]
    assert [item.manifest.commit_count for item in result.typed_boundaries] == [2, 4]
    assert result.accounting.commits_generated == 4
    assert result.accounting.commits_ingested == 4
    assert result.accounting.prefix_commits_reingested == 0
    assert result.accounting.worker_count == 1
    assert result.accounting.store_count == 1
    assert result.accounting.stream_count == 1
    assert progress[-1]["phase"] == "capacity_complete"
    assert progress[-1]["commits_ingested"] == 4


def test_real_process_cut_reuses_raw_suffix_without_prefix_reingestion(
    tmp_path: Path,
) -> None:
    manifests = _cumulative_manifests((4, 8))
    interrupted_root = (tmp_path / "interrupted-cumulative").absolute()
    process = get_context("spawn").Process(
        target=_crash_after_raw_suffix,
        args=(interrupted_root, manifests),
        daemon=False,
    )
    process.start()
    process.join(60)
    if process.is_alive():
        process.terminate()
        process.join()
    assert process.exitcode == 73
    process.close()

    certificate_directory = (
        interrupted_root.parent / f".{interrupted_root.name}.phase1c-boundaries"
    )
    first_certificate = next(certificate_directory.glob("0000000000000004-*.json"))
    first_certificate_bytes = first_certificate.read_bytes()
    resumed_emissions: list[int] = []

    def prefix_bomb_factory(
        manifest: CapacityWorkloadManifest,
        start_sequence: int,
        stream_sequences: Mapping[str, int],
    ) -> Iterator[SyntheticCapacityCommit]:
        assert start_sequence == 5
        for commit in iter_capacity_commits(
            manifest.config,
            start_sequence=start_sequence,
            initial_stream_sequences=stream_sequences,
        ):
            if commit.sequence < start_sequence:
                raise AssertionError("certified prefix was emitted again")
            resumed_emissions.append(commit.sequence)
            yield commit

    resumed_runner = OfflinePhase1CCapacityRunner(
        candidate_root=interrupted_root,
        code_identity=Hash32(b"\x91" * 32),
        runtime_identity=Hash32(b"\x92" * 32),
        batch_size=2,
        checkpoint_every_batches=1,
        rss_probe=lambda: None,
    )
    resumed = resumed_runner.resume_cumulative_capacity_workload(
        manifests=manifests,
        commit_factory=prefix_bomb_factory,
    )

    continuous_runner = OfflinePhase1CCapacityRunner(
        candidate_root=(tmp_path / "continuous-cumulative").absolute(),
        code_identity=Hash32(b"\x91" * 32),
        runtime_identity=Hash32(b"\x92" * 32),
        batch_size=2,
        checkpoint_every_batches=1,
        rss_probe=lambda: None,
    )
    continuous = continuous_runner.run_cumulative_capacity_workload(
        manifests=manifests,
        commits=iter_capacity_commits(manifests[-1].config),
    )

    assert resumed_emissions == [5, 6, 7, 8]
    assert first_certificate.read_bytes() == first_certificate_bytes
    assert [item.commit_count for item in resumed.boundaries] == [4, 8]
    assert [item.manifest.commit_count for item in resumed.typed_boundaries] == [8]
    recovered_boundary = resumed.boundaries[0]
    assert recovered_boundary.path == first_certificate
    assert recovered_boundary.path.read_bytes() == recovered_boundary.canonical_payload
    assert recovered_boundary.canonical_payload == first_certificate_bytes
    assert recovered_boundary.sha256 == hashlib.sha256(first_certificate_bytes).hexdigest()
    assert recovered_boundary.payload_mapping["measurement"] == (
        recovered_boundary.measurement_mapping
    )
    assert recovered_boundary.payload_mapping["evidence"] == (
        recovered_boundary.evidence_mapping
    )
    assert recovered_boundary.typed_measurement is None
    assert recovered_boundary.typed_evidence is None
    assert resumed.accounting.commits_generated == 8
    assert resumed.accounting.commits_ingested == 8
    assert resumed.accounting.raw_commits_reused == 2
    assert resumed.accounting.prefix_commits_reingested == 0
    assert resumed.accounting.prefix_commits_audited == 4
    assert resumed.accounting.payload()["generator_emissions"] == 10
    resumed_terminal = resumed.typed_boundaries[-1]
    continuous_terminal = continuous.typed_boundaries[-1]
    assert resumed_terminal.raw_manifest_root == continuous_terminal.raw_manifest_root
    assert resumed_terminal.paper_manifest_root == continuous_terminal.paper_manifest_root
    assert resumed_terminal.checkpoint_root == continuous_terminal.checkpoint_root
    assert (
        resumed_terminal.evidence.certification.native_audit.final_prefix_root
        == continuous_terminal.evidence.certification.native_audit.final_prefix_root
    )
    assert (
        resumed_terminal.evidence.certification.native_audit.raw_reference_prefix_root
        == continuous_terminal.evidence.certification.native_audit.raw_reference_prefix_root
    )


def test_cumulative_worker_resumes_existing_raw_suffix_in_a_new_spawn(
    tmp_path: Path,
) -> None:
    manifests = _cumulative_manifests((2, 4))
    candidate_root = (tmp_path / "worker-resume-cumulative").absolute()
    interrupted = get_context("spawn").Process(
        target=_crash_after_raw_suffix,
        args=(candidate_root, manifests, 2),
        daemon=False,
    )
    interrupted.start()
    interrupted.join(60)
    if interrupted.is_alive():
        interrupted.terminate()
        interrupted.join()
    assert interrupted.exitcode == 73
    interrupted.close()

    request = Phase1CCumulativeCapacityWorkerRequest(
        manifests=manifests,
        candidate_root=candidate_root,
        code_identity=Hash32(b"\x91" * 32),
        runtime_identity=Hash32(b"\x92" * 32),
        batch_size=2,
        resume_existing=True,
    )
    progress: list[dict[str, object]] = []

    result = run_phase1c_cumulative_capacity_worker(
        request,
        progress=lambda payload: progress.append(dict(payload)),
    )

    assert [item.commit_count for item in result.boundaries] == [2, 4]
    assert [item.manifest.commit_count for item in result.typed_boundaries] == [4]
    assert result.accounting.commits_generated == 4
    assert result.accounting.commits_ingested == 4
    assert result.accounting.raw_commits_reused == 2
    assert result.accounting.prefix_commits_audited == 2
    assert result.accounting.prefix_commits_reingested == 0
    assert result.accounting.resume_count == 1
    assert result.accounting.worker_count == 1
    assert progress[-1]["phase"] == "capacity_complete"
    assert progress[-1]["raw_commits_reused"] == 2


def test_capacity_worker_returns_structured_child_failure_and_preserves_candidate(
    tmp_path: Path,
) -> None:
    request = _capacity_request(tmp_path, name="raced-candidate")
    request.candidate_root.mkdir()
    sentinel = request.candidate_root / "user.txt"
    sentinel.write_text("preserve", encoding="utf-8")

    with pytest.raises(Phase1CWorkerRemoteError) as caught:
        run_phase1c_capacity_worker(request)

    assert caught.value.code is Phase1CWorkerErrorCode.REMOTE_FAILURE
    assert caught.value.failure.worker_kind is Phase1CWorkerKind.CAPACITY
    assert caught.value.failure.exception_qualname == "OfflineCapacityRunnerError"
    assert "OFFLINE_CAPACITY_CANDIDATE_EXISTS" in caught.value.failure.message
    assert "Traceback (most recent call last)" in caught.value.failure.traceback_text
    assert caught.value.failure.traceback_text in str(caught.value)
    assert sentinel.read_text(encoding="utf-8") == "preserve"
    assert not (request.candidate_root / "COMPLETE").exists()


class _FakeProcess:
    def __init__(self, *, alive: bool, exitcode: int | None) -> None:
        self.pid: int | None = 4815
        self.exitcode = exitcode
        self.alive = alive
        self.started = False
        self.terminated = False
        self.joined = False
        self.closed = False

    def start(self) -> None:
        self.started = True

    def is_alive(self) -> bool:
        return self.alive

    def terminate(self) -> None:
        self.terminated = True
        self.alive = False
        self.exitcode = -15

    def join(self, timeout: float | None = None) -> None:
        del timeout
        self.joined = True

    def close(self) -> None:
        self.closed = True


class _FakeQueue:
    def __init__(
        self,
        process: _FakeProcess,
        *,
        gets: list[object] | None = None,
        drained: list[object] | None = None,
        interrupt: bool = False,
    ) -> None:
        self.process = process
        self.gets = list(gets or [])
        self.drained = list(drained or [])
        self.interrupt = interrupt
        self.closed = False
        self.joined = False

    def put(self, value: object) -> None:
        self.gets.append(value)

    def get(self, block: bool = True, timeout: float | None = None) -> object:
        del block, timeout
        if self.interrupt:
            raise KeyboardInterrupt
        if self.gets:
            value = self.gets.pop(0)
            if isinstance(value, BaseException):
                raise value
            return value
        raise queue_module.Empty

    def get_nowait(self) -> object:
        if self.drained:
            return self.drained.pop(0)
        raise queue_module.Empty

    def close(self) -> None:
        self.closed = True

    def join_thread(self) -> None:
        self.joined = True


class _FakeContext:
    def __init__(self, process: _FakeProcess, queue: _FakeQueue) -> None:
        self.process = process
        self.queue = queue

    def Queue(self) -> _FakeQueue:
        return self.queue

    def Process(
        self,
        *,
        target: Callable[..., object],
        args: tuple[object, ...],
        daemon: bool,
        name: str,
    ) -> _FakeProcess:
        del target, args, daemon, name
        return self.process


def _unused_capacity_target(
    request: Phase1CCapacityWorkerRequest,
    queue: _QueueLike,
) -> None:
    del request, queue


def _unused_golden_target(
    request: GoldenNativeWorkerRequest,
    queue: _QueueLike,
) -> None:
    del request, queue


def test_parent_rejects_more_than_one_terminal_envelope(tmp_path: Path) -> None:
    request = _capacity_request(tmp_path, name="duplicate-terminal")
    process = _FakeProcess(alive=False, exitcode=0)
    failure = Phase1CWorkerFailure(
        worker_kind=Phase1CWorkerKind.CAPACITY,
        exception_module="builtins",
        exception_qualname="RuntimeError",
        message="first terminal",
        traceback_text="Traceback: first terminal",
    )
    terminal = _WorkerMessage(
        kind=_MessageKind.ERROR,
        worker_kind=Phase1CWorkerKind.CAPACITY,
        payload=failure,
    )
    queue = _FakeQueue(process, drained=[terminal, terminal])

    with pytest.raises(Phase1CWorkerError, match="more than one terminal envelope") as caught:
        run_phase1c_capacity_worker(
            request,
            _context=_FakeContext(process, queue),
            _target=_unused_capacity_target,
        )

    assert caught.value.code is Phase1CWorkerErrorCode.PROTOCOL_INVALID


def test_keyboard_interrupt_terminates_and_joins_only_child(tmp_path: Path) -> None:
    request = _capacity_request(tmp_path, name="interrupted")
    process = _FakeProcess(alive=True, exitcode=None)
    queue = _FakeQueue(process, interrupt=True)
    context = _FakeContext(process, queue)

    with pytest.raises(KeyboardInterrupt):
        run_phase1c_capacity_worker(
            request,
            _context=context,
            _target=_unused_capacity_target,
        )

    assert process.terminated is True
    assert process.joined is True
    assert process.closed is True
    assert queue.closed is True
    assert queue.joined is True
    assert not request.candidate_root.exists()
    assert not (request.candidate_root / "COMPLETE").exists()


def test_golden_result_is_drained_after_child_is_already_observed_dead(
    tmp_path: Path,
) -> None:
    request = _golden_request(tmp_path)
    result = object.__new__(GoldenNativeRunResult)
    process = _FakeProcess(alive=False, exitcode=0)
    message = _WorkerMessage(
        kind=_MessageKind.RESULT,
        worker_kind=Phase1CWorkerKind.GOLDEN_NATIVE,
        payload=result,
    )
    queue = _FakeQueue(process, drained=[message])

    observed = run_golden_native_worker(
        request,
        _context=_FakeContext(process, queue),
        _target=_unused_golden_target,
    )

    assert observed is result
    assert process.joined is True


def test_nonzero_child_exit_without_terminal_result_fails_closed(tmp_path: Path) -> None:
    request = _golden_request(tmp_path)
    process = _FakeProcess(alive=False, exitcode=17)
    queue = _FakeQueue(process)

    with pytest.raises(Phase1CWorkerProcessError) as caught:
        run_golden_native_worker(
            request,
            _context=_FakeContext(process, queue),
            _target=_unused_golden_target,
        )

    assert caught.value.code is Phase1CWorkerErrorCode.PROCESS_EXIT_NONZERO
    assert caught.value.exit_code == 17


class _AdvancingClock:
    def __init__(self) -> None:
        self.values = iter(
            (
                0,
                0,
                30_000_000_000,
                30_000_000_000,
                30_000_000_000,
            )
        )

    def __call__(self) -> int:
        return next(self.values)


class _HeartbeatQueue(_FakeQueue):
    def __init__(
        self,
        process: _FakeProcess,
        terminal: _WorkerMessage,
    ) -> None:
        super().__init__(process)
        self.terminal = terminal
        self.calls = 0

    def get(self, block: bool = True, timeout: float | None = None) -> object:
        del block, timeout
        self.calls += 1
        if self.calls == 1:
            raise queue_module.Empty
        self.process.alive = False
        self.process.exitcode = 0
        return self.terminal


def test_heartbeat_is_emitted_only_while_child_is_observed_running(
    tmp_path: Path,
) -> None:
    request = _golden_request(tmp_path)
    result = object.__new__(GoldenNativeRunResult)
    process = _FakeProcess(alive=True, exitcode=None)
    terminal = _WorkerMessage(
        kind=_MessageKind.RESULT,
        worker_kind=Phase1CWorkerKind.GOLDEN_NATIVE,
        payload=result,
    )
    queue = _HeartbeatQueue(process, terminal)
    progress: list[dict[str, object]] = []

    observed = run_golden_native_worker(
        request,
        progress=lambda payload: progress.append(dict(payload)),
        heartbeat_interval_seconds=30,
        _context=_FakeContext(process, queue),
        _target=_unused_golden_target,
        _monotonic_ns=_AdvancingClock(),
    )

    assert observed is result
    assert len(progress) == 1
    heartbeat = progress[0]
    assert heartbeat["candidate_root"] == str(request.candidate_root)
    assert heartbeat["child_pid"] == 4815
    assert heartbeat["elapsed_ns"] == 30_000_000_000
    assert heartbeat["event"] == "heartbeat"
    assert heartbeat["heartbeat_scope"] == "PHASE1C_ISOLATED_WORKER"
    assert heartbeat["last_progress"] is None
    assert heartbeat["phase"] == "phase1c_isolated_worker"
    assert heartbeat["workload"] is None
    assert heartbeat["commits_completed"] is None
    assert heartbeat["recent_throughput_status"] == (
        "UNAVAILABLE_NO_ACTIVE_WORKLOAD_PROGRESS"
    )
    assert heartbeat["conservative_eta_ns"] is None
    assert heartbeat["conservative_eta_status"] == (
        "UNAVAILABLE_NO_ACTIVE_WORKLOAD_PROGRESS"
    )
    assert "stagnation_assessment" not in heartbeat
    assert heartbeat["status"] == "RUNNING"
    assert heartbeat["worker_kind"] == "GOLDEN_NATIVE"


def test_heartbeat_flattens_child_progress_and_uses_same_workload_window(
    tmp_path: Path,
) -> None:
    window = Phase1CHeartbeatWindow()
    common: dict[str, object] = {
        "phase": "capacity_ingest",
        "status": "RUNNING",
        "workload": "SYNTHETIC_CAPACITY_V1",
        "workload_profile": "GOLDEN_SHAPED",
        "workload_id": "manifest-a",
        "commits_total": 100,
        "logical_rows_total": 200,
        "cpu_ns": 1,
        "peak_rss_bytes": 2,
        "bytes_written": 3,
        "raw_segment_count": 1,
        "paper_segment_count": 1,
        "segment_count": 2,
        "checkpoint_count": 1,
    }
    first_progress = {
        **common,
        "commits_completed": 20,
        "logical_rows_completed": 40,
        "workload_elapsed_ns": 10_000_000_000,
    }
    first = _heartbeat_payload(
        candidate_root=tmp_path.absolute(),
        child_pid=123,
        elapsed_ns=10_000_000_000,
        last_progress=first_progress,
        worker_kind=Phase1CWorkerKind.CAPACITY,
        window=window,
    )
    assert first["phase"] == "capacity_ingest"
    assert first["last_progress"] == first_progress
    assert first["recent_throughput_status"] == (
        "UNAVAILABLE_INSUFFICIENT_HEARTBEAT_WINDOW"
    )

    second = _heartbeat_payload(
        candidate_root=tmp_path.absolute(),
        child_pid=123,
        elapsed_ns=20_000_000_000,
        last_progress={
            **common,
            "commits_completed": 30,
            "logical_rows_completed": 60,
            "workload_elapsed_ns": 20_000_000_000,
        },
        worker_kind=Phase1CWorkerKind.CAPACITY,
        window=window,
    )
    assert second["workload"] == "SYNTHETIC_CAPACITY_V1"
    assert second["commits_completed"] == 30
    assert second["logical_rows_completed"] == 60
    assert second["segment_count"] == 2
    assert second["checkpoint_count"] == 1
    assert second["recent_commits_per_second"] == "1"
    assert second["recent_logical_rows_per_second"] == "2"
    assert second["conservative_eta_ns"] == 70_000_000_000


def test_requests_and_heartbeat_bounds_fail_before_process_start(tmp_path: Path) -> None:
    request = _capacity_request(tmp_path, name="bounds")

    with pytest.raises(Phase1CWorkerError) as low:
        run_phase1c_capacity_worker(request, heartbeat_interval_seconds=29.9)
    assert low.value.code is Phase1CWorkerErrorCode.INPUT_INVALID

    with pytest.raises(Phase1CWorkerError) as high:
        run_phase1c_capacity_worker(request, heartbeat_interval_seconds=60.1)
    assert high.value.code is Phase1CWorkerErrorCode.INPUT_INVALID

    with pytest.raises(Phase1CWorkerError, match="absolute"):
        Phase1CCapacityWorkerRequest(
            manifest=request.manifest,
            candidate_root=Path("relative"),
            code_identity=request.code_identity,
            runtime_identity=request.runtime_identity,
        )
