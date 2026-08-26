from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import hyperlab.paper.storage_v4.capacity_runner as capacity_runner_module
from hyperlab.paper.storage_v4.candidate_tree import CandidateTreeWitness
from hyperlab.paper.storage_v4.capacity import (
    CapacityMeasurement,
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
    CapacityWorkloadHasher,
    CapacityWorkloadManifest,
    build_capacity_workload_manifest,
    iter_capacity_commits,
    run_capacity_workload,
)
from hyperlab.paper.storage_v4.capacity_runner import (
    CumulativeCapacityRunResult,
    OfflineCapacityRunEvidence,
    OfflineCapacityRunnerError,
    OfflineCapacityRunnerErrorCode,
    OfflinePhase1CCapacityRunner,
)
from hyperlab.paper.storage_v4.phase1c_certification import (
    Phase1CCertificationError,
    _validate_capacity_result,
)
from hyperlab.paper.storage_v4.phase1c_pipeline import Phase1CAuthorityStatus
from hyperlab.paper.storage_v4.raw_segment import RawSegmentThresholds
from hyperlab.paper.storage_v4.startup_trace import (
    STARTUP_TRACE_STATUS,
    StartupFileCategory,
)
from hyperlab.paper.storage_v4.types import Hash32

_CODE_IDENTITY = Hash32(b"\xc1" * 32)
_RUNTIME_IDENTITY = Hash32(b"\xd1" * 32)


def _config(*, commit_count: int = 5) -> CapacityWorkloadConfig:
    return CapacityWorkloadConfig(
        profile=CapacityProfile.GOLDEN_SHAPED,
        seed=91,
        commit_count=commit_count,
        start_time_ns=1_700_000_000_000_000_000,
        cadence_ns=250_000_000,
        type_distribution=(
            CapacityTypeSpec(
                record_type="PUBLIC_BBO",
                stream="inbox",
                weight=3,
                payload_min_bytes=8,
                payload_max_bytes=31,
                payload_cardinality=2,
            ),
            CapacityTypeSpec(
                record_type="PUBLIC_TRADE",
                stream="trades",
                weight=1,
                payload_min_bytes=4,
                payload_max_bytes=17,
                payload_cardinality=3,
            ),
        ),
        strategies=("phase05_cash_and_carry", "phase08_cross_venue"),
        alert_every_commits=2,
        incident_every_commits=3,
        ledger_every_commits=2,
        market_gap_count=1,
        alert_payload_bytes=5,
        incident_payload_bytes=6,
        ledger_payload_bytes=7,
        market_gap_payload_bytes=8,
        golden_census_sha256="b" * 64,
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


def test_cumulative_runner_uses_one_stream_store_and_durable_prefix_chain(
    tmp_path: Path,
) -> None:
    manifests = _cumulative_manifests()
    terminal = manifests[-1]
    emitted: list[int] = []

    def commits():  # type: ignore[no-untyped-def]
        for commit in iter_capacity_commits(terminal.config):
            emitted.append(commit.sequence)
            yield commit

    progress: list[dict[str, object]] = []
    runner = OfflinePhase1CCapacityRunner(
        candidate_root=(tmp_path / "cumulative").absolute(),
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
        batch_size=2,
        checkpoint_every_batches=1,
        rss_probe=lambda: None,
        progress=lambda payload: progress.append(dict(payload)),
    )

    result = runner.run_cumulative_capacity_workload(
        manifests=manifests,
        commits=commits(),
    )

    assert isinstance(result, CumulativeCapacityRunResult)
    assert emitted == [1, 2, 3, 4, 5, 6]
    assert [item.commit_count for item in result.boundaries] == [2, 4, 6]
    assert [item.manifest.commit_count for item in result.typed_boundaries] == [2, 4, 6]
    assert result.certificates is result.boundaries
    assert all(item.path.is_file() for item in result.boundaries)
    assert result.accounting.payload() == {
        "commits_generated": 6,
        "commits_ingested": 6,
        "generator_emissions": 6,
        "prefix_commits_audited": 6,
        "prefix_commits_reingested": 0,
        "raw_commits_reused": 0,
        "raw_seal_count": 3,
        "resume_count": 0,
        "store_count": 1,
        "stream_count": 1,
        "suffix_commits_reconstructed": 0,
        "worker_count": 1,
    }
    assert len({item.evidence.raw_store_id for item in result.typed_boundaries}) == 1
    assert len({item.evidence.paper_store_id for item in result.typed_boundaries}) == 1
    assert len({item.evidence.run_id for item in result.typed_boundaries}) == 1
    assert result.terminal_shared_candidate_tree is (
        result.typed_boundaries[-1].evidence.audited_candidate_tree
    )
    assert result.typed_boundaries[0].evidence.audited_candidate_tree != (
        result.terminal_shared_candidate_tree
    )
    assert [
        item["boundary_commit_count"]
        for item in progress
        if item["phase"] == "capacity_boundary_complete"
    ] == [2, 4, 6]
    assert progress[-1]["commits_ingested"] == 6
    assert progress[-1]["prefix_commits_reingested"] == 0
    assert progress[-1]["terminal_shared_candidate_tree_sha256"] == (
        result.terminal_shared_candidate_tree.tree_sha256
    )
    assert runner.last_cumulative_result is result
    assert runner.last_evidence is result.typed_boundaries[-1].evidence


def test_runner_counts_every_raw_rotation_inside_one_paper_batch(tmp_path: Path) -> None:
    config = replace(_config(commit_count=3), market_gap_count=0)
    manifest = build_capacity_workload_manifest(config)
    progress: list[dict[str, object]] = []
    runner = OfflinePhase1CCapacityRunner(
        candidate_root=(tmp_path / "raw-rotations").absolute(),
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
        batch_size=3,
        checkpoint_every_batches=1,
        raw_thresholds=RawSegmentThresholds(
            max_records=1,
            max_logical_payload_bytes=1024 * 1024,
            max_physical_bytes=2 * 1024 * 1024,
            max_single_payload_bytes=1024 * 1024,
        ),
        rss_probe=lambda: None,
        progress=lambda payload: progress.append(dict(payload)),
    )

    measurement = runner.run_capacity_workload(
        manifest=manifest,
        commits=iter_capacity_commits(config),
    )

    assert measurement.segment_count == 4
    assert measurement.checkpoint_count == 1
    assert runner.last_evidence is not None
    assert runner.last_evidence.certification.raw_audit.segments_read == 3
    ingest = [item for item in progress if item["phase"] == "capacity_ingest"]
    assert [item["raw_segment_count"] for item in ingest] == [3]
    assert [item["segment_count"] for item in ingest] == [4]


def test_runner_streams_repeated_seals_reopens_audits_and_censuses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = (tmp_path / "candidate").absolute()
    progress: list[dict[str, object]] = []
    overlay_journal_sizes: list[int] = []
    original_transient_bytes = capacity_runner_module._transient_bytes

    def witness_transient_bytes(root: Path) -> int:
        observed = original_transient_bytes(root)
        for journal in root.rglob("overlay.sqlite3-journal"):
            if journal.is_file():
                size = journal.stat().st_size
                if size > 0:
                    overlay_journal_sizes.append(size)
        return observed

    monkeypatch.setattr(
        capacity_runner_module,
        "_transient_bytes",
        witness_transient_bytes,
    )
    runner = OfflinePhase1CCapacityRunner(
        candidate_root=candidate,
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
        batch_size=2,
        checkpoint_every_batches=1,
        rss_probe=lambda: 123_456,
        progress=lambda payload: progress.append(dict(payload)),
    )

    result = run_capacity_workload(config=_config(), runner=runner)
    measurement = result.measurement
    evidence = runner.last_evidence

    assert evidence is not None
    assert measurement.workload_manifest_sha256 == result.manifest.sha256
    assert measurement.observed_workload_sha256 == result.manifest.workload_sha256
    assert measurement.commit_count == 5
    assert measurement.logical_row_count == result.manifest.logical_row_count
    assert measurement.peak_rss_bytes == 123_456
    assert measurement.raw_input_bytes is not None
    assert measurement.raw_input_bytes > 0
    assert measurement.cumulative_bytes_written is None
    assert measurement.payload()["write_amplification"] == {
        "ratio": None,
        "status": "UNAVAILABLE_CUMULATIVE_BYTES_WRITTEN_NOT_MEASURED",
    }

    assert evidence.batch_count == 3
    assert evidence.seal_count == 3
    assert evidence.max_batch_commits_observed == 2
    assert len(measurement.seal_durations.observations_ns) == 3
    assert len(measurement.checkpoint_durations.observations_ns) == 3
    assert len(measurement.manifest_publish_durations.observations_ns) == 6
    assert measurement.segment_count == 6
    assert measurement.checkpoint_count == 3
    assert measurement.manifest_count == 6

    certification = evidence.certification
    assert certification.alignment.status is Phase1CAuthorityStatus.ALIGNED
    assert certification.raw_startup.historical_segments_read == 0
    assert certification.raw_startup.manifests_opened == 1
    assert certification.paper_startup.segments_read == 0
    assert certification.paper_startup.tail_entries_replayed == 0
    assert certification.paper_startup.checkpoint_used is True
    assert certification.raw_audit.records_read == 5
    assert certification.paper_audit.commits_read == 5
    assert certification.native_audit.raw_reference_count == 5
    assert evidence.audited_candidate_tree.root == candidate
    evidence_payload = evidence.payload()
    integrity = evidence_payload["integrity"]
    assert isinstance(integrity, dict)
    assert integrity["audited_candidate_tree"] == (
        evidence.audited_candidate_tree.payload()
    )
    # Native certification and the independent workload oracle each stream all
    # three raw segments; the resolver intentionally retains one segment only.
    assert certification.raw_resolver_physical_hash_passes == 2 * 3
    startup_trace = evidence.startup_file_trace
    assert startup_trace.status == STARTUP_TRACE_STATUS
    assert startup_trace.historical_segment_open_count == 0
    assert {
        StartupFileCategory.RAW_MANIFEST,
        StartupFileCategory.PAPER_MANIFEST,
        StartupFileCategory.PAPER_CHECKPOINT,
        StartupFileCategory.PAPER_OVERLAY,
        StartupFileCategory.RAW_ANCHOR,
        StartupFileCategory.PAPER_ANCHOR,
    }.issubset({item.category for item in startup_trace.opens})
    assert all("/segments/" not in item.relative_path for item in startup_trace.opens)
    startup = evidence_payload["startup"]
    assert isinstance(startup, dict)
    assert startup["file_access_trace"] == startup_trace.payload()

    census = measurement.byte_census
    assert census.raw_bytes > 0
    assert census.raw_index_bytes > 0
    assert census.raw_segments_bytes > 0
    assert census.paper_incremental_bytes > 0
    assert census.raw_anchors_witnesses_bytes > 0
    assert census.paper_anchors_witnesses_bytes > 0
    assert census.raw_current_cache_bytes > 0
    assert census.paper_current_cache_bytes > 0
    assert census.total_bytes == census.raw_bytes + census.paper_incremental_bytes
    assert census.scratch_current_bytes == 0
    assert census.scratch_peak_bytes > 0
    assert overlay_journal_sizes
    assert census.scratch_peak_bytes >= max(overlay_journal_sizes)
    assert evidence.scratch_status == (
        "EXACT_RECOGNIZED_TRANSIENT_FILE_PEAK_AT_INSTRUMENTED_BOUNDARIES"
    )
    assert measurement.startup_historical_segments_read == 0
    assert measurement.startup_historical_commits_replayed == 0
    assert measurement.startup_tail_entries_replayed == 0
    assert measurement.metadata_authentication_ns >= measurement.startup_ns
    assert measurement.full_history_audit_ns >= 0
    assert measurement.logical_span_ns == 1_000_000_000
    ingest_progress = [item for item in progress if item["phase"] == "capacity_ingest"]
    assert [item["commits_completed"] for item in ingest_progress] == [2, 4, 5]
    assert all(item["commits_total"] == 5 for item in ingest_progress)
    assert all(
        item["workload"] == "SYNTHETIC_CAPACITY_V1" for item in ingest_progress
    )
    assert all(
        item["workload_profile"] == CapacityProfile.GOLDEN_SHAPED.value
        for item in ingest_progress
    )
    assert all(
        item["workload_id"] == result.manifest.sha256 for item in ingest_progress
    )
    assert [item["raw_segment_count"] for item in ingest_progress] == [1, 2, 3]
    assert [item["paper_segment_count"] for item in ingest_progress] == [1, 2, 3]
    assert [item["segment_count"] for item in ingest_progress] == [2, 4, 6]
    assert [item["checkpoint_count"] for item in ingest_progress] == [1, 2, 3]
    assert ingest_progress[-1]["logical_rows_completed"] == (
        result.manifest.logical_row_count
    )
    assert all(
        item["logical_rows_total"] == result.manifest.logical_row_count
        for item in ingest_progress
    )
    assert all(item["bytes_written"] is None for item in ingest_progress)
    assert all(
        item["bytes_written_status"] == "UNAVAILABLE_WRITE_BYTE_PROBE"
        for item in ingest_progress
    )
    for audit_phase in (
        "raw_full_audit",
        "paper_full_audit",
        "native_full_audit",
        "capacity_oracle_full_audit",
    ):
        audit_events = [item for item in progress if item["phase"] == audit_phase]
        assert audit_events[0]["audit_event"] == "STARTED"
        assert audit_events[-1]["audit_event"] == "COMPLETE"
        assert [item["heartbeat_sequence"] for item in audit_events] == list(
            range(len(audit_events))
        )
        assert all(
            item["audit_progress_authority"]
            == "NON_AUTHORITATIVE_OBSERVABILITY_ONLY"
            for item in audit_events
        )
        assert [
            int(item["phase_elapsed_ns"]) for item in audit_events
        ] == sorted(int(item["phase_elapsed_ns"]) for item in audit_events)
    assert progress[-1]["phase"] == "capacity_complete"
    assert progress[-1]["commits_completed"] == 5
    assert progress[-1]["logical_rows_completed"] == result.manifest.logical_row_count
    assert progress[-1]["segment_count"] == 6
    assert progress[-1]["checkpoint_count"] == 3
    assert progress[-1]["status"] == "STORAGE_V4_PHASE_1C_CAPACITY_LEVEL_EXACT"


def test_runner_rejects_candidate_mutation_across_read_only_audit_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = (tmp_path / "mutated-during-audit").absolute()
    original_witness = capacity_runner_module.witness_candidate_tree
    witness_calls = 0

    def witness_then_mutate(
        root: Path,
        **kwargs: Any,
    ) -> CandidateTreeWitness:
        nonlocal witness_calls
        witnessed = original_witness(root, **kwargs)
        witness_calls += 1
        if witness_calls == 1:
            (root / "unbound-after-pre-audit-witness.bin").write_bytes(b"drift")
        return witnessed

    monkeypatch.setattr(
        capacity_runner_module,
        "witness_candidate_tree",
        witness_then_mutate,
    )
    runner = OfflinePhase1CCapacityRunner(
        candidate_root=candidate,
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
        batch_size=1,
        checkpoint_every_batches=1,
        rss_probe=lambda: None,
    )

    with pytest.raises(
        OfflineCapacityRunnerError,
        match="candidate tree changed across read-only audits",
    ):
        run_capacity_workload(config=_config(commit_count=1), runner=runner)

    assert witness_calls == 2
    assert candidate.is_dir()


def test_runner_records_cumulative_bytes_only_from_monotone_probe(tmp_path: Path) -> None:
    observations = iter((1_000, 1_777))
    runner = OfflinePhase1CCapacityRunner(
        candidate_root=(tmp_path / "probed").absolute(),
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
        batch_size=3,
        write_bytes_probe=lambda: next(observations),
        rss_probe=lambda: None,
    )
    config = _config(commit_count=3)

    measurement = runner.run_capacity_workload(
        manifest=build_capacity_workload_manifest(config),
        commits=iter_capacity_commits(config),
    )

    assert measurement.cumulative_bytes_written == 777
    assert measurement.peak_rss_bytes is None
    write_amplification = measurement.payload()["write_amplification"]
    assert isinstance(write_amplification, dict)
    assert write_amplification["status"] == "AVAILABLE"


def test_runner_rejects_existing_candidate_without_mutation(tmp_path: Path) -> None:
    candidate = (tmp_path / "existing").absolute()
    candidate.mkdir()
    sentinel = candidate / "user.txt"
    sentinel.write_text("preserve", encoding="utf-8")
    runner = OfflinePhase1CCapacityRunner(
        candidate_root=candidate,
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
    )
    manifest = build_capacity_workload_manifest(_config(commit_count=1))

    with pytest.raises(OfflineCapacityRunnerError) as caught:
        runner.run_capacity_workload(
            manifest=manifest,
            commits=iter_capacity_commits(manifest.config),
        )

    assert caught.value.code is OfflineCapacityRunnerErrorCode.CANDIDATE_EXISTS
    assert sentinel.read_text(encoding="utf-8") == "preserve"
    assert runner.last_evidence is None


def test_runner_preserves_divergent_candidate_as_non_authoritative(tmp_path: Path) -> None:
    expected = _config(commit_count=3)
    divergent = replace(expected, seed=expected.seed + 1)
    manifest = build_capacity_workload_manifest(expected)
    candidate = (tmp_path / "divergent").absolute()
    runner = OfflinePhase1CCapacityRunner(
        candidate_root=candidate,
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
        batch_size=2,
        checkpoint_every_batches=1,
        rss_probe=lambda: None,
    )

    with pytest.raises(OfflineCapacityRunnerError) as caught:
        runner.run_capacity_workload(
            manifest=manifest,
            commits=iter_capacity_commits(divergent),
        )

    assert caught.value.code is OfflineCapacityRunnerErrorCode.WORKLOAD_DIVERGENCE
    assert candidate.is_dir()
    assert not (candidate / "COMPLETE").exists()
    assert runner.last_evidence is None


def test_runner_rejects_relative_root_and_invalid_batch_bound() -> None:
    with pytest.raises(OfflineCapacityRunnerError) as caught:
        OfflinePhase1CCapacityRunner(
            candidate_root=Path("relative"),
            code_identity=_CODE_IDENTITY,
            runtime_identity=_RUNTIME_IDENTITY,
        )
    assert caught.value.code is OfflineCapacityRunnerErrorCode.PATH_INVALID

    with pytest.raises(OfflineCapacityRunnerError) as caught:
        OfflinePhase1CCapacityRunner(
            candidate_root=Path.cwd() / "unused-candidate",
            code_identity=_CODE_IDENTITY,
            runtime_identity=_RUNTIME_IDENTITY,
            batch_size=10_001,
        )
    assert caught.value.code is OfflineCapacityRunnerErrorCode.TYPE_INVALID


def test_effective_code_and_runtime_identities_change_paper_authority_and_are_verified(
    tmp_path: Path,
) -> None:
    manifest = build_capacity_workload_manifest(_config(commit_count=1))

    def run_candidate(
        name: str,
        *,
        code_identity: Hash32,
        runtime_identity: Hash32,
    ) -> tuple[CapacityMeasurement, OfflineCapacityRunEvidence]:
        runner = OfflinePhase1CCapacityRunner(
            candidate_root=(tmp_path / name).absolute(),
            code_identity=code_identity,
            runtime_identity=runtime_identity,
            batch_size=1,
            rss_probe=lambda: None,
        )
        measurement = runner.run_capacity_workload(
            manifest=manifest,
            commits=iter_capacity_commits(manifest.config),
        )
        assert runner.last_evidence is not None
        return measurement, runner.last_evidence

    baseline = run_candidate(
        "baseline",
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
    )
    code_changed = run_candidate(
        "code-changed",
        code_identity=Hash32(b"\xc2" * 32),
        runtime_identity=_RUNTIME_IDENTITY,
    )
    runtime_changed = run_candidate(
        "runtime-changed",
        code_identity=_CODE_IDENTITY,
        runtime_identity=Hash32(b"\xd2" * 32),
    )
    paper_roots = {
        item[1].certification.paper_audit.manifest_root
        for item in (baseline, code_changed, runtime_changed)
    }
    assert len(paper_roots) == 3
    assert baseline[1].code_identity == _CODE_IDENTITY.hex()
    assert baseline[1].runtime_identity == _RUNTIME_IDENTITY.hex()
    assert _validate_capacity_result(
        manifest,
        baseline[0],
        baseline[1],
        expected_code_identity=_CODE_IDENTITY,
        expected_runtime_identity=_RUNTIME_IDENTITY,
    )["verified"] is True

    with pytest.raises(Phase1CCertificationError, match="code identity differs"):
        _validate_capacity_result(
            manifest,
            baseline[0],
            baseline[1],
            expected_code_identity=Hash32(b"\xff" * 32),
            expected_runtime_identity=_RUNTIME_IDENTITY,
        )
    with pytest.raises(Phase1CCertificationError, match="runtime identity differs"):
        _validate_capacity_result(
            manifest,
            baseline[0],
            baseline[1],
            expected_code_identity=_CODE_IDENTITY,
            expected_runtime_identity=Hash32(b"\xee" * 32),
        )
