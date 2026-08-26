from __future__ import annotations

import hashlib
import json
import pickle
from pathlib import Path

import pytest

import hyperlab.paper.storage_v4.tail_runner as tail_runner_module
from hyperlab.paper.storage_v4.capacity import (
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
    build_capacity_workload_manifest,
    iter_capacity_commits,
)
from hyperlab.paper.storage_v4.phase1c_pipeline import Phase1CAuthorityStatus
from hyperlab.paper.storage_v4.phase1c_progress import Phase1CHeartbeatWindow
from hyperlab.paper.storage_v4.startup_trace import (
    STARTUP_TRACE_STATUS,
    StartupFileCategory,
)
from hyperlab.paper.storage_v4.tail_runner import (
    BoundedTailRestartMatrixRunner,
    TailMatrixExactness,
    TailMatrixRunnerError,
    TailMatrixRunnerErrorCode,
)
from hyperlab.paper.storage_v4.types import Hash32

_CODE_IDENTITY = Hash32(b"\xa1" * 32)
_RUNTIME_IDENTITY = Hash32(b"\xb1" * 32)


def _small_manifest():  # type: ignore[no-untyped-def]
    config = CapacityWorkloadConfig(
        profile=CapacityProfile.GOLDEN_SHAPED,
        seed=91,
        commit_count=7,
        start_time_ns=1_700_000_000_000_000_000,
        cadence_ns=250_000_000,
        type_distribution=(
            CapacityTypeSpec(
                record_type="PUBLIC_BBO",
                stream="inbox",
                weight=3,
                payload_min_bytes=1,
                payload_max_bytes=7,
                payload_cardinality=1,
            ),
            CapacityTypeSpec(
                record_type="PUBLIC_FUNDING",
                stream="events",
                weight=1,
                payload_min_bytes=3,
                payload_max_bytes=9,
                payload_cardinality=7,
            ),
        ),
        strategies=("phase05_cash_and_carry", "phase08_lead_lag"),
        alert_every_commits=2,
        incident_every_commits=3,
        ledger_every_commits=2,
        market_gap_count=1,
        alert_payload_bytes=3,
        incident_payload_bytes=5,
        ledger_payload_bytes=7,
        market_gap_payload_bytes=9,
        golden_census_sha256="a" * 64,
    )
    return build_capacity_workload_manifest(config)


def test_reduced_matrix_runs_real_zero_nonzero_and_max_tail_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StepClock:
        def __init__(self) -> None:
            self.value = 0

        def perf_counter_ns(self) -> int:
            self.value += 1_000
            return self.value

    monkeypatch.setattr(tail_runner_module, "time", StepClock())
    manifest = _small_manifest()
    root = (tmp_path / "tail-matrix").absolute()
    progress: list[dict[str, object]] = []
    runner = BoundedTailRestartMatrixRunner(
        candidate_root=root,
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
        batch_size=2,
        progress=lambda payload: progress.append(dict(payload)),
        _test_tail_sizes=(0, 1, 3),
    )

    report = runner.run(manifest)

    assert report.requested_tail_sizes == (0, 1, 3)
    assert report.test_override is True
    assert report.candidate_root == root
    assert report.audited_candidate_tree.root == root
    assert report.sha256 == hashlib.sha256(report.canonical_bytes).hexdigest()
    assert pickle.loads(pickle.dumps(report)) == report
    payload = json.loads(report.canonical_bytes)
    assert payload["artifact"] == "STORAGE_V4_PHASE1C_BOUNDED_TAIL_RESTART_MATRIX_V1"
    assert payload["contract_scope"] == "TEST_OVERRIDE"
    assert payload["requested_tail_sizes"] == [0, 1, 3]
    assert payload["audited_candidate_tree"] == report.audited_candidate_tree.payload()
    assert "COMPLETE" not in payload

    for case in report.cases:
        checkpoint_count = manifest.commit_count - case.tail_entries
        certification = case.certification
        assert case.checkpoint_commit_count == checkpoint_count
        assert case.max_batch_commits_observed <= 2
        assert case.exactness is TailMatrixExactness.EXACT
        assert case.audited_candidate_tree.root == case.candidate_root
        assert case.payload()["audited_candidate_tree"] == (
            case.audited_candidate_tree.payload()
        )
        assert case.startup_ns > 0
        assert case.certification_with_reopen_ns > 0
        assert case.independent_oracle_ns > 0
        assert certification.raw_startup.historical_segments_read == 0
        assert case.startup_file_trace.status == STARTUP_TRACE_STATUS
        assert case.startup_file_trace.historical_segment_open_count == 0
        assert {
            StartupFileCategory.RAW_MANIFEST,
            StartupFileCategory.PAPER_MANIFEST,
            StartupFileCategory.PAPER_CHECKPOINT,
            StartupFileCategory.PAPER_OVERLAY,
            StartupFileCategory.RAW_ANCHOR,
            StartupFileCategory.PAPER_ANCHOR,
        }.issubset({item.category for item in case.startup_file_trace.opens})
        assert all(
            "/segments/" not in item.relative_path
            for item in case.startup_file_trace.opens
        )
        assert case.payload()["startup_file_access_trace"] == (
            case.startup_file_trace.payload()
        )
        assert certification.raw_startup.manifests_opened == 1
        assert certification.raw_startup.manifest_namespace_entries_scanned == 0
        assert certification.paper_startup.segments_read == 0
        assert certification.paper_startup.historical_commits_not_read == checkpoint_count
        assert certification.paper_startup.tail_entries_replayed == case.tail_entries
        assert len(certification.paper_startup.tail_frames) == case.tail_entries
        assert certification.raw_audit.records_read == manifest.commit_count
        assert certification.paper_audit.commits_read == checkpoint_count
        assert certification.native_audit.commit_count == manifest.commit_count
        assert certification.alignment.binding is not None
        assert certification.alignment.binding.raw_record_count == checkpoint_count
        assert case.oracle.commit_count == manifest.commit_count
        assert case.oracle.logical_row_count == manifest.logical_row_count
        assert case.oracle.workload_sha256 == manifest.workload_sha256
        assert case.oracle.final_prefix_root == certification.native_audit.final_prefix_root.hex()
        expected_status = (
            Phase1CAuthorityStatus.ALIGNED
            if case.tail_entries == 0
            else Phase1CAuthorityStatus.RAW_AHEAD_OF_PAPER
        )
        assert certification.alignment.status is expected_status
        if case.tail_entries == 0:
            assert certification.alignment.raw_generation == certification.alignment.binding.raw_generation
        else:
            assert certification.alignment.raw_generation > certification.alignment.binding.raw_generation
        assert (root / case.candidate_id).is_dir()

    assert not any(path.name == "COMPLETE" for path in root.rglob("*"))
    completed = [
        item for item in progress if item["phase"] == "bounded_tail_case_complete"
    ]
    assert [item["tail_entries"] for item in completed] == [0, 1, 3]
    assert all(item["cases_total"] == 3 for item in completed)

    commits = tuple(iter_capacity_commits(manifest.config))
    logical_rows_by_commit: dict[int, int] = {}
    logical_rows = 0
    for commit in commits:
        logical_rows += len(commit.rows)
        logical_rows_by_commit[commit.sequence] = logical_rows

    expected_workload_ids = {
        tail_entries: f"{manifest.sha256}:tail:{tail_entries}"
        for tail_entries in (0, 1, 3)
    }
    assert {item["workload_id"] for item in completed} == set(
        expected_workload_ids.values()
    )
    for tail_entries, workload_id in expected_workload_ids.items():
        case_progress = [
            item for item in progress if item.get("workload_id") == workload_id
        ]
        assert case_progress
        started = case_progress[0]
        assert started["phase"] == "bounded_tail_ingest"
        assert started["status"] == "STARTED"
        assert started["batch_count"] == 0
        assert started["checkpoint_boundary_commit_count"] == (
            manifest.commit_count - tail_entries
        )
        for counter in (
            "commits_completed",
            "logical_rows_completed",
            "raw_segment_count",
            "paper_segment_count",
            "checkpoint_count",
        ):
            assert started[counter] == 0
        assert all(
            item["workload"] == "SYNTHETIC_BOUNDED_TAIL_RESTART_V1"
            and item["workload_profile"] == manifest.config.profile.value
            and item["workload_manifest_sha256"] == manifest.sha256
            and item["workload_sha256"] == manifest.workload_sha256
            and item["commits_total"] == manifest.commit_count
            and item["logical_rows_total"] == manifest.logical_row_count
            and item["segment_checkpoint_status"]
            == "EXACT_DURABLE_PUBLICATION_COUNTS"
            and item["progress_metrics_scope"]
            == (
                "CURRENT_BOUNDED_TAIL_CASE_AT_COMPLETED_PROGRESS_BOUNDARY; "
                "CPU_RSS_AND_WRITE_BYTES_NOT_MEASURED"
            )
            for item in case_progress
        )
        elapsed = [int(item["workload_elapsed_ns"]) for item in case_progress]
        assert elapsed[0] == 1_000
        assert elapsed == sorted(elapsed)
        assert len(elapsed) == len(set(elapsed))
        assert all(
            item["elapsed_ns"] == item["workload_elapsed_ns"]
            for item in case_progress
        )
        for counter in (
            "commits_completed",
            "logical_rows_completed",
            "raw_segment_count",
            "paper_segment_count",
            "segment_count",
            "checkpoint_count",
        ):
            values = [int(item[counter]) for item in case_progress]
            assert values == sorted(values)

        checkpoint_commit_count = manifest.commit_count - tail_entries
        batch_boundaries: list[int] = []
        pending_count = 0
        for commit in commits:
            pending_count += 1
            if commit.sequence == checkpoint_commit_count or pending_count == 2:
                batch_boundaries.append(commit.sequence)
                pending_count = 0
        if pending_count:
            batch_boundaries.append(manifest.commit_count)
        ingestion_progress = [
            item
            for item in case_progress
            if item["phase"] in {"bounded_tail_ingest", "bounded_tail_checkpoint"}
            and item["status"] != "STARTED"
        ]
        expected_ingestion = [
            (
                boundary,
                logical_rows_by_commit[boundary],
                index,
                1 if boundary >= checkpoint_commit_count else 0,
                "bounded_tail_checkpoint"
                if boundary == checkpoint_commit_count
                else "bounded_tail_ingest",
            )
            for index, boundary in enumerate(batch_boundaries, start=1)
        ]
        assert [
            (
                item["commits_completed"],
                item["logical_rows_completed"],
                item["raw_segment_count"],
                item["paper_segment_count"],
                item["phase"],
            )
            for item in ingestion_progress
        ] == expected_ingestion
        assert [item["checkpoint_count"] for item in ingestion_progress] == [
            expected[3] for expected in expected_ingestion
        ]
        assert all(
            item["segment_count"]
            == item["raw_segment_count"] + item["paper_segment_count"]
            for item in ingestion_progress
        )
        checkpoint_progress = [
            item
            for item in ingestion_progress
            if item["phase"] == "bounded_tail_checkpoint"
        ]
        assert len(checkpoint_progress) == 1
        assert checkpoint_progress[0]["status"] == "CHECKPOINT_PUBLISHED"
        assert checkpoint_progress[0]["checkpoint_boundary_commit_count"] == (
            checkpoint_commit_count
        )

        final = case_progress[-1]
        assert final["phase"] == "bounded_tail_case_complete"
        assert final["commits_completed"] == manifest.commit_count
        assert final["logical_rows_completed"] == manifest.logical_row_count
        assert final["raw_segment_count"] == len(batch_boundaries)
        assert final["paper_segment_count"] == 1
        assert final["checkpoint_count"] == 1
        window = Phase1CHeartbeatWindow()
        rendered = [
            window.render(
                item,
                observed_elapsed_ns=int(item["workload_elapsed_ns"]),
            )
            for item in case_progress
        ]
        assert any(
            item["recent_throughput_status"] == "AVAILABLE_SAME_WORKLOAD_WINDOW"
            and item["conservative_eta_ns"] is not None
            for item in rendered
        )


def test_tail_case_rejects_mutation_across_read_only_audit_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_witness = tail_runner_module.witness_candidate_tree
    witness_calls = 0

    def witness_then_mutate(
        root: Path,
        **kwargs: object,
    ):  # type: ignore[no-untyped-def]
        nonlocal witness_calls
        witnessed = original_witness(root, **kwargs)
        witness_calls += 1
        if witness_calls == 1:
            (root / "unbound-after-pre-audit-witness.bin").write_bytes(b"drift")
        return witnessed

    monkeypatch.setattr(
        tail_runner_module,
        "witness_candidate_tree",
        witness_then_mutate,
    )
    runner = BoundedTailRestartMatrixRunner(
        candidate_root=(tmp_path / "tail-audit-drift").absolute(),
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
        batch_size=2,
        _test_tail_sizes=(0,),
    )

    with pytest.raises(
        TailMatrixRunnerError,
        match="tail case tree changed across read-only audits",
    ):
        runner.run(_small_manifest())

    assert witness_calls == 2


def test_matrix_composition_rejects_earlier_case_mutated_during_later_case(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_witness = tail_runner_module.witness_candidate_tree
    witness_calls = 0

    def witness_then_mutate_prior_case(
        root: Path,
        **kwargs: object,
    ):  # type: ignore[no-untyped-def]
        nonlocal witness_calls
        witnessed = original_witness(root, **kwargs)
        witness_calls += 1
        if witness_calls == 3:
            prior = root.parent / "tail-00000000000000000000"
            (prior / "unbound-after-case-audit.bin").write_bytes(b"drift")
        return witnessed

    monkeypatch.setattr(
        tail_runner_module,
        "witness_candidate_tree",
        witness_then_mutate_prior_case,
    )
    runner = BoundedTailRestartMatrixRunner(
        candidate_root=(tmp_path / "tail-parent-drift").absolute(),
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
        batch_size=2,
        _test_tail_sizes=(0, 1),
    )

    with pytest.raises(
        TailMatrixRunnerError,
        match="tail matrix tree differs from the exact audited case composition",
    ):
        runner.run(_small_manifest())

    assert witness_calls == 5


def test_production_contract_and_existing_root_fail_closed_without_mutation(
    tmp_path: Path,
) -> None:
    manifest = _small_manifest()
    wrong_profile_root = (tmp_path / "wrong-profile").absolute()
    with pytest.raises(TailMatrixRunnerError) as wrong_profile:
        BoundedTailRestartMatrixRunner(
            candidate_root=wrong_profile_root,
            code_identity=_CODE_IDENTITY,
            runtime_identity=_RUNTIME_IDENTITY,
        ).run(manifest)
    assert wrong_profile.value.code is TailMatrixRunnerErrorCode.CONTRACT_INVALID
    assert not wrong_profile_root.exists()

    existing = (tmp_path / "existing").absolute()
    existing.mkdir()
    sentinel = existing / "preserve.txt"
    sentinel.write_text("preserve", encoding="utf-8")
    runner = BoundedTailRestartMatrixRunner(
        candidate_root=existing,
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
        _test_tail_sizes=(1, 3),
    )
    with pytest.raises(TailMatrixRunnerError) as caught:
        runner.run(manifest)
    assert caught.value.code is TailMatrixRunnerErrorCode.CANDIDATE_EXISTS
    assert sentinel.read_text(encoding="utf-8") == "preserve"


def test_tail_authority_binds_effective_code_and_runtime_for_identical_workload(
    tmp_path: Path,
) -> None:
    manifest = _small_manifest()

    def run_case(
        name: str,
        *,
        code_identity: Hash32,
        runtime_identity: Hash32,
    ):  # type: ignore[no-untyped-def]
        return BoundedTailRestartMatrixRunner(
            candidate_root=(tmp_path / name).absolute(),
            code_identity=code_identity,
            runtime_identity=runtime_identity,
            batch_size=2,
            _test_tail_sizes=(0,),
        ).run(manifest).cases[0]

    baseline = run_case(
        "baseline",
        code_identity=_CODE_IDENTITY,
        runtime_identity=_RUNTIME_IDENTITY,
    )
    code_changed = run_case(
        "code-changed",
        code_identity=Hash32(b"\xa2" * 32),
        runtime_identity=_RUNTIME_IDENTITY,
    )
    runtime_changed = run_case(
        "runtime-changed",
        code_identity=_CODE_IDENTITY,
        runtime_identity=Hash32(b"\xb2" * 32),
    )

    assert len(
        {
            baseline.certification.paper_audit.manifest_root,
            code_changed.certification.paper_audit.manifest_root,
            runtime_changed.certification.paper_audit.manifest_root,
        }
    ) == 3
    assert baseline.code_identity == _CODE_IDENTITY.hex()
    assert baseline.runtime_identity == _RUNTIME_IDENTITY.hex()
