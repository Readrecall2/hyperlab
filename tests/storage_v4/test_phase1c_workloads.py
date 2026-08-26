from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from itertools import pairwise

import pytest

from hyperlab.paper.storage_v4.capacity import (
    CAPACITY_MARKERS,
    MAX_SYNTHETIC_PAYLOAD_BYTES,
    ByteCategoryCensus,
    CapacityProfile,
    StorageGrowthAssessment,
    build_capacity_workload_manifest,
    iter_capacity_commits,
)
from hyperlab.paper.storage_v4.capacity_shape import (
    GoldenCapacityShape,
    GoldenCapacityTypeObservation,
)
from hyperlab.paper.storage_v4.phase1c_workloads import (
    AUTHORITATIVE_BASELINES,
    CANONICAL_TARGET_GIB_PER_HOUR,
    GOLDEN_EXPORT_PHYSICAL_BYTES,
    ORIGINAL_V3_SOURCE_BYTES,
    PHASE1B_ANCHOR_BYTES,
    PHASE1B_COMPATIBILITY_SEGMENT_BYTES,
    PHASE1B_STORAGE_V4_STORE_BYTES,
    PHASE1B_STORE_PLUS_ANCHOR_BYTES,
    PHASE1C_ADVERSARIAL_COMMIT_COUNT,
    PHASE1C_CAPACITY_LEVELS,
    PHASE1C_MANIFEST_PROGRESS_EVERY_COMMITS,
    PHASE1C_NO_CANONICAL_TARGET_VERDICT,
    PHASE1C_PRODUCTION_BATCH_COMMITS,
    PHASE1C_PROVEN_VERDICT,
    PHASE1C_TAIL_COMMIT_COUNT,
    PHASE1C_TAIL_RESTART_SIZES,
    PHASE1C_TARGET_NOT_MET_VERDICT,
    PHASE1C_WORKLOAD_SEED,
    TARGET_MET,
    TARGET_NOT_MET,
    Phase1CRatioComparability,
    Phase1CTargetDecisionRole,
    Phase1CWorkloadProgress,
    Phase1CWorkloadProgressStatus,
    Phase1CWorkloadSuite,
    build_phase1c_baseline_ratio_report,
    build_phase1c_workload_suite,
    decide_phase1c_target_verdict,
)


def _shape() -> GoldenCapacityShape:
    return GoldenCapacityShape(
        golden_root="a" * 64,
        source_sha256="b" * 64,
        commit_count=10,
        logical_row_count=20,
        start_time_ns=1_000_000_000,
        end_time_ns=10_000_000_000,
        cadence_ns=1_000_000_000,
        type_observations=(
            GoldenCapacityTypeObservation(
                record_type="PUBLIC_FUNDING_SETTLEMENT",
                count=2,
                payload_min_bytes=10,
                payload_max_bytes=20,
                distinct_payload_hashes=1,
            ),
            GoldenCapacityTypeObservation(
                record_type="PUBLIC_MARKET_EVENT",
                count=8,
                payload_min_bytes=30,
                payload_max_bytes=60,
                distinct_payload_hashes=8,
            ),
        ),
        strategies=("phase05_cash_and_carry", "phase08_robust_pairs"),
        alert_count=2,
        incident_count=1,
        ledger_transaction_count=1,
        market_gap_count=1,
        alert_payload_bytes=32,
        incident_payload_bytes=64,
        ledger_payload_bytes=96,
        market_gap_payload_bytes=128,
    )


def _census(*, raw: int, paper: int) -> ByteCategoryCensus:
    return ByteCategoryCensus(
        raw_segments_bytes=raw,
        raw_manifests_bytes=3,
        raw_index_bytes=5,
        paper_segments_bytes=paper,
        paper_overlay_bytes=7,
        paper_checkpoints_bytes=11,
        paper_manifests_bytes=13,
        raw_anchors_witnesses_bytes=17,
        paper_anchors_witnesses_bytes=19,
        raw_current_cache_bytes=23,
        paper_current_cache_bytes=29,
        scratch_current_bytes=0,
        scratch_peak_bytes=31,
    )


def _available(*, gib_per_hour: str, passed: bool) -> StorageGrowthAssessment:
    return StorageGrowthAssessment(
        status="AVAILABLE",
        basis="LOGICAL_SPAN",
        gib_per_hour=gib_per_hour,
        bytes_per_hour="1",
        passed=passed,
    )


@pytest.fixture(scope="module")
def production_build() -> tuple[
    Phase1CWorkloadSuite,
    tuple[Phase1CWorkloadProgress, ...],
]:
    progress: list[Phase1CWorkloadProgress] = []
    suite = build_phase1c_workload_suite(
        _shape(),
        progress_callback=progress.append,
    )
    return suite, tuple(progress)


@pytest.fixture(scope="module")
def production_suite(
    production_build: tuple[
        Phase1CWorkloadSuite,
        tuple[Phase1CWorkloadProgress, ...],
    ],
) -> Phase1CWorkloadSuite:
    return production_build[0]


def test_progress_callback_is_bounded_and_does_not_change_manifest_sha(
    production_build: tuple[
        Phase1CWorkloadSuite,
        tuple[Phase1CWorkloadProgress, ...],
    ],
) -> None:
    suite, progress = production_build
    labels = (
        *(f"GOLDEN_SHAPED_{count}" for count in PHASE1C_CAPACITY_LEVELS),
        "BOUNDED_TAIL_RESTART",
        "ADVERSARIAL_STORAGE",
    )
    for label in labels:
        events = tuple(event for event in progress if event.manifest_label == label)
        manifest = next(
            manifest
            for manifest in suite.all_manifests
            if (
                f"GOLDEN_SHAPED_{manifest.commit_count}"
                if manifest.config.profile is CapacityProfile.GOLDEN_SHAPED
                else manifest.config.profile.value
            )
            == label
        )
        config = manifest.config
        expected_logical_rows = config.commit_count + config.market_gap_count + sum(
            config.commit_count // interval
            for interval in (
                config.alert_every_commits,
                config.incident_every_commits,
                config.ledger_every_commits,
                config.projection_every_commits,
            )
            if interval is not None
        )
        assert events[0].status is Phase1CWorkloadProgressStatus.STARTED
        assert events[0].processed_commits == 0
        assert events[0].processed_logical_rows == 0
        assert events[0].workload_elapsed_ns == 0
        assert events[-1].status is Phase1CWorkloadProgressStatus.COMPLETE
        assert events[-1].processed_commits == events[-1].total_commits
        assert events[-1].processed_logical_rows == expected_logical_rows
        assert events[-1].total_logical_rows == expected_logical_rows
        assert events[-1].total_logical_rows == manifest.logical_row_count
        assert all(
            0
            < current.processed_commits - previous.processed_commits
            <= PHASE1C_MANIFEST_PROGRESS_EVERY_COMMITS
            for previous, current in pairwise(events)
        )
        assert all(
            current.processed_logical_rows > previous.processed_logical_rows
            and current.workload_elapsed_ns >= previous.workload_elapsed_ns
            and current.total_logical_rows == previous.total_logical_rows
            for previous, current in pairwise(events)
        )

    callback_manifest = suite.golden_shaped_manifests[0]
    without_callback = build_capacity_workload_manifest(callback_manifest.config)
    assert callback_manifest.workload_sha256 == without_callback.workload_sha256
    assert callback_manifest.sha256 == without_callback.sha256


def test_workload_progress_rejects_inconsistent_row_time_and_status_fields() -> None:
    valid = {
        "manifest_label": "GOLDEN_SHAPED_100000",
        "profile": CapacityProfile.GOLDEN_SHAPED,
        "processed_commits": 10,
        "total_commits": 100,
        "processed_logical_rows": 12,
        "total_logical_rows": 120,
        "workload_elapsed_ns": 1,
        "status": Phase1CWorkloadProgressStatus.IN_PROGRESS,
    }
    Phase1CWorkloadProgress(**valid)  # type: ignore[arg-type]

    for replacement, match in (
        ({"processed_logical_rows": True}, "logical row counts"),
        ({"processed_logical_rows": 5}, "logical row counts"),
        ({"workload_elapsed_ns": -1}, "workload_elapsed_ns"),
        (
            {
                "processed_commits": 0,
                "processed_logical_rows": 0,
                "status": Phase1CWorkloadProgressStatus.IN_PROGRESS,
            },
            "IN_PROGRESS",
        ),
        (
            {
                "processed_commits": 100,
                "processed_logical_rows": 119,
                "status": Phase1CWorkloadProgressStatus.COMPLETE,
            },
            "COMPLETE",
        ),
    ):
        with pytest.raises(ValueError, match=match):
            Phase1CWorkloadProgress(**{**valid, **replacement})  # type: ignore[arg-type]


def test_production_suite_freezes_exact_profiles_levels_seed_and_markers(
    production_suite: Phase1CWorkloadSuite,
) -> None:
    assert production_suite.seed == PHASE1C_WORKLOAD_SEED
    assert tuple(production_suite.capacity_level_manifests) == PHASE1C_CAPACITY_LEVELS
    assert (
        tuple(manifest.config.profile for manifest in production_suite.golden_shaped_manifests)
        == (CapacityProfile.GOLDEN_SHAPED,) * 3
    )
    assert all(
        manifest.config.golden_census_sha256 == _shape().sha256
        for manifest in production_suite.golden_shaped_manifests
    )
    assert all(
        tuple(manifest.payload()["markers"]) == CAPACITY_MARKERS
        for manifest in production_suite.all_manifests
    )
    assert production_suite.payload()["production_boundaries"] == {
        "batch_commits": 10_000,
        "checkpoint_every_batches": 1,
    }
    prefix_proof = production_suite.payload()["golden_shaped_cumulative_prefix_proof"]
    assert prefix_proof["artifact"] == "STORAGE_V4_PHASE1C_CUMULATIVE_PREFIX_PLAN_V1"
    assert prefix_proof["execution_contract"] == {
        "commits_generated": 1_000_000,
        "commits_ingested": 1_000_000,
        "prefix_commits_reingested": 0,
        "store_count": 1,
        "stream_count": 1,
        "worker_count": 1,
    }
    assert [item["commit_count"] for item in prefix_proof["boundaries"]] == list(
        PHASE1C_CAPACITY_LEVELS
    )
    assert prefix_proof["configuration_compatibility"]["market_gap_schedule"] == (
        "EXACT_TERMINAL_PREFIX"
    )


def test_tail_manifest_is_fact_derived_and_has_exact_distinct_restart_sizes(
    production_suite: Phase1CWorkloadSuite,
) -> None:
    tail = production_suite.bounded_tail_restart_manifest
    assert tail.config.profile is CapacityProfile.BOUNDED_TAIL_RESTART
    assert tail.commit_count == PHASE1C_TAIL_COMMIT_COUNT == 20_001
    assert tail.config.bounded_tail_max == 20_000
    assert tail.config.tail_restart_sizes == PHASE1C_TAIL_RESTART_SIZES
    assert len(set(tail.config.tail_restart_sizes)) == 5
    assert tail.config.golden_census_sha256 == _shape().sha256
    assert tuple(
        (
            item.record_type,
            item.payload_min_bytes,
            item.payload_max_bytes,
            item.payload_cardinality,
        )
        for item in tail.config.type_distribution
    ) == tuple(
        (
            item.record_type,
            item.payload_min_bytes,
            item.payload_max_bytes,
            item.distinct_payload_hashes,
        )
        for item in _shape().type_observations
    )


def test_adversarial_manifest_exercises_required_storage_edges(
    production_suite: Phase1CWorkloadSuite,
) -> None:
    config = production_suite.adversarial_storage_manifest.config
    assert config.profile is CapacityProfile.ADVERSARIAL_STORAGE
    assert config.commit_count == PHASE1C_ADVERSARIAL_COMMIT_COUNT == 20_000
    public = tuple(item for item in config.type_distribution if item.record_type.startswith("PUBLIC_"))
    assert len(public) == 3
    assert sum(item.payload_cardinality >= 10_000 for item in public) == 2
    funding = next(item for item in public if "FUNDING" in item.record_type)
    assert funding.payload_cardinality == 1
    assert max(item.payload_max_bytes for item in public) == MAX_SYNTHETIC_PAYLOAD_BYTES
    assert min(item.payload_min_bytes for item in public) == 1
    assert all(item.payload_min_bytes < item.payload_max_bytes for item in public)
    assert config.alert_every_commits == PHASE1C_PRODUCTION_BATCH_COMMITS
    assert config.incident_every_commits == PHASE1C_ADVERSARIAL_COMMIT_COUNT
    assert config.ledger_every_commits == PHASE1C_PRODUCTION_BATCH_COMMITS
    assert config.projection_every_commits == PHASE1C_PRODUCTION_BATCH_COMMITS
    assert config.market_gap_count == 2
    assert config.adversarial_boundary_intervals == (10_000, 20_000)

    maximum_probe_commits = tuple(
        commit
        for commit in iter_capacity_commits(config)
        if commit.rows[0].payload.size_bytes == MAX_SYNTHETIC_PAYLOAD_BYTES
    )
    assert tuple(commit.sequence for commit in maximum_probe_commits) == (
        1_866,
        4_592,
        10_000,
        20_000,
    )
    boundary_commits = tuple(
        commit
        for commit in maximum_probe_commits
        if commit.sequence in config.adversarial_boundary_intervals
    )
    assert tuple(commit.sequence for commit in boundary_commits) == (10_000, 20_000)
    assert all(
        commit.rows[0].record_type == "PUBLIC_SOURCE_FAILURE"
        and commit.rows[0].payload.size_bytes == MAX_SYNTHETIC_PAYLOAD_BYTES
        for commit in boundary_commits
    )


def test_ratio_report_uses_exact_denominators_without_equivalence_claim() -> None:
    golden = _census(raw=100, paper=50)
    levels = {
        100_000: _census(raw=200, paper=75),
        500_000: _census(raw=300, paper=100),
        1_000_000: _census(raw=400, paper=125),
    }
    report = build_phase1c_baseline_ratio_report(
        golden_census=golden,
        level_censuses=levels,
    )

    assert ORIGINAL_V3_SOURCE_BYTES == 2_014_072_832
    assert GOLDEN_EXPORT_PHYSICAL_BYTES == 2_456_283_751
    assert PHASE1B_STORAGE_V4_STORE_BYTES == 528_250_030
    assert PHASE1B_ANCHOR_BYTES == 12_288
    assert PHASE1B_STORE_PLUS_ANCHOR_BYTES == 528_262_318
    assert PHASE1B_COMPATIBILITY_SEGMENT_BYTES == 317_492_777
    assert report.baselines == AUTHORITATIVE_BASELINES
    golden_export_baseline = next(
        baseline
        for baseline in report.baselines
        if baseline.label == "GOLDEN_EXPORT_PHYSICAL_BYTES"
    )
    assert golden_export_baseline.semantic_basis == (
        "GOLDEN_V3_EXPORT_PAYLOAD_SHARDS_PHYSICAL_BYTES_EXCLUDING_CONTROLS"
    )
    assert tuple(item.commit_count for item in report.observations) == (
        252_262,
        *PHASE1C_CAPACITY_LEVELS,
    )
    assert all(
        ratio.comparability is Phase1CRatioComparability.NON_LIKE_FOR_LIKE_DIAGNOSTIC
        for observation in report.observations
        for ratio in observation.ratios
    )
    assert all(
        "diagnostic only" in ratio.limitation
        for observation in report.observations
        for ratio in observation.ratios
    )
    first = report.observations[0]
    assert first.ratios[0].numerator_bytes == golden.total_bytes
    assert first.ratios[0].denominator.byte_count == ORIGINAL_V3_SOURCE_BYTES
    assert first.ratios[-1].numerator_bytes == 150
    assert report.payload()["comparison_policy"] == (
        "ALL_RATIOS_ARE_NON_LIKE_FOR_LIKE_DIAGNOSTICS_NO_CAPACITY_GATE"
    )

    with pytest.raises(ValueError, match="exactly 100k, 500k, and 1m"):
        build_phase1c_baseline_ratio_report(
            golden_census=golden,
            level_censuses={100_000: levels[100_000]},
        )


def test_terminal_verdict_depends_only_on_strict_1m_assessment() -> None:
    golden_miss = _available(gib_per_hour="0.25", passed=False)
    levels = {
        100_000: _available(gib_per_hour="0.10", passed=True),
        500_000: _available(gib_per_hour="0.30", passed=False),
        1_000_000: _available(gib_per_hour="0.199999999999", passed=True),
    }
    proven = decide_phase1c_target_verdict(
        golden_assessment=golden_miss,
        level_assessments=levels,
    )
    assert proven.canonical_target_gib_per_hour == CANONICAL_TARGET_GIB_PER_HOUR
    assert proven.terminal_target_status == TARGET_MET
    assert proven.terminal_verdict == PHASE1C_PROVEN_VERDICT
    assert tuple(item.target_status for item in proven.diagnostics) == (
        TARGET_NOT_MET,
        TARGET_MET,
        TARGET_NOT_MET,
        TARGET_MET,
    )
    assert tuple(item.role for item in proven.diagnostics) == (
        Phase1CTargetDecisionRole.DIAGNOSTIC_ONLY,
        Phase1CTargetDecisionRole.DIAGNOSTIC_ONLY,
        Phase1CTargetDecisionRole.DIAGNOSTIC_ONLY,
        Phase1CTargetDecisionRole.TERMINAL_DECISION,
    )

    exact_boundary = replace(
        levels[1_000_000],
        gib_per_hour="0.20",
        passed=False,
    )
    missed = decide_phase1c_target_verdict(
        golden_assessment=golden_miss,
        level_assessments={**levels, 1_000_000: exact_boundary},
    )
    assert missed.terminal_target_status == TARGET_NOT_MET
    assert missed.terminal_verdict == PHASE1C_TARGET_NOT_MET_VERDICT


def test_no_target_is_generic_and_invalid_assessments_fail_closed() -> None:
    unavailable = StorageGrowthAssessment(
        status="UNAVAILABLE_SPAN_AND_RATE_UNDEFINED",
        basis="UNAVAILABLE",
        gib_per_hour=None,
        bytes_per_hour=None,
        passed=None,
    )
    no_target = decide_phase1c_target_verdict(
        golden_assessment=unavailable,
        level_assessments={count: unavailable for count in PHASE1C_CAPACITY_LEVELS},
        canonical_target_gib_per_hour=None,
    )
    assert no_target.terminal_verdict == PHASE1C_NO_CANONICAL_TARGET_VERDICT
    assert no_target.terminal_target_status is None
    assert all(item.target_status is None for item in no_target.diagnostics)

    with pytest.raises(ValueError, match="1m Golden-shaped assessment"):
        decide_phase1c_target_verdict(
            golden_assessment=unavailable,
            level_assessments={count: unavailable for count in PHASE1C_CAPACITY_LEVELS},
        )
    with pytest.raises(ValueError, match="exact boolean result"):
        decide_phase1c_target_verdict(
            golden_assessment=replace(
                _available(gib_per_hour="0.10", passed=True),
                passed=None,
            ),
            level_assessments={
                count: _available(gib_per_hour="0.10", passed=True) for count in PHASE1C_CAPACITY_LEVELS
            },
        )
    with pytest.raises(ValueError, match=r"strict canonical 0\.20"):
        decide_phase1c_target_verdict(
            golden_assessment=unavailable,
            level_assessments={count: unavailable for count in PHASE1C_CAPACITY_LEVELS},
            canonical_target_gib_per_hour=Decimal("0.30"),
        )
