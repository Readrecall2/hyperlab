from __future__ import annotations

import hashlib
import json
import zlib
from dataclasses import replace
from itertools import islice
from pathlib import Path

import pytest

from hyperlab.paper.storage_v4.capacity import (
    CAPACITY_MARKERS,
    GIB_BYTES,
    NOT_ALPHA_EVIDENCE,
    NOT_ECONOMIC_EVIDENCE,
    PAPER_ONLY,
    SYNTHETIC_CAPACITY_WORKLOAD,
    ByteCategoryCensus,
    CapacityBytePaths,
    CapacityMeasurement,
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
    CapacityWorkloadHasher,
    DurationObservations,
    assess_storage_growth,
    build_capacity_complete_artifact,
    build_capacity_report_artifact,
    build_capacity_workload_manifest,
    census_byte_categories,
    compute_capacity_scaling,
    iter_capacity_commits,
    run_capacity_workload,
)


def _config(
    *,
    profile: CapacityProfile = CapacityProfile.GOLDEN_SHAPED,
    commit_count: int = 10,
    seed: int = 17,
) -> CapacityWorkloadConfig:
    return CapacityWorkloadConfig(
        profile=profile,
        seed=seed,
        commit_count=commit_count,
        start_time_ns=1_700_000_000_000_000_000,
        cadence_ns=1_000_000_000,
        type_distribution=(
            CapacityTypeSpec(
                record_type="PUBLIC_BBO",
                stream="inbox",
                weight=3,
                payload_min_bytes=1,
                payload_max_bytes=17,
                payload_cardinality=1,
            ),
            CapacityTypeSpec(
                record_type="PUBLIC_FUNDING",
                stream="events",
                weight=1,
                payload_min_bytes=5,
                payload_max_bytes=29,
                payload_cardinality=101,
            ),
        ),
        strategies=("phase05_cash_and_carry", "phase08_lead_lag"),
        alert_every_commits=3,
        incident_every_commits=5,
        ledger_every_commits=2,
        market_gap_count=1,
        alert_payload_bytes=11,
        incident_payload_bytes=13,
        ledger_payload_bytes=19,
        market_gap_payload_bytes=23,
        golden_census_sha256=("a" * 64 if profile is CapacityProfile.GOLDEN_SHAPED else None),
        bounded_tail_max=(20_000 if profile is CapacityProfile.BOUNDED_TAIL_RESTART else None),
        projection_every_commits=(4 if profile is CapacityProfile.ADVERSARIAL_STORAGE else None),
        projection_payload_bytes=7,
        adversarial_boundary_intervals=(3, 5) if profile is CapacityProfile.ADVERSARIAL_STORAGE else (),
    )


def _empty_census(*, raw_segments_bytes: int = 0, paper_segments_bytes: int = 0) -> ByteCategoryCensus:
    return ByteCategoryCensus(
        raw_segments_bytes=raw_segments_bytes,
        raw_manifests_bytes=0,
        raw_index_bytes=0,
        paper_segments_bytes=paper_segments_bytes,
        paper_overlay_bytes=0,
        paper_checkpoints_bytes=0,
        paper_manifests_bytes=0,
        raw_anchors_witnesses_bytes=0,
        paper_anchors_witnesses_bytes=0,
        raw_current_cache_bytes=0,
        paper_current_cache_bytes=0,
        scratch_current_bytes=0,
        scratch_peak_bytes=0,
    )


def _measurement(
    *,
    commit_count: int,
    row_count: int,
    wall_ns: int,
    total_bytes: int,
    startup_ns: int,
    tail_entries: int,
) -> CapacityMeasurement:
    return CapacityMeasurement(
        workload_manifest_sha256="b" * 64,
        observed_workload_sha256="c" * 64,
        commit_count=commit_count,
        logical_row_count=row_count,
        wall_ns=wall_ns,
        cpu_ns=wall_ns // 2,
        peak_rss_bytes=1_000_000 + commit_count,
        byte_census=_empty_census(raw_segments_bytes=total_bytes),
        segment_count=max(1, commit_count // 10),
        checkpoint_count=max(1, commit_count // 20),
        manifest_count=max(1, commit_count // 20),
        startup_ns=startup_ns,
        startup_historical_segments_read=1,
        startup_historical_commits_replayed=0,
        startup_tail_entries_replayed=tail_entries,
        metadata_authentication_ns=101,
        full_history_audit_ns=202,
        seal_durations=DurationObservations(tuple(range(1, 101))),
        checkpoint_durations=DurationObservations((11, 13)),
        manifest_publish_durations=DurationObservations((17, 19)),
        logical_span_ns=3_600_000_000_000,
        commits_per_hour=None,
        raw_input_bytes=max(1, total_bytes // 2),
    )


def test_streaming_workload_is_deterministic_lazy_and_visibly_synthetic() -> None:
    config = _config()
    first_manifest = build_capacity_workload_manifest(config)
    second_manifest = build_capacity_workload_manifest(config)

    assert CAPACITY_MARKERS == (
        SYNTHETIC_CAPACITY_WORKLOAD,
        NOT_ECONOMIC_EVIDENCE,
        NOT_ALPHA_EVIDENCE,
        PAPER_ONLY,
    )
    assert first_manifest.canonical_bytes == second_manifest.canonical_bytes
    assert first_manifest.sha256 == hashlib.sha256(first_manifest.canonical_bytes).hexdigest()
    assert first_manifest.commit_count == 10
    assert first_manifest.logical_row_count == 21

    payload = json.loads(first_manifest.canonical_bytes)
    assert payload["markers"] == list(CAPACITY_MARKERS)
    assert payload["profile"] == "GOLDEN_SHAPED"
    assert payload["golden_census_sha256"] == "a" * 64
    assert payload["expected"]["workload_sha256"] == first_manifest.workload_sha256
    assert payload["activity_rates"]["market_gap_count"] == 1

    huge_stream = iter_capacity_commits(replace(config, commit_count=1_000_000))
    assert iter(huge_stream) is huge_stream
    first_two = tuple(islice(huge_stream, 2))
    assert len(first_two) == 2
    assert first_two[0].rows[0].payload.size_bytes in range(1, 18)
    chunks = tuple(first_two[0].rows[0].payload.iter_chunks(chunk_size=3))
    assert sum(map(len, chunks)) == first_two[0].rows[0].payload.size_bytes
    assert b"".join(chunks) == first_two[0].rows[0].payload.to_bytes()
    assert first_two[0].rows[0].descriptor()["markers"] == list(CAPACITY_MARKERS)

    hasher = CapacityWorkloadHasher()
    prefix_snapshot = None
    prefix_stream_sequences: dict[str, int] = {}
    for commit in iter_capacity_commits(config):
        hasher.update(commit)
        if commit.sequence == 5:
            prefix_snapshot = hasher.snapshot()
            prefix_stream_sequences = {
                row.stream: row.source_sequence
                for prior in islice(iter_capacity_commits(config), 5)
                for row in prior.rows
            }
    observed = hasher.finalize()
    assert prefix_snapshot is not None
    assert prefix_snapshot.commit_count == 5
    assert prefix_snapshot == build_capacity_workload_manifest(
        replace(config, commit_count=5)
    ).digest
    resumed = CapacityWorkloadHasher.resume_from_prefix(prefix_snapshot)
    for commit in iter_capacity_commits(
        config,
        start_sequence=6,
        initial_stream_sequences=prefix_stream_sequences,
    ):
        resumed.update(commit)
    assert resumed.finalize() == observed
    assert observed.sha256 == first_manifest.workload_sha256
    assert observed.commit_count == first_manifest.commit_count
    assert observed.logical_row_count == first_manifest.logical_row_count
    assert build_capacity_workload_manifest(replace(config, seed=18)).workload_sha256 != (
        first_manifest.workload_sha256
    )

    with pytest.raises(ValueError, match="authenticated stream sequences"):
        tuple(iter_capacity_commits(config, start_sequence=2))

    high_entropy = replace(first_two[0].rows[0].payload, size_bytes=1024 * 1024)
    materialized = high_entropy.to_bytes()
    assert b"".join(high_entropy.iter_chunks(chunk_size=997)) == materialized
    assert len(zlib.compress(materialized, level=6)) > int(len(materialized) * 0.98)


def test_profile_contracts_cover_adversarial_edges_and_bounded_restart_tails() -> None:
    adversarial = _config(profile=CapacityProfile.ADVERSARIAL_STORAGE)
    first = tuple(islice(iter_capacity_commits(adversarial), 10))
    base_sizes = [commit.rows[0].payload.size_bytes for commit in first]
    selected_bounds = {
        bound
        for spec in adversarial.type_distribution
        for bound in (spec.payload_min_bytes, spec.payload_max_bytes)
    }
    assert set(base_sizes).issubset(selected_bounds)
    assert len(set(base_sizes)) >= 2
    assert all("FUNDING" in commit.rows[0].record_type for commit in first[:8])
    assert any(
        row.stream == "projection_history"
        and row.record_type == "SYNTHETIC_PROJECTION_ACTIVITY"
        for commit in first
        for row in commit.rows
    )
    manifest_payload = build_capacity_workload_manifest(adversarial).payload()
    assert manifest_payload["activity_rates"]["projection_every_commits"] == 4
    assert manifest_payload["configuration"]["adversarial_schedule"] == {
        "boundary_intervals": [3, 5],
        "funding_burst_period": 97,
        "funding_burst_width": 8,
    }

    bounded = _config(
        profile=CapacityProfile.BOUNDED_TAIL_RESTART,
        commit_count=20_001,
    )
    assert bounded.tail_restart_sizes == (0, 1, 100, 10_000, 20_000)

    with pytest.raises(ValueError, match="golden_census_sha256"):
        replace(_config(), golden_census_sha256=None)
    with pytest.raises(ValueError, match="bounded_tail_max"):
        replace(bounded, bounded_tail_max=9_999)
    with pytest.raises(ValueError, match="high-cardinality"):
        replace(
            adversarial,
            type_distribution=tuple(
                replace(spec, payload_cardinality=1)
                for spec in adversarial.type_distribution
            ),
        )
    with pytest.raises(ValueError, match="funding input"):
        replace(
            adversarial,
            type_distribution=tuple(
                replace(spec, record_type=spec.record_type.replace("FUNDING", "RATE"))
                for spec in adversarial.type_distribution
            ),
        )


def test_byte_category_census_is_exact_and_rejects_double_counting(tmp_path: Path) -> None:
    files: dict[str, Path] = {}
    for name, size in {
        "raw_segment": 2,
        "raw_manifest": 3,
        "raw_index": 5,
        "paper_segment": 7,
        "paper_overlay": 11,
        "paper_checkpoint": 13,
        "paper_manifest": 17,
        "anchor": 19,
        "current": 23,
        "scratch": 29,
    }.items():
        path = tmp_path / name
        path.write_bytes(b"x" * size)
        files[name] = path

    paths = CapacityBytePaths(
        raw_segments=(files["raw_segment"],),
        raw_manifests=(files["raw_manifest"],),
        raw_index=(files["raw_index"],),
        paper_segments=(files["paper_segment"],),
        paper_overlay=(files["paper_overlay"],),
        paper_checkpoints=(files["paper_checkpoint"],),
        paper_manifests=(files["paper_manifest"],),
        raw_anchors_witnesses=(files["anchor"],),
        paper_current_cache=(files["current"],),
        scratch=(files["scratch"],),
    )
    census = census_byte_categories(paths, scratch_peak_bytes=31)

    assert census.raw_bytes == 29
    assert census.paper_incremental_bytes == 71
    assert census.total_bytes == 100
    assert census.scratch_current_bytes == 29
    assert census.total_with_current_scratch_bytes == 129
    assert census.payload()["total_bytes"] == 100
    assert census.payload()["category_shares_status"] == "AVAILABLE"
    assert census.payload()["category_shares_of_total"] == {
        "paper_checkpoints_bytes": "0.13",
        "paper_anchors_witnesses_bytes": "0",
        "paper_current_cache_bytes": "0.23",
        "paper_manifests_bytes": "0.17",
        "paper_overlay_bytes": "0.11",
        "paper_segments_bytes": "0.07",
        "raw_index_bytes": "0.05",
        "raw_anchors_witnesses_bytes": "0.19",
        "raw_current_cache_bytes": "0",
        "raw_manifests_bytes": "0.03",
        "raw_segments_bytes": "0.02",
    }

    reclassified = census_byte_categories(
        replace(
            paths,
            raw_embedded_index_bytes=((files["raw_segment"], 1),),
        ),
        scratch_peak_bytes=31,
    )
    assert reclassified.raw_segments_bytes == 1
    assert reclassified.raw_index_bytes == 6
    assert reclassified.total_bytes == census.total_bytes

    with pytest.raises(ValueError, match="smaller than the physical raw segment"):
        census_byte_categories(
            replace(
                paths,
                raw_embedded_index_bytes=((files["raw_segment"], 2),),
            ),
            scratch_peak_bytes=31,
        )
    second_segment = tmp_path / "second-raw-segment"
    second_segment.write_bytes(b"second")
    with pytest.raises(ValueError, match="lacks authenticated embedded index"):
        census_byte_categories(
            replace(
                paths,
                raw_segments=(files["raw_segment"], second_segment),
                raw_embedded_index_bytes=((files["raw_segment"], 1),),
            ),
            scratch_peak_bytes=31,
        )
    with pytest.raises(ValueError, match="were not witnessed as raw segment"):
        census_byte_categories(
            replace(
                paths,
                raw_segments=(),
                raw_embedded_index_bytes=((tmp_path / "absent.hl4r", 1),),
            ),
            scratch_peak_bytes=31,
        )

    with pytest.raises(ValueError, match="more than one byte category"):
        census_byte_categories(
            replace(paths, paper_segments=(files["raw_segment"],)),
            scratch_peak_bytes=31,
        )

    candidate = tmp_path / "candidate"
    candidate.mkdir()
    assigned = candidate / "assigned"
    assigned.write_bytes(b"assigned")
    unexpected = candidate / "unexpected"
    unexpected.write_bytes(b"unexpected")
    exhaustive_paths = CapacityBytePaths(raw_segments=(assigned,))
    with pytest.raises(ValueError, match="does not exactly cover candidate_root"):
        census_byte_categories(
            exhaustive_paths,
            scratch_peak_bytes=0,
            candidate_root=candidate.resolve(),
        )
    unexpected.unlink()
    exhaustive = census_byte_categories(
        exhaustive_paths,
        scratch_peak_bytes=0,
        candidate_root=candidate.resolve(),
    )
    assert exhaustive.total_bytes == len(b"assigned")


def test_percentiles_require_enough_observations_and_use_nearest_rank() -> None:
    insufficient = DurationObservations(tuple(range(1, 100))).payload()
    assert insufficient["percentiles_status"] == "UNAVAILABLE_INSUFFICIENT_OBSERVATIONS"
    assert "p50_ns" not in insufficient
    assert insufficient["observations_ns"] == list(range(1, 100))

    sufficient = DurationObservations(tuple(range(1, 101))).payload()
    assert sufficient["percentiles_status"] == "AVAILABLE_NEAREST_RANK"
    assert sufficient["p50_ns"] == 50
    assert sufficient["p95_ns"] == 95
    assert sufficient["p99_ns"] == 99

    with pytest.raises(ValueError, match="non-negative"):
        DurationObservations((1, -1))


def test_storage_growth_target_is_unavailable_without_span_or_rate_and_strict_when_available() -> None:
    unavailable = assess_storage_growth(total_bytes=100, commit_count=10)
    assert unavailable.status == "UNAVAILABLE_SPAN_AND_RATE_UNDEFINED"
    assert unavailable.passed is None
    assert unavailable.gib_per_hour is None

    just_below = assess_storage_growth(
        total_bytes=GIB_BYTES // 5,
        commit_count=10,
        logical_span_ns=3_600_000_000_000,
    )
    assert just_below.status == "AVAILABLE"
    assert just_below.passed is True

    just_above = assess_storage_growth(
        total_bytes=(GIB_BYTES // 5) + 1,
        commit_count=10,
        logical_span_ns=3_600_000_000_000,
    )
    assert just_above.passed is False

    rate_based = assess_storage_growth(
        total_bytes=GIB_BYTES // 10,
        commit_count=100,
        commits_per_hour=100,
    )
    assert rate_based.basis == "COMMITS_PER_HOUR"
    assert rate_based.passed is True


def test_measurements_scaling_and_canonical_artifacts_keep_exact_links() -> None:
    manifest = build_capacity_workload_manifest(_config())
    small = _measurement(
        commit_count=100,
        row_count=210,
        wall_ns=1_000,
        total_bytes=1_000,
        startup_ns=100,
        tail_entries=1,
    )
    large = _measurement(
        commit_count=500,
        row_count=1_050,
        wall_ns=4_000,
        total_bytes=4_500,
        startup_ns=110,
        tail_entries=100,
    )
    scaling = compute_capacity_scaling((small, large))
    assert scaling["status"] == "AVAILABLE"
    assert scaling["transitions"][0]["wall_multiplier"] == "4"
    assert scaling["transitions"][0]["bytes_per_commit_multiplier"] == "0.9"

    linked_small = replace(
        _measurement(
            commit_count=manifest.commit_count,
            row_count=manifest.logical_row_count,
            wall_ns=1_000,
            total_bytes=1_000,
            startup_ns=100,
            tail_entries=1,
        ),
        workload_manifest_sha256=manifest.sha256,
        observed_workload_sha256=manifest.workload_sha256,
    )
    report = build_capacity_report_artifact(
        status="CAPACITY_MEASURED",
        manifest=manifest,
        measurement=linked_small,
        scaling=scaling,
        limitations=("WINDOWS_ONLY",),
    )
    report_payload = json.loads(report.canonical_bytes)
    assert report_payload["markers"] == list(CAPACITY_MARKERS)
    assert report_payload["manifest"]["sha256"] == manifest.sha256
    assert report_payload["measurement"]["throughput"]["gib_per_million_commits"]
    assert report_payload["measurement"]["byte_census"]["category_shares_status"] == (
        "AVAILABLE"
    )
    assert report_payload["measurement"]["write_amplification"]["status"] == (
        "UNAVAILABLE_CUMULATIVE_BYTES_WRITTEN_NOT_MEASURED"
    )
    assert report.sha256 == hashlib.sha256(report.canonical_bytes).hexdigest()

    complete = build_capacity_complete_artifact(
        status="CAPACITY_MEASURED",
        manifest=manifest,
        report=report,
    )
    complete_payload = json.loads(complete.canonical_bytes)
    assert complete_payload["markers"] == list(CAPACITY_MARKERS)
    assert complete_payload["report_sha256"] == report.sha256
    assert complete_payload["manifest_sha256"] == manifest.sha256


def test_runner_boundary_consumes_a_fresh_stream_and_validates_observed_digest() -> None:
    config = _config(commit_count=7)

    class FakeRunner:
        def run_capacity_workload(self, *, manifest, commits):  # type: ignore[no-untyped-def]
            hasher = CapacityWorkloadHasher()
            for commit in commits:
                hasher.update(commit)
            digest = hasher.finalize()
            return CapacityMeasurement(
                workload_manifest_sha256=manifest.sha256,
                observed_workload_sha256=digest.sha256,
                commit_count=digest.commit_count,
                logical_row_count=digest.logical_row_count,
                wall_ns=1,
                cpu_ns=1,
                peak_rss_bytes=None,
                byte_census=_empty_census(),
                segment_count=0,
                checkpoint_count=0,
                manifest_count=0,
                startup_ns=1,
                startup_historical_segments_read=0,
                startup_historical_commits_replayed=0,
                startup_tail_entries_replayed=0,
                metadata_authentication_ns=1,
                full_history_audit_ns=1,
            )

    result = run_capacity_workload(config=config, runner=FakeRunner())
    assert result.manifest.commit_count == result.measurement.commit_count == 7
    assert result.manifest.workload_sha256 == result.measurement.observed_workload_sha256
