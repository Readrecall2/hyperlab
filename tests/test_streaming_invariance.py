from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

import pandas as pd
import pyarrow.parquet as pq
import pytest

from hyperlab.analysis import streaming_lake
from hyperlab.analysis.lead_lag import (
    LeadLagConfig,
    StrictInterval,
    analyze_lead_lag,
)
from hyperlab.analysis.streaming import (
    run_bounded_analysis_in_staging,
    run_bounded_lead_lag_study,
)
from hyperlab.analysis.streaming_kernel import run_streaming_kernel
from hyperlab.analysis.streaming_reporting import (
    StreamingEventArtifact,
    StreamingLeadLagAnalysis,
    write_streaming_metadata_artifacts,
)
from hyperlab.analysis.streaming_store import (
    PHASE10_EVENT_COLUMNS,
    EventSpool,
    ExactTimestampNs,
    SourceRowSpool,
)
from hyperlab.analysis.synthetic import (
    SyntheticLeadLagFixture,
    generate_synthetic_lead_lag_dataset,
)

_EVENT_ORDER = (
    "signal_time",
    "asset",
    "signal_family",
    "horizon_ms",
    "signal_id",
    "row_kind",
    "execution_scenario",
    "execution_model",
)
_TIMESTAMP_COLUMNS = (
    "signal_time",
    "target_time",
    "time_bucket",
    "interval_start",
    "interval_end",
    "baseline_time",
    "response_state_time",
    "entry_time",
    "exit_time",
)
_SELECTED_MANIFEST_BYTES = b'{"relative_data_path":"synthetic-part.parquet"}\n'


@dataclass(slots=True)
class _SyntheticWindow:
    root: Path
    start: datetime
    end: datetime
    assets: tuple[str, ...]
    intervals: tuple[StrictInterval, ...]
    manifest_fingerprint: str
    source_spool: SourceRowSpool
    observability: dict[str, object]
    gate_report_sha256: str = "1" * 64
    semantic_gate_sha256: str = "2" * 64
    semantic_gate_canonicalizer_version: str = "phase10_semantic_gate_payload_v1"
    excluded_json_pointers: tuple[str, ...] = ("/observability",)
    selected_manifests_sha256: str = hashlib.sha256(_SELECTED_MANIFEST_BYTES).hexdigest()
    selected_manifest_count: int = 1


@dataclass(frozen=True, slots=True)
class _CompletedRun:
    analysis: StreamingLeadLagAnalysis
    event_artifact: StreamingEventArtifact
    observability: dict[str, object]
    paths: dict[str, Path]


def _compact_fixture() -> tuple[
    SyntheticLeadLagFixture,
    dict[str, pd.DataFrame],
    tuple[StrictInterval, ...],
]:
    fixture = generate_synthetic_lead_lag_dataset(
        event_count=32,
        injected_lag_ms=250,
        seed=20260815,
    )
    end = pd.Timestamp(fixture.signal_times[0]) + pd.Timedelta(seconds=6)
    frames = {
        kind: frame.loc[pd.to_datetime(frame["received_time"], utc=True) < end].copy().reset_index(drop=True)
        for kind, frame in (
            ("bbo", fixture.dataset.bbo),
            ("trade", fixture.dataset.trades),
            ("l2", fixture.dataset.l2),
        )
    }
    interval = StrictInterval(
        start=fixture.intervals[0].start,
        end=end.to_pydatetime(),
        tag="synthetic-compact-invariance",
    )
    return fixture, frames, (interval,)


def _populate_source(
    spool: SourceRowSpool,
    frames: dict[str, pd.DataFrame],
    *,
    fragmented: bool,
) -> None:
    ordered = (("bbo", frames["bbo"]), ("trade", frames["trade"]), ("l2", frames["l2"]))
    if not fragmented:
        for manifest_order, (kind, frame) in enumerate(ordered):
            spool.add_rows(
                kind=kind,
                rows=frame.to_dict(orient="records"),
                manifest_order=manifest_order,
                first_row_order=0,
            )
        return

    # Deliberately change both insertion order and physical fragment boundaries.
    # The source spool must recover the same complete received-time batches from
    # semantic ordering keys, including multi-level L2 frames split across calls.
    for kind_position, (kind, frame) in enumerate(reversed(ordered)):
        rows = frame.to_dict(orient="records")
        fragments = [rows[offset::3] for offset in range(3)]
        for fragment_position, fragment in enumerate(reversed(fragments)):
            spool.add_rows(
                kind=kind,
                rows=list(reversed(fragment)),
                manifest_order=100 + kind_position * 10 + fragment_position,
                first_row_order=0,
            )


def _config(*, writer_buffer_rows: int, parquet_row_group_rows: int) -> LeadLagConfig:
    return LeadLagConfig(
        randomization_resamples=19,
        minimum_events=30,
        max_source_rows_per_chunk=10_000,
        max_simultaneous_batch_rows=1_000,
        max_l2_frame_levels=100,
        max_l2_levels_per_chunk=10_000,
        max_pending_response_states=100_000,
        max_pending_execution_states=200_000,
        quantile_sort_run_rows=101,
        parquet_row_group_rows=parquet_row_group_rows,
        writer_buffer_rows=writer_buffer_rows,
        scratch_low_watermark_bytes=1,
        scratch_reserve_bytes=1,
    )


def _make_window(
    *,
    root: Path,
    scratch_dir: Path,
    selected_manifests_path: Path,
    fragmented: bool,
) -> _SyntheticWindow:
    _fixture, frames, intervals = _compact_fixture()
    scratch_dir.mkdir(parents=True, exist_ok=True)
    selected_manifests_path.write_bytes(_SELECTED_MANIFEST_BYTES)
    spool = SourceRowSpool(scratch_dir / "source.sqlite3")
    _populate_source(spool, frames, fragmented=fragmented)
    rows_scanned = sum(len(frame) for frame in frames.values())
    return _SyntheticWindow(
        root=root,
        start=intervals[0].start,
        end=intervals[-1].end,
        assets=("BTC", "ETH"),
        intervals=intervals,
        manifest_fingerprint="4" * 64,
        source_spool=spool,
        observability={
            "rows_scanned": rows_scanned,
            "files_scanned": 3,
            "clock_sync_diagnostics": {"usage": "DIAGNOSTIC_ONLY_STRICT_INTERVALS_DEFINE_CAUSAL_VALIDITY"},
        },
    )


def _completed_run(
    base: Path,
    *,
    fragmented: bool,
    config: LeadLagConfig,
) -> _CompletedRun:
    root = base / "lake"
    root.mkdir(parents=True)
    staging = base / "staging"
    scratch = staging / ".scratch"
    scratch.mkdir(parents=True)
    window = _make_window(
        root=root,
        scratch_dir=scratch,
        selected_manifests_path=staging / "selected_manifests.jsonl",
        fragmented=fragmented,
    )
    try:
        analysis, event_artifact, observability = run_bounded_analysis_in_staging(
            window=window,
            config=config,
            staging=staging,
        )
        paths = write_streaming_metadata_artifacts(
            staging=staging,
            analysis=analysis,
            window=window,
            config=config,
            event_artifact=event_artifact,
            resource_observability=observability,
        )
    finally:
        window.source_spool.close()
    return _CompletedRun(
        analysis=analysis,
        event_artifact=event_artifact,
        observability=observability,
        paths=paths,
    )


def _semantic_event_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path, columns=list(PHASE10_EVENT_COLUMNS))


def _canonical_events(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.reindex(columns=PHASE10_EVENT_COLUMNS).copy()
    for name in _TIMESTAMP_COLUMNS:
        result[name] = result[name].map(
            lambda value: (
                pd.Timestamp(value.value, tz="UTC") if isinstance(value, ExactTimestampNs) else value
            )
        )
        result[name] = pd.to_datetime(result[name], utc=True, format="mixed")
    return result.sort_values(list(_EVENT_ORDER), kind="mergesort").reset_index(drop=True)


def _assert_events_equal(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    observed = _canonical_events(actual)
    wanted = _canonical_events(expected)
    assert len(observed) == len(wanted)
    for name in PHASE10_EVENT_COLUMNS:
        if pd.api.types.is_float_dtype(wanted[name].dtype):
            pd.testing.assert_series_equal(
                observed[name],
                wanted[name],
                check_dtype=False,
                check_exact=False,
                rtol=1e-12,
                atol=1e-12,
            )
        else:
            pd.testing.assert_series_equal(
                observed[name],
                wanted[name],
                check_dtype=False,
                check_exact=True,
            )


def _logical_analysis_payload(analysis: StreamingLeadLagAnalysis) -> dict[str, object]:
    payload = analysis.as_dict()
    summary = cast(dict[str, object], payload["summary"])
    summary.pop("config_sha256")
    summary.pop("event_parquet_bytes")
    return payload


def test_logical_artifacts_are_invariant_to_fragmentation_and_writer_knobs(
    tmp_path: Path,
) -> None:
    baseline_config = _config(writer_buffer_rows=7, parquet_row_group_rows=31)
    alternate_config = _config(writer_buffer_rows=11, parquet_row_group_rows=43)
    baseline = _completed_run(
        tmp_path / "baseline",
        fragmented=False,
        config=baseline_config,
    )
    fragmented = _completed_run(
        tmp_path / "fragmented",
        fragmented=True,
        config=baseline_config,
    )
    alternate_writer = _completed_run(
        tmp_path / "alternate-writer",
        fragmented=True,
        config=alternate_config,
    )

    # Same logical configuration is byte-for-byte deterministic even when rows
    # arrive through different physical fragments and insertion order.
    assert baseline.event_artifact == fragmented.event_artifact
    for name in ("events", "metrics", "controls", "result", "report"):
        assert baseline.paths[name].read_bytes() == fragmented.paths[name].read_bytes()

    # Writer buffering and row groups are physical provenance. They may change
    # file/config hashes, but never the causal rows or exact aggregate semantics.
    _assert_events_equal(
        _semantic_event_frame(alternate_writer.paths["events"]),
        _semantic_event_frame(baseline.paths["events"]),
    )
    assert _logical_analysis_payload(alternate_writer.analysis) == (
        _logical_analysis_payload(baseline.analysis)
    )
    assert alternate_writer.event_artifact.row_count == baseline.event_artifact.row_count
    assert alternate_writer.event_artifact.logical_sha256 != (baseline.event_artifact.logical_sha256)
    baseline_metadata = pq.ParquetFile(baseline.paths["events"]).metadata
    alternate_metadata = pq.ParquetFile(alternate_writer.paths["events"]).metadata
    assert baseline_metadata.num_rows == alternate_metadata.num_rows
    assert baseline_metadata.num_row_groups != alternate_metadata.num_row_groups


def test_partial_parquet_failure_cleans_top_level_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    output = tmp_path / "report"
    partial_writes: list[tuple[int, Path]] = []

    def load_window(
        _admission: object,
        *,
        scratch_dir: Path,
        selected_manifests_path: Path,
        **_kwargs: object,
    ) -> _SyntheticWindow:
        return _make_window(
            root=root,
            scratch_dir=scratch_dir,
            selected_manifests_path=selected_manifests_path,
            fragmented=True,
        )

    def fail_after_partial_parquet(
        spool: EventSpool,
        path: Path,
        **_kwargs: object,
    ) -> tuple[int, int, str]:
        partial_writes.append((spool.total_rows, path))
        path.write_bytes(b"PAR1synthetic-incomplete-parquet")
        raise OSError("synthetic failure after partial Parquet staging")

    monkeypatch.setattr(
        streaming_lake,
        "validate_bounded_lead_lag_gate",
        lambda *_args: object(),
    )
    monkeypatch.setattr(streaming_lake, "load_bounded_lead_lag_window", load_window)
    monkeypatch.setattr(EventSpool, "write_parquet", fail_after_partial_parquet)

    with pytest.raises(OSError, match=r"^synthetic failure after partial Parquet staging$"):
        run_bounded_lead_lag_study(
            root,
            tmp_path / "synthetic-gate.json",
            _config(writer_buffer_rows=7, parquet_row_group_rows=31),
            output,
        )

    assert len(partial_writes) == 1
    spooled_rows, partial_path = partial_writes[0]
    assert spooled_rows > 0
    assert partial_path.name == "events.parquet"
    assert not output.exists()
    assert not output.with_name(f".{output.name}.phase10-streaming.tmp").exists()


def test_disjoint_intervals_and_complete_boundary_batches_match_oracle(
    tmp_path: Path,
) -> None:
    fixture = generate_synthetic_lead_lag_dataset(
        event_count=32,
        injected_lag_ms=250,
        seed=20260815,
    )
    signal_time = pd.Timestamp(fixture.signal_times[0])
    first_end = signal_time + pd.Timedelta(milliseconds=250)
    second_start = signal_time + pd.Timedelta(milliseconds=500)
    second_end = signal_time + pd.Timedelta(milliseconds=1_250)
    intervals = (
        StrictInterval(
            start=fixture.intervals[0].start,
            end=first_end.to_pydatetime(),
            tag="before-gap",
        ),
        StrictInterval(
            start=second_start.to_pydatetime(),
            end=second_end.to_pydatetime(),
            tag="after-gap",
        ),
    )
    config = LeadLagConfig(
        horizons_ms=(50, 100, 250),
        randomization_resamples=19,
        max_source_rows_per_chunk=10_000,
        max_simultaneous_batch_rows=1_000,
        max_l2_levels_per_chunk=10_000,
        writer_buffer_rows=17,
    )
    spool = SourceRowSpool(tmp_path / "source.sqlite3")
    _populate_source(
        spool,
        {
            "bbo": fixture.dataset.bbo,
            "trade": fixture.dataset.trades,
            "l2": fixture.dataset.l2,
        },
        fragmented=True,
    )
    emitted: list[dict[str, object]] = []
    try:
        for asset in config.assets:
            for interval in intervals:
                run_streaming_kernel(
                    spool,
                    asset=asset,
                    interval=interval,
                    config=config,
                    sink=lambda rows: emitted.extend(dict(row) for row in rows),
                    include_execution=True,
                )
    finally:
        spool.close()

    oracle = analyze_lead_lag(fixture.dataset, intervals, config)
    _assert_events_equal(pd.DataFrame(emitted), oracle.events)

    signal_ns = signal_time.value
    first_end_ns = first_end.value
    second_start_ns = second_start.value
    second_end_ns = second_end.value
    assert any(
        isinstance(row["signal_time"], ExactTimestampNs)
        and row["signal_time"].value == signal_ns
        and row["interval_tag"] == "before-gap"
        for row in emitted
    )
    assert not any(
        isinstance(row["signal_time"], ExactTimestampNs)
        and row["signal_time"].value == first_end_ns
        and row["interval_tag"] == "before-gap"
        for row in emitted
    )
    assert any(
        isinstance(row["signal_time"], ExactTimestampNs)
        and row["signal_time"].value == second_start_ns
        and row["interval_tag"] == "after-gap"
        for row in emitted
    )
    for row in emitted:
        signal = row["signal_time"]
        assert isinstance(signal, ExactTimestampNs)
        if row["interval_tag"] == "before-gap":
            assert signal.value < first_end_ns
        else:
            assert second_start_ns <= signal.value < second_end_ns


def test_same_config_result_hash_is_stable_across_runtime_timings(
    tmp_path: Path,
) -> None:
    config = _config(writer_buffer_rows=7, parquet_row_group_rows=31)
    first = _completed_run(tmp_path / "first", fragmented=False, config=config)
    second = _completed_run(tmp_path / "second", fragmented=False, config=config)

    first_result = json.loads(first.paths["result"].read_bytes())
    second_result = json.loads(second.paths["result"].read_bytes())
    assert first_result["analysis_semantic_sha256"] == (second_result["analysis_semantic_sha256"])
    assert first.paths["result"].read_bytes() == second.paths["result"].read_bytes()
    assert first.paths["observability"].read_bytes() != (second.paths["observability"].read_bytes())
