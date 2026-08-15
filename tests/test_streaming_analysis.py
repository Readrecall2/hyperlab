from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from hyperlab.analysis import streaming as streaming_module
from hyperlab.analysis.lead_lag import LeadLagConfig, StrictInterval, analyze_lead_lag
from hyperlab.analysis.streaming import (
    run_bounded_analysis_in_staging,
    run_bounded_lead_lag_study,
)
from hyperlab.analysis.streaming_reporting import evidence_bindings
from hyperlab.analysis.streaming_store import (
    PHASE10_EVENT_COLUMNS,
    PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS,
    SourceRowSpool,
)
from hyperlab.analysis.synthetic import generate_synthetic_lead_lag_dataset


@dataclass(slots=True)
class _SyntheticWindow:
    root: Path
    start: object
    end: object
    assets: tuple[str, ...]
    intervals: tuple[StrictInterval, ...]
    manifest_fingerprint: str
    source_spool: SourceRowSpool
    observability: dict[str, object]
    gate_report_sha256: str = "1" * 64
    semantic_gate_sha256: str = "2" * 64
    semantic_gate_canonicalizer_version: str = "phase10_semantic_gate_payload_v1"
    excluded_json_pointers: tuple[str, ...] = ("/observability",)
    selected_manifests_sha256: str = "3" * 64
    selected_manifest_count: int = 3


def _spool_fixture(path: Path) -> tuple[_SyntheticWindow, object]:
    fixture = generate_synthetic_lead_lag_dataset(event_count=32, seed=20260815)
    spool = SourceRowSpool(path)
    spool.add_rows(
        kind="bbo",
        rows=fixture.dataset.bbo.to_dict(orient="records"),
        manifest_order=0,
        first_row_order=0,
    )
    spool.add_rows(
        kind="trade",
        rows=fixture.dataset.trades.to_dict(orient="records"),
        manifest_order=1,
        first_row_order=0,
    )
    spool.add_rows(
        kind="l2",
        rows=fixture.dataset.l2.to_dict(orient="records"),
        manifest_order=2,
        first_row_order=0,
    )
    interval = fixture.intervals[0]
    return (
        _SyntheticWindow(
            root=path.parent,
            start=interval.start,
            end=interval.end,
            assets=("BTC", "ETH"),
            intervals=fixture.intervals,
            manifest_fingerprint="4" * 64,
            source_spool=spool,
            observability={
                "rows_scanned": len(fixture.dataset.bbo)
                + len(fixture.dataset.trades)
                + len(fixture.dataset.l2),
                "clock_sync_diagnostics": {
                    "row_count": len(fixture.dataset.clock_sync),
                    "usage": "DIAGNOSTIC_ONLY_STRICT_INTERVALS_DEFINE_CAUSAL_VALIDITY",
                },
            },
        ),
        fixture,
    )


def test_production_streaming_module_has_no_pandas_or_oracle_materialization() -> None:
    source = Path("src/hyperlab/analysis/streaming.py").read_text(encoding="utf-8")
    for forbidden in (
        "import pandas",
        "LeadLagDataset",
        "analyze_lead_lag_chunk",
        ".dataframe(",
    ):
        assert forbidden not in source


def test_bounded_two_pass_engine_matches_pandas_oracle_and_ignores_legacy_caps(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    (staging / ".scratch").mkdir(parents=True)
    window, raw_fixture = _spool_fixture(staging / ".scratch" / "source.sqlite3")
    fixture = raw_fixture
    config = LeadLagConfig(
        randomization_resamples=19,
        minimum_events=2,
        max_event_rows=1,
        max_estimated_event_bytes=1,
        max_source_rows_per_chunk=5_000,
        max_simultaneous_batch_rows=1_000,
        max_l2_frame_levels=100,
        max_pending_response_states=50_000,
        max_pending_execution_states=100_000,
        parquet_row_group_rows=256,
        writer_buffer_rows=64,
        scratch_low_watermark_bytes=1,
        scratch_reserve_bytes=1,
    )
    oracle_config = replace(
        config,
        max_event_rows=5_000_000,
        max_estimated_event_bytes=8_000_000_000,
    )
    oracle = analyze_lead_lag(
        fixture.dataset,
        fixture.intervals,
        oracle_config,
    )

    try:
        actual, event_artifact, observability = run_bounded_analysis_in_staging(
            window=window,
            config=config,
            staging=staging,
        )
    finally:
        window.source_spool.close()

    pd.testing.assert_frame_equal(
        actual.metrics,
        oracle.metrics,
        check_dtype=False,
        check_exact=False,
        rtol=1e-12,
        atol=1e-12,
    )
    pd.testing.assert_frame_equal(
        actual.bucket_metrics,
        oracle.bucket_metrics,
        check_dtype=False,
        check_exact=False,
        rtol=1e-12,
        atol=1e-12,
    )
    pd.testing.assert_frame_equal(
        actual.controls,
        oracle.controls,
        check_dtype=False,
        check_exact=False,
        rtol=1e-12,
        atol=1e-12,
    )
    events = pd.read_parquet(staging / "events.parquet")
    assert tuple(oracle.events.columns) == PHASE10_EVENT_COLUMNS
    assert tuple(events.columns) == (
        *PHASE10_EVENT_COLUMNS,
        *PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS,
    )
    for name in PHASE10_EVENT_COLUMNS:
        expected = oracle.events[name]
        observed = events[name]
        if pd.api.types.is_float_dtype(expected.dtype):
            pd.testing.assert_series_equal(
                observed,
                expected,
                check_dtype=False,
                check_exact=False,
                rtol=1e-12,
                atol=1e-12,
            )
        else:
            pd.testing.assert_series_equal(
                observed,
                expected,
                check_dtype=False,
                check_exact=True,
            )
    expected_bindings = evidence_bindings(window, config)
    assert tuple(expected_bindings) == PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS
    for name, value in expected_bindings.items():
        assert events[name].eq(value).all()
    assert event_artifact.row_count == len(oracle.events)
    assert actual.summary["legacy_in_memory_limits_used"] is False
    assert "available_bytes" not in actual.summary["capacity_preflight"]
    assert "projected_remaining_bytes" not in actual.summary["capacity_preflight"]
    assert observability["analysis_passes"] == 2
    assert int(observability["chunks_processed"]) > 1
    assert observability["output_rows_written"] == len(oracle.events)
    assert set(actual.metrics["horizon_ms"]) == set(config.horizons_ms)
    execution = actual.metrics.loc[actual.metrics["analysis_kind"].eq("execution")]
    assert set(execution["execution_model"].dropna()) == {"maker", "taker"}
    assert set(execution["execution_scenario"].dropna()) == {
        item.name for item in config.execution_scenarios
    }


@pytest.mark.parametrize("failure", [OSError("Parquet writer failed"), KeyboardInterrupt()])
def test_streaming_failure_or_interrupt_leaves_no_completed_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
) -> None:
    from hyperlab.analysis import streaming_lake

    root = tmp_path / "lake"
    root.mkdir()
    output = tmp_path / "report"
    closed: list[bool] = []
    fake_spool = SimpleNamespace(close=lambda: closed.append(True))
    fake_window = SimpleNamespace(source_spool=fake_spool)
    monkeypatch.setattr(
        streaming_lake,
        "validate_bounded_lead_lag_gate",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        streaming_lake,
        "load_bounded_lead_lag_window",
        lambda *_args, **_kwargs: fake_window,
    )
    monkeypatch.setattr(
        streaming_module,
        "run_bounded_analysis_in_staging",
        lambda **_kwargs: (_ for _ in ()).throw(failure),
    )

    with pytest.raises(type(failure), match=str(failure) or None):
        run_bounded_lead_lag_study(
            root,
            tmp_path / "gate.json",
            LeadLagConfig(randomization_resamples=19),
            output,
        )

    assert closed == [True]
    assert not output.exists()
    assert not output.with_name(f".{output.name}.phase10-streaming.tmp").exists()
