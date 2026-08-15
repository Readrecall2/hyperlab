from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hyperlab.analysis.lead_lag import LeadLagConfig
from hyperlab.analysis.streaming_kernel import run_streaming_kernel
from hyperlab.analysis.streaming_store import (
    PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS,
    EventSpool,
    ExactTimestampNs,
    SourceRowSpool,
    StreamingStoreError,
)
from hyperlab.analysis.synthetic import generate_synthetic_lead_lag_dataset


def _bindings() -> dict[str, str]:
    result = {
        name: f"kernel-seam-{name}"
        for name in PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS
    }
    result["gate_report_sha256"] = "7" * 64
    return result


def test_kernel_event_spool_parquet_preserves_exact_nanoseconds(
    tmp_path: Path,
) -> None:
    fixture = generate_synthetic_lead_lag_dataset(
        event_count=32,
        injected_lag_ms=250,
        seed=20260815,
    )
    shift = pd.Timedelta(nanoseconds=123)
    dataset = fixture.dataset

    def shifted(frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        result["received_time"] = pd.to_datetime(
            result["received_time"], utc=True
        ) + shift
        return result

    shifted_dataset = replace(
        dataset,
        bbo=shifted(dataset.bbo),
        trades=shifted(dataset.trades),
        l2=shifted(dataset.l2),
        source_fingerprint="e" * 64,
    )
    source = SourceRowSpool(tmp_path / "scratch" / "source.sqlite3")
    events = EventSpool(
        tmp_path / "scratch" / "events.sqlite3",
        quantile_run_rows=101,
    )
    output = tmp_path / "events.parquet"
    try:
        for manifest_order, (kind, frame) in enumerate(
            (
                ("bbo", shifted_dataset.bbo),
                ("trade", shifted_dataset.trades),
                ("l2", shifted_dataset.l2),
            )
        ):
            source.add_rows(
                kind=kind,
                rows=frame.to_dict(orient="records"),
                manifest_order=manifest_order,
                first_row_order=0,
            )
        config = LeadLagConfig(
            randomization_resamples=19,
            writer_buffer_rows=31,
            parquet_row_group_rows=127,
        )
        result = run_streaming_kernel(
            source,
            asset="BTC",
            interval=fixture.intervals[0],
            config=config,
            sink=events.add_rows,
            include_execution=True,
        )
        written, _size, _logical_hash = events.write_parquet(
            output,
            bindings=_bindings(),
            row_group_rows=config.parquet_row_group_rows,
            writer_buffer_rows=config.writer_buffer_rows,
        )
    finally:
        events.close()
        source.close()

    assert written == result.counts.total_rows > 0
    table = pq.read_table(
        output,
        columns=["signal_time", "target_time", "entry_time", "exit_time"],
    )
    for required in ("signal_time", "target_time"):
        values = table[required].cast(pa.int64()).to_pylist()
        assert values
        assert {int(value) % 1_000 for value in values} == {123}
    for optional in ("entry_time", "exit_time"):
        values = [
            int(value)
            for value in table[optional].cast(pa.int64()).to_pylist()
            if value is not None
        ]
        assert values
        assert {value % 1_000 for value in values} == {123}


def test_exact_timestamp_is_narrow_and_does_not_admit_strings() -> None:
    exact = ExactTimestampNs(1_735_689_600_000_000_123)
    assert exact.value % 1_000 == 123
    with pytest.raises(StreamingStoreError, match="must be an integer"):
        ExactTimestampNs("2025-01-01T00:00:00.000000123Z")  # type: ignore[arg-type]
