from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from hyperlab.analysis.streaming_store import (
    PHASE10_EVENT_COLUMNS,
    PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS,
    PHASE10_EVENT_PARQUET_SCHEMA,
    EventSpool,
    SourceRowSpool,
    StreamingStoreError,
    timestamp_ns,
)

BASE = datetime(2026, 8, 15, tzinfo=UTC)


def _bindings() -> dict[str, str]:
    result = {
        name: f"test-{name}"
        for name in PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS
    }
    result["gate_report_sha256"] = "1" * 64
    return result


def test_source_spool_orders_rows_without_retaining_the_population(tmp_path: Path) -> None:
    with SourceRowSpool(tmp_path / "scratch" / "source.sqlite3") as spool:
        spool.add_rows(
            kind="bbo",
            manifest_order=1,
            first_row_order=0,
            rows=(
                {
                    "venue": "hyperliquid",
                    "asset": "BTC",
                    "received_time": BASE + timedelta(milliseconds=2),
                    "connection_id": "public",
                    "source_sequence": 2,
                    "update_id": "u2",
                },
                {
                    "venue": "hyperliquid",
                    "asset": "BTC",
                    "received_time": BASE + timedelta(milliseconds=1),
                    "connection_id": "public",
                    "source_sequence": 1,
                    "update_id": "u1",
                },
            ),
        )

        rows = list(
            spool.iter_rows(
                kind="bbo",
                asset="BTC",
                start_ns=timestamp_ns(BASE, label="start"),
                end_ns=timestamp_ns(BASE + timedelta(seconds=1), label="end"),
            )
        )
        batches = list(
            spool.iter_receive_batches(
                asset="BTC",
                start_ns=timestamp_ns(BASE, label="start"),
                end_ns=timestamp_ns(BASE + timedelta(seconds=1), label="end"),
            )
        )
        ordered_batches = list(
            spool.iter_ordered_batches(
                asset="BTC",
                start_ns=timestamp_ns(BASE, label="start"),
                end_ns=timestamp_ns(BASE + timedelta(seconds=1), label="end"),
                fetch_rows=1,
            )
        )

    assert [row["update_id"] for row in rows] == ["u1", "u2"]
    assert [count for _received, count in batches] == [1, 1]
    assert [
        row[1]["update_id"]
        for _received, batch in ordered_batches
        for row in batch
    ] == ["u1", "u2"]


def _event_frame() -> pd.DataFrame:
    values = [0.0, 1.0, 2.0, 9.0]
    return pd.DataFrame(
        [
            {
                "signal_id": f"signal-{position}",
                "signal_venue": "binance_usdm",
                "asset": "BTC",
                "signal_family": "agg_trade",
                "signal_time": pd.Timestamp(BASE + timedelta(milliseconds=position)),
                "signal_value": 1.0,
                "signal_strength": 1.0,
                "signal_direction": 1,
                "signal_role": "primary",
                "time_axis": "received_time",
                "source_time_status": "NOT_ADMISSIBLE",
                "horizon_ms": 50,
                "target_time": pd.Timestamp(BASE + timedelta(milliseconds=position + 50)),
                "time_bucket": pd.Timestamp(BASE),
                "interval_tag": "capture",
                "interval_id": "interval",
                "interval_start": pd.Timestamp(BASE),
                "interval_end": pd.Timestamp(BASE + timedelta(seconds=1)),
                "evaluable": True,
                "response_bps": value,
                "negative_lag_response_bps": -value,
                "first_move_delay_ms": float(position),
                "first_move_direction": "same",
                "classification": "same_direction",
                "randomization_block": "interval|000000000000",
                "row_kind": "information",
                "execution_scenario": None,
                "execution_model": None,
                "execution_status": "NOT_APPLICABLE",
            }
            for position, value in enumerate(values)
        ]
    )


def test_event_spool_preserves_exact_linear_quantiles_and_fixed_output(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "events.parquet"
    with EventSpool(tmp_path / "scratch" / "events.sqlite3") as spool:
        spool.add_frame(_event_frame())
        filters = {
            "asset": "BTC",
            "signal_family": "agg_trade",
            "horizon_ms": 50,
            "execution_scenario": None,
            "execution_model": None,
        }
        assert spool.exact_quantile(
            metric="information_response", filters=filters, quantile=0.1
        ) == pytest.approx(pd.Series([0.0, 1.0, 2.0, 9.0]).quantile(0.1), abs=0.0)
        assert spool.exact_quantile(
            metric="information_response", filters=filters, quantile=0.5
        ) == pytest.approx(1.5, abs=0.0)
        written, _size, logical_hash = spool.write_parquet(
            parquet_path,
            bindings=_bindings(),
            row_group_rows=2,
        )

    frame = pd.read_parquet(parquet_path)
    assert written == 4
    assert len(logical_hash) == 64
    assert frame["signal_id"].tolist() == [f"signal-{index}" for index in range(4)]
    assert frame["gate_report_sha256"].eq("1" * 64).all()


def test_parquet_row_groups_and_bytes_ignore_writer_input_buffer_size(
    tmp_path: Path,
) -> None:
    outputs: list[Path] = []
    logical_hashes: list[str] = []
    for buffer_rows in (1, 2):
        output = tmp_path / f"events-{buffer_rows}.parquet"
        outputs.append(output)
        with EventSpool(
            tmp_path / f"scratch-{buffer_rows}" / "events.sqlite3",
            quantile_run_rows=2,
        ) as spool:
            spool.add_frame(_event_frame())
            _written, _size, logical_hash = spool.write_parquet(
                output,
                bindings=_bindings(),
                row_group_rows=3,
                writer_buffer_rows=buffer_rows,
            )
            logical_hashes.append(logical_hash)

    assert logical_hashes[0] == logical_hashes[1]
    assert outputs[0].read_bytes() == outputs[1].read_bytes()
    for output in outputs:
        metadata = pq.ParquetFile(output).metadata
        assert metadata.num_row_groups == 2
        assert [metadata.row_group(index).num_rows for index in range(2)] == [3, 1]


def test_event_parquet_preserves_nanosecond_timestamps(tmp_path: Path) -> None:
    frame = _event_frame().iloc[:1].copy()
    signal_time = pd.Timestamp(BASE) + pd.Timedelta(nanoseconds=123)
    frame.loc[frame.index[0], "signal_time"] = signal_time
    output = tmp_path / "events-nanoseconds.parquet"

    with EventSpool(tmp_path / "scratch-nanoseconds" / "events.sqlite3") as spool:
        spool.add_frame(frame)
        spool.write_parquet(
            output,
            bindings=_bindings(),
            row_group_rows=2,
            writer_buffer_rows=1,
        )

    restored = pd.read_parquet(output)
    assert int(restored.loc[0, "signal_time"].value) == int(signal_time.value)


def test_fixed_event_schema_is_stable_for_null_and_populated_execution_fields(
    tmp_path: Path,
) -> None:
    null_frame = _event_frame().iloc[:1].copy()
    populated_frame = null_frame.copy()
    populated_frame.loc[populated_frame.index[0], "row_kind"] = "execution"
    populated_frame.loc[populated_frame.index[0], "execution_scenario"] = "baseline"
    populated_frame.loc[populated_frame.index[0], "execution_model"] = "maker"
    populated_frame.loc[populated_frame.index[0], "execution_status"] = "FILLED"
    populated_frame["entry_time"] = pd.Series(
        [pd.Timestamp(BASE) + pd.Timedelta(nanoseconds=321)],
        dtype="datetime64[ns, UTC]",
    )
    populated_frame["entry_price"] = 100_000.25
    populated_frame["net_execution_bps"] = 1.25

    outputs: list[Path] = []
    for label, frame in (("null", null_frame), ("populated", populated_frame)):
        output = tmp_path / f"events-{label}.parquet"
        outputs.append(output)
        with EventSpool(tmp_path / f"scratch-{label}" / "events.sqlite3") as spool:
            spool.add_rows(frame.to_dict(orient="records"))
            spool.write_parquet(
                output,
                bindings=_bindings(),
                row_group_rows=2,
                writer_buffer_rows=1,
            )

    schemas = [pq.ParquetFile(output).schema_arrow for output in outputs]
    assert schemas == [PHASE10_EVENT_PARQUET_SCHEMA, PHASE10_EVENT_PARQUET_SCHEMA]
    assert tuple(schemas[0].names[: len(PHASE10_EVENT_COLUMNS)]) == PHASE10_EVENT_COLUMNS
    assert schemas[0].field("entry_time").type == schemas[1].field("entry_time").type
    assert schemas[0].field("entry_price").type == schemas[1].field("entry_price").type
    restored_null = pd.read_parquet(outputs[0])
    restored_populated = pd.read_parquet(outputs[1])
    assert pd.isna(restored_null.loc[0, "entry_time"])
    assert int(restored_populated.loc[0, "entry_time"].value) == int(
        populated_frame.loc[populated_frame.index[0], "entry_time"].value
    )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"unknown_output": "forbidden"}, "unknown columns"),
        ({"horizon_ms": "50"}, "integer has a type conflict"),
        ({"evaluable": 1}, "bool has a type conflict"),
        ({"signal_time": "2026-08-15T00:00:00Z"}, "timestamp has a type conflict"),
    ],
)
def test_event_spool_fails_closed_on_schema_conflicts(
    tmp_path: Path,
    mutation: dict[str, object],
    message: str,
) -> None:
    row = _event_frame().iloc[0].to_dict()
    row.update(mutation)
    with (
        EventSpool(tmp_path / message.replace(" ", "-") / "events.sqlite3") as spool,
        pytest.raises(StreamingStoreError, match=message),
    ):
        spool.add_rows((row,))


def test_event_writer_requires_the_complete_v2_binding_schema(tmp_path: Path) -> None:
    output = tmp_path / "events.parquet"
    with EventSpool(tmp_path / "scratch-bindings" / "events.sqlite3") as spool:
        spool.add_frame(_event_frame().iloc[:1])
        with pytest.raises(
            StreamingStoreError, match=r"missing=.*semantic_gate_sha256"
        ):
            spool.write_parquet(
                output,
                bindings={"gate_report_sha256": "1" * 64},
                row_group_rows=2,
            )
