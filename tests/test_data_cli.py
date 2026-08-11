from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from typer.testing import CliRunner

from hyperlab.cli import app
from hyperlab.data import cli as data_cli
from hyperlab.data.lake import PartitionKey, write_partition
from hyperlab.data.schema import schema_for

runner = CliRunner()


def _write_lifecycle_partition(root: Path) -> Path:
    event_time = datetime(2026, 1, 2, tzinfo=UTC)
    spec = schema_for("instrument_lifecycle")
    table = pa.Table.from_pylist(
        [
            {
                "schema_version": 1,
                "record_type": "instrument_lifecycle",
                "venue": "HL",
                "asset": "OLD",
                "event_time": event_time,
                "exchange_time": None,
                "received_time": event_time + timedelta(milliseconds=1),
                "source_sequence": None,
                "connection_id": None,
                "source_symbol": "OLD",
                "instrument_id": "HL:OLD:perp",
                "instrument_kind": "perp",
                "status": "delisted",
                "valid_from": event_time,
                "valid_to": None,
            }
        ],
        schema=spec.schema,
    )
    manifest = write_partition(
        root,
        PartitionKey("HL", date(2026, 1, 2), "OLD", "instrument_lifecycle"),
        table,
    )
    return root / manifest.relative_data_path


def test_data_help_exposes_only_read_only_data_commands() -> None:
    result = runner.invoke(app, ["data", "--help"])

    assert result.exit_code == 0
    assert "validate" in result.output
    assert "inventory" in result.output
    assert "export" in result.output
    for forbidden in ("live", "trade", "mainnet"):
        assert forbidden not in result.output.lower()


def test_validate_reports_partition_corruption_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def corrupt_inventory(root: Path) -> list[object]:
        assert root == tmp_path
        raise ValueError(
            "CORRUPT_PARTITION [hash_mismatch] partition=venue=HL/date=2026-01-01/"
            "asset=BTC/type=trades expected_sha256=abc actual_sha256=def"
        )

    monkeypatch.setattr(data_cli, "inventory_partitions", corrupt_inventory)

    result = runner.invoke(app, ["data", "validate", str(tmp_path)])

    assert result.exit_code == 2
    assert "CORRUPT_PARTITION [hash_mismatch]" in result.output
    assert "Traceback" not in result.output


def test_validate_writes_a_byte_reproducible_daily_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    payload = {
        "status": "ok",
        "date": "2026-01-02",
        "partitions": [
            {"relative_path": "venue=HL/date=2026-01-02/asset=ETH/type=trades", "rows": 2},
            {"relative_path": "venue=HL/date=2026-01-02/asset=BTC/type=trades", "rows": 3},
        ],
        "row_count": 5,
    }

    def report(root: Path, report_date: date) -> dict[str, object]:
        assert root == lake
        assert report_date == date(2026, 1, 2)
        return payload

    monkeypatch.setattr(data_cli, "daily_quality_report", report)
    first = tmp_path / "quality-a.json"
    second = tmp_path / "quality-b.json"

    first_result = runner.invoke(
        app,
        ["data", "validate", str(lake), "--date", "2026-01-02", "--report", str(first)],
    )
    second_result = runner.invoke(
        app,
        ["data", "validate", str(lake), "--date", "2026-01-02", "--report", str(second)],
    )

    assert first_result.exit_code == 0
    assert second_result.exit_code == 0
    assert first.read_bytes() == second.read_bytes()
    assert first.read_bytes().endswith(b"\n")
    assert json.loads(first.read_text(encoding="utf-8")) == payload


def test_inventory_json_is_stable_and_builds_the_local_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifests = [
        {"relative_path": "venue=HL/date=2026-01-02/asset=ETH/type=trades", "row_count": 2},
        {"relative_path": "venue=HL/date=2026-01-01/asset=BTC/type=candles", "row_count": 3},
    ]
    catalog_calls: list[tuple[Path, Path]] = []
    monkeypatch.setattr(data_cli, "inventory_partitions", lambda root: manifests)
    monkeypatch.setattr(
        data_cli,
        "build_catalog",
        lambda root, database: catalog_calls.append((root, database)) or database,
    )

    result = runner.invoke(app, ["data", "inventory", str(tmp_path), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert [item["relative_path"] for item in payload["partitions"]] == sorted(
        item["relative_path"] for item in manifests
    )
    assert catalog_calls == [(tmp_path, tmp_path / "catalog.duckdb")]


@pytest.mark.parametrize(
    ("inventory", "expected_status"),
    [
        ([], "missing"),
        ([{"relative_path": "p", "row_count": 1, "quality": "ok"}], "ok"),
        ([{"relative_path": "p", "row_count": 1, "quality": "unobservable"}], "unobservable"),
        ([{"relative_path": "p", "row_count": 1, "quality": "degraded"}], "degraded"),
        (
            {
                "partitions": [{"relative_path": "p", "row_count": 1, "quality": "ok"}],
                "cross_segment_gaps": [{"partition": {"date": "2026-01-02"}}],
            },
            "degraded",
        ),
    ],
)
def test_inventory_status_reflects_observed_quality(
    inventory: object,
    expected_status: str,
) -> None:
    assert data_cli._inventory_payload(inventory, None)["status"] == expected_status


def test_validate_refuses_a_report_inside_the_lake_before_writing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    target = lake / "catalog.duckdb"
    target.write_bytes(b"catalogue intact")
    monkeypatch.setattr(
        data_cli,
        "inventory_partitions",
        lambda root: pytest.fail("lake must not be read when the report destination is unsafe"),
    )

    result = runner.invoke(app, ["data", "validate", str(lake), "--report", str(target)])

    assert result.exit_code == 2
    assert "REPORT_REFUSED [inside_lake]" in result.output
    assert "Traceback" not in result.output
    assert target.read_bytes() == b"catalogue intact"


def test_validate_atomically_replaces_a_report_outside_the_lake(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    target = tmp_path / "quality.json"
    target.write_text("obsolete", encoding="utf-8")
    old_fixed_temporary = tmp_path / ".quality.json.tmp"
    old_fixed_temporary.write_text("unrelated", encoding="utf-8")
    payload = {"quality": "ok", "partition_count": 1}
    monkeypatch.setattr(data_cli, "daily_quality_report", lambda root, report_date: payload)

    result = runner.invoke(
        app,
        ["data", "validate", str(lake), "--date", "2026-01-03", "--report", str(target)],
    )

    assert result.exit_code == 0
    assert json.loads(target.read_text(encoding="utf-8")) == payload
    assert old_fixed_temporary.read_text(encoding="utf-8") == "unrelated"
    assert list(tmp_path.glob(".quality.json.*.tmp")) == []


def test_daily_missing_report_is_written_and_exits_two_without_false_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    report_path = tmp_path / "missing.json"
    payload = {
        "date": "2026-01-03",
        "quality": "missing",
        "partition_count": 0,
        "partitions": [],
    }
    monkeypatch.setattr(data_cli, "daily_quality_report", lambda root, report_date: payload)

    result = runner.invoke(
        app,
        ["data", "validate", str(lake), "--date", "2026-01-03", "--report", str(report_path)],
    )

    assert result.exit_code == 2
    assert json.loads(report_path.read_text(encoding="utf-8")) == payload
    assert '"quality":"missing"' in result.output
    assert "DATA_QUALITY [missing] date=2026-01-03 partition_count=0" in result.output
    assert "Validation réussie" not in result.output
    assert "Traceback" not in result.output


def test_real_empty_daily_quality_report_is_missing_and_fails(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    report_path = tmp_path / "missing-real.json"

    result = runner.invoke(
        app,
        ["data", "validate", str(lake), "--date", "2026-01-03", "--report", str(report_path)],
    )

    assert result.exit_code == 2
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["quality"] == "missing"
    assert payload["partition_count"] == 0
    assert "DATA_QUALITY [missing] date=2026-01-03 partition_count=0" in result.output
    assert "Traceback" not in result.output


@pytest.mark.parametrize(
    ("quality", "message"),
    [
        ("degraded", "qualité dégradée"),
        ("unobservable", "qualité non observable"),
    ],
)
def test_non_ok_quality_is_reported_without_a_false_success_message(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    quality: str,
    message: str,
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    payload = {"date": "2026-01-03", "quality": quality, "partition_count": 1}
    monkeypatch.setattr(data_cli, "daily_quality_report", lambda root, report_date: payload)

    result = runner.invoke(app, ["data", "validate", str(lake), "--date", "2026-01-03"])

    assert result.exit_code == 0
    assert message in result.output
    assert "Validation réussie" not in result.output


def test_export_forwards_filters_without_overwriting_or_transforming_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    output = tmp_path / "trades.csv"
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(data_cli, "inventory_partitions", lambda root: [{"relative_path": "p"}])
    monkeypatch.setattr(data_cli, "build_catalog", lambda root, database: database)

    def export(root: Path, destination: Path, **filters: object) -> dict[str, object]:
        calls.append({"root": root, "output": destination, **filters})
        destination.write_text("event_time\n", encoding="utf-8")
        return {"output": destination, "row_count": 0}

    monkeypatch.setattr(data_cli, "export_dataset", export)

    result = runner.invoke(
        app,
        [
            "data",
            "export",
            str(lake),
            str(output),
            "--type",
            "trade",
            "--venue",
            "HL",
            "--asset",
            "BTC",
            "--start",
            "2026-01-01",
            "--end",
            "2026-01-02",
            "--schema-version",
            "1",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        {
            "root": lake,
            "output": output,
            "record_type": "trade",
            "venue": "HL",
            "asset": "BTC",
            "start": date(2026, 1, 1),
            "end": date(2026, 1, 2),
            "schema_version": 1,
        }
    ]
    assert output.read_text(encoding="utf-8") == "event_time\n"


def test_export_refuses_an_existing_output_before_loading_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    output = tmp_path / "existing.parquet"
    output.write_bytes(b"keep me")
    monkeypatch.setattr(
        data_cli,
        "export_dataset",
        lambda *args, **kwargs: pytest.fail("export must not be called"),
    )

    result = runner.invoke(
        app,
        ["data", "export", str(lake), str(output), "--format", "parquet"],
    )

    assert result.exit_code == 2
    assert "EXPORT_REFUSED [output_exists]" in result.output
    assert output.read_bytes() == b"keep me"


def test_export_refuses_every_destination_inside_the_lake(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    output = lake / "nested" / "export.csv"
    monkeypatch.setattr(
        data_cli,
        "inventory_partitions",
        lambda root: pytest.fail("lake must not be read when the export destination is unsafe"),
    )

    result = runner.invoke(
        app,
        ["data", "export", str(lake), str(output), "--format", "csv"],
    )

    assert result.exit_code == 2
    assert "EXPORT_REFUSED [inside_lake]" in result.output
    assert "Traceback" not in result.output
    assert not output.exists()


def test_real_inventory_and_export_keep_a_delisted_asset(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _write_lifecycle_partition(lake)

    inventory = runner.invoke(app, ["data", "inventory", str(lake), "--json"])
    assert inventory.exit_code == 0
    payload = json.loads(inventory.output)
    assert payload["partition_count"] == 1
    assert payload["delisted_assets"] == ["HL:OLD"]
    assert payload["status"] == "unobservable"
    assert payload["partitions"][0]["partition"] == {
        "asset": "OLD",
        "date": "2026-01-02",
        "record_type": "instrument_lifecycle",
        "venue": "HL",
    }

    output = tmp_path / "delisted.csv"
    export = runner.invoke(
        app,
        [
            "data",
            "export",
            str(lake),
            str(output),
            "--type",
            "instrument_lifecycle",
            "--schema-version",
            "1",
            "--format",
            "csv",
        ],
    )
    assert export.exit_code == 0, export.output
    assert output.is_file()
    assert "OLD" in output.read_text(encoding="utf-8")


def test_real_corrupted_partition_has_a_stable_cli_error(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    data_file = _write_lifecycle_partition(lake)
    corrupted = bytearray(data_file.read_bytes())
    corrupted[len(corrupted) // 2] ^= 0xFF
    data_file.write_bytes(corrupted)

    result = runner.invoke(app, ["data", "validate", str(lake)])

    assert result.exit_code == 2
    assert "CORRUPT_PARTITION [hash_mismatch]" in result.output
    assert "expected_sha256=" in result.output
    assert "actual_sha256=" in result.output
    assert "Traceback" not in result.output


def test_real_daily_quality_report_is_byte_reproducible(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _write_lifecycle_partition(lake)
    first = tmp_path / "quality-a.json"
    second = tmp_path / "quality-b.json"

    first_result = runner.invoke(
        app,
        ["data", "validate", str(lake), "--date", "2026-01-02", "--report", str(first)],
    )
    second_result = runner.invoke(
        app,
        ["data", "validate", str(lake), "--date", "2026-01-02", "--report", str(second)],
    )

    assert first_result.exit_code == 0, first_result.output
    assert second_result.exit_code == 0, second_result.output
    assert first.read_bytes() == second.read_bytes()
    assert first.read_bytes().endswith(b"\n")
    payload = json.loads(first.read_text(encoding="utf-8"))
    assert payload["date"] == "2026-01-02"
    assert payload["partition_count"] == 1
    assert payload["delisted_assets"] == ["HL:OLD"]
    assert len(payload["manifest_set_sha256"]) == 64
