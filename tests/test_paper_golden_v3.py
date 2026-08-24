from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from hyperlab.backtest.protocol import canonical_json, canonical_sha256
from hyperlab.paper import (
    MarketEvent,
    PaperEngine,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    PaperStore,
)
from hyperlab.paper.golden_v3 import (
    GoldenRefusal,
    GoldenVerificationError,
    compare_golden_exports,
    export_golden_v3,
    verify_golden_v3,
    write_external_pin,
)

_START = datetime(2026, 8, 24, 8, tzinfo=UTC)
_INSTRUMENT = "HYPERLIQUID:BTC:perp"


def _config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="golden_v3_synthetic_fixture",
        strategy_hash="a" * 64,
        parameters={"fixture": "golden-v3", "warning": "SYNTHETIC_DATA"},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(
            calibration_status="SYNTHETIC",
            source="deterministic-test-fixture",
        ),
        risk=PaperRiskLimits(),
        seed=17,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        run_kind="DEMO",
        data_calibration_status="SYNTHETIC",
        data_source="deterministic-test-fixture",
    )


def _market(ordinal: int) -> MarketEvent:
    return MarketEvent.create(
        received_at=_START + timedelta(seconds=ordinal + 1),
        instrument=_INSTRUMENT,
        bid_price=Decimal("100") + Decimal(ordinal) / Decimal("10"),
        ask_price=Decimal("101") + Decimal(ordinal) / Decimal("10"),
        bid_depth=Decimal("100"),
        ask_depth=Decimal("100"),
        source_sequence=ordinal + 1,
    )


def _insert_unlinked_alert(database: Path, run_id: str) -> None:
    payload = {
        "code": "SYNTHETIC_UNLINKED_ALERT",
        "message": "synthetic guard alert without an owning commit",
        "run_id": run_id,
        "severity": "WARNING",
    }
    payload_json = canonical_json(payload)
    alert_id = canonical_sha256(
        {
            "domain": "hyperlab-golden-v3-synthetic-unlinked-alert-v1",
            "run_id": run_id,
        }
    )
    with sqlite3.connect(database) as connection:
        event_sequence = int(
            connection.execute(
                "SELECT event_count FROM paper_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]
        )
        connection.execute(
            """
            INSERT INTO paper_alerts (
                run_id, alert_id, commit_sequence, event_sequence, severity,
                code, payload_json, payload_hash, created_at
            ) VALUES (?, ?, NULL, ?, 'WARNING', ?, ?, ?, ?)
            """,
            (
                run_id,
                alert_id,
                event_sequence,
                payload["code"],
                payload_json,
                canonical_sha256(payload),
                _START.isoformat(),
            ),
        )


def _build_source(
    database: Path,
    *,
    market_count: int = 5,
    include_unlinked_alert: bool = True,
) -> str:
    config = _config()
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    for ordinal in range(market_count):
        engine.process_market(_market(ordinal))
    engine.post_funding(
        instrument=_INSTRUMENT,
        amount=Decimal("0"),
        occurred_at=_START + timedelta(seconds=market_count + 1),
        source_event_id="c" * 64,
    )
    engine.pause(
        as_of=_START + timedelta(seconds=market_count + 2),
        reason="golden v3 synthetic committed alert fixture",
        operator_artifact_hash="d" * 64,
    )
    assert store.inspect_integrity_readonly(config.run_id).ok is True
    store.close()
    if include_unlinked_alert:
        _insert_unlinked_alert(database, config.run_id)
    return config.run_id


def _export(
    source: Path,
    output_root: Path,
    run_id: str,
    *,
    progress: object | None = None,
    shard_rows: int = 1_000,
) -> object:
    return export_golden_v3(
        source,
        output_root,
        run_id,
        sentinel_path=source.parent / "forbidden-original.sqlite3",
        require_readonly=False,
        shard_rows=shard_rows,
        shard_bytes=1_000_000,
        progress=progress,
    )


def _manifest(export_root: Path) -> dict[str, Any]:
    decoded = json.loads((export_root / "manifest.json").read_text(encoding="utf-8"))
    assert isinstance(decoded, dict)
    return cast(dict[str, Any], decoded)


def _stream_paths(export_root: Path, stream_name: str) -> tuple[Path, ...]:
    manifest = _manifest(export_root)
    streams = cast(Mapping[str, object], manifest["streams"])
    stream = cast(Mapping[str, object], streams[stream_name])
    shards = cast(list[Mapping[str, object]], stream["shards"])
    return tuple(export_root / str(shard["path"]) for shard in shards)


def _read_stream_rows(export_root: Path, stream_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _stream_paths(export_root, stream_name):
        assert path.suffix == ".jsonl"
        for line in path.read_text(encoding="utf-8").splitlines():
            decoded = json.loads(line)
            assert isinstance(decoded, dict)
            rows.append(cast(dict[str, Any], decoded))
    return rows


def _stream_identity(manifest: Mapping[str, Any]) -> dict[str, tuple[int, str]]:
    streams = cast(Mapping[str, Mapping[str, object]], manifest["streams"])
    return {
        name: (int(stream["row_count"]), str(stream["logical_sha256"]))
        for name, stream in streams.items()
    }


def _iter_jsonl(export_root: Path) -> Iterator[dict[str, Any]]:
    manifest = _manifest(export_root)
    for stream_name in cast(Mapping[str, object], manifest["streams"]):
        yield from _read_stream_rows(export_root, stream_name)


def test_two_independent_exports_have_the_same_complete_logical_identity(
    tmp_path: Path,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    first = tmp_path / "golden-a"
    second = tmp_path / "golden-b"

    _export(source, first, run_id)
    _export(source, second, run_id)

    verify_golden_v3(first)
    verify_golden_v3(second)
    compare_golden_exports(first, second)
    first_manifest = _manifest(first)
    second_manifest = _manifest(second)
    assert first_manifest["root_hash"] == second_manifest["root_hash"]
    assert _stream_identity(first_manifest) == _stream_identity(second_manifest)
    assert (first / "COMPLETE").is_file()
    assert (second / "COMPLETE").is_file()
    assert tuple(_iter_jsonl(first)) == tuple(_iter_jsonl(second))
    census = cast(Mapping[str, object], first_manifest["census"])
    assert census["sqlite_integrity_check"] == "ok"
    assert census["sqlite_foreign_key_violation_count"] == 0


def test_progress_is_bounded_while_every_logical_row_is_exported(tmp_path: Path) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source, market_count=25)
    exported = tmp_path / "golden"
    progress: list[Mapping[str, object]] = []

    _export(source, exported, run_id, progress=progress.append)

    manifest = _manifest(exported)
    streams = cast(Mapping[str, Mapping[str, object]], manifest["streams"])
    total_rows = sum(int(stream["row_count"]) for stream in streams.values())
    stream_progress = [record for record in progress if record.get("phase") == "stream"]
    assert total_rows > len(streams)
    assert len(stream_progress) <= len(streams) + total_rows // 1_000 + 1


def test_source_readonly_size_hash_and_sidecars_are_fail_closed(tmp_path: Path) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    expected_size = source.stat().st_size
    expected_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()

    with pytest.raises(GoldenRefusal, match=r"read.only|writable"):
        export_golden_v3(
            source,
            tmp_path / "writable-refused",
            run_id,
            sentinel_path=tmp_path / "forbidden-original.sqlite3",
            expected_source_size=expected_size,
            expected_source_sha256=expected_sha256,
            require_readonly=True,
        )

    source.chmod(stat.S_IREAD)
    try:
        export_golden_v3(
            source,
            tmp_path / "readonly-export",
            run_id,
            sentinel_path=tmp_path / "forbidden-original.sqlite3",
            expected_source_size=expected_size,
            expected_source_sha256=expected_sha256,
            require_readonly=True,
        )
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)

    assert not any(
        source.with_name(source.name + suffix).exists()
        for suffix in ("-journal", "-shm", "-wal")
    )
    with pytest.raises(GoldenRefusal, match=r"size|SHA-256|fingerprint"):
        export_golden_v3(
            source,
            tmp_path / "wrong-size-refused",
            run_id,
            sentinel_path=tmp_path / "forbidden-original.sqlite3",
            expected_source_size=expected_size + 1,
            expected_source_sha256=expected_sha256,
            require_readonly=False,
        )


def test_projection_history_is_exported_as_complete_logical_json(
    tmp_path: Path,
) -> None:
    source = tmp_path / "compressed-history.sqlite3"
    run_id = _build_source(source)
    with sqlite3.connect(source) as connection:
        source_rows = connection.execute(
            """
            SELECT revision, payload_json, payload_zlib, payload_codec
            FROM paper_projection_history WHERE run_id=? ORDER BY revision
            """,
            (run_id,),
        ).fetchall()
    assert source_rows
    assert {str(row[3]) for row in source_rows} == {"zlib-json-v1"}
    assert all(str(row[1]) == "" and row[2] is not None for row in source_rows)

    exported = tmp_path / "golden"
    _export(source, exported, run_id, shard_rows=2)

    history = _read_stream_rows(exported, "projection_history")
    revisions = [int(row["revision"]) for row in history]
    assert revisions == list(range(len(history)))
    assert revisions[0] == 0
    assert all(isinstance(row["payload"], dict) for row in history)
    assert all("payload_zlib" not in row and "payload_json" not in row for row in history)
    current = _read_stream_rows(exported, "projection_current")
    assert len(current) == 1
    assert current[0]["revision"] == history[-1]["revision"]
    assert current[0]["projection_hash"] == history[-1]["projection_hash"]
    assert current[0]["payload"] == history[-1]["payload"]
    verify_golden_v3(exported)


def test_external_pin_is_distinct_read_only_and_authenticates_the_root(
    tmp_path: Path,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    exported = tmp_path / "golden"
    pin_path = tmp_path / "pins" / "golden-v3.pin.json"
    _export(source, exported, run_id)

    write_external_pin(exported, pin_path)

    try:
        assert pin_path.is_file()
        assert exported not in pin_path.parents
        assert pin_path.stat().st_mode & stat.S_IWRITE == 0
        pin = json.loads(pin_path.read_text(encoding="utf-8"))
        assert pin["root_hash"] == _manifest(exported)["root_hash"]
        verify_golden_v3(exported, pin_path=pin_path)
    finally:
        if pin_path.exists():
            pin_path.chmod(stat.S_IREAD | stat.S_IWRITE)


def test_source_may_not_alias_the_forbidden_original_sentinel(tmp_path: Path) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)

    with pytest.raises(GoldenRefusal, match=r"sentinel|forbidden|same path"):
        export_golden_v3(
            source,
            tmp_path / "golden",
            run_id,
            sentinel_path=source,
            require_readonly=False,
        )


def test_source_rejects_same_name_trigger_with_altered_body(tmp_path: Path) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    with sqlite3.connect(source) as connection:
        connection.execute("DROP TRIGGER paper_events_no_update")
        connection.execute(
            """
            CREATE TRIGGER paper_events_no_update
            BEFORE UPDATE ON paper_events BEGIN
                SELECT 1;
            END
            """
        )

    with pytest.raises(GoldenRefusal, match=r"sqlite_schema DDL|exact PaperStore"):
        _export(source, tmp_path / "golden", run_id)


def test_external_pin_hardlink_alias_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    exported = tmp_path / "golden"
    pin = tmp_path / "golden.pin.json"
    alias = tmp_path / "golden-pin-hardlink.json"
    _export(source, exported, run_id)
    write_external_pin(exported, pin)
    try:
        os.link(pin, alias)
        with pytest.raises(GoldenVerificationError, match=r"pin.*hardlink|hardlinks"):
            verify_golden_v3(exported, pin_path=alias)
    finally:
        if alias.exists():
            alias.chmod(stat.S_IREAD | stat.S_IWRITE)
            alias.unlink()
        if pin.exists():
            pin.chmod(stat.S_IREAD | stat.S_IWRITE)
