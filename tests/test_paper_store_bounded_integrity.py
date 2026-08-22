from __future__ import annotations

import sqlite3
import zlib
from collections import defaultdict
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import TracebackType

import pytest

from hyperlab.paper import (
    MarketEvent,
    PaperEngine,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    PaperStore,
)

_START = datetime(2026, 8, 17, 8, tzinfo=UTC)
_INSTRUMENT = "HYPERLIQUID:BTC:perp"


def _config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="bounded_integrity_fixture",
        strategy_hash="a" * 64,
        parameters={"fixture": "bounded-integrity"},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(
            calibration_status="SYNTHETIC",
            source="deterministic-test-fixture",
        ),
        risk=PaperRiskLimits(),
        seed=7,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        data_calibration_status="SYNTHETIC",
        data_source="deterministic-test-fixture",
    )


def _build_journal(
    database: Path,
    *,
    market_count: int,
) -> tuple[PaperStore, PaperRunConfig]:
    config = _config()
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    for ordinal in range(market_count):
        engine.process_market(
            MarketEvent.create(
                received_at=_START + timedelta(seconds=ordinal + 1),
                instrument=_INSTRUMENT,
                bid_price=Decimal("100"),
                ask_price=Decimal("101"),
                bid_depth=Decimal("100"),
                ask_depth=Decimal("100"),
                source_sequence=ordinal + 1,
            )
        )
    funding_at = _START + timedelta(seconds=market_count + 1)
    engine.post_funding(
        instrument=_INSTRUMENT,
        amount=Decimal("0"),
        occurred_at=funding_at,
        source_event_id="d" * 64,
    )
    engine.pause(
        as_of=funding_at + timedelta(seconds=1),
        reason="bounded integrity alert fixture",
        operator_artifact_hash="c" * 64,
    )
    return store, config


class _NoFetchAllCursor:
    def __init__(self, inner: sqlite3.Cursor) -> None:
        self._inner = inner

    def __iter__(self) -> Iterator[sqlite3.Row]:
        return iter(self._inner)

    def fetchone(self) -> sqlite3.Row | None:
        return self._inner.fetchone()

    def fetchall(self) -> list[sqlite3.Row]:
        raise AssertionError("full integrity inspection must not call fetchall")


class _NoFetchAllConnection:
    def __init__(self, inner: sqlite3.Connection) -> None:
        self._inner = inner

    def __enter__(self) -> _NoFetchAllConnection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self._inner.close()

    def execute(
        self,
        sql: str,
        parameters: tuple[object, ...] = (),
    ) -> _NoFetchAllCursor:
        return _NoFetchAllCursor(self._inner.execute(sql, parameters))


def test_full_integrity_inspection_forbids_fetchall_for_all_history_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, config = _build_journal(tmp_path / "no-fetchall.sqlite3", market_count=12)
    read_connection = store._read_connection

    def no_fetchall_connection() -> _NoFetchAllConnection:
        return _NoFetchAllConnection(read_connection())

    monkeypatch.setattr(store, "_read_connection", no_fetchall_connection)

    assert store.inspect_integrity_readonly(config.run_id).ok is True


def test_large_journal_integrity_scan_has_bounded_python_side_cardinality(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "large-journal.sqlite3"
    store, config = _build_journal(database, market_count=512)
    maxima: defaultdict[str, int] = defaultdict(int)

    def observe(name: str, size: int) -> None:
        maxima[name] = max(maxima[name], size)

    monkeypatch.setattr(store, "_observe_integrity_buffer", observe)

    report = store.inspect_integrity_readonly(config.run_id)

    assert report.ok is True
    for name in (
        "event_row",
        "ledger_transaction_row",
        "ledger_entry_row",
        "alert_row",
        "inbox_row",
        "commit_row",
        "projection_history_row",
    ):
        assert maxima[name] == 1
    assert maxima["transaction_entries"] <= 4
    assert maxima["transaction_units"] <= 2
    for name in (
        "commit_event_hashes",
        "commit_ledger_hashes",
        "commit_alert_hashes",
        "stored_commit_event_hashes",
        "stored_commit_ledger_hashes",
        "stored_commit_alert_hashes",
    ):
        assert maxima[name] <= 4

    with sqlite3.connect(database) as connection:
        counts = [
            int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE run_id=?",
                    (config.run_id,),
                ).fetchone()[0]
            )
            for table in (
                "paper_events",
                "paper_inbox",
                "paper_commits",
                "paper_projection_history",
            )
        ]
    assert min(counts) > 500

    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER paper_events_no_update")
        connection.execute(
            "UPDATE paper_events SET payload_json='{}' WHERE run_id=?",
            (config.run_id,),
        )
    maxima.clear()

    corrupt_report = store.inspect_integrity_readonly(config.run_id)

    assert corrupt_report.ok is False
    assert maxima["event_row"] == 1
    assert maxima["issue_codes"] < 16
    assert len(corrupt_report.issues) < 16


def test_large_journal_integrity_scan_has_bounded_sql_query_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, config = _build_journal(
        tmp_path / "bounded-query-count.sqlite3",
        market_count=512,
    )
    read_connection = store._read_connection
    statements: list[str] = []

    class CountingConnection(_NoFetchAllConnection):
        def execute(
            self,
            sql: str,
            parameters: tuple[object, ...] = (),
        ) -> _NoFetchAllCursor:
            statements.append(" ".join(sql.split()))
            return super().execute(sql, parameters)

    def counting_connection() -> CountingConnection:
        return CountingConnection(read_connection())

    monkeypatch.setattr(store, "_read_connection", counting_connection)

    assert store.inspect_integrity_readonly(config.run_id).ok is True
    # Commit components and projection/commit anchors must be streamed in
    # order. Query count must not grow once per retained commit/revision.
    assert len(statements) < 64
    assert not any(
        "SELECT event_hash FROM paper_events WHERE run_id=? AND input_id=?" in statement
        for statement in statements
    )
    assert not any(
        "FROM paper_projection_history WHERE run_id=? AND revision=?" in statement for statement in statements
    )


def test_ledger_heavy_semantic_reconciliation_uses_one_streaming_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    store = PaperStore(tmp_path / "ledger-heavy-reconciliation.sqlite3")
    engine = PaperEngine(store, config)
    engine.start()
    for ordinal in range(512):
        engine.post_funding(
            instrument=_INSTRUMENT,
            amount=Decimal("0"),
            occurred_at=_START + timedelta(milliseconds=ordinal + 1),
            source_event_id=f"{ordinal + 1:064x}",
        )
    projection = engine.projection()
    ledger_entry_count = sum(1 for _entry in store.iter_ledger_entries(config.run_id))
    read_connection = store._read_connection
    statements: list[str] = []

    class CountingConnection(_NoFetchAllConnection):
        def execute(
            self,
            sql: str,
            parameters: tuple[object, ...] = (),
        ) -> _NoFetchAllCursor:
            statements.append(" ".join(sql.split()))
            return super().execute(sql, parameters)

    def counting_connection() -> CountingConnection:
        return CountingConnection(read_connection())

    monkeypatch.setattr(store, "_read_connection", counting_connection)

    assert ledger_entry_count == 1_026
    assert engine._ledger_reconciliation_errors(projection) == ()
    assert len(statements) == 1
    assert "FROM paper_ledger_entries AS ledger" in statements[0]


def test_v1_store_migrates_to_covering_event_input_index_without_head_drift(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper-v1-index-migration.sqlite3"
    store, config = _build_journal(database, market_count=32)
    before = store.get_run(config.run_id)
    before_projection = store.get_projection(config.run_id).to_dict()
    store.close()
    with sqlite3.connect(database) as connection:
        connection.execute("DROP INDEX paper_events_input_idx")
        connection.execute("CREATE INDEX paper_events_input_idx ON paper_events(run_id, input_id)")
        connection.execute("UPDATE paper_schema SET version=1 WHERE singleton=1")
        connection.execute("PRAGMA user_version=1")

    migrated = PaperStore(database)
    after = migrated.get_run(config.run_id)
    assert after.head_identity == before.head_identity
    assert migrated.get_projection(config.run_id).to_dict() == before_projection
    assert migrated.inspect_integrity_readonly(config.run_id).ok is True
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)
        columns = tuple(
            str(row[2]) for row in connection.execute("PRAGMA index_info('paper_events_input_idx')")
        )
        input_id = connection.execute(
            "SELECT input_id FROM paper_commits WHERE run_id=? AND commit_sequence=2",
            (config.run_id,),
        ).fetchone()[0]
        plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT event_hash FROM paper_events
            WHERE run_id=? AND input_id=? ORDER BY sequence
            """,
            (config.run_id, input_id),
        ).fetchall()
    assert columns == ("run_id", "input_id", "sequence")
    assert any("paper_events_input_idx" in str(row[3]) for row in plan)


@pytest.mark.parametrize(
    ("setup_sql", "tamper_sql", "expected_issue"),
    [
        (
            "DROP TRIGGER paper_events_no_update",
            "UPDATE paper_events SET payload_json='{}' WHERE sequence=1",
            "EVENT_PAYLOAD_HASH",
        ),
        (
            "DROP TRIGGER paper_ledger_entries_no_update",
            "UPDATE paper_ledger_entries SET amount_text='99' WHERE entry_index=0",
            "LEDGER_AMOUNT_MISMATCH",
        ),
        (
            "DROP TRIGGER paper_ledger_entries_no_update",
            "UPDATE paper_ledger_entries SET account='tampered' WHERE entry_index=0",
            "LEDGER_ENTRY_METADATA",
        ),
        (
            "DROP TRIGGER paper_alerts_no_update",
            "UPDATE paper_alerts SET payload_hash=printf('%064d', 0) WHERE commit_sequence IS NOT NULL",
            "ALERT_HASH",
        ),
        (
            "DROP TRIGGER paper_alerts_no_update",
            "UPDATE paper_alerts SET severity='INFO' WHERE commit_sequence IS NOT NULL",
            "ALERT_METADATA_MISMATCH",
        ),
        (
            "DROP TRIGGER paper_inbox_no_update",
            "UPDATE paper_inbox SET payload_hash=printf('%064d', 0) WHERE commit_sequence=1",
            "INPUT_HASH",
        ),
        (
            "DROP TRIGGER paper_commits_no_update",
            "UPDATE paper_commits SET event_hashes_json='[]' WHERE commit_sequence=1",
            "COMMIT_EVENT_HASHES",
        ),
        (
            "DROP TRIGGER paper_projection_history_no_update",
            "UPDATE paper_projection_history SET projection_hash=printf('%064d', 0) WHERE revision=1",
            "PROJECTION_HASH",
        ),
    ],
)
def test_streaming_integrity_scan_still_detects_authoritative_history_tampering(
    tmp_path: Path,
    setup_sql: str,
    tamper_sql: str,
    expected_issue: str,
) -> None:
    database = tmp_path / f"{expected_issue}.sqlite3"
    store, config = _build_journal(database, market_count=4)
    with sqlite3.connect(database) as connection:
        connection.execute(setup_sql)
        connection.execute(tamper_sql + " AND run_id=?", (config.run_id,))

    report = store.inspect_integrity_readonly(config.run_id)

    assert report.ok is False
    assert expected_issue in {issue.code for issue in report.issues}


def test_list_runs_applies_optional_limit_in_sql_and_preserves_unlimited_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = PaperStore(tmp_path / "bounded-runs.sqlite3")
    for ordinal in range(4):
        store.create_run(
            f"bounded-run-{ordinal}",
            {"ordinal": ordinal},
            created_at=f"2026-08-17T08:00:0{ordinal}Z",
        )

    queries: list[tuple[str, tuple[object, ...]]] = []
    read_connection = store._read_connection

    class RecordingConnection:
        def __init__(self, inner: sqlite3.Connection) -> None:
            self._inner = inner

        def __enter__(self) -> RecordingConnection:
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            del exc_type, exc_value, traceback
            self._inner.close()

        def execute(
            self,
            sql: str,
            parameters: tuple[object, ...] = (),
        ) -> sqlite3.Cursor:
            queries.append((sql, parameters))
            return self._inner.execute(sql, parameters)

    def recording_connection() -> RecordingConnection:
        return RecordingConnection(read_connection())

    monkeypatch.setattr(store, "_read_connection", recording_connection)

    bounded = store.list_runs(limit=2)
    unlimited = store.list_runs()

    assert [run.run_id for run in bounded] == [
        "bounded-run-2",
        "bounded-run-3",
    ]
    assert [run.run_id for run in unlimited] == [
        "bounded-run-0",
        "bounded-run-1",
        "bounded-run-2",
        "bounded-run-3",
    ]
    assert " LIMIT ?" in queries[0][0]
    assert queries[0][1] == (2,)
    assert " LIMIT ?" not in queries[1][0]
    assert "ORDER BY created_at DESC, run_id DESC" in queries[0][0]
    assert [run.created_at for run in bounded] == [
        "2026-08-17T08:00:02Z",
        "2026-08-17T08:00:03Z",
    ]
    with pytest.raises(ValueError, match="positive integer"):
        store.list_runs(limit=0)
    with pytest.raises(ValueError, match="positive integer"):
        store.list_runs(limit=True)


def test_contains_alert_is_exact_indexed_point_lookup_without_history_collection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, config = _build_journal(tmp_path / "contains-alert.sqlite3", market_count=2)
    alert = store.get_recent_alerts(config.run_id, limit=1)[0]

    def forbid_get_alerts(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("contains_alert may not collect alert history")

    monkeypatch.setattr(store, "get_alerts", forbid_get_alerts)

    assert store.contains_alert(config.run_id, alert.alert_id) is True
    assert store.contains_alert(config.run_id, "0" * 64) is False


def test_get_latest_alert_uses_bounded_indexed_severity_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "latest-alert.sqlite3"
    store, config = _build_journal(database, market_count=2)
    engine = PaperEngine(store, config)
    engine.start()
    first = engine.pause(
        as_of=_START + timedelta(seconds=5),
        reason="first critical runtime failure",
        operator_artifact_hash="d" * 64,
        origin="PAPER_RUNTIME_FAILURE",
    )
    second = engine.pause(
        as_of=_START + timedelta(seconds=6),
        reason="latest critical runtime failure",
        operator_artifact_hash="e" * 64,
        origin="PAPER_RUNTIME_FAILURE",
    )

    def forbid_history(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("latest alert lookup may not collect alert history")

    monkeypatch.setattr(store, "get_alerts", forbid_history)
    monkeypatch.setattr(store, "get_recent_alerts", forbid_history)

    latest = store.get_latest_alert(config.run_id)
    latest_critical = store.get_latest_alert(config.run_id, severity="CRITICAL")
    latest_warning = store.get_latest_alert(config.run_id, severity="WARNING")

    assert latest is not None
    assert latest_critical is not None
    assert latest_warning is not None
    assert latest.alert_id == latest_critical.alert_id
    assert latest_critical.event_sequence == second.append.last_sequence
    assert latest_critical.event_sequence > first.append.last_sequence
    assert latest_critical.code == "PAPER_RUNTIME_FAILURE"
    assert latest_warning.code == "OPERATOR_PAUSE"
    assert store.get_latest_alert(config.run_id, severity="INFO") is None

    with sqlite3.connect(database) as connection:
        indexes = {str(row[1]) for row in connection.execute("PRAGMA index_list('paper_alerts')")}
        plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT * FROM paper_alerts
            WHERE run_id=? AND severity=?
            ORDER BY event_sequence DESC, created_at DESC, alert_id DESC
            LIMIT 1
            """,
            (config.run_id, "CRITICAL"),
        ).fetchall()
    assert "paper_alerts_run_severity_sequence_idx" in indexes
    assert any("paper_alerts_run_severity_sequence_idx" in str(row[3]) for row in plan)


def test_v3_projection_history_uses_compressed_payloads(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper-v3-compressed-history.sqlite3"
    store, config = _build_journal(database, market_count=8)

    assert store.inspect_integrity_readonly(config.run_id).ok is True

    with sqlite3.connect(database) as connection:
        row = connection.execute(
            """
            SELECT payload_json, payload_zlib, payload_codec
            FROM paper_projection_history
            WHERE run_id=?
            ORDER BY revision DESC
            LIMIT 1
            """,
            (config.run_id,),
        ).fetchone()

        assert row is not None
        assert row[0] == ""
        assert isinstance(row[1], bytes)
        assert len(row[1]) > 0
        assert row[2] == "zlib-json-v1"
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)

    projection = store.get_projection(config.run_id)
    before = store.get_projection_before_received_at(
        config.run_id,
        before=projection.last_received_at + timedelta(microseconds=1),
    )
    assert before is not None
    assert before.to_dict() == projection.to_dict()

    daily = store.get_daily_projection_records(config.run_id, limit=7)
    assert daily
    assert daily[-1].projection_hash == store.get_run(config.run_id).projection_hash


def test_v2_store_migrates_to_v3_without_rewriting_existing_history(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper-v2-to-v3.sqlite3"
    store, config = _build_journal(database, market_count=8)
    before = store.get_run(config.run_id)
    before_projection = store.get_projection(config.run_id).to_dict()
    store.close()

    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER paper_projection_history_no_update")
        rows = connection.execute(
            """
            SELECT run_id, revision, payload_zlib
            FROM paper_projection_history
            """
        ).fetchall()
        for run_id, revision, payload_zlib in rows:
            assert payload_zlib is not None
            payload_json = zlib.decompress(payload_zlib).decode("utf-8")
            connection.execute(
                """
                UPDATE paper_projection_history
                SET payload_json=?,
                    payload_codec='json',
                    payload_zlib=NULL,
                    last_received_at=NULL,
                    utc_date=NULL
                WHERE run_id=? AND revision=?
                """,
                (payload_json, run_id, revision),
            )
        connection.execute("ALTER TABLE paper_projection_history RENAME TO paper_projection_history_v3")
        connection.execute(
            """
            CREATE TABLE paper_projection_history (
                run_id TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK (revision >= 0),
                input_id TEXT,
                event_sequence INTEGER NOT NULL CHECK (event_sequence >= 0),
                event_head_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                projection_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_id, revision),
                FOREIGN KEY (run_id) REFERENCES paper_runs(run_id)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO paper_projection_history (
                run_id, revision, input_id, event_sequence,
                event_head_hash, status, payload_json,
                projection_hash, created_at
            )
            SELECT
                run_id, revision, input_id, event_sequence,
                event_head_hash, status, payload_json,
                projection_hash, created_at
            FROM paper_projection_history_v3
            """
        )
        connection.execute("DROP TABLE paper_projection_history_v3")
        connection.executescript(
            """
            CREATE TRIGGER paper_projection_history_no_update
            BEFORE UPDATE ON paper_projection_history BEGIN
                SELECT RAISE(
                    ABORT,
                    'paper projection history is append-only'
                );
            END;

            CREATE TRIGGER paper_projection_history_no_delete
            BEFORE DELETE ON paper_projection_history BEGIN
                SELECT RAISE(
                    ABORT,
                    'paper projection history is append-only'
                );
            END;
            """
        )
        connection.execute("UPDATE paper_schema SET version=2 WHERE singleton=1")
        connection.execute("PRAGMA user_version=2")

    migrated = PaperStore(database)

    assert migrated.get_run(config.run_id).head_identity == before.head_identity
    assert migrated.get_projection(config.run_id).to_dict() == before_projection
    assert migrated.inspect_integrity_readonly(config.run_id).ok is True

    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)
        codecs = connection.execute(
            """
            SELECT DISTINCT payload_codec
            FROM paper_projection_history
            WHERE run_id=?
            """,
            (config.run_id,),
        ).fetchall()

    assert codecs == [("json",)]
