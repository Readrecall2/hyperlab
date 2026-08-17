from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from hyperlab.dashboard.app import (
    _paper_readiness,
    _resolve_report_download,
    _runtime_summary,
    create_app,
)
from hyperlab.paper import (
    PaperEngine,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    PaperStore,
)
from hyperlab.storage.sqlite import initialize


def _healthy_runtime(*, updated_at: datetime | None = None) -> dict[str, object]:
    return {
        "schema_version": 1,
        "ok": True,
        "mode": "readonly",
        "orders_enabled": False,
        "updated_at": (updated_at or datetime.now(tz=UTC)).isoformat(),
        "pending_rows": 0,
        "metrics": {
            "state": "live",
            "connection_alive": True,
            "connections": 1,
            "reconnects": 0,
            "gaps": 0,
            "stale_channels": [],
        },
    }


def test_dashboard_is_explicitly_read_only(tmp_path: Path) -> None:
    app = create_app(data_dir=tmp_path)
    client = TestClient(app)
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json() == {"ok": True, "mode": "readonly", "orders_enabled": False}
    paper = client.get("/api/paper")
    assert paper.status_code == 200
    assert paper.json() == {
        "mode": "paper-simulation-only",
        "orders_enabled": False,
        "runs": [],
        "status": "NOT_STARTED",
    }
    for method in ("post", "put", "patch", "delete"):
        assert getattr(client, method)("/api/paper").status_code == 405
    paper_route = next(route for route in app.routes if getattr(route, "path", None) == "/api/paper")
    assert paper_route.methods == {"GET"}
    page = client.get("/")
    assert page.status_code == 200
    assert "ORDRES IMPOSSIBLES" in page.text
    assert "sans attendre Gate D" in page.text
    assert "Tout exécuteur Micro/Mainnet" in page.text


def test_liveness_is_distinct_from_fail_closed_readiness(tmp_path: Path) -> None:
    client = TestClient(create_app(data_dir=tmp_path))

    for path in ("/health", "/health/live", "/live"):
        response = client.get(path)
        assert response.status_code == 200
        assert response.json() == {
            "ok": True,
            "mode": "readonly",
            "orders_enabled": False,
        }

    for path in ("/health/ready", "/ready"):
        response = client.get(path)
        assert response.status_code == 503
        assert response.json() == {
            "ok": False,
            "ready": False,
            "mode": "readonly",
            "orders_enabled": False,
            "checks": {
                "runtime_status": "missing",
                "legacy_database": "ready",
                "paper_database": "not_configured",
            },
        }


def test_readiness_requires_fresh_connected_readonly_collector_status(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "runtime_status.json").write_text(
        json.dumps(_healthy_runtime()),
        encoding="utf-8",
    )
    client = TestClient(create_app(data_dir=tmp_path / "data", runtime_dir=runtime_dir))

    response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "ready": True,
        "mode": "readonly",
        "orders_enabled": False,
        "checks": {
            "runtime_status": "ready",
            "legacy_database": "ready",
            "paper_database": "not_configured",
        },
    }


@pytest.mark.parametrize(
    ("mutation", "expected_check"),
    [
        ({"updated_at": "2026-08-13T00:00:00+00:00"}, "stale"),
        ({"orders_enabled": True}, "unsafe_mode"),
        ({"ok": False}, "collector_unhealthy"),
        (
            {"metrics": {"state": "live", "connection_alive": False, "stale_channels": []}},
            "collector_disconnected",
        ),
        (
            {"metrics": {"state": "live", "connection_alive": True, "stale_channels": ["l2Book:BTC"]}},
            "stale_data",
        ),
    ],
)
def test_readiness_rejects_stale_unsafe_or_unhealthy_runtime(
    tmp_path: Path,
    mutation: dict[str, object],
    expected_check: str,
) -> None:
    runtime = _healthy_runtime()
    runtime.update(mutation)
    (tmp_path / "runtime_status.json").write_text(json.dumps(runtime), encoding="utf-8")

    response = TestClient(create_app(data_dir=tmp_path)).get("/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["runtime_status"] == expected_check
    assert response.json()["orders_enabled"] is False


def test_dashboard_masks_a_corrupt_projection_without_mutating_the_database(
    tmp_path: Path,
) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    database = paper_dir / "paper.sqlite3"
    config = PaperRunConfig(
        strategy_name="dashboard_corruption_fixture",
        strategy_hash="a" * 64,
        parameters={"version": 1},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(),
        risk=PaperRiskLimits(),
        seed=12,
        initial_cash=Decimal("100000"),
        validation_started_at=datetime(2026, 8, 13, tzinfo=UTC),
    )
    PaperEngine(PaperStore(database), config).start()
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER paper_events_no_update")
        connection.execute(
            "UPDATE paper_events SET payload_json = ? WHERE run_id = ? AND sequence = 1",
            ("{}", config.run_id),
        )
    corrupt_bytes = database.read_bytes()

    response = TestClient(create_app(data_dir=tmp_path)).get("/api/paper")
    (tmp_path / "runtime_status.json").write_text(
        json.dumps(_healthy_runtime()),
        encoding="utf-8",
    )
    readiness = TestClient(create_app(data_dir=tmp_path)).get("/ready")

    assert response.status_code == 200
    assert readiness.status_code == 503
    assert readiness.json()["checks"]["paper_database"] == "corrupt"
    assert database.read_bytes() == corrupt_bytes
    payload = response.json()
    assert payload["status"] == "AVAILABLE"
    assert len(payload["runs"]) == 1
    run = payload["runs"][0]
    assert run["run_id"] == config.run_id
    assert run["integrity"] == "HEAD_ANCHORS_FAILED_READONLY"
    assert run["status"] == "MANUAL_REVIEW"
    assert run["projection"] is None
    assert run["orders_enabled"] is False


def _dashboard_paper_config(seed: int) -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name=f"dashboard_same_head_fixture_{seed}",
        strategy_hash="a" * 64,
        parameters={"version": 1},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(),
        risk=PaperRiskLimits(),
        seed=seed,
        initial_cash=Decimal("100000"),
        validation_started_at=datetime(2026, 8, 17, tzinfo=UTC),
    )


def test_paper_api_bounds_run_and_alert_queries_without_lifetime_collectors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    database = paper_dir / "paper.sqlite3"
    config = PaperRunConfig(
        strategy_name="dashboard_bounded_status_fixture",
        strategy_hash="a" * 64,
        parameters={"version": 1},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(),
        risk=PaperRiskLimits(),
        seed=31,
        initial_cash=Decimal("100000"),
        validation_started_at=datetime(2026, 8, 17, tzinfo=UTC),
    )
    store = PaperStore(database)
    PaperEngine(store, config).start()
    run = store.get_run(config.run_id)
    list_limits: list[int | None] = []
    alert_limits: list[int] = []

    def bounded_list_runs(
        _store: PaperStore,
        *,
        limit: int | None = None,
    ):  # type: ignore[no-untyped-def]
        list_limits.append(limit)
        return (run,) * 51

    def bounded_recent_alerts(
        _store: PaperStore,
        _run_id: str,
        *,
        limit: int,
    ):  # type: ignore[no-untyped-def]
        alert_limits.append(limit)
        return ()

    def forbid_unbounded_alerts(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("dashboard may not collect lifetime alert history")

    monkeypatch.setattr(PaperStore, "list_runs", bounded_list_runs)
    monkeypatch.setattr(PaperStore, "get_recent_alerts", bounded_recent_alerts)
    monkeypatch.setattr(PaperStore, "get_alerts", forbid_unbounded_alerts)

    response = TestClient(create_app(data_dir=tmp_path)).get("/api/paper")

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_limit"] == 50
    assert payload["runs_truncated"] is True
    assert len(payload["runs"]) == 50
    assert list_limits == [51, 51]
    assert alert_limits == [51] * 50
    assert all(run_payload["alert_limit"] == 50 for run_payload in payload["runs"])
    assert all(run_payload["alerts_truncated"] is False for run_payload in payload["runs"])


def test_paper_api_exposes_bounded_active_runtime_session_and_incident(tmp_path: Path) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    database = paper_dir / "paper.sqlite3"
    config = _dashboard_paper_config(32)
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    engine.start_runtime_session(
        as_of=datetime(2026, 8, 17, 0, 0, 1, tzinfo=UTC),
        session_id="e" * 64,
        generation=1,
    )
    engine.pause(
        as_of=datetime(2026, 8, 17, 0, 0, 2, tzinfo=UTC),
        reason="terminal paper runtime failure: TEST_PHASE: RuntimeError",
        operator_artifact_hash="f" * 64,
        origin="PAPER_RUNTIME_FAILURE",
    )
    store.close()

    response = TestClient(create_app(data_dir=tmp_path)).get("/api/paper")

    assert response.status_code == 200
    session = response.json()["runs"][0]["runtime_session"]
    assert session["active"] is True
    assert session["unclosed"] is True
    assert session["generation"] == 1
    assert session["session_id"] == "e" * 64
    assert session["started_at"] == "2026-08-17T00:00:01.000000Z"
    assert session["stopped_at"] is None
    assert [incident["code"] for incident in session["recent_incidents"]] == [
        "PAPER_RUNTIME_FAILURE"
    ]
    assert {"pid", "process_id", "database_path", "lock_path"}.isdisjoint(session)


def test_paper_readiness_fails_closed_before_inspecting_more_than_100_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "paper.sqlite3"
    store = PaperStore(database)
    store.create_run("bounded-readiness-run", {"fixture": True})
    run = store.get_run("bounded-readiness-run")
    limits: list[int | None] = []

    def bounded_list_runs(
        _store: PaperStore,
        *,
        limit: int | None = None,
    ):  # type: ignore[no-untyped-def]
        limits.append(limit)
        return (run,) * 101

    def forbid_integrity(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("too-many-runs must fail before per-run inspection")

    monkeypatch.setattr(PaperStore, "list_runs", bounded_list_runs)
    monkeypatch.setattr(PaperStore, "inspect_head_integrity_readonly", forbid_integrity)

    assert _paper_readiness(database) == "too_many_runs"
    assert limits == [101]


@pytest.mark.parametrize(
    ("injected_commits", "expected_status_code", "expected_status"),
    [
        (1, 200, "AVAILABLE"),
        (2, 409, "HEAD_CHANGED_RETRY"),
    ],
)
def test_paper_api_same_head_retry_is_bounded_and_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    injected_commits: int,
    expected_status_code: int,
    expected_status: str,
) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    database = paper_dir / "paper.sqlite3"
    config = _dashboard_paper_config(40 + injected_commits)
    writer_store = PaperStore(database)
    writer = PaperEngine(writer_store, config)
    writer.start()
    original_recent_alerts = PaperStore.get_recent_alerts
    calls = 0

    def racing_recent_alerts(
        store: PaperStore,
        run_id: str,
        *,
        limit: int,
    ):  # type: ignore[no-untyped-def]
        nonlocal calls
        alerts = original_recent_alerts(store, run_id, limit=limit)
        calls += 1
        if calls <= injected_commits:
            writer.reconcile(as_of=datetime(2026, 8, 17, tzinfo=UTC) + timedelta(seconds=calls))
        return alerts

    monkeypatch.setattr(PaperStore, "get_recent_alerts", racing_recent_alerts)

    response = TestClient(create_app(data_dir=tmp_path)).get("/api/paper")
    writer_store.close()

    assert response.status_code == expected_status_code
    payload = response.json()
    assert payload["status"] == expected_status
    assert calls == 2
    if expected_status_code == 200:
        assert payload["same_head_assembly"] is True
        assert payload["head_read_attempt_limit"] == 2
        assert payload["runs"][0]["same_head_assembly"] is True
    else:
        assert payload["retryable"] is True
        assert payload["orders_enabled"] is False


@pytest.mark.parametrize(
    ("injected_commits", "expected"),
    [(1, "ready"), (2, "head_changed_retry")],
)
def test_paper_readiness_same_head_retry_is_bounded_and_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    injected_commits: int,
    expected: str,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _dashboard_paper_config(50 + injected_commits)
    writer_store = PaperStore(database)
    writer = PaperEngine(writer_store, config)
    writer.start()
    original_integrity = PaperStore.inspect_head_integrity_readonly
    calls = 0

    def racing_integrity(
        store: PaperStore,
        run_id: str,
    ):  # type: ignore[no-untyped-def]
        nonlocal calls
        integrity = original_integrity(store, run_id)
        calls += 1
        if calls <= injected_commits:
            writer.reconcile(as_of=datetime(2026, 8, 17, tzinfo=UTC) + timedelta(seconds=calls))
        return integrity

    monkeypatch.setattr(
        PaperStore,
        "inspect_head_integrity_readonly",
        racing_integrity,
    )

    assert _paper_readiness(database) == expected
    assert calls == 2
    writer_store.close()


def test_paper_api_retains_newest_runs_after_dropping_the_oldest_sentinel(
    tmp_path: Path,
) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    database = paper_dir / "paper.sqlite3"
    store = PaperStore(database)
    for ordinal in range(52):
        store.create_run(
            f"bounded-dashboard-{ordinal:02d}",
            {"fixture": ordinal},
            seed=ordinal,
            created_at=f"2026-08-17T08:{ordinal:02d}:00Z",
        )
    store.close()

    response = TestClient(create_app(data_dir=tmp_path)).get("/api/paper")

    assert response.status_code == 200
    payload = response.json()
    assert payload["runs_truncated"] is True
    assert [run["run_id"] for run in payload["runs"]] == [
        f"bounded-dashboard-{ordinal:02d}" for ordinal in range(2, 52)
    ]


def test_legacy_status_queries_do_not_initialize_or_mutate_sqlite(tmp_path: Path) -> None:
    database = tmp_path / "hyperlab.sqlite3"

    initialize(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO carry_snapshots VALUES (
                1, 'BTC', 'BTC/USDC', '1', '1', '0', '0', '1', '1', '1'
            )
            """
        )
    original_bytes = database.read_bytes()
    original_names = {path.name for path in tmp_path.iterdir()}
    client = TestClient(create_app(data_dir=tmp_path))

    status = client.get("/api/status")
    page = client.get("/")

    assert status.status_code == 200
    assert status.json()["snapshot_count"] == 1
    assert page.status_code == 200
    assert database.read_bytes() == original_bytes
    assert {path.name for path in tmp_path.iterdir()} == original_names


def test_corrupt_legacy_sqlite_fails_closed_without_mutation(tmp_path: Path) -> None:
    database = tmp_path / "hyperlab.sqlite3"
    database.write_bytes(b"not a sqlite database")
    (tmp_path / "runtime_status.json").write_text(
        json.dumps(_healthy_runtime()),
        encoding="utf-8",
    )
    original_bytes = database.read_bytes()
    client = TestClient(create_app(data_dir=tmp_path))

    status = client.get("/api/status")
    readiness = client.get("/ready")

    assert status.status_code == 503
    assert status.json()["status"] == "UNREADABLE_FAIL_CLOSED"
    assert readiness.status_code == 503
    assert readiness.json()["checks"]["legacy_database"] == "unreadable"
    assert database.read_bytes() == original_bytes


def test_dashboard_escapes_report_title(tmp_path: Path) -> None:
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True)
    (report_dir / "latest_summary.json").write_text(
        '{"title": "<script>alert(1)</script>"}',
        encoding="utf-8",
    )
    page = TestClient(create_app(data_dir=tmp_path)).get("/")
    assert page.status_code == 200
    assert "<script>alert(1)</script>" not in page.text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page.text
    assert 'href="/api/reports/latest_summary.json"' in page.text


def test_dashboard_latest_summary_links_to_verified_export_target(tmp_path: Path) -> None:
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True)
    (report_dir / "public export.parquet").write_bytes(b"PAR1")
    (report_dir / "latest_summary.json").write_text(
        '{"title":"Public export","download_path":"public export.parquet"}',
        encoding="utf-8",
    )

    page = TestClient(create_app(data_dir=tmp_path)).get("/")

    assert page.status_code == 200
    assert 'href="/api/reports/public%20export.parquet"' in page.text


def test_report_download_uses_explicit_root_and_safe_attachment_headers(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    reports_dir = tmp_path / "published-reports"
    nested = reports_dir / "research"
    nested.mkdir(parents=True)
    report = nested / "result.csv"
    report.write_bytes(b"metric,value\nnet,0\n")
    outside = tmp_path / "secret.json"
    outside.write_text('{"private": true}', encoding="utf-8")
    client = TestClient(create_app(data_dir=data_dir, reports_dir=reports_dir))

    response = client.get("/api/reports/research/result.csv")

    assert response.status_code == 200
    assert response.content == report.read_bytes()
    assert response.headers["content-type"] == "application/octet-stream"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["content-disposition"].startswith("attachment;")
    assert "result.csv" in response.headers["content-disposition"]
    for unsafe in (
        "../secret.json",
        ".hidden.json",
        "research/../result.csv",
        "research\\result.csv",
        "research/private.key",
        "research/result.csv:stream",
    ):
        assert _resolve_report_download(reports_dir, unsafe) is None
    assert _resolve_report_download(reports_dir, "research/result.csv") == report.resolve()
    for method in ("post", "put", "patch", "delete"):
        assert getattr(client, method)("/api/reports/research/result.csv").status_code == 405


def test_report_download_rejects_symlink_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    link = reports_dir / "linked.json"
    try:
        link.symlink_to(outside)
    except OSError:
        path_type = type(link)
        original_is_symlink = path_type.is_symlink
        monkeypatch.setattr(
            path_type,
            "is_symlink",
            lambda self: self == link or original_is_symlink(self),
        )

    assert _resolve_report_download(reports_dir, link.name) is None
    response = TestClient(create_app(data_dir=tmp_path / "data", reports_dir=reports_dir)).get(
        "/api/reports/linked.json"
    )
    assert response.status_code == 404


def test_serve_uses_explicit_runtime_report_and_paper_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from hyperlab import __version__
    from hyperlab import cli as cli_module

    data_dir = tmp_path / "data"
    runtime_dir = tmp_path / "runtime"
    reports_dir = tmp_path / "reports"
    paper_dir = tmp_path / "paper"
    runtime_dir.mkdir()
    reports_dir.mkdir()
    paper_dir.mkdir()
    (runtime_dir / "runtime_status.json").write_text(
        json.dumps(_healthy_runtime()),
        encoding="utf-8",
    )
    (reports_dir / "latest_summary.json").write_text(
        '{"title": "Explicit report root"}',
        encoding="utf-8",
    )
    monkeypatch.setenv("HYPERLAB_RUNTIME_DIR", str(runtime_dir))
    monkeypatch.setenv("HYPERLAB_REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("HYPERLAB_PAPER_DIR", str(paper_dir))
    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda: SimpleNamespace(app=SimpleNamespace(data_dir=data_dir)),
    )
    captured: dict[str, object] = {}

    def fake_run(app: object, *, host: str, port: int) -> None:
        captured.update({"app": app, "host": host, "port": port})

    monkeypatch.setattr("uvicorn.run", fake_run)

    cli_module.serve(port=8123)

    dashboard = cast(FastAPI, captured["app"])
    assert dashboard.version == __version__
    client = TestClient(dashboard)
    assert client.get("/ready").status_code == 200
    assert client.get("/api/reports/latest_summary.json").status_code == 200
    assert client.get("/api/paper").json()["status"] == "NOT_STARTED"
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8123


def test_old_live_runtime_status_is_not_reported_as_active(tmp_path: Path) -> None:
    now = datetime(2026, 8, 12, 12, tzinfo=UTC)
    runtime = {
        "updated_at": (now - timedelta(minutes=2)).isoformat(),
        "metrics": {
            "state": "live",
            "connection_alive": True,
            "connections": 1,
            "reconnects": 0,
            "gaps": 0,
            "stale_channels": [],
        },
    }

    summary = _runtime_summary(runtime, now=now)

    assert summary["state"] == "RUNTIME STATUS STALE"
    assert summary["connection"].startswith("inactive")
    assert "runtime_status.json stale" in summary["stale_detail"]

    (tmp_path / "runtime_status.json").write_text(
        json.dumps(runtime),
        encoding="utf-8",
    )
    response = TestClient(create_app(data_dir=tmp_path)).get("/api/status")
    assert response.status_code == 200
    assert response.json()["runtime_status_stale"] is True
