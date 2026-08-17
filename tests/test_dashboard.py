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

from hyperlab.dashboard.app import _resolve_report_download, _runtime_summary, create_app
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
    assert run["integrity"] == "FAILED_READONLY"
    assert run["status"] == "MANUAL_REVIEW"
    assert run["projection"] is None
    assert run["orders_enabled"] is False


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
