from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from hyperlab.dashboard.app import _runtime_summary, create_app
from hyperlab.paper import (
    PaperEngine,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    PaperStore,
)


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

    assert response.status_code == 200
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
