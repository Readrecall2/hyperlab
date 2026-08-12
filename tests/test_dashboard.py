from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from hyperlab.dashboard.app import _runtime_summary, create_app


def test_dashboard_is_explicitly_read_only(tmp_path: Path) -> None:
    client = TestClient(create_app(data_dir=tmp_path))
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json() == {"ok": True, "mode": "readonly", "orders_enabled": False}
    page = client.get("/")
    assert page.status_code == 200
    assert "ORDRES IMPOSSIBLES" in page.text


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
