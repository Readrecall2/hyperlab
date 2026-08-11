from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from hyperlab.dashboard.app import create_app


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

