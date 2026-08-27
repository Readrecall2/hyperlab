from __future__ import annotations

import http.client
import json
import threading
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ops.prediction_markets_launch_v1 import cockpit, runner

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_PACK = (
    ROOT
    / "ops"
    / "prediction_markets_candidate_v1"
    / "prediction-markets-v1-20260901t000000z-aa60c0ff"
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(runner.canonical_json_bytes(value) + b"\n")


def _campaign(tmp_path: Path) -> Path:
    campaign = tmp_path / "campaign"
    campaign.mkdir(parents=True)
    for name in ("campaign-manifest.json", "campaign-manifest.sha256"):
        (campaign / name).write_bytes((CANDIDATE_PACK / name).read_bytes())
    _write_json(
        campaign / "state" / "preflight-report.json",
        {
            "boundary": cockpit.BOUNDARY,
            "eligible_venues": ["polymarket", "kalshi"],
            "installation_admissible": True,
            "network": {
                "polymarket": {
                    "dns": {"gamma-api.polymarket.com": ["192.0.2.1"]},
                    "errors": [],
                    "verdict": "NETWORK_PREFLIGHT_GREEN",
                },
                "kalshi": {
                    "dns": {"external-api.kalshi.com": ["192.0.2.2"]},
                    "errors": [],
                    "verdict": "NETWORK_PREFLIGHT_GREEN",
                },
            },
        },
    )
    _write_json(
        campaign / "state" / "activation-receipt.json",
        {
            "boundary": cockpit.BOUNDARY,
            "source_commit": "3f188b9c28c9fec406b904a9e3307b43f54243e8",
        },
    )
    return campaign


def _ledger_entry(venue: str, ordinal: int, *, frames: int, terminal: str = "COMPLETE") -> dict[str, object]:
    return {
        "boundary": cockpit.BOUNDARY,
        "bytes": frames * 100,
        "duplicates": 1,
        "error": None,
        "frames": frames,
        "gaps": 2,
        "manifest_sha256": str(ordinal + 1) * 64,
        "ordinal": ordinal,
        "reconnects": 3,
        "recorded_at_utc": "2026-09-01T00:03:00.000000Z",
        "root_sha256": str(ordinal + 3) * 64,
        "scheduled_start_utc": "2026-09-01T00:00:00.000000Z",
        "segments": 4,
        "terminal_health": terminal,
        "venue": venue,
    }


def _state(campaign: Path, venue: str, lifecycle: str = "WAITING_NEXT_SLOT") -> None:
    _write_json(
        campaign / venue.lower() / "state.json",
        {
            "boundary": cockpit.BOUNDARY,
            "capacity": {
                "admitted": True,
                "available_bytes": 200_000_000_000,
                "required_free_bytes": 194_347_270_144,
            },
            "lifecycle": lifecycle,
            "updated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "venue": venue,
        },
    )


def test_fixture_matrix_is_complete_holdout_safe_and_marks_missing_metrics() -> None:
    assert cockpit.FIXTURES == (
        "PREPARED",
        "BOTH_RUNNING",
        "POLYMARKET_UNAVAILABLE_KALSHI_RUNNING",
        "KALSHI_UNAVAILABLE_POLYMARKET_RUNNING",
        "BOTH_UNAVAILABLE",
        "STALE_RECONNECTING",
        "INTEGRITY_FAILED",
        "INTERRUPTED_RECOVERABLE",
        "COMPLETE_WINDOW",
        "HOLDOUT_SEALED",
    )
    for name in cockpit.FIXTURES:
        value = cockpit.fixture_snapshot(name)
        assert value["fixture"] is True
        assert value["mode"] == "readonly"
        assert value["orders_enabled"] is False
        assert value["holdout"] == {
            "access": "SEALED",
            "metrics_exposed": False,
            "status": "HOLDOUT_SEALED",
        }
        assert value["economic_evidence_status"] == cockpit.ECONOMIC_STATUS
    prepared = cockpit.fixture_snapshot("PREPARED")
    assert prepared["venues"]["polymarket"]["collection"]["frames"] == {
        "available": False,
        "provenance": cockpit.FIXTURE_LABEL,
        "value": None,
    }


def test_real_snapshot_authenticates_identity_ledgers_and_exact_metrics(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    runner.append_ledger(
        campaign / "polymarket" / "ledger.jsonl",
        _ledger_entry("POLYMARKET", 0, frames=10),
    )
    runner.append_ledger(
        campaign / "kalshi" / "ledger.jsonl",
        _ledger_entry("KALSHI", 0, frames=20),
    )
    _state(campaign, "POLYMARKET")
    _state(campaign, "KALSHI")
    value = cockpit.campaign_snapshot(campaign)
    assert value["fixture"] is False
    assert value["state"] == {
        "code": "BOTH_RUNNING",
        "integrity": "AUTHENTICATED",
        "severity": "ok",
    }
    assert value["venues"]["polymarket"]["collection"]["frames"]["value"] == 10
    assert value["venues"]["kalshi"]["collection"]["frames"]["value"] == 20
    assert value["identity"]["source_commit"] == "3f188b9c28c9fec406b904a9e3307b43f54243e8"
    assert value["downloads"] == [
        {"id": "campaign-manifest", "path": "campaign-manifest.json"},
        {"id": "campaign-manifest-pin", "path": "campaign-manifest.sha256"},
        {"id": "preflight-report", "path": "state/preflight-report.json"},
        {"id": "activation-receipt", "path": "state/activation-receipt.json"},
        {"id": "polymarket-ledger", "path": "polymarket/ledger.jsonl"},
        {"id": "kalshi-ledger", "path": "kalshi/ledger.jsonl"},
    ]


def test_real_snapshot_never_turns_absent_collection_metrics_into_zero(tmp_path: Path) -> None:
    value = cockpit.campaign_snapshot(_campaign(tmp_path))
    assert value["state"] == {
        "code": "SERVICE_STATE_UNAVAILABLE",
        "integrity": "AUTHENTICATED",
        "severity": "critical",
    }
    for venue in ("polymarket", "kalshi"):
        for metric in value["venues"][venue]["collection"].values():
            assert metric["available"] is False
            assert metric["value"] is None


def test_runtime_public_unavailability_is_isolated_and_visible(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    runner.append_ledger(
        campaign / "polymarket" / "ledger.jsonl",
        _ledger_entry(
            "POLYMARKET",
            0,
            frames=1,
            terminal="PUBLIC_SOURCE_UNAVAILABLE_RECOVERED",
        ),
    )
    runner.append_ledger(
        campaign / "kalshi" / "ledger.jsonl",
        _ledger_entry("KALSHI", 0, frames=1),
    )
    _state(campaign, "POLYMARKET")
    _state(campaign, "KALSHI")
    value = cockpit.campaign_snapshot(campaign)
    assert value["state"]["code"] == "POLYMARKET_UNAVAILABLE_KALSHI_RUNNING"
    assert (
        value["venues"]["polymarket"]["connectivity"]["verdict"]
        == "PUBLIC_SOURCE_UNAVAILABLE_RUNTIME"
    )
    assert value["venues"]["kalshi"]["connectivity"]["verdict"] == "NETWORK_PREFLIGHT_GREEN"


def test_integrity_tamper_symlink_and_oversize_fail_closed(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    manifest = campaign / "campaign-manifest.json"
    manifest.write_bytes(manifest.read_bytes() + b" ")
    with pytest.raises(cockpit.CockpitIntegrityError, match="pin diverged"):
        cockpit.campaign_snapshot(campaign)

    safe = _campaign(tmp_path / "second")
    target = safe / "outside.json"
    target.write_text("{}", encoding="utf-8")
    link = safe / "state" / "activation-receipt.json"
    link.unlink()
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    with pytest.raises(cockpit.CockpitError):
        cockpit.campaign_snapshot(safe)


def test_oversize_ledger_is_refused_before_read(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    ledger = campaign / "polymarket" / "ledger.jsonl"
    ledger.parent.mkdir()
    with ledger.open("wb") as handle:
        handle.truncate(cockpit._MAX_LEDGER_BYTES + 1)
    with pytest.raises(cockpit.CockpitIntegrityError, match="oversized"):
        cockpit.campaign_snapshot(campaign)


def test_state_capacity_uses_the_same_coherent_bounded_read(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    _state(campaign, "POLYMARKET")
    _state(campaign, "KALSHI")
    original = cockpit._optional_read
    state_reads: dict[str, int] = {"polymarket": 0, "kalshi": 0}

    def counted(
        root: Path,
        relative: cockpit.PurePosixPath,
        *,
        maximum_bytes: int,
    ) -> cockpit._Read | None:
        for venue in state_reads:
            if relative.as_posix() == f"{venue}/state.json":
                state_reads[venue] += 1
        return original(root, relative, maximum_bytes=maximum_bytes)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(cockpit, "_optional_read", counted)
    try:
        value = cockpit.campaign_snapshot(campaign)
    finally:
        monkeypatch.undo()
    assert value["capacity"]["admitted"] is True
    assert state_reads == {"polymarket": 1, "kalshi": 1}


def test_downloads_use_fixed_allowlist_and_refuse_traversal(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    content_type, payload = cockpit.report_download(campaign, "campaign-manifest")
    assert content_type == "application/json"
    assert payload == (campaign / "campaign-manifest.json").read_bytes()
    with pytest.raises(cockpit.CockpitIntegrityError, match="allowlisted"):
        cockpit.report_download(campaign, "../campaign-manifest")
    assert not any("holdout" in value.as_posix().lower() for value in cockpit.DOWNLOAD_ALLOWLIST.values())


def test_http_surface_is_get_head_only_loopback_and_fail_closed(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    server = cockpit._Server(("127.0.0.1", 0), campaign, True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    try:
        connection = http.client.HTTPConnection(host, port, timeout=5)
        connection.request("GET", "/api/snapshot")
        response = connection.getresponse()
        payload = json.loads(response.read())
        assert response.status == 200
        assert payload["mode"] == "readonly" and payload["orders_enabled"] is False
        connection.request("GET", "/health/ready")
        not_ready = connection.getresponse()
        assert not_ready.status == 503
        assert json.loads(not_ready.read())["status"] == "not-ready"
        _state(campaign, "POLYMARKET")
        _state(campaign, "KALSHI")
        connection.request("GET", "/health/ready")
        ready = connection.getresponse()
        assert ready.status == 200
        assert json.loads(ready.read())["status"] == "ready"
        connection.request("HEAD", "/")
        head = connection.getresponse()
        assert head.status == 200 and head.read() == b""
        connection.request("POST", "/api/snapshot", body=b"{}")
        post = connection.getresponse()
        assert post.status == 405
        assert json.loads(post.read())["orders_enabled"] is False
        connection.request("OPTIONS", "/api/snapshot")
        options = connection.getresponse()
        assert options.status == 405
        assert json.loads(options.read())["orders_enabled"] is False
        connection.request("GET", "/api/download/%2e%2e%2fcampaign-manifest")
        traversal = connection.getresponse()
        assert traversal.status == 400
        traversal.read()
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_page_is_self_contained_premium_and_has_no_command_surface() -> None:
    html = cockpit._HTML
    css = cockpit._CSS
    js = cockpit._JS
    assert "Prediction Markets <span>Observatory" in html
    assert "READ ONLY" in html and "ORDERS ENABLED <b>FALSE" in html
    assert "backdrop-filter" in css and "aurora" in css
    assert "NON DISPONIBLE" in js
    lowered = (html + css + js).lower()
    for forbidden in ("place order", "cancel order", "private key", "wallet", "signer"):
        assert forbidden not in lowered
