from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import hyperlab.research_data.h1_campaign as campaign_module
from hyperlab.dashboard.app import create_app
from hyperlab.dashboard.h1_dashboard import (
    H1ExpectedIdentity,
    H1SnapshotHeadChangedError,
    h1_fixture_names,
    h1_fixture_snapshot,
    h1_snapshot,
)
from hyperlab.research_data.adapters import HYPERLIQUID_METADATA_VERSION, HYPERLIQUID_PUBLIC_HTTP_URL
from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.envelope import CaptureProvenance, SessionEnvelopeFactory, Venue
from hyperlab.research_data.h1_campaign import prepare_h1_campaign
from hyperlab.research_data.segments import ResearchSegmentWriter

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config" / "research" / "hyperliquid-h1-ghost-v1.json"
FEES = ROOT / "config" / "paper" / "hyperliquid-tier0-fees-2026-08-16.json"
HOLDOUT_SENTINEL = "SEALED_HOLDOUT_PRIVATE_METRIC_9f41a0"


def _write_health(
    campaign_root: Path,
    *,
    terminal: str,
    manifest_sha256: str | None = None,
    raw_root_sha256: str | None = None,
    frames: int | None = None,
    gaps: int = 0,
    queue_high_water: int = 3,
    reconnects: int = 0,
    segments: int | None = None,
    stored_bytes: int | None = None,
) -> None:
    campaign = json.loads((campaign_root / "campaign-manifest.json").read_text(encoding="utf-8"))
    (campaign_root / "state" / "health.json").write_bytes(
        canonical_json_bytes(
            {
                "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
                "campaign_id": campaign["campaign_id"],
                "error": None,
                "frames": (1 if manifest_sha256 else 0) if frames is None else frames,
                "gaps": gaps,
                "manifest_sha256": manifest_sha256,
                "monitoring": "state/health.json",
                "queue_high_water": queue_high_water,
                "raw_root_sha256": raw_root_sha256,
                "reconnects": reconnects,
                "segments": (1 if manifest_sha256 else 0) if segments is None else segments,
                "stored_bytes": (
                    (1 if manifest_sha256 else 0) if stored_bytes is None else stored_bytes
                ),
                "terminal_health": terminal,
            }
        )
    )


def _prepared_campaign(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    starts: datetime | None = None,
) -> Path:
    current = datetime.now(tz=UTC)
    campaign_start = starts or current + timedelta(hours=2)
    monkeypatch.setattr(campaign_module, "_utc_now", lambda: campaign_start - timedelta(hours=1))
    campaign_root = tmp_path / "campaign"
    prepare_h1_campaign(
        campaign_root,
        config_path=CONFIG,
        fee_artifact_path=FEES,
        starts_at_utc=campaign_start,
        fee_reviewed_at_utc=campaign_start - timedelta(hours=1),
    )
    return campaign_root


def _expected_identity(campaign_root: Path) -> H1ExpectedIdentity:
    campaign = json.loads((campaign_root / "campaign-manifest.json").read_text(encoding="utf-8"))
    pin = (campaign_root / "campaign-manifest.sha256").read_text(encoding="ascii").split()[0]
    return H1ExpectedIdentity(
        campaign_id=str(campaign["campaign_id"]),
        campaign_manifest_sha256=pin,
        campaign_root=campaign_root,
        campaign_slug=campaign_root.name,
        collector_source_commit="1" * 40,
        dashboard_source_commit="2" * 40,
    )


def _assert_no_synthetic_error_payload(payload: dict[str, object]) -> None:
    assert payload["fixture"] is False
    assert "SYNTHETIC" not in json.dumps(payload, ensure_ascii=False)
    state = payload["state"]
    assert isinstance(state, dict)
    assert payload["safety"] == {
        "kill_rules": [],
        "stale_feeds": [],
        "integrity": state["integrity"],
    }


def _publish_public_tail(campaign_root: Path) -> tuple[str, str, tuple[str, ...]]:
    campaign = json.loads((campaign_root / "campaign-manifest.json").read_text(encoding="utf-8"))
    factory = SessionEnvelopeFactory(
        venue=Venue.HYPERLIQUID,
        collector_identity="hyperlab-h1-prospective-campaign-v1",
        session_identity="dashboard-test-session",
        source_metadata_version=HYPERLIQUID_METADATA_VERSION,
        provenance=CaptureProvenance(
            str(campaign["campaign_id"]),
            HYPERLIQUID_PUBLIC_HTTP_URL,
            "PUBLIC_HTTP",
        ),
    )
    writer = ResearchSegmentWriter(
        campaign_root / "raw",
        collection_id=str(campaign["campaign_id"]),
        max_segment_bytes=1024 * 1024,
        rotation_seconds=300,
        max_total_bytes=16 * 1024 * 1024,
    )
    writer.append(
        factory.make(
            feed_type="metadata",
            instrument_id="HL:GLOBAL:public",
            market_id=None,
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=int(datetime.now(tz=UTC).timestamp() * 1_000_000_000),
            receive_monotonic_ns=1,
            raw_payload=b'[{"universe":[]}]',
        )
    )
    manifest = writer.close()
    assert manifest is not None
    return (
        manifest.manifest_sha256,
        manifest.root_sha256,
        tuple(item.physical_sha256 for item in manifest.segments),
    )


def _write_final_report(
    campaign_root: Path,
    *,
    manifest_sha256: str,
    raw_root_sha256: str,
    segment_sha256s: tuple[str, ...],
) -> bytes:
    campaign = json.loads((campaign_root / "campaign-manifest.json").read_text(encoding="utf-8"))
    attribution = {
        "adverse_selection": "-1.2",
        "fees": "-0.5",
        "funding": "0",
        "net": "0.3",
        "opportunity_cost": "0",
        "realized_pnl": "0.3",
        "spread": "2.0",
        "unrealized_pnl": "0",
    }
    body: dict[str, object] = {
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "economic_status": "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE",
        "latency_reports": [
            {
                "attribution": attribution,
                "concentration": {
                    "closeout_slippage_p99_bps": "3.1",
                    "top_one_percent_share": "0.11",
                },
                "decisions": [],
                "economic_gates": {"minimum_fills_5000": False, "reconciliation_exact": True},
                "ghost": {
                    "exposure": {
                        "gross_filled_notional": "1200",
                        "positions": {},
                        "unresolved_closeout": {},
                    },
                    "fills": [],
                    "orders": [],
                },
                "latency_ms": 500,
            }
        ],
        "policy_config_sha256": campaign["policy_config_sha256"],
        "raw_manifest_sha256": manifest_sha256,
        "raw_root_sha256": raw_root_sha256,
        "schema_version": 1,
        "segment_sha256s": list(segment_sha256s),
        "synthetic": False,
        "technical_verdict": "HYPERLIQUID_H1_GHOST_V1_READY_FOR_PROSPECTIVE_EVIDENCE",
        "variants": campaign["variants"],
    }
    report_sha256 = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    encoded = canonical_json_bytes({**body, "report_sha256": report_sha256}) + b"\n"
    (campaign_root / "state" / "verified-threshold-report.json").write_bytes(encoded)
    return encoded


def test_h1_dashboard_default_fixture_is_visible_readonly_and_accessible(tmp_path: Path) -> None:
    client = TestClient(create_app(data_dir=tmp_path))

    page = client.get("/")
    snapshot = client.get("/api/h1/snapshot")

    assert page.status_code == 200
    assert "GHOST ONLY" in page.text
    assert "PUBLIC DATA" in page.text
    assert "ORDERS IMPOSSIBLE" in page.text
    assert 'lang="fr"' in page.text
    assert 'class="skip-link"' in page.text
    assert 'aria-live="polite"' in page.text
    assert page.headers["content-security-policy"].startswith("default-src 'none'")
    assert snapshot.status_code == 200
    assert snapshot.json()["fixture"] is True
    assert snapshot.json()["data_classification"].startswith("SYNTHETIC/FIXTURE")
    assert snapshot.json()["mode"] == "readonly"
    assert snapshot.json()["orders_enabled"] is False
    assert snapshot.json()["economic_evidence_status"] == "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE"
    stylesheet = client.get("/assets/h1-dashboard.css")
    assert ":focus-visible" in stylesheet.text
    assert "prefers-reduced-motion" in stylesheet.text
    assert "@media (max-width: 520px)" in stylesheet.text


def test_h1_routes_are_get_head_only_and_always_repeat_safety_boundary(tmp_path: Path) -> None:
    app = create_app(data_dir=tmp_path)
    client = TestClient(app)
    endpoints = (
        "/api/h1/snapshot",
        "/api/h1/identity",
        "/api/h1/collection",
        "/api/h1/markets",
        "/api/h1/strategy",
        "/api/h1/economics",
        "/api/h1/incidents",
        "/api/h1/fixtures/RUNNING_HEALTHY",
    )
    for endpoint in endpoints:
        response = client.get(endpoint)
        assert response.status_code == 200
        assert response.json()["mode"] == "readonly"
        assert response.json()["orders_enabled"] is False
        assert client.head(endpoint).status_code == 200
        for method in ("post", "put", "patch", "delete"):
            assert getattr(client, method)(endpoint).status_code == 405
    h1_routes = [
        route
        for route in app.routes
        if isinstance(getattr(route, "path", None), str) and route.path.startswith("/api/h1/")
    ]
    assert h1_routes
    assert all(route.methods <= {"GET", "HEAD"} for route in h1_routes)


def test_all_required_h1_fixtures_are_explicitly_synthetic_and_demonstrable(tmp_path: Path) -> None:
    client = TestClient(create_app(data_dir=tmp_path))
    assert set(h1_fixture_names()) >= {
        "PREPARED_NOT_STARTED",
        "ARMED",
        "RUNNING_HEALTHY",
        "STALE_RECONNECTING",
        "INTERRUPTED_RECOVERABLE",
        "INTEGRITY_FAILED",
        "COMPLETE_COLLECTION_WINDOW",
        "HOLDOUT_SEALED",
        "HOLDOUT_OPEN",
    }
    for fixture_name in h1_fixture_names():
        payload = client.get(f"/api/h1/fixtures/{fixture_name}").json()
        assert payload["fixture"] is True
        assert payload["data_classification"].startswith("SYNTHETIC/FIXTURE")
        assert payload["orders_enabled"] is False
        assert payload["economic_evidence_status"] == "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE"
    prepared = client.get("/api/h1/fixtures/PREPARED_NOT_STARTED").json()
    assert all(value is None for value in prepared["collection"].values() if value != "SYNTHETIC_TAIL")
    assert all(value is None for value in prepared["strategy"]["decisions"].values())
    stale = client.get("/api/h1/fixtures/STALE_RECONNECTING").json()
    assert stale["state"]["code"] == "RECONNECTING"
    assert stale["state"]["freshness"] == "STALE"
    complete = client.get("/api/h1/fixtures/COMPLETE_COLLECTION_WINDOW").json()
    assert complete["progress"]["holdout"]["access"] == "OPEN"
    sealed = client.get("/api/h1/fixtures/HOLDOUT_SEALED").json()
    assert all(value is None for value in sealed["strategy"]["decisions"].values())
    assert sealed["strategy"]["fills"] is None
    assert sealed["reports"] == []
    assert client.get("/api/h1/fixtures/../../secret").status_code == 404


def test_prepared_campaign_uses_explicit_root_and_authenticates_campaign_pin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(tmp_path, monkeypatch)
    original_names = sorted(path.relative_to(campaign_root) for path in campaign_root.rglob("*"))
    client = TestClient(
        create_app(data_dir=tmp_path / "data", h1_campaign_root=campaign_root, h1_policy_path=CONFIG)
    )

    payload = client.get("/api/h1/snapshot").json()

    assert payload["fixture"] is False
    assert payload["state"]["code"] == "PREPARED_NOT_STARTED"
    assert payload["progress"]["holdout"]["access"] == "SEALED"
    assert payload["identity"]["campaign_manifest_sha256"] == (
        campaign_root / "campaign-manifest.sha256"
    ).read_text(encoding="ascii").split()[0]
    assert sorted(path.relative_to(campaign_root) for path in campaign_root.rglob("*")) == original_names


def test_bound_campaign_requires_exact_external_id_pin_and_slug(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(tmp_path, monkeypatch)
    expected = _expected_identity(campaign_root)
    candidates = (
        H1ExpectedIdentity(
            campaign_id="h1-" + "0" * 24,
            campaign_manifest_sha256=expected.campaign_manifest_sha256,
            campaign_slug=campaign_root.name,
        ),
        H1ExpectedIdentity(
            campaign_id=expected.campaign_id,
            campaign_manifest_sha256="0" * 64,
            campaign_slug=campaign_root.name,
        ),
        H1ExpectedIdentity(
            campaign_id=expected.campaign_id,
            campaign_manifest_sha256=expected.campaign_manifest_sha256,
            campaign_slug="different-campaign-slug",
        ),
    )

    for candidate in candidates:
        response = TestClient(
            create_app(
                data_dir=tmp_path / "data",
                h1_campaign_root=campaign_root,
                h1_policy_path=CONFIG,
                h1_expected_identity=candidate,
            )
        ).get("/api/h1/snapshot")
        assert response.status_code == 503
        payload = response.json()
        assert payload["state"]["code"] == "INTEGRITY_FAILED"
        assert payload["identity"] == {}
        _assert_no_synthetic_error_payload(payload)


def test_bound_campaign_missing_root_and_unbound_readiness_fail_closed(tmp_path: Path) -> None:
    expected = H1ExpectedIdentity(
        campaign_id="h1-" + "1" * 24,
        campaign_manifest_sha256="2" * 64,
        campaign_root=(tmp_path / "missing-campaign").resolve(),
        campaign_slug="missing-campaign",
    )
    bound = TestClient(
        create_app(
            data_dir=tmp_path / "data",
            h1_campaign_root=tmp_path / "missing-campaign",
            h1_policy_path=CONFIG,
            h1_expected_identity=expected,
        )
    )
    missing = bound.get("/api/h1/snapshot")
    assert missing.status_code == 503
    missing_payload = missing.json()
    assert missing_payload["state"]["code"] == "UNREADABLE_FAIL_CLOSED"
    _assert_no_synthetic_error_payload(missing_payload)
    assert bound.get("/health/h1-ready").status_code == 503
    fixture = bound.get("/api/h1/fixtures/RUNNING_HEALTHY")
    assert fixture.status_code == 404
    assert fixture.json()["status"] == "H1_FIXTURES_DISABLED_FOR_BOUND_CAMPAIGN"
    assert 'data-fixtures-enabled="false"' in bound.get("/").text

    unbound = TestClient(create_app(data_dir=tmp_path / "unbound"))
    readiness = unbound.get("/health/h1-ready")
    assert readiness.status_code == 503
    assert readiness.json()["status"] == "H1_BINDING_NOT_CONFIGURED"


def test_bound_dashboard_observes_prepared_to_running_without_restart_or_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(
        tmp_path,
        monkeypatch,
        starts=datetime.now(tz=UTC) - timedelta(seconds=1),
    )
    expected = _expected_identity(campaign_root)
    client = TestClient(
        create_app(
            data_dir=tmp_path / "data",
            h1_campaign_root=campaign_root,
            h1_policy_path=CONFIG,
            h1_expected_identity=expected,
        )
    )
    assert client.get("/api/h1/snapshot").json()["state"]["code"] == "PREPARED_NOT_STARTED"

    _write_health(campaign_root, terminal="RUNNING")
    before = {
        path.relative_to(campaign_root): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in campaign_root.rglob("*")
        if path.is_file()
    }
    running = client.get("/api/h1/snapshot")
    readiness = client.get("/health/h1-ready")
    after = {
        path.relative_to(campaign_root): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in campaign_root.rglob("*")
        if path.is_file()
    }

    assert running.status_code == 200
    assert running.json()["state"]["code"] == "RUNNING_HEALTHY"
    assert running.json()["identity"]["collector_source_commit"] == "1" * 40
    assert running.json()["identity"]["dashboard_source_commit"] == "2" * 40
    assert readiness.status_code == 200
    assert readiness.json()["status"] == "H1_BOUND_READONLY_READY"
    assert before == after


def test_running_health_preserves_initial_zero_storage_before_raw_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(
        tmp_path,
        monkeypatch,
        starts=datetime.now(tz=UTC) - timedelta(seconds=21),
    )
    _write_health(
        campaign_root,
        terminal="RUNNING",
        manifest_sha256=None,
        raw_root_sha256=None,
        frames=834,
        segments=0,
        stored_bytes=0,
    )

    payload, status = h1_snapshot(
        campaign_root,
        policy_path=CONFIG,
        expected_identity=_expected_identity(campaign_root),
    )

    assert status == 200
    assert payload["state"]["code"] == "RUNNING_HEALTHY"
    assert payload["identity"]["raw_manifest_sha256"] is None
    assert payload["identity"]["raw_root_sha256"] is None
    assert payload["collection"]["frames"] == 834
    assert payload["collection"]["segments"] == 0
    assert payload["collection"]["stored_bytes"] == 0
    assert payload["incidents"] == []


def test_interrupted_recoverable_is_honest_readonly_and_keeps_holdout_sealed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(
        tmp_path,
        monkeypatch,
        starts=datetime.now(tz=UTC) - timedelta(hours=1),
    )
    _write_health(
        campaign_root,
        terminal="INTERRUPTED_RECOVERABLE",
        manifest_sha256=None,
        raw_root_sha256=None,
        frames=3_999_005,
        gaps=0,
        queue_high_water=9,
        reconnects=19,
        segments=358,
        stored_bytes=1_092_828_859,
    )

    payload, status = h1_snapshot(
        campaign_root,
        policy_path=CONFIG,
        expected_identity=_expected_identity(campaign_root),
    )

    assert status == 200
    assert payload["state"]["code"] == "INTERRUPTED_RECOVERABLE"
    assert payload["state"]["retryable"] is True
    assert payload["progress"]["holdout"]["access"] == "SEALED"
    assert payload["identity"]["raw_manifest_sha256"] is None
    assert payload["identity"]["raw_root_sha256"] is None
    assert payload["collection"] == {
        "connection_generation": 20,
        "duplicates": None,
        "duplicates_scope": "NON DISPONIBLE",
        "frames": 3_999_005,
        "gaps": 0,
        "queue_high_water": 9,
        "reconnects": 19,
        "segments": 358,
        "stored_bytes": 1_092_828_859,
    }


def test_every_http_route_is_get_head_only(tmp_path: Path) -> None:
    dashboard = create_app(data_dir=tmp_path)
    assert all(route.methods is None or route.methods <= {"GET", "HEAD"} for route in dashboard.routes)


def test_same_head_retry_is_bounded_and_returns_retryable_409(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaign"
    campaign_root.mkdir()
    attempts = 0

    def always_changes(_root: Path, _policy, _now: datetime):
        nonlocal attempts
        attempts += 1
        raise H1SnapshotHeadChangedError("HEAD_CHANGED_RETRY")

    payload, status = h1_snapshot(
        campaign_root,
        policy_path=None,
        snapshot_once=always_changes,
    )

    assert attempts == 2
    assert status == 409
    assert payload["state"]["code"] == "HEAD_CHANGED_RETRY"
    assert payload["state"]["retryable"] is True
    assert payload["orders_enabled"] is False
    _assert_no_synthetic_error_payload(payload)


def test_unreadable_real_campaign_error_never_inherits_synthetic_fixture_values(
    tmp_path: Path,
) -> None:
    campaign_root = tmp_path / "unreadable-campaign"
    campaign_root.mkdir()

    payload, status = h1_snapshot(campaign_root, policy_path=None)

    assert status == 503
    assert payload["state"]["code"] == "UNREADABLE_FAIL_CLOSED"
    _assert_no_synthetic_error_payload(payload)


def test_unknown_real_campaign_terminal_health_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(tmp_path, monkeypatch)
    _write_health(campaign_root, terminal="UNKNOWN_FUTURE_STATE")

    payload, status = h1_snapshot(campaign_root, policy_path=CONFIG)

    assert status == 503
    assert payload["state"]["code"] == "INTEGRITY_FAILED"
    _assert_no_synthetic_error_payload(payload)


def test_same_head_retry_can_recover_on_the_second_bounded_attempt(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaign"
    campaign_root.mkdir()
    attempts = 0

    def changes_once(_root: Path, _policy, now: datetime):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise H1SnapshotHeadChangedError("HEAD_CHANGED_RETRY")
        return h1_fixture_snapshot("RUNNING_HEALTHY", now=now)

    payload, status = h1_snapshot(
        campaign_root,
        policy_path=None,
        snapshot_once=changes_once,
    )

    assert attempts == 2
    assert status == 200
    assert payload["state"]["code"] == "RUNNING_HEALTHY"


def test_oversized_special_or_symlinked_campaign_files_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = tmp_path / "campaign"
    campaign_root.mkdir()
    manifest = campaign_root / "campaign-manifest.json"
    manifest.write_bytes(b"{" + b" " * (2 * 1024 * 1024) + b"}")
    (campaign_root / "campaign-manifest.sha256").write_text(
        f"{hashlib.sha256(manifest.read_bytes()).hexdigest()}  campaign-manifest.json\n",
        encoding="ascii",
    )
    client = TestClient(create_app(data_dir=tmp_path / "data", h1_campaign_root=campaign_root))
    oversized = client.get("/api/h1/snapshot")
    assert oversized.status_code == 503
    assert oversized.json()["state"]["code"] == "INTEGRITY_FAILED"

    manifest.write_text("{}", encoding="utf-8")
    path_type = type(manifest)
    original_is_symlink = path_type.is_symlink
    monkeypatch.setattr(
        path_type,
        "is_symlink",
        lambda self: self == manifest or original_is_symlink(self),
    )
    linked = client.get("/api/h1/snapshot")
    assert linked.status_code == 503
    assert linked.json()["orders_enabled"] is False


def test_configured_policy_is_required_to_be_regular_bounded_and_valid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(tmp_path, monkeypatch)
    invalid_policy = tmp_path / "invalid-policy.json"
    invalid_policy.write_text("{}", encoding="utf-8")
    client = TestClient(
        create_app(
            data_dir=tmp_path / "data",
            h1_campaign_root=campaign_root,
            h1_policy_path=invalid_policy,
        )
    )

    response = client.get("/api/h1/snapshot")

    assert response.status_code == 503
    assert response.json()["state"]["code"] == "INTEGRITY_FAILED"
    assert response.json()["identity"] == {}


def test_sealed_holdout_never_leaks_report_through_html_json_download_or_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(tmp_path, monkeypatch)
    (campaign_root / "state" / "verified-threshold-report.json").write_text(
        json.dumps({"holdout_pnl": HOLDOUT_SENTINEL}),
        encoding="utf-8",
    )
    client = TestClient(
        create_app(data_dir=tmp_path / "data", h1_campaign_root=campaign_root, h1_policy_path=CONFIG)
    )

    responses = [
        client.get("/"),
        client.get("/api/h1/snapshot"),
        client.get("/api/h1/economics"),
        client.get("/api/h1/reports/verified-threshold"),
        client.get("/api/h1/reports/%2e%2e%2fcampaign-manifest.json"),
    ]

    assert responses[3].status_code == 404
    assert responses[4].status_code == 404
    for response in responses:
        assert HOLDOUT_SENTINEL not in response.text
    snapshot = responses[1].json()
    assert snapshot["progress"]["holdout"]["access"] == "SEALED"
    assert snapshot["reports"] == []
    assert all(metric["value"] is None for metric in snapshot["economics"]["metrics"].values())


def test_early_terminal_health_cannot_open_holdout_or_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(tmp_path, monkeypatch)
    manifest_sha, raw_root_sha, _segments = _publish_public_tail(campaign_root)
    (campaign_root / "state" / "verified-threshold-report.json").write_text(
        HOLDOUT_SENTINEL,
        encoding="utf-8",
    )
    _write_health(
        campaign_root,
        terminal="COMPLETE_COLLECTION_WINDOW",
        manifest_sha256=manifest_sha,
        raw_root_sha256=raw_root_sha,
    )
    client = TestClient(
        create_app(data_dir=tmp_path / "data", h1_campaign_root=campaign_root, h1_policy_path=CONFIG)
    )

    snapshot = client.get("/api/h1/snapshot")
    download = client.get("/api/h1/reports/verified-threshold")

    assert snapshot.status_code == 200
    assert snapshot.json()["progress"]["holdout"]["access"] == "SEALED"
    assert snapshot.json()["reports"] == []
    assert HOLDOUT_SENTINEL not in snapshot.text
    assert download.status_code == 404
    assert HOLDOUT_SENTINEL not in download.text


def test_completed_campaign_can_download_only_an_authenticated_allowlisted_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(
        tmp_path,
        monkeypatch,
        starts=datetime.now(tz=UTC) - timedelta(days=14),
    )
    manifest_sha, raw_root_sha, segments = _publish_public_tail(campaign_root)
    report = _write_final_report(
        campaign_root,
        manifest_sha256=manifest_sha,
        raw_root_sha256=raw_root_sha,
        segment_sha256s=segments,
    )
    _write_health(
        campaign_root,
        terminal="COMPLETE_COLLECTION_WINDOW",
        manifest_sha256=manifest_sha,
        raw_root_sha256=raw_root_sha,
    )
    client = TestClient(
        create_app(data_dir=tmp_path / "data", h1_campaign_root=campaign_root, h1_policy_path=CONFIG)
    )

    snapshot = client.get("/api/h1/snapshot")
    download = client.get("/api/h1/reports/verified-threshold")

    assert snapshot.status_code == 200
    assert snapshot.json()["fixture"] is False
    assert snapshot.json()["progress"]["holdout"]["access"] == "OPEN"
    assert snapshot.json()["economics"]["certifiable"] is True
    assert snapshot.json()["economic_evidence_status"] == "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE"
    assert download.status_code == 200
    assert download.content == report
    assert download.headers["content-disposition"].startswith("attachment;")
    assert download.headers["x-content-sha256"] == hashlib.sha256(report).hexdigest()
    assert client.get("/api/h1/reports/arbitrary-file").status_code == 404


def test_invalid_open_report_fails_closed_without_exposing_report_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_root = _prepared_campaign(
        tmp_path,
        monkeypatch,
        starts=datetime.now(tz=UTC) - timedelta(days=14),
    )
    manifest_sha, raw_root_sha, _segments = _publish_public_tail(campaign_root)
    (campaign_root / "state" / "verified-threshold-report.json").write_text(
        HOLDOUT_SENTINEL,
        encoding="utf-8",
    )
    _write_health(
        campaign_root,
        terminal="COMPLETE_COLLECTION_WINDOW",
        manifest_sha256=manifest_sha,
        raw_root_sha256=raw_root_sha,
    )
    client = TestClient(
        create_app(data_dir=tmp_path / "data", h1_campaign_root=campaign_root, h1_policy_path=CONFIG)
    )

    response = client.get("/api/h1/snapshot")

    assert response.status_code == 503
    assert response.json()["state"]["code"] == "INTEGRITY_FAILED"
    assert HOLDOUT_SENTINEL not in response.text
    assert client.get("/api/h1/reports/verified-threshold").status_code == 404


def test_dashboard_source_contains_no_control_surface_or_exchange_sdk_import() -> None:
    source = (ROOT / "src" / "hyperlab" / "dashboard" / "h1_dashboard.py").read_text(
        encoding="utf-8"
    )
    page = (ROOT / "src" / "hyperlab" / "dashboard" / "h1_page.py").read_text(
        encoding="utf-8"
    )
    assert "hyperliquid.exchange" not in source
    assert "subprocess" not in source
    assert "os.system" not in source
    assert "WebSocket" not in source
    assert "<form" not in page
    assert "type=\"submit\"" not in page
    assert "fetch(endpoint, { headers" in page
    assert "method:" not in page
