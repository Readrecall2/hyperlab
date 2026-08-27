from __future__ import annotations

import http.client
import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ops.prediction_markets_launch_v1 import cockpit, preflight, runner

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_PACK = (
    ROOT
    / "ops"
    / "prediction_markets_candidate_v1"
    / "prediction-markets-v1-20260901t000000z-aa60c0ff"
)
CAMPAIGN_MANIFEST = json.loads(
    (CANDIDATE_PACK / "campaign-manifest.json").read_text(encoding="utf-8")
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(runner.canonical_json_bytes(value) + b"\n")


def _campaign(tmp_path: Path) -> Path:
    campaign = tmp_path / "campaign"
    campaign.mkdir(parents=True)
    for name in ("campaign-manifest.json", "campaign-manifest.sha256"):
        (campaign / name).write_bytes((CANDIDATE_PACK / name).read_bytes())
    preflight_value = {
        "boundary": cockpit.BOUNDARY,
        "eligible_venues": ["polymarket", "kalshi"],
        "errors": [],
        "host_admitted": True,
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
        "schema_version": 1,
        "terminal_signal": "PREDICTION_HOST_PREFLIGHT_GREEN",
    }
    preflight_raw = runner.canonical_json_bytes(preflight_value) + b"\n"
    preflight_path = campaign / "state" / "preflight-report.json"
    preflight_path.parent.mkdir(parents=True, exist_ok=True)
    preflight_path.write_bytes(preflight_raw)
    activation_body = {
        "boundary": cockpit.BOUNDARY,
        "campaign_id": CAMPAIGN_MANIFEST["campaign_id"],
        "campaign_manifest_sha256": CAMPAIGN_MANIFEST["manifest_sha256"],
        "campaign_root": str(campaign),
        "dashboard_port": 18081,
        "economic_evidence_status": cockpit.ECONOMIC_STATUS,
        "eligible_venues": ["polymarket", "kalshi"],
        "h1_actions": "NONE",
        "preflight_report_sha256": runner.sha256_bytes(preflight_raw),
        "quick_start": True,
        "recorded_at_utc": "2026-09-01T00:00:00.000000Z",
        "schema_version": 1,
        "source_commit": "3f188b9c28c9fec406b904a9e3307b43f54243e8",
        "starts_at_utc": CAMPAIGN_MANIFEST["starts_at_utc"],
    }
    _write_json(
        campaign / "state" / "activation-receipt.json",
        {
            **activation_body,
            "receipt_sha256": runner.sha256_bytes(
                runner.canonical_json_bytes(activation_body)
            ),
        },
    )
    return campaign


def _ledger_entry(venue: str, ordinal: int, *, frames: int, terminal: str = "COMPLETE") -> dict[str, object]:
    venue_name = venue.lower()
    starts_at = datetime.fromisoformat(
        str(CAMPAIGN_MANIFEST["starts_at_utc"]).replace("Z", "+00:00")
    )
    policy = CAMPAIGN_MANIFEST["prospective_shard_policy"]
    assert isinstance(policy, dict)
    scheduled_start = starts_at + timedelta(
        seconds=ordinal * int(policy["cadence_seconds"])
    )
    scheduled_text = scheduled_start.isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    collection_plans = CAMPAIGN_MANIFEST["collection_plans"]
    assert isinstance(collection_plans, dict)
    plan = collection_plans[venue_name]
    assert isinstance(plan, dict)
    identity = {
        "campaign_manifest_sha256": CAMPAIGN_MANIFEST["manifest_sha256"],
        "ordinal": ordinal,
        "scheduled_start_utc": scheduled_text,
        "venue": venue_name,
    }
    identity_sha256 = runner.sha256_bytes(runner.canonical_json_bytes(identity))
    return {
        "boundary": cockpit.BOUNDARY,
        "bytes": frames * 100,
        "campaign_manifest_sha256": CAMPAIGN_MANIFEST["manifest_sha256"],
        "candidate_config_sha256": CAMPAIGN_MANIFEST["candidate_config_sha256"],
        "collection_id": (
            f"{plan['collection_id']}-shard-{ordinal:04d}-{identity_sha256[:16]}"
        ),
        "economic_eligible": True,
        "duplicates": 1,
        "error": (
            "synthetic recovered public source interruption"
            if terminal == "PUBLIC_SOURCE_UNAVAILABLE_RECOVERED"
            else None
        ),
        "frames": frames,
        "gaps": 2,
        "manifest_sha256": str(ordinal + 1) * 64,
        "official_contract_sha256": CAMPAIGN_MANIFEST["contracts"][venue_name],
        "ordinal": ordinal,
        "probe_binding_sha256": str(ordinal + 2) * 64,
        "receipt_classification": "AUTHENTICATED_COLLECTION_ADMISSIBLE_FOR_DERIVATION",
        "reconnects": 3,
        "recorded_at_utc": "2026-09-01T00:03:00.000000Z",
        "root_sha256": str(ordinal + 3) * 64,
        "scheduled_start_utc": scheduled_text,
        "segments": 4,
        "source_usable": True,
        "terminal_health": terminal,
        "terminal_result_sha256": str(ordinal + 5) * 64,
        "venue": venue_name,
    }


def _invalid_entry(venue: str, ordinal: int) -> dict[str, object]:
    return {
        **_ledger_entry(venue, ordinal, frames=1),
        "economic_eligible": False,
        "error": "SYNTHETIC/FIXTURE public source payload invalid",
        "receipt_classification": (
            "CAMPAIGN_BOUND_EXPLICIT_GAP_EXCLUDED_FROM_ECONOMICS"
        ),
        "source_usable": False,
        "terminal_health": "PUBLIC_SOURCE_INVALID",
    }


def _state(campaign: Path, venue: str, lifecycle: str = "WAITING_NEXT_SLOT") -> None:
    venue_name = venue.lower()
    ledger_path = campaign / venue_name / "ledger.jsonl"
    rows = runner.read_ledger(ledger_path)
    invalid_rows = [
        row for row in rows if row.get("terminal_health") == "PUBLIC_SOURCE_INVALID"
    ]
    latest_invalid = None if not invalid_rows else invalid_rows[-1]
    available = 190_000_000_000 if lifecycle == "CAPACITY_REFUSED" else 200_000_000_000
    _write_json(
        campaign / venue_name / "state.json",
        {
            "active_ordinal": None,
            "boundary": cockpit.BOUNDARY,
            "campaign_id": CAMPAIGN_MANIFEST["campaign_id"],
            "capacity": {
                "admitted": lifecycle != "CAPACITY_REFUSED",
                "available_bytes": available,
                "h1_reserved_bytes": 144 * 1024**3,
                "prediction_remaining_bytes": 21 * 1024**3,
                "required_free_bytes": 194_347_270_144,
                "safety_margin_bytes": 16 * 1024**3,
            },
            "data_quality": (
                None
                if latest_invalid is None
                else {
                    "alert": True,
                    "count": len(invalid_rows),
                    "error": latest_invalid["error"],
                    "latest_ordinal": latest_invalid["ordinal"],
                    "source_usable": False,
                    "terminal_health": "PUBLIC_SOURCE_INVALID",
                    "terminal_result_sha256": latest_invalid["terminal_result_sha256"],
                }
            ),
            "economic_evidence_status": cockpit.ECONOMIC_STATUS,
            "error": (
                "SYNTHETIC/FIXTURE capacity reservation refused"
                if lifecycle == "CAPACITY_REFUSED"
                else None
            ),
            "expected_slots": CAMPAIGN_MANIFEST["prospective_shard_policy"][
                "expected_shards_per_venue"
            ],
            "holdout": {"access": "SEALED", "metrics_exposed": False},
            "last_terminal": None if not rows else rows[-1]["terminal_health"],
            "lifecycle": lifecycle,
            "recorded_slots": len(rows),
            "schema_version": 1,
            "updated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "venue": venue_name,
        },
    )


def test_fixture_matrix_is_complete_holdout_safe_and_marks_missing_metrics() -> None:
    assert cockpit.FIXTURES == (
        "PREPARED",
        "BOTH_RUNNING",
        "POLYMARKET_UNAVAILABLE_KALSHI_RUNNING",
        "KALSHI_UNAVAILABLE_POLYMARKET_RUNNING",
        "BOTH_UNAVAILABLE",
        "POLYMARKET_SOURCE_INVALID_KALSHI_RUNNING",
        "KALSHI_SOURCE_INVALID_POLYMARKET_RUNNING",
        "BOTH_SOURCE_INVALID",
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
    invalid = cockpit.fixture_snapshot("POLYMARKET_SOURCE_INVALID_KALSHI_RUNNING")
    assert invalid["state"] == {
        "code": "POLYMARKET_SOURCE_INVALID_KALSHI_RUNNING",
        "integrity": "SYNTHETIC_AUTHENTIC",
        "severity": "warning",
    }
    assert invalid["venues"]["polymarket"]["collection"]["frames"]["value"] is None
    assert invalid["venues"]["polymarket"]["collection"]["slots_recorded"]["value"] == 1
    assert len(invalid["venues"]["polymarket"]["last_manifest_sha256"]) == 64


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


def test_authenticated_recovery_admission_replaces_only_the_initial_venue_verdict(
    tmp_path: Path,
) -> None:
    campaign = _campaign(tmp_path)
    initial_path = campaign / "state" / "preflight-report.json"
    initial = json.loads(initial_path.read_text(encoding="utf-8"))
    initial["eligible_venues"] = ["kalshi"]
    initial["network"]["polymarket"] = {
        "dns": None,
        "errors": ["SYNTHETIC/FIXTURE initial unavailability"],
        "verdict": "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT",
    }
    _write_json(initial_path, initial)
    initial_raw = runner.canonical_json_bytes(initial) + b"\n"
    activation_path = campaign / "state" / "activation-receipt.json"
    activation = json.loads(activation_path.read_bytes())
    activation["eligible_venues"] = ["kalshi"]
    activation["preflight_report_sha256"] = runner.sha256_bytes(initial_raw)
    activation_body = {
        key: value for key, value in activation.items() if key != "receipt_sha256"
    }
    activation["receipt_sha256"] = runner.sha256_bytes(
        runner.canonical_json_bytes(activation_body)
    )
    _write_json(activation_path, activation)
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    handoff = {
        "boundary": cockpit.BOUNDARY,
        "campaign_root": str(campaign),
        "schema_version": 1,
        "source_commit": "3f188b9c28c9fec406b904a9e3307b43f54243e8",
        "source_root": "/synthetic/fixture/source",
    }
    handoff_raw = preflight.canonical_json_bytes(handoff) + b"\n"
    handoff_path = incoming / "handoff.json"
    handoff_path.write_bytes(handoff_raw)
    (incoming / "handoff.sha256").write_text(
        f"{preflight.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    recovery_network = {
        "dns": {"gamma-api.polymarket.com": ["192.0.2.10"]},
        "errors": [],
        "venue": "polymarket",
        "verdict": "NETWORK_PREFLIGHT_GREEN",
    }
    recovery_path = incoming / "recovery-network-polymarket.json"
    recovery_path.write_bytes(preflight.canonical_json_bytes(recovery_network) + b"\n")
    preflight.recovery_network_admission(
        handoff_path,
        recovery_path,
        venue="polymarket",
        output_path=campaign / "state" / "recovery-admission-polymarket.json",
    )
    for venue in ("POLYMARKET", "KALSHI"):
        runner.append_ledger(
            campaign / venue.lower() / "ledger.jsonl",
            _ledger_entry(venue, 0, frames=5),
        )
        _state(campaign, venue)

    value = cockpit.campaign_snapshot(campaign)
    assert value["state"]["code"] == "BOTH_RUNNING"
    assert (
        value["venues"]["polymarket"]["connectivity"]["verdict"]
        == "NETWORK_PREFLIGHT_GREEN"
    )
    assert value["venues"]["kalshi"]["connectivity"]["verdict"] == (
        "NETWORK_PREFLIGHT_GREEN"
    )


def test_runtime_public_source_invalid_is_counted_but_never_usable(
    tmp_path: Path,
) -> None:
    campaign = _campaign(tmp_path)
    runner.append_ledger(
        campaign / "polymarket" / "ledger.jsonl",
        _invalid_entry("POLYMARKET", 0),
    )
    runner.append_ledger(
        campaign / "kalshi" / "ledger.jsonl",
        _ledger_entry("KALSHI", 0, frames=20),
    )
    _state(campaign, "POLYMARKET")
    _state(campaign, "KALSHI")
    value = cockpit.campaign_snapshot(campaign)
    assert value["state"] == {
        "code": "POLYMARKET_SOURCE_INVALID_KALSHI_RUNNING",
        "integrity": "AUTHENTICATED",
        "severity": "warning",
    }
    polymarket = value["venues"]["polymarket"]
    assert polymarket["connectivity"]["verdict"] == "PUBLIC_SOURCE_INVALID_RUNTIME"
    assert polymarket["collection"]["slots_recorded"]["value"] == 1
    assert polymarket["collection"]["usable_slots"]["value"] is None
    for metric in ("bytes", "duplicates", "frames", "gaps", "reconnects", "segments"):
        assert polymarket["collection"][metric] == {
            "available": False,
            "provenance": "AUTHENTICATED_USABLE_SLOT_LEDGER",
            "value": None,
        }
    assert polymarket["data_quality"]["terminal_result_sha256"] == "5" * 64
    assert polymarket["last_manifest_sha256"] == "1" * 64
    assert value["holdout"]["access"] == "SEALED"
    assert value["economic_evidence_status"] == cockpit.ECONOMIC_STATUS


def test_latest_terminal_controls_connectivity_while_data_quality_history_persists(
    tmp_path: Path,
) -> None:
    campaign = _campaign(tmp_path)
    ledger = campaign / "polymarket" / "ledger.jsonl"
    runner.append_ledger(ledger, _invalid_entry("POLYMARKET", 0))
    runner.append_ledger(ledger, _ledger_entry("POLYMARKET", 1, frames=7))
    runner.append_ledger(
        campaign / "kalshi" / "ledger.jsonl",
        _ledger_entry("KALSHI", 0, frames=9),
    )
    _state(campaign, "POLYMARKET")
    _state(campaign, "KALSHI")
    value = cockpit.campaign_snapshot(campaign)
    polymarket = value["venues"]["polymarket"]
    assert value["state"]["code"] == "BOTH_RUNNING"
    assert polymarket["connectivity"]["verdict"] == "NETWORK_PREFLIGHT_GREEN"
    assert polymarket["data_quality"]["count"] == 1
    assert polymarket["collection"]["frames"]["value"] == 7


def test_missing_and_process_error_slots_never_enter_usable_metrics(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    missing = {
        **_invalid_entry("POLYMARKET", 0),
        "bytes": None,
        "duplicates": None,
        "error": "SYNTHETIC/FIXTURE slot elapsed",
        "frames": None,
        "gaps": None,
        "manifest_sha256": None,
        "probe_binding_sha256": None,
        "receipt_classification": (
            "SCHEDULED_SLOT_WITHOUT_AUTHENTICATED_RECEIPT_EXCLUDED_FROM_ECONOMICS"
        ),
        "reconnects": None,
        "root_sha256": None,
        "segments": None,
        "terminal_health": "PROCESS_ERROR_NO_TERMINAL_RECEIPT",
        "terminal_result_sha256": None,
    }
    ledger = campaign / "polymarket" / "ledger.jsonl"
    runner.append_ledger(ledger, missing)
    runner.append_ledger(ledger, _ledger_entry("POLYMARKET", 1, frames=7))
    _state(campaign, "POLYMARKET")
    _state(campaign, "KALSHI")
    value = cockpit.campaign_snapshot(campaign)
    collection = value["venues"]["polymarket"]["collection"]
    assert collection["slots_recorded"]["value"] == 2
    assert collection["usable_slots"]["value"] == 1
    assert collection["frames"]["value"] == 7


def test_capacity_refused_is_critical_and_mixed_degradation_is_not_called_running(
    tmp_path: Path,
) -> None:
    campaign = _campaign(tmp_path)
    _state(campaign, "POLYMARKET", lifecycle="CAPACITY_REFUSED")
    _state(campaign, "KALSHI")
    refused = cockpit.campaign_snapshot(campaign)
    assert refused["state"] == {
        "code": "CAPACITY_REFUSED",
        "integrity": "AUTHENTICATED",
        "severity": "critical",
    }

    mixed = _campaign(tmp_path / "mixed")
    runner.append_ledger(
        mixed / "polymarket" / "ledger.jsonl",
        _invalid_entry("POLYMARKET", 0),
    )
    runner.append_ledger(
        mixed / "kalshi" / "ledger.jsonl",
        {
            **_ledger_entry("KALSHI", 0, frames=0),
            "bytes": 0,
            "duplicates": 0,
            "economic_eligible": False,
            "error": "SYNTHETIC/FIXTURE source unavailable",
            "frames": 0,
            "gaps": 0,
            "manifest_sha256": None,
            "probe_binding_sha256": None,
            "receipt_classification": "CAMPAIGN_BOUND_PUBLIC_UNAVAILABILITY_RECEIPT",
            "reconnects": 0,
            "root_sha256": None,
            "segments": 0,
            "source_usable": False,
            "terminal_health": "PUBLIC_SOURCE_UNAVAILABLE",
        },
    )
    _state(mixed, "POLYMARKET")
    _state(mixed, "KALSHI")
    degraded = cockpit.campaign_snapshot(mixed)
    assert degraded["state"]["code"] == (
        "POLYMARKET_SOURCE_INVALID_KALSHI_UNAVAILABLE"
    )
    assert degraded["state"]["severity"] == "warning"


def test_integrity_tamper_symlink_and_oversize_fail_closed(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    manifest = campaign / "campaign-manifest.json"
    manifest.write_bytes(manifest.read_bytes() + b" ")
    with pytest.raises(cockpit.CockpitIntegrityError, match="pin diverged"):
        cockpit.campaign_snapshot(campaign)

    activation_campaign = _campaign(tmp_path / "activation")
    activation_path = activation_campaign / "state" / "activation-receipt.json"
    activation = json.loads(activation_path.read_bytes())
    activation["eligible_venues"] = ["polymarket"]
    activation_body = {
        key: value for key, value in activation.items() if key != "receipt_sha256"
    }
    activation["receipt_sha256"] = runner.sha256_bytes(
        runner.canonical_json_bytes(activation_body)
    )
    _write_json(activation_path, activation)
    with pytest.raises(cockpit.CockpitIntegrityError, match="binding diverged"):
        cockpit.campaign_snapshot(activation_campaign)

    manifest_bound_campaign = _campaign(tmp_path / "manifest-bound-activation")
    manifest_bound_path = (
        manifest_bound_campaign / "state" / "activation-receipt.json"
    )
    manifest_bound = json.loads(manifest_bound_path.read_bytes())
    manifest_bound["campaign_manifest_sha256"] = "f" * 64
    manifest_bound_body = {
        key: value
        for key, value in manifest_bound.items()
        if key != "receipt_sha256"
    }
    manifest_bound["receipt_sha256"] = runner.sha256_bytes(
        runner.canonical_json_bytes(manifest_bound_body)
    )
    _write_json(manifest_bound_path, manifest_bound)
    with pytest.raises(cockpit.CockpitIntegrityError, match="binding diverged"):
        cockpit.campaign_snapshot(manifest_bound_campaign)

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


@pytest.mark.parametrize(
    "relative",
    ("state/preflight-report.json", "state/activation-receipt.json"),
)
def test_campaign_requires_preflight_and_activation_evidence(
    tmp_path: Path,
    relative: str,
) -> None:
    campaign = _campaign(tmp_path)
    (campaign / relative).unlink()
    with pytest.raises(
        cockpit.CockpitIntegrityError,
        match="lacks authenticated preflight or activation evidence",
    ):
        cockpit.campaign_snapshot(campaign)


def test_completed_service_requires_a_loaded_authenticated_unit() -> None:
    properties = {
        "ActiveState": "inactive",
        "ExecMainStatus": "0",
        "LoadState": "not-found",
        "SubState": "dead",
    }
    assert not cockpit.complete_service_is_admissible(
        complete=True,
        show_returncode=0,
        system_error=None,
        properties=properties,
        pid=0,
        command_verified=False,
        fragment_verified=True,
    )
    assert cockpit.complete_service_is_admissible(
        complete=True,
        show_returncode=0,
        system_error=None,
        properties={**properties, "LoadState": "loaded"},
        pid=0,
        command_verified=False,
        fragment_verified=True,
    )
    assert not cockpit.complete_service_is_admissible(
        complete=True,
        show_returncode=0,
        system_error=None,
        properties={**properties, "LoadState": "loaded"},
        pid=0,
        command_verified=False,
        fragment_verified=False,
    )


@pytest.mark.parametrize(
    ("terminal", "condition"),
    (
        ("PUBLIC_SOURCE_INVALID", "PUBLIC_SOURCE_INVALID"),
        ("PUBLIC_SOURCE_UNAVAILABLE", "PUBLIC_SOURCE_UNAVAILABLE_RUNTIME"),
    ),
)
def test_completed_service_preserves_terminal_quality_condition(
    terminal: str,
    condition: str,
) -> None:
    status, terminal_condition = cockpit.classify_monitored_service(
        name="polymarket",
        ledger_error=None,
        lifecycle="COMPLETE_WINDOW",
        last_terminal=terminal,
        network_verdict="NETWORK_PREFLIGHT_GREEN",
        complete_service_ok=True,
        command_verified=False,
        active_state="inactive",
        prepared_stale=False,
    )
    assert status == "COMPLETE_WINDOW"
    assert terminal_condition == condition


def test_prepared_state_becomes_explicit_operational_failure_after_bounded_grace(
    tmp_path: Path,
) -> None:
    campaign = _campaign(tmp_path)
    for venue in ("POLYMARKET", "KALSHI"):
        _state(campaign, venue, lifecycle="PREPARED")
    starts_at = datetime.fromisoformat(
        str(CAMPAIGN_MANIFEST["starts_at_utc"]).replace("Z", "+00:00")
    )

    within_grace = cockpit.campaign_snapshot(
        campaign,
        now=starts_at + timedelta(seconds=cockpit.PREPARED_START_GRACE_SECONDS),
    )
    assert within_grace["state"]["code"] == "PREPARED"

    stale = cockpit.campaign_snapshot(
        campaign,
        now=starts_at
        + timedelta(seconds=cockpit.PREPARED_START_GRACE_SECONDS + 1),
    )
    assert stale["state"] == {
        "code": "PREPARED_STALE",
        "integrity": "AUTHENTICATED",
        "severity": "critical",
    }
    assert {
        stale["venues"][venue]["service_state"]
        for venue in ("polymarket", "kalshi")
    } == {"PREPARED_STALE"}

    status, terminal_condition = cockpit.classify_monitored_service(
        name="polymarket",
        ledger_error=None,
        lifecycle="PREPARED",
        last_terminal=None,
        network_verdict="NETWORK_PREFLIGHT_GREEN",
        complete_service_ok=False,
        command_verified=True,
        active_state="active",
        prepared_stale=True,
    )
    assert status == "PREPARED_STALE"
    assert terminal_condition is None


def test_recovery_dashboard_allows_only_authenticated_active_optional_collector() -> None:
    arguments = {
        "recovery_dashboard": True,
        "name": "kalshi",
        "eligible": True,
        "show_returncode": 0,
        "load_state": "loaded",
        "active_state": "active",
        "pid": 123,
        "command_verified": True,
        "state_present": True,
        "venue_status": "RUNNING",
    }
    assert cockpit.active_optional_service_is_admissible(**arguments)
    assert not cockpit.active_optional_service_is_admissible(
        **{**arguments, "command_verified": False}
    )
    assert not cockpit.active_optional_service_is_admissible(
        **{**arguments, "venue_status": "INTEGRITY_FAILED"}
    )


def test_semantically_tampered_but_rechained_ledger_fails_closed(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    ledger = campaign / "polymarket" / "ledger.jsonl"
    runner.append_ledger(ledger, _ledger_entry("POLYMARKET", 0, frames=10))
    row = json.loads(ledger.read_text(encoding="utf-8"))
    row["candidate_config_sha256"] = "f" * 64
    body = {key: value for key, value in row.items() if key != "entry_sha256"}
    row["entry_sha256"] = runner.sha256_bytes(runner.canonical_json_bytes(body))
    ledger.write_bytes(runner.canonical_json_bytes(row) + b"\n")

    with pytest.raises(
        cockpit.CockpitIntegrityError,
        match="ledger semantic authentication failed",
    ):
        cockpit.campaign_snapshot(campaign)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("recorded_slots", 0, "state and authenticated ledger diverged"),
        ("lifecycle", "SYNTHETIC_UNKNOWN", "lifecycle is not allowlisted"),
        ("lifecycle", "PREPARED", "lifecycle and slot plan diverged"),
        ("lifecycle", "COMPLETE_WINDOW", "lifecycle and slot plan diverged"),
    ),
)
def test_semantically_tampered_venue_state_fails_closed(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    campaign = _campaign(tmp_path)
    runner.append_ledger(
        campaign / "polymarket" / "ledger.jsonl",
        _ledger_entry("POLYMARKET", 0, frames=10),
    )
    _state(campaign, "POLYMARKET")
    state_path = campaign / "polymarket" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state[field] = value
    _write_json(state_path, state)

    with pytest.raises(cockpit.CockpitIntegrityError, match=message):
        cockpit.campaign_snapshot(campaign)


def test_collecting_state_must_name_the_next_authenticated_ordinal(
    tmp_path: Path,
) -> None:
    campaign = _campaign(tmp_path)
    runner.append_ledger(
        campaign / "polymarket" / "ledger.jsonl",
        _ledger_entry("POLYMARKET", 0, frames=10),
    )
    _state(campaign, "POLYMARKET")
    state_path = campaign / "polymarket" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.update({"active_ordinal": 0, "lifecycle": "COLLECTING"})
    _write_json(state_path, state)
    with pytest.raises(cockpit.CockpitIntegrityError, match="active ordinal diverged"):
        cockpit.campaign_snapshot(campaign)


def test_public_source_invalid_zero_raw_counters_are_refused(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path)
    row = _invalid_entry("POLYMARKET", 0)
    row.update({"bytes": 0, "frames": 0, "segments": 0})
    runner.append_ledger(campaign / "polymarket" / "ledger.jsonl", row)
    with pytest.raises(
        cockpit.CockpitIntegrityError,
        match="ledger semantic authentication failed",
    ):
        cockpit.campaign_snapshot(campaign)


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
        connection.request("GET", "/health/live")
        live = connection.getresponse()
        assert live.status == 200
        assert json.loads(live.read()) == {
            "mode": "readonly",
            "orders_enabled": False,
            "status": "alive",
        }
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
        (campaign / "state" / "activation-receipt.json").unlink()
        connection.request("GET", "/health/ready")
        missing_activation = connection.getresponse()
        assert missing_activation.status == 503
        assert json.loads(missing_activation.read())["status"] == "not-ready"
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
