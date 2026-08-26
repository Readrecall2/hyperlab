from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from ops.h1_campaign import launch_pack

ROOT = Path(__file__).resolve().parents[1]
OPS = ROOT / "ops" / "h1_campaign"
PLAN_PATH = OPS / "launch-plan-v1.json"


def _plan() -> dict[str, object]:
    decoded = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    assert isinstance(decoded, dict)
    return decoded


def _handoff() -> dict[str, object]:
    plan = launch_pack.validate_plan(_plan())
    return {
        "boundary": launch_pack.BOUNDARY,
        "bundle": {
            "filename": "hyperlab-h1-prospective-campaign-launch-v1.bundle",
            "ref": "refs/heads/codex/h1-prospective-campaign-launch-v1",
            "sha256": "a" * 64,
        },
        "campaign_id": "h1-" + "b" * 24,
        "campaign_slug": plan["campaign_slug"],
        "disk": plan["disk"],
        "fee_reviewed_at_utc": plan["fee_reviewed_at_utc"],
        "files": {
            "campaign_health_sha256": "b" * 64,
            "campaign_manifest_pin_sha256": "c" * 64,
            "campaign_manifest_sha256": "d" * 64,
        },
        "inventory": {
            "fee_artifact_sha256": "e" * 64,
            "fee_review_sha256": "f" * 64,
            "policy_config_file_sha256": "1" * 64,
            "policy_config_sha256": "2" * 64,
            "requirements_lock_sha256": "3" * 64,
        },
        "launch_plan_path": "ops/h1_campaign/launch-plan-v1.json",
        "launch_plan_sha256": "4" * 64,
        "remote": plan["remote"],
        "schema_version": 1,
        "service_name": plan["service_name"],
        "source_commit": "5" * 40,
        "starts_at_utc": plan["starts_at_utc"],
        "arm_deadline_utc": plan["arm_deadline_utc"],
    }


def test_launch_plan_freezes_unique_roots_times_and_full_disk_budget() -> None:
    plan = launch_pack.validate_plan(_plan())
    assert plan["boundary"] == "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
    assert plan["campaign_slug"] == "h1-20260827t190000z-7b91d4e2"
    assert plan["starts_at_utc"] == "2026-08-27T19:00:00Z"
    assert plan["arm_deadline_utc"] == "2026-08-27T18:30:00Z"
    disk = plan["disk"]
    assert isinstance(disk, dict)
    assert disk["maximum_raw_bytes"] == 128 * 1024**3
    assert disk["margin_bytes"] == 16 * 1024**3
    assert disk["required_free_bytes"] == 144 * 1024**3
    remote = plan["remote"]
    assert isinstance(remote, dict)
    assert len({remote["incoming_root"], remote["source_root"], remote["campaign_root"]}) == 3
    assert "hyperliquid-h1-001" not in json.dumps(plan)


@pytest.mark.parametrize(
    "path",
    [
        "/tmp/h1-run",
        "/home/hyperlab/hyperlab-h1/incoming/../escape",
        "/home/hyperlab/hyperlab-h1/incoming",
        "home/hyperlab/hyperlab-h1/incoming/run",
    ],
)
def test_incoming_path_validation_refuses_escape_or_reuse(path: str) -> None:
    with pytest.raises(launch_pack.LaunchPackError):
        launch_pack.validate_handoff_remote_path(path, category="incoming")


def test_inventory_binds_policy_fee_review_lock_and_raw_ceiling() -> None:
    inventory = launch_pack.build_inventory(ROOT, _plan())
    assert inventory == {
        "fee_artifact_sha256": "b01bc3787fc4d1f45e7f138e0803966d0dd4ca2595dbc0fedbb631ad74c9fb26",
        "fee_review_sha256": launch_pack.sha256_file(
            ROOT / "config/paper/hyperliquid-tier0-fee-review-2026-08-26.json"
        ),
        "policy_config_file_sha256": "cdaf814e0b8a24524f6372ed6f83da76e1a1e7016336cb27407e9021a12f0063",
        "policy_config_sha256": "020a3410b1c6adc8605b87f0827f5909a9fefc4e400d14bc3eb76f1453735244",
        "requirements_lock_sha256": "55438cb49b92215e78fc78888643654b70b982dd62b14b2775039a6dad8194d6",
    }


def test_fee_review_is_official_current_and_does_not_rewrite_history() -> None:
    review = json.loads(
        (ROOT / "config/paper/hyperliquid-tier0-fee-review-2026-08-26.json").read_text(
            encoding="utf-8"
        )
    )
    assert review["official_source"]["source_url"] == (
        "https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees"
    )
    assert review["official_source"]["content_sha256"] == (
        "75e55504b1b887a89e1668a9544d09b013242747bcc29f8648a5410142a71258"
    )
    assert review["structured_extract"]["perps"]["maker_fee_bps"] == "1.5"
    assert review["structured_extract"]["perps"]["taker_fee_bps"] == "4.5"
    assert review["comparison"]["policy_change_required"] is False
    assert launch_pack.sha256_file(
        ROOT / "config/paper/hyperliquid-tier0-fees-2026-08-16.json"
    ) == review["comparison"]["historical_fee_artifact_sha256"]


def test_capacity_fails_closed_and_resume_uses_remaining_budget_plus_margin() -> None:
    handoff = _handoff()
    disk = handoff["disk"]
    assert isinstance(disk, dict)
    required = int(disk["required_free_bytes"])
    starts = datetime.fromisoformat(str(handoff["starts_at_utc"]).replace("Z", "+00:00"))
    initial = launch_pack.preflight_snapshot(
        handoff=handoff,
        current_user="hyperlab",
        now=starts - timedelta(hours=1),
        ntp_synchronized=True,
        available_bytes=required,
        raw_exists=False,
        raw_stored_bytes=0,
        writer_lock_available=True,
        forbidden_environment=(),
    )
    assert initial["collection_mode"] == "INITIAL"
    with pytest.raises(launch_pack.LaunchPackError, match="H1_DISK_CAPACITY_INSUFFICIENT"):
        launch_pack.preflight_snapshot(
            handoff=handoff,
            current_user="hyperlab",
            now=starts - timedelta(hours=1),
            ntp_synchronized=True,
            available_bytes=required - 1,
            raw_exists=False,
            raw_stored_bytes=0,
            writer_lock_available=True,
            forbidden_environment=(),
        )
    stored = 8 * 1024**3
    remaining_required = required - stored
    resumed = launch_pack.preflight_snapshot(
        handoff=handoff,
        current_user="hyperlab",
        now=starts + timedelta(hours=1),
        ntp_synchronized=True,
        available_bytes=remaining_required,
        raw_exists=True,
        raw_stored_bytes=stored,
        writer_lock_available=True,
        forbidden_environment=(),
    )
    assert resumed["collection_mode"] == "RESUME"
    capacity = resumed["capacity"]
    assert isinstance(capacity, dict)
    assert capacity["required_free_bytes"] == remaining_required


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"current_user": "root"}, "H1_USER_REFUSED"),
        ({"ntp_synchronized": False}, "H1_NTP_NOT_SYNCHRONIZED"),
        ({"writer_lock_available": False}, "H1_WRITER_ALREADY_ACTIVE"),
        ({"forbidden_environment": ("PRIVATE_KEY",)}, "H1_PRIVATE_SURFACE"),
    ],
)
def test_preflight_refuses_wrong_user_ntp_writer_or_private_environment(
    changes: dict[str, object], match: str
) -> None:
    handoff = _handoff()
    disk = handoff["disk"]
    assert isinstance(disk, dict)
    starts = datetime.fromisoformat(str(handoff["starts_at_utc"]).replace("Z", "+00:00"))
    arguments: dict[str, object] = {
        "handoff": handoff,
        "current_user": "hyperlab",
        "now": starts - timedelta(hours=1),
        "ntp_synchronized": True,
        "available_bytes": int(disk["required_free_bytes"]),
        "raw_exists": False,
        "raw_stored_bytes": 0,
        "writer_lock_available": True,
        "forbidden_environment": (),
    }
    arguments.update(changes)
    with pytest.raises(launch_pack.LaunchPackError, match=match):
        launch_pack.preflight_snapshot(**arguments)  # type: ignore[arg-type]


def test_first_launch_after_frozen_start_is_refused_but_resume_is_allowed() -> None:
    handoff = _handoff()
    disk = handoff["disk"]
    assert isinstance(disk, dict)
    starts = datetime.fromisoformat(str(handoff["starts_at_utc"]).replace("Z", "+00:00"))
    base = {
        "handoff": handoff,
        "current_user": "hyperlab",
        "now": starts + timedelta(seconds=1),
        "ntp_synchronized": True,
        "available_bytes": int(disk["required_free_bytes"]),
        "raw_stored_bytes": 0,
        "writer_lock_available": True,
        "forbidden_environment": (),
    }
    with pytest.raises(launch_pack.LaunchPackError, match="PROSPECTIVE_START_MISSED"):
        launch_pack.preflight_snapshot(raw_exists=False, **base)  # type: ignore[arg-type]
    assert launch_pack.preflight_snapshot(raw_exists=True, **base)["collection_mode"] == "RESUME"  # type: ignore[arg-type]


def test_canonical_h1_prepare_creates_new_portable_seed_without_network(tmp_path: Path) -> None:
    plan = launch_pack.validate_plan(_plan())
    inventory = launch_pack.build_inventory(ROOT, plan)
    seed = tmp_path / "unique-seed"
    prepared = launch_pack._prepare_campaign_seed(ROOT, plan, seed)
    manifest = launch_pack.validate_campaign_manifest(
        seed / "campaign-manifest.json",
        seed / "campaign-manifest.sha256",
        plan,
        inventory,
    )
    assert prepared["campaign_manifest_sha256"] == launch_pack.sha256_file(
        seed / "campaign-manifest.json"
    )
    assert manifest["policy_config_path"] == "config/research/hyperliquid-h1-ghost-v1.json"
    assert manifest["fee_artifact_path"] == (
        "config/paper/hyperliquid-tier0-fees-2026-08-16.json"
    )
    assert manifest["holdout"]["access"] == "SEALED_UNTIL_COLLECTION_COMPLETE"
    assert (seed / "state/health.json").is_file()


def test_systemd_render_is_persistent_bounded_hardened_and_graceful() -> None:
    unit = launch_pack.render_systemd_unit(_handoff())
    assert "User=hyperlab" in unit
    assert "After=network-online.target time-sync.target" in unit
    assert "ExecCondition=" in unit and "vps-preflight" in unit
    assert "ExecStart=/usr/bin/bash" in unit and "run_collector.sh" in unit
    assert "Restart=on-failure" in unit
    assert "RestartSec=60" in unit
    assert "StartLimitBurst=3" in unit
    assert "StartLimitIntervalSec=1800" in unit
    assert "KillSignal=SIGINT" in unit
    assert "SuccessExitStatus=130" in unit
    assert "SendSIGKILL=no" in unit
    assert "ProtectSystem=strict" in unit
    assert "ProtectHome=read-only" in unit
    assert "NoNewPrivileges=yes" in unit
    assert "CapabilityBoundingSet=\n" in unit
    assert "ReadWritePaths=/home/hyperlab/hyperlab-h1/campaigns/" in unit
    assert "ListenStream" not in unit and "ListenDatagram" not in unit


def test_monitor_distinguishes_armed_running_terminal_and_false_pid() -> None:
    handoff = _handoff()
    remote = handoff["remote"]
    assert isinstance(remote, dict)
    campaign = str(remote["campaign_root"])
    starts = datetime.fromisoformat(str(handoff["starts_at_utc"]).replace("Z", "+00:00"))
    prepared_health = {
        "boundary": launch_pack.BOUNDARY,
        "campaign_id": handoff["campaign_id"],
        "manifest_sha256": None,
        "terminal_health": "PREPARED_NOT_STARTED",
    }
    waiting_cmd = (
        "/usr/bin/bash /home/hyperlab/hyperlab-h1/sources/"
        "h1-20260827t190000z-7b91d4e2/ops/h1_campaign/run_collector.sh "
        "/home/hyperlab/hyperlab-h1/incoming/h1-20260827t190000z-7b91d4e2/handoff.json"
    )
    armed = launch_pack.evaluate_monitor(
        active_state="active",
        main_pid=123,
        command_line=waiting_cmd,
        health=prepared_health,
        handoff=handoff,
        now=starts - timedelta(minutes=5),
    )
    assert armed["status"] == "H1_SERVICE_ARMED_PREPARED_NOT_STARTED"
    running_health = {**prepared_health, "terminal_health": "RUNNING"}
    collect_cmd = (
        "/home/hyperlab/hyperlab-h1/sources/h1-20260827t190000z-7b91d4e2/.venv/bin/python "
        "-m hyperlab research-data h1-collect "
        f"--campaign-root {campaign} --config /home/hyperlab/config.json"
    )
    running = launch_pack.evaluate_monitor(
        active_state="active",
        main_pid=124,
        command_line=collect_cmd,
        health=running_health,
        handoff=handoff,
        now=starts + timedelta(seconds=1),
    )
    assert running["status"] == "H1_SERVICE_RUNNING_HEALTH_GREEN"
    with pytest.raises(launch_pack.LaunchPackError, match="FALSE_SYSTEMD_PID"):
        launch_pack.evaluate_monitor(
            active_state="active",
            main_pid=0,
            command_line="",
            health=prepared_health,
            handoff=handoff,
            now=starts - timedelta(minutes=5),
        )
    with pytest.raises(launch_pack.LaunchPackError, match="NOT_ACTIVE"):
        launch_pack.evaluate_monitor(
            active_state="inactive",
            main_pid=0,
            command_line="",
            health=running_health,
            handoff=handoff,
            now=starts + timedelta(seconds=1),
        )


def test_runtime_scripts_have_exact_public_collector_resume_and_no_private_route() -> None:
    run_script = (OPS / "run_collector.sh").read_text(encoding="utf-8")
    bootstrap = (OPS / "bootstrap-linux.sh").read_text(encoding="utf-8")
    installer = (OPS / "vps-install.sh").read_text(encoding="utf-8")
    combined = "\n".join((run_script, bootstrap, installer))
    assert run_script.count("research-data h1-collect") == 2
    assert "--resume" in run_script and '[[ -d "$CAMPAIGN_ROOT/raw" ]]' in run_script
    assert "--system-site-packages" not in bootstrap
    assert "include-system-site-packages = false" in bootstrap
    assert "--require-hashes" in bootstrap
    assert "--only-binary=:all:" in bootstrap
    assert "sys.version_info[:3] == (3, 12, 13)" in bootstrap
    assert "timeout --signal=INT --kill-after=60s 30m" in bootstrap
    assert "hyperliquid.exchange.Exchange" not in combined
    assert "research-data probe" not in combined
    assert "lighter-access-completion" not in combined
    assert "--live" not in combined and "--trade" not in combined and "--mainnet" not in combined
    assert "rm -rf" not in combined


def test_shell_separation_atomic_unit_install_and_read_only_monitor() -> None:
    windows = (OPS / "New-H1CampaignBundle.ps1").read_text(encoding="utf-8")
    installer = (OPS / "vps-install.sh").read_text(encoding="utf-8")
    monitor = (OPS / "monitor.sh").read_text(encoding="utf-8")
    assert "#!/usr/bin/env bash" not in windows
    assert "case \"$" not in windows
    assert "git bundle create" in windows and "git bundle verify" in windows
    assert 'case "$INCOMING_ROOT" in' in installer
    assert '"$HOME"/hyperlab-h1/incoming/*)' in installer
    assert 'sudo ln "$UNIT_TEMP" "$UNIT_TARGET"' in installer
    assert '[[ ! -e "/etc/systemd/system/$SERVICE" ]]' in installer
    assert "SERVICE_LOAD_STATE=$(systemctl show" in installer
    assert "systemd-analyze verify" in installer
    assert "--property=FragmentPath" in installer
    assert "installed systemd unit bytes differ" in installer
    assert "systemctl enable --now" in installer
    assert "cat \"$CAMPAIGN_ROOT/state/health.json\"" in monitor
    assert "journalctl -u" in monitor and "systemctl show" in monitor
    assert not any(token in monitor for token in ("rm ", "mv ", "install ", "mkdir ", ">>"))


def test_final_operator_blocks_are_exact_and_never_mix_shells(tmp_path: Path) -> None:
    handoff = _handoff()
    windows = launch_pack.render_windows_operator_block(
        handoff, output_root=tmp_path / "artifacts", repo_root=ROOT
    )
    tabby = launch_pack.render_tabby_operator_block(handoff)
    assert "LOCATION: Windows PowerShell local" in windows
    assert '"$env:USERPROFILE\\.ssh\\hyperlab_hetzner"' in windows
    assert "$RemoteIp = '5.223.60.130'" in windows
    assert "Test-NetConnection" in windows
    assert "sftp.exe" in windows and "scp.exe" in windows
    assert "H1_WINDOWS_TRANSFER_GREEN_NOT_LAUNCHED" in windows
    assert "#!/usr/bin/env bash" not in windows
    assert "set -Eeuo pipefail" not in windows
    assert "LOCATION: Tabby - VPS" in tabby
    assert 'case "$H1_INCOMING_ROOT" in' in tabby
    assert '"$HOME"/hyperlab-h1/incoming/*)' in tabby
    assert "sha256sum -c launch-files.sha256" in tabby
    assert "timedatectl show --property=NTPSynchronized" in tabby
    assert "df -PB1" in tabby
    assert "git clone --no-checkout" in tabby
    assert "checkout --detach" in tabby
    assert "watch -n 10" in tabby
    assert "sudo systemctl stop" in tabby and "sudo systemctl start" in tabby
    assert "Test-NetConnection" not in tabby
    assert "$env:" not in tabby
