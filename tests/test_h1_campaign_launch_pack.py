from __future__ import annotations

import json
import os
import subprocess
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
            "source_inventory_sha256": "6" * 64,
            "systemd_unit_sha256": "7" * 64,
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
        "volume": plan["volume"],
    }


def _volume_snapshot() -> dict[str, object]:
    return {
        "canonical_device": "/dev/sdb",
        "filesystem": "ext4",
        "model": "Volume",
        "mount_options": ("rw", "relatime", "discard"),
        "mount_target": "/mnt/HC_Volume_106716684",
        "serial": "106716684",
        "source_device": "/dev/sdb",
    }


def test_launch_plan_freezes_unique_roots_times_and_full_disk_budget() -> None:
    plan = launch_pack.validate_plan(_plan())
    assert plan["boundary"] == "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
    assert plan["campaign_slug"] == "h1-20260827t210000z-e52a227b"
    assert plan["starts_at_utc"] == "2026-08-27T21:00:00Z"
    assert plan["arm_deadline_utc"] == "2026-08-27T20:30:00Z"
    disk = plan["disk"]
    assert isinstance(disk, dict)
    assert disk["maximum_raw_bytes"] == 128 * 1024**3
    assert disk["margin_bytes"] == 16 * 1024**3
    assert disk["required_free_bytes"] == 144 * 1024**3
    assert disk["incoming_staging_max_bytes"] == 64 * 1024**2
    remote = plan["remote"]
    assert isinstance(remote, dict)
    assert len({remote["incoming_root"], remote["source_root"], remote["campaign_root"]}) == 3
    assert str(remote["incoming_root"]).startswith("/home/hyperlab/hyperlab-h1/incoming/")
    assert str(remote["source_root"]).startswith(
        "/mnt/HC_Volume_106716684/hyperlab-h1/sources/"
    )
    assert str(remote["campaign_root"]).startswith(
        "/mnt/HC_Volume_106716684/hyperlab-h1/campaigns/"
    )
    volume = plan["volume"]
    assert isinstance(volume, dict)
    assert volume == {
        "device": "/dev/sdb",
        "filesystem": "ext4",
        "model": "Volume",
        "mount_point": "/mnt/HC_Volume_106716684",
        "observed_available_bytes": 199487336448,
        "observed_mount_options": ["rw", "relatime", "discard"],
        "serial": "106716684",
    }
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


def test_remote_path_validation_accepts_only_split_home_and_volume_roots() -> None:
    slug = "h1-20260827t210000z-e52a227b"
    assert launch_pack.validate_remote_path(
        f"/home/hyperlab/hyperlab-h1/incoming/{slug}", category="incoming", slug=slug
    )
    assert launch_pack.validate_remote_path(
        f"/mnt/HC_Volume_106716684/hyperlab-h1/sources/{slug}",
        category="sources",
        slug=slug,
    )
    with pytest.raises(launch_pack.LaunchPackError):
        launch_pack.validate_remote_path(
            f"/home/hyperlab/hyperlab-h1/sources/{slug}", category="sources", slug=slug
        )


def test_inventory_binds_policy_fee_review_lock_and_raw_ceiling() -> None:
    inventory = launch_pack.build_inventory(ROOT, _plan())
    assert inventory == {
        "fee_artifact_sha256": "b01bc3787fc4d1f45e7f138e0803966d0dd4ca2595dbc0fedbb631ad74c9fb26",
        "fee_review_sha256": "76c0c8645b02e04de4f1e4d044d0b164c0654757365a03f25b3f9ae9e1be23be",
        "policy_config_file_sha256": "2e27bcd0bdb8ab94e48c3f08ff8c0f325f03d6e3a1da9be4f82d64217e372bab",
        "policy_config_sha256": "020a3410b1c6adc8605b87f0827f5909a9fefc4e400d14bc3eb76f1453735244",
        "requirements_lock_sha256": "4cef405fa03cb354539256c975e54e49c0785b3797a638e4ca6920e3ec5b067e",
    }


def test_git_identity_hash_uses_lf_blob_for_crlf_worktree_and_refuses_other_drift(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "portable-git-identity"
    repo.mkdir()

    def git(*arguments: str) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["git", "-C", str(repo), *arguments], check=True, capture_output=True
        )

    git("init", "--quiet")
    git("config", "user.email", "h1-portability@example.invalid")
    git("config", "user.name", "H1 Portability Test")
    git("config", "core.autocrlf", "true")
    identity = repo / "identity.txt"
    canonical = b"policy=public-only\nfees=tier0\n"
    identity.write_bytes(canonical)
    git("add", "identity.txt")
    git("commit", "--quiet", "-m", "portable identity fixture")
    assert git("show", "HEAD:identity.txt").stdout == canonical

    identity.unlink()
    git("checkout-index", "--force", "--", "identity.txt")
    assert identity.read_bytes() == canonical.replace(b"\n", b"\r\n")
    subprocess.run(
        ["git", "-C", str(repo), "update-index", "--refresh", "--", "identity.txt"],
        check=False,
        capture_output=True,
    )
    assert git("status", "--porcelain", "--", "identity.txt").stdout == b""
    assert launch_pack.portable_git_file_sha256(repo, "identity.txt") == (
        launch_pack.sha256_bytes(canonical)
    )

    identity.write_bytes(b"policy=public-only\r\nfees=changed\r\n")
    with pytest.raises(launch_pack.LaunchPackError, match="beyond reversible CRLF"):
        launch_pack.portable_git_file_sha256(repo, "identity.txt")


def test_fee_review_is_official_current_and_does_not_rewrite_history() -> None:
    review = json.loads(
        (
            ROOT / "config/paper/hyperliquid-tier0-fee-review-2026-08-26T223048Z.json"
        ).read_text(
            encoding="utf-8"
        )
    )
    assert (ROOT / "config/paper/hyperliquid-tier0-fee-review-2026-08-26.json").is_file()
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


def test_v4_df_failure_is_append_only_and_preparation_never_started() -> None:
    receipt = json.loads(
        (OPS / "h1-20260827t200000z-21fa9dba-abandonment.json").read_text(encoding="utf-8")
    )
    assert receipt == {
        "abandonment_status": "ABANDONED_BEFORE_VOLUME_PREPARATION_DF_OPTION_INCOMPATIBILITY",
        "campaign_slug": "h1-20260827t200000z-21fa9dba",
        "cause": "GNU_DF_OPTIONS_P_AND_OUTPUT_ARE_MUTUALLY_EXCLUSIVE",
        "execution_effect": (
            "NO_DIRECTORY_PREPARATION_NO_TRANSFER_NO_SERVICE_NO_NETWORK_COLLECTION"
        ),
    }


def test_v5_portable_identity_failure_is_append_only_and_no_service_started() -> None:
    receipt = json.loads(
        (OPS / "h1-20260827t180000z-a007df56-abandonment.json").read_text(encoding="utf-8")
    )
    assert receipt == {
        "abandonment_status": (
            "ABANDONED_AFTER_TRANSFER_BEFORE_SYSTEMD_PORTABLE_IDENTITY_MISMATCH"
        ),
        "campaign_slug": "h1-20260827t180000z-a007df56",
        "cause": "WINDOWS_CRLF_WORKTREE_HASHES_DIVERGED_FROM_CANONICAL_GIT_LF_CHECKOUT",
        "execution_effect": (
            "TRANSFER_CLONE_BOOTSTRAP_IMPORT_PREFLIGHT_CAMPAIGN_SEED_ONLY_"
            "NO_SYSTEMD_NO_SERVICE_NO_NETWORK_COLLECTION"
        ),
    }


def test_v6_systemd_sandbox_failure_receipt_is_append_only_and_no_execstart() -> None:
    receipt = json.loads(
        (OPS / "h1-20260827t210000z-c0043345-abandonment.json").read_text(
            encoding="utf-8"
        )
    )
    assert receipt == {
        "abandonment_status": (
            "SYSTEMD_EXEC_CONDITION_SANDBOX_FALSE_READ_ONLY_NO_EXECSTART_NO_COLLECTION"
        ),
        "campaign_slug": "h1-20260827t210000z-c0043345",
        "cause": "HOST_VOLUME_RW_BUT_PROTECTSYSTEM_STRICT_PARENT_VIEW_READ_ONLY_IN_EXEC_CONDITION",
        "prepared_state": {
            "manifest_sha256": None,
            "raw_root_sha256": None,
            "terminal_health": "PREPARED_NOT_STARTED",
        },
        "systemd_state": {
            "active_state": "inactive",
            "exec_main_status": 0,
            "load_state": "loaded",
            "main_pid": 0,
            "n_restarts": 0,
            "sub_state": "dead",
        },
    }


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
        volume_snapshot=_volume_snapshot(),
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
            volume_snapshot=_volume_snapshot(),
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
        volume_snapshot=_volume_snapshot(),
    )
    assert resumed["collection_mode"] == "RESUME"
    capacity = resumed["capacity"]
    assert isinstance(capacity, dict)
    assert capacity["required_free_bytes"] == remaining_required


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"mount_target": "/"}, "VOLUME_MOUNT_REFUSED"),
        ({"source_device": "/dev/sda"}, "VOLUME_DEVICE_REFUSED"),
        ({"canonical_device": "/dev/disk/by-id/alias"}, "VOLUME_DEVICE_REFUSED"),
        ({"filesystem": "xfs"}, "VOLUME_FILESYSTEM_REFUSED"),
        ({"mount_options": ("ro", "relatime")}, "VOLUME_READ_ONLY_REFUSED"),
        ({"serial": "different"}, "VOLUME_SERIAL_REFUSED"),
        ({"model": "different"}, "VOLUME_MODEL_REFUSED"),
    ],
)
def test_volume_snapshot_refuses_path_device_fs_readonly_or_identity_drift(
    changes: dict[str, object], match: str
) -> None:
    plan = launch_pack.validate_plan(_plan())
    volume = plan["volume"]
    disk = plan["disk"]
    assert isinstance(volume, dict) and isinstance(disk, dict)
    snapshot = _volume_snapshot()
    snapshot.update(changes)
    mount_options = snapshot["mount_options"]
    assert isinstance(mount_options, tuple)
    with pytest.raises(launch_pack.LaunchPackError, match=match):
        launch_pack.validate_volume_snapshot(
            contract=volume,
            mount_target=str(snapshot["mount_target"]),
            source_device=str(snapshot["source_device"]),
            canonical_device=str(snapshot["canonical_device"]),
            filesystem=str(snapshot["filesystem"]),
            mount_options=tuple(str(item) for item in mount_options),
            serial=str(snapshot["serial"]) if snapshot.get("serial") else None,
            model=str(snapshot["model"]) if snapshot.get("model") else None,
            available_bytes=int(volume["observed_available_bytes"]),
            required_free_bytes=int(disk["required_free_bytes"]),
        )


def test_volume_observed_capacity_has_explicit_non_silent_margin() -> None:
    plan = launch_pack.validate_plan(_plan())
    volume = plan["volume"]
    disk = plan["disk"]
    assert isinstance(volume, dict) and isinstance(disk, dict)
    assert int(volume["observed_available_bytes"]) - int(disk["required_free_bytes"]) == 44868513792


def test_v2_abandonment_receipt_contains_only_authorized_facts() -> None:
    receipt = json.loads(
        (OPS / "h1-20260827t190000z-7b91d4e2-abandonment.json").read_text(encoding="utf-8")
    )
    assert receipt == {
        "abandonment_status": "ABANDONED_BEFORE_TRANSFER_INSUFFICIENT_ROOT_DISK",
        "available_root_bytes": 36332081152,
        "capacity_verdict": "H1_DISK_CAPACITY_INSUFFICIENT",
        "required_free_bytes": 154618822656,
        "transfer_launch_collection": "NO_TRANSFER_NO_LAUNCH_NO_NETWORK_COLLECTION",
    }


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
        "volume_snapshot": _volume_snapshot(),
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
        "volume_snapshot": _volume_snapshot(),
    }
    with pytest.raises(launch_pack.LaunchPackError, match="PROSPECTIVE_START_MISSED"):
        launch_pack.preflight_snapshot(raw_exists=False, **base)  # type: ignore[arg-type]
    assert launch_pack.preflight_snapshot(raw_exists=True, **base)["collection_mode"] == "RESUME"  # type: ignore[arg-type]


def test_service_preflight_admits_strict_parent_ro_with_campaign_root_rw() -> None:
    handoff = _handoff()
    disk = handoff["disk"]
    assert isinstance(disk, dict)
    deadline = datetime.fromisoformat(
        str(handoff["arm_deadline_utc"]).replace("Z", "+00:00")
    )
    result = launch_pack.service_preflight_snapshot(
        handoff=handoff,
        current_user="hyperlab",
        now=deadline - timedelta(minutes=1),
        ntp_synchronized=True,
        available_bytes=int(disk["required_free_bytes"]),
        raw_exists=False,
        raw_stored_bytes=0,
        writer_lock_available=True,
        forbidden_environment=(),
        campaign_root_writable=True,
    )
    assert result["status"] == "H1_SYSTEMD_SERVICE_PREFLIGHT_GREEN"
    assert result["campaign_root_write_probe"] == (
        "FSYNC_FILE_DIRECTORY_DELETE_FSYNC_GREEN"
    )
    assert "volume" not in result


def test_service_preflight_refuses_non_writable_campaign_root_and_missed_deadline() -> None:
    handoff = _handoff()
    disk = handoff["disk"]
    assert isinstance(disk, dict)
    deadline = datetime.fromisoformat(
        str(handoff["arm_deadline_utc"]).replace("Z", "+00:00")
    )
    arguments = {
        "handoff": handoff,
        "current_user": "hyperlab",
        "now": deadline,
        "ntp_synchronized": True,
        "available_bytes": int(disk["required_free_bytes"]),
        "raw_exists": False,
        "raw_stored_bytes": 0,
        "writer_lock_available": True,
        "forbidden_environment": (),
    }
    with pytest.raises(launch_pack.LaunchPackError, match="CAMPAIGN_ROOT_NOT_WRITABLE"):
        launch_pack.service_preflight_snapshot(
            campaign_root_writable=False, **arguments  # type: ignore[arg-type]
        )
    with pytest.raises(launch_pack.LaunchPackError, match="ARM_DEADLINE_MISSED"):
        launch_pack.service_preflight_snapshot(
            campaign_root_writable=True,
            **{**arguments, "now": deadline + timedelta(seconds=1)},  # type: ignore[arg-type]
        )


def test_campaign_write_probe_is_exclusive_fsynced_removed_and_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[tuple[object, ...]] = []
    probe_path = tmp_path / launch_pack.SERVICE_WRITE_PROBE_NAME

    def tracked_open(path: object, flags: int, mode: int = 0o777) -> int:
        events.append(("open", Path(path), flags, mode))
        return 10 if Path(path) == tmp_path else 11

    def tracked_write(fd: int, value: bytes | memoryview) -> int:
        events.append(("write", bytes(value)))
        return len(value)

    def tracked_fsync(fd: int) -> None:
        events.append(("fsync", fd))

    def tracked_unlink(path: object) -> None:
        events.append(("unlink", Path(path)))

    def tracked_close(fd: int) -> None:
        events.append(("close", fd))

    monkeypatch.setattr(launch_pack.os, "open", tracked_open)
    monkeypatch.setattr(launch_pack.os, "write", tracked_write)
    monkeypatch.setattr(launch_pack.os, "fsync", tracked_fsync)
    monkeypatch.setattr(launch_pack.os, "unlink", tracked_unlink)
    monkeypatch.setattr(launch_pack.os, "close", tracked_close)
    launch_pack._campaign_root_write_probe(tmp_path)
    probe_open = next(event for event in events if event[0] == "open" and event[1] == probe_path)
    assert int(probe_open[2]) & os.O_EXCL
    assert ("write", launch_pack.SERVICE_WRITE_PROBE_BYTES) in events
    assert len([event for event in events if event[0] == "fsync"]) == 3
    assert ("unlink", probe_path) in events

    def refused_open(path: object, flags: int, mode: int = 0o777) -> int:
        if Path(path) == probe_path:
            raise PermissionError("campaign root is read-only")
        return 10

    monkeypatch.setattr(launch_pack.os, "open", refused_open)
    with pytest.raises(launch_pack.LaunchPackError, match="CAMPAIGN_ROOT_NOT_WRITABLE"):
        launch_pack._campaign_root_write_probe(tmp_path)


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
    assert "ExecCondition=" in unit and "service-preflight" in unit
    assert "vps-preflight" not in unit
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
    assert "RequiresMountsFor=/mnt/HC_Volume_106716684" in unit
    assert "ConditionPathIsMountPoint=/mnt/HC_Volume_106716684" in unit
    assert "ReadWritePaths=/mnt/HC_Volume_106716684/hyperlab-h1/campaigns/" in unit
    read_write_lines = [line for line in unit.splitlines() if line.startswith("ReadWritePaths=")]
    assert read_write_lines == [
        "ReadWritePaths=/mnt/HC_Volume_106716684/hyperlab-h1/campaigns/"
        "h1-20260827t210000z-e52a227b"
    ]
    assert "/mnt/HC_Volume_106716684/hyperlab-h1/sources/" not in read_write_lines[0]
    assert "ExecCondition=" in unit.split("ExecStart=", maxsplit=1)[0]
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
        "/usr/bin/bash /mnt/HC_Volume_106716684/hyperlab-h1/sources/"
        "h1-20260827t210000z-e52a227b/ops/h1_campaign/run_collector.sh "
        "/home/hyperlab/hyperlab-h1/incoming/h1-20260827t210000z-e52a227b/handoff.json"
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
        "/mnt/HC_Volume_106716684/hyperlab-h1/sources/h1-20260827t210000z-e52a227b/.venv/bin/python "
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
    assert "/mnt/HC_Volume_106716684/hyperlab-h1/sources/" in bootstrap
    assert "/mnt/HC_Volume_106716684/hyperlab-h1/campaigns/" in run_script
    assert "findmnt -rn -T" in installer
    assert "vps-preflight --handoff" in installer
    assert "service-preflight --handoff" not in installer
    assert "H1_ARM_DEADLINE_MISSED" in installer
    assert 'VOLUME_DEVICE=${VALUES[12]}' in installer
    assert 'VOLUME_FS=${VALUES[13]}' in installer
    assert "incoming staging must never contain raw campaign data" in installer
    assert "cmp --silent" in installer
    assert "hyperliquid.exchange.Exchange" not in combined
    assert "research-data probe" not in combined
    assert "lighter-access-completion" not in combined
    assert "--live" not in combined and "--trade" not in combined and "--mainnet" not in combined
    assert "rm -rf" not in combined
    assert not any(
        token in combined
        for token in ("mkfs ", "fdisk ", "parted ", "resize2fs ", " /etc/fstab")
    )


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
    assert "transferred and canonical systemd units differ" in installer
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
    volume = launch_pack.render_volume_preparation_block(handoff)
    assert "LOCATION: Windows PowerShell local" in windows
    assert '"$env:USERPROFILE\\.ssh\\hyperlab_hetzner"' in windows
    assert "$RemoteIp = '5.223.60.130'" in windows
    assert "Test-NetConnection" in windows
    assert "sftp.exe" in windows and "scp.exe" in windows
    assert "[System.IO.Path]::GetTempPath()" in windows
    assert "Remove-Item -LiteralPath $SftpBatch" in windows
    assert "Join-Path $ArtifactRoot '.sftp" not in windows
    for directory in ("inventory", "operator", "scripts", "systemd"):
        assert f"'{directory}'" in windows
    assert "H1_WINDOWS_TRANSFER_GREEN_NOT_LAUNCHED" in windows
    assert "H1_ARM_DEADLINE_MISSED" in windows
    assert "#!/usr/bin/env bash" not in windows
    assert "set -Eeuo pipefail" not in windows
    assert "LOCATION: Tabby - VPS" in tabby
    assert 'case "$H1_INCOMING_ROOT" in' in tabby
    assert '"$HOME"/hyperlab-h1/incoming/*)' in tabby
    assert "sha256sum -c launch-files.sha256" in tabby
    assert "timedatectl show --property=NTPSynchronized" in tabby
    assert "df -PB1" in tabby
    assert "--output" not in tabby
    assert "awk 'NR == 2 {gsub(/[[:space:]]/, \"\", $4); print $4}'" in tabby
    assert "findmnt -rn -T" in tabby
    assert "H1_VOLUME_DEVICE='/dev/sdb'" in tabby
    assert "H1_VOLUME_MOUNT='/mnt/HC_Volume_106716684'" in tabby
    assert "H1_ARM_DEADLINE_UTC='2026-08-27T20:30:00Z'" in tabby
    assert "git clone --no-checkout" in tabby
    assert "checkout --detach" in tabby
    assert r"printf '%s  %s\n'" in tabby
    assert "watch -n 10" in tabby
    assert "sudo systemctl stop" in tabby and "sudo systemctl start" in tabby
    assert "Test-NetConnection" not in tabby
    assert "$env:" not in tabby
    assert "LOCATION: Tabby/VPS - unique volume preparation" in volume
    assert "sudo install -d -o hyperlab -g hyperlab -m 0700" in volume
    assert "findmnt -rn -T" in volume
    assert "H1_VOLUME_PREPARATION_GREEN_NO_SERVICE_NO_COLLECTOR" in volume
    assert "--output" not in volume
    assert "awk 'NR == 2 {gsub(/[[:space:]]/, \"\", $4); print $4}'" in volume
    assert "systemctl" not in volume
    assert "h1-collect" not in volume
    assert not any(
        token in volume
        for token in ("mkfs ", "fdisk ", "parted ", "umount ", "resize2fs ", "/etc/fstab")
    )


def test_v6_disable_block_authenticates_inactive_state_and_preserves_all_artifacts() -> None:
    block = launch_pack.render_v6_disable_block()
    assert "LOCATION: Tabby/VPS - preserve and disable failed V6" in block
    assert "fccfe59ab511ec41077926cdc627a39d03c4fe88" in block
    assert "cef9aa76d859496e26b4f01acf37cac56f7ff504f6d742629e2b9232938e391d" in block
    assert "92bce3e8aaf35914fe266d21532615dc6b9bf1929fa43e656308cd3585a1aade" in block
    assert "b79f42b400ebee650430ed6c46df5f70cd55825f8d415031627f9683c0ce98d5" in block
    for expected in (
        "LoadState",
        "ActiveState",
        "SubState",
        "MainPID",
        "NRestarts",
        "ExecMainStatus",
        "PREPARED_NOT_STARTED",
        '"manifest_sha256": None',
        '"raw_root_sha256": None',
        '[[ ! -e "$V6_CAMPAIGN_ROOT/raw" ]]',
    ):
        assert expected in block
    assert 'sudo systemctl disable "$V6_SERVICE"' in block
    assert "H1_V6_SERVICE_DISABLED_PRESERVED_NO_EXECSTART_NO_COLLECTION" in block
    assert "systemctl enable" not in block
    assert "systemctl start" not in block
    assert "rm " not in block and "rm\n" not in block


def test_gnu_df_posix_format_never_combines_p_with_output() -> None:
    sources = [
        (OPS / "launch_pack.py").read_text(encoding="utf-8"),
        (OPS / "vps-install.sh").read_text(encoding="utf-8"),
        launch_pack.render_volume_preparation_block(_handoff()),
        launch_pack.render_tabby_operator_block(_handoff()),
    ]
    df_lines = [line for source in sources for line in source.splitlines() if "df -" in line]
    assert df_lines
    assert all(not ("-P" in line and "--output" in line) for line in df_lines)
    assert all("df -PB1" in line and "print $4" in line for line in df_lines)
