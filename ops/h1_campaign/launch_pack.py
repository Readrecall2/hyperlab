from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Final, NoReturn

BOUNDARY: Final = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
HOME_ROOT: Final = PurePosixPath("/home/hyperlab/hyperlab-h1")
VOLUME_MOUNT: Final = PurePosixPath("/mnt/HC_Volume_106716684")
VOLUME_ROOT: Final = VOLUME_MOUNT / "hyperlab-h1"
EXPECTED_USER: Final = "hyperlab"
SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE: Final = re.compile(r"^[0-9a-f]{40}$")
SLUG_RE: Final = re.compile(r"^h1-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}$")
SERVICE_RE: Final = re.compile(r"^hyperlab-h1-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}\.service$")
FORBIDDEN_ENVIRONMENT: Final = (
    "API_KEY",
    "HYPERLAB_TESTNET_ACCOUNT_ADDRESS",
    "HYPERLAB_TESTNET_API_WALLET_ADDRESS",
    "HYPERLAB_TESTNET_PRIVATE_KEY",
    "MNEMONIC",
    "PRIVATE_KEY",
    "SEED_PHRASE",
    "WALLET_KEY",
)
TERMINAL_HEALTH: Final = frozenset(
    {
        "COMPLETE_COLLECTION_WINDOW",
        "COMPLETE_VERIFIED_THRESHOLDS",
        "FINAL_THRESHOLD_REPLAY_INVALID_FAIL_CLOSED",
        "INTERRUPTED_RECOVERABLE",
        "MAX_BYTES_REACHED",
        "PUBLIC_SOURCE_INVALID_FAIL_CLOSED",
        "PUBLIC_SOURCE_UNAVAILABLE_RECOVERABLE",
        "THRESHOLD_CANDIDATE_NOT_FINAL_RESUME_REQUIRED",
    }
)


class LaunchPackError(ValueError):
    """A launch-pack invariant failed before public collection."""


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path) -> dict[str, Any]:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LaunchPackError(f"invalid JSON artifact: {path}") from error
    if not isinstance(decoded, dict):
        raise LaunchPackError(f"JSON artifact must be an object: {path}")
    return decoded


def _required_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise LaunchPackError(f"{label} must be non-empty text")
    return value


def _required_int(value: object, *, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise LaunchPackError(f"{label} must be a positive integer")
    return value


def _required_sha256(value: object, *, label: str) -> str:
    text = _required_text(value, label=label)
    if SHA256_RE.fullmatch(text) is None:
        raise LaunchPackError(f"{label} must be a lowercase SHA-256")
    return text


def _parse_utc(value: object, *, label: str) -> datetime:
    text = _required_text(value, label=label)
    if not text.endswith("Z"):
        raise LaunchPackError(f"{label} must use a UTC Z suffix")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as error:
        raise LaunchPackError(f"{label} is not ISO-8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LaunchPackError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _remote_parent(category: str) -> PurePosixPath:
    if category == "incoming":
        return HOME_ROOT / category
    if category in {"sources", "campaigns"}:
        return VOLUME_ROOT / category
    raise LaunchPackError(f"unsupported remote path category: {category}")


def validate_remote_path(value: object, *, category: str, slug: str) -> str:
    text = _required_text(value, label=f"remote.{category}_root")
    path = PurePosixPath(text)
    if not path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise LaunchPackError(f"remote {category} path is not canonical and absolute")
    expected_parent = _remote_parent(category)
    if path.parent != expected_parent or path.name != slug:
        raise LaunchPackError(
            f"remote {category} path must be the unique {expected_parent}/{slug} leaf"
        )
    return text


def _path_beneath(path: PurePosixPath, parent: PurePosixPath) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return path != parent


def validate_handoff_remote_path(value: object, *, category: str) -> str:
    text = _required_text(value, label=f"remote.{category}_root")
    path = PurePosixPath(text)
    expected_parent = _remote_parent(category)
    if (
        not path.is_absolute()
        or ".." in path.parts
        or "." in path.parts
        or not _path_beneath(path, expected_parent)
        or path.parent != expected_parent
    ):
        raise LaunchPackError(f"remote {category} path leaves {expected_parent}")
    return text


def validate_plan(plan: Mapping[str, object]) -> dict[str, object]:
    expected = {
        "arm_deadline_utc",
        "boundary",
        "campaign_slug",
        "disk",
        "fee_artifact_path",
        "fee_review_path",
        "fee_reviewed_at_utc",
        "policy_config_path",
        "remote",
        "requirements_lock_path",
        "schema_version",
        "service_name",
        "starts_at_utc",
        "volume",
    }
    if set(plan) != expected or plan.get("schema_version") != 1:
        raise LaunchPackError("launch plan fields or schema version differ from v1")
    if plan.get("boundary") != BOUNDARY:
        raise LaunchPackError("launch boundary differs from the H1 public Ghost boundary")
    slug = _required_text(plan.get("campaign_slug"), label="campaign_slug")
    if SLUG_RE.fullmatch(slug) is None:
        raise LaunchPackError("campaign_slug must include UTC timestamp and an eight-hex nonce")
    service = _required_text(plan.get("service_name"), label="service_name")
    if SERVICE_RE.fullmatch(service) is None or service != f"hyperlab-{slug}.service":
        raise LaunchPackError("service_name is not uniquely derived from campaign_slug")
    reviewed = _parse_utc(plan.get("fee_reviewed_at_utc"), label="fee_reviewed_at_utc")
    starts = _parse_utc(plan.get("starts_at_utc"), label="starts_at_utc")
    deadline = _parse_utc(plan.get("arm_deadline_utc"), label="arm_deadline_utc")
    if reviewed >= starts or starts - reviewed > timedelta(hours=24):
        raise LaunchPackError("fee review must precede start by no more than 24 hours")
    if deadline >= starts or starts - deadline > timedelta(hours=1):
        raise LaunchPackError("arm deadline must be in the final hour before starts_at_utc")
    if starts.minute != 0 or starts.second != 0 or starts.microsecond != 0:
        raise LaunchPackError("starts_at_utc must be rounded to the UTC hour")
    disk = plan.get("disk")
    if not isinstance(disk, dict):
        raise LaunchPackError("disk plan must be an object")
    maximum = _required_int(disk.get("maximum_raw_bytes"), label="maximum_raw_bytes")
    margin = _required_int(disk.get("margin_bytes"), label="margin_bytes")
    required = _required_int(disk.get("required_free_bytes"), label="required_free_bytes")
    staging_max = _required_int(
        disk.get("incoming_staging_max_bytes"), label="incoming_staging_max_bytes"
    )
    if required != maximum + margin:
        raise LaunchPackError("required disk bytes must equal raw ceiling plus explicit margin")
    if staging_max > margin:
        raise LaunchPackError("incoming staging ceiling must remain below the safety margin")
    remote = plan.get("remote")
    if not isinstance(remote, dict) or set(remote) != {
        "campaign_root",
        "home_root",
        "incoming_root",
        "source_root",
        "volume_root",
    }:
        raise LaunchPackError("remote roots differ from the launch-plan schema")
    if remote.get("home_root") != str(HOME_ROOT):
        raise LaunchPackError("remote home root must be /home/hyperlab/hyperlab-h1")
    if remote.get("volume_root") != str(VOLUME_ROOT):
        raise LaunchPackError(f"remote volume root must be {VOLUME_ROOT}")
    for category in ("incoming", "sources", "campaigns"):
        key = {"incoming": "incoming_root", "sources": "source_root", "campaigns": "campaign_root"}[
            category
        ]
        validate_remote_path(remote.get(key), category=category, slug=slug)
    volume = validate_volume_contract(plan.get("volume"))
    if volume["mount_point"] != str(VOLUME_MOUNT):
        raise LaunchPackError(f"volume mount must be {VOLUME_MOUNT}")
    for label in (
        "fee_artifact_path",
        "fee_review_path",
        "policy_config_path",
        "requirements_lock_path",
    ):
        relative = PurePosixPath(_required_text(plan.get(label), label=label))
        if relative.is_absolute() or ".." in relative.parts:
            raise LaunchPackError(f"{label} must be a repository-relative path")
    return dict(plan)


def validate_volume_contract(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != {
        "device",
        "filesystem",
        "model",
        "mount_point",
        "observed_available_bytes",
        "observed_mount_options",
        "serial",
    }:
        raise LaunchPackError("volume contract fields differ")
    if value.get("device") != "/dev/sdb":
        raise LaunchPackError("volume device must be /dev/sdb")
    if value.get("filesystem") != "ext4":
        raise LaunchPackError("volume filesystem must be ext4")
    if value.get("mount_point") != str(VOLUME_MOUNT):
        raise LaunchPackError(f"volume mount point must be {VOLUME_MOUNT}")
    if value.get("model") != "Volume" or value.get("serial") != "106716684":
        raise LaunchPackError("volume model or serial differs from discovery evidence")
    observed = _required_int(
        value.get("observed_available_bytes"), label="observed_available_bytes"
    )
    options = value.get("observed_mount_options")
    if options != ["rw", "relatime", "discard"]:
        raise LaunchPackError("observed volume mount options differ from discovery evidence")
    return {**value, "observed_available_bytes": observed}


def validate_volume_snapshot(
    *,
    contract: Mapping[str, object],
    mount_target: str,
    source_device: str,
    canonical_device: str,
    filesystem: str,
    mount_options: Sequence[str],
    serial: str | None,
    model: str | None,
    available_bytes: int,
    required_free_bytes: int,
) -> dict[str, object]:
    volume = validate_volume_contract(contract)
    if mount_target != volume["mount_point"]:
        raise LaunchPackError(
            f"H1_VOLUME_MOUNT_REFUSED: expected={volume['mount_point']} actual={mount_target}"
        )
    if source_device != volume["device"] or canonical_device != volume["device"]:
        raise LaunchPackError(
            f"H1_VOLUME_DEVICE_REFUSED: expected={volume['device']} "
            f"source={source_device} canonical={canonical_device}"
        )
    if filesystem != volume["filesystem"]:
        raise LaunchPackError(
            f"H1_VOLUME_FILESYSTEM_REFUSED: expected={volume['filesystem']} actual={filesystem}"
        )
    options = frozenset(mount_options)
    if "rw" not in options or "ro" in options:
        raise LaunchPackError("H1_VOLUME_READ_ONLY_REFUSED")
    expected_serial = str(volume["serial"])
    if serial and serial != expected_serial:
        raise LaunchPackError(
            f"H1_VOLUME_SERIAL_REFUSED: expected={expected_serial} actual={serial}"
        )
    expected_model = str(volume["model"])
    if model and model != expected_model:
        raise LaunchPackError(
            f"H1_VOLUME_MODEL_REFUSED: expected={expected_model} actual={model}"
        )
    capacity = assess_capacity(available_bytes, required_free_bytes)
    return {
        "available_bytes": capacity["available_bytes"],
        "canonical_device": canonical_device,
        "filesystem": filesystem,
        "model": model,
        "mount_options": sorted(options),
        "mount_target": mount_target,
        "required_free_bytes": capacity["required_free_bytes"],
        "serial": serial,
        "source_device": source_device,
        "status": "H1_VOLUME_RUNTIME_GREEN",
    }


def assess_capacity(available_bytes: int, required_free_bytes: int) -> dict[str, object]:
    if type(available_bytes) is not int or available_bytes < 0:
        raise LaunchPackError("available disk bytes must be a non-negative integer")
    if type(required_free_bytes) is not int or required_free_bytes <= 0:
        raise LaunchPackError("required disk bytes must be a positive integer")
    if available_bytes < required_free_bytes:
        raise LaunchPackError(
            "H1_DISK_CAPACITY_INSUFFICIENT: "
            f"available={available_bytes} required={required_free_bytes}"
        )
    return {
        "available_bytes": available_bytes,
        "required_free_bytes": required_free_bytes,
        "status": "H1_DISK_CAPACITY_GREEN",
    }


def _canonical_config_sha256(path: Path) -> str:
    return sha256_bytes(canonical_json_bytes(_load_object(path)))


def build_inventory(repo_root: Path, plan: Mapping[str, object]) -> dict[str, object]:
    checked = validate_plan(plan)
    config_path = repo_root / str(checked["policy_config_path"])
    fee_path = repo_root / str(checked["fee_artifact_path"])
    review_path = repo_root / str(checked["fee_review_path"])
    lock_path = repo_root / str(checked["requirements_lock_path"])
    config = _load_object(config_path)
    _load_object(fee_path)
    review = _load_object(review_path)
    config_fee_sha = str(config.get("costs", {}).get("fee_artifact_sha256", ""))
    fee_sha = sha256_file(fee_path)
    if config_fee_sha != fee_sha:
        raise LaunchPackError("H1 config does not bind the reviewed historical fee artifact")
    if review.get("boundary") != BOUNDARY:
        raise LaunchPackError("fee review crosses the H1 safety boundary")
    if review.get("reviewed_at_utc") != checked["fee_reviewed_at_utc"]:
        raise LaunchPackError("launch plan and fee review timestamp differ")
    comparison = review.get("comparison")
    if not isinstance(comparison, dict) or comparison.get("policy_change_required") is not False:
        raise LaunchPackError("fee review does not authorize retaining the H1 fee policy")
    if comparison.get("historical_fee_artifact_sha256") != fee_sha:
        raise LaunchPackError("fee review does not bind the H1 fee artifact bytes")
    extract = review.get("structured_extract")
    if not isinstance(extract, dict):
        raise LaunchPackError("fee review structured extract is absent")
    if sha256_bytes(canonical_json_bytes(extract)) != review.get(
        "structured_extract_canonical_sha256"
    ):
        raise LaunchPackError("fee review structured extract hash diverged")
    perps = extract.get("perps")
    if not isinstance(perps, dict) or perps.get("maker_fee_bps") != "1.5" or perps.get(
        "taker_fee_bps"
    ) != "4.5":
        raise LaunchPackError("official review no longer matches prudent tier-0 perps fees")
    runner = config.get("runner")
    disk = checked["disk"]
    if not isinstance(runner, dict) or not isinstance(disk, dict):
        raise LaunchPackError("H1 runner or disk plan is absent")
    if runner.get("maximum_raw_bytes") != disk.get("maximum_raw_bytes"):
        raise LaunchPackError("disk budget no longer covers the exact H1 raw ceiling")
    return {
        "fee_artifact_sha256": fee_sha,
        "fee_review_sha256": sha256_file(review_path),
        "policy_config_file_sha256": sha256_file(config_path),
        "policy_config_sha256": _canonical_config_sha256(config_path),
        "requirements_lock_sha256": sha256_file(lock_path),
    }


def _git_output(repo_root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _command_output(*arguments: str) -> str:
    completed = subprocess.run(
        list(arguments),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise LaunchPackError(f"command refused or unavailable: {arguments[0]}")
    return completed.stdout.strip()


def _optional_command_output(*arguments: str) -> str | None:
    completed = subprocess.run(
        list(arguments),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        return None
    return completed.stdout.strip()


def _prepare_campaign_seed(
    repo_root: Path,
    plan: Mapping[str, object],
    campaign_root: Path,
) -> dict[str, object]:
    checked = validate_plan(plan)
    source_root = repo_root / "src"
    sys.path.insert(0, str(source_root))
    try:
        from hyperlab.research_data.h1_campaign import prepare_h1_campaign

        original = Path.cwd()
        os.chdir(repo_root)
        try:
            result = prepare_h1_campaign(
                campaign_root,
                config_path=Path(str(checked["policy_config_path"])),
                fee_artifact_path=Path(str(checked["fee_artifact_path"])),
                starts_at_utc=_parse_utc(checked["starts_at_utc"], label="starts_at_utc"),
                fee_reviewed_at_utc=_parse_utc(
                    checked["fee_reviewed_at_utc"], label="fee_reviewed_at_utc"
                ),
            )
        finally:
            os.chdir(original)
    finally:
        with suppress(ValueError):
            sys.path.remove(str(source_root))
    return {
        "campaign_id": result.campaign_id,
        "campaign_manifest_sha256": result.manifest_sha256,
        "policy_config_sha256": result.policy_config_sha256,
    }


def validate_campaign_manifest(
    manifest_path: Path,
    manifest_pin_path: Path,
    plan: Mapping[str, object],
    inventory: Mapping[str, object],
) -> dict[str, Any]:
    raw = manifest_path.read_bytes()
    actual = sha256_bytes(raw)
    pin_parts = manifest_pin_path.read_text(encoding="ascii").split()
    if len(pin_parts) != 2 or pin_parts != [actual, "campaign-manifest.json"]:
        raise LaunchPackError("campaign manifest pin is not exact")
    manifest = _load_object(manifest_path)
    if manifest.get("boundary") != BOUNDARY:
        raise LaunchPackError("campaign manifest boundary differs")
    for key in ("starts_at_utc", "fee_reviewed_at_utc"):
        if manifest.get(key) != plan.get(key):
            raise LaunchPackError(f"campaign manifest {key} differs from launch plan")
    if manifest.get("policy_config_sha256") != inventory.get("policy_config_sha256"):
        raise LaunchPackError("campaign manifest policy hash differs")
    if manifest.get("fee_artifact_sha256") != inventory.get("fee_artifact_sha256"):
        raise LaunchPackError("campaign manifest fee hash differs")
    collection = manifest.get("collection")
    holdout = manifest.get("holdout")
    if (
        not isinstance(collection, dict)
        or collection.get("minimum_days") != 7
        or collection.get("maximum_days") != 14
        or not isinstance(holdout, dict)
        or holdout.get("access") != "SEALED_UNTIL_COLLECTION_COMPLETE"
    ):
        raise LaunchPackError("campaign manifest changed the H1 window or sealed holdout")
    return manifest


def _write_exclusive(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(value)


def _handoff_body(
    *,
    plan: Mapping[str, object],
    inventory: Mapping[str, object],
    source_commit: str,
    bundle_path: Path,
    manifest_path: Path,
    manifest_pin_path: Path,
    health_path: Path,
    created_at: datetime,
    repo_root: Path,
) -> dict[str, object]:
    manifest = validate_campaign_manifest(manifest_path, manifest_pin_path, plan, inventory)
    return {
        "boundary": BOUNDARY,
        "bundle": {
            "filename": bundle_path.name,
            "ref": "refs/heads/codex/h1-prospective-campaign-launch-v1",
            "sha256": sha256_file(bundle_path),
        },
        "campaign_id": manifest["campaign_id"],
        "campaign_slug": plan["campaign_slug"],
        "created_at_utc": _utc_text(created_at),
        "disk": plan["disk"],
        "fee_reviewed_at_utc": plan["fee_reviewed_at_utc"],
        "files": {
            "campaign_health_sha256": sha256_file(health_path),
            "campaign_manifest_pin_sha256": sha256_file(manifest_pin_path),
            "campaign_manifest_sha256": sha256_file(manifest_path),
        },
        "inventory": dict(inventory),
        "launch_plan_path": "ops/h1_campaign/launch-plan-v1.json",
        "launch_plan_sha256": sha256_file(repo_root / "ops/h1_campaign/launch-plan-v1.json"),
        "remote": plan["remote"],
        "schema_version": 1,
        "service_name": plan["service_name"],
        "source_commit": source_commit,
        "starts_at_utc": plan["starts_at_utc"],
        "arm_deadline_utc": plan["arm_deadline_utc"],
        "volume": plan["volume"],
    }


def render_volume_preparation_block(handoff: Mapping[str, object]) -> str:
    checked = validate_handoff(handoff)
    remote = checked["remote"]
    disk = checked["disk"]
    volume = checked["volume"]
    assert isinstance(remote, dict) and isinstance(disk, dict) and isinstance(volume, dict)
    return rf"""# LOCATION: Tabby/VPS - unique volume preparation, Bash, logged in as hyperlab.
# EXPECTED_DURATION: 1-3 minutes; MAXIMUM_DURATION: 10 minutes.
# PROMPTS: sudo may request the hyperlab password once; no HyperLab prompt and no network collection.
# MONITORING: read the fail-closed checks printed here; no second tab is required for this short step.
# CTRL+C: stops only this directory-preparation step; no service or collector is started.
# TERMINAL_SIGNAL: H1_VOLUME_PREPARATION_GREEN_NO_SERVICE_NO_COLLECTOR.
set -Eeuo pipefail
umask 077

fail() {{ printf 'H1_VOLUME_PREPARATION_REFUSED:%s\n' "$1" >&2; exit 4; }}

H1_VOLUME_DEVICE='{volume['device']}'
H1_VOLUME_MOUNT='{volume['mount_point']}'
H1_VOLUME_ROOT='{remote['volume_root']}'
H1_SOURCE_ROOT='{remote['source_root']}'
H1_CAMPAIGN_ROOT='{remote['campaign_root']}'
H1_REQUIRED_FREE_BYTES='{disk['required_free_bytes']}'
H1_EXPECTED_FS='{volume['filesystem']}'
H1_EXPECTED_MODEL='{volume['model']}'
H1_EXPECTED_SERIAL='{volume['serial']}'

[[ $(id -un) == hyperlab && $HOME == /home/hyperlab ]] || fail 'run as hyperlab with exact HOME'
for command_name in readlink findmnt df awk stat sudo install; do
  command -v "$command_name" >/dev/null 2>&1 || fail "missing command: $command_name"
done
[[ -b "$H1_VOLUME_DEVICE" ]] || fail 'expected block device is absent'
[[ $(readlink -f -- "$H1_VOLUME_DEVICE") == "$H1_VOLUME_DEVICE" ]] || fail 'device real path differs'
[[ -d "$H1_VOLUME_MOUNT" && ! -L "$H1_VOLUME_MOUNT" ]] || fail 'exact volume mount path is absent or a symlink'
[[ $(readlink -f -- "$H1_VOLUME_MOUNT") == "$H1_VOLUME_MOUNT" ]] || fail 'volume mount real path differs'

H1_FOUND_TARGET=$(findmnt -rn -T "$H1_VOLUME_MOUNT" -o TARGET)
H1_FOUND_SOURCE=$(findmnt -rn -T "$H1_VOLUME_MOUNT" -o SOURCE)
H1_FOUND_FS=$(findmnt -rn -T "$H1_VOLUME_MOUNT" -o FSTYPE)
H1_FOUND_OPTIONS=$(findmnt -rn -T "$H1_VOLUME_MOUNT" -o OPTIONS)
[[ $H1_FOUND_TARGET == "$H1_VOLUME_MOUNT" ]] || fail "mount target differs: $H1_FOUND_TARGET"
[[ $H1_FOUND_SOURCE == "$H1_VOLUME_DEVICE" ]] || fail "mount device differs: $H1_FOUND_SOURCE"
[[ $H1_FOUND_FS == "$H1_EXPECTED_FS" ]] || fail "filesystem differs: $H1_FOUND_FS"
case ",$H1_FOUND_OPTIONS," in *,rw,*) ;; *) fail "volume is not rw: $H1_FOUND_OPTIONS" ;; esac
case ",$H1_FOUND_OPTIONS," in *,ro,*) fail "volume is read-only: $H1_FOUND_OPTIONS" ;; esac

H1_FOUND_SERIAL=''
H1_FOUND_MODEL=''
if command -v lsblk >/dev/null 2>&1; then
  H1_FOUND_SERIAL=$(lsblk -dn -o SERIAL "$H1_VOLUME_DEVICE" 2>/dev/null | awk '{{gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print}}') || H1_FOUND_SERIAL=''
  H1_FOUND_MODEL=$(lsblk -dn -o MODEL "$H1_VOLUME_DEVICE" 2>/dev/null | awk '{{gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print}}') || H1_FOUND_MODEL=''
fi
[[ -z $H1_FOUND_SERIAL || $H1_FOUND_SERIAL == "$H1_EXPECTED_SERIAL" ]] \
  || fail "stable serial differs: $H1_FOUND_SERIAL"
[[ -z $H1_FOUND_MODEL || $H1_FOUND_MODEL == "$H1_EXPECTED_MODEL" ]] \
  || fail "stable model differs: $H1_FOUND_MODEL"

H1_AVAILABLE_BYTES=$(df -PB1 --output=avail "$H1_VOLUME_MOUNT" | awk 'NR == 2 {{gsub(/[[:space:]]/, ""); print}}')
[[ $H1_AVAILABLE_BYTES =~ ^[0-9]+$ ]] || fail 'cannot measure volume free bytes'
(( H1_AVAILABLE_BYTES >= H1_REQUIRED_FREE_BYTES )) \
  || fail "H1_DISK_CAPACITY_INSUFFICIENT available=$H1_AVAILABLE_BYTES required=$H1_REQUIRED_FREE_BYTES"
[[ $(readlink -m -- "$H1_VOLUME_ROOT") == "$H1_VOLUME_ROOT" ]] || fail 'volume base root is not canonical'
[[ ! -e "$H1_SOURCE_ROOT" && ! -e "$H1_CAMPAIGN_ROOT" ]] \
  || fail 'new source or campaign leaf already exists'
for path in "$H1_VOLUME_ROOT" "$H1_VOLUME_ROOT/sources" "$H1_VOLUME_ROOT/campaigns"; do
  [[ ! -e "$path" || ! -L "$path" ]] || fail "symlink forbidden: $path"
done

sudo install -d -o hyperlab -g hyperlab -m 0700 \
  "$H1_VOLUME_ROOT" "$H1_VOLUME_ROOT/sources" "$H1_VOLUME_ROOT/campaigns"
for path in "$H1_VOLUME_ROOT" "$H1_VOLUME_ROOT/sources" "$H1_VOLUME_ROOT/campaigns"; do
  [[ $(readlink -f -- "$path") == "$path" ]] || fail "real path differs: $path"
  [[ $(stat -c '%U:%G:%a' "$path") == 'hyperlab:hyperlab:700' ]] \
    || fail "ownership or mode differs: $path"
done

printf 'H1_VOLUME_DEVICE=%s\n' "$H1_FOUND_SOURCE"
printf 'H1_VOLUME_FILESYSTEM=%s\n' "$H1_FOUND_FS"
printf 'H1_VOLUME_AVAILABLE_BYTES=%s\n' "$H1_AVAILABLE_BYTES"
printf 'H1_VOLUME_PREPARATION_GREEN_NO_SERVICE_NO_COLLECTOR\n'
"""


def render_windows_operator_block(
    handoff: Mapping[str, object], *, output_root: Path, repo_root: Path
) -> str:
    checked = validate_handoff(handoff)
    bundle = checked["bundle"]
    files = checked["files"]
    remote = checked["remote"]
    assert isinstance(bundle, dict) and isinstance(files, dict) and isinstance(remote, dict)
    artifact = str(output_root)
    worktree = str(repo_root)
    slug = str(checked["campaign_slug"])
    bundle_name = str(bundle["filename"])
    return rf"""# LOCATION: Windows PowerShell local, from {worktree}
# EXPECTED_DURATION: 5-15 minutes; MAXIMUM_DURATION: 30 minutes.
# PROMPTS: first SSH host-key trust and SSH-key passphrase may prompt; HyperLab has no prompt.
# MONITORING: Get-FileHash locally, Test-NetConnection, then SFTP/SCP exit codes.
# CTRL+C: stops only the local transfer; a partial remote incoming root is abandoned, never reused.
# TERMINAL_SIGNAL: H1_WINDOWS_TRANSFER_GREEN_NOT_LAUNCHED (no collector or service started).
$ErrorActionPreference = 'Stop'
$Commit = '{checked['source_commit']}'
$Worktree = '{worktree}'
$ArtifactRoot = '{artifact}'
$SshKey = "$env:USERPROFILE\.ssh\hyperlab_hetzner"
$RemoteUser = 'hyperlab'
$RemoteIp = '5.223.60.130'
$CampaignSlug = '{slug}'
$ArmDeadlineUtc = [DateTimeOffset]::Parse('{checked['arm_deadline_utc']}')
$RemoteIncomingRelative = "hyperlab-h1/incoming/$CampaignSlug"
$BundlePath = Join-Path $ArtifactRoot '{bundle_name}'
$HandoffPath = Join-Path $ArtifactRoot 'handoff.json'
$ManifestPath = Join-Path $ArtifactRoot 'campaign-seed\campaign-manifest.json'

function Assert-Sha256 {{
    param([string] $Path, [string] $Expected)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {{ throw "Missing file: $Path" }}
    $Actual = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($Actual -ne $Expected) {{ throw "SHA-256 mismatch: $Path expected=$Expected actual=$Actual" }}
}}

if ((git -C $Worktree rev-parse HEAD) -ne $Commit) {{ throw 'Local HEAD differs.' }}
if (git -C $Worktree status --porcelain) {{ throw 'Launch worktree is not clean.' }}
if ([DateTimeOffset]::UtcNow -gt $ArmDeadlineUtc) {{ throw 'H1_ARM_DEADLINE_MISSED' }}
Assert-Sha256 $BundlePath '{bundle['sha256']}'
Assert-Sha256 $HandoffPath '{sha256_bytes(canonical_json_bytes(checked))}'
Assert-Sha256 $ManifestPath '{files['campaign_manifest_sha256']}'
$LaunchFilesPath = Join-Path $ArtifactRoot 'launch-files.sha256'
Get-Content -LiteralPath $LaunchFilesPath | ForEach-Object {{
    $Parts = $_ -split '  ', 2
    if ($Parts.Count -ne 2) {{ throw "Invalid launch-files entry: $_" }}
    Assert-Sha256 (Join-Path $ArtifactRoot $Parts[1]) $Parts[0]
}}
if (-not (Test-Path -LiteralPath $SshKey -PathType Leaf)) {{ throw "SSH key is absent: $SshKey" }}
$Reachability = Test-NetConnection -ComputerName $RemoteIp -Port 22 -InformationLevel Detailed
if (-not $Reachability.TcpTestSucceeded) {{ throw 'TCP/22 is not reachable.' }}

$SftpBatch = Join-Path ([System.IO.Path]::GetTempPath()) "h1-sftp-$CampaignSlug-$PID.txt"
if (Test-Path -LiteralPath $SftpBatch) {{ throw "Temporary SFTP batch already exists: $SftpBatch" }}
try {{
    @(
        '-mkdir hyperlab-h1'
        '-mkdir hyperlab-h1/incoming'
        "mkdir $RemoteIncomingRelative"
    ) | Set-Content -LiteralPath $SftpBatch -Encoding ascii
    & sftp.exe -i $SshKey -b $SftpBatch "$RemoteUser@$RemoteIp"
    if ($LASTEXITCODE -ne 0) {{ throw 'SFTP unique incoming-root creation failed.' }}
}}
finally {{
    if (Test-Path -LiteralPath $SftpBatch) {{ Remove-Item -LiteralPath $SftpBatch -Force }}
}}

$RemoteTarget = "${{RemoteUser}}@${{RemoteIp}}:${{RemoteIncomingRelative}}/"
& scp.exe -i $SshKey $BundlePath $HandoffPath `
    (Join-Path $ArtifactRoot 'handoff.sha256') `
    (Join-Path $ArtifactRoot 'launch-files.sha256') `
    $RemoteTarget
if ($LASTEXITCODE -ne 0) {{ throw 'SCP file transfer failed.' }}
& scp.exe -i $SshKey -r `
    (Join-Path $ArtifactRoot 'campaign-seed') `
    (Join-Path $ArtifactRoot 'inventory') `
    (Join-Path $ArtifactRoot 'operator') `
    (Join-Path $ArtifactRoot 'scripts') `
    (Join-Path $ArtifactRoot 'systemd') `
    $RemoteTarget
if ($LASTEXITCODE -ne 0) {{ throw 'SCP directory transfer failed.' }}
Write-Output 'H1_WINDOWS_TRANSFER_GREEN_NOT_LAUNCHED'
"""


def render_tabby_operator_block(handoff: Mapping[str, object]) -> str:
    checked = validate_handoff(handoff)
    bundle = checked["bundle"]
    files = checked["files"]
    remote = checked["remote"]
    disk = checked["disk"]
    volume = checked["volume"]
    assert (
        isinstance(bundle, dict)
        and isinstance(files, dict)
        and isinstance(remote, dict)
        and isinstance(disk, dict)
        and isinstance(volume, dict)
    )
    source = str(remote["source_root"])
    incoming = str(remote["incoming_root"])
    campaign = str(remote["campaign_root"])
    service = str(checked["service_name"])
    bundle_name = str(bundle["filename"])
    return rf"""# LOCATION: Tabby - VPS, Bash, logged in as hyperlab.
# EXPECTED_DURATION: 10-25 minutes to arm; MAXIMUM_DURATION: 45 minutes.
# PROMPTS: sudo may request the hyperlab password; pip is non-interactive and bounded.
# MONITORING: command output, then the exact second-Tabby watch command printed at the end.
# CTRL+C: stops this foreground block only; after enable, systemd persists and must be inspected separately.
# TERMINAL_SIGNAL: H1_SERVICE_ARMED_OR_RUNNING_GREEN plus the exact H1_* identity lines.
set -Eeuo pipefail
umask 077

H1_COMMIT='{checked['source_commit']}'
H1_INCOMING_ROOT='{incoming}'
H1_SOURCE_ROOT='{source}'
H1_CAMPAIGN_ROOT='{campaign}'
H1_SERVICE='{service}'
H1_BUNDLE="$H1_INCOMING_ROOT/{bundle_name}"
H1_REQUIRED_FREE_BYTES='{disk['required_free_bytes']}'
H1_INCOMING_STAGING_MAX_BYTES='{disk['incoming_staging_max_bytes']}'
H1_ARM_DEADLINE_UTC='{checked['arm_deadline_utc']}'
H1_VOLUME_DEVICE='{volume['device']}'
H1_VOLUME_MOUNT='{volume['mount_point']}'
H1_VOLUME_ROOT='{remote['volume_root']}'
H1_VOLUME_FS='{volume['filesystem']}'
H1_VOLUME_MODEL='{volume['model']}'
H1_VOLUME_SERIAL='{volume['serial']}'

[[ $(id -un) == hyperlab && $HOME == /home/hyperlab ]]
case "$H1_INCOMING_ROOT" in
  "$HOME"/hyperlab-h1/incoming/*) ;;
  *) printf 'H1_INCOMING_PATH_REFUSED:%s\n' "$H1_INCOMING_ROOT" >&2; exit 4 ;;
esac
[[ $(readlink -f -- "$H1_INCOMING_ROOT") == "$H1_INCOMING_ROOT" ]]
chmod 0700 "$HOME/hyperlab-h1" "$HOME/hyperlab-h1/incoming" "$H1_INCOMING_ROOT"
cd "$H1_INCOMING_ROOT"
printf '%s  %s\n' '{bundle['sha256']}' '{bundle_name}' | sha256sum -c -
printf '%s  %s\n' '{sha256_bytes(canonical_json_bytes(checked))}' 'handoff.json' | sha256sum -c -
printf '%s  %s\n' '{files['campaign_manifest_sha256']}' 'campaign-seed/campaign-manifest.json' | sha256sum -c -
sha256sum -c launch-files.sha256
[[ -z $(find "$H1_INCOMING_ROOT" -type l -print -quit) ]]
[[ ! -d "$H1_INCOMING_ROOT/raw" ]]
[[ -z $(find "$H1_INCOMING_ROOT" -type f -name '*.rdpseg' -print -quit) ]]
INCOMING_BYTES=$(du -sb -- "$H1_INCOMING_ROOT" | awk '{{print $1}}')
[[ $INCOMING_BYTES =~ ^[0-9]+$ ]]
(( INCOMING_BYTES <= H1_INCOMING_STAGING_MAX_BYTES ))

[[ $(timedatectl show --property=NTPSynchronized --value) == yes ]]
[[ $(date -u +%Z) == UTC ]]
ARM_DEADLINE_EPOCH=$(date -u -d "$H1_ARM_DEADLINE_UTC" +%s)
(( $(date -u +%s) <= ARM_DEADLINE_EPOCH )) || {{ printf 'H1_ARM_DEADLINE_MISSED\n' >&2; exit 4; }}
[[ -b "$H1_VOLUME_DEVICE" ]]
[[ $(readlink -f -- "$H1_VOLUME_DEVICE") == "$H1_VOLUME_DEVICE" ]]
[[ -d "$H1_VOLUME_MOUNT" && ! -L "$H1_VOLUME_MOUNT" ]]
[[ $(readlink -f -- "$H1_VOLUME_MOUNT") == "$H1_VOLUME_MOUNT" ]]
FOUND_TARGET=$(findmnt -rn -T "$H1_VOLUME_MOUNT" -o TARGET)
FOUND_SOURCE=$(findmnt -rn -T "$H1_VOLUME_MOUNT" -o SOURCE)
FOUND_FS=$(findmnt -rn -T "$H1_VOLUME_MOUNT" -o FSTYPE)
FOUND_OPTIONS=$(findmnt -rn -T "$H1_VOLUME_MOUNT" -o OPTIONS)
[[ $FOUND_TARGET == "$H1_VOLUME_MOUNT" ]]
[[ $FOUND_SOURCE == "$H1_VOLUME_DEVICE" ]]
[[ $FOUND_FS == "$H1_VOLUME_FS" ]]
case ",$FOUND_OPTIONS," in *,rw,*) ;; *) printf 'H1_VOLUME_READ_ONLY_REFUSED\n' >&2; exit 4 ;; esac
case ",$FOUND_OPTIONS," in *,ro,*) printf 'H1_VOLUME_READ_ONLY_REFUSED\n' >&2; exit 4 ;; esac
FOUND_SERIAL=''
FOUND_MODEL=''
if command -v lsblk >/dev/null 2>&1; then
  FOUND_SERIAL=$(lsblk -dn -o SERIAL "$H1_VOLUME_DEVICE" 2>/dev/null | awk '{{gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print}}') || FOUND_SERIAL=''
  FOUND_MODEL=$(lsblk -dn -o MODEL "$H1_VOLUME_DEVICE" 2>/dev/null | awk '{{gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print}}') || FOUND_MODEL=''
fi
[[ -z $FOUND_SERIAL || $FOUND_SERIAL == "$H1_VOLUME_SERIAL" ]]
[[ -z $FOUND_MODEL || $FOUND_MODEL == "$H1_VOLUME_MODEL" ]]
for path in "$H1_VOLUME_ROOT" "$H1_VOLUME_ROOT/sources" "$H1_VOLUME_ROOT/campaigns"; do
  [[ -d "$path" && ! -L "$path" ]]
  [[ $(readlink -f -- "$path") == "$path" ]]
  [[ $(stat -c '%U:%G:%a' "$path") == 'hyperlab:hyperlab:700' ]]
done
[[ $(readlink -m -- "$H1_SOURCE_ROOT") == "$H1_SOURCE_ROOT" ]]
[[ $(readlink -m -- "$H1_CAMPAIGN_ROOT") == "$H1_CAMPAIGN_ROOT" ]]
AVAILABLE_BYTES=$(df -PB1 --output=avail "$H1_VOLUME_MOUNT" | awk 'NR == 2 {{gsub(/[[:space:]]/, ""); print}}')
[[ $AVAILABLE_BYTES =~ ^[0-9]+$ ]]
(( AVAILABLE_BYTES >= H1_REQUIRED_FREE_BYTES )) || {{
  printf 'H1_DISK_CAPACITY_INSUFFICIENT available=%s required=%s\n' \
    "$AVAILABLE_BYTES" "$H1_REQUIRED_FREE_BYTES" >&2
  exit 4
}}
[[ ! -e "$H1_SOURCE_ROOT" && ! -e "$H1_CAMPAIGN_ROOT" ]]
git clone --no-checkout "$H1_BUNDLE" "$H1_SOURCE_ROOT"
chmod 0700 "$H1_SOURCE_ROOT"
[[ $(readlink -f -- "$H1_SOURCE_ROOT") == "$H1_SOURCE_ROOT" ]]
git -C "$H1_SOURCE_ROOT" checkout --detach "$H1_COMMIT"
[[ $(git -C "$H1_SOURCE_ROOT" rev-parse HEAD) == "$H1_COMMIT" ]]
[[ -z $(git -C "$H1_SOURCE_ROOT" status --porcelain) ]]
cd "$H1_SOURCE_ROOT"
bash ops/h1_campaign/vps-install.sh "$H1_INCOMING_ROOT"

# POINT STATUS (read-only):
# bash '{source}/ops/h1_campaign/monitor.sh' '{incoming}/handoff.json'
# SECOND TABBY TAB (continuous, read-only):
# watch -n 10 -- bash '{source}/ops/h1_campaign/monitor.sh' '{incoming}/handoff.json'
# GRACEFUL STOP (SIGINT -> authenticated tail -> INTERRUPTED_RECOVERABLE):
# sudo systemctl stop '{service}'
# RESUME THE SAME CAMPAIGN MANIFEST AND RAW CHAIN:
# sudo systemctl start '{service}'
"""


def finalize_launch_pack(
    *,
    repo_root: Path,
    plan_path: Path,
    bundle_path: Path,
    output_root: Path,
    source_commit: str,
    created_at: datetime | None = None,
) -> dict[str, object]:
    if COMMIT_RE.fullmatch(source_commit) is None:
        raise LaunchPackError("source commit must be a full lowercase Git SHA")
    if _git_output(repo_root, "rev-parse", "HEAD") != source_commit:
        raise LaunchPackError("source commit differs from local HEAD")
    if _git_output(repo_root, "status", "--porcelain"):
        raise LaunchPackError("launch source worktree must be clean")
    plan = validate_plan(_load_object(plan_path))
    inventory = build_inventory(repo_root, plan)
    created = (created_at or datetime.now(tz=UTC)).astimezone(UTC)
    starts = _parse_utc(plan["starts_at_utc"], label="starts_at_utc")
    lead = starts - created
    if not timedelta(hours=18) <= lead <= timedelta(hours=22):
        raise LaunchPackError(
            "final package must be created 18-22 hours before starts_at_utc; "
            f"actual_seconds={int(lead.total_seconds())}"
        )
    if not bundle_path.is_file():
        raise LaunchPackError("Git bundle is absent")
    expected_ref = f"{source_commit} refs/heads/codex/h1-prospective-campaign-launch-v1"
    if expected_ref not in _git_output(repo_root, "bundle", "list-heads", str(bundle_path)).splitlines():
        raise LaunchPackError("Git bundle does not expose the exact target branch and commit")
    output_root.mkdir(parents=True, exist_ok=True)
    seed_root = output_root / "campaign-seed"
    if seed_root.exists():
        raise FileExistsError("campaign seed root must be new")
    prepared = _prepare_campaign_seed(repo_root, plan, seed_root)
    generated_operator_root = seed_root / "operator"
    generated_operator_files = {
        generated_operator_root / "tabby-vps-bash.txt",
        generated_operator_root / "windows-powershell.txt",
    }
    if {path for path in generated_operator_root.iterdir()} != generated_operator_files:
        raise LaunchPackError("canonical H1 preparation emitted unexpected operator files")
    for path in sorted(generated_operator_files):
        if path.is_symlink() or not path.is_file():
            raise LaunchPackError("canonical H1 preparation operator artifact is unsafe")
        path.unlink()
    generated_operator_root.rmdir()
    manifest_path = seed_root / "campaign-manifest.json"
    pin_path = seed_root / "campaign-manifest.sha256"
    health_path = seed_root / "state" / "health.json"
    script_names = (
        "bootstrap-linux.sh",
        "launch_pack.py",
        "monitor.sh",
        "run_collector.sh",
        "vps-install.sh",
    )
    source_artifacts = {
        f"ops/h1_campaign/{name}": sha256_file(repo_root / "ops/h1_campaign" / name)
        for name in script_names
    }
    source_inventory = {
        "boundary": BOUNDARY,
        "inventory": inventory,
        "launch_plan_path": "ops/h1_campaign/launch-plan-v1.json",
        "launch_plan_sha256": sha256_file(repo_root / "ops/h1_campaign/launch-plan-v1.json"),
        "source_artifacts": source_artifacts,
        "source_commit": source_commit,
    }
    inventory_path = output_root / "inventory" / "source-policy-readiness.json"
    _write_exclusive(inventory_path, canonical_json_bytes(source_inventory))
    scripts_root = output_root / "scripts"
    for name in script_names:
        _write_exclusive(
            scripts_root / name,
            (repo_root / "ops/h1_campaign" / name).read_bytes(),
        )
    handoff = _handoff_body(
        plan=plan,
        inventory=inventory,
        source_commit=source_commit,
        bundle_path=bundle_path,
        manifest_path=manifest_path,
        manifest_pin_path=pin_path,
        health_path=health_path,
        created_at=created,
        repo_root=repo_root,
    )
    handoff_files = handoff["files"]
    assert isinstance(handoff_files, dict)
    handoff_files["source_inventory_sha256"] = sha256_file(inventory_path)
    handoff_files["systemd_unit_sha256"] = "0" * 64
    systemd_path = output_root / "systemd" / str(handoff["service_name"])
    _write_exclusive(systemd_path, render_systemd_unit(handoff).encode("utf-8"))
    handoff_files["systemd_unit_sha256"] = sha256_file(systemd_path)
    validate_handoff(handoff)
    if handoff["campaign_id"] != prepared["campaign_id"]:
        raise LaunchPackError("prepared campaign identity changed during finalization")
    handoff_path = output_root / "handoff.json"
    _write_exclusive(handoff_path, canonical_json_bytes(handoff))
    handoff_sha = sha256_file(handoff_path)
    _write_exclusive(
        output_root / "handoff.sha256",
        f"{handoff_sha}  handoff.json\n".encode("ascii"),
    )
    operator_root = output_root / "operator"
    windows_path = operator_root / "windows-powershell.txt"
    tabby_path = operator_root / "tabby-vps-bash.txt"
    volume_path = operator_root / "tabby-vps-volume-preparation.txt"
    _write_exclusive(
        windows_path,
        render_windows_operator_block(handoff, output_root=output_root, repo_root=repo_root).encode(
            "utf-8"
        ),
    )
    _write_exclusive(tabby_path, render_tabby_operator_block(handoff).encode("utf-8"))
    _write_exclusive(volume_path, render_volume_preparation_block(handoff).encode("utf-8"))
    launch_files = {
        bundle_path.name: sha256_file(bundle_path),
        "campaign-seed/campaign-manifest.json": sha256_file(manifest_path),
        "campaign-seed/campaign-manifest.sha256": sha256_file(pin_path),
        "campaign-seed/state/health.json": sha256_file(health_path),
        "handoff.json": handoff_sha,
        "handoff.sha256": sha256_file(output_root / "handoff.sha256"),
        "inventory/source-policy-readiness.json": sha256_file(inventory_path),
        "operator/tabby-vps-bash.txt": sha256_file(tabby_path),
        "operator/tabby-vps-volume-preparation.txt": sha256_file(volume_path),
        "operator/windows-powershell.txt": sha256_file(windows_path),
        f"systemd/{handoff['service_name']}": sha256_file(systemd_path),
    }
    for name in script_names:
        launch_files[f"scripts/{name}"] = sha256_file(scripts_root / name)
    lines = [f"{digest}  {name}" for name, digest in sorted(launch_files.items())]
    _write_exclusive(
        output_root / "launch-files.sha256",
        ("\n".join(lines) + "\n").encode("ascii"),
    )
    result = {
        **handoff,
        "handoff_sha256": handoff_sha,
        "launch_files_sha256": sha256_file(output_root / "launch-files.sha256"),
        "output_root": str(output_root),
        "status": "H1_LAUNCH_PACK_FINALIZED_NOT_STARTED",
    }
    return result


def validate_handoff(handoff: Mapping[str, object]) -> dict[str, object]:
    if handoff.get("schema_version") != 1 or handoff.get("boundary") != BOUNDARY:
        raise LaunchPackError("handoff schema or boundary differs")
    commit = _required_text(handoff.get("source_commit"), label="source_commit")
    if COMMIT_RE.fullmatch(commit) is None:
        raise LaunchPackError("handoff source commit is invalid")
    slug = _required_text(handoff.get("campaign_slug"), label="campaign_slug")
    if SLUG_RE.fullmatch(slug) is None:
        raise LaunchPackError("handoff campaign slug is invalid")
    service = _required_text(handoff.get("service_name"), label="service_name")
    if service != f"hyperlab-{slug}.service":
        raise LaunchPackError("handoff service is not derived from campaign slug")
    remote = handoff.get("remote")
    if (
        not isinstance(remote, dict)
        or remote.get("home_root") != str(HOME_ROOT)
        or remote.get("volume_root") != str(VOLUME_ROOT)
    ):
        raise LaunchPackError("handoff remote roots are absent")
    for category, key in (
        ("incoming", "incoming_root"),
        ("sources", "source_root"),
        ("campaigns", "campaign_root"),
    ):
        path = validate_handoff_remote_path(remote.get(key), category=category)
        if PurePosixPath(path).name != slug:
            raise LaunchPackError(f"handoff {category} root does not use campaign slug")
    disk = handoff.get("disk")
    if not isinstance(disk, dict):
        raise LaunchPackError("handoff disk contract is absent")
    maximum = _required_int(disk.get("maximum_raw_bytes"), label="maximum_raw_bytes")
    margin = _required_int(disk.get("margin_bytes"), label="margin_bytes")
    required = _required_int(disk.get("required_free_bytes"), label="required_free_bytes")
    staging_max = _required_int(
        disk.get("incoming_staging_max_bytes"), label="incoming_staging_max_bytes"
    )
    if required != maximum + margin:
        raise LaunchPackError("handoff disk budget is incoherent")
    if staging_max > margin:
        raise LaunchPackError("handoff incoming staging ceiling is unsafe")
    volume = validate_volume_contract(handoff.get("volume"))
    observed_available = _required_int(
        volume.get("observed_available_bytes"), label="observed_available_bytes"
    )
    if observed_available < required:
        raise LaunchPackError("discovered volume capacity does not cover the frozen H1 budget")
    reviewed = _parse_utc(handoff.get("fee_reviewed_at_utc"), label="fee_reviewed_at_utc")
    starts = _parse_utc(handoff.get("starts_at_utc"), label="starts_at_utc")
    if reviewed >= starts or starts - reviewed > timedelta(hours=24):
        raise LaunchPackError("handoff fee review is stale at campaign start")
    bundle = handoff.get("bundle")
    inventory = handoff.get("inventory")
    files = handoff.get("files")
    if not isinstance(bundle, dict) or not isinstance(inventory, dict) or not isinstance(files, dict):
        raise LaunchPackError("handoff hashes are absent")
    for label, value in (
        ("bundle.sha256", bundle.get("sha256")),
        ("fee_artifact_sha256", inventory.get("fee_artifact_sha256")),
        ("fee_review_sha256", inventory.get("fee_review_sha256")),
        ("policy_config_sha256", inventory.get("policy_config_sha256")),
        ("requirements_lock_sha256", inventory.get("requirements_lock_sha256")),
        ("campaign_manifest_sha256", files.get("campaign_manifest_sha256")),
        ("source_inventory_sha256", files.get("source_inventory_sha256")),
        ("systemd_unit_sha256", files.get("systemd_unit_sha256")),
    ):
        _required_sha256(value, label=label)
    return dict(handoff)


def render_systemd_unit(handoff: Mapping[str, object]) -> str:
    checked = validate_handoff(handoff)
    remote = checked["remote"]
    assert isinstance(remote, dict)
    source = str(remote["source_root"])
    incoming = str(remote["incoming_root"])
    campaign = str(remote["campaign_root"])
    service = str(checked["service_name"])
    return f"""[Unit]
Description=HyperLab H1 prospective public Ghost campaign {checked['campaign_slug']}
Documentation=file:{source}/docs/H1_PROSPECTIVE_CAMPAIGN_LAUNCH_PACK_V1.md
Wants=network-online.target
After=network-online.target time-sync.target
RequiresMountsFor={VOLUME_MOUNT}
ConditionPathIsMountPoint={VOLUME_MOUNT}
StartLimitIntervalSec=1800
StartLimitBurst=3

[Service]
Type=simple
User=hyperlab
Group=hyperlab
WorkingDirectory={source}
Environment=HOME=/home/hyperlab
Environment=PYTHONPATH={source}/src
Environment=PYTHONNOUSERSITE=1
Environment=PYTHONDONTWRITEBYTECODE=1
Environment=PYTHONUNBUFFERED=1
Environment=TZ=UTC
UnsetEnvironment={' '.join(FORBIDDEN_ENVIRONMENT)}
ExecCondition={source}/.venv/bin/python {source}/ops/h1_campaign/launch_pack.py vps-preflight --handoff {incoming}/handoff.json --mode start
ExecStart=/usr/bin/bash {source}/ops/h1_campaign/run_collector.sh {incoming}/handoff.json
Restart=on-failure
RestartSec=60
SuccessExitStatus=130
TimeoutStartSec=infinity
TimeoutStopSec=180
KillSignal=SIGINT
KillMode=mixed
SendSIGKILL=no
UMask=0077
NoNewPrivileges=yes
PrivateDevices=yes
PrivateTmp=yes
ProtectClock=yes
ProtectControlGroups=yes
ProtectHostname=yes
ProtectKernelLogs=yes
ProtectKernelModules=yes
ProtectKernelTunables=yes
ProtectSystem=strict
ProtectHome=read-only
ReadOnlyPaths={source} {incoming}
ReadWritePaths={campaign}
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictRealtime=yes
RestrictSUIDSGID=yes
LockPersonality=yes
CapabilityBoundingSet=
AmbientCapabilities=
StandardOutput=journal
StandardError=journal
SyslogIdentifier={service.removesuffix('.service')}

[Install]
WantedBy=multi-user.target
"""


def _assert_safe_real_path(path: Path, allowed_parent: Path, *, must_exist: bool) -> None:
    if not path.is_absolute() or not allowed_parent.is_absolute():
        raise LaunchPackError(f"authoritative path must be absolute: {path}")
    resolved_parent = allowed_parent.resolve(strict=True)
    if resolved_parent != allowed_parent or allowed_parent.is_symlink():
        raise LaunchPackError(f"authoritative parent is not an exact real path: {allowed_parent}")
    try:
        relative = path.relative_to(allowed_parent)
    except ValueError as error:
        raise LaunchPackError(f"path leaves allowed root: {path}") from error
    current = allowed_parent
    for part in relative.parts:
        current /= part
        if current.exists() and current.is_symlink():
            raise LaunchPackError(f"symlink forbidden in authoritative path: {current}")
    resolved = path.resolve(strict=must_exist)
    try:
        resolved.relative_to(resolved_parent)
    except ValueError as error:
        raise LaunchPackError(f"path leaves allowed root: {path}") from error
    if resolved != path:
        raise LaunchPackError(f"authoritative path is not byte-exact after readlink: {path}")


def _timedatectl_synchronized() -> bool:
    completed = subprocess.run(
        ["timedatectl", "show", "--property=NTPSynchronized", "--value"],
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.returncode == 0 and completed.stdout.strip().lower() == "yes"


def _writer_lock_is_available(raw_root: Path) -> bool:
    lock_path = raw_root / ".writer.lock"
    if not lock_path.exists():
        return True
    fcntl = importlib.import_module("fcntl")
    with lock_path.open("r+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return True


def preflight_snapshot(
    *,
    handoff: Mapping[str, object],
    current_user: str,
    now: datetime,
    ntp_synchronized: bool,
    available_bytes: int,
    raw_exists: bool,
    raw_stored_bytes: int,
    writer_lock_available: bool,
    forbidden_environment: Sequence[str],
    volume_snapshot: Mapping[str, object],
) -> dict[str, object]:
    checked = validate_handoff(handoff)
    if current_user != EXPECTED_USER:
        raise LaunchPackError(f"H1_USER_REFUSED: expected={EXPECTED_USER} actual={current_user}")
    if not ntp_synchronized:
        raise LaunchPackError("H1_NTP_NOT_SYNCHRONIZED")
    if forbidden_environment:
        raise LaunchPackError(
            "H1_PRIVATE_SURFACE_ENVIRONMENT_REFUSED:" + ",".join(sorted(forbidden_environment))
        )
    if type(raw_stored_bytes) is not int or raw_stored_bytes < 0:
        raise LaunchPackError("stored raw bytes must be a non-negative integer")
    disk = checked["disk"]
    assert isinstance(disk, dict)
    remaining_raw_bytes = max(0, int(disk["maximum_raw_bytes"]) - raw_stored_bytes)
    required_now = remaining_raw_bytes + int(disk["margin_bytes"])
    volume = checked["volume"]
    assert isinstance(volume, dict)
    mount_options_value = volume_snapshot.get("mount_options")
    if isinstance(mount_options_value, str) or not isinstance(mount_options_value, Sequence):
        raise LaunchPackError("H1_VOLUME_OPTIONS_REFUSED")
    runtime_volume = validate_volume_snapshot(
        contract=volume,
        mount_target=str(volume_snapshot.get("mount_target", "")),
        source_device=str(volume_snapshot.get("source_device", "")),
        canonical_device=str(volume_snapshot.get("canonical_device", "")),
        filesystem=str(volume_snapshot.get("filesystem", "")),
        mount_options=tuple(str(item) for item in mount_options_value),
        serial=(str(volume_snapshot["serial"]) if volume_snapshot.get("serial") else None),
        model=(str(volume_snapshot["model"]) if volume_snapshot.get("model") else None),
        available_bytes=available_bytes,
        required_free_bytes=required_now,
    )
    capacity = assess_capacity(available_bytes, required_now)
    capacity["remaining_raw_budget_bytes"] = remaining_raw_bytes
    capacity["stored_raw_bytes"] = raw_stored_bytes
    if not writer_lock_available:
        raise LaunchPackError("H1_WRITER_ALREADY_ACTIVE")
    starts = _parse_utc(checked["starts_at_utc"], label="starts_at_utc")
    if not raw_exists and now.astimezone(UTC) > starts:
        raise LaunchPackError("H1_PROSPECTIVE_START_MISSED_WITHOUT_RAW_ROOT")
    return {
        "boundary": BOUNDARY,
        "campaign_slug": checked["campaign_slug"],
        "capacity": capacity,
        "collection_mode": "RESUME" if raw_exists else "INITIAL",
        "ntp_synchronized": True,
        "status": "H1_VPS_PREFLIGHT_GREEN",
        "volume": runtime_volume,
    }


def _verify_repository_and_campaign(handoff_path: Path) -> tuple[dict[str, object], Path, Path]:
    handoff = validate_handoff(_load_object(handoff_path))
    remote = handoff["remote"]
    assert isinstance(remote, dict)
    source_root = Path(str(remote["source_root"]))
    campaign_root = Path(str(remote["campaign_root"]))
    home_root = Path(str(remote["home_root"]))
    volume_root = Path(str(remote["volume_root"]))
    _assert_safe_real_path(source_root, volume_root / "sources", must_exist=True)
    _assert_safe_real_path(campaign_root, volume_root / "campaigns", must_exist=True)
    _assert_safe_real_path(handoff_path, home_root / "incoming", must_exist=True)
    if _git_output(source_root, "rev-parse", "HEAD") != handoff["source_commit"]:
        raise LaunchPackError("VPS source HEAD differs from handoff commit")
    if _git_output(source_root, "status", "--porcelain"):
        raise LaunchPackError("VPS source clone is not clean")
    plan_path = source_root / str(handoff["launch_plan_path"])
    if sha256_file(plan_path) != handoff["launch_plan_sha256"]:
        raise LaunchPackError("launch plan hash differs from handoff")
    plan = validate_plan(_load_object(plan_path))
    inventory = build_inventory(source_root, plan)
    if inventory != handoff["inventory"]:
        raise LaunchPackError("VPS policy/fee/readiness inventory differs from handoff")
    files = handoff["files"]
    assert isinstance(files, dict)
    manifest_path = campaign_root / "campaign-manifest.json"
    pin_path = campaign_root / "campaign-manifest.sha256"
    health_path = campaign_root / "state" / "health.json"
    if sha256_file(manifest_path) != files["campaign_manifest_sha256"]:
        raise LaunchPackError("VPS campaign manifest differs from handoff")
    if sha256_file(pin_path) != files["campaign_manifest_pin_sha256"]:
        raise LaunchPackError("VPS campaign manifest pin differs from handoff")
    if not health_path.is_file():
        raise LaunchPackError("VPS campaign health file is absent")
    if not (campaign_root / "raw").exists() and sha256_file(health_path) != files.get(
        "campaign_health_sha256"
    ):
        raise LaunchPackError("initial VPS campaign health differs from handoff")
    validate_campaign_manifest(manifest_path, pin_path, plan, inventory)
    return handoff, source_root, campaign_root


def _stored_segment_file_bytes(raw_root: Path) -> int:
    segments_root = raw_root / "segments"
    if not segments_root.exists():
        return 0
    total = 0
    for path in segments_root.iterdir():
        if path.is_symlink() or not path.is_file() or not path.name.endswith(".rdpseg"):
            raise LaunchPackError(f"unexpected artifact in raw segments directory: {path}")
        total += path.stat().st_size
    return total


def run_vps_preflight(handoff_path: Path) -> dict[str, object]:
    handoff, _source_root, campaign_root = _verify_repository_and_campaign(handoff_path)
    pwd = importlib.import_module("pwd")
    geteuid = getattr(os, "geteuid", None)
    if not callable(geteuid):
        raise LaunchPackError("effective user lookup is unavailable")
    current_user = str(pwd.getpwuid(int(geteuid())).pw_name)
    forbidden = [name for name in FORBIDDEN_ENVIRONMENT if os.environ.get(name)]
    volume = handoff["volume"]
    assert isinstance(volume, dict)
    mount_point = str(volume["mount_point"])
    disk = shutil.disk_usage(mount_point)
    mount_target = _command_output("findmnt", "-rn", "-T", mount_point, "-o", "TARGET")
    source_device = _command_output("findmnt", "-rn", "-T", mount_point, "-o", "SOURCE")
    filesystem = _command_output("findmnt", "-rn", "-T", mount_point, "-o", "FSTYPE")
    mount_options = _command_output("findmnt", "-rn", "-T", mount_point, "-o", "OPTIONS").split(
        ","
    )
    canonical_device = _command_output("readlink", "-f", str(volume["device"]))
    serial = _optional_command_output("lsblk", "-dn", "-o", "SERIAL", str(volume["device"]))
    model = _optional_command_output("lsblk", "-dn", "-o", "MODEL", str(volume["device"]))
    raw_root = campaign_root / "raw"
    return preflight_snapshot(
        handoff=handoff,
        current_user=current_user,
        now=datetime.now(tz=UTC),
        ntp_synchronized=_timedatectl_synchronized(),
        available_bytes=disk.free,
        raw_exists=raw_root.exists(),
        raw_stored_bytes=_stored_segment_file_bytes(raw_root),
        writer_lock_available=_writer_lock_is_available(raw_root),
        forbidden_environment=forbidden,
        volume_snapshot={
            "canonical_device": canonical_device,
            "filesystem": filesystem,
            "model": model,
            "mount_options": mount_options,
            "mount_target": mount_target,
            "serial": serial,
            "source_device": source_device,
        },
    )


def evaluate_monitor(
    *,
    active_state: str,
    main_pid: int,
    command_line: str,
    health: Mapping[str, object],
    handoff: Mapping[str, object],
    now: datetime,
) -> dict[str, object]:
    checked = validate_handoff(handoff)
    remote = checked["remote"]
    assert isinstance(remote, dict)
    campaign_root = str(remote["campaign_root"])
    expected_manifest = checked["files"]
    assert isinstance(expected_manifest, dict)
    if health.get("boundary") != BOUNDARY or health.get("campaign_id") != checked.get("campaign_id"):
        raise LaunchPackError("H1_HEALTH_IDENTITY_DIVERGENCE")
    terminal = _required_text(health.get("terminal_health"), label="terminal_health")
    manifest_value = health.get("manifest_sha256")
    if terminal == "PREPARED_NOT_STARTED":
        if manifest_value is not None:
            raise LaunchPackError("prepared health unexpectedly claims a raw manifest")
    elif terminal == "RUNNING":
        if manifest_value is not None:
            _required_sha256(manifest_value, label="raw manifest_sha256")
    elif terminal not in TERMINAL_HEALTH:
        raise LaunchPackError("H1_HEALTH_STATE_NOT_ADMISSIBLE")
    starts = _parse_utc(checked["starts_at_utc"], label="starts_at_utc")
    if active_state == "active":
        if main_pid <= 0 or not command_line:
            raise LaunchPackError("H1_FALSE_SYSTEMD_PID")
        waiting = "run_collector.sh" in command_line and str(checked["source_commit"]) not in command_line
        collecting = (
            "-m hyperlab research-data h1-collect" in command_line
            and f"--campaign-root {campaign_root}" in command_line
        )
        if not waiting and not collecting:
            raise LaunchPackError("H1_SYSTEMD_PROCESS_IDENTITY_DIVERGENCE")
        if now.astimezone(UTC) < starts:
            if not waiting or terminal != "PREPARED_NOT_STARTED":
                raise LaunchPackError("H1_SERVICE_OPENED_COLLECTION_BEFORE_FROZEN_START")
            status = "H1_SERVICE_ARMED_PREPARED_NOT_STARTED"
        else:
            if not collecting or terminal != "RUNNING":
                raise LaunchPackError("H1_SERVICE_ACTIVE_WITHOUT_ADMISSIBLE_RUNNING_HEALTH")
            status = "H1_SERVICE_RUNNING_HEALTH_GREEN"
    else:
        if terminal in {"PREPARED_NOT_STARTED", "RUNNING"}:
            raise LaunchPackError("H1_SERVICE_NOT_ACTIVE_WITH_NONTERMINAL_HEALTH")
        status = f"H1_SERVICE_STOPPED_{terminal}"
    return {
        "active_state": active_state,
        "campaign_manifest_sha256": expected_manifest["campaign_manifest_sha256"],
        "main_pid": main_pid,
        "status": status,
        "terminal_health": terminal,
    }


def _systemd_properties(service_name: str) -> dict[str, str]:
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            service_name,
            "--property=ActiveState,SubState,MainPID",
            "--no-pager",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise LaunchPackError(f"systemd cannot inspect {service_name}")
    properties: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            properties[key] = value
    return properties


def run_monitor_check(handoff_path: Path) -> dict[str, object]:
    handoff, _source_root, campaign_root = _verify_repository_and_campaign(handoff_path)
    properties = _systemd_properties(str(handoff["service_name"]))
    try:
        main_pid = int(properties.get("MainPID", "0"))
    except ValueError as error:
        raise LaunchPackError("systemd MainPID is invalid") from error
    command_line = ""
    if main_pid > 0:
        try:
            command_line = (
                Path(f"/proc/{main_pid}/cmdline")
                .read_bytes()
                .replace(b"\x00", b" ")
                .decode("utf-8", errors="strict")
                .strip()
            )
        except (OSError, UnicodeDecodeError) as error:
            raise LaunchPackError("H1_FALSE_SYSTEMD_PID") from error
    health = _load_object(campaign_root / "state" / "health.json")
    return evaluate_monitor(
        active_state=properties.get("ActiveState", "unknown"),
        main_pid=main_pid,
        command_line=command_line,
        health=health,
        handoff=handoff,
        now=datetime.now(tz=UTC),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HyperLab H1 fail-closed launch-pack tooling")
    subparsers = parser.add_subparsers(dest="command", required=True)
    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--repo-root", type=Path, required=True)
    finalize.add_argument("--plan", type=Path, required=True)
    finalize.add_argument("--bundle", type=Path, required=True)
    finalize.add_argument("--output-root", type=Path, required=True)
    finalize.add_argument("--source-commit", required=True)
    render = subparsers.add_parser("render-unit")
    render.add_argument("--handoff", type=Path, required=True)
    render.add_argument("--output", type=Path, required=True)
    preflight = subparsers.add_parser("vps-preflight")
    preflight.add_argument("--handoff", type=Path, required=True)
    preflight.add_argument("--mode", choices=("start",), required=True)
    monitor = subparsers.add_parser("monitor-check")
    monitor.add_argument("--handoff", type=Path, required=True)
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("--repo-root", type=Path, required=True)
    inspect.add_argument("--plan", type=Path, required=True)
    return parser


def _fail(message: str) -> NoReturn:
    print(json.dumps({"status": "H1_LAUNCH_PACK_REFUSED", "error": message}, sort_keys=True))
    raise SystemExit(4)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "finalize":
            result = finalize_launch_pack(
                repo_root=arguments.repo_root.resolve(),
                plan_path=arguments.plan.resolve(),
                bundle_path=arguments.bundle.resolve(),
                output_root=arguments.output_root.resolve(),
                source_commit=arguments.source_commit,
            )
        elif arguments.command == "render-unit":
            unit = render_systemd_unit(_load_object(arguments.handoff))
            _write_exclusive(arguments.output, unit.encode("utf-8"))
            result = {"output": str(arguments.output), "status": "H1_SYSTEMD_UNIT_RENDERED"}
        elif arguments.command == "vps-preflight":
            result = run_vps_preflight(arguments.handoff.resolve())
        elif arguments.command == "monitor-check":
            result = run_monitor_check(arguments.handoff.resolve())
        elif arguments.command == "inspect":
            plan = validate_plan(_load_object(arguments.plan.resolve()))
            result = {
                "inventory": build_inventory(arguments.repo_root.resolve(), plan),
                "plan": plan,
                "status": "H1_LAUNCH_PLAN_GREEN",
            }
        else:
            raise LaunchPackError("unsupported command")
    except (FileExistsError, LaunchPackError, OSError, subprocess.CalledProcessError) as error:
        _fail(str(error))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
