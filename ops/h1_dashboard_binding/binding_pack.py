from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import stat
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, Final, NoReturn

BOUNDARY: Final = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
STATUS: Final = (
    "H1_V8_DASHBOARD_BINDING_V2_GREEN_CRLF_PARENT_OWNERSHIP_FIXED_"
    "PUSHED_AWAITING_HUMAN_TRANSFER"
)
EXPECTED_BRANCH: Final = "codex/h1-v7-dashboard-binding-v1"
EXPECTED_BASE_COMMIT: Final = "926c878718c9f7d4095526061893e9f041d40c2b"
EXPECTED_INTEGRATION_COMMIT: Final = "cda0681b726fadba1a77bd72d2fca9f84dd14566"
EXPECTED_ORIGINAL_COMMIT: Final = "decb0e08aeabff71859fad052b84bff4af0ed990"
EXPECTED_BINDING_NAME: Final = "h1-20260827t004500z-5973abde-dashboard-v2"
EXPECTED_V1_BINDING_NAME: Final = "h1-20260827t004500z-5973abde-dashboard-v1"
EXPECTED_CAMPAIGN_ID: Final = "h1-68c6493652abd667420b9a5b"
EXPECTED_CAMPAIGN_SLUG: Final = "h1-20260827t004500z-5973abde"
EXPECTED_MANIFEST_SHA256: Final = (
    "3d8aeb91115ca7302266f85e55a2cd89404adbf4285991c35ad5c55b2647c2d5"
)
EXPECTED_STARTS_AT_UTC: Final = "2026-08-27T00:45:00Z"
EXPECTED_REMOTE_HOST: Final = "5.223.60.130"
EXPECTED_PORT: Final = 18080
EXPECTED_VOLUME_DEVICE: Final = "/dev/sdb"
EXPECTED_VOLUME_MOUNT: Final = "/mnt/HC_Volume_106716684"
EXPECTED_VOLUME_FILESYSTEM: Final = "ext4"
INPUT_FILENAME: Final = "binding-input-v8-v2.json"
BUNDLE_FILENAME: Final = "hyperlab-h1-v8-dashboard-binding-v2.bundle"
SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE: Final = re.compile(r"^[0-9a-f]{40}$")
CAMPAIGN_ID_RE: Final = re.compile(r"^h1-[0-9a-f]{24}$")
BRANCH_RE: Final = re.compile(r"^codex/[a-z0-9][a-z0-9._/-]*$")
SLUG_RE: Final = re.compile(r"^[a-z0-9][a-z0-9-]{4,95}$")
SERVICE_RE: Final = re.compile(r"^[a-z0-9][a-z0-9-]{4,127}\.service$")
USER_RE: Final = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")
REMOTE_HOST_RE: Final = re.compile(
    r"^(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$"
)
SAFE_RELATIVE_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")
UTC_RE: Final = re.compile(r"^20[0-9]{2}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
PACK_FILES: Final = (
    ".gitattributes",
    "ops/h1_dashboard_binding/New-H1V8DashboardBindingV2Bundle.ps1",
    "ops/h1_dashboard_binding/README.md",
    "ops/h1_dashboard_binding/__init__.py",
    f"ops/h1_dashboard_binding/{INPUT_FILENAME}",
    "ops/h1_dashboard_binding/binding_pack.py",
    "ops/h1_dashboard_binding/bootstrap-linux-v2.sh",
)
FORBIDDEN_ENVIRONMENT: Final = (
    "API_KEY",
    "API_SECRET",
    "HYPERLIQUID_API_KEY",
    "HYPERLIQUID_API_SECRET",
    "HYPERLIQUID_PRIVATE_KEY",
    "MNEMONIC",
    "PRIVATE_KEY",
    "SEED_PHRASE",
    "SIGNER",
    "SIGNER_KEY",
    "WALLET",
    "WALLET_ADDRESS",
    "WALLET_KEY",
)


class BindingPackError(ValueError):
    """A reusable dashboard-binding invariant failed closed."""


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


def _load_object(path: Path, *, maximum_bytes: int = 1024 * 1024) -> dict[str, Any]:
    try:
        details = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(details.st_mode) or details.st_size > maximum_bytes:
            raise BindingPackError(f"JSON artifact is unsafe: {path}")
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BindingPackError(f"invalid JSON artifact: {path}") from error
    if not isinstance(decoded, dict):
        raise BindingPackError(f"JSON artifact must be an object: {path}")
    return decoded


def _required_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise BindingPackError(f"{label} must be non-empty text")
    return value


def _required_match(value: object, *, label: str, pattern: re.Pattern[str]) -> str:
    text = _required_text(value, label=label)
    if pattern.fullmatch(text) is None:
        raise BindingPackError(f"{label} has an invalid format")
    return text


def _required_commit(value: object, *, label: str) -> str:
    return _required_match(value, label=label, pattern=COMMIT_RE)


def _required_sha256(value: object, *, label: str) -> str:
    return _required_match(value, label=label, pattern=SHA256_RE)


def _exact_leaf(value: object, *, label: str, parent: str) -> str:
    text = _required_text(value, label=label)
    path = PurePosixPath(text)
    if (
        not path.is_absolute()
        or path.parent != PurePosixPath(parent)
        or path.name in {"", ".", ".."}
        or "." in path.parts
        or ".." in path.parts
        or SLUG_RE.fullmatch(path.name) is None
    ):
        raise BindingPackError(f"{label} must be one exact leaf beneath {parent}")
    return text


def _safe_relative_file(value: object, *, label: str) -> str:
    text = _required_text(value, label=label)
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or not path.parts
        or "." in path.parts
        or ".." in path.parts
        or SAFE_RELATIVE_RE.fullmatch(text) is None
        or "//" in text
        or not text.endswith(".json")
    ):
        raise BindingPackError(f"{label} must be a safe relative file")
    return text


def _git_output(repo_root: Path, *arguments: str) -> str:
    environment = dict(os.environ)
    environment["GIT_OPTIONAL_LOCKS"] = "0"
    completed = subprocess.run(
        ["git", "-c", f"safe.directory={repo_root}", "-C", str(repo_root), *arguments],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    if completed.returncode != 0:
        raise BindingPackError(completed.stderr.strip() or "git command failed")
    return completed.stdout.strip()


def portable_git_file_bytes(repo_root: Path, relative_path: str) -> bytes:
    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise BindingPackError("Git identity path is unsafe")
    worktree = repo_root.joinpath(*relative.parts)
    if worktree.is_symlink() or not worktree.is_file():
        raise BindingPackError(f"Git identity path is absent or unsafe: {relative_path}")
    completed = subprocess.run(
        [
            "git",
            "-c",
            f"safe.directory={repo_root}",
            "-C",
            str(repo_root),
            "show",
            f"HEAD:{relative.as_posix()}",
        ],
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise BindingPackError(f"Git identity path is not tracked at HEAD: {relative_path}")
    canonical = completed.stdout
    materialized = worktree.read_bytes()
    if materialized != canonical and materialized.replace(b"\r\n", b"\n") != canonical:
        raise BindingPackError(f"worktree bytes differ from HEAD: {relative_path}")
    return canonical


def portable_git_file_sha256(repo_root: Path, relative_path: str) -> str:
    return sha256_bytes(portable_git_file_bytes(repo_root, relative_path))


def _manifest_checks(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or not value:
        raise BindingPackError("campaign.manifest_checks must be a non-empty object")
    result: dict[str, object] = {}
    for key, expected in value.items():
        if not isinstance(key, str) or re.fullmatch(r"[a-z][a-z0-9_]*", key) is None:
            raise BindingPackError("manifest check field is unsafe")
        if type(expected) not in {str, int, bool}:
            raise BindingPackError("manifest check value must be scalar")
        result[key] = expected
    return result


def validate_input(plan: Mapping[str, object]) -> dict[str, object]:
    """Validate a non-circular binding input whose source commit is not known yet."""
    if set(plan) != {
        "boundary",
        "campaign",
        "dashboard",
        "provenance",
        "schema_version",
        "status",
    } or plan.get("schema_version") != 1:
        raise BindingPackError("binding input fields or schema version differ from v1")
    if plan.get("boundary") != BOUNDARY or plan.get("status") != STATUS:
        raise BindingPackError("binding input safety boundary or status differs")

    provenance = plan.get("provenance")
    if not isinstance(provenance, dict) or set(provenance) != {
        "base_launch_commit",
        "branch",
        "dashboard_integration_commit",
        "dashboard_original_commit",
    }:
        raise BindingPackError("provenance fields differ from v1")
    _required_match(provenance.get("branch"), label="provenance.branch", pattern=BRANCH_RE)
    commits = [
        _required_commit(provenance.get(key), label=f"provenance.{key}")
        for key in (
            "base_launch_commit",
            "dashboard_original_commit",
            "dashboard_integration_commit",
        )
    ]
    if len(set(commits)) != len(commits):
        raise BindingPackError("provenance commits must remain distinct")

    campaign = plan.get("campaign")
    if not isinstance(campaign, dict) or set(campaign) != {
        "campaign_id",
        "campaign_root",
        "campaign_slug",
        "collector_service",
        "collector_source_root",
        "manifest_checks",
        "manifest_sha256",
        "starts_at_utc",
    }:
        raise BindingPackError("campaign fields differ from v1")
    slug = _required_match(campaign.get("campaign_slug"), label="campaign.slug", pattern=SLUG_RE)
    campaign_id = _required_match(
        campaign.get("campaign_id"), label="campaign.id", pattern=CAMPAIGN_ID_RE
    )
    campaign_root = _exact_leaf(
        campaign.get("campaign_root"),
        label="campaign.root",
        parent="/mnt/HC_Volume_106716684/hyperlab-h1/campaigns",
    )
    source_root = _exact_leaf(
        campaign.get("collector_source_root"),
        label="campaign.collector_source_root",
        parent="/mnt/HC_Volume_106716684/hyperlab-h1/sources",
    )
    if PurePosixPath(campaign_root).name != slug or PurePosixPath(source_root).name != slug:
        raise BindingPackError("campaign roots must end in the exact slug")
    service = _required_match(
        campaign.get("collector_service"), label="campaign.collector_service", pattern=SERVICE_RE
    )
    if service != f"hyperlab-{slug}.service":
        raise BindingPackError("collector service must derive from the campaign slug")
    _required_sha256(campaign.get("manifest_sha256"), label="campaign.manifest_sha256")
    starts = _required_match(
        campaign.get("starts_at_utc"), label="campaign.starts_at_utc", pattern=UTC_RE
    )
    checks = _manifest_checks(campaign.get("manifest_checks"))
    if checks.get("campaign_id") != campaign_id or checks.get("boundary") != BOUNDARY:
        raise BindingPackError("manifest checks must bind campaign ID and safety boundary")
    if checks.get("schema_version") != 1 or checks.get("starts_at_utc") != starts:
        raise BindingPackError("manifest checks must bind schema version and campaign start")

    dashboard = plan.get("dashboard")
    if not isinstance(dashboard, dict) or set(dashboard) != {
        "bind_host",
        "bind_port",
        "handoff_root",
        "incoming_root",
        "policy_path",
        "remote_host",
        "runtime_directory",
        "service_name",
        "source_root",
        "user",
    }:
        raise BindingPackError("dashboard fields differ from v1")
    if dashboard.get("bind_host") != "127.0.0.1":
        raise BindingPackError("dashboard must bind IPv4 loopback")
    port = dashboard.get("bind_port")
    if type(port) is not int or not 1024 <= port <= 65535:
        raise BindingPackError("dashboard port must be an unprivileged TCP port")
    remote_host = _required_match(
        dashboard.get("remote_host"), label="dashboard.remote_host", pattern=REMOTE_HOST_RE
    )
    if remote_host.casefold() in {
        "localhost",
        "127.0.0.1",
        "0.0.0.0",
    }:
        raise BindingPackError("dashboard remote host is unsafe")
    user = _required_match(dashboard.get("user"), label="dashboard.user", pattern=USER_RE)
    _safe_relative_file(dashboard.get("policy_path"), label="dashboard.policy_path")
    binding_name = PurePosixPath(
        _exact_leaf(
            dashboard.get("source_root"),
            label="dashboard.source_root",
            parent="/mnt/HC_Volume_106716684/hyperlab-h1/dashboard-sources",
        )
    ).name
    incoming = _exact_leaf(
        dashboard.get("incoming_root"),
        label="dashboard.incoming_root",
        parent=f"/home/{user}/hyperlab-h1/dashboard-bindings",
    )
    handoff = _exact_leaf(
        dashboard.get("handoff_root"),
        label="dashboard.handoff_root",
        parent="/etc/hyperlab-h1-dashboard",
    )
    if PurePosixPath(incoming).name != binding_name or PurePosixPath(handoff).name != binding_name:
        raise BindingPackError("dashboard roots must share one unique binding name")
    dashboard_service = _required_match(
        dashboard.get("service_name"), label="dashboard.service_name", pattern=SERVICE_RE
    )
    if dashboard_service == service:
        raise BindingPackError("dashboard service must be separate from the collector")
    runtime = _required_match(
        dashboard.get("runtime_directory"), label="dashboard.runtime_directory", pattern=SLUG_RE
    )
    if runtime not in dashboard_service:
        raise BindingPackError("runtime directory must identify the dashboard service")
    return {
        "boundary": BOUNDARY,
        "campaign": dict(campaign),
        "dashboard": dict(dashboard),
        "provenance": dict(provenance),
        "schema_version": 1,
        "status": STATUS,
    }


def validate_plan(plan: Mapping[str, object]) -> dict[str, object]:
    """Validate a finalized plan after injecting the clean final source commit."""
    provenance = plan.get("provenance")
    if not isinstance(provenance, dict) or "source_commit" not in provenance:
        raise BindingPackError("final plan lacks source_commit")
    source_commit = _required_commit(provenance.get("source_commit"), label="source_commit")
    input_shape = dict(plan)
    input_provenance = dict(provenance)
    del input_provenance["source_commit"]
    input_shape["provenance"] = input_provenance
    checked = validate_input(input_shape)
    checked_provenance = checked["provenance"]
    assert isinstance(checked_provenance, dict)
    if source_commit in checked_provenance.values():
        raise BindingPackError("final source commit must be distinct from frozen provenance")
    checked["provenance"] = {**checked_provenance, "source_commit": source_commit}
    return checked


def validate_frozen_v8_input(plan: Mapping[str, object]) -> dict[str, object]:
    checked = validate_input(plan)
    campaign = checked["campaign"]
    dashboard = checked["dashboard"]
    provenance = checked["provenance"]
    assert isinstance(campaign, dict) and isinstance(dashboard, dict) and isinstance(provenance, dict)
    expected_campaign = {
        "campaign_id": EXPECTED_CAMPAIGN_ID,
        "campaign_root": (
            "/mnt/HC_Volume_106716684/hyperlab-h1/campaigns/" + EXPECTED_CAMPAIGN_SLUG
        ),
        "campaign_slug": EXPECTED_CAMPAIGN_SLUG,
        "collector_service": f"hyperlab-{EXPECTED_CAMPAIGN_SLUG}.service",
        "collector_source_root": (
            "/mnt/HC_Volume_106716684/hyperlab-h1/sources/" + EXPECTED_CAMPAIGN_SLUG
        ),
        "manifest_checks": {
            "boundary": BOUNDARY,
            "campaign_id": EXPECTED_CAMPAIGN_ID,
            "schema_version": 1,
            "starts_at_utc": EXPECTED_STARTS_AT_UTC,
        },
        "manifest_sha256": EXPECTED_MANIFEST_SHA256,
        "starts_at_utc": EXPECTED_STARTS_AT_UTC,
    }
    expected_dashboard = {
        "bind_host": "127.0.0.1",
        "bind_port": EXPECTED_PORT,
        "handoff_root": f"/etc/hyperlab-h1-dashboard/{EXPECTED_BINDING_NAME}",
        "incoming_root": (
            f"/home/hyperlab/hyperlab-h1/dashboard-bindings/{EXPECTED_BINDING_NAME}"
        ),
        "policy_path": "config/research/hyperliquid-h1-ghost-v1.json",
        "remote_host": EXPECTED_REMOTE_HOST,
        "runtime_directory": "hyperlab-h1-dashboard-20260827t004500z-5973abde-v2",
        "service_name": "hyperlab-h1-dashboard-20260827t004500z-5973abde-v2.service",
        "source_root": (
            "/mnt/HC_Volume_106716684/hyperlab-h1/dashboard-sources/"
            + EXPECTED_BINDING_NAME
        ),
        "user": "hyperlab",
    }
    expected_provenance = {
        "base_launch_commit": EXPECTED_BASE_COMMIT,
        "branch": EXPECTED_BRANCH,
        "dashboard_integration_commit": EXPECTED_INTEGRATION_COMMIT,
        "dashboard_original_commit": EXPECTED_ORIGINAL_COMMIT,
    }
    if campaign != expected_campaign:
        raise BindingPackError("frozen V8 campaign identity differs")
    if dashboard != expected_dashboard:
        raise BindingPackError("frozen V8 dashboard identity differs")
    if provenance != expected_provenance:
        raise BindingPackError("frozen V8 provenance differs")
    return checked


def build_final_plan(binding_input: Mapping[str, object], source_commit: str) -> dict[str, object]:
    checked = validate_frozen_v8_input(binding_input)
    _required_commit(source_commit, label="source_commit")
    provenance = checked["provenance"]
    assert isinstance(provenance, dict)
    return validate_plan(
        {**checked, "provenance": {**provenance, "source_commit": source_commit}}
    )


def validate_handoff(handoff: Mapping[str, object]) -> dict[str, object]:
    if set(handoff) != {
        "boundary",
        "bundle",
        "inventory",
        "plan",
        "schema_version",
        "status",
    } or handoff.get("schema_version") != 1:
        raise BindingPackError("handoff fields or schema version differ from v1")
    if handoff.get("boundary") != BOUNDARY or handoff.get("status") != STATUS:
        raise BindingPackError("handoff boundary or status differs")
    plan_value = handoff.get("plan")
    if not isinstance(plan_value, dict):
        raise BindingPackError("handoff plan must be an object")
    plan = validate_plan(plan_value)
    provenance = plan["provenance"]
    assert isinstance(provenance, dict)
    frozen_provenance = dict(provenance)
    del frozen_provenance["source_commit"]
    validate_frozen_v8_input({**plan, "provenance": frozen_provenance})
    bundle = handoff.get("bundle")
    if not isinstance(bundle, dict) or set(bundle) != {"filename", "ref", "sha256"}:
        raise BindingPackError("handoff bundle fields differ")
    if bundle.get("filename") != BUNDLE_FILENAME:
        raise BindingPackError("handoff bundle filename differs")
    if bundle.get("ref") != f"refs/heads/{provenance['branch']}":
        raise BindingPackError("handoff bundle ref differs")
    _required_sha256(bundle.get("sha256"), label="handoff.bundle.sha256")
    inventory = handoff.get("inventory")
    if not isinstance(inventory, dict) or set(inventory) != {"pack_files"}:
        raise BindingPackError("handoff inventory differs")
    pack_files = inventory.get("pack_files")
    if not isinstance(pack_files, dict) or set(pack_files) != set(PACK_FILES):
        raise BindingPackError("handoff pack-file inventory differs")
    for path, digest in pack_files.items():
        _required_sha256(digest, label=f"handoff.inventory.{path}")
    return dict(handoff)


def _validate_source_causality(
    repo_root: Path,
    *,
    plan: Mapping[str, object],
    bundle_path: Path,
) -> None:
    checked = validate_plan(plan)
    provenance = checked["provenance"]
    assert isinstance(provenance, dict)
    source_commit = str(provenance["source_commit"])
    if _git_output(repo_root, "rev-parse", "HEAD") != source_commit:
        raise BindingPackError("final source HEAD differs")
    if _git_output(repo_root, "branch", "--show-current") != provenance["branch"]:
        raise BindingPackError("final source branch differs")
    if _git_output(repo_root, "status", "--porcelain"):
        raise BindingPackError("final source worktree must be clean")
    if _git_output(repo_root, "rev-parse", f"{EXPECTED_INTEGRATION_COMMIT}^") != EXPECTED_BASE_COMMIT:
        raise BindingPackError("dashboard integration parent differs from the frozen V8 base")
    for ancestor, descendant in (
        (EXPECTED_BASE_COMMIT, EXPECTED_INTEGRATION_COMMIT),
        (EXPECTED_INTEGRATION_COMMIT, source_commit),
    ):
        completed = subprocess.run(
            [
                "git",
                "-c",
                f"safe.directory={repo_root}",
                "-C",
                str(repo_root),
                "merge-base",
                "--is-ancestor",
                ancestor,
                descendant,
            ],
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            raise BindingPackError(f"causal ancestry differs: {ancestor} -> {descendant}")
    message = _git_output(
        repo_root, "show", "-s", "--format=%B", EXPECTED_INTEGRATION_COMMIT
    )
    if f"(cherry picked from commit {EXPECTED_ORIGINAL_COMMIT})" not in message:
        raise BindingPackError("integration commit lacks the exact original-dashboard marker")
    if not bundle_path.is_file() or bundle_path.is_symlink():
        raise BindingPackError("Git bundle is absent or unsafe")
    expected_head = f"{source_commit} refs/heads/{EXPECTED_BRANCH}"
    heads = _git_output(repo_root, "bundle", "list-heads", str(bundle_path)).splitlines()
    if expected_head not in heads:
        raise BindingPackError("Git bundle does not expose the exact final ref")


def build_handoff(
    *,
    repo_root: Path,
    plan: Mapping[str, object],
    bundle_path: Path,
) -> dict[str, object]:
    checked = validate_plan(plan)
    result = {
        "boundary": BOUNDARY,
        "bundle": {
            "filename": bundle_path.name,
            "ref": f"refs/heads/{EXPECTED_BRANCH}",
            "sha256": sha256_file(bundle_path),
        },
        "inventory": {
            "pack_files": {
                path: portable_git_file_sha256(repo_root, path) for path in PACK_FILES
            }
        },
        "plan": checked,
        "schema_version": 1,
        "status": STATUS,
    }
    return validate_handoff(result)


def render_systemd_unit(plan: Mapping[str, object]) -> str:
    checked = validate_plan(plan)
    campaign = checked["campaign"]
    dashboard = checked["dashboard"]
    provenance = checked["provenance"]
    assert isinstance(campaign, dict) and isinstance(dashboard, dict) and isinstance(provenance, dict)
    source = str(dashboard["source_root"])
    campaign_root = str(campaign["campaign_root"])
    handoff = f"{dashboard['handoff_root']}/binding-plan.json"
    runtime = str(dashboard["runtime_directory"])
    port = dashboard["bind_port"]
    return f"""[Unit]
Description=HyperLab H1 read-only observability dashboard
Documentation=file:{source}/ops/h1_dashboard_binding/README.md
After=network.target {campaign['collector_service']}
RequiresMountsFor=/mnt/HC_Volume_106716684
ConditionPathIsDirectory={campaign_root}
ConditionPathIsDirectory={source}

[Service]
Type=simple
User={dashboard['user']}
Group={dashboard['user']}
WorkingDirectory={source}
Environment=HOME=/home/{dashboard['user']}
Environment=HYPERLAB_CONFIG={source}/config/research.toml
Environment=HYPERLAB_MODE=readonly
Environment=HYPERLAB_REQUIRE_PERSISTENT_LAYOUT=0
Environment=HYPERLAB_DATA_DIR=/run/{runtime}
Environment=HYPERLAB_RUNTIME_DIR=/run/{runtime}
Environment=HYPERLAB_REPORTS_DIR=/run/{runtime}
Environment=HYPERLAB_PAPER_DIR=/run/{runtime}
Environment=HYPERLAB_H1_CAMPAIGN_ROOT={campaign_root}
Environment=HYPERLAB_H1_POLICY_CONFIG={source}/{dashboard['policy_path']}
Environment=HYPERLAB_H1_EXPECTED_CAMPAIGN_ID={campaign['campaign_id']}
Environment=HYPERLAB_H1_EXPECTED_CAMPAIGN_MANIFEST_SHA256={campaign['manifest_sha256']}
Environment=HYPERLAB_H1_EXPECTED_CAMPAIGN_SLUG={campaign['campaign_slug']}
Environment=HYPERLAB_H1_COLLECTOR_SOURCE_COMMIT={provenance['base_launch_commit']}
Environment=HYPERLAB_H1_DASHBOARD_SOURCE_COMMIT={provenance['source_commit']}
Environment=HYPERLAB_H1_DASHBOARD_ORIGINAL_COMMIT={provenance['dashboard_original_commit']}
Environment=HYPERLAB_H1_DASHBOARD_INTEGRATION_COMMIT={provenance['dashboard_integration_commit']}
Environment=PYTHONPATH={source}/src
Environment=PYTHONNOUSERSITE=1
Environment=PYTHONDONTWRITEBYTECODE=1
Environment=PYTHONUNBUFFERED=1
Environment=TZ=UTC
UnsetEnvironment={' '.join(FORBIDDEN_ENVIRONMENT)}
ExecCondition={source}/.venv/bin/python -B {source}/ops/h1_dashboard_binding/binding_pack.py service-preflight --plan {handoff}
ExecStart={source}/.venv/bin/python -B -m hyperlab h1-dashboard-serve --port {port}
Restart=on-failure
RestartSec=10
TimeoutStartSec=30
TimeoutStopSec=30
KillSignal=SIGINT
KillMode=mixed
UMask=0077
RuntimeDirectory={runtime}
RuntimeDirectoryMode=0700
NoNewPrivileges=yes
PrivateDevices=yes
PrivateTmp=yes
ProtectClock=yes
ProtectControlGroups=yes
ProtectHostname=yes
ProtectKernelLogs=yes
ProtectKernelModules=yes
ProtectKernelTunables=yes
ProtectProc=invisible
ProtectSystem=strict
ProtectHome=yes
BindReadOnlyPaths={campaign_root}
ReadOnlyPaths={campaign_root} {source} {dashboard['handoff_root']}
InaccessiblePaths=/home/{dashboard['user']}/.ssh /root
RestrictAddressFamilies=AF_UNIX AF_INET
IPAddressDeny=any
IPAddressAllow=localhost
SocketBindDeny=any
SocketBindAllow=ipv4:tcp:{port}
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
CapabilityBoundingSet=
AmbientCapabilities=
StandardOutput=journal
StandardError=journal
SyslogIdentifier={str(dashboard['service_name']).removesuffix('.service')}

[Install]
WantedBy=multi-user.target
"""


def render_windows_transfer(handoff: Mapping[str, object]) -> str:
    checked = validate_handoff(handoff)
    plan = checked["plan"]
    bundle = checked["bundle"]
    assert isinstance(plan, dict) and isinstance(bundle, dict)
    dashboard = plan["dashboard"]
    assert isinstance(dashboard, dict)
    incoming_relative = str(dashboard["incoming_root"]).removeprefix(
        f"/home/{dashboard['user']}/"
    )
    return rf"""[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{{64}}$')]
    [string] $ExpectedInventorySha256,

    [switch] $SelfCheck
)

# STEP A/3 - LOCATION: Windows PowerShell 5.1 on the Beelink.
# EXPECTED_DURATION: 2-10 minutes; MAXIMUM_DURATION: 30 minutes.
# PROMPTS: SSH host-key trust or SSH-key passphrase only; HyperLab never prompts.
# MONITORING: local SHA-256, TCP/22 reachability and exact SFTP/SCP exit codes.
# CTRL+C: interrupts only this transfer; neither VPS service is changed.
# TERMINAL_SIGNAL: H1_V8_DASHBOARD_BINDING_V2_STEP_A_TRANSFER_GREEN_NOT_INSTALLED.
$ErrorActionPreference = 'Stop'
$ArtifactRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$SshKey = "$env:USERPROFILE\.ssh\hyperlab_hetzner"
$RemoteUser = '{dashboard['user']}'
$RemoteHost = '{dashboard['remote_host']}'
$RemoteIncomingRelative = '{incoming_relative}'
$InventoryPath = Join-Path $ArtifactRoot 'binding-files.sha256'

if ($SelfCheck) {{
    Write-Output 'H1_V8_DASHBOARD_BINDING_V2_STEP_A_SELFCHECK_GREEN'
    return
}}

$ActualInventorySha256 = (Get-FileHash -LiteralPath $InventoryPath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($ActualInventorySha256 -ne $ExpectedInventorySha256) {{
    throw 'binding-files.sha256 identity differs before parsing.'
}}
Get-Content -LiteralPath $InventoryPath | ForEach-Object {{
    $Parts = $_ -split '  ', 2
    if ($Parts.Count -ne 2) {{ throw "Invalid binding-files entry: $_" }}
    if ($Parts[1] -notmatch '^[A-Za-z0-9][A-Za-z0-9._/-]*$' -or
            $Parts[1].Split('/') -contains '..') {{ throw "Unsafe binding-files path: $($Parts[1])" }}
    $Path = Join-Path $ArtifactRoot $Parts[1]
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {{ throw "Missing file: $Path" }}
    $Actual = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($Actual -ne $Parts[0]) {{ throw "SHA-256 mismatch: $Path" }}
}}
if (-not (Test-Path -LiteralPath $SshKey -PathType Leaf)) {{ throw "SSH key absent: $SshKey" }}
if (-not (Test-NetConnection -ComputerName $RemoteHost -Port 22).TcpTestSucceeded) {{
    throw 'TCP/22 is not reachable.'
}}
$SftpBatch = Join-Path ([System.IO.Path]::GetTempPath()) "h1-v8-dashboard-sftp-$PID.txt"
if (Test-Path -LiteralPath $SftpBatch) {{ throw "Temporary SFTP batch exists: $SftpBatch" }}
try {{
    @('-mkdir hyperlab-h1/dashboard-bindings', "mkdir $RemoteIncomingRelative") |
        Set-Content -LiteralPath $SftpBatch -Encoding ascii
    & sftp.exe -i $SshKey -b $SftpBatch "$RemoteUser@$RemoteHost"
    if ($LASTEXITCODE -ne 0) {{ throw 'Unique incoming-root creation failed.' }}
}} finally {{
    if (Test-Path -LiteralPath $SftpBatch) {{ Remove-Item -LiteralPath $SftpBatch -Force }}
}}
$RemoteTarget = "${{RemoteUser}}@${{RemoteHost}}:${{RemoteIncomingRelative}}/"
& scp.exe -i $SshKey `
    (Join-Path $ArtifactRoot '{bundle['filename']}') `
    (Join-Path $ArtifactRoot '{INPUT_FILENAME}') `
    (Join-Path $ArtifactRoot 'binding-plan.json') `
    (Join-Path $ArtifactRoot 'handoff.json') `
    (Join-Path $ArtifactRoot 'binding-files.sha256') `
    (Join-Path $ArtifactRoot 'README.md') `
    $RemoteTarget
if ($LASTEXITCODE -ne 0) {{ throw 'Dashboard binding file transfer failed.' }}
& scp.exe -i $SshKey -r `
    (Join-Path $ArtifactRoot 'operator') `
    (Join-Path $ArtifactRoot 'scripts') `
    (Join-Path $ArtifactRoot 'systemd') `
    $RemoteTarget
if ($LASTEXITCODE -ne 0) {{ throw 'Dashboard binding directory transfer failed.' }}
Write-Output 'H1_V8_DASHBOARD_BINDING_V2_STEP_A_TRANSFER_GREEN_NOT_INSTALLED'
"""


def admit_dashboard_source_parent(snapshot: Mapping[str, object]) -> str:
    """Model the exact fail-closed V2 parent admission used by the Bash installer."""
    required = {
        "parent_exists",
        "parent_is_directory",
        "parent_is_symlink",
        "parent_is_canonical",
        "parent_same_device",
        "parent_owner",
        "parent_mode",
        "children",
        "v1_is_directory",
        "v1_is_symlink",
        "v1_is_canonical",
        "v1_same_device",
        "v1_owner",
        "v1_mode",
        "v1_empty",
    }
    if set(snapshot) != required:
        raise BindingPackError("dashboard source parent snapshot fields differ")
    if snapshot["parent_exists"] is False:
        if snapshot["children"] != []:
            raise BindingPackError("absent dashboard source parent has children")
        return "CREATE_PARENT_THEN_V2_LEAF"
    if snapshot["parent_exists"] is not True:
        raise BindingPackError("dashboard source parent existence is malformed")
    if (
        snapshot["parent_is_directory"] is not True
        or snapshot["parent_is_symlink"] is not False
        or snapshot["parent_is_canonical"] is not True
        or snapshot["parent_same_device"] is not True
    ):
        raise BindingPackError("dashboard source parent is unsafe")
    if snapshot["parent_mode"] != "700":
        raise BindingPackError("dashboard source parent mode differs")
    children = snapshot["children"]
    if not isinstance(children, list) or any(not isinstance(item, str) for item in children):
        raise BindingPackError("dashboard source parent children are malformed")
    if children not in ([], [EXPECTED_V1_BINDING_NAME]):
        raise BindingPackError("dashboard source parent contains foreign content")
    owner = snapshot["parent_owner"]
    if owner not in {"hyperlab:hyperlab", "root:root"}:
        raise BindingPackError("dashboard source parent owner differs")
    if owner == "root:root" and children != [EXPECTED_V1_BINDING_NAME]:
        raise BindingPackError("root-owned parent is not the exact V1 residue")
    if children == [EXPECTED_V1_BINDING_NAME] and (
        snapshot["v1_is_directory"] is not True
        or snapshot["v1_is_symlink"] is not False
        or snapshot["v1_is_canonical"] is not True
        or snapshot["v1_same_device"] is not True
        or snapshot["v1_owner"] != "hyperlab:hyperlab"
        or snapshot["v1_mode"] != "700"
        or snapshot["v1_empty"] is not True
    ):
        raise BindingPackError("V1 dashboard source residue differs")
    return "REPAIR_PARENT_ONLY_THEN_CREATE_V2_LEAF" if owner == "root:root" else (
        "CREATE_V2_LEAF"
    )


def render_tabby_install(handoff: Mapping[str, object]) -> str:
    checked = validate_handoff(handoff)
    plan = checked["plan"]
    bundle = checked["bundle"]
    assert isinstance(plan, dict) and isinstance(bundle, dict)
    campaign = plan["campaign"]
    dashboard = plan["dashboard"]
    provenance = plan["provenance"]
    assert isinstance(campaign, dict) and isinstance(dashboard, dict) and isinstance(provenance, dict)
    port = dashboard["bind_port"]
    return rf"""#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

# STEP B/3 - LOCATION: Tabby - VPS, Bash, logged in as {dashboard['user']}.
# EXPECTED_DURATION: 10-25 minutes; MAXIMUM_DURATION: 45 minutes.
# PROMPTS: sudo may request the operator password; pip is non-interactive and bounded.
# MONITORING: collector show, parent admission, canonical unit, loopback and GET/HEAD are printed.
# CTRL+C: before enable stops installation; after enable the dashboard remains under systemd.
# TERMINAL_SIGNAL: H1_V8_DASHBOARD_BINDING_V2_STEP_B_INSTALL_GREEN.
fail() {{ printf 'H1_V8_DASHBOARD_BINDING_V2_REFUSED:%s\n' "$1" >&2; exit 4; }}
if (($# == 1)) && [[ $1 == --self-check ]]; then
  printf 'H1_V8_DASHBOARD_BINDING_V2_STEP_B_SELFCHECK_GREEN\n'
  exit 0
fi
(($# == 1)) || fail 'usage: B-tabby-vps-install.sh INVENTORY_SHA256'
EXPECTED_INVENTORY_SHA256=$1
[[ $EXPECTED_INVENTORY_SHA256 =~ ^[0-9a-f]{{64}}$ ]] || fail 'inventory SHA-256 format differs'

SOURCE_COMMIT='{provenance['source_commit']}'
BASE_COMMIT='{provenance['base_launch_commit']}'
BRANCH='{provenance['branch']}'
VOLUME_DEVICE='{EXPECTED_VOLUME_DEVICE}'
VOLUME_MOUNT='{EXPECTED_VOLUME_MOUNT}'
VOLUME_ROOT='{EXPECTED_VOLUME_MOUNT}/hyperlab-h1'
INCOMING_ROOT='{dashboard['incoming_root']}'
DASHBOARD_SOURCE_PARENT='{EXPECTED_VOLUME_MOUNT}/hyperlab-h1/dashboard-sources'
V1_SOURCE_ROOT="$DASHBOARD_SOURCE_PARENT/{EXPECTED_V1_BINDING_NAME}"
DASHBOARD_SOURCE_ROOT='{dashboard['source_root']}'
HANDOFF_PARENT='/etc/hyperlab-h1-dashboard'
HANDOFF_ROOT='{dashboard['handoff_root']}'
CAMPAIGN_ROOT='{campaign['campaign_root']}'
COLLECTOR_SOURCE_ROOT='{campaign['collector_source_root']}'
CAMPAIGN_MANIFEST_SHA256='{campaign['manifest_sha256']}'
COLLECTOR_SERVICE='{campaign['collector_service']}'
DASHBOARD_SERVICE='{dashboard['service_name']}'
BUNDLE="$INCOMING_ROOT/{bundle['filename']}"
UNIT_SOURCE="$INCOMING_ROOT/systemd/$DASHBOARD_SERVICE"
UNIT_TARGET="/etc/systemd/system/$DASHBOARD_SERVICE"

[[ $(id -un) == '{dashboard['user']}' && $HOME == '/home/{dashboard['user']}' ]] \
  || fail 'wrong operator identity'
for command_name in readlink findmnt stat find sort sudo install git sha256sum systemctl curl ss cmp; do
  command -v "$command_name" >/dev/null 2>&1 || fail "missing command: $command_name"
done
for path in "/home/{dashboard['user']}" "/home/{dashboard['user']}/hyperlab-h1" \
    "/home/{dashboard['user']}/hyperlab-h1/dashboard-bindings" "$INCOMING_ROOT"; do
  [[ -d "$path" && ! -L "$path" ]] || fail "incoming ancestor is unsafe: $path"
  [[ $(readlink -f -- "$path") == "$path" ]] || fail "incoming ancestor path differs: $path"
  [[ $(stat -c '%U:%G:%a' "$path") == '{dashboard['user']}:{dashboard['user']}:700' ]] \
    || fail "incoming ancestor owner or mode differs: $path"
done
cd "$INCOMING_ROOT"
printf '%s  %s\n' "$EXPECTED_INVENTORY_SHA256" 'binding-files.sha256' | sha256sum -c -
sha256sum -c binding-files.sha256
[[ -z $(find "$INCOMING_ROOT" -type l -print -quit) ]] || fail 'incoming contains a symlink'

[[ -b "$VOLUME_DEVICE" ]] || fail 'expected volume device is absent'
[[ $(readlink -f -- "$VOLUME_DEVICE") == "$VOLUME_DEVICE" ]] || fail 'volume device path differs'
[[ -d "$VOLUME_MOUNT" && ! -L "$VOLUME_MOUNT" ]] || fail 'volume mount is unsafe'
[[ $(readlink -f -- "$VOLUME_MOUNT") == "$VOLUME_MOUNT" ]] || fail 'volume mount path differs'
[[ $(findmnt -rn -T "$VOLUME_MOUNT" -o TARGET) == "$VOLUME_MOUNT" ]] || fail 'mount target differs'
[[ $(findmnt -rn -T "$VOLUME_MOUNT" -o SOURCE) == "$VOLUME_DEVICE" ]] || fail 'mount device differs'
[[ $(findmnt -rn -T "$VOLUME_MOUNT" -o FSTYPE) == '{EXPECTED_VOLUME_FILESYSTEM}' ]] \
  || fail 'mount filesystem differs'
VOLUME_OPTIONS=$(findmnt -rn -T "$VOLUME_MOUNT" -o OPTIONS)
case ",$VOLUME_OPTIONS," in *,rw,*) ;; *) fail 'volume is not writable for V2 source creation' ;; esac
case ",$VOLUME_OPTIONS," in *,ro,*) fail 'volume unexpectedly read-only' ;; esac
VOLUME_DEV_ID=$(stat -c %d "$VOLUME_MOUNT")

assert_volume_dir() {{
  local path=$1 label=$2
  sudo test -d "$path" && ! sudo test -L "$path" || fail "$label is not a real directory"
  [[ $(sudo readlink -f -- "$path") == "$path" ]] || fail "$label canonical path differs"
  [[ $(sudo stat -c %d "$path") == "$VOLUME_DEV_ID" ]] || fail "$label device differs"
}}
assert_volume_dir "$VOLUME_ROOT" 'volume root'
[[ $(stat -c '%U:%G:%a' "$VOLUME_ROOT") == 'hyperlab:hyperlab:700' ]] \
  || fail 'volume root owner or mode differs'
assert_volume_dir "$CAMPAIGN_ROOT" 'campaign root'
assert_volume_dir "$COLLECTOR_SOURCE_ROOT" 'collector source root'
printf '%s  %s\n' "$CAMPAIGN_MANIFEST_SHA256" "$CAMPAIGN_ROOT/campaign-manifest.json" \
  | sha256sum -c -
[[ $(git -c safe.directory="$COLLECTOR_SOURCE_ROOT" -C "$COLLECTOR_SOURCE_ROOT" rev-parse HEAD) \
    == "$BASE_COMMIT" ]] || fail 'collector source commit differs'
[[ -z $(GIT_OPTIONAL_LOCKS=0 git -c safe.directory="$COLLECTOR_SOURCE_ROOT" \
    -C "$COLLECTOR_SOURCE_ROOT" status --porcelain) ]] || fail 'collector source is not clean'
[[ ! -e "$DASHBOARD_SOURCE_ROOT" && ! -L "$DASHBOARD_SOURCE_ROOT" ]] \
  || fail 'V2 dashboard source collision'
[[ ! -e "$HANDOFF_ROOT" && ! -L "$HANDOFF_ROOT" ]] || fail 'V2 handoff root collision'
[[ ! -e "$UNIT_TARGET" && ! -L "$UNIT_TARGET" ]] || fail 'V2 dashboard service collision'
PORT_LISTENERS=$(ss -H -ltn 'sport = :{port}') || fail 'cannot inspect dashboard port 18080'
[[ -z $PORT_LISTENERS ]] || fail 'dashboard port 18080 is already in use'

# Collector inspection is strictly read-only. No collector lifecycle command exists in this pack.
mapfile -t COLLECTOR_STATE < <(systemctl show "$COLLECTOR_SERVICE" --no-pager \
  --property=Id --property=LoadState --property=ActiveState --property=SubState \
  --property=MainPID --property=NRestarts --property=Result --property=ExecMainStatus)
printf '%s\n' "${{COLLECTOR_STATE[@]}}"
[[ " ${{COLLECTOR_STATE[*]}} " == *" Id=$COLLECTOR_SERVICE "* ]] || fail 'collector identity differs'
[[ " ${{COLLECTOR_STATE[*]}} " == *' LoadState=loaded '* ]] || fail 'collector not loaded'
[[ " ${{COLLECTOR_STATE[*]}} " == *' ActiveState=active '* ]] || fail 'collector not active'
[[ " ${{COLLECTOR_STATE[*]}} " == *' SubState=running '* ]] || fail 'collector not running'

# Create/authenticate the dedicated parent first. Only the exact known V1 residue may be repaired.
PARENT_CREATED=0
if ! sudo test -e "$DASHBOARD_SOURCE_PARENT" && ! sudo test -L "$DASHBOARD_SOURCE_PARENT"; then
  sudo install -d -o '{dashboard['user']}' -g '{dashboard['user']}' -m 0700 \
    "$DASHBOARD_SOURCE_PARENT"
  PARENT_CREATED=1
fi
assert_volume_dir "$DASHBOARD_SOURCE_PARENT" 'dashboard source parent'
PARENT_META=$(sudo stat -c '%U:%G:%a' "$DASHBOARD_SOURCE_PARENT")
mapfile -t PARENT_CHILDREN < <(sudo find "$DASHBOARD_SOURCE_PARENT" -mindepth 1 -maxdepth 1 \
  -printf '%f\n' | LC_ALL=C sort)
if ((PARENT_CREATED)); then
  [[ $PARENT_META == '{dashboard['user']}:{dashboard['user']}:700' ]] \
    || fail 'new dashboard source parent owner or mode differs'
  ((${{#PARENT_CHILDREN[@]}} == 0)) || fail 'new dashboard source parent is not empty'
else
  [[ $PARENT_META == 'root:root:700' || $PARENT_META == '{dashboard['user']}:{dashboard['user']}:700' ]] \
    || fail 'existing dashboard source parent owner or mode differs'
  [[ ${{#PARENT_CHILDREN[@]}} -eq 0 || \
      (${{#PARENT_CHILDREN[@]}} -eq 1 && ${{PARENT_CHILDREN[0]}} == '{EXPECTED_V1_BINDING_NAME}') ]] \
    || fail 'dashboard source parent contains foreign content'
  if [[ $PARENT_META == 'root:root:700' ]]; then
    [[ ${{#PARENT_CHILDREN[@]}} -eq 1 && ${{PARENT_CHILDREN[0]}} == '{EXPECTED_V1_BINDING_NAME}' ]] \
      || fail 'root-owned dashboard parent is not the exact V1 residue'
    assert_volume_dir "$V1_SOURCE_ROOT" 'V1 dashboard source residue before parent repair'
    [[ $(sudo stat -c '%U:%G:%a' "$V1_SOURCE_ROOT") == \
        '{dashboard['user']}:{dashboard['user']}:700' ]] \
      || fail 'V1 dashboard source residue owner or mode differs before parent repair'
    [[ -z $(sudo find "$V1_SOURCE_ROOT" -mindepth 1 -print -quit) ]] \
      || fail 'V1 dashboard source residue contains foreign content before parent repair'
    sudo chown --no-dereference '{dashboard['user']}:{dashboard['user']}' "$DASHBOARD_SOURCE_PARENT"
    sudo chmod 0700 "$DASHBOARD_SOURCE_PARENT"
  fi
fi
[[ $(stat -c '%U:%G:%a' "$DASHBOARD_SOURCE_PARENT") == \
    '{dashboard['user']}:{dashboard['user']}:700' ]] || fail 'dashboard source parent repair differs'

# Preserve and inspect V1 without following it; V2 never reuses or deletes it.
if [[ -e "$V1_SOURCE_ROOT" || -L "$V1_SOURCE_ROOT" ]]; then
  assert_volume_dir "$V1_SOURCE_ROOT" 'V1 dashboard source residue'
  [[ $(stat -c '%U:%G:%a' "$V1_SOURCE_ROOT") == '{dashboard['user']}:{dashboard['user']}:700' ]] \
    || fail 'V1 dashboard source residue owner or mode differs'
  [[ -z $(find "$V1_SOURCE_ROOT" -mindepth 1 -print -quit) ]] \
    || fail 'V1 dashboard source residue contains foreign content'
fi
[[ ! -e "$DASHBOARD_SOURCE_ROOT" && ! -L "$DASHBOARD_SOURCE_ROOT" ]] \
  || fail 'V2 dashboard source collision after parent admission'
sudo install -d -o '{dashboard['user']}' -g '{dashboard['user']}' -m 0700 \
  "$DASHBOARD_SOURCE_ROOT"
assert_volume_dir "$DASHBOARD_SOURCE_ROOT" 'V2 dashboard source leaf'
[[ $(stat -c '%U:%G:%a' "$DASHBOARD_SOURCE_ROOT") == '{dashboard['user']}:{dashboard['user']}:700' ]] \
  || fail 'V2 dashboard source leaf owner or mode differs'
[[ -z $(find "$DASHBOARD_SOURCE_ROOT" -mindepth 1 -print -quit) ]] \
  || fail 'V2 dashboard source leaf is not empty'

git clone --branch "$BRANCH" --single-branch "$BUNDLE" "$DASHBOARD_SOURCE_ROOT"
[[ $(git -C "$DASHBOARD_SOURCE_ROOT" rev-parse HEAD) == "$SOURCE_COMMIT" ]] \
  || fail 'dashboard source HEAD differs'
[[ $(git -C "$DASHBOARD_SOURCE_ROOT" branch --show-current) == "$BRANCH" ]] \
  || fail 'dashboard source branch differs'
[[ -z $(GIT_OPTIONAL_LOCKS=0 git -C "$DASHBOARD_SOURCE_ROOT" status --porcelain) ]] \
  || fail 'dashboard source is not clean'
bash "$DASHBOARD_SOURCE_ROOT/ops/h1_dashboard_binding/bootstrap-linux-v2.sh" \
  "$DASHBOARD_SOURCE_ROOT" "$SOURCE_COMMIT"

VENV_PYTHON="$DASHBOARD_SOURCE_ROOT/.venv/bin/python"
"$VENV_PYTHON" -B "$DASHBOARD_SOURCE_ROOT/ops/h1_dashboard_binding/binding_pack.py" \
  inspect-handoff --handoff "$INCOMING_ROOT/handoff.json" >/dev/null
RENDERED_UNIT="/tmp/h1-v8-dashboard-v2-unit-$PID"
"$VENV_PYTHON" -B "$DASHBOARD_SOURCE_ROOT/ops/h1_dashboard_binding/binding_pack.py" \
  render-unit --plan "$INCOMING_ROOT/binding-plan.json" >"$RENDERED_UNIT"
cmp --silent "$RENDERED_UNIT" "$UNIT_SOURCE" || fail 'canonical unit bytes differ'
unlink "$RENDERED_UNIT"
systemd-analyze verify "$UNIT_SOURCE"
[[ -z $(find "$DASHBOARD_SOURCE_ROOT" -xdev -type l -print -quit) ]] \
  || fail 'dashboard source contains a symlink before freeze'

# Freeze only the V2 dashboard source. V1, campaign and collector stay untouched.
sudo find "$DASHBOARD_SOURCE_ROOT" -xdev -exec chown --no-dereference root:root {{}} +
sudo find "$DASHBOARD_SOURCE_ROOT" -xdev -exec chmod a-w {{}} +
if [[ ! -e "$HANDOFF_PARENT" && ! -L "$HANDOFF_PARENT" ]]; then
  sudo install -d -o root -g root -m 0755 "$HANDOFF_PARENT"
fi
[[ -d "$HANDOFF_PARENT" && ! -L "$HANDOFF_PARENT" ]] || fail 'handoff parent is unsafe'
[[ $(readlink -f -- "$HANDOFF_PARENT") == "$HANDOFF_PARENT" ]] || fail 'handoff parent path differs'
[[ $(stat -c '%U:%G:%a' "$HANDOFF_PARENT") == 'root:root:755' ]] \
  || fail 'handoff parent owner or mode differs'
sudo install -d -o root -g root -m 0555 "$HANDOFF_ROOT"
sudo install -o root -g root -m 0444 "$INCOMING_ROOT/binding-plan.json" \
  "$HANDOFF_ROOT/binding-plan.json"
sudo install -o root -g root -m 0444 "$INCOMING_ROOT/handoff.json" \
  "$HANDOFF_ROOT/handoff.json"
cmp --silent "$INCOMING_ROOT/binding-plan.json" "$HANDOFF_ROOT/binding-plan.json" \
  || fail 'installed binding plan bytes differ'
cmp --silent "$INCOMING_ROOT/handoff.json" "$HANDOFF_ROOT/handoff.json" \
  || fail 'installed handoff bytes differ'
UNIT_TEMP="/etc/systemd/system/.${{DASHBOARD_SERVICE}}.tmp-$PID"
[[ ! -e "$UNIT_TEMP" ]] || fail 'temporary unit path already exists'
sudo install -o root -g root -m 0444 "$UNIT_SOURCE" "$UNIT_TEMP"
sudo ln "$UNIT_TEMP" "$UNIT_TARGET"
sudo unlink "$UNIT_TEMP"
cmp --silent "$UNIT_SOURCE" "$UNIT_TARGET" || fail 'installed unit bytes differ'
sudo systemctl daemon-reload
sudo systemctl enable --now "$DASHBOARD_SERVICE"

mapfile -t DASHBOARD_STATE < <(systemctl show "$DASHBOARD_SERVICE" --no-pager \
  --property=Id --property=LoadState --property=ActiveState --property=SubState \
  --property=MainPID --property=NRestarts --property=FragmentPath)
printf '%s\n' "${{DASHBOARD_STATE[@]}}"
[[ " ${{DASHBOARD_STATE[*]}} " == *" Id=$DASHBOARD_SERVICE "* ]] \
  || fail 'dashboard service identity differs'
[[ " ${{DASHBOARD_STATE[*]}} " == *' LoadState=loaded '* ]] || fail 'dashboard not loaded'
[[ " ${{DASHBOARD_STATE[*]}} " == *' ActiveState=active '* ]] || fail 'dashboard not active'
[[ " ${{DASHBOARD_STATE[*]}} " == *' SubState=running '* ]] || fail 'dashboard not running'
[[ " ${{DASHBOARD_STATE[*]}} " == *' NRestarts=0 '* ]] || fail 'dashboard restart count differs'
[[ " ${{DASHBOARD_STATE[*]}} " == *" FragmentPath=$UNIT_TARGET "* ]] \
  || fail 'dashboard unit path differs'
DASHBOARD_MAIN_PID=''
for _property in "${{DASHBOARD_STATE[@]}}"; do
  case "$_property" in MainPID=*) DASHBOARD_MAIN_PID=${{_property#MainPID=}} ;; esac
done
[[ $DASHBOARD_MAIN_PID =~ ^[1-9][0-9]*$ ]] || fail 'dashboard MainPID is not live'

SNAPSHOT="/tmp/h1-v8-dashboard-v2-snapshot-$PID.json"
for _attempt in $(seq 1 30); do
  if curl --fail --silent --show-error --max-time 2 \
      'http://127.0.0.1:{port}/api/h1/snapshot' >"$SNAPSHOT"; then break; fi
  sleep 1
done
[[ -s "$SNAPSHOT" ]] || fail 'dashboard GET did not become available'
python3.12 -I - "$SNAPSHOT" <<'PY'
import json, pathlib, re, sys
payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
assert payload["mode"] == "readonly"
assert payload["orders_enabled"] is False
assert payload["fixture"] is False
assert payload["identity"]["campaign_id"] == "{campaign['campaign_id']}"
assert payload["identity"]["campaign_slug"] == "{campaign['campaign_slug']}"
assert payload["identity"]["source_commit"] == "{provenance['base_launch_commit']}"
assert payload["identity"]["campaign_manifest_sha256"] == "{campaign['manifest_sha256']}"
assert payload["identity"]["collector_source_commit"] == "{provenance['base_launch_commit']}"
assert payload["identity"]["dashboard_source_commit"] == "{provenance['source_commit']}"
assert payload["state"]["code"] in {{
    "PREPARED_NOT_STARTED", "RUNNING_HEALTHY", "INTERRUPTED_RECOVERABLE"
}}
for field in ("frames", "segments", "stored_bytes", "gaps", "reconnects"):
    value = payload["collection"][field]
    assert value is None or (isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0)
for field in ("raw_manifest_sha256", "raw_root_sha256"):
    value = payload["identity"][field]
    assert value is None or re.fullmatch(r"[0-9a-f]{{64}}", value)
print(json.dumps({{
    "collection": payload["collection"],
    "raw_manifest_sha256": payload["identity"]["raw_manifest_sha256"],
    "raw_root_sha256": payload["identity"]["raw_root_sha256"],
    "state": payload["state"],
}}, sort_keys=True))
PY
unlink "$SNAPSHOT"
HEAD_BODY="/tmp/h1-v8-dashboard-v2-head-$PID"
HEAD_CODE=$(curl --silent --show-error --max-time 3 --request HEAD \
  --output "$HEAD_BODY" --write-out '%{{http_code}}' \
  'http://127.0.0.1:{port}/api/h1/snapshot')
[[ $HEAD_CODE == 200 && ! -s "$HEAD_BODY" ]] || fail 'dashboard HEAD contract differs'
unlink "$HEAD_BODY"
mapfile -t LISTEN_LINES < <(ss -H -ltn 'sport = :{port}')
printf '%s\n' "${{LISTEN_LINES[@]}}"
[[ ${{#LISTEN_LINES[@]}} -eq 1 ]] || fail 'dashboard listener count differs'
[[ ${{LISTEN_LINES[0]}} == *'127.0.0.1:{port}'* ]] || fail 'dashboard is not IPv4 loopback-only'
[[ ${{LISTEN_LINES[0]}} != *'0.0.0.0:{port}'* && ${{LISTEN_LINES[0]}} != *'[::]'* ]] \
  || fail 'dashboard has a non-loopback listener'
printf 'H1_V8_DASHBOARD_BINDING_V2_STEP_B_INSTALL_GREEN\n'

# SECOND TABBY TAB, continuous read-only monitoring; Ctrl+C stops only watch:
# watch -n 10 -- sh -c "systemctl show '$DASHBOARD_SERVICE' --no-pager \
#   --property=ActiveState --property=SubState --property=MainPID --property=NRestarts; \
#   curl --fail --silent --max-time 3 http://127.0.0.1:{port}/api/h1/snapshot"
"""


def render_windows_tunnel(plan: Mapping[str, object]) -> str:
    checked = validate_plan(plan)
    dashboard = checked["dashboard"]
    assert isinstance(dashboard, dict)
    port = dashboard["bind_port"]
    return rf"""[CmdletBinding()]
param([switch] $SelfCheck)

# STEP C/3 - LOCATION: Windows PowerShell 5.1 on the Beelink.
# PREREQUISITE: Tabby printed H1_V8_DASHBOARD_BINDING_V2_STEP_B_INSTALL_GREEN.
# EXPECTED_DURATION: continuous foreground session; MAXIMUM_DURATION: operator-controlled.
# PROMPTS: SSH host-key trust or SSH-key passphrase only; HyperLab never prompts.
# MONITORING: browser http://127.0.0.1:{port}; SSH keepalives detect a dead tunnel.
# CTRL+C: closes only this tunnel; it never stops or restarts either VPS service.
# TERMINAL_SIGNAL: ssh remains foreground; a successful browser GET is the signal.
$ErrorActionPreference = 'Stop'
if ($SelfCheck) {{
    Write-Output 'H1_V8_DASHBOARD_BINDING_V2_STEP_C_SELFCHECK_GREEN'
    return
}}
$SshKey = "$env:USERPROFILE\.ssh\hyperlab_hetzner"
if ((Get-NetTCPConnection -LocalPort {port} -State Listen -ErrorAction SilentlyContinue)) {{
    throw 'Local port already used.'
}}
& ssh.exe -N -T -i $SshKey `
    -o ClearAllForwardings=yes `
    -o ExitOnForwardFailure=yes `
    -o ServerAliveInterval=30 `
    -o ServerAliveCountMax=3 `
    -L '127.0.0.1:{port}:127.0.0.1:{port}' `
    '{dashboard['user']}@{dashboard['remote_host']}'
"""


def _write_exclusive(path: Path, payload: bytes, *, executable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
        0o700 if executable else 0o600,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise BindingPackError(f"short write: {path}")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def validate_lf_shell_bytes(payload: bytes, *, label: str) -> bytes:
    if not payload or b"\x00" in payload or b"\r" in payload:
        raise BindingPackError(f"{label} must be non-empty LF-only shell bytes")
    if payload.startswith((b"\xef\xbb\xbf", b"\xff\xfe", b"\xfe\xff")):
        raise BindingPackError(f"{label} must be UTF-8 without BOM")
    try:
        payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise BindingPackError(f"{label} must be UTF-8") from error
    return payload


def finalize_binding_pack(
    *,
    repo_root: Path,
    input_path: Path,
    bundle_path: Path,
    output_root: Path,
    source_commit: str,
) -> dict[str, object]:
    expected_input = repo_root / "ops" / "h1_dashboard_binding" / INPUT_FILENAME
    if input_path != expected_input or input_path.resolve(strict=True) != input_path:
        raise BindingPackError("binding input path differs from the tracked frozen V8 input")
    binding_input = validate_frozen_v8_input(_load_object(input_path))
    plan = build_final_plan(binding_input, source_commit)
    _validate_source_causality(repo_root, plan=plan, bundle_path=bundle_path)
    if output_root.is_symlink() or not output_root.is_dir():
        raise BindingPackError("output root must be the new real bundle directory")
    if bundle_path.parent != output_root or {path.name for path in output_root.iterdir()} != {
        bundle_path.name
    }:
        raise BindingPackError("output root must initially contain only the exact Git bundle")
    handoff = build_handoff(repo_root=repo_root, plan=plan, bundle_path=bundle_path)
    dashboard = plan["dashboard"]
    assert isinstance(dashboard, dict)
    _write_exclusive(
        output_root / INPUT_FILENAME, canonical_json_bytes(binding_input) + b"\n"
    )
    _write_exclusive(output_root / "binding-plan.json", canonical_json_bytes(plan) + b"\n")
    _write_exclusive(output_root / "handoff.json", canonical_json_bytes(handoff) + b"\n")
    _write_exclusive(
        output_root / "README.md",
        portable_git_file_bytes(repo_root, "ops/h1_dashboard_binding/README.md"),
    )
    _write_exclusive(
        output_root / "scripts" / "bootstrap-linux-v2.sh",
        validate_lf_shell_bytes(
            portable_git_file_bytes(
                repo_root, "ops/h1_dashboard_binding/bootstrap-linux-v2.sh"
            ),
            label="bootstrap-linux-v2.sh",
        ),
        executable=True,
    )
    _write_exclusive(
        output_root / "systemd" / str(dashboard["service_name"]),
        render_systemd_unit(plan).encode("utf-8"),
    )
    operator_paths = (
        output_root / "operator" / "A-windows-transfer.ps1",
        output_root / "operator" / "B-tabby-vps-install.sh",
        output_root / "operator" / "C-windows-tunnel.ps1",
    )
    operator_payloads = (
        render_windows_transfer(handoff).encode("utf-8"),
        render_tabby_install(handoff).encode("utf-8"),
        render_windows_tunnel(plan).encode("utf-8"),
    )
    for path, payload in zip(operator_paths, operator_payloads, strict=True):
        _write_exclusive(
            path,
            validate_lf_shell_bytes(payload, label=path.name),
            executable=path.suffix == ".sh",
        )
    for shell_path in (
        output_root / "scripts" / "bootstrap-linux-v2.sh",
        *operator_paths,
    ):
        validate_lf_shell_bytes(shell_path.read_bytes(), label=f"materialized {shell_path.name}")
    essential_paths = (
        bundle_path,
        output_root / "README.md",
        output_root / INPUT_FILENAME,
        output_root / "binding-plan.json",
        output_root / "handoff.json",
        output_root / "scripts" / "bootstrap-linux-v2.sh",
        output_root / "systemd" / str(dashboard["service_name"]),
        *operator_paths,
    )
    inventory = [
        f"{sha256_file(path)}  {path.relative_to(output_root).as_posix()}"
        for path in sorted(essential_paths)
    ]
    inventory_path = output_root / "binding-files.sha256"
    _write_exclusive(
        inventory_path,
        ("\n".join(inventory) + "\n").encode("ascii"),
    )
    inventory_sha256 = sha256_file(inventory_path)
    _validate_source_causality(repo_root, plan=plan, bundle_path=bundle_path)
    return {
        "bundle_sha256": sha256_file(bundle_path),
        "file_count": sum(1 for path in output_root.rglob("*") if path.is_file()),
        "inventory_sha256": inventory_sha256,
        "source_commit": source_commit,
        "status": STATUS,
    }


def _stable_regular_file(path: Path, *, maximum_bytes: int) -> bytes:
    before = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size > maximum_bytes:
        raise BindingPackError(f"unsafe bounded file: {path}")
    with path.open("rb") as handle:
        opened_before = os.fstat(handle.fileno())
        payload = handle.read(maximum_bytes + 1)
        opened_after = os.fstat(handle.fileno())
    after = path.lstat()
    identities = {
        (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns)
        for item in (before, opened_before, opened_after, after)
    }
    if len(identities) != 1 or len(payload) > maximum_bytes:
        raise BindingPackError(f"file changed while read: {path}")
    return payload


def _assert_root_owned_readonly_tree(root: Path) -> None:
    resolved_root = root.resolve(strict=True)
    if resolved_root != root or root.is_symlink() or not root.is_dir():
        raise BindingPackError("dashboard source root is not an exact real directory")
    for path in (root, *root.rglob("*")):
        details = path.lstat()
        if details.st_uid != 0:
            raise BindingPackError(f"dashboard source entry is not root-owned: {path}")
        if path.is_symlink():
            try:
                path.resolve(strict=True).relative_to(resolved_root)
            except (OSError, ValueError) as error:
                raise BindingPackError(f"dashboard source link escapes: {path}") from error
        elif details.st_mode & 0o222:
            raise BindingPackError(f"dashboard source entry remains writable: {path}")


def _validate_collector_checkout(root: Path, expected_commit: str) -> None:
    if root.resolve(strict=True) != root or root.is_symlink() or not root.is_dir():
        raise BindingPackError("collector source root is not an exact real directory")
    if _git_output(root, "rev-parse", "HEAD") != expected_commit:
        raise BindingPackError("collector source commit differs")
    if _git_output(root, "status", "--porcelain"):
        raise BindingPackError("collector source checkout is not clean")


def _systemctl_show_collector(service: str) -> dict[str, str]:
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            service,
            "--no-pager",
            "--property=Id",
            "--property=LoadState",
            "--property=ActiveState",
            "--property=SubState",
            "--property=MainPID",
            "--property=NRestarts",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise BindingPackError("collector systemctl show failed")
    result: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        key, separator, value = line.partition("=")
        if not separator or not key or key in result:
            raise BindingPackError("collector systemctl show returned malformed output")
        result[key] = value
    return result


def run_service_preflight(plan_path: Path) -> dict[str, object]:
    plan = validate_plan(_load_object(plan_path))
    campaign = plan["campaign"]
    dashboard = plan["dashboard"]
    provenance = plan["provenance"]
    assert isinstance(campaign, dict) and isinstance(dashboard, dict) and isinstance(provenance, dict)
    expected_plan = Path(str(dashboard["handoff_root"])) / "binding-plan.json"
    if plan_path != expected_plan or plan_path.resolve(strict=True) != plan_path:
        raise BindingPackError("service plan path differs from the root-owned exact path")
    plan_details = plan_path.lstat()
    if (
        plan_path.is_symlink()
        or not stat.S_ISREG(plan_details.st_mode)
        or plan_details.st_uid != 0
        or plan_details.st_mode & 0o222
    ):
        raise BindingPackError("service plan must be a root-owned read-only regular file")
    getuid = getattr(os, "getuid", None)
    if getuid is None:
        raise BindingPackError("dashboard service preflight requires Linux")
    if getuid() == 0:
        raise BindingPackError("dashboard service must be unprivileged")
    for name in FORBIDDEN_ENVIRONMENT:
        if os.getenv(name):
            raise BindingPackError(f"forbidden environment is populated: {name}")
    source_root = Path(str(dashboard["source_root"]))
    _assert_root_owned_readonly_tree(source_root)
    if _git_output(source_root, "rev-parse", "HEAD") != provenance["source_commit"]:
        raise BindingPackError("dashboard source commit differs")
    if _git_output(source_root, "branch", "--show-current") != provenance["branch"]:
        raise BindingPackError("dashboard source branch differs")
    if _git_output(source_root, "status", "--porcelain"):
        raise BindingPackError("dashboard source checkout is not clean")
    _validate_collector_checkout(
        Path(str(campaign["collector_source_root"])),
        str(provenance["base_launch_commit"]),
    )
    campaign_root = Path(str(campaign["campaign_root"]))
    if campaign_root.resolve(strict=True) != campaign_root or campaign_root.is_symlink():
        raise BindingPackError("campaign root is not an exact real path")
    manifest_bytes = _stable_regular_file(
        campaign_root / "campaign-manifest.json", maximum_bytes=1024 * 1024
    )
    if sha256_bytes(manifest_bytes) != campaign["manifest_sha256"]:
        raise BindingPackError("campaign manifest SHA-256 differs")
    try:
        manifest = json.loads(manifest_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BindingPackError("campaign manifest is invalid JSON") from error
    if not isinstance(manifest, dict):
        raise BindingPackError("campaign manifest must be an object")
    for key, expected in campaign["manifest_checks"].items():
        if manifest.get(key) != expected:
            raise BindingPackError(f"campaign manifest {key} differs")
    statvfs = getattr(os, "statvfs", None)
    if statvfs is None:
        raise BindingPackError("dashboard service preflight requires statvfs")
    readonly_flag = getattr(os, "ST_RDONLY", 1)
    if not statvfs(campaign_root).f_flag & readonly_flag:
        raise BindingPackError("campaign root is not read-only inside the dashboard namespace")
    properties = _systemctl_show_collector(str(campaign["collector_service"]))
    if properties.get("Id") != campaign["collector_service"]:
        raise BindingPackError("collector identity differs")
    if properties.get("LoadState") != "loaded":
        raise BindingPackError("collector is not loaded")
    if properties.get("ActiveState") != "active" or properties.get("SubState") != "running":
        raise BindingPackError("collector is not active/running")
    if not properties.get("MainPID", "").isdigit() or properties.get("MainPID") == "0":
        raise BindingPackError("collector MainPID is not live")
    if not properties.get("NRestarts", "").isdigit():
        raise BindingPackError("collector NRestarts is malformed")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", int(dashboard["bind_port"])))
    return {
        "campaign_id": campaign["campaign_id"],
        "dashboard_source_commit": provenance["source_commit"],
        "listen": f"127.0.0.1:{dashboard['bind_port']}",
        "mode": "readonly",
        "orders_enabled": False,
        "status": "H1_DASHBOARD_SERVICE_PREFLIGHT_GREEN",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="H1 V8 read-only dashboard binding pack")
    commands = parser.add_subparsers(dest="command", required=True)
    finalize = commands.add_parser("finalize")
    finalize.add_argument("--repo-root", type=Path, required=True)
    finalize.add_argument("--input", type=Path, required=True)
    finalize.add_argument("--bundle", type=Path, required=True)
    finalize.add_argument("--output-root", type=Path, required=True)
    finalize.add_argument("--source-commit", required=True)
    inspect_input = commands.add_parser("inspect-input")
    inspect_input.add_argument("--input", type=Path, required=True)
    inspect = commands.add_parser("inspect-plan")
    inspect.add_argument("--plan", type=Path, required=True)
    inspect_handoff = commands.add_parser("inspect-handoff")
    inspect_handoff.add_argument("--handoff", type=Path, required=True)
    render_unit = commands.add_parser("render-unit")
    render_unit.add_argument("--plan", type=Path, required=True)
    render_tunnel = commands.add_parser("render-tunnel")
    render_tunnel.add_argument("--plan", type=Path, required=True)
    preflight = commands.add_parser("service-preflight")
    preflight.add_argument("--plan", type=Path, required=True)
    return parser


def _fail(message: str) -> NoReturn:
    print(json.dumps({"error": message, "status": "H1_DASHBOARD_BINDING_REFUSED"}, sort_keys=True))
    raise SystemExit(4)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "finalize":
            result: object = finalize_binding_pack(
                repo_root=arguments.repo_root.resolve(),
                input_path=arguments.input.resolve(),
                bundle_path=arguments.bundle.resolve(),
                output_root=arguments.output_root.resolve(),
                source_commit=arguments.source_commit,
            )
        elif arguments.command == "inspect-input":
            result = {
                "input": validate_frozen_v8_input(_load_object(arguments.input.resolve())),
                "status": STATUS,
            }
        elif arguments.command == "inspect-plan":
            result = {
                "plan": validate_plan(_load_object(arguments.plan.resolve())),
                "status": STATUS,
            }
        elif arguments.command == "inspect-handoff":
            result = {
                "handoff": validate_handoff(_load_object(arguments.handoff.resolve())),
                "status": STATUS,
            }
        elif arguments.command == "render-unit":
            result = render_systemd_unit(_load_object(arguments.plan.resolve()))
        elif arguments.command == "render-tunnel":
            result = render_windows_tunnel(_load_object(arguments.plan.resolve()))
        elif arguments.command == "service-preflight":
            result = run_service_preflight(arguments.plan.resolve())
        else:
            raise BindingPackError("unsupported command")
    except (BindingPackError, OSError, subprocess.SubprocessError) as error:
        _fail(str(error))
    if isinstance(result, str):
        print(result, end="")
    else:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
