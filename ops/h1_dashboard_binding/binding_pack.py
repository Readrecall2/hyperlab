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
STATUS: Final = "H1_DASHBOARD_GENERIC_INTEGRATION_READY_AWAITING_V8_IDENTITY"
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
        ["git", "-C", str(repo_root), *arguments],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    if completed.returncode != 0:
        raise BindingPackError(completed.stderr.strip() or "git command failed")
    return completed.stdout.strip()


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


def validate_plan(plan: Mapping[str, object]) -> dict[str, object]:
    """Validate a complete external identity; this module ships no campaign identity."""
    if set(plan) != {
        "boundary",
        "campaign",
        "dashboard",
        "provenance",
        "schema_version",
    } or plan.get("schema_version") != 1:
        raise BindingPackError("binding plan fields or schema version differ from v1")
    if plan.get("boundary") != BOUNDARY:
        raise BindingPackError("binding plan safety boundary differs")

    provenance = plan.get("provenance")
    if not isinstance(provenance, dict) or set(provenance) != {
        "base_launch_commit",
        "branch",
        "dashboard_integration_commit",
        "dashboard_original_commit",
        "source_commit",
    }:
        raise BindingPackError("provenance fields differ from v1")
    _required_match(provenance.get("branch"), label="provenance.branch", pattern=BRANCH_RE)
    commits = [
        _required_commit(provenance.get(key), label=f"provenance.{key}")
        for key in (
            "base_launch_commit",
            "dashboard_original_commit",
            "dashboard_integration_commit",
            "source_commit",
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
    checks = _manifest_checks(campaign.get("manifest_checks"))
    if checks.get("campaign_id") != campaign_id or checks.get("boundary") != BOUNDARY:
        raise BindingPackError("manifest checks must bind campaign ID and safety boundary")

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
    }


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


def render_windows_tunnel(plan: Mapping[str, object]) -> str:
    checked = validate_plan(plan)
    dashboard = checked["dashboard"]
    assert isinstance(dashboard, dict)
    port = dashboard["bind_port"]
    return rf"""# LOCATION: Windows PowerShell on the Beelink, after human VPS installation is green.
# EXPECTED_DURATION: continuous foreground session; MAXIMUM_DURATION: operator-controlled.
# PROMPTS: SSH host-key trust or SSH-key passphrase only; HyperLab never prompts.
# MONITORING: browser http://127.0.0.1:{port}; SSH keepalives detect a dead tunnel.
# CTRL+C: closes only this tunnel; it never stops or restarts either VPS service.
# TERMINAL_SIGNAL: ssh remains foreground; a successful browser GET is the signal.
$ErrorActionPreference = 'Stop'
$SshKey = "$env:USERPROFILE\.ssh\hyperlab_hetzner"
if ((Get-NetTCPConnection -LocalAddress 127.0.0.1 -LocalPort {port} `
        -State Listen -ErrorAction SilentlyContinue)) {{ throw 'Local endpoint already used.' }}
& ssh.exe -N -T -i $SshKey `
    -o ClearAllForwardings=yes `
    -o ExitOnForwardFailure=yes `
    -o ServerAliveInterval=30 `
    -o ServerAliveCountMax=3 `
    -L '127.0.0.1:{port}:127.0.0.1:{port}' `
    '{dashboard['user']}@{dashboard['remote_host']}'
"""


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
    if os.getuid() == 0:
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
    readonly_flag = getattr(os, "ST_RDONLY", 1)
    if not os.statvfs(campaign_root).f_flag & readonly_flag:
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
    parser = argparse.ArgumentParser(description="reusable H1 read-only dashboard binding")
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect-plan")
    inspect.add_argument("--plan", type=Path, required=True)
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
        if arguments.command == "inspect-plan":
            result: object = {
                "plan": validate_plan(_load_object(arguments.plan.resolve())),
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
