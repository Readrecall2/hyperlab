from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import ipaddress
import json
import os
import platform
import shutil
import socket
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn
from uuid import uuid4

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
_MAX_JSON_BYTES = 8 * 1024 * 1024
_NETWORK_TIMEOUT_SECONDS = 3.0
_COMMAND_TIMEOUT_SECONDS = 20.0
_SHA256 = frozenset("0123456789abcdef")


class PreflightError(RuntimeError):
    """Fail-closed target preflight error."""


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc_now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _safe_regular_bytes(path: Path, *, maximum_bytes: int = _MAX_JSON_BYTES) -> bytes:
    try:
        before = path.lstat()
    except OSError as error:
        raise PreflightError(f"required file is unreadable: {path}") from error
    if path.is_symlink() or not path.is_file() or before.st_size > maximum_bytes:
        raise PreflightError(f"required file is unsafe: {path}")
    raw = path.read_bytes()
    after = path.lstat()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise PreflightError(f"file changed while it was authenticated: {path}")
    if len(raw) != before.st_size:
        raise PreflightError(f"short read: {path}")
    return raw


def _object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(_safe_regular_bytes(path).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError(f"invalid JSON object: {path}") from error
    if not isinstance(value, dict):
        raise PreflightError(f"JSON root must be an object: {path}")
    return value


def _validate_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in _SHA256 for char in value):
        raise PreflightError(f"{label} is not a lowercase SHA-256")
    return value


def load_handoff(path: Path) -> dict[str, Any]:
    handoff = _object(path)
    pin_path = path.with_name("handoff.sha256")
    pin = _safe_regular_bytes(pin_path, maximum_bytes=256).decode("ascii").strip().split()
    if len(pin) != 2 or pin[1] != path.name:
        raise PreflightError("handoff pin is malformed")
    if sha256_bytes(_safe_regular_bytes(path)) != pin[0]:
        raise PreflightError("handoff physical SHA-256 diverged")
    if handoff.get("boundary") != BOUNDARY or handoff.get("schema_version") != 1:
        raise PreflightError("handoff boundary or schema diverged")
    return handoff


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str]], CommandResult]


def _command(arguments: Sequence[str]) -> CommandResult:
    completed = subprocess.run(
        list(arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=_COMMAND_TIMEOUT_SECONDS,
    )
    return CommandResult(completed.returncode, completed.stdout.strip(), completed.stderr.strip())


def _required_command(name: str) -> str:
    value = shutil.which(name)
    if value is None:
        raise PreflightError(f"required offline command is absent: {name}")
    return value


def verify_transfer_inventory(incoming_root: Path, handoff: Mapping[str, object]) -> dict[str, object]:
    if incoming_root.is_symlink() or not incoming_root.is_dir():
        raise PreflightError("incoming root is absent or unsafe")
    if incoming_root.resolve(strict=True) != incoming_root:
        raise PreflightError("incoming root real path differs")
    declared = handoff.get("transfer_inventory_sha256")
    _validate_sha256(declared, label="transfer inventory hash")
    inventory_path = incoming_root / "transfer-inventory.json"
    raw_inventory = _safe_regular_bytes(inventory_path)
    if sha256_bytes(raw_inventory) != declared:
        raise PreflightError("transfer inventory hash diverged")
    try:
        inventory = json.loads(raw_inventory.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError("transfer inventory is invalid") from error
    if (
        not isinstance(inventory, dict)
        or set(inventory) != {"files", "schema_version"}
        or inventory.get("schema_version") != 1
        or not isinstance(inventory.get("files"), list)
    ):
        raise PreflightError("transfer inventory schema diverged")
    checked = 0
    seen_paths: set[str] = set()
    for item in inventory["files"]:
        if not isinstance(item, dict) or set(item) != {"path", "sha256", "size"}:
            raise PreflightError("transfer inventory entry is invalid")
        relative = item.get("path")
        expected = _validate_sha256(item.get("sha256"), label="transfer file hash")
        expected_size = item.get("size")
        if (
            not isinstance(relative, str)
            or not relative
            or "\\" in relative
            or relative in seen_paths
            or type(expected_size) is not int
            or expected_size < 0
        ):
            raise PreflightError("transfer inventory path is invalid")
        seen_paths.add(relative)
        pure = PurePosixPath(relative)
        if pure.is_absolute() or ".." in pure.parts:
            raise PreflightError("transfer inventory path escapes incoming root")
        candidate = incoming_root.joinpath(*pure.parts)
        if candidate.resolve(strict=True).parent != candidate.parent.resolve(strict=True):
            raise PreflightError("transfer inventory file uses a symlink")
        payload = _safe_regular_bytes(candidate, maximum_bytes=512 * 1024 * 1024)
        if len(payload) != expected_size or sha256_bytes(payload) != expected:
            raise PreflightError(f"transfer file hash diverged: {relative}")
        checked += 1
    return {"files_checked": checked, "inventory_sha256": declared}


def verify_wheelhouse(incoming_root: Path, handoff: Mapping[str, object]) -> dict[str, object]:
    manifest_path = incoming_root / "wheelhouse.sha256"
    raw = _safe_regular_bytes(manifest_path, maximum_bytes=1024 * 1024)
    expected_manifest = _validate_sha256(
        handoff.get("wheelhouse_manifest_sha256"), label="wheelhouse manifest hash"
    )
    if sha256_bytes(raw) != expected_manifest:
        raise PreflightError("wheelhouse manifest hash diverged")
    wheelhouse = incoming_root / "wheelhouse"
    if wheelhouse.is_symlink() or not wheelhouse.is_dir():
        raise PreflightError("wheelhouse is absent or unsafe")
    count = 0
    total = 0
    declared_names: set[str] = set()
    for line in raw.decode("ascii").splitlines():
        fields = line.split("  ", maxsplit=1)
        if len(fields) != 2:
            raise PreflightError("wheelhouse manifest line is malformed")
        expected = _validate_sha256(fields[0], label="wheel hash")
        name = fields[1]
        if not name.endswith(".whl") or Path(name).name != name or name in declared_names:
            raise PreflightError("wheelhouse filename is invalid")
        declared_names.add(name)
        wheel = wheelhouse / name
        payload = _safe_regular_bytes(wheel, maximum_bytes=512 * 1024 * 1024)
        if sha256_bytes(payload) != expected:
            raise PreflightError(f"wheel hash diverged: {name}")
        count += 1
        total += len(payload)
    if count == 0:
        raise PreflightError("wheelhouse is empty")
    entries = list(wheelhouse.iterdir())
    actual_names = {
        item.name for item in entries if item.is_file() and not item.is_symlink()
    }
    if actual_names != declared_names or len(entries) != len(declared_names):
        raise PreflightError("wheelhouse contains undeclared or unsafe entries")
    return {"bytes": total, "files": count, "manifest_sha256": expected_manifest}


def verify_git_bundle(
    incoming_root: Path,
    handoff: Mapping[str, object],
    run: CommandRunner,
) -> dict[str, object]:
    bundle = incoming_root / str(handoff["bundle_filename"])
    expected = _validate_sha256(handoff.get("bundle_sha256"), label="Git bundle hash")
    payload = _safe_regular_bytes(bundle, maximum_bytes=512 * 1024 * 1024)
    if sha256_bytes(payload) != expected:
        raise PreflightError("Git bundle SHA-256 diverged")
    verify_root = incoming_root / f".git-bundle-verify-{uuid4().hex}"
    verify_root.mkdir(mode=0o700, exist_ok=False)
    try:
        initialized = run(["git", "init", "--bare", "--quiet", str(verify_root)])
        if initialized.returncode != 0:
            raise PreflightError(
                f"temporary bare repository initialization failed: {initialized.stderr}"
            )
        verified = run(
            ["git", "-C", str(verify_root), "bundle", "verify", str(bundle)]
        )
        if verified.returncode != 0:
            raise PreflightError(f"Git bundle verification failed: {verified.stderr}")
    finally:
        if (
            verify_root.is_symlink()
            or verify_root.resolve(strict=True) != verify_root
            or verify_root.parent != incoming_root
            or not verify_root.name.startswith(".git-bundle-verify-")
        ):
            raise PreflightError("refusing unsafe Git bundle verification cleanup")
        shutil.rmtree(verify_root)
    return {
        "sha256": expected,
        "temporary_repository_removed": not verify_root.exists(),
        "verified": True,
    }


def _dns(host: str) -> list[str]:
    try:
        completed = subprocess.run(
            ["getent", "ahosts", host],
            check=False,
            capture_output=True,
            text=True,
            timeout=_NETWORK_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise TimeoutError(f"DNS lookup exceeded {_NETWORK_TIMEOUT_SECONDS:g}s") from error
    if completed.returncode != 0:
        raise LookupError(completed.stderr.strip() or "getent returned no address")
    addresses: list[str] = []
    for line in completed.stdout.splitlines():
        fields = line.split()
        if not fields:
            continue
        try:
            addresses.append(str(ipaddress.ip_address(fields[0])))
        except ValueError:
            continue
    addresses = sorted(set(addresses))
    if not addresses:
        raise LookupError("no address returned")
    return addresses[:8]


def _tls(host: str) -> str:
    context = ssl.create_default_context()
    with (
        socket.create_connection((host, 443), timeout=_NETWORK_TIMEOUT_SECONDS) as plain,
        context.wrap_socket(plain, server_hostname=host) as secured,
    ):
        certificate = secured.getpeercert()
        if not certificate:
            raise ssl.SSLError("peer certificate is absent")
        return secured.version() or "TLS_VERSION_UNKNOWN"


def _https_get(url: str) -> int:
    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(
            self,
            req: urllib.request.Request,
            fp: object,
            code: int,
            msg: str,
            headers: object,
            newurl: str,
        ) -> urllib.request.Request | None:
            return None

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "HyperLab-Public-Preflight/1"},
        method="GET",
    )
    try:
        with opener.open(request, timeout=_NETWORK_TIMEOUT_SECONDS) as response:
            response.read(4096)
            return int(response.status)
    except urllib.error.HTTPError as error:
        return int(error.code)


def _wss_handshake(host: str, path: str) -> int:
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    context = ssl.create_default_context()
    with (
        socket.create_connection((host, 443), timeout=_NETWORK_TIMEOUT_SECONDS) as plain,
        context.wrap_socket(plain, server_hostname=host) as secured,
    ):
        secured.settimeout(_NETWORK_TIMEOUT_SECONDS)
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Connection: Upgrade\r\n"
            "Upgrade: websocket\r\n"
            "Origin: https://polymarket.com\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        secured.sendall(request)
        response = secured.recv(4096).split(b"\r\n", maxsplit=1)[0]
    fields = response.decode("ascii", errors="replace").split()
    if len(fields) < 2 or not fields[1].isdigit():
        raise ConnectionError("invalid WebSocket HTTP response")
    return int(fields[1])


def probe_venue_connectivity(
    venue: str,
    *,
    dns_probe: Callable[[str], list[str]] = _dns,
    tls_probe: Callable[[str], str] = _tls,
    https_probe: Callable[[str], int] = _https_get,
    wss_probe: Callable[[str, str], int] = _wss_handshake,
) -> dict[str, object]:
    hosts: tuple[str, ...]
    if venue == "polymarket":
        hosts = (
            "gamma-api.polymarket.com",
            "clob.polymarket.com",
            "data-api.polymarket.com",
            "ws-subscriptions-clob.polymarket.com",
        )
        https_url = "https://gamma-api.polymarket.com/markets/keyset?limit=1"
    elif venue == "kalshi":
        hosts = ("external-api.kalshi.com",)
        https_url = "https://external-api.kalshi.com/trade-api/v2/markets?limit=1"
    else:
        raise PreflightError("unknown prediction venue")
    started = time.monotonic()
    errors: list[str] = []
    dns: dict[str, object] = {}
    tls: dict[str, object] = {}
    for host in hosts:
        try:
            dns[host] = dns_probe(host)
        except (LookupError, OSError, socket.gaierror) as error:
            dns[host] = None
            errors.append(f"DNS:{host}:{type(error).__name__}:{error}")
            continue
        try:
            tls[host] = tls_probe(host)
        except (ConnectionError, OSError, ssl.SSLError, TimeoutError) as error:
            tls[host] = None
            errors.append(f"TLS:{host}:{type(error).__name__}:{error}")
    https_status: int | None = None
    if not errors:
        try:
            https_status = https_probe(https_url)
            if https_status in {403, 429}:
                errors.append(f"HTTPS_STATUS_{https_status}")
            elif not 200 <= https_status < 300:
                errors.append(f"HTTPS_UNEXPECTED_STATUS_{https_status}")
        except (ConnectionError, OSError, TimeoutError, urllib.error.URLError) as error:
            errors.append(f"HTTPS:{type(error).__name__}:{error}")
    wss: dict[str, object]
    if venue == "polymarket" and not errors:
        try:
            status = wss_probe("ws-subscriptions-clob.polymarket.com", "/ws/market")
            wss = {"documented_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market", "status": status}
            if status != http.client.SWITCHING_PROTOCOLS:
                errors.append(f"WSS_UNEXPECTED_STATUS_{status}")
        except (ConnectionError, OSError, ssl.SSLError, TimeoutError) as error:
            wss = {"documented_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market", "status": None}
            errors.append(f"WSS:{type(error).__name__}:{error}")
    elif venue == "kalshi":
        wss = {
            "documented_status": "AUTHENTICATED_HANDSHAKE_REQUIRED",
            "probe": "NOT_EXECUTED_CREDENTIALS_FORBIDDEN",
            "status": None,
        }
    else:
        wss = {"status": None}
    return {
        "attempts": 1,
        "dns": dns,
        "elapsed_ms": int((time.monotonic() - started) * 1000),
        "errors": errors,
        "https": {"status": https_status, "url": https_url},
        "max_duration_seconds": 40,
        "proxy_policy": "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED",
        "tls": tls,
        "venue": venue,
        "verdict": "NETWORK_PREFLIGHT_GREEN" if not errors else "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT",
        "wss": wss,
    }


def _parse_df_available(output: str) -> int:
    lines = [line for line in output.splitlines() if line.strip()]
    if len(lines) < 2:
        raise PreflightError("df output is incomplete")
    fields = lines[-1].split()
    if len(fields) < 4 or not fields[3].isdigit():
        raise PreflightError("df available bytes are invalid")
    return int(fields[3])


def _systemd_collision(service: str, run: CommandRunner) -> dict[str, object]:
    result = run(
        [
            "systemctl",
            "show",
            service,
            "--property=LoadState,ActiveState,SubState,MainPID",
            "--no-pager",
        ]
    )
    values: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key] = value
    load = values.get("LoadState", "not-found")
    active = values.get("ActiveState", "inactive")
    if load not in {"", "not-found"} or active == "active":
        raise PreflightError(f"service collision: {service} load={load} active={active}")
    return {"active_state": active, "load_state": load, "service": service}


def host_preflight(
    handoff_path: Path,
    *,
    run: CommandRunner = _command,
    connectivity_probe: Callable[[str], dict[str, object]] = probe_venue_connectivity,
) -> dict[str, object]:
    handoff = load_handoff(handoff_path)
    incoming = handoff_path.parent.resolve(strict=True)
    errors: list[str] = []
    checks: dict[str, object] = {}
    try:
        checks["transfer"] = verify_transfer_inventory(incoming, handoff)
        checks["wheelhouse"] = verify_wheelhouse(incoming, handoff)
        if os.environ.get("USER") != handoff.get("service_user"):
            raise PreflightError("preflight must run as the dedicated service user")
        if Path.home().as_posix() != f"/home/{handoff.get('service_user')}":
            raise PreflightError("service user HOME diverged")
        for name in (
            "df",
            "findmnt",
            "getent",
            "git",
            "python3.12",
            "sha256sum",
            "systemctl",
            "timedatectl",
        ):
            _required_command(name)
        python = run(
            [
                "python3.12",
                "-I",
                "-c",
                (
                    "import platform,ssl,sys,venv;"
                    "assert sys.version_info[:2]==(3,12);"
                    "assert platform.machine() in {'x86_64','AMD64'};"
                    "libc,version=platform.libc_ver();"
                    "assert libc=='glibc' and tuple(map(int,version.split('.')[:2]))>=(2,28);"
                    "assert ssl.OPENSSL_VERSION"
                ),
            ]
        )
        if python.returncode != 0:
            raise PreflightError(f"offline CPython preflight failed: {python.stderr}")
        checks["python"] = {
            "implementation": platform.python_implementation(),
            "minimum_glibc": "2.28",
            "required": "CPython 3.12 x86_64",
            "system_command_green": True,
        }
        checks["git_bundle"] = verify_git_bundle(incoming, handoff, run)
        ntp = run(["timedatectl", "show", "--property=NTPSynchronized", "--value"])
        if ntp.returncode != 0 or ntp.stdout != "yes":
            raise PreflightError("NTP is not synchronized")
        checks["ntp"] = {"synchronized": True}
        mount = str(handoff["volume_mount"])
        target = run(["findmnt", "-rn", "-T", mount, "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"])
        if target.returncode != 0:
            raise PreflightError("campaign filesystem is not mounted")
        fields = target.stdout.split(maxsplit=3)
        if len(fields) != 4 or fields[0] != mount or fields[2] != "ext4":
            raise PreflightError(f"campaign filesystem identity diverged: {target.stdout}")
        options = fields[3].split(",")
        if "rw" not in options or "ro" in options:
            raise PreflightError("campaign filesystem is not read-write")
        capacity = run(["df", "-PB1", mount])
        available = _parse_df_available(capacity.stdout)
        disk = handoff.get("disk")
        if not isinstance(disk, Mapping):
            raise PreflightError("disk reservation contract is absent")
        required = disk.get("required_free_bytes")
        if type(required) is not int or required <= 0:
            raise PreflightError("required capacity is invalid")
        if available < required:
            raise PreflightError(
                "PREDICTION_CAPACITY_REFUSED_COEXISTENCE_NOT_PROVEN "
                f"available={available} required={required}; use a distinct host or ext4 volume"
            )
        checks["filesystem"] = {
            "available_bytes": available,
            "filesystem": fields[2],
            "mount": mount,
            "options": options,
            "required_free_bytes": required,
            "source": fields[1],
        }
        for key in ("source_root", "campaign_root"):
            candidate = Path(str(handoff[key]))
            if candidate.exists():
                raise PreflightError(f"unique {key} already exists")
        dashboard_port = handoff.get("dashboard_port")
        if type(dashboard_port) is not int or not 1024 <= dashboard_port <= 65535:
            raise PreflightError("dashboard port is invalid")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
            probe.bind(("127.0.0.1", dashboard_port))
        checks["loopback_port"] = {"host": "127.0.0.1", "port": dashboard_port, "free": True}
        services = handoff.get("services")
        if not isinstance(services, Mapping) or set(services) != {"dashboard", "kalshi", "polymarket"}:
            raise PreflightError("service identity map diverged")
        checks["services"] = [
            _systemd_collision(str(services[name]), run)
            for name in ("polymarket", "kalshi", "dashboard")
        ]
    except (OSError, PreflightError, subprocess.SubprocessError) as error:
        errors.append(str(error))
    connectivity: dict[str, dict[str, object]] = {}
    for venue in ("polymarket", "kalshi"):
        try:
            connectivity[venue] = connectivity_probe(venue)
        except (OSError, PreflightError, TimeoutError) as error:
            connectivity[venue] = {
                "errors": [f"PREFLIGHT_INTERNAL:{type(error).__name__}:{error}"],
                "venue": venue,
                "verdict": "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT",
            }
    eligible = [
        venue
        for venue in ("polymarket", "kalshi")
        if connectivity[venue].get("verdict") == "NETWORK_PREFLIGHT_GREEN"
    ]
    return {
        "boundary": BOUNDARY,
        "checks": checks,
        "eligible_venues": eligible,
        "errors": errors,
        "host_admitted": not errors,
        "installation_admissible": not errors,
        "network": connectivity,
        "recorded_at_utc": _utc_now_text(),
        "schema_version": 1,
        "terminal_signal": (
            "PREDICTION_HOST_PREFLIGHT_GREEN"
            if not errors
            else "PREDICTION_HOST_PREFLIGHT_REFUSED"
        ),
    }


def fsync_probe(handoff_path: Path) -> dict[str, object]:
    handoff = load_handoff(handoff_path)
    base = Path(str(handoff["volume_base"]))
    if base.is_symlink() or not base.is_dir() or base.resolve(strict=True) != base:
        raise PreflightError("dedicated Prediction Markets volume base is absent or unsafe")
    getuid = getattr(os, "getuid", lambda: -1)
    if base.stat().st_uid != getuid() or (base.stat().st_mode & 0o777) != 0o700:
        raise PreflightError("dedicated volume base ownership or mode diverged")
    probe = base / f".prediction-fsync-probe-{uuid4().hex}"
    descriptor = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(b"PREDICTION_MARKETS_FSYNC_PROBE_V1\n")
            handle.flush()
            os.fsync(handle.fileno())
        directory = os.open(base, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
            probe.unlink()
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if probe.exists():
            probe.unlink()
    return {
        "boundary": BOUNDARY,
        "filesystem_write_surface": str(base),
        "probe_removed": not probe.exists(),
        "recorded_at_utc": _utc_now_text(),
        "schema_version": 1,
        "terminal_signal": "PREDICTION_FILESYSTEM_FSYNC_GREEN",
    }


def resume_preflight(
    handoff_path: Path,
    *,
    run: CommandRunner = _command,
) -> dict[str, object]:
    handoff = load_handoff(handoff_path)
    errors: list[str] = []
    checks: dict[str, object] = {}
    try:
        if os.environ.get("USER") != handoff.get("service_user"):
            raise PreflightError("resume preflight must run as the dedicated service user")
        if Path.home().as_posix() != f"/home/{handoff.get('service_user')}":
            raise PreflightError("resume service user HOME diverged")
        source = Path(str(handoff["source_root"]))
        campaign = Path(str(handoff["campaign_root"]))
        volume_base = Path(str(handoff["volume_base"])).as_posix()
        if (
            source.is_symlink()
            or campaign.is_symlink()
            or source.resolve(strict=True) != source
            or campaign.resolve(strict=True) != campaign
            or not source.as_posix().startswith(f"{volume_base}/sources/")
            or not campaign.as_posix().startswith(f"{volume_base}/campaigns/")
        ):
            raise PreflightError("resume roots are absent or leave the Prediction Markets tree")
        runtime = source / ".venv" / "bin" / "python"
        if runtime.is_symlink() or not runtime.is_file():
            raise PreflightError("resume offline Python is absent or unsafe")
        imported = run(
            [
                str(runtime),
                "-I",
                "-c",
                (
                    "import sys;"
                    f"sys.path[:0]=[{str(source / 'src')!r},{str(source)!r}];"
                    "import hyperlab,platform,requests,ssl,websocket;"
                    "libc,version=platform.libc_ver();"
                    "assert libc=='glibc' and tuple(map(int,version.split('.')[:2]))>=(2,28)"
                ),
            ]
        )
        if imported.returncode != 0:
            raise PreflightError(f"resume offline imports failed: {imported.stderr}")
        ntp = run(["timedatectl", "show", "--property=NTPSynchronized", "--value"])
        if ntp.returncode != 0 or ntp.stdout != "yes":
            raise PreflightError("NTP is not synchronized for resume")
        mount = str(handoff["volume_mount"])
        target = run(["findmnt", "-rn", "-T", mount, "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"])
        fields = target.stdout.split(maxsplit=3)
        if (
            target.returncode != 0
            or len(fields) != 4
            or fields[0] != mount
            or fields[2] != "ext4"
            or "rw" not in fields[3].split(",")
            or "ro" in fields[3].split(",")
        ):
            raise PreflightError("resume filesystem is not the admitted ext4 rw mount")
        capacity = run(["df", "-PB1", mount])
        available = _parse_df_available(capacity.stdout)
        disk = handoff.get("disk")
        if not isinstance(disk, Mapping) or type(disk.get("required_free_bytes")) is not int:
            raise PreflightError("resume disk reservation contract is absent")
        required = int(disk["required_free_bytes"])
        if available < required:
            raise PreflightError(
                "PREDICTION_CAPACITY_REFUSED_COEXISTENCE_NOT_PROVEN "
                f"available={available} required={required}; use a distinct host or ext4 volume"
            )
        checks = {
            "filesystem": {
                "available_bytes": available,
                "filesystem": "ext4",
                "mount": mount,
                "required_free_bytes": required,
            },
            "ntp": {"synchronized": True},
            "offline_imports": {"verified": True},
            "roots": {"campaign": str(campaign), "source": str(source)},
        }
    except (OSError, PreflightError, subprocess.SubprocessError) as error:
        errors.append(str(error))
    return {
        "boundary": BOUNDARY,
        "checks": checks,
        "errors": errors,
        "recorded_at_utc": _utc_now_text(),
        "resume_admissible": not errors,
        "schema_version": 1,
        "terminal_signal": (
            "PREDICTION_RESUME_PREFLIGHT_GREEN"
            if not errors
            else "PREDICTION_RESUME_PREFLIGHT_REFUSED"
        ),
    }


def _atomic_report(path: Path, report: Mapping[str, object]) -> None:
    if path.exists():
        raise PreflightError("preflight report path must be new")
    payload = canonical_json_bytes(report) + b"\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb", closefd=True) as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    if os.name != "nt":
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prediction Markets fail-closed target preflight")
    subparsers = parser.add_subparsers(dest="command", required=True)
    host = subparsers.add_parser("host")
    host.add_argument("--handoff", type=Path, required=True)
    host.add_argument("--report", type=Path, required=True)
    fsync = subparsers.add_parser("fsync")
    fsync.add_argument("--handoff", type=Path, required=True)
    fsync.add_argument("--report", type=Path, required=True)
    network = subparsers.add_parser("network")
    network.add_argument("--venue", choices=("polymarket", "kalshi"), required=True)
    network.add_argument("--report", type=Path, required=True)
    resume = subparsers.add_parser("resume")
    resume.add_argument("--handoff", type=Path, required=True)
    resume.add_argument("--report", type=Path, required=True)
    return parser


def _fail(message: str) -> NoReturn:
    print(f"PREDICTION_PREFLIGHT_REFUSED:{message}", file=sys.stderr)
    raise SystemExit(4)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "host":
            report = host_preflight(arguments.handoff)
        elif arguments.command == "fsync":
            report = fsync_probe(arguments.handoff)
        elif arguments.command == "resume":
            report = resume_preflight(arguments.handoff)
        else:
            report = probe_venue_connectivity(arguments.venue)
        _atomic_report(arguments.report, report)
        print(canonical_json_bytes(report).decode("utf-8"))
        if arguments.command == "host" and report["installation_admissible"] is not True:
            return 4
        if arguments.command == "network" and report["verdict"] != "NETWORK_PREFLIGHT_GREEN":
            return 4
        if arguments.command == "resume" and report["resume_admissible"] is not True:
            return 4
        return 0
    except (OSError, PreflightError, subprocess.SubprocessError) as error:
        _fail(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
