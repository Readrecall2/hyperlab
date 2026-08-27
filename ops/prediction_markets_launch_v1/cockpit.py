from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn
from urllib.parse import parse_qs, unquote, urlsplit

from ops.prediction_markets_launch_v1.runner import canonical_json_bytes

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
ECONOMIC_STATUS = "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE"
MODE = "readonly"
FIXTURE_LABEL = "SYNTHETIC/FIXTURE — NOT ALPHA OR ECONOMIC EVIDENCE"
FIXTURES = (
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
DOWNLOAD_ALLOWLIST = {
    "campaign-manifest": PurePosixPath("campaign-manifest.json"),
    "campaign-manifest-pin": PurePosixPath("campaign-manifest.sha256"),
    "preflight-report": PurePosixPath("state/preflight-report.json"),
    "activation-receipt": PurePosixPath("state/activation-receipt.json"),
    "polymarket-ledger": PurePosixPath("polymarket/ledger.jsonl"),
    "kalshi-ledger": PurePosixPath("kalshi/ledger.jsonl"),
}
_MAX_JSON_BYTES = 4 * 1024 * 1024
_MAX_LEDGER_BYTES = 8 * 1024 * 1024


class CockpitError(RuntimeError):
    """Base class for fail-closed cockpit reads."""


class CockpitHeadChangedError(CockpitError):
    """A file changed during a coherent snapshot read."""


class CockpitIntegrityError(CockpitError):
    """Authenticated content diverged."""


@dataclass(frozen=True, slots=True)
class _Identity:
    device: int
    inode: int
    size: int
    modified_ns: int


@dataclass(frozen=True, slots=True)
class _Read:
    path: Path
    relative: PurePosixPath
    identity: _Identity
    payload: bytes


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _identity(path: Path) -> _Identity:
    value = path.lstat()
    return _Identity(value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


def _validate_root(root: Path) -> Path:
    if root.is_symlink():
        raise CockpitIntegrityError("campaign root is a symlink")
    resolved = root.resolve(strict=True)
    if not resolved.is_dir() or resolved != root:
        raise CockpitIntegrityError("campaign root must be an exact real directory")
    return resolved


def _bounded_read(root: Path, relative: PurePosixPath, *, maximum_bytes: int) -> _Read:
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise CockpitIntegrityError("cockpit path leaves its fixed allowlist")
    candidate = root.joinpath(*relative.parts)
    cursor = root
    for part in relative.parts[:-1]:
        cursor /= part
        try:
            parent_stat = cursor.lstat()
        except OSError as error:
            raise CockpitError(f"cockpit parent is unreadable: {relative.as_posix()}") from error
        if stat.S_ISLNK(parent_stat.st_mode) or not stat.S_ISDIR(parent_stat.st_mode):
            raise CockpitIntegrityError(f"cockpit parent is unsafe: {relative.as_posix()}")
    parent = candidate.parent.resolve(strict=True)
    if parent != root and root not in parent.parents:
        raise CockpitIntegrityError("cockpit path escapes campaign root")
    try:
        path_stat = candidate.lstat()
    except OSError as error:
        raise CockpitError(f"required cockpit file is unreadable: {relative.as_posix()}") from error
    if (
        stat.S_ISLNK(path_stat.st_mode)
        or not stat.S_ISREG(path_stat.st_mode)
        or path_stat.st_size > maximum_bytes
    ):
        raise CockpitIntegrityError(f"unsafe or oversized cockpit file: {relative.as_posix()}")
    path_identity = _Identity(
        path_stat.st_dev,
        path_stat.st_ino,
        path_stat.st_size,
        path_stat.st_mtime_ns,
    )
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as error:
        raise CockpitError(f"required cockpit file is unreadable: {relative.as_posix()}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum_bytes:
            raise CockpitIntegrityError(f"unsafe or oversized cockpit file: {relative.as_posix()}")
        opened_identity = _Identity(
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        if opened_identity != path_identity:
            raise CockpitHeadChangedError(f"file changed before read: {relative.as_posix()}")
        payload = b""
        while len(payload) < before.st_size:
            chunk = os.read(descriptor, min(65536, before.st_size - len(payload)))
            if not chunk:
                break
            payload += chunk
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    expected = _Identity(before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    observed = _Identity(after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if expected != observed or len(payload) != before.st_size:
        raise CockpitHeadChangedError(f"file changed during read: {relative.as_posix()}")
    return _Read(candidate, relative, expected, payload)


def _optional_read(
    root: Path,
    relative: PurePosixPath,
    *,
    maximum_bytes: int,
) -> _Read | None:
    cursor = root
    for part in relative.parts[:-1]:
        cursor /= part
        try:
            parent_stat = cursor.lstat()
        except FileNotFoundError:
            return None
        except OSError as error:
            raise CockpitError(
                f"optional cockpit parent is unreadable: {relative.as_posix()}"
            ) from error
        if stat.S_ISLNK(parent_stat.st_mode) or not stat.S_ISDIR(parent_stat.st_mode):
            raise CockpitIntegrityError(
                f"optional cockpit parent is unsafe: {relative.as_posix()}"
            )
    candidate = root.joinpath(*relative.parts)
    try:
        candidate.lstat()
    except FileNotFoundError:
        return None
    except OSError as error:
        raise CockpitError(f"optional cockpit file is unreadable: {relative.as_posix()}") from error
    return _bounded_read(root, relative, maximum_bytes=maximum_bytes)


def _decode_object(read: _Read, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(read.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CockpitIntegrityError(f"{label} is not strict UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise CockpitIntegrityError(f"{label} must be an object")
    return value


def _ledger(read: _Read | None, *, venue: str) -> list[dict[str, Any]]:
    if read is None:
        return []
    rows: list[dict[str, Any]] = []
    previous = "0" * 64
    seen: set[int] = set()
    for index, line in enumerate(read.payload.splitlines(), start=1):
        try:
            value = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CockpitIntegrityError(f"{venue} ledger line {index} is invalid") from error
        if not isinstance(value, dict):
            raise CockpitIntegrityError(f"{venue} ledger line {index} is not an object")
        ordinal = value.get("ordinal")
        claimed = value.get("entry_sha256")
        body = {key: item for key, item in value.items() if key != "entry_sha256"}
        computed = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
        if (
            type(ordinal) is not int
            or ordinal < 0
            or ordinal in seen
            or value.get("venue") != venue
            or value.get("boundary") != BOUNDARY
            or value.get("previous_entry_sha256") != previous
            or claimed != computed
        ):
            raise CockpitIntegrityError(f"{venue} ledger chain diverged at line {index}")
        seen.add(ordinal)
        previous = computed
        rows.append(value)
    return rows


def _sum(rows: Sequence[Mapping[str, object]], field: str) -> int | None:
    values = [row.get(field) for row in rows]
    if any(value is not None and (type(value) is not int or value < 0) for value in values):
        raise CockpitIntegrityError(f"ledger metric is invalid: {field}")
    present = [value for value in values if type(value) is int]
    if not present:
        return None
    return sum(int(value) for value in present)


def _metric(value: object, *, provenance: str) -> dict[str, object]:
    return {"available": value is not None, "provenance": provenance, "value": value}


def _venue_snapshot(
    venue: str,
    rows: Sequence[Mapping[str, object]],
    state: Mapping[str, object] | None,
    connectivity: Mapping[str, object] | None,
    *,
    now: datetime,
) -> dict[str, object]:
    updated = None if state is None else _parse_utc(state.get("updated_at_utc"))
    freshness = None if updated is None else max(0, int((now - updated).total_seconds()))
    latest = None if not rows else rows[-1]
    lifecycle = None if state is None else state.get("lifecycle")
    verdict = None if connectivity is None else connectivity.get("verdict")
    runtime_terminal = None if latest is None else latest.get("terminal_health")
    if runtime_terminal is not None and "PUBLIC_SOURCE_UNAVAILABLE" in str(runtime_terminal):
        verdict = "PUBLIC_SOURCE_UNAVAILABLE_RUNTIME"
    return {
        "collection": {
            "bytes": _metric(_sum(rows, "bytes"), provenance="AUTHENTICATED_SLOT_LEDGER"),
            "duplicates": _metric(_sum(rows, "duplicates"), provenance="AUTHENTICATED_SLOT_LEDGER"),
            "frames": _metric(_sum(rows, "frames"), provenance="AUTHENTICATED_SLOT_LEDGER"),
            "gaps": _metric(_sum(rows, "gaps"), provenance="AUTHENTICATED_SLOT_LEDGER"),
            "reconnects": _metric(_sum(rows, "reconnects"), provenance="AUTHENTICATED_SLOT_LEDGER"),
            "segments": _metric(_sum(rows, "segments"), provenance="AUTHENTICATED_SLOT_LEDGER"),
            "slots_recorded": _metric(len(rows) if rows else None, provenance="AUTHENTICATED_SLOT_LEDGER"),
        },
        "connectivity": {
            "dns": None if connectivity is None else connectivity.get("dns"),
            "errors": None if connectivity is None else connectivity.get("errors"),
            "verdict": verdict,
        },
        "freshness_seconds": freshness,
        "last_manifest_sha256": None if latest is None else latest.get("manifest_sha256"),
        "last_root_sha256": None if latest is None else latest.get("root_sha256"),
        "last_terminal_health": None if latest is None else latest.get("terminal_health"),
        "recovery": (
            "INTERRUPTED_RECOVERABLE"
            if latest is not None and "INTERRUPTED" in str(latest.get("terminal_health"))
            else "NO_RECOVERY_PENDING"
        ),
        "service_state": lifecycle,
        "venue": venue,
    }


def _state_code(
    venues: Mapping[str, Mapping[str, object]],
    *,
    activated: bool,
    integrity: str,
) -> str:
    if integrity != "AUTHENTICATED":
        return "INTEGRITY_FAILED"
    poly = venues["polymarket"]
    kalshi = venues["kalshi"]
    lifecycles = {poly.get("service_state"), kalshi.get("service_state")}
    terminals = {poly.get("last_terminal_health"), kalshi.get("last_terminal_health")}
    if any(value in {"CAPACITY_REFUSED", "INTEGRITY_FAILED"} for value in lifecycles):
        return "INTEGRITY_FAILED"
    if lifecycles == {"PREPARED"}:
        return "PREPARED"
    if lifecycles == {"COMPLETE_WINDOW"}:
        return "COMPLETE_WINDOW"
    if any(
        value is not None and "INTERRUPTED" in str(value)
        for value in (*lifecycles, *terminals)
    ):
        return "INTERRUPTED_RECOVERABLE"
    freshness_values = [value.get("freshness_seconds") for value in venues.values()]
    if any(type(value) is int and value > 120 for value in freshness_values):
        return "STALE_RECONNECTING"
    unavailable: set[str] = set()
    for name, value in venues.items():
        connectivity = value.get("connectivity")
        if isinstance(connectivity, Mapping) and str(
            connectivity.get("verdict")
        ).startswith("PUBLIC_SOURCE_UNAVAILABLE"):
            unavailable.add(name)
    missing = {
        name for name, value in venues.items() if value.get("service_state") is None
    }
    if activated and any(name not in unavailable for name in missing):
        return "SERVICE_STATE_UNAVAILABLE"
    if unavailable == {"polymarket", "kalshi"}:
        return "BOTH_UNAVAILABLE"
    if unavailable == {"polymarket"}:
        return "POLYMARKET_UNAVAILABLE_KALSHI_RUNNING"
    if unavailable == {"kalshi"}:
        return "KALSHI_UNAVAILABLE_POLYMARKET_RUNNING"
    if all(value.get("service_state") is None for value in venues.values()):
        return "SERVICE_STATE_UNAVAILABLE" if activated else "PREPARED"
    return "BOTH_RUNNING"


def _base_snapshot(*, now: datetime, fixture: bool, fixture_name: str | None) -> dict[str, Any]:
    return {
        "boundary": BOUNDARY,
        "economic_evidence_status": ECONOMIC_STATUS,
        "fixture": fixture,
        "fixture_label": FIXTURE_LABEL if fixture else None,
        "fixture_name": fixture_name,
        "generated_at_utc": _utc_text(now),
        "mode": MODE,
        "orders_enabled": False,
        "schema_version": 1,
    }


def fixture_snapshot(name: str, *, now: datetime | None = None) -> dict[str, Any]:
    if name not in FIXTURES:
        raise CockpitIntegrityError("fixture name is not allowlisted")
    current = datetime.now(UTC) if now is None else now.astimezone(UTC)
    running = name in {
        "BOTH_RUNNING",
        "POLYMARKET_UNAVAILABLE_KALSHI_RUNNING",
        "KALSHI_UNAVAILABLE_POLYMARKET_RUNNING",
        "STALE_RECONNECTING",
        "HOLDOUT_SEALED",
    }
    unavailable = {
        "polymarket": name in {"POLYMARKET_UNAVAILABLE_KALSHI_RUNNING", "BOTH_UNAVAILABLE"},
        "kalshi": name in {"KALSHI_UNAVAILABLE_POLYMARKET_RUNNING", "BOTH_UNAVAILABLE"},
    }
    venues: dict[str, Any] = {}
    for index, venue in enumerate(("polymarket", "kalshi"), start=1):
        available = running and not unavailable[venue]
        metrics = {
            "bytes": _metric(8_388_608 * index if available else None, provenance=FIXTURE_LABEL),
            "duplicates": _metric(2 * index if available else None, provenance=FIXTURE_LABEL),
            "frames": _metric(840 * index if available else None, provenance=FIXTURE_LABEL),
            "gaps": _metric(0 if available else None, provenance=FIXTURE_LABEL),
            "reconnects": _metric(1 if name == "STALE_RECONNECTING" and available else (0 if available else None), provenance=FIXTURE_LABEL),
            "segments": _metric(12 * index if available else None, provenance=FIXTURE_LABEL),
            "slots_recorded": _metric(21 * index if available else None, provenance=FIXTURE_LABEL),
        }
        venues[venue] = {
            "collection": metrics,
            "connectivity": {
                "dns": None if unavailable[venue] else {f"{venue}.official.example": ["203.0.113.10"]},
                "errors": ["SYNTHETIC DNS FAILURE"] if unavailable[venue] else [],
                "verdict": "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT" if unavailable[venue] else "NETWORK_PREFLIGHT_GREEN",
            },
            "freshness_seconds": 420 if name == "STALE_RECONNECTING" and available else (7 if available else None),
            "last_manifest_sha256": (str(index) * 64) if available else None,
            "last_root_sha256": (str(index + 2) * 64) if available else None,
            "last_terminal_health": (
                "INTERRUPTED_RECOVERABLE"
                if name == "INTERRUPTED_RECOVERABLE"
                else ("COMPLETE" if name == "COMPLETE_WINDOW" else None)
            ),
            "recovery": "INTERRUPTED_RECOVERABLE" if name == "INTERRUPTED_RECOVERABLE" else "NO_RECOVERY_PENDING",
            "service_state": (
                "COMPLETE_WINDOW"
                if name == "COMPLETE_WINDOW"
                else (
                    "DISABLED_NETWORK_PREFLIGHT"
                    if unavailable[venue]
                    else ("PREPARED" if name == "PREPARED" else "WAITING_NEXT_SLOT")
                )
            ),
            "venue": venue,
        }
    state_name = name
    return {
        **_base_snapshot(now=current, fixture=True, fixture_name=name),
        "capacity": {
            "admitted": name != "INTEGRITY_FAILED",
            "available_bytes": 220_000_000_000,
            "required_free_bytes": 194_347_270_144,
        },
        "downloads": [],
        "holdout": {
            "access": "SEALED",
            "metrics_exposed": False,
            "status": "HOLDOUT_SEALED",
        },
        "identity": {
            "campaign_id": "prediction-markets-fixture-v1",
            "campaign_manifest_sha256": "a" * 64,
            "candidate_config_sha256": "aa60c0ff0ef95813d79f56b6ea93a31952061b562905dc9729162f7b16e41964",
            "source_commit": "3f188b9c28c9fec406b904a9e3307b43f54243e8",
        },
        "state": {
            "code": state_name,
            "integrity": "FAILED" if name == "INTEGRITY_FAILED" else "SYNTHETIC_AUTHENTIC",
            "severity": "critical" if name == "INTEGRITY_FAILED" else ("warning" if "UNAVAILABLE" in name or "STALE" in name or "INTERRUPTED" in name else "ok"),
        },
        "venues": venues,
    }


def _snapshot_once(root: Path, *, now: datetime) -> dict[str, Any]:
    reads: list[_Read] = []
    manifest_read = _bounded_read(root, PurePosixPath("campaign-manifest.json"), maximum_bytes=_MAX_JSON_BYTES)
    pin_read = _bounded_read(root, PurePosixPath("campaign-manifest.sha256"), maximum_bytes=256)
    reads.extend((manifest_read, pin_read))
    manifest = _decode_object(manifest_read, label="campaign manifest")
    claimed_manifest = manifest.get("manifest_sha256")
    manifest_body = {
        key: value for key, value in manifest.items() if key != "manifest_sha256"
    }
    if (
        not isinstance(claimed_manifest, str)
        or hashlib.sha256(canonical_json_bytes(manifest_body)).hexdigest()
        != claimed_manifest
    ):
        raise CockpitIntegrityError("campaign manifest logical SHA-256 diverged")
    pin = pin_read.payload.decode("ascii").strip().split()
    if len(pin) != 2 or pin[1] != "campaign-manifest.json" or hashlib.sha256(manifest_read.payload).hexdigest() != pin[0]:
        raise CockpitIntegrityError("campaign manifest pin diverged")
    if (
        manifest.get("boundary") != BOUNDARY
        or manifest.get("economic_evidence_status") != ECONOMIC_STATUS
        or manifest.get("vps_or_h1_path") != "NONE"
        or manifest.get("holdout", {}).get("access") != "SEALED"
    ):
        raise CockpitIntegrityError("campaign safety boundary diverged")
    preflight_read = _optional_read(root, PurePosixPath("state/preflight-report.json"), maximum_bytes=_MAX_JSON_BYTES)
    activation_read = _optional_read(root, PurePosixPath("state/activation-receipt.json"), maximum_bytes=_MAX_JSON_BYTES)
    if preflight_read is not None:
        reads.append(preflight_read)
    if activation_read is not None:
        reads.append(activation_read)
    preflight = None if preflight_read is None else _decode_object(preflight_read, label="preflight report")
    activation = None if activation_read is None else _decode_object(activation_read, label="activation receipt")
    venue_values: dict[str, dict[str, object]] = {}
    venue_states: dict[str, Mapping[str, object] | None] = {}
    for venue in ("polymarket", "kalshi"):
        ledger_read = _optional_read(root, PurePosixPath(f"{venue}/ledger.jsonl"), maximum_bytes=_MAX_LEDGER_BYTES)
        state_read = _optional_read(root, PurePosixPath(f"{venue}/state.json"), maximum_bytes=_MAX_JSON_BYTES)
        if ledger_read is not None:
            reads.append(ledger_read)
        if state_read is not None:
            reads.append(state_read)
        rows = _ledger(ledger_read, venue=venue.upper())
        state = None if state_read is None else _decode_object(state_read, label=f"{venue} state")
        venue_states[venue] = state
        network = None
        if preflight is not None and isinstance(preflight.get("network"), Mapping):
            selected = preflight["network"].get(venue)
            if isinstance(selected, Mapping):
                network = selected
        venue_values[venue] = _venue_snapshot(venue, rows, state, network, now=now)
    for read in reads:
        try:
            current = _identity(read.path)
        except OSError as error:
            raise CockpitHeadChangedError("snapshot input disappeared") from error
        if current != read.identity:
            raise CockpitHeadChangedError(f"snapshot input changed: {read.relative.as_posix()}")
    capacity: object = None
    for venue in ("polymarket", "kalshi"):
        state_value = venue_states[venue]
        if state_value is not None and isinstance(state_value.get("capacity"), Mapping):
            capacity = state_value["capacity"]
            break
    source_commit = None if activation is None else activation.get("source_commit")
    downloads = [
        {"id": identifier, "path": relative.as_posix()}
        for identifier, relative in DOWNLOAD_ALLOWLIST.items()
        if root.joinpath(*relative.parts).is_file() and not root.joinpath(*relative.parts).is_symlink()
    ]
    code = _state_code(
        venue_values,
        activated=activation is not None,
        integrity="AUTHENTICATED",
    )
    severity = (
        "critical"
        if code in {"INTEGRITY_FAILED", "SERVICE_STATE_UNAVAILABLE"}
        else (
            "warning"
            if "UNAVAILABLE" in code or "STALE" in code or "INTERRUPTED" in code
            else "ok"
        )
    )
    return {
        **_base_snapshot(now=now, fixture=False, fixture_name=None),
        "capacity": capacity,
        "downloads": downloads,
        "holdout": {"access": "SEALED", "metrics_exposed": False, "status": "HOLDOUT_SEALED"},
        "identity": {
            "campaign_id": manifest.get("campaign_id"),
            "campaign_manifest_sha256": manifest.get("manifest_sha256"),
            "candidate_config_sha256": manifest.get("candidate_config_sha256"),
            "physical_manifest_sha256": pin[0],
            "source_commit": source_commit,
        },
        "state": {
            "code": code,
            "integrity": "AUTHENTICATED",
            "severity": severity,
        },
        "venues": venue_values,
    }


def campaign_snapshot(root: Path, *, now: datetime | None = None) -> dict[str, Any]:
    campaign_root = _validate_root(root)
    current = datetime.now(UTC) if now is None else now.astimezone(UTC)
    for attempt in range(2):
        try:
            return _snapshot_once(campaign_root, now=current)
        except CockpitHeadChangedError:
            if attempt == 1:
                raise
    raise AssertionError("bounded snapshot retry exhausted")


def report_download(root: Path, identifier: str) -> tuple[str, bytes]:
    relative = DOWNLOAD_ALLOWLIST.get(identifier)
    if relative is None:
        raise CockpitIntegrityError("download identifier is not allowlisted")
    campaign_root = _validate_root(root)
    maximum = _MAX_LEDGER_BYTES if relative.suffix == ".jsonl" else _MAX_JSON_BYTES
    read = _bounded_read(campaign_root, relative, maximum_bytes=maximum)
    content_type = "application/x-ndjson" if relative.suffix == ".jsonl" else "application/json"
    return content_type, read.payload


_HTML = """<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>HyperLab · Prediction Markets Observatory</title><link rel="stylesheet" href="/assets/cockpit.css"></head>
<body><div class="aurora a"></div><div class="aurora b"></div><main>
<header><div><p class="eyebrow">HYPERLAB / PROSPECTIVE EVIDENCE</p><h1>Prediction Markets <span>Observatory</span></h1>
<p class="sub">Polymarket + Kalshi · collecte publique isolée · aucune route d'ordre</p></div>
<div class="rail"><span class="lock">READ ONLY</span><span>ORDERS ENABLED <b>FALSE</b></span><span>PORT 18081</span></div></header>
<section id="fixture" class="fixture hidden"></section><section class="hero">
<div><p class="label">ÉTAT DE CAMPAGNE</p><h2 id="state">CHARGEMENT</h2><p id="summary">Assemblage borné du snapshot…</p></div>
<div class="seal"><div>HOLDOUT</div><strong id="holdout">SEALED</strong><small>NO METRICS EXPOSED</small></div></section>
<section class="grid"><article class="venue" id="polymarket"><div class="venue-head"><div><p class="label">VENUE 01</p><h3>Polymarket</h3></div><span class="dot"></span></div><div class="status"></div><div class="metrics"></div><div class="hashes"></div></article>
<article class="venue" id="kalshi"><div class="venue-head"><div><p class="label">VENUE 02</p><h3>Kalshi</h3></div><span class="dot"></span></div><div class="status"></div><div class="metrics"></div><div class="hashes"></div></article></section>
<section class="lower"><article><p class="label">IDENTITÉ AUTHENTIFIÉE</p><dl id="identity"></dl></article><article><p class="label">CAPACITÉ & COHABITATION H1</p><dl id="capacity"></dl></article><article><p class="label">FRONTIÈRE ÉCONOMIQUE</p><strong class="economic">ECONOMIC EVIDENCE<br>NOT YET AVAILABLE</strong><p class="muted">Technique prospective uniquement. Aucune conclusion alpha, capacité ou rentabilité.</p></article></section>
<footer><span>PAPER ONLY</span><span>GHOST ONLY</span><span>PUBLIC DATA ONLY</span><span id="clock"></span></footer></main><script src="/assets/cockpit.js"></script></body></html>"""

_CSS = r"""
:root{--ink:#edf5ff;--muted:#8292a8;--line:rgba(157,194,255,.14);--panel:rgba(8,15,27,.78);--cyan:#69e1ff;--mint:#7dffc4;--amber:#ffc875;--red:#ff7c92;--violet:#aa8cff}*{box-sizing:border-box}body{margin:0;min-height:100vh;background:#050912;color:var(--ink);font-family:Inter,"Segoe UI",sans-serif;overflow-x:hidden}.aurora{position:fixed;border-radius:50%;filter:blur(110px);opacity:.16;pointer-events:none}.aurora.a{width:560px;height:560px;background:#1665ff;right:-120px;top:-220px}.aurora.b{width:460px;height:460px;background:#00d9ad;left:-180px;bottom:-220px}main{position:relative;width:min(1380px,calc(100% - 48px));margin:auto;padding:42px 0 28px}header{display:flex;justify-content:space-between;align-items:flex-start;padding-bottom:28px;border-bottom:1px solid var(--line)}.eyebrow,.label{font-size:11px;font-weight:800;letter-spacing:.2em;color:var(--cyan);margin:0 0 10px}h1{font-size:42px;letter-spacing:-.045em;margin:0;font-weight:680}h1 span{font-weight:300;color:#a8b8cb}.sub{color:var(--muted);margin:9px 0 0}.rail{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end;max-width:460px}.rail span{border:1px solid var(--line);background:rgba(10,20,34,.7);padding:9px 12px;border-radius:999px;font:700 10px/1 monospace;color:#94a7bd}.rail .lock{color:var(--mint);border-color:rgba(125,255,196,.35)}.rail b{color:var(--red)}.fixture{margin-top:18px;padding:10px 14px;border:1px solid rgba(255,200,117,.3);background:rgba(255,200,117,.08);color:var(--amber);font:700 11px monospace;border-radius:8px}.hidden{display:none}.hero{margin-top:18px;min-height:176px;padding:30px 34px;background:linear-gradient(105deg,rgba(13,27,48,.94),rgba(8,15,27,.7));border:1px solid var(--line);border-radius:20px;display:flex;align-items:center;justify-content:space-between;box-shadow:0 24px 80px rgba(0,0,0,.28)}h2{margin:0;font:700 clamp(26px,4vw,50px)/1.08 monospace;letter-spacing:-.05em}#summary{color:var(--muted);max-width:730px}.seal{width:180px;height:118px;border-radius:16px;border:1px solid rgba(105,225,255,.28);background:radial-gradient(circle at top,rgba(105,225,255,.12),transparent 70%);display:grid;place-content:center;text-align:center;gap:6px}.seal div,.seal small{font:700 10px monospace;color:var(--muted);letter-spacing:.12em}.seal strong{color:var(--cyan);font:800 20px monospace}.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:18px}.venue,.lower article{border:1px solid var(--line);background:var(--panel);border-radius:18px;padding:24px;backdrop-filter:blur(18px)}.venue-head{display:flex;justify-content:space-between;align-items:center}h3{margin:0;font-size:27px}.dot{width:11px;height:11px;border-radius:50%;background:var(--muted);box-shadow:0 0 18px currentColor}.status{margin:20px 0;padding:12px 14px;border-left:2px solid var(--cyan);background:rgba(105,225,255,.05);font:700 12px monospace;color:#b8c7d9}.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}.metric{min-height:72px;border:1px solid rgba(157,194,255,.1);border-radius:11px;padding:11px;background:rgba(255,255,255,.018)}.metric span{display:block;color:var(--muted);font-size:10px;letter-spacing:.08em}.metric strong{display:block;margin-top:8px;font:700 18px monospace}.na{color:#6f7e91!important;font-size:12px!important}.hashes{margin-top:14px;color:var(--muted);font:11px/1.8 monospace;overflow:hidden;text-overflow:ellipsis}.lower{display:grid;grid-template-columns:1.15fr 1fr 1fr;gap:18px;margin-top:18px}.lower dl{display:grid;grid-template-columns:auto 1fr;gap:10px 16px;margin:18px 0 0}.lower dt{color:var(--muted);font-size:11px}.lower dd{margin:0;text-align:right;font:11px monospace;overflow:hidden;text-overflow:ellipsis}.economic{display:block;color:var(--amber);font:700 17px/1.35 monospace;margin:22px 0}.muted{color:var(--muted);font-size:12px;line-height:1.55}footer{display:flex;gap:22px;margin-top:24px;color:#627087;font:700 10px monospace;letter-spacing:.1em}footer #clock{margin-left:auto}.critical h2,.warning h2{color:var(--red)}.warning h2{color:var(--amber)}.ok h2{color:var(--mint)}@media(max-width:900px){main{width:min(100% - 24px,1380px);padding-top:24px}header,.hero{flex-direction:column;gap:24px}.rail{justify-content:flex-start}.grid,.lower{grid-template-columns:1fr}.seal{width:100%}.metrics{grid-template-columns:repeat(2,1fr)}}
"""

_JS = r"""
const q=new URLSearchParams(location.search);const fixture=q.get('fixture');const url=fixture?`/api/fixtures/${encodeURIComponent(fixture)}`:'/api/snapshot';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const short=v=>v?`${String(v).slice(0,12)}…${String(v).slice(-8)}`:'NON DISPONIBLE';
const fmt=v=>v===null||v===undefined?'NON DISPONIBLE':new Intl.NumberFormat('fr-FR').format(v);
function metric(label,m){const na=!m||!m.available;return `<div class="metric"><span>${label}</span><strong class="${na?'na':''}">${na?'NON DISPONIBLE':fmt(m.value)}</strong></div>`}
function venue(id,v){const e=document.getElementById(id);const c=v.collection||{};e.querySelector('.status').innerHTML=`SERVICE · ${esc(v.service_state||'NON DISPONIBLE')}<br>NETWORK · ${esc(v.connectivity?.verdict||'NON DISPONIBLE')}<br>FRESHNESS · ${v.freshness_seconds===null||v.freshness_seconds===undefined?'NON DISPONIBLE':fmt(v.freshness_seconds)+' s'}`;e.querySelector('.metrics').innerHTML=metric('FRAMES',c.frames)+metric('SEGMENTS',c.segments)+metric('OCTETS',c.bytes)+metric('GAPS',c.gaps)+metric('DUPLICATES',c.duplicates)+metric('RECONNECTS',c.reconnects);e.querySelector('.hashes').innerHTML=`MANIFEST ${esc(short(v.last_manifest_sha256))}<br>ROOT ${esc(short(v.last_root_sha256))}<br>RECOVERY ${esc(v.recovery||'NON DISPONIBLE')}`;const good=v.connectivity?.verdict==='NETWORK_PREFLIGHT_GREEN';e.querySelector('.dot').style.background=good?'var(--mint)':'var(--red)'}
function dl(obj){return Object.entries(obj).map(([k,v])=>`<dt>${esc(k.replaceAll('_',' ').toUpperCase())}</dt><dd>${esc(v===null||v===undefined?'NON DISPONIBLE':typeof v==='boolean'?String(v).toUpperCase():typeof v==='number'?fmt(v):short(v))}</dd>`).join('')}
fetch(url,{headers:{Accept:'application/json'}}).then(async r=>{const d=await r.json();if(!r.ok)throw d;document.body.classList.add(d.state.severity);document.getElementById('state').textContent=d.state.code;document.getElementById('summary').textContent=d.state.integrity==='AUTHENTICATED'?'Snapshot cohérent, fichiers bornés et identité revalidée avant/après lecture.':'État synthétique de QA — aucune preuve de marché.';document.getElementById('holdout').textContent=d.holdout.access;venue('polymarket',d.venues.polymarket);venue('kalshi',d.venues.kalshi);document.getElementById('identity').innerHTML=dl(d.identity);document.getElementById('capacity').innerHTML=dl(d.capacity||{});document.getElementById('clock').textContent=d.generated_at_utc;if(d.fixture){const f=document.getElementById('fixture');f.classList.remove('hidden');f.textContent=d.fixture_label+' · '+d.fixture_name}}).catch(e=>{document.body.classList.add('critical');document.getElementById('state').textContent='INTEGRITY_FAILED';document.getElementById('summary').textContent='Lecture refusée: '+(e.error||e.detail||'snapshot indisponible')});
"""


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], root: Path | None, fixtures_enabled: bool) -> None:
        super().__init__(address, _Handler)
        self.campaign_root = root
        self.fixtures_enabled = fixtures_enabled


class _Handler(BaseHTTPRequestHandler):
    server: _Server

    def log_message(self, format: str, *args: object) -> None:
        print(f"PREDICTION_COCKPIT_HTTP:{self.address_string()}:{format % args}")

    def _send(self, status: int, content_type: str, payload: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'",
        )
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def _json(self, status: int, value: Mapping[str, object]) -> None:
        self._send(status, "application/json; charset=utf-8", canonical_json_bytes(value))

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_GET(self) -> None:
        if len(self.path) > 2048:
            self._json(HTTPStatus.REQUEST_URI_TOO_LONG, {"error": "REQUEST_PATH_TOO_LONG", "mode": MODE, "orders_enabled": False})
            return
        parsed = urlsplit(self.path)
        path = unquote(parsed.path)
        if ".." in PurePosixPath(path).parts or "\\" in path or "\x00" in path:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "PATH_REFUSED", "mode": MODE, "orders_enabled": False})
            return
        if path == "/":
            self._send(HTTPStatus.OK, "text/html; charset=utf-8", _HTML.encode("utf-8"))
            return
        if path == "/assets/cockpit.css":
            self._send(HTTPStatus.OK, "text/css; charset=utf-8", _CSS.encode("utf-8"))
            return
        if path == "/assets/cockpit.js":
            self._send(HTTPStatus.OK, "text/javascript; charset=utf-8", _JS.encode("utf-8"))
            return
        if path == "/health/live":
            self._json(HTTPStatus.OK, {"mode": MODE, "orders_enabled": False, "status": "alive"})
            return
        if path == "/health/ready":
            try:
                snapshot = (
                    fixture_snapshot("PREPARED")
                    if self.server.campaign_root is None
                    else campaign_snapshot(self.server.campaign_root)
                )
                state = snapshot["state"]
                ready = isinstance(state, Mapping) and state.get("severity") != "critical"
            except CockpitError as error:
                self._json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "detail": str(error),
                        "mode": MODE,
                        "orders_enabled": False,
                        "status": "not-ready",
                    },
                )
                return
            self._json(
                HTTPStatus.OK if ready else HTTPStatus.SERVICE_UNAVAILABLE,
                {
                    "mode": MODE,
                    "orders_enabled": False,
                    "state": state,
                    "status": "ready" if ready else "not-ready",
                },
            )
            return
        if path == "/api/snapshot":
            query = parse_qs(parsed.query)
            if query:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "CLIENT_PATH_OR_QUERY_NOT_ACCEPTED", "mode": MODE, "orders_enabled": False})
                return
            try:
                snapshot = (
                    fixture_snapshot("PREPARED")
                    if self.server.campaign_root is None
                    else campaign_snapshot(self.server.campaign_root)
                )
            except CockpitHeadChangedError as error:
                self._json(HTTPStatus.CONFLICT, {"error": "HEAD_CHANGED_RETRY", "detail": str(error), "mode": MODE, "orders_enabled": False})
                return
            except CockpitError as error:
                self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "INTEGRITY_FAILED", "detail": str(error), "mode": MODE, "orders_enabled": False})
                return
            self._json(HTTPStatus.OK, snapshot)
            return
        fixture_prefix = "/api/fixtures/"
        if path.startswith(fixture_prefix):
            if not self.server.fixtures_enabled:
                self._json(HTTPStatus.NOT_FOUND, {"error": "FIXTURES_DISABLED", "mode": MODE, "orders_enabled": False})
                return
            name = path.removeprefix(fixture_prefix)
            try:
                snapshot = fixture_snapshot(name)
            except CockpitError as error:
                self._json(HTTPStatus.NOT_FOUND, {"error": "FIXTURE_NOT_ALLOWLISTED", "detail": str(error), "mode": MODE, "orders_enabled": False})
                return
            self._json(HTTPStatus.OK, snapshot)
            return
        download_prefix = "/api/download/"
        if path.startswith(download_prefix):
            if self.server.campaign_root is None:
                self._json(HTTPStatus.NOT_FOUND, {"error": "REPORT_NOT_AVAILABLE", "mode": MODE, "orders_enabled": False})
                return
            identifier = path.removeprefix(download_prefix)
            try:
                content_type, payload = report_download(self.server.campaign_root, identifier)
            except CockpitError as error:
                self._json(HTTPStatus.NOT_FOUND, {"error": "DOWNLOAD_REFUSED", "detail": str(error), "mode": MODE, "orders_enabled": False})
                return
            self._send(HTTPStatus.OK, content_type, payload)
            return
        self._json(HTTPStatus.NOT_FOUND, {"error": "ROUTE_NOT_FOUND", "mode": MODE, "orders_enabled": False})

    def _method_not_allowed(self) -> None:
        self._json(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "READ_ONLY_GET_HEAD_ONLY", "mode": MODE, "orders_enabled": False})

    do_POST = _method_not_allowed
    do_PUT = _method_not_allowed
    do_PATCH = _method_not_allowed
    do_DELETE = _method_not_allowed
    do_OPTIONS = _method_not_allowed
    do_TRACE = _method_not_allowed
    do_CONNECT = _method_not_allowed


def serve(*, root: Path | None, host: str, port: int, fixtures_enabled: bool) -> None:
    if host != "127.0.0.1" or port != 18081:
        raise CockpitIntegrityError("cockpit must bind exactly to 127.0.0.1:18081")
    if root is not None:
        root = _validate_root(root)
    server = _Server((host, port), root, fixtures_enabled)
    print("PREDICTION_COCKPIT_READY:http://127.0.0.1:18081")
    try:
        try:
            server.serve_forever(poll_interval=0.5)
        except KeyboardInterrupt:
            print("PREDICTION_COCKPIT_STOPPED_READONLY")
    finally:
        server.server_close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prediction Markets read-only cockpit")
    parser.add_argument("--campaign-root", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=18081, type=int)
    parser.add_argument("--fixtures-enabled", action="store_true")
    return parser


def _fail(message: str) -> NoReturn:
    print(f"PREDICTION_COCKPIT_REFUSED:{message}", file=sys.stderr)
    raise SystemExit(4)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        serve(
            root=arguments.campaign_root,
            host=arguments.host,
            port=arguments.port,
            fixtures_enabled=arguments.fixtures_enabled,
        )
    except (CockpitError, OSError) as error:
        _fail(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
