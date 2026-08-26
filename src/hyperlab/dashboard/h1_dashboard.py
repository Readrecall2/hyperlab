from __future__ import annotations

import hashlib
import json
import stat
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from hyperlab.ghost.h1 import ECONOMIC_NOT_AVAILABLE, H1PolicyConfig
from hyperlab.research_data.adapters import (
    HYPERLIQUID_METADATA_VERSION,
    HYPERLIQUID_PUBLIC_HTTP_URL,
    HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
)
from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.segments import (
    MANIFEST_SUFFIX,
    SEGMENT_SUFFIX,
    ManifestRecord,
    ResearchDataIntegrityError,
    decode_manifest,
    decode_segment,
)

H1_BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
H1_MODE = "readonly"
H1_FIXTURE_LABEL = "SYNTHETIC/FIXTURE — NOT ALPHA OR ECONOMIC EVIDENCE"
H1_SNAPSHOT_SCHEMA_VERSION = 1

_MAX_CAMPAIGN_MANIFEST_BYTES = 2 * 1024 * 1024
_MAX_HEALTH_BYTES = 2 * 1024 * 1024
_MAX_RAW_MANIFEST_BYTES = 16 * 1024 * 1024
_MAX_SEGMENT_BYTES = 40 * 1024 * 1024
_MAX_REPORT_BYTES = 32 * 1024 * 1024
_TAIL_SEGMENT_LIMIT = 1
_INCIDENT_LIMIT = 40
_HEAD_READ_ATTEMPTS = 2
_HEALTH_STALE_AFTER_SECONDS = 30.0
_REPORT_PATHS = {
    "verified-threshold": Path("state/verified-threshold-report.json"),
    "ghost-h1": Path("reports/ghost/hyperliquid-h1-ghost-v1.json"),
}
_COMPLETE_STATES = frozenset({"COMPLETE_COLLECTION_WINDOW", "COMPLETE_VERIFIED_THRESHOLDS"})
_RECOVERABLE_STATES = frozenset(
    {
        "INTERRUPTED_RECOVERABLE",
        "PUBLIC_SOURCE_UNAVAILABLE_RECOVERABLE",
        "THRESHOLD_CANDIDATE_NOT_FINAL_RESUME_REQUIRED",
    }
)


class H1SnapshotHeadChangedError(RuntimeError):
    """A bounded dashboard snapshot raced an atomic campaign publication."""


class H1SnapshotIntegrityError(RuntimeError):
    """A campaign publication failed bounded read-only authentication."""


class H1SnapshotUnreadableError(RuntimeError):
    """A campaign publication could not be read safely."""


@dataclass(frozen=True, slots=True)
class _ReadIdentity:
    size: int
    modified_ns: int
    inode: int
    digest: str


@dataclass(frozen=True, slots=True)
class _BoundedRead:
    value: bytes
    identity: _ReadIdentity


@dataclass(frozen=True, slots=True)
class H1ReportDownload:
    report_id: str
    filename: str
    value: bytes
    sha256: str


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


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


def _safe_number(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _safe_text(value: object, *, maximum: int = 512) -> str | None:
    return value if isinstance(value, str) and 0 < len(value) <= maximum and "\x00" not in value else None


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _identity(path: Path) -> _ReadIdentity:
    try:
        details = path.stat(follow_symlinks=False)
    except OSError as error:
        raise H1SnapshotUnreadableError("campaign publication is unreadable") from error
    if not stat.S_ISREG(details.st_mode):
        raise H1SnapshotIntegrityError("campaign publication is not a regular file")
    return _ReadIdentity(
        size=details.st_size,
        modified_ns=details.st_mtime_ns,
        inode=details.st_ino,
        digest="",
    )


def _validate_root(root: Path) -> Path:
    try:
        if root.is_symlink() or not root.exists() or not root.is_dir():
            raise H1SnapshotUnreadableError("configured campaign root is unavailable")
        resolved = root.resolve(strict=True)
    except OSError as error:
        raise H1SnapshotUnreadableError("configured campaign root is unreadable") from error
    if root.is_symlink():
        raise H1SnapshotIntegrityError("configured campaign root cannot be a symlink")
    return resolved


def _bounded_read(root: Path, relative: Path, *, maximum_bytes: int) -> _BoundedRead:
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise H1SnapshotIntegrityError("campaign path is outside the fixed contract")
    resolved_root = _validate_root(root)
    current = root
    try:
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise H1SnapshotIntegrityError("campaign publication cannot use symlinks")
        resolved = current.resolve(strict=True)
    except FileNotFoundError:
        raise
    except OSError as error:
        raise H1SnapshotUnreadableError("campaign publication is unreadable") from error
    if not resolved.is_relative_to(resolved_root):
        raise H1SnapshotIntegrityError("campaign publication escaped its explicit root")
    before = _identity(current)
    if before.size > maximum_bytes:
        raise H1SnapshotIntegrityError("campaign publication exceeds its bounded size")
    try:
        value = current.read_bytes()
    except OSError as error:
        raise H1SnapshotUnreadableError("campaign publication is unreadable") from error
    after = _identity(current)
    if (before.size, before.modified_ns, before.inode) != (
        after.size,
        after.modified_ns,
        after.inode,
    ) or len(value) != before.size:
        raise H1SnapshotHeadChangedError("HEAD_CHANGED_RETRY")
    return _BoundedRead(
        value=value,
        identity=_ReadIdentity(before.size, before.modified_ns, before.inode, _sha256(value)),
    )


def _decode_object(read: _BoundedRead, *, label: str, canonical: bool = False) -> dict[str, Any]:
    try:
        decoded = json.loads(read.value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise H1SnapshotIntegrityError(f"{label} is not valid UTF-8 JSON") from error
    if not isinstance(decoded, dict):
        raise H1SnapshotIntegrityError(f"{label} must be a JSON object")
    if canonical and canonical_json_bytes(decoded) != read.value:
        raise H1SnapshotIntegrityError(f"{label} is not canonical JSON")
    return decoded


def _pin_digest(value: bytes) -> str:
    try:
        text = value.decode("ascii")
    except UnicodeDecodeError as error:
        raise H1SnapshotIntegrityError("campaign pin is not ASCII") from error
    parts = text.split()
    if len(parts) != 2 or parts[1] != "campaign-manifest.json":
        raise H1SnapshotIntegrityError("campaign pin format is invalid")
    digest = parts[0]
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise H1SnapshotIntegrityError("campaign pin hash is invalid")
    return digest


def _metric(value: object, *, provenance: str, certifiable: bool) -> dict[str, object]:
    return {
        "available": value is not None,
        "certifiable": certifiable and value is not None,
        "provenance": provenance if value is not None else "NON DISPONIBLE",
        "value": value,
    }


def _base_snapshot(*, now: datetime, fixture: bool, fixture_name: str | None) -> dict[str, Any]:
    return {
        "schema_version": H1_SNAPSHOT_SCHEMA_VERSION,
        "mode": H1_MODE,
        "orders_enabled": False,
        "boundary": H1_BOUNDARY,
        "generated_at_utc": _utc_text(now),
        "fixture": fixture,
        "fixture_name": fixture_name,
        "data_classification": H1_FIXTURE_LABEL if fixture else "AUTHENTICATED_CAMPAIGN_PUBLICATION",
        "economic_evidence_status": ECONOMIC_NOT_AVAILABLE,
        "limits": {
            "head_read_attempts": _HEAD_READ_ATTEMPTS,
            "incident_limit": _INCIDENT_LIMIT,
            "tail_segment_limit": _TAIL_SEGMENT_LIMIT,
        },
    }


def _empty_economics(*, provenance: str = "NON DISPONIBLE") -> dict[str, Any]:
    names = (
        "fees",
        "funding",
        "slippage",
        "spread",
        "adverse_selection",
        "opportunity_cost",
        "realized_pnl",
        "unrealized_pnl",
        "net_pnl",
        "drawdown",
        "turnover_notional",
        "gross_exposure",
        "concentration_top_one_percent",
    )
    return {
        "status": ECONOMIC_NOT_AVAILABLE,
        "certifiable": False,
        "provenance": provenance,
        "metrics": {name: _metric(None, provenance="NON DISPONIBLE", certifiable=False) for name in names},
        "gates": [],
    }


def _variant_rows(policy: H1PolicyConfig | None, manifest: Mapping[str, object] | None) -> list[dict[str, Any]]:
    raw = None if manifest is None else manifest.get("variants")
    if not isinstance(raw, list) and policy is not None:
        raw = [item.to_dict() for item in policy.variants]
    rows: list[dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw[:32]:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "variant_id": _safe_text(item.get("variant_id")) or "NON DISPONIBLE",
                    "role": "PRIMARY" if item.get("status") == "PRIMARY_FROZEN_UNOBSERVED" else "REGISTERED",
                    "status": _safe_text(item.get("status")) or "NON DISPONIBLE",
                    "holdout_access": _safe_text(item.get("holdout_access")) or "SEALED",
                    "ranking_mutable": False,
                }
            )
    return rows


def _fixture_feeds(*, stale: bool = False) -> list[dict[str, Any]]:
    feeds = ("metadata", "bbo", "l2_book", "trades", "all_mids", "active_asset_context")
    rows: list[dict[str, Any]] = []
    for market in ("BTC", "ETH", "SOL", "HYPE"):
        for feed in feeds:
            is_stale = stale and (market, feed) in {("SOL", "l2_book"), ("HYPE", "trades")}
            rows.append(
                {
                    "market": market,
                    "feed": feed,
                    "status": "STALE" if is_stale else "FRESH",
                    "age_seconds": 18 if is_stale else 1,
                    "last_observed_at_utc": "SYNTHETIC",
                }
            )
    return rows


def _fixture_snapshot(name: str, *, now: datetime) -> dict[str, Any]:
    normalized = name.upper()
    states = {
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
    if normalized not in states:
        raise KeyError(name)
    starts = now - timedelta(days=4, hours=7)
    if normalized in {"PREPARED_NOT_STARTED", "ARMED"}:
        starts = now + timedelta(hours=6)
    if normalized == "HOLDOUT_SEALED":
        starts = now - timedelta(days=11, hours=2)
    if normalized in {"COMPLETE_COLLECTION_WINDOW", "HOLDOUT_OPEN"}:
        starts = now - timedelta(days=14)
    ends = starts + timedelta(days=14)
    holdout_starts = starts + timedelta(days=10)
    running = normalized in {"RUNNING_HEALTHY", "STALE_RECONNECTING", "HOLDOUT_SEALED"}
    stale = normalized == "STALE_RECONNECTING"
    completed = normalized in {"COMPLETE_COLLECTION_WINDOW", "HOLDOUT_OPEN"}
    strategy_available = (running or completed) and normalized != "HOLDOUT_SEALED"
    holdout_open = normalized in {"COMPLETE_COLLECTION_WINDOW", "HOLDOUT_OPEN"}
    phase = "PRE_START"
    if running:
        phase = "HOLDOUT" if now >= holdout_starts else "TRAIN"
    if completed:
        phase = "COMPLETE"
    state_code = normalized
    if normalized == "HOLDOUT_SEALED":
        state_code = "RUNNING_HEALTHY"
    elif normalized == "STALE_RECONNECTING":
        state_code = "RECONNECTING"
    elif normalized == "HOLDOUT_OPEN":
        state_code = "COMPLETE_COLLECTION_WINDOW"
    snapshot = _base_snapshot(now=now, fixture=True, fixture_name=normalized)
    snapshot.update(
        {
            "state": {
                "code": state_code,
                "freshness": "STALE" if stale else ("FRESH" if running else "NON DISPONIBLE"),
                "integrity": "FAILED" if normalized == "INTEGRITY_FAILED" else "SYNTHETIC_OK",
                "retryable": normalized in {"STALE_RECONNECTING", "INTERRUPTED_RECOVERABLE"},
                "last_updated_at_utc": _utc_text(now - timedelta(seconds=42 if stale else 2)),
                "last_observation_at_utc": _utc_text(now - timedelta(seconds=48 if stale else 1)),
            },
            "identity": {
                "campaign_id": "h1-synthetic-dashboard-v1",
                "policy_id": "hyperliquid-h1-one-sided-selective-maker-v1",
                "policy_config_sha256": "synthetic-" + "1" * 54,
                "campaign_manifest_sha256": "synthetic-" + "2" * 54,
                "raw_manifest_sha256": None if not running and not completed else "synthetic-" + "3" * 54,
                "raw_root_sha256": None if not running and not completed else "synthetic-" + "4" * 54,
                "fee_artifact_sha256": "synthetic-" + "5" * 54,
                "source_commit": "SYNTHETIC_FIXTURE",
            },
            "progress": {
                "starts_at_utc": _utc_text(starts),
                "ends_at_utc": _utc_text(ends),
                "elapsed_seconds": max(0, int((now - starts).total_seconds())),
                "progress_percent": 100 if completed else (31 if running and not stale else (79 if stale else 0)),
                "phase": phase,
                "train": {"start_day": 0, "end_day": 7},
                "validation": {"start_day": 7, "end_day": 10},
                "holdout": {
                    "start_day": 10,
                    "end_day": 14,
                    "access": "OPEN" if holdout_open else "SEALED",
                    "remaining_seconds": 0 if holdout_open else max(0, int((ends - now).total_seconds())),
                    "integrity_controls": "SYNTHETIC_FIXTURE_ONLY",
                },
            },
            "collection": {
                "frames": None if not running and not completed else 18_432_901,
                "segments": None if not running and not completed else 1234,
                "stored_bytes": None if not running and not completed else 18_734_201_992,
                "gaps": None if not running and not completed else (2 if stale else 0),
                "duplicates": None if not running and not completed else (17 if stale else 0),
                "duplicates_scope": "SYNTHETIC_TAIL",
                "reconnects": None if not running and not completed else (3 if stale else 0),
                "connection_generation": None if not running and not completed else (4 if stale else 1),
                "queue_high_water": None if not running and not completed else 84,
            },
            "feeds": _fixture_feeds(stale=stale) if running or completed else [],
            "safety": {
                "kill_rules": [
                    "RAW_MANIFEST_AUTHENTICATION_FAILURE",
                    "SOURCE_GAP_OR_RECONNECT",
                    "BOOK_OR_CONTEXT_STALE",
                    "RECONCILIATION_DIVERGENCE",
                ],
                "stale_feeds": ["SOL/l2_book", "HYPE/trades"] if stale else [],
                "disk_capacity": _metric("72% libre", provenance=H1_FIXTURE_LABEL, certifiable=False),
                "integrity": "FAILED" if normalized == "INTEGRITY_FAILED" else "SYNTHETIC_OK",
            },
            "strategy": {
                "decisions": {"BID_ONLY": 2780, "ASK_ONLY": 2594, "NO_QUOTE": 31_442}
                if strategy_available
                else {"BID_ONLY": None, "ASK_ONLY": None, "NO_QUOTE": None},
                "no_quote_reasons": [
                    {"reason": "SIGNAL_NOT_SELECTIVE", "count": 18_140},
                    {"reason": "TRADE_FLOW_INSUFFICIENT", "count": 7_102},
                    {"reason": "SPREAD_OUTSIDE_FROZEN_BOUNDS", "count": 3_406},
                ]
                if strategy_available
                else [],
                "intentions": 5374 if strategy_available else None,
                "fills": 821 if strategy_available else None,
                "partial_fills": 112 if strategy_available else None,
                "missed_fills": 4553 if strategy_available else None,
                "inventory": {"BTC": "0.0004", "ETH": "-0.006", "SOL": "0", "HYPE": "12"}
                if strategy_available
                else {},
                "unresolved_closeouts": (
                    {} if completed else {"HYPE": "12"} if strategy_available else {}
                ),
            },
            "variants": [
                {
                    "variant_id": "imbalance-flow-confirm-v1",
                    "role": "PRIMARY",
                    "status": "PRIMARY_FROZEN_UNOBSERVED",
                    "holdout_access": "OPEN" if holdout_open else "SEALED",
                    "ranking_mutable": False,
                },
                {
                    "variant_id": "depth-only-loose-losing-registry-v1",
                    "role": "REGISTERED",
                    "status": "REGISTERED_UNOBSERVED",
                    "holdout_access": "OPEN" if holdout_open else "SEALED",
                    "ranking_mutable": False,
                },
                {
                    "variant_id": "imbalance-flow-strict-losing-registry-v1",
                    "role": "REGISTERED",
                    "status": "REGISTERED_UNOBSERVED",
                    "holdout_access": "OPEN" if holdout_open else "SEALED",
                    "ranking_mutable": False,
                },
            ],
            "incidents": [
                {
                    "at_utc": _utc_text(now - timedelta(minutes=4)),
                    "code": "RECONNECTING",
                    "detail": "Fixture synthétique : reconnexion bornée en cours.",
                    "severity": "warning",
                }
            ]
            if stale
            else [],
            "reports": [],
        }
    )
    economics = _empty_economics(provenance=H1_FIXTURE_LABEL)
    if holdout_open:
        economics["metrics"] = {
            "fees": _metric("-124.81", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "funding": _metric("0", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "slippage": _metric("4.2 bps p99", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "spread": _metric("182.10", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "adverse_selection": _metric("-77.30", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "opportunity_cost": _metric("0", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "realized_pnl": _metric("-20.01", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "unrealized_pnl": _metric("0", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "net_pnl": _metric("-20.01", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "drawdown": _metric(None, provenance="NON DISPONIBLE", certifiable=False),
            "turnover_notional": _metric("384201", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "gross_exposure": _metric("0", provenance=H1_FIXTURE_LABEL, certifiable=False),
            "concentration_top_one_percent": _metric("0.18", provenance=H1_FIXTURE_LABEL, certifiable=False),
        }
        economics["gates"] = [
            {"gate": "minimum_fills_5000", "passed": False},
            {"gate": "minimum_markets_3", "passed": True},
            {"gate": "lcb95_net_positive_500ms_zero_rebate", "passed": False},
            {"gate": "reconciliation_exact", "passed": True},
        ]
        snapshot["reports"] = [
            {
                "report_id": "ghost-h1",
                "title": "Rapport H1 synthétique de démonstration",
                "sha256": "synthetic-" + "6" * 54,
                "download_url": None,
                "synthetic": True,
            }
        ]
    snapshot["economics"] = economics
    return snapshot


def h1_fixture_names() -> tuple[str, ...]:
    return (
        "PREPARED_NOT_STARTED",
        "ARMED",
        "RUNNING_HEALTHY",
        "STALE_RECONNECTING",
        "INTERRUPTED_RECOVERABLE",
        "INTEGRITY_FAILED",
        "COMPLETE_COLLECTION_WINDOW",
        "HOLDOUT_SEALED",
        "HOLDOUT_OPEN",
    )


def _load_policy(path: Path | None) -> H1PolicyConfig | None:
    if path is None:
        return None
    try:
        if path.is_symlink():
            raise H1SnapshotIntegrityError("configured H1 policy cannot be a symlink")
        before = _identity(path)
        if before.size > _MAX_CAMPAIGN_MANIFEST_BYTES:
            raise H1SnapshotIntegrityError("configured H1 policy exceeds its bounded size")
        value = path.read_bytes()
        after = _identity(path)
    except H1SnapshotIntegrityError:
        raise
    except OSError as error:
        raise H1SnapshotUnreadableError("configured H1 policy is unreadable") from error
    if (before.size, before.modified_ns, before.inode) != (
        after.size,
        after.modified_ns,
        after.inode,
    ) or len(value) != before.size:
        raise H1SnapshotHeadChangedError("HEAD_CHANGED_RETRY")
    try:
        return H1PolicyConfig.from_bytes(value)
    except (TypeError, ValueError) as error:
        raise H1SnapshotIntegrityError("configured H1 policy is invalid") from error


def _progress(manifest: Mapping[str, object], *, now: datetime, holdout_open: bool) -> dict[str, Any]:
    starts = _parse_utc(manifest.get("starts_at_utc"))
    ends = _parse_utc(manifest.get("ends_at_utc"))
    holdout = manifest.get("holdout")
    holdout_map = holdout if isinstance(holdout, dict) else {}
    holdout_starts = _parse_utc(holdout_map.get("starts_at_utc"))
    total = None if starts is None or ends is None else max(0.0, (ends - starts).total_seconds())
    elapsed = None if starts is None else max(0, int((now - starts).total_seconds()))
    percent = None
    if total is not None and total > 0 and elapsed is not None:
        percent = min(100, max(0, int(elapsed / total * 100)))
    phase = "NON DISPONIBLE"
    if starts is not None and now < starts:
        phase = "PRE_START"
    elif starts is not None and holdout_starts is not None and now < holdout_starts:
        validation_start = starts + timedelta(days=7)
        phase = "TRAIN" if now < validation_start else "VALIDATION"
    elif holdout_starts is not None and now >= holdout_starts:
        phase = "HOLDOUT"
    if ends is not None and now >= ends:
        phase = "COMPLETE"
    remaining = None if ends is None else max(0, int((ends - now).total_seconds()))
    return {
        "starts_at_utc": None if starts is None else _utc_text(starts),
        "ends_at_utc": None if ends is None else _utc_text(ends),
        "elapsed_seconds": elapsed,
        "progress_percent": percent,
        "phase": phase,
        "train": {"start_day": 0, "end_day": 7},
        "validation": {"start_day": 7, "end_day": 10},
        "holdout": {
            "start_day": 10,
            "end_day": 14,
            "access": "OPEN" if holdout_open else "SEALED",
            "remaining_seconds": remaining,
            "integrity_controls": "CAMPAIGN_MANIFEST_PIN_AND_RAW_CONTENT_ADDRESSED_CHAIN",
        },
    }


def _state_code(health: Mapping[str, object], *, now: datetime, modified_ns: int) -> tuple[str, str]:
    terminal = _safe_text(health.get("terminal_health")) or "UNREADABLE_FAIL_CLOSED"
    age = max(0.0, now.timestamp() - modified_ns / 1_000_000_000)
    if terminal == "RUNNING" and age > _HEALTH_STALE_AFTER_SECONDS:
        return "STALE", "STALE"
    if terminal == "RUNNING":
        return "RUNNING_HEALTHY", "FRESH"
    if terminal in _RECOVERABLE_STATES:
        return "INTERRUPTED_RECOVERABLE", "STALE"
    if terminal in _COMPLETE_STATES:
        return terminal, "FINAL"
    if terminal in {"PREPARED_NOT_STARTED", "ARMED"}:
        return terminal, "NON DISPONIBLE"
    if terminal in {
        "PUBLIC_SOURCE_INVALID_FAIL_CLOSED",
        "FINAL_THRESHOLD_REPLAY_INVALID_FAIL_CLOSED",
        "MAX_BYTES_REACHED",
    }:
        return "INTEGRITY_FAILED", "FAILED"
    return terminal, "NON DISPONIBLE"


def _validate_raw_tail(
    root: Path,
    manifest_sha256: str,
) -> tuple[ManifestRecord, list[Any], dict[Path, _ReadIdentity]]:
    manifest_read = _bounded_read(
        root,
        Path("raw/manifests") / f"{manifest_sha256}{MANIFEST_SUFFIX}",
        maximum_bytes=_MAX_RAW_MANIFEST_BYTES,
    )
    try:
        manifest = decode_manifest(manifest_read.value, expected_manifest_sha256=manifest_sha256)
    except (ResearchDataIntegrityError, ValueError) as error:
        raise H1SnapshotIntegrityError("raw manifest authentication failed") from error
    identities = {
        Path("raw/manifests") / f"{manifest_sha256}{MANIFEST_SUFFIX}": manifest_read.identity
    }
    envelopes: list[Any] = []
    for descriptor in manifest.segments[-_TAIL_SEGMENT_LIMIT:]:
        relative = Path("raw/segments") / f"{descriptor.physical_sha256}{SEGMENT_SUFFIX}"
        segment_read = _bounded_read(root, relative, maximum_bytes=_MAX_SEGMENT_BYTES)
        try:
            artifact = decode_segment(
                segment_read.value,
                expected_physical_sha256=descriptor.physical_sha256,
            )
        except (ResearchDataIntegrityError, ValueError) as error:
            raise H1SnapshotIntegrityError("raw tail segment authentication failed") from error
        if artifact.descriptor != descriptor:
            raise H1SnapshotIntegrityError("raw tail segment differs from its manifest")
        allowed_provenance = {
            (HYPERLIQUID_PUBLIC_HTTP_URL, "PUBLIC_HTTP"),
            (HYPERLIQUID_PUBLIC_WEBSOCKET_URL, "PUBLIC_WEBSOCKET"),
        }
        if any(
            envelope.venue.value != "hyperliquid"
            or envelope.provenance.fixture_label is not None
            or (envelope.provenance.source_url, envelope.provenance.transport)
            not in allowed_provenance
            or envelope.source_metadata_version != HYPERLIQUID_METADATA_VERSION
            for envelope in artifact.envelopes
        ):
            raise H1SnapshotIntegrityError("raw tail provenance is not official public Hyperliquid")
        envelopes.extend(artifact.envelopes)
        identities[relative] = segment_read.identity
    return manifest, envelopes, identities


def _feed_rows(envelopes: list[Any], *, now: datetime) -> tuple[list[dict[str, Any]], int, list[dict[str, Any]]]:
    newest: dict[tuple[str, str], Any] = {}
    duplicates = 0
    incidents: list[dict[str, Any]] = []
    for envelope in envelopes:
        market = "GLOBAL"
        if isinstance(envelope.instrument_id, str):
            parts = envelope.instrument_id.split(":")
            if len(parts) >= 2:
                market = parts[1]
        newest[(market, envelope.feed_type)] = envelope
        duplicates += int(envelope.state.duplicate)
        if envelope.state.gap_detected or envelope.state.reconnect:
            incidents.append(
                {
                    "at_utc": _utc_text(
                        datetime.fromtimestamp(envelope.receive_timestamp_utc_ns / 1_000_000_000, tz=UTC)
                    ),
                    "code": "GAP" if envelope.state.gap_detected else "RECONNECT",
                    "detail": envelope.state.reason or "SOURCE_GAP_OR_RECONNECT",
                    "severity": "warning",
                }
            )
    rows: list[dict[str, Any]] = []
    for market in ("BTC", "ETH", "SOL", "HYPE"):
        for feed in ("metadata", "bbo", "l2_book", "trades", "all_mids", "active_asset_context"):
            envelope = newest.get((market, feed)) or newest.get(("GLOBAL", feed))
            if envelope is None:
                rows.append(
                    {
                        "market": market,
                        "feed": feed,
                        "status": "NON DISPONIBLE",
                        "age_seconds": None,
                        "last_observed_at_utc": None,
                    }
                )
                continue
            observed = datetime.fromtimestamp(envelope.receive_timestamp_utc_ns / 1_000_000_000, tz=UTC)
            age = max(0, int((now - observed).total_seconds()))
            rows.append(
                {
                    "market": market,
                    "feed": feed,
                    "status": "FRESH" if age <= _HEALTH_STALE_AFTER_SECONDS else "STALE",
                    "age_seconds": age,
                    "last_observed_at_utc": _utc_text(observed),
                }
            )
    return rows, duplicates, incidents[-_INCIDENT_LIMIT:]


def _report_payload(
    raw: bytes,
    *,
    policy_sha256: str | None,
    raw_manifest: ManifestRecord,
) -> dict[str, Any]:
    stripped = raw.rstrip(b"\r\n")
    if raw[len(stripped) :] not in {b"", b"\n", b"\r\n"}:
        raise H1SnapshotIntegrityError("H1 report has unsupported trailing bytes")
    try:
        decoded = json.loads(stripped.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise H1SnapshotIntegrityError("H1 report is not valid JSON") from error
    if not isinstance(decoded, dict) or canonical_json_bytes(decoded) != stripped:
        raise H1SnapshotIntegrityError("H1 report is not canonical JSON")
    report_sha = decoded.get("report_sha256")
    if not isinstance(report_sha, str):
        raise H1SnapshotIntegrityError("H1 report identity is absent")
    body = dict(decoded)
    del body["report_sha256"]
    if _sha256(canonical_json_bytes(body)) != report_sha:
        raise H1SnapshotIntegrityError("H1 report self-identity failed")
    if decoded.get("boundary") != H1_BOUNDARY:
        raise H1SnapshotIntegrityError("H1 report crossed the public Ghost boundary")
    if policy_sha256 is not None and decoded.get("policy_config_sha256") != policy_sha256:
        raise H1SnapshotIntegrityError("H1 report policy identity differs from campaign")
    if decoded.get("raw_manifest_sha256") != raw_manifest.manifest_sha256:
        raise H1SnapshotIntegrityError("H1 report raw manifest is not the current campaign head")
    if decoded.get("raw_root_sha256") != raw_manifest.root_sha256:
        raise H1SnapshotIntegrityError("H1 report raw root differs from campaign")
    expected_segments = [item.physical_sha256 for item in raw_manifest.segments]
    if decoded.get("segment_sha256s") != expected_segments:
        raise H1SnapshotIntegrityError("H1 report segment identities differ from campaign")
    return decoded


def _report_sections(report: Mapping[str, object]) -> tuple[dict[str, Any], dict[str, Any]]:
    latency = report.get("latency_reports")
    selected = None
    if isinstance(latency, list):
        selected = next(
            (item for item in latency if isinstance(item, dict) and item.get("latency_ms") == 500),
            None,
        )
    if not isinstance(selected, dict):
        return {
            "decisions": {"BID_ONLY": None, "ASK_ONLY": None, "NO_QUOTE": None},
            "no_quote_reasons": [],
            "intentions": None,
            "fills": None,
            "partial_fills": None,
            "missed_fills": None,
            "inventory": {},
            "unresolved_closeouts": {},
        }, _empty_economics(provenance="NON DISPONIBLE")
    decisions = selected.get("decisions")
    decision_rows = [item for item in decisions if isinstance(item, dict)] if isinstance(decisions, list) else []
    decisions_available = isinstance(decisions, list)
    action_counts = Counter(str(item.get("action")) for item in decision_rows)
    reasons = Counter(
        str(item.get("reason"))
        for item in decision_rows
        if item.get("action") == "NO_QUOTE" and isinstance(item.get("reason"), str)
    )
    ghost = selected.get("ghost")
    ghost_map = ghost if isinstance(ghost, dict) else {}
    orders = ghost_map.get("orders")
    order_rows = [item for item in orders if isinstance(item, dict)] if isinstance(orders, list) else []
    orders_available = isinstance(orders, list)
    fills = ghost_map.get("fills")
    fill_rows = [item for item in fills if isinstance(item, dict)] if isinstance(fills, list) else []
    fills_available = isinstance(fills, list)
    partial = (
        sum(
            1
            for item in order_rows
            if item.get("filled_quantity") not in {None, "0", 0}
            and item.get("unfilled_quantity") not in {None, "0", 0}
        )
        if orders_available
        else None
    )
    missed = (
        sum(1 for item in order_rows if item.get("filled_quantity") in {"0", 0})
        if orders_available
        else None
    )
    exposure = ghost_map.get("exposure")
    exposure_map = exposure if isinstance(exposure, dict) else {}
    attribution = selected.get("attribution")
    attribution_map = attribution if isinstance(attribution, dict) else {}
    concentration = selected.get("concentration")
    concentration_map = concentration if isinstance(concentration, dict) else {}
    gates = selected.get("economic_gates")
    gate_map = gates if isinstance(gates, dict) else {}
    provenance = "AUTHENTICATED_FINAL_H1_REPORT_500MS"
    metrics = {
        "fees": _metric(attribution_map.get("fees"), provenance=provenance, certifiable=True),
        "funding": _metric(attribution_map.get("funding"), provenance=provenance, certifiable=True),
        "slippage": _metric(
            concentration_map.get("closeout_slippage_p99_bps"), provenance=provenance, certifiable=True
        ),
        "spread": _metric(attribution_map.get("spread"), provenance=provenance, certifiable=True),
        "adverse_selection": _metric(
            attribution_map.get("adverse_selection"), provenance=provenance, certifiable=True
        ),
        "opportunity_cost": _metric(
            attribution_map.get("opportunity_cost"), provenance=provenance, certifiable=True
        ),
        "realized_pnl": _metric(
            attribution_map.get("realized_pnl"), provenance=provenance, certifiable=True
        ),
        "unrealized_pnl": _metric(
            attribution_map.get("unrealized_pnl"), provenance=provenance, certifiable=True
        ),
        "net_pnl": _metric(attribution_map.get("net"), provenance=provenance, certifiable=True),
        "drawdown": _metric(None, provenance="NON DISPONIBLE", certifiable=False),
        "turnover_notional": _metric(
            exposure_map.get("gross_filled_notional"), provenance=provenance, certifiable=True
        ),
        "gross_exposure": _metric(
            exposure_map.get("gross_exposure"), provenance=provenance, certifiable=True
        ),
        "concentration_top_one_percent": _metric(
            concentration_map.get("top_one_percent_share"), provenance=provenance, certifiable=True
        ),
    }
    strategy = {
        "decisions": {
            "BID_ONLY": action_counts["BID_ONLY"] if decisions_available else None,
            "ASK_ONLY": action_counts["ASK_ONLY"] if decisions_available else None,
            "NO_QUOTE": action_counts["NO_QUOTE"] if decisions_available else None,
        },
        "no_quote_reasons": [
            {"reason": reason, "count": count} for reason, count in reasons.most_common(8)
        ],
        "intentions": len(order_rows) if orders_available else None,
        "fills": len(fill_rows) if fills_available else None,
        "partial_fills": partial,
        "missed_fills": missed,
        "inventory": exposure_map.get("positions") if isinstance(exposure_map.get("positions"), dict) else {},
        "unresolved_closeouts": (
            exposure_map.get("unresolved_closeout")
            if isinstance(exposure_map.get("unresolved_closeout"), dict)
            else {}
        ),
    }
    economics = {
        "status": report.get("economic_status", ECONOMIC_NOT_AVAILABLE),
        "certifiable": report.get("synthetic") is False,
        "provenance": provenance,
        "metrics": metrics,
        "gates": [
            {"gate": str(name), "passed": passed}
            for name, passed in sorted(gate_map.items())
            if type(passed) is bool
        ],
    }
    return strategy, economics


def _snapshot_once(
    root: Path,
    *,
    policy: H1PolicyConfig | None,
    now: datetime,
) -> dict[str, Any]:
    tracked: dict[Path, _ReadIdentity] = {}
    manifest_read = _bounded_read(root, Path("campaign-manifest.json"), maximum_bytes=_MAX_CAMPAIGN_MANIFEST_BYTES)
    tracked[Path("campaign-manifest.json")] = manifest_read.identity
    pin_read = _bounded_read(root, Path("campaign-manifest.sha256"), maximum_bytes=512)
    tracked[Path("campaign-manifest.sha256")] = pin_read.identity
    pin = _pin_digest(pin_read.value)
    if pin != manifest_read.identity.digest:
        raise H1SnapshotIntegrityError("campaign manifest differs from its immutable pin")
    manifest = _decode_object(manifest_read, label="campaign manifest", canonical=True)
    if manifest.get("boundary") != H1_BOUNDARY or manifest.get("schema_version") != 1:
        raise H1SnapshotIntegrityError("campaign manifest boundary or schema is invalid")
    policy_sha = _safe_text(manifest.get("policy_config_sha256"), maximum=64)
    if policy is not None and policy_sha != policy.config_sha256:
        raise H1SnapshotIntegrityError("campaign manifest policy differs from configured H1 policy")
    health_read = _bounded_read(root, Path("state/health.json"), maximum_bytes=_MAX_HEALTH_BYTES)
    tracked[Path("state/health.json")] = health_read.identity
    health = _decode_object(health_read, label="campaign health")
    if health.get("boundary") != H1_BOUNDARY:
        raise H1SnapshotIntegrityError("campaign health crossed the public Ghost boundary")
    if health.get("campaign_id") != manifest.get("campaign_id"):
        raise H1SnapshotIntegrityError("campaign health identity differs from manifest")
    state_code, freshness = _state_code(health, now=now, modified_ns=health_read.identity.modified_ns)
    terminal = _safe_text(health.get("terminal_health")) or "UNREADABLE_FAIL_CLOSED"
    holdout_map = manifest.get("holdout")
    holdout_starts = _parse_utc(
        holdout_map.get("starts_at_utc") if isinstance(holdout_map, dict) else None
    )
    holdout_open = (
        terminal in _COMPLETE_STATES
        and holdout_starts is not None
        and now >= holdout_starts
    )
    raw_manifest_sha = _safe_text(health.get("manifest_sha256"), maximum=64)
    raw_manifest: ManifestRecord | None = None
    tail: list[Any] = []
    if raw_manifest_sha is not None:
        if len(raw_manifest_sha) != 64 or any(c not in "0123456789abcdef" for c in raw_manifest_sha):
            raise H1SnapshotIntegrityError("health raw manifest identity is invalid")
        raw_manifest, tail, raw_identities = _validate_raw_tail(root, raw_manifest_sha)
        tracked.update(raw_identities)
        if raw_manifest.collection_id != manifest.get("campaign_id"):
            raise H1SnapshotIntegrityError("raw collection identity differs from campaign")
        published_root = health.get("raw_root_sha256")
        if published_root is not None and published_root != raw_manifest.root_sha256:
            raise H1SnapshotIntegrityError("health raw root differs from authenticated manifest")
    feeds, tail_duplicates, tail_incidents = _feed_rows(tail, now=now)
    report_payload: dict[str, Any] | None = None
    reports: list[dict[str, Any]] = []
    report_id: str | None = None
    if holdout_open and raw_manifest is not None:
        for candidate_id, relative in _REPORT_PATHS.items():
            try:
                report_read = _bounded_read(root, relative, maximum_bytes=_MAX_REPORT_BYTES)
            except FileNotFoundError:
                continue
            decoded = _report_payload(
                report_read.value,
                policy_sha256=policy_sha,
                raw_manifest=raw_manifest,
            )
            tracked[relative] = report_read.identity
            if decoded.get("synthetic") is not False:
                raise H1SnapshotIntegrityError("real campaign report cannot use synthetic provenance")
            report_payload = decoded
            report_id = candidate_id
            reports.append(
                {
                    "report_id": candidate_id,
                    "title": "Rapport H1 authentifié",
                    "sha256": report_read.identity.digest,
                    "download_url": f"/api/h1/reports/{candidate_id}",
                    "synthetic": False,
                }
            )
            break
    for relative, expected in tracked.items():
        current = _bounded_read(
            root,
            relative,
            maximum_bytes={
                Path("campaign-manifest.json"): _MAX_CAMPAIGN_MANIFEST_BYTES,
                Path("campaign-manifest.sha256"): 512,
                Path("state/health.json"): _MAX_HEALTH_BYTES,
            }.get(relative, _MAX_REPORT_BYTES if relative in _REPORT_PATHS.values() else _MAX_SEGMENT_BYTES),
        ).identity
        if current != expected:
            raise H1SnapshotHeadChangedError("HEAD_CHANGED_RETRY")
    strategy, economics = (
        _report_sections(report_payload)
        if report_payload is not None
        else (
            {
                "decisions": {"BID_ONLY": 0, "ASK_ONLY": 0, "NO_QUOTE": 0},
                "no_quote_reasons": [],
                "intentions": 0,
                "fills": 0,
                "partial_fills": 0,
                "missed_fills": 0,
                "inventory": {},
                "unresolved_closeouts": {},
            },
            _empty_economics(),
        )
    )
    last_observation = None
    if raw_manifest is not None and raw_manifest.segments:
        last_ns = raw_manifest.segments[-1].last_receive_timestamp_utc_ns
        last_observation = _utc_text(datetime.fromtimestamp(last_ns / 1_000_000_000, tz=UTC))
    health_modified = datetime.fromtimestamp(health_read.identity.modified_ns / 1_000_000_000, tz=UTC)
    reconnects = _safe_number(health.get("reconnects"))
    stale_feeds = [f"{row['market']}/{row['feed']}" for row in feeds if row["status"] == "STALE"]
    snapshot = _base_snapshot(now=now, fixture=False, fixture_name=None)
    snapshot.update(
        {
            "state": {
                "code": state_code,
                "freshness": freshness,
                "integrity": "AUTHENTICATED_TAIL_READONLY",
                "retryable": state_code in {"STALE", "INTERRUPTED_RECOVERABLE"},
                "last_updated_at_utc": _utc_text(health_modified),
                "last_observation_at_utc": last_observation,
            },
            "identity": {
                "campaign_id": manifest.get("campaign_id"),
                "policy_id": manifest.get("policy_id"),
                "policy_config_sha256": policy_sha,
                "campaign_manifest_sha256": pin,
                "raw_manifest_sha256": None if raw_manifest is None else raw_manifest.manifest_sha256,
                "raw_root_sha256": None if raw_manifest is None else raw_manifest.root_sha256,
                "fee_artifact_sha256": manifest.get("fee_artifact_sha256"),
                "source_commit": manifest.get("source_commit") or manifest.get("source_commit_sha256"),
            },
            "progress": _progress(manifest, now=now, holdout_open=holdout_open),
            "collection": {
                "frames": (
                    raw_manifest.frame_count if raw_manifest is not None else _safe_number(health.get("frames"))
                ),
                "segments": (
                    len(raw_manifest.segments) if raw_manifest is not None else _safe_number(health.get("segments"))
                ),
                "stored_bytes": (
                    raw_manifest.stored_segment_bytes
                    if raw_manifest is not None
                    else _safe_number(health.get("stored_bytes"))
                ),
                "gaps": _safe_number(health.get("gaps")),
                "duplicates": tail_duplicates if tail else None,
                "duplicates_scope": "AUTHENTICATED_LATEST_SEGMENT_ONLY" if tail else "NON DISPONIBLE",
                "reconnects": reconnects,
                "connection_generation": None if reconnects is None else reconnects + 1,
                "queue_high_water": _safe_number(health.get("queue_high_water")),
            },
            "feeds": feeds,
            "safety": {
                "kill_rules": [] if policy is None else list(policy.body["risk"]["kill_rules"]),
                "stale_feeds": stale_feeds,
                "disk_capacity": _metric(
                    health.get("disk_capacity"),
                    provenance="state/health.json",
                    certifiable=False,
                ),
                "integrity": "AUTHENTICATED_TAIL_READONLY",
            },
            "strategy": strategy,
            "economics": economics,
            "variants": _variant_rows(policy, manifest),
            "incidents": tail_incidents,
            "reports": reports,
            "report_binding": report_id,
        }
    )
    return snapshot


def h1_snapshot(
    campaign_root: Path | None,
    *,
    policy_path: Path | None,
    default_fixture: str = "PREPARED_NOT_STARTED",
    now: datetime | None = None,
    snapshot_once: Callable[[Path, H1PolicyConfig | None, datetime], dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], int]:
    current = (now or datetime.now(tz=UTC)).astimezone(UTC)
    if campaign_root is None:
        return _fixture_snapshot(default_fixture, now=current), 200
    if not campaign_root.exists():
        payload = _base_snapshot(now=current, fixture=False, fixture_name=None)
        payload.update(
            {
                "state": {
                    "code": "ABSENT",
                    "freshness": "NON DISPONIBLE",
                    "integrity": "NOT_STARTED",
                    "retryable": True,
                    "last_updated_at_utc": None,
                    "last_observation_at_utc": None,
                },
                "identity": {},
                "progress": {},
                "collection": {},
                "feeds": [],
                "safety": {"kill_rules": [], "stale_feeds": [], "integrity": "NOT_STARTED"},
                "strategy": {},
                "economics": _empty_economics(),
                "variants": [],
                "incidents": [],
                "reports": [],
            }
        )
        return payload, 200
    reader = snapshot_once or (lambda root, configured_policy, stamp: _snapshot_once(
        root, policy=configured_policy, now=stamp
    ))
    for _attempt in range(_HEAD_READ_ATTEMPTS):
        try:
            policy = _load_policy(policy_path)
            return reader(campaign_root, policy, current), 200
        except H1SnapshotHeadChangedError:
            continue
        except H1SnapshotIntegrityError:
            payload = _fixture_snapshot("INTEGRITY_FAILED", now=current)
            payload.update(
                {
                    "fixture": False,
                    "fixture_name": None,
                    "data_classification": "CAMPAIGN_PUBLICATION_REJECTED_FAIL_CLOSED",
                    "state": {
                        "code": "INTEGRITY_FAILED",
                        "freshness": "FAILED",
                        "integrity": "FAILED_READONLY",
                        "retryable": False,
                        "last_updated_at_utc": None,
                        "last_observation_at_utc": None,
                    },
                    "identity": {},
                    "progress": {},
                    "collection": {},
                    "feeds": [],
                    "strategy": {},
                    "economics": _empty_economics(),
                    "variants": [],
                    "incidents": [],
                    "reports": [],
                }
            )
            return payload, 503
        except (H1SnapshotUnreadableError, OSError):
            payload = _fixture_snapshot("INTEGRITY_FAILED", now=current)
            payload.update(
                {
                    "fixture": False,
                    "fixture_name": None,
                    "data_classification": "CAMPAIGN_PUBLICATION_UNREADABLE_FAIL_CLOSED",
                    "state": {
                        "code": "UNREADABLE_FAIL_CLOSED",
                        "freshness": "FAILED",
                        "integrity": "UNREADABLE_FAIL_CLOSED",
                        "retryable": True,
                        "last_updated_at_utc": None,
                        "last_observation_at_utc": None,
                    },
                    "identity": {},
                    "progress": {},
                    "collection": {},
                    "feeds": [],
                    "strategy": {},
                    "economics": _empty_economics(),
                    "variants": [],
                    "incidents": [],
                    "reports": [],
                }
            )
            return payload, 503
    payload = _fixture_snapshot("INTEGRITY_FAILED", now=current)
    payload.update(
        {
            "fixture": False,
            "fixture_name": None,
            "data_classification": "HEAD_CHANGED_RETRY",
            "state": {
                "code": "HEAD_CHANGED_RETRY",
                "freshness": "RETRY",
                "integrity": "HEAD_CHANGED_RETRY",
                "retryable": True,
                "last_updated_at_utc": None,
                "last_observation_at_utc": None,
            },
            "identity": {},
            "progress": {},
            "collection": {},
            "feeds": [],
            "strategy": {},
            "economics": _empty_economics(),
            "variants": [],
            "incidents": [],
            "reports": [],
        }
    )
    return payload, 409


def h1_fixture_snapshot(name: str, *, now: datetime | None = None) -> dict[str, Any]:
    return _fixture_snapshot(name, now=(now or datetime.now(tz=UTC)).astimezone(UTC))


def h1_report_download(
    campaign_root: Path | None,
    report_id: str,
    *,
    policy_path: Path | None,
    now: datetime | None = None,
) -> H1ReportDownload | None:
    if campaign_root is None or report_id not in _REPORT_PATHS:
        return None
    snapshot, status_code = h1_snapshot(
        campaign_root,
        policy_path=policy_path,
        now=now,
    )
    if status_code != 200 or snapshot.get("fixture") is True:
        return None
    reports = snapshot.get("reports")
    if not isinstance(reports, list):
        return None
    allowed = next(
        (
            item
            for item in reports
            if isinstance(item, dict) and item.get("report_id") == report_id
        ),
        None,
    )
    if not isinstance(allowed, dict):
        return None
    expected_sha256 = allowed.get("sha256")
    if not isinstance(expected_sha256, str):
        return None
    try:
        read = _bounded_read(campaign_root, _REPORT_PATHS[report_id], maximum_bytes=_MAX_REPORT_BYTES)
    except (FileNotFoundError, H1SnapshotHeadChangedError, H1SnapshotIntegrityError, H1SnapshotUnreadableError):
        return None
    if read.identity.digest != expected_sha256:
        return None
    return H1ReportDownload(
        report_id=report_id,
        filename=_REPORT_PATHS[report_id].name,
        value=read.value,
        sha256=read.identity.digest,
    )


__all__ = [
    "H1_BOUNDARY",
    "H1_FIXTURE_LABEL",
    "H1_MODE",
    "H1ReportDownload",
    "H1SnapshotHeadChangedError",
    "H1SnapshotIntegrityError",
    "H1SnapshotUnreadableError",
    "h1_fixture_names",
    "h1_fixture_snapshot",
    "h1_report_download",
    "h1_snapshot",
]
