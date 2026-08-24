"""End-to-end certification for a complete Paper Golden V3 oracle.

This module deliberately contains only orchestration and independent binding
logic.  Logical extraction, export verification, differential comparison, and
replay remain implemented by :mod:`hyperlab.paper.golden_v3` and
:mod:`hyperlab.paper.golden_v3_replay`.

A candidate is append-only from an operator's point of view: the candidate
root must not exist for a fresh certification, failed work is retained for
diagnosis, and ``COMPLETE`` is the final file created.  The sole resumable
state is one exact, pinned export A with no export B or final publication;
every other failed layout remains non-resumable and non-certifiable in place.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import sqlite3
import stat
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Final
from uuid import uuid4

from hyperlab.paper.golden_v3 import (
    GOLDEN_STREAM_NAMES,
    GoldenDifferentialError,
    GoldenExportResult,
    GoldenVerification,
    _fsync_directory,
    _mkdir_durable,
    compare_golden_exports,
    export_golden_v3,
    iter_golden_stream,
    verify_golden_v3,
    write_external_pin,
)
from hyperlab.paper.golden_v3_replay import (
    GoldenReplayMismatchError,
    _compare_streams_connection,
    replay_golden_v3,
)
from hyperlab.paper.store import PaperStore

_MINIMUM_FREE_BYTES: Final[int] = 30 * 1024**3
_READ_CHUNK_BYTES: Final[int] = 8 * 1024**2
_CERTIFICATION_SCHEMA: Final[str] = "hyperlab-paper-golden-v3-certification-v1"
_CERTIFIED_STATUS: Final[str] = "GOLDEN_V3_CERTIFIED"
_REPLAY_EXACT_STATUS: Final[str] = "REPLAY_DIFFERENTIAL_EXACT"
_EXACT: Final[str] = "EXACT"
_SOURCE_SIDECAR_SUFFIXES: Final[tuple[str, ...]] = ("-wal", "-shm", "-journal")
_GOLDEN_SCOPE: Final[str] = "TECHNICAL_STORAGE_AND_REPLAY_ORACLE"
_NON_BLOCKING_COVERAGE_CODES: Final[frozenset[str]] = frozenset(
    {
        "NO_FILL_COVERAGE",
        "NO_FUNDING_SETTLEMENT_COVERAGE",
        "NO_SUCCESSFUL_RECONCILE_COVERAGE",
        "NO_TIMER_COVERAGE",
        "PHASE05_PHASE08_DECISIONS_NOT_BOTH_OBSERVED",
        "PHASE05_PHASE08_NOT_BOTH_FROZEN",
        "REPLAY_NOT_PERFORMED",
    }
)
_BLOCKING_CENSUS_CODES: Final[frozenset[str]] = frozenset(
    {
        "NON_REPLAYABLE_GUARD_ALERT_PRESENT",
        "UNCOMMITTED_ALERTS_PRESENT",
    }
)
_CLASSIFICATION_KEYS: Final[tuple[str, ...]] = (
    "golden_scope",
    "phase05_decision_coverage",
    "phase08_decision_coverage",
    "market_gap_coverage",
    "strategy_behavior_complete",
    "economic_evidence",
    "authorizes_real_money",
    "BLOCKING_INTEGRITY_GATES",
    "COVERAGE_METADATA_NON_BLOCKING",
    "ECONOMIC_EVIDENCE",
)

ProgressCallback = Callable[[Mapping[str, object]], None]


class GoldenCertificationError(RuntimeError):
    """Raised when an end-to-end Golden V3 certification cannot be proven."""


class GoldenReplayDivergenceError(GoldenCertificationError):
    """Raised when the replay cannot reproduce the Golden V3 oracle exactly."""


@dataclass(frozen=True)
class GoldenCertificationResult:
    """Paths and identity of a successfully completed certification."""

    status: str
    candidate_root: Path
    certification_root_hash: str
    manifest_path: Path
    pin_path: Path
    complete_path: Path
    tested: Mapping[str, object]
    classification: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        payload = {
            "status": self.status,
            "candidate_root": str(self.candidate_root),
            "certification_root_hash": self.certification_root_hash,
            "manifest_path": str(self.manifest_path),
            "pin_path": str(self.pin_path),
            "complete_path": str(self.complete_path),
            "tested": _jsonable(self.tested),
        }
        payload.update(
            {
                key: _jsonable(self.classification[key])
                for key in _CLASSIFICATION_KEYS
            }
        )
        return payload


@dataclass(frozen=True)
class GoldenCertificationVerification:
    """Successful independent verification of a completed candidate."""

    candidate_root: Path
    certification_root_hash: str
    manifest: Mapping[str, object]
    tested: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_root": str(self.candidate_root),
            "certification_root_hash": self.certification_root_hash,
            "manifest": _jsonable(self.manifest),
            "tested": _jsonable(self.tested),
        }


@dataclass(frozen=True)
class _ResumeExportA:
    expected_root_hash: str
    expected_file_count: int
    expected_bytes: int


def _jsonable(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_jsonable(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _jsonable(to_dict())
    raise GoldenCertificationError(
        f"certification result contains a non-JSON value: {type(value).__name__}"
    )


def _canonical_bytes(value: object) -> bytes:
    try:
        text = json.dumps(
            _jsonable(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise GoldenCertificationError("cannot encode canonical certification JSON") from exc
    return (text + "\n").encode("utf-8")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_READ_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _write_new_canonical_json(path: Path, value: object) -> None:
    """Create one canonical JSON artifact without replacing an existing file."""

    data = _canonical_bytes(value)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        # The candidate root was required to be absent.  This second check also
        # protects against an accidental duplicate artifact name in this run.
        if path.exists():
            raise GoldenCertificationError(f"refusing to overwrite artifact: {path}")
        temporary.replace(path)
        _fsync_directory(path.parent)
    finally:
        with suppress(OSError):
            temporary.unlink()


def _read_canonical_mapping(path: Path, *, label: str) -> dict[str, object]:
    _reject_reparse_path(path, label=label)
    if not path.is_file() or path.is_symlink():
        raise GoldenCertificationError(f"missing or unsafe {label} artifact: {path}")

    def pairs_hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise GoldenCertificationError(f"duplicate JSON key in {label}: {key}")
            result[key] = value
        return result

    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs_hook)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GoldenCertificationError(f"invalid {label} artifact: {path}") from exc
    if not isinstance(value, dict):
        raise GoldenCertificationError(f"{label} artifact is not a JSON object: {path}")
    if raw != _canonical_bytes(value):
        raise GoldenCertificationError(f"non-canonical {label} artifact: {path}")
    return value


def _nearest_existing_directory(path: Path) -> Path:
    candidate = path
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            raise GoldenCertificationError(
                f"cannot find an existing directory for disk preflight: {path}"
            )
        candidate = parent
    if not candidate.is_dir():
        candidate = candidate.parent
    return candidate


def _safe_resolve(path: Path, *, must_exist: bool, label: str) -> Path:
    try:
        return path.expanduser().resolve(strict=must_exist)
    except OSError as exc:
        raise GoldenCertificationError(f"cannot resolve {label}: {path}") from exc


def _lexical_absolute(path: Path) -> Path:
    """Return an absolute lexical path without following reparse points."""

    return Path(os.path.abspath(os.fspath(path.expanduser())))


def _has_reparse_component(path: Path) -> bool:
    absolute = _lexical_absolute(path)
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        try:
            value = current.lstat()
        except FileNotFoundError:
            break
        except OSError as exc:
            raise GoldenCertificationError(
                f"cannot inspect path component for reparse safety: {current}"
            ) from exc
        attributes = int(getattr(value, "st_file_attributes", 0))
        if stat.S_ISLNK(value.st_mode):
            return True
        if (
            attributes
            and hasattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT")
            and attributes & int(stat.FILE_ATTRIBUTE_REPARSE_POINT)
        ):
            return True
    return False


def _reject_reparse_path(path: Path, *, label: str) -> None:
    if _has_reparse_component(path):
        raise GoldenCertificationError(
            f"{label} contains a symlink, junction, or reparse component: {path}"
        )


def _same_or_contains(left: Path, right: Path) -> bool:
    """Return whether either lexical path equals or contains the other."""

    try:
        right.relative_to(left)
        return True
    except ValueError:
        pass
    try:
        left.relative_to(right)
        return True
    except ValueError:
        return False


def _same_existing_file(left: Path, right: Path) -> bool:
    if not left.exists() or not right.exists():
        return False
    try:
        return os.path.samefile(left, right)
    except OSError as exc:
        raise GoldenCertificationError(
            f"cannot establish same-file identity for {left} and {right}"
        ) from exc


def _discover_partial_candidate(golden_root: Path | str) -> Path:
    lexical_root = _lexical_absolute(Path(golden_root))
    _reject_reparse_path(lexical_root, label="Golden V3 root")
    if (
        not lexical_root.is_dir()
        or lexical_root.is_symlink()
        or _has_reparse_component(lexical_root)
    ):
        raise GoldenCertificationError(
            f"Golden V3 root is not a regular directory: {lexical_root}"
        )
    root = lexical_root.resolve(strict=True)
    children = list(root.iterdir())
    if any(
        child.is_symlink()
        or _has_reparse_component(child)
        or not child.is_dir()
        for child in children
    ):
        raise GoldenCertificationError(
            "Golden V3 root contains an unsafe or non-candidate child"
        )
    if len(children) != 1:
        raise GoldenCertificationError(
            "Golden V3 resume requires exactly one unique partial candidate; "
            f"found {len(children)}"
        )
    return children[0].resolve(strict=True)


def _validated_operator_paths(
    source: Path,
    candidate_root: Path,
    sentinel: Path,
    *,
    candidate_must_exist: bool = False,
) -> tuple[Path, Path, Path]:
    """Validate operator paths lexically before any resolution or creation."""

    lexical_source = _lexical_absolute(source)
    lexical_candidate = _lexical_absolute(candidate_root)
    lexical_sentinel = _lexical_absolute(sentinel)
    for path, label in (
        (lexical_source, "source"),
        (lexical_candidate, "candidate root"),
        (lexical_sentinel, "forbidden sentinel"),
    ):
        _reject_reparse_path(path, label=label)

    if not lexical_source.is_file():
        raise GoldenCertificationError(
            f"source is not a regular SQLite file: {lexical_source}"
        )
    if candidate_must_exist:
        if not lexical_candidate.is_dir() or lexical_candidate.is_symlink():
            raise GoldenCertificationError(
                f"resume candidate is not a regular directory: {lexical_candidate}"
            )
    elif lexical_candidate.exists() or lexical_candidate.is_symlink():
        raise GoldenCertificationError(f"candidate root already exists: {lexical_candidate}")
    if lexical_sentinel.exists() and not lexical_sentinel.is_file():
        raise GoldenCertificationError(
            f"forbidden sentinel is not a regular file path: {lexical_sentinel}"
        )

    if _same_or_contains(lexical_source, lexical_candidate):
        raise GoldenCertificationError(
            "source and candidate paths collide or contain one another"
        )
    if _same_or_contains(lexical_source, lexical_sentinel):
        raise GoldenCertificationError(
            "source and forbidden sentinel paths collide or contain one another"
        )
    if _same_or_contains(lexical_candidate, lexical_sentinel):
        raise GoldenCertificationError(
            "candidate and forbidden sentinel paths collide or contain one another"
        )
    if _same_existing_file(lexical_source, lexical_sentinel):
        raise GoldenCertificationError(
            "source and forbidden sentinel alias the same physical file"
        )

    source_stat = lexical_source.stat()
    if int(source_stat.st_nlink) != 1:
        raise GoldenCertificationError("source hardlinks are forbidden")
    attributes = int(getattr(source_stat, "st_file_attributes", 0))
    readonly_attribute = bool(
        attributes
        and hasattr(stat, "FILE_ATTRIBUTE_READONLY")
        and attributes & int(stat.FILE_ATTRIBUTE_READONLY)
    )
    writable_mode = bool(
        source_stat.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    )
    if not readonly_attribute and writable_mode:
        raise GoldenCertificationError("source must be read-only before certification")

    for suffix in _SOURCE_SIDECAR_SUFFIXES:
        sidecar = Path(f"{lexical_source}{suffix}")
        _reject_reparse_path(sidecar, label="SQLite source sidecar")
        if sidecar.exists() or sidecar.is_symlink():
            raise GoldenCertificationError(
                f"source SQLite sidecars are forbidden for certification: {sidecar}"
            )

    resolved_source = _safe_resolve(
        lexical_source, must_exist=True, label="source"
    )
    resolved_sentinel = _safe_resolve(
        lexical_sentinel, must_exist=False, label="sentinel"
    )
    return resolved_source, lexical_candidate, resolved_sentinel


def _source_sidecars(source: Path) -> dict[str, object]:
    sidecars: dict[str, object] = {}
    for suffix in _SOURCE_SIDECAR_SUFFIXES:
        path = Path(f"{source}{suffix}")
        if not path.exists():
            sidecars[suffix] = {"exists": False}
            continue
        if not path.is_file() or path.is_symlink():
            raise GoldenCertificationError(f"unsafe SQLite source sidecar: {path}")
        before = path.stat()
        digest = _sha256_file(path)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise GoldenCertificationError(f"SQLite source sidecar changed while hashing: {path}")
        sidecars[suffix] = {
            "exists": True,
            "bytes": after.st_size,
            "mtime_ns": after.st_mtime_ns,
            "sha256": digest,
        }
    return sidecars


def _source_fingerprint(source: Path) -> dict[str, object]:
    if not source.is_file() or source.is_symlink():
        raise GoldenCertificationError(f"source is not a regular SQLite file: {source}")
    before = source.stat()
    digest = _sha256_file(source)
    after = source.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise GoldenCertificationError("source changed while its SHA-256 was calculated")
    return {
        "path": str(source),
        "bytes": after.st_size,
        "ctime_ns": after.st_ctime_ns,
        "device": after.st_dev,
        "file_attributes": int(getattr(after, "st_file_attributes", 0)),
        "inode": after.st_ino,
        "link_count": after.st_nlink,
        "mode": after.st_mode,
        "mtime_ns": after.st_mtime_ns,
        "sha256": digest,
        "sidecars": _source_sidecars(source),
    }


def _require_expected_source(
    fingerprint: Mapping[str, object],
    *,
    expected_size: int,
    expected_sha256: str,
) -> None:
    if expected_size < 0:
        raise GoldenCertificationError("expected source size must be non-negative")
    normalized_hash = expected_sha256.strip().lower()
    if len(normalized_hash) != 64 or any(
        character not in "0123456789abcdef" for character in normalized_hash
    ):
        raise GoldenCertificationError("expected source SHA-256 is invalid")
    if fingerprint.get("bytes") != expected_size:
        raise GoldenCertificationError(
            "source size mismatch: "
            f"expected {expected_size}, observed {fingerprint.get('bytes')}"
        )
    if fingerprint.get("sha256") != normalized_hash:
        raise GoldenCertificationError(
            "source SHA-256 mismatch: "
            f"expected {normalized_hash}, observed {fingerprint.get('sha256')}"
        )


def _source_is_unchanged(
    before: Mapping[str, object], after: Mapping[str, object]
) -> bool:
    return _canonical_bytes(before) == _canonical_bytes(after)


def _as_mapping(value: object, *, label: str) -> dict[str, object]:
    converted = _jsonable(value)
    if not isinstance(converted, dict):
        raise GoldenCertificationError(f"{label} did not return an object")
    return converted


def _non_negative_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GoldenCertificationError(f"{label} must be a non-negative integer")
    return value


def _count_mapping(census: Mapping[str, object], key: str) -> dict[str, int]:
    value = census.get(key)
    if not isinstance(value, Mapping):
        raise GoldenCertificationError(f"census {key} must be an object")
    result: dict[str, int] = {}
    for raw_name, raw_count in value.items():
        if not isinstance(raw_name, str) or not raw_name:
            raise GoldenCertificationError(f"census {key} contains an invalid name")
        result[raw_name] = _non_negative_int(
            raw_count,
            label=f"census {key}.{raw_name}",
        )
    return dict(sorted(result.items()))


def _classify_census(census: Mapping[str, object]) -> dict[str, object]:
    """Partition structural blockers from non-blocking behavioral coverage.

    The export census is immutable input to this classifier. Unknown gap codes
    fail closed, while ordinary alert names are data and are never interpreted
    through substring matching.
    """

    blocking: list[dict[str, object]] = []

    def add_blocker(code: str, *, source: str, detail: object | None = None) -> None:
        blocker: dict[str, object] = {"code": code, "source": source}
        if detail is not None:
            blocker["detail"] = _jsonable(detail)
        blocking.append(blocker)

    if census.get("status") != "GOLDEN_CANDIDATE":
        add_blocker(
            "CENSUS_STATUS_INVALID",
            source="status",
            detail=census.get("status"),
        )
    if census.get("integrity_status") != "PASS":
        add_blocker(
            "SQLITE_INTEGRITY_NOT_PASS",
            source="integrity_status",
            detail=census.get("integrity_status"),
        )
    if census.get("sqlite_integrity_check") != "ok":
        add_blocker(
            "SQLITE_INTEGRITY_CHECK_NOT_OK",
            source="sqlite_integrity_check",
            detail=census.get("sqlite_integrity_check"),
        )
    foreign_keys = _non_negative_int(
        census.get("sqlite_foreign_key_violation_count"),
        label="census sqlite_foreign_key_violation_count",
    )
    if foreign_keys:
        add_blocker(
            "SQLITE_FOREIGN_KEY_VIOLATIONS",
            source="sqlite_foreign_key_violation_count",
            detail=foreign_keys,
        )
    uncommitted_alerts = _non_negative_int(
        census.get("uncommitted_alert_count"),
        label="census uncommitted_alert_count",
    )
    if uncommitted_alerts:
        add_blocker(
            "UNCOMMITTED_ALERTS_PRESENT",
            source="uncommitted_alert_count",
            detail=uncommitted_alerts,
        )

    raw_gaps = census.get("coverage_gaps")
    if not isinstance(raw_gaps, Sequence) or isinstance(
        raw_gaps, (str, bytes, bytearray)
    ):
        raise GoldenCertificationError("census coverage_gaps must be an array")
    non_blocking_codes: set[str] = set()
    placeholders: set[str] = set()
    for raw_gap in raw_gaps:
        if not isinstance(raw_gap, str) or not raw_gap.strip():
            raise GoldenCertificationError(
                "census coverage_gaps contains a non-string or empty code"
            )
        code = raw_gap.strip().upper()
        if code in _NON_BLOCKING_COVERAGE_CODES:
            if code == "REPLAY_NOT_PERFORMED":
                placeholders.add(code)
            else:
                non_blocking_codes.add(code)
        elif code in _BLOCKING_CENSUS_CODES:
            add_blocker(code, source="coverage_gaps")
        else:
            add_blocker(code, source="coverage_gaps", detail="UNKNOWN_FAIL_CLOSED")

    strategy_counts = _count_mapping(census, "strategy_decision_counts")
    alert_counts = _count_mapping(census, "alert_code_counts")
    input_counts = _count_mapping(census, "input_type_counts")
    event_counts = _count_mapping(census, "event_type_counts")
    raw_strategy_ids = census.get("strategy_ids")
    if not isinstance(raw_strategy_ids, Sequence) or isinstance(
        raw_strategy_ids, (str, bytes, bytearray)
    ):
        raise GoldenCertificationError("census strategy_ids must be an array")
    strategy_ids: list[str] = []
    for raw_strategy_id in raw_strategy_ids:
        if not isinstance(raw_strategy_id, str) or not raw_strategy_id:
            raise GoldenCertificationError("census strategy_ids contains an invalid id")
        strategy_ids.append(raw_strategy_id)
    if len(strategy_ids) != len(set(strategy_ids)):
        raise GoldenCertificationError("census strategy_ids contains duplicates")

    phase05_coverage = strategy_counts.get("phase05_cash_and_carry", 0) > 0
    phase08_coverage = strategy_counts.get("phase08_robust_pairs", 0) > 0
    market_gap_count = alert_counts.get("MARKET_GAP", 0)
    required_strategies = {"phase05_cash_and_carry", "phase08_robust_pairs"}
    strategy_behavior_complete = (
        required_strategies.issubset(strategy_ids)
        and phase05_coverage
        and phase08_coverage
        and not non_blocking_codes
    )

    unique_blockers: dict[bytes, dict[str, object]] = {}
    for blocker in blocking:
        unique_blockers.setdefault(_canonical_bytes(blocker), blocker)
    ordered_blockers = [unique_blockers[key] for key in sorted(unique_blockers)]
    coverage_metadata: dict[str, object] = {
        "alert_code_counts": alert_counts,
        "coverage_limit_codes": sorted(non_blocking_codes),
        "event_type_counts": event_counts,
        "export_census_placeholders": sorted(placeholders),
        "input_type_counts": input_counts,
        "market_gap_count": market_gap_count,
        "market_gap_coverage": market_gap_count > 0,
        "observed_input_types": sorted(
            name for name, count in input_counts.items() if count > 0
        ),
        "phase05_decision_coverage": phase05_coverage,
        "phase08_decision_coverage": phase08_coverage,
        "strategy_behavior_complete": strategy_behavior_complete,
        "strategy_decision_counts": strategy_counts,
        "strategy_ids": strategy_ids,
    }
    return {
        "golden_scope": _GOLDEN_SCOPE,
        "phase05_decision_coverage": phase05_coverage,
        "phase08_decision_coverage": phase08_coverage,
        "market_gap_coverage": market_gap_count > 0,
        "strategy_behavior_complete": strategy_behavior_complete,
        "economic_evidence": False,
        "authorizes_real_money": False,
        "BLOCKING_INTEGRITY_GATES": ordered_blockers,
        "COVERAGE_METADATA_NON_BLOCKING": coverage_metadata,
        "ECONOMIC_EVIDENCE": {
            "authorizes_real_money": False,
            "economic_evidence": False,
            "status": "NOT_ECONOMIC_PROOF",
        },
    }


def _blocking_census_gaps(census: Mapping[str, object]) -> list[dict[str, object]]:
    classification = _classify_census(census)
    blockers = classification["BLOCKING_INTEGRITY_GATES"]
    if not isinstance(blockers, list):
        raise GoldenCertificationError("census blocker classification is invalid")
    return blockers


def _require_exact_certification_gates(gates: object) -> None:
    required = (
        "source_expected_identity",
        "source_immutability",
        "dual_extraction",
        "dual_extraction_bytes",
        "replay_differential",
    )
    if not isinstance(gates, Mapping) or any(gates.get(name) != _EXACT for name in required):
        raise GoldenCertificationError("certification exactness gates are not satisfied")


def _emit(progress: ProgressCallback | None, **payload: object) -> None:
    if progress is not None:
        progress(dict(payload))


def _relative_artifact(candidate_root: Path, path: Path) -> str:
    resolved_root = candidate_root.resolve(strict=True)
    resolved_path = path.resolve(strict=True)
    try:
        relative = resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise GoldenCertificationError(
            f"certification artifact escapes candidate root: {path}"
        ) from exc
    return relative.as_posix()


def _artifact_record(candidate_root: Path, path: Path) -> dict[str, object]:
    _reject_reparse_path(path, label="certification artifact")
    if not path.is_file() or path.is_symlink():
        raise GoldenCertificationError(f"missing or unsafe certification artifact: {path}")
    return {
        "path": _relative_artifact(candidate_root, path),
        "bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
    }


def _artifact_from_record(candidate_root: Path, record: object) -> Path:
    if not isinstance(record, Mapping):
        raise GoldenCertificationError("invalid certification artifact record")
    raw_path = record.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        raise GoldenCertificationError("certification artifact record has no path")
    relative = Path(raw_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise GoldenCertificationError(f"unsafe certification artifact path: {raw_path}")
    path = candidate_root / relative
    _reject_reparse_path(path, label="certification artifact")
    if not path.is_file() or path.is_symlink():
        raise GoldenCertificationError(f"missing certification artifact: {raw_path}")
    expected_bytes = record.get("bytes")
    expected_hash = record.get("sha256")
    if not isinstance(expected_bytes, int) or not isinstance(expected_hash, str):
        raise GoldenCertificationError(f"invalid certification artifact binding: {raw_path}")
    observed_bytes = path.stat().st_size
    observed_hash = _sha256_file(path)
    if observed_bytes != expected_bytes or observed_hash != expected_hash:
        raise GoldenCertificationError(
            f"certification result/artifact hash mismatch: {raw_path}"
        )
    return path


def _replay_sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise GoldenCertificationError(f"{label} must be a lowercase SHA-256")
    return value


def _replay_seconds(value: object, *, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise GoldenCertificationError(f"{label} must be finite and non-negative")
    return float(value)


def _validate_replay_result(
    replay: Mapping[str, object],
    *,
    candidate_root: Path,
    export_root: Path,
    verification: GoldenVerification,
    run_identity: Mapping[str, object],
    expected_run_id: str,
    expected_config_hash: str,
    expected_source_root_hash: str,
    census: Mapping[str, object],
    expected_streams: Mapping[str, object],
    progress: ProgressCallback | None = None,
    progress_phase: str = "preserved_target_validation",
) -> Path:
    """Validate the complete replay contract and its preserved target artifact."""

    required_keys = {
        "commit_count",
        "config_hash",
        "differential",
        "event_count",
        "mode",
        "orders_enabled",
        "projection_hash",
        "rows_compared",
        "run_id",
        "source_root_hash",
        "status",
        "target_bytes",
        "target_head_identity",
        "target_path",
        "target_sha256",
        "timings",
    }
    if set(replay) != required_keys:
        raise GoldenCertificationError("replay result schema is incomplete or unexpected")
    if replay.get("status") != _REPLAY_EXACT_STATUS:
        raise GoldenReplayDivergenceError(
            f"replay differential is not exact: {replay.get('status')}"
        )
    if replay.get("mode") != "PAPER_ONLY" or replay.get("orders_enabled") is not False:
        raise GoldenCertificationError("replay result is not strictly PAPER_ONLY")
    if replay.get("run_id") != expected_run_id:
        raise GoldenCertificationError("replay run id differs from the Golden run")
    if replay.get("config_hash") != expected_config_hash:
        raise GoldenCertificationError("replay config hash differs from the Golden run")
    if replay.get("source_root_hash") != expected_source_root_hash:
        raise GoldenCertificationError("replay source root differs from export A")

    expected_commit_count = _non_negative_int(
        census.get("commit_count"), label="census commit_count"
    )
    expected_event_count = _non_negative_int(
        census.get("event_count"), label="census event_count"
    )
    if expected_commit_count <= 0 or expected_event_count <= 0:
        raise GoldenCertificationError("replay census counts must be positive")
    if replay.get("commit_count") != expected_commit_count:
        raise GoldenCertificationError("replay commit count differs from census")
    if replay.get("event_count") != expected_event_count:
        raise GoldenCertificationError("replay event count differs from census")

    if set(expected_streams) != set(GOLDEN_STREAM_NAMES):
        raise GoldenCertificationError("Golden export stream schema is incomplete")
    expected_row_counts: dict[str, int] = {}
    for stream_name in GOLDEN_STREAM_NAMES:
        stream_manifest = expected_streams.get(stream_name)
        if not isinstance(stream_manifest, Mapping):
            raise GoldenCertificationError(
                f"Golden export stream {stream_name} has no manifest"
            )
        expected_row_counts[stream_name] = _non_negative_int(
            stream_manifest.get("row_count"),
            label=f"Golden export stream {stream_name}.row_count",
        )

    differential = replay.get("differential")
    if not isinstance(differential, Mapping) or set(differential) != {
        "rows_compared",
        "streams",
    }:
        raise GoldenCertificationError("replay differential schema is invalid")
    replay_streams = differential.get("streams")
    if not isinstance(replay_streams, Mapping) or set(replay_streams) != set(
        GOLDEN_STREAM_NAMES
    ):
        raise GoldenCertificationError("replay differential does not bind all streams")
    for stream_name, expected_rows in expected_row_counts.items():
        stream_result = replay_streams.get(stream_name)
        if not isinstance(stream_result, Mapping) or set(stream_result) != {
            "rows_compared",
            "seconds",
        }:
            raise GoldenCertificationError(
                f"replay differential stream {stream_name} schema is invalid"
            )
        if stream_result.get("rows_compared") != expected_rows:
            raise GoldenCertificationError(
                f"replay differential stream {stream_name} count differs from export"
            )
        _replay_seconds(
            stream_result.get("seconds"),
            label=f"replay differential stream {stream_name}.seconds",
        )
    expected_total_rows = sum(expected_row_counts.values())
    if (
        differential.get("rows_compared") != expected_total_rows
        or replay.get("rows_compared") != expected_total_rows
    ):
        raise GoldenCertificationError("replay total row count differs from exports")

    projection_hash = _replay_sha256(
        replay.get("projection_hash"), label="replay projection_hash"
    )
    target_head = replay.get("target_head_identity")
    if not isinstance(target_head, list) or len(target_head) != 9:
        raise GoldenCertificationError("replay target head identity is invalid")
    if (
        target_head[0] != expected_run_id
        or target_head[1] != expected_config_hash
        or target_head[2] != census.get("terminal_projection_state")
        or target_head[3] != expected_event_count
        or target_head[5] != expected_commit_count
        or target_head[7] != expected_commit_count
        or target_head[8] != projection_hash
    ):
        raise GoldenCertificationError("replay target head differs from census or run")
    _replay_sha256(target_head[4], label="replay target event head")
    _replay_sha256(target_head[6], label="replay target commit head")
    expected_head_identity = [
        expected_run_id,
        expected_config_hash,
        run_identity.get("status"),
        run_identity.get("event_count"),
        run_identity.get("event_head_hash"),
        run_identity.get("commit_count"),
        run_identity.get("commit_head_hash"),
        run_identity.get("projection_revision"),
        run_identity.get("projection_hash"),
    ]
    if target_head != expected_head_identity:
        raise GoldenCertificationError("replay target head differs from export run identity")

    timings = replay.get("timings")
    timing_keys = {
        "differential_seconds",
        "preserve_fingerprint_seconds",
        "replay_seconds",
        "target_integrity_seconds",
        "total_seconds",
        "verify_export_seconds",
    }
    if not isinstance(timings, Mapping) or set(timings) != timing_keys:
        raise GoldenCertificationError("replay timing schema is invalid")
    observed_timings = {
        name: _replay_seconds(timings.get(name), label=f"replay timings.{name}")
        for name in timing_keys
    }
    if observed_timings["total_seconds"] < max(observed_timings.values()):
        raise GoldenCertificationError("replay total timing is incoherent")

    raw_target_path = replay.get("target_path")
    if not isinstance(raw_target_path, str) or not raw_target_path:
        raise GoldenCertificationError("replay result has no target_path")
    target_lexical = Path(raw_target_path)
    if not target_lexical.is_absolute():
        target_lexical = candidate_root / target_lexical
    _reject_reparse_path(target_lexical, label="replay target")
    target = _safe_resolve(target_lexical, must_exist=True, label="replay target")
    if not target.is_file() or target.is_symlink():
        raise GoldenCertificationError("replay target is not a regular file")
    candidate_resolved = candidate_root.resolve(strict=True)
    try:
        target_relative = target.relative_to(candidate_resolved)
    except ValueError as exc:
        raise GoldenCertificationError("replay target escapes candidate root") from exc
    if (
        len(target_relative.parts) != 3
        or target_relative.parts[0] != "scratch"
        or not target_relative.parts[1].startswith("golden-v3-replay-preserved-")
    ):
        raise GoldenCertificationError("replay target is not the preserved scratch artifact")
    scratch = candidate_resolved / "scratch"
    scratch_entries = list(scratch.iterdir())
    target_parent_entries = list(target.parent.iterdir())
    if (
        scratch_entries != [target.parent]
        or target_parent_entries != [target]
        or target.stat().st_nlink != 1
    ):
        raise GoldenCertificationError("replay scratch layout is not singular and closed")

    target_bytes = _non_negative_int(
        replay.get("target_bytes"), label="replay target_bytes"
    )
    if target_bytes <= 0 or target.stat().st_size != target_bytes:
        raise GoldenCertificationError("replay target size binding is invalid")
    target_sha256 = _replay_sha256(
        replay.get("target_sha256"), label="replay target_sha256"
    )
    validation_started = perf_counter()
    validation_fields = {
        "target_path": str(target),
        "target_store_bytes": target_bytes,
        "total_expected": target_bytes,
    }
    _emit(
        progress,
        phase=progress_phase,
        validation_step="start",
        bytes_completed=0,
        elapsed_seconds=0.0,
        **validation_fields,
    )
    _emit(
        progress,
        phase=progress_phase,
        validation_step="fingerprint",
        bytes_completed=0,
        elapsed_seconds=perf_counter() - validation_started,
        **validation_fields,
    )
    if _sha256_file(target) != target_sha256:
        raise GoldenCertificationError("replay target SHA-256 binding is invalid")

    sidecars = tuple(Path(f"{target}{suffix}") for suffix in _SOURCE_SIDECAR_SUFFIXES)
    if any(sidecar.exists() for sidecar in sidecars):
        raise GoldenCertificationError("replay target has unexpected SQLite sidecars")

    _emit(
        progress,
        phase=progress_phase,
        validation_step="integrity",
        bytes_completed=0,
        elapsed_seconds=perf_counter() - validation_started,
        **validation_fields,
    )
    target_store = PaperStore(target, initialize=False)
    try:
        integrity = target_store.inspect_integrity_readonly(expected_run_id)
        if not integrity.ok:
            raise GoldenCertificationError("preserved replay target failed integrity")
        observed_head = list(target_store.get_run(expected_run_id).head_identity)
    except GoldenCertificationError:
        raise
    except Exception as exc:
        raise GoldenCertificationError(
            "preserved replay target cannot be reopened and validated"
        ) from exc
    finally:
        target_store.close()
    if observed_head != expected_head_identity or observed_head != target_head:
        raise GoldenCertificationError("preserved replay target head is not exact")

    _emit(
        progress,
        phase=progress_phase,
        validation_step="differential",
        elapsed_seconds=perf_counter() - validation_started,
        rows_completed=0,
        target_path=str(target),
        target_store_bytes=target_bytes,
        total_expected=expected_total_rows,
    )
    uri = f"{target.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, isolation_level=None)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise GoldenCertificationError("replay target connection is not query-only")
        connection.execute("BEGIN")
        try:
            independently_compared = _compare_streams_connection(
                export_root,
                connection,
                expected_run_id,
                verification=verification,
                progress=progress,
                target_path=target,
                progress_phase=progress_phase,
                progress_complete_phase=progress_phase,
                validation_step="differential",
            )
        except GoldenDifferentialError as exc:
            raise GoldenCertificationError(
                "preserved replay target differs from verified export"
            ) from exc
        independent_streams = independently_compared.get("streams")
        if (
            independently_compared.get("rows_compared") != sum(expected_row_counts.values())
            or not isinstance(independent_streams, Mapping)
        ):
            raise GoldenCertificationError(
                "preserved replay target census differs from export"
            )
        for stream_name, expected_rows in expected_row_counts.items():
            stream_result = independent_streams.get(stream_name)
            if (
                not isinstance(stream_result, Mapping)
                or stream_result.get("rows_compared") != expected_rows
            ):
                raise GoldenCertificationError(
                    f"preserved replay stream {stream_name} census differs from export"
                )
        connection.rollback()
    finally:
        connection.close()

    _emit(
        progress,
        phase=progress_phase,
        validation_step="final_fingerprint",
        bytes_completed=0,
        elapsed_seconds=perf_counter() - validation_started,
        **validation_fields,
    )
    if (
        target.stat().st_size != target_bytes
        or _sha256_file(target) != target_sha256
        or any(sidecar.exists() for sidecar in sidecars)
    ):
        raise GoldenCertificationError("replay target changed during independent validation")
    _emit(
        progress,
        phase=f"{progress_phase}_complete",
        validation_step="complete",
        bytes_completed=target_bytes,
        elapsed_seconds=perf_counter() - validation_started,
        **validation_fields,
    )
    return target


def _validate_resume_candidate_layout(candidate: Path) -> None:
    expected_directories = {"corpus", "manifests", "pin", "results", "scratch"}
    root_entries = list(candidate.iterdir())
    if (
        {entry.name for entry in root_entries} != expected_directories
        or any(
            not entry.is_dir()
            or entry.is_symlink()
            or _has_reparse_component(entry)
            for entry in root_entries
        )
    ):
        raise GoldenCertificationError(
            "resume candidate root does not have the exact partial layout"
        )

    corpus = candidate / "corpus"
    corpus_entries = list(corpus.iterdir())
    if (
        len(corpus_entries) != 1
        or corpus_entries[0].name != "extract-a"
        or not corpus_entries[0].is_dir()
        or corpus_entries[0].is_symlink()
        or _has_reparse_component(corpus_entries[0])
    ):
        raise GoldenCertificationError(
            "resume candidate must contain only the complete extract-a corpus"
        )
    if any(
        any((candidate / name).iterdir())
        for name in ("manifests", "scratch")
    ):
        raise GoldenCertificationError(
            "resume candidate contains final, partial, or scratch artifacts"
        )

    pin_entries = list((candidate / "pin").iterdir())
    if (
        len(pin_entries) != 1
        or pin_entries[0].name != "extract-a.pin.json"
        or not pin_entries[0].is_file()
        or pin_entries[0].is_symlink()
        or _has_reparse_component(pin_entries[0])
    ):
        raise GoldenCertificationError(
            "resume candidate must contain only the authenticated extract-a pin"
        )

    result_entries = list((candidate / "results").iterdir())
    names = {entry.name for entry in result_entries}
    if "extract-a.json" not in names:
        raise GoldenCertificationError("resume candidate has no extract-a result")
    for entry in result_entries:
        if (
            not entry.is_file()
            or entry.is_symlink()
            or _has_reparse_component(entry)
            or (
                entry.name != "extract-a.json"
                and entry.suffix.lower() != ".jsonl"
            )
        ):
            raise GoldenCertificationError(
                f"resume candidate contains an unexpected result artifact: {entry}"
            )


def _measure_complete_export(export_root: Path) -> tuple[int, int]:
    files = [path for path in export_root.rglob("*") if path.is_file()]
    return len(files), sum(path.stat().st_size for path in files)


def _require_reused_source_binding(
    verification: GoldenVerification,
    source_path: Path,
    source_fingerprint: Mapping[str, object],
) -> None:
    manifest = verification.manifest
    raw_source = manifest.get("source")
    if not isinstance(raw_source, Mapping) or manifest.get("source_unchanged") is not True:
        raise GoldenCertificationError("export A has no immutable source binding")
    raw_stat = raw_source.get("stat")
    if not isinstance(raw_stat, Mapping):
        raise GoldenCertificationError("export A source stat binding is invalid")
    sidecars = source_fingerprint.get("sidecars")
    if (
        not isinstance(sidecars, Mapping)
        or any(
            not isinstance(value, Mapping) or value.get("exists") is not False
            for value in sidecars.values()
        )
    ):
        raise GoldenCertificationError("current source sidecar binding is invalid")

    attributes = _non_negative_int(
        source_fingerprint.get("file_attributes"),
        label="current source file_attributes",
    )
    mode = _non_negative_int(
        source_fingerprint.get("mode"),
        label="current source mode",
    )
    readonly_attribute = bool(
        attributes
        and hasattr(stat, "FILE_ATTRIBUTE_READONLY")
        and attributes & int(stat.FILE_ATTRIBUTE_READONLY)
    )
    expected_stat = {
        "ctime_ns": source_fingerprint.get("ctime_ns"),
        "device": source_fingerprint.get("device"),
        "file_attributes": attributes,
        "inode": source_fingerprint.get("inode"),
        "link_count": source_fingerprint.get("link_count"),
        "mode": mode,
        "mtime_ns": source_fingerprint.get("mtime_ns"),
        "readonly": readonly_attribute
        or not bool(mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)),
        "size": source_fingerprint.get("bytes"),
    }
    if (
        raw_source.get("realpath") != str(source_path)
        or raw_source.get("sha256") != source_fingerprint.get("sha256")
        or raw_source.get("sidecars") != []
        or dict(raw_stat) != expected_stat
    ):
        raise GoldenCertificationError(
            "export A source identity differs from the current read-only source"
        )


def _reused_export_a(
    candidate: Path,
    verification: GoldenVerification,
) -> GoldenExportResult:
    census = verification.manifest.get("census")
    source = verification.manifest.get("source")
    if not isinstance(census, Mapping) or not isinstance(source, Mapping):
        raise GoldenCertificationError("export A manifest lacks census or source")
    census_status = census.get("status")
    source_sha256 = source.get("sha256")
    if not isinstance(census_status, str) or not isinstance(source_sha256, str):
        raise GoldenCertificationError("export A manifest identity is invalid")
    export = GoldenExportResult(
        output_root=verification.export_root,
        manifest_path=verification.export_root / "manifest.json",
        complete_path=verification.export_root / "COMPLETE",
        root_hash=verification.root_hash,
        census_status=census_status,
        source_sha256=source_sha256,
    )
    recorded = _read_canonical_mapping(
        candidate / "results" / "extract-a.json",
        label="extract-a result",
    )
    expected = {
        "export": export.to_dict(),
        "verification": verification.to_dict(),
    }
    if recorded != expected:
        raise GoldenCertificationError(
            "extract-a result differs from exhaustive export A verification"
        )
    return export


def _compare_export_bytes(
    expected_root: Path,
    actual_root: Path,
    *,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    expected_files = {
        path.relative_to(expected_root).as_posix(): path
        for path in expected_root.rglob("*")
        if path.is_file()
    }
    actual_files = {
        path.relative_to(actual_root).as_posix(): path
        for path in actual_root.rglob("*")
        if path.is_file()
    }
    if expected_files.keys() != actual_files.keys():
        raise GoldenCertificationError("dual extraction file inventories differ")
    total_expected = sum(path.stat().st_size for path in expected_files.values())
    bytes_completed = 0
    files_completed = 0
    next_progress = 256 * 1024**2
    _emit(
        progress,
        phase="dual_extraction_bytes",
        bytes_completed=0,
        files_completed=0,
        files_total=len(expected_files),
        total_expected=total_expected,
    )
    for relative in sorted(expected_files):
        expected_path = expected_files[relative]
        actual_path = actual_files[relative]
        expected_size = expected_path.stat().st_size
        if expected_size != actual_path.stat().st_size:
            raise GoldenCertificationError(
                f"dual extraction byte size differs: {relative}"
            )
        with expected_path.open("rb") as expected_handle, actual_path.open(
            "rb"
        ) as actual_handle:
            while True:
                expected_chunk = expected_handle.read(_READ_CHUNK_BYTES)
                actual_chunk = actual_handle.read(_READ_CHUNK_BYTES)
                if expected_chunk != actual_chunk:
                    raise GoldenCertificationError(
                        f"dual extraction bytes differ: {relative}"
                    )
                if not expected_chunk:
                    break
                bytes_completed += len(expected_chunk)
                if bytes_completed >= next_progress:
                    _emit(
                        progress,
                        phase="dual_extraction_bytes",
                        bytes_completed=bytes_completed,
                        files_completed=files_completed,
                        files_total=len(expected_files),
                        total_expected=total_expected,
                    )
                    next_progress += 256 * 1024**2
        files_completed += 1
    result: dict[str, object] = {
        "bytes_compared": bytes_completed,
        "files_compared": files_completed,
        "status": "BYTE_IDENTICAL",
    }
    _emit(
        progress,
        phase="dual_extraction_bytes_complete",
        bytes_completed=bytes_completed,
        files_completed=files_completed,
        files_total=len(expected_files),
        total_expected=total_expected,
    )
    return result


def _make_read_only(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode & ~stat.S_IWRITE & ~stat.S_IWGRP & ~stat.S_IWOTH)
    if path.stat().st_mode & stat.S_IWRITE:
        raise GoldenCertificationError(f"could not make certification pin read-only: {path}")
    _fsync_directory(path.parent)


def _quarantine_final_markers(
    manifest_path: Path,
    pin_path: Path,
    complete_path: Path,
) -> None:
    # Preserve every partial byte while ensuring no terminal certified filename
    # remains consumable after a failed invocation.
    terminal_paths = (complete_path, pin_path, manifest_path)
    errors: list[BaseException] = []
    for path in terminal_paths:
        if not path.exists():
            continue
        try:
            for _attempt in range(16):
                partial = path.with_name(f"{path.name}.partial.{uuid4().hex}")
                if not partial.exists():
                    break
            else:
                raise GoldenCertificationError(
                    f"cannot allocate unique quarantine path for {path}"
                )
            os.replace(path, partial)
            _fsync_directory(path.parent)
        except BaseException as quarantine_error:
            errors.append(quarantine_error)

    remaining = [path for path in terminal_paths if path.exists()]
    if remaining:
        errors.append(
            GoldenCertificationError(
                "terminal certification markers remain after quarantine: "
                + ", ".join(str(path) for path in remaining)
            )
        )
    if errors:
        primary = errors[0]
        for additional_error in errors[1:]:
            primary.add_note(
                "additional quarantine failure: "
                f"{type(additional_error).__name__}: {additional_error}"
            )
        raise primary


def certify_golden_v3(
    source: Path | str,
    candidate_root: Path | str,
    run_id: str,
    *,
    sentinel_path: Path | str,
    expected_source_size: int,
    expected_source_sha256: str,
    progress: ProgressCallback | None = None,
    shard_rows: int = 100_000,
    shard_bytes: int = 64 * 1024**2,
    _resume_export_a: _ResumeExportA | None = None,
) -> GoldenCertificationResult:
    """Build and certify two exact exports plus one exhaustive fresh replay."""

    source_path, requested_root, sentinel = _validated_operator_paths(
        Path(source),
        Path(candidate_root),
        Path(sentinel_path),
        candidate_must_exist=_resume_export_a is not None,
    )
    if not run_id or not run_id.strip():
        raise GoldenCertificationError("run_id must be non-empty")
    if shard_rows <= 0 or shard_bytes <= 0:
        raise GoldenCertificationError("shard limits must be positive")

    disk_anchor = _nearest_existing_directory(requested_root.parent)
    free_bytes = shutil.disk_usage(disk_anchor).free
    if free_bytes < _MINIMUM_FREE_BYTES:
        raise GoldenCertificationError(
            "insufficient free disk for Golden V3 certification: "
            f"requires at least {_MINIMUM_FREE_BYTES} bytes, observed {free_bytes}"
        )

    if _resume_export_a is None:
        _mkdir_durable(requested_root, exist_ok=False)
        _reject_reparse_path(requested_root, label="created candidate root")
        candidate = requested_root.resolve(strict=True)
    else:
        candidate = requested_root.resolve(strict=True)
        _validate_resume_candidate_layout(candidate)
    corpus_dir = candidate / "corpus"
    manifests_dir = candidate / "manifests"
    results_dir = candidate / "results"
    scratch_dir = candidate / "scratch"
    pins_dir = candidate / "pin"
    if _resume_export_a is None:
        for directory in (corpus_dir, manifests_dir, results_dir, scratch_dir, pins_dir):
            directory.mkdir()
        _fsync_directory(candidate)

    manifest_path = manifests_dir / "certification-manifest.json"
    final_pin_path = pins_dir / "certification.pin.json"
    complete_path = candidate / "COMPLETE"
    completed = False

    try:
        _emit(
            progress,
            phase="certification_start",
            candidate_root=str(candidate),
            free_bytes=free_bytes,
        )
        source_before = _source_fingerprint(source_path)
        _require_expected_source(
            source_before,
            expected_size=expected_source_size,
            expected_sha256=expected_source_sha256,
        )

        export_a_root = corpus_dir / "extract-a"
        export_b_root = corpus_dir / "extract-b"
        export_a_result_path = results_dir / "extract-a.json"
        export_a_pin = pins_dir / "extract-a.pin.json"
        if _resume_export_a is None:
            export_a = export_golden_v3(
                source_path,
                export_a_root,
                run_id,
                sentinel_path=sentinel,
                require_readonly=True,
                shard_rows=shard_rows,
                shard_bytes=shard_bytes,
                progress=progress,
                expected_source_size=expected_source_size,
                expected_source_sha256=expected_source_sha256,
            )
            verification_a = verify_golden_v3(export_a_root)
            export_a_pin = write_external_pin(
                export_a_root,
                export_a_pin,
                verification=verification_a,
            )
            _write_new_canonical_json(
                export_a_result_path,
                {
                    "export": export_a.to_dict(),
                    "verification": verification_a.to_dict(),
                },
            )
        else:
            verification_a = verify_golden_v3(
                export_a_root,
                pin_path=export_a_pin,
            )
            if (
                verification_a.root_hash != _resume_export_a.expected_root_hash
                or verification_a.manifest.get("run_id") != run_id
            ):
                raise GoldenCertificationError(
                    "reused export A root or run identity differs from expectation"
                )
            observed_files, observed_bytes = _measure_complete_export(export_a_root)
            if (
                observed_files != _resume_export_a.expected_file_count
                or observed_bytes != _resume_export_a.expected_bytes
            ):
                raise GoldenCertificationError(
                    "reused export A file count or byte size differs from expectation"
                )
            _require_reused_source_binding(
                verification_a,
                source_path,
                source_before,
            )
            export_a = _reused_export_a(candidate, verification_a)
            _emit(
                progress,
                phase="extraction_a_reused_verified",
                bytes_completed=observed_bytes,
                files_completed=observed_files,
                files_total=observed_files,
                root_hash=export_a.root_hash,
                total_expected=observed_bytes,
            )
        census_a = verification_a.manifest.get("census")
        if not isinstance(census_a, Mapping):
            raise GoldenCertificationError("export A verified manifest has no census")
        classification = _classify_census(census_a)
        gaps_a = classification["BLOCKING_INTEGRITY_GATES"]
        if gaps_a:
            raise GoldenCertificationError(
                f"export A census has blocking integrity gates: {gaps_a}"
            )
        _emit(
            progress,
            phase=(
                "extraction_a_verified"
                if _resume_export_a is None
                else "extraction_a_reuse_attested"
            ),
            root_hash=export_a.root_hash,
        )

        raw_streams_a = verification_a.manifest.get("streams")
        if not isinstance(raw_streams_a, Mapping):
            raise GoldenCertificationError("export A manifest has no stream census")

        def export_b_progress(record: Mapping[str, object]) -> None:
            enriched = dict(record)
            if enriched.get("phase") == "stream":
                stream_name = enriched.get("stream")
                stream_record = (
                    raw_streams_a.get(stream_name)
                    if isinstance(stream_name, str)
                    else None
                )
                if not isinstance(stream_record, Mapping):
                    raise GoldenCertificationError(
                        "export B progress references an unknown stream"
                    )
                enriched["total_expected"] = _non_negative_int(
                    stream_record.get("row_count"),
                    label=f"export A {stream_name} row_count",
                )
            if progress is not None:
                progress(enriched)

        export_b = export_golden_v3(
            source_path,
            export_b_root,
            run_id,
            sentinel_path=sentinel,
            require_readonly=True,
            shard_rows=shard_rows,
            shard_bytes=shard_bytes,
            progress=export_b_progress if progress is not None else None,
            expected_source_size=expected_source_size,
            expected_source_sha256=expected_source_sha256,
        )
        verification_b = verify_golden_v3(export_b_root)
        export_b_pin = write_external_pin(
            export_b_root,
            pins_dir / "extract-b.pin.json",
            verification=verification_b,
        )
        export_b_result_path = results_dir / "extract-b.json"
        _write_new_canonical_json(
            export_b_result_path,
            {
                "export": export_b.to_dict(),
                "verification": verification_b.to_dict(),
            },
        )
        census_b = verification_b.manifest.get("census")
        if not isinstance(census_b, Mapping):
            raise GoldenCertificationError("export B verified manifest has no census")
        classification_b = _classify_census(census_b)
        gaps_b = classification_b["BLOCKING_INTEGRITY_GATES"]
        if gaps_b:
            raise GoldenCertificationError(
                f"export B census has blocking integrity gates: {gaps_b}"
            )
        if _canonical_bytes(census_a) != _canonical_bytes(census_b):
            raise GoldenCertificationError("dual extraction census records differ")
        if _canonical_bytes(classification) != _canonical_bytes(classification_b):
            raise GoldenCertificationError(
                "dual extraction coverage classifications differ"
            )
        coverage_result_path = results_dir / "coverage-classification.json"
        _write_new_canonical_json(coverage_result_path, classification)
        _emit(progress, phase="extraction_b_verified", root_hash=export_b.root_hash)

        differential = compare_golden_exports(
            export_a_root,
            export_b_root,
            expected_verification=verification_a,
            actual_verification=verification_b,
        )
        if export_a.root_hash != export_b.root_hash:
            raise GoldenCertificationError(
                "dual extraction root hashes differ despite differential comparison"
            )
        byte_comparison = _compare_export_bytes(
            export_a_root,
            export_b_root,
            progress=progress,
        )
        differential_payload = differential.to_dict()
        differential_payload["byte_comparison"] = byte_comparison
        differential_result_path = results_dir / "dual-extraction.json"
        _write_new_canonical_json(differential_result_path, differential_payload)
        _emit(progress, phase="dual_extraction_exact", root_hash=export_a.root_hash)

        run_rows_a = list(
            iter_golden_stream(
                export_a_root,
                "run",
                verification=verification_a,
            )
        )
        run_rows_b = list(
            iter_golden_stream(
                export_b_root,
                "run",
                verification=verification_b,
            )
        )
        if len(run_rows_a) != 1 or len(run_rows_b) != 1:
            raise GoldenCertificationError("dual extraction run stream is not singular")
        run_identity = _as_mapping(run_rows_a[0], label="Golden V3 run identity")
        if _canonical_bytes(run_identity) != _canonical_bytes(run_rows_b[0]):
            raise GoldenCertificationError("dual extraction run identities differ")

        try:
            replay_payload = replay_golden_v3(
                export_a_root,
                scratch_dir,
                progress=progress,
                verification=verification_a,
            )
        except (GoldenDifferentialError, GoldenReplayMismatchError) as exc:
            raise GoldenReplayDivergenceError(
                f"replay differential diverged: {exc}"
            ) from exc
        replay = _as_mapping(replay_payload, label="Golden V3 replay")
        if replay.get("status") != _REPLAY_EXACT_STATUS:
            raise GoldenReplayDivergenceError(
                f"replay differential is not exact: {replay.get('status')}"
            )
        run_config_hash = run_identity.get("config_hash")
        if not isinstance(run_config_hash, str) or not run_config_hash:
            raise GoldenCertificationError("Golden V3 run identity has no config hash")
        export_streams_a = verification_a.manifest.get("streams")
        if not isinstance(export_streams_a, Mapping):
            raise GoldenCertificationError("export A verified manifest has no streams")
        replay_target = _validate_replay_result(
            replay,
            candidate_root=candidate,
            export_root=export_a_root,
            verification=verification_a,
            run_identity=run_identity,
            expected_run_id=run_id,
            expected_config_hash=run_config_hash,
            expected_source_root_hash=export_a.root_hash,
            census=census_a,
            expected_streams=export_streams_a,
            progress=progress,
        )
        replay_result_path = results_dir / "replay.json"
        _write_new_canonical_json(replay_result_path, replay)
        _emit(progress, phase="replay_differential_exact")

        source_after = _source_fingerprint(source_path)
        _require_expected_source(
            source_after,
            expected_size=expected_source_size,
            expected_sha256=expected_source_sha256,
        )
        if not _source_is_unchanged(source_before, source_after):
            raise GoldenCertificationError(
                "source size/SHA-256/metadata/SQLite sidecars changed during certification"
            )
        source_result_path = results_dir / "source-immutability.json"
        _write_new_canonical_json(
            source_result_path,
            {"before": source_before, "after": source_after, "unchanged": True},
        )

        tested: dict[str, object] = {
            "source_expected_identity": _EXACT,
            "source_unchanged": _EXACT,
            "export_a_verified": _EXACT,
            "export_a_reuse_attestation": (
                _EXACT if _resume_export_a is not None else "FRESH_EXPORT"
            ),
            "export_b_verified": _EXACT,
            "dual_extraction": _EXACT,
            "dual_extraction_bytes": _EXACT,
            "coverage_classification": _EXACT,
            "replay_differential": _EXACT,
        }
        artifact_paths = (
            export_a_result_path,
            export_b_result_path,
            coverage_result_path,
            differential_result_path,
            replay_result_path,
            source_result_path,
            export_a.manifest_path,
            export_b.manifest_path,
            export_a.complete_path,
            export_b.complete_path,
            export_a_pin,
            export_b_pin,
        )
        artifacts: dict[str, object] = {}
        for path in artifact_paths:
            record = _artifact_record(candidate, path)
            artifacts[str(record["path"])] = record

        target_record = _artifact_record(candidate, replay_target)
        artifacts[str(target_record["path"])] = target_record

        manifest_without_hash: dict[str, object] = {
            "schema": _CERTIFICATION_SCHEMA,
            "status": _CERTIFIED_STATUS,
            "run_id": run_id,
            "run_identity": run_identity,
            "census": census_a,
            "source": {"before": source_before, "after": source_after},
            "minimum_free_bytes": _MINIMUM_FREE_BYTES,
            "observed_free_bytes_at_start": free_bytes,
            "coverage_gaps": [],
            "resume": {
                "export_a_reused": _resume_export_a is not None,
                "export_a_reuse_attestation": (
                    _EXACT if _resume_export_a is not None else "NOT_APPLICABLE"
                ),
            },
            "gates": {
                "source_expected_identity": _EXACT,
                "source_immutability": _EXACT,
                "dual_extraction": _EXACT,
                "dual_extraction_bytes": _EXACT,
                "export_a_reuse_attestation": (
                    _EXACT if _resume_export_a is not None else "NOT_APPLICABLE"
                ),
                "replay_differential": _EXACT,
            },
            "exports": {
                "a": {
                    "path": _relative_artifact(candidate, export_a.complete_path.parent),
                    "root_hash": export_a.root_hash,
                    "pin": _relative_artifact(candidate, export_a_pin),
                },
                "b": {
                    "path": _relative_artifact(candidate, export_b.complete_path.parent),
                    "root_hash": export_b.root_hash,
                    "pin": _relative_artifact(candidate, export_b_pin),
                },
            },
            "dual_extraction": differential_payload,
            "replay": replay,
            "artifacts": artifacts,
            "tested": tested,
        }
        manifest_without_hash.update(classification)
        certification_root_hash = _sha256_bytes(_canonical_bytes(manifest_without_hash))
        manifest = dict(manifest_without_hash)
        manifest["certification_root_hash"] = certification_root_hash
        _write_new_canonical_json(manifest_path, manifest)
        manifest_digest = _sha256_file(manifest_path)

        final_pin = {
            "schema": _CERTIFICATION_SCHEMA,
            "status": _CERTIFIED_STATUS,
            "certification_root_hash": certification_root_hash,
            "manifest": _relative_artifact(candidate, manifest_path),
            "manifest_sha256": manifest_digest,
        }
        _write_new_canonical_json(final_pin_path, final_pin)
        _make_read_only(final_pin_path)

        # Verify the complete binding before writing the sole completion marker.
        _verify_certification_material(
            candidate,
            require_complete=False,
            progress=progress,
        )
        _emit(
            progress,
            phase="certification_ready_to_publish",
            certification_root_hash=certification_root_hash,
        )
        _write_new_canonical_json(
            complete_path,
            {
                "status": _CERTIFIED_STATUS,
                "certification_root_hash": certification_root_hash,
                "manifest_sha256": manifest_digest,
            },
        )
        completed = True
        return GoldenCertificationResult(
            status=_CERTIFIED_STATUS,
            candidate_root=candidate,
            certification_root_hash=certification_root_hash,
            manifest_path=manifest_path,
            pin_path=final_pin_path,
            complete_path=complete_path,
            tested=tested,
            classification=classification,
        )
    except GoldenCertificationError:
        raise
    except Exception as exc:
        raise GoldenCertificationError(f"Golden V3 certification failed: {exc}") from exc
    finally:
        if not completed:
            _quarantine_final_markers(manifest_path, final_pin_path, complete_path)


def resume_golden_v3_certification(
    source: Path | str,
    golden_root: Path | str,
    run_id: str,
    *,
    sentinel_path: Path | str,
    expected_source_size: int,
    expected_source_sha256: str,
    expected_export_a_root_hash: str,
    expected_export_a_file_count: int,
    expected_export_a_bytes: int,
    progress: ProgressCallback | None = None,
    shard_rows: int = 100_000,
    shard_bytes: int = 64 * 1024**2,
) -> GoldenCertificationResult:
    """Resume the unique partial candidate after exhaustive export A reuse checks."""

    normalized_root = expected_export_a_root_hash.strip().lower()
    if len(normalized_root) != 64 or any(
        character not in "0123456789abcdef" for character in normalized_root
    ):
        raise GoldenCertificationError("expected export A root hash is invalid")
    if (
        isinstance(expected_export_a_file_count, bool)
        or expected_export_a_file_count <= 0
        or isinstance(expected_export_a_bytes, bool)
        or expected_export_a_bytes <= 0
    ):
        raise GoldenCertificationError(
            "expected export A file count and bytes must be positive integers"
        )
    candidate = _discover_partial_candidate(golden_root)
    return certify_golden_v3(
        source,
        candidate,
        run_id,
        sentinel_path=sentinel_path,
        expected_source_size=expected_source_size,
        expected_source_sha256=expected_source_sha256,
        progress=progress,
        shard_rows=shard_rows,
        shard_bytes=shard_bytes,
        _resume_export_a=_ResumeExportA(
            expected_root_hash=normalized_root,
            expected_file_count=expected_export_a_file_count,
            expected_bytes=expected_export_a_bytes,
        ),
    )


def _manifest_material(manifest: Mapping[str, object]) -> dict[str, object]:
    material = dict(manifest)
    root_hash = material.pop("certification_root_hash", None)
    if not isinstance(root_hash, str) or len(root_hash) != 64:
        raise GoldenCertificationError("certification manifest has no valid root hash")
    return material


def _manifest_entry_path(
    candidate_root: Path, entry: Mapping[str, object], key: str
) -> Path:
    value = entry.get(key)
    if not isinstance(value, str) or not value:
        raise GoldenCertificationError(f"certification manifest has no {key}")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise GoldenCertificationError(f"unsafe certification manifest path: {value}")
    path = candidate_root / relative
    _reject_reparse_path(path, label="certification manifest path")
    try:
        path.resolve(strict=True).relative_to(candidate_root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise GoldenCertificationError(f"certification path escapes candidate: {value}") from exc
    return path


def _verify_certification_material(
    candidate_root: Path,
    *,
    require_complete: bool,
    progress: ProgressCallback | None = None,
) -> GoldenCertificationVerification:
    manifest_path = candidate_root / "manifests" / "certification-manifest.json"
    pin_path = candidate_root / "pin" / "certification.pin.json"
    complete_path = candidate_root / "COMPLETE"
    manifest = _read_canonical_mapping(manifest_path, label="certification manifest")
    if manifest.get("schema") != _CERTIFICATION_SCHEMA:
        raise GoldenCertificationError("unexpected certification manifest schema")
    if manifest.get("status") != _CERTIFIED_STATUS:
        raise GoldenCertificationError("certification manifest status is not certified")
    material = _manifest_material(manifest)
    expected_root_hash = manifest.get("certification_root_hash")
    observed_root_hash = _sha256_bytes(_canonical_bytes(material))
    if expected_root_hash != observed_root_hash:
        raise GoldenCertificationError("certification manifest/root hash mismatch")

    pin = _read_canonical_mapping(pin_path, label="certification pin")
    pin_stat = pin_path.stat()
    if int(pin_stat.st_nlink) != 1:
        raise GoldenCertificationError("certification pin hardlinks are forbidden")
    if pin_stat.st_mode & stat.S_IWRITE:
        raise GoldenCertificationError("certification pin is not read-only")
    if (
        pin.get("schema") != _CERTIFICATION_SCHEMA
        or pin.get("status") != _CERTIFIED_STATUS
        or pin.get("certification_root_hash") != observed_root_hash
        or pin.get("manifest") != "manifests/certification-manifest.json"
        or pin.get("manifest_sha256") != _sha256_file(manifest_path)
    ):
        raise GoldenCertificationError("certification pin/manifest hash mismatch")

    if require_complete:
        complete = _read_canonical_mapping(complete_path, label="COMPLETE")
        if (
            complete.get("status") != _CERTIFIED_STATUS
            or complete.get("certification_root_hash") != observed_root_hash
            or complete.get("manifest_sha256") != _sha256_file(manifest_path)
        ):
            raise GoldenCertificationError("COMPLETE/manifest hash mismatch")
    elif complete_path.exists():
        raise GoldenCertificationError("COMPLETE existed before final certification write")

    gaps = manifest.get("coverage_gaps")
    gates = manifest.get("gates")
    if gaps != [] or not isinstance(gates, Mapping):
        raise GoldenCertificationError("certification coverage gaps are not empty")
    _require_exact_certification_gates(gates)
    census = manifest.get("census")
    if not isinstance(census, Mapping):
        raise GoldenCertificationError("certification census is absent")
    classification = _classify_census(census)
    if classification["BLOCKING_INTEGRITY_GATES"] != []:
        raise GoldenCertificationError(
            "certification census has blocking integrity gates"
        )
    observed_classification = {
        key: manifest.get(key)
        for key in _CLASSIFICATION_KEYS
    }
    if observed_classification != classification:
        raise GoldenCertificationError(
            "certification coverage classification differs from census"
        )
    resume = manifest.get("resume")
    if not isinstance(resume, Mapping):
        raise GoldenCertificationError("certification resume evidence is absent")
    if resume.get("export_a_reused") is True:
        if (
            resume.get("export_a_reuse_attestation") != _EXACT
            or gates.get("export_a_reuse_attestation") != _EXACT
        ):
            raise GoldenCertificationError(
                "reused export A attestation is not exact"
            )
    elif resume.get("export_a_reused") is not False:
        raise GoldenCertificationError("certification resume evidence is invalid")

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, Mapping) or not artifacts:
        raise GoldenCertificationError("certification manifest has no artifact bindings")
    verified_artifacts: set[str] = set()
    for key, record in artifacts.items():
        if not isinstance(key, str) or not isinstance(record, Mapping):
            raise GoldenCertificationError("invalid certification artifact binding")
        if record.get("path") != key:
            raise GoldenCertificationError("certification artifact key/path mismatch")
        _artifact_from_record(candidate_root, record)
        verified_artifacts.add(key)

    replay_result_key = "results/replay.json"
    if replay_result_key not in verified_artifacts:
        raise GoldenCertificationError("replay result artifact is not bound")
    replay_result = _read_canonical_mapping(
        candidate_root / replay_result_key, label="replay result"
    )
    replay_manifest = manifest.get("replay")
    if replay_result != replay_manifest or replay_result.get("status") != _REPLAY_EXACT_STATUS:
        raise GoldenCertificationError("replay result/manifest binding is invalid")
    coverage_result_key = "results/coverage-classification.json"
    if coverage_result_key not in verified_artifacts:
        raise GoldenCertificationError("coverage classification result is not bound")
    coverage_result = _read_canonical_mapping(
        candidate_root / coverage_result_key,
        label="coverage classification result",
    )
    if coverage_result != classification:
        raise GoldenCertificationError(
            "coverage classification result/manifest binding is invalid"
        )

    exports = manifest.get("exports")
    if not isinstance(exports, Mapping):
        raise GoldenCertificationError("certification manifest has no exports")
    verified_exports: dict[str, GoldenVerification] = {}
    export_paths: dict[str, Path] = {}
    for name in ("a", "b"):
        entry = exports.get(name)
        if not isinstance(entry, Mapping):
            raise GoldenCertificationError(f"certification manifest has no export {name}")
        export_root = _manifest_entry_path(candidate_root, entry, "path")
        export_pin = _manifest_entry_path(candidate_root, entry, "pin")
        verification = verify_golden_v3(export_root, pin_path=export_pin)
        if entry.get("root_hash") != verification.root_hash:
            raise GoldenCertificationError(f"export {name} root hash mismatch")
        verified_exports[name] = verification
        export_paths[name] = export_root
    compare_golden_exports(
        export_paths["a"],
        export_paths["b"],
        expected_verification=verified_exports["a"],
        actual_verification=verified_exports["b"],
    )
    if verified_exports["a"].root_hash != verified_exports["b"].root_hash:
        raise GoldenCertificationError("certified export root hashes differ")
    byte_comparison = _compare_export_bytes(
        export_paths["a"],
        export_paths["b"],
    )
    dual_extraction = manifest.get("dual_extraction")
    dual_result_key = "results/dual-extraction.json"
    if (
        not isinstance(dual_extraction, Mapping)
        or dual_extraction.get("byte_comparison") != byte_comparison
        or dual_result_key not in verified_artifacts
        or _read_canonical_mapping(
            candidate_root / dual_result_key,
            label="dual extraction result",
        )
        != dict(dual_extraction)
    ):
        raise GoldenCertificationError(
            "dual extraction byte/result binding is invalid"
        )

    manifest_run_id = manifest.get("run_id")
    run_identity = manifest.get("run_identity")
    if not isinstance(manifest_run_id, str) or not isinstance(run_identity, Mapping):
        raise GoldenCertificationError("certification run identity is invalid")
    manifest_config_hash = run_identity.get("config_hash")
    if not isinstance(manifest_config_hash, str) or not manifest_config_hash:
        raise GoldenCertificationError("certification run config hash is invalid")
    export_streams = verified_exports["a"].manifest.get("streams")
    if not isinstance(export_streams, Mapping):
        raise GoldenCertificationError("certified export A has no stream manifest")
    replay_target = _validate_replay_result(
        replay_result,
        candidate_root=candidate_root,
        export_root=export_paths["a"],
        verification=verified_exports["a"],
        run_identity=run_identity,
        expected_run_id=manifest_run_id,
        expected_config_hash=manifest_config_hash,
        expected_source_root_hash=verified_exports["a"].root_hash,
        census=census,
        expected_streams=export_streams,
        progress=progress,
        progress_phase="final_preserved_target_validation",
    )
    replay_target_key = _relative_artifact(candidate_root, replay_target)
    if replay_target_key not in verified_artifacts:
        raise GoldenCertificationError("replay target artifact is not bound")

    source = manifest.get("source")
    if not isinstance(source, Mapping):
        raise GoldenCertificationError("certification source evidence is absent")
    before = source.get("before")
    after = source.get("after")
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        raise GoldenCertificationError("certification source evidence is invalid")
    if not _source_is_unchanged(before, after):
        raise GoldenCertificationError("certification source evidence is not immutable")

    tested: dict[str, object] = {
        "manifest_root_hash": _EXACT,
        "pin": _EXACT,
        "complete": _EXACT if require_complete else "PENDING",
        "artifact_hashes": _EXACT,
        "export_a": _EXACT,
        "export_b": _EXACT,
        "dual_extraction": _EXACT,
        "dual_extraction_bytes": _EXACT,
        "coverage_classification": _EXACT,
        "replay_result": _EXACT,
        "source_evidence": _EXACT,
    }
    return GoldenCertificationVerification(
        candidate_root=candidate_root,
        certification_root_hash=observed_root_hash,
        manifest=manifest,
        tested=tested,
    )


def verify_golden_v3_certification(
    candidate_root: Path | str,
) -> GoldenCertificationVerification:
    """Verify every binding of a completed Golden V3 certification."""

    lexical_candidate = _lexical_absolute(Path(candidate_root))
    _reject_reparse_path(
        lexical_candidate,
        label="certification candidate",
    )
    candidate = _safe_resolve(
        lexical_candidate, must_exist=True, label="certification candidate"
    )
    if not candidate.is_dir() or candidate.is_symlink():
        raise GoldenCertificationError(f"candidate is not a regular directory: {candidate}")
    try:
        return _verify_certification_material(candidate, require_complete=True)
    except GoldenCertificationError:
        raise
    except Exception as exc:
        raise GoldenCertificationError(f"certification verification failed: {exc}") from exc
