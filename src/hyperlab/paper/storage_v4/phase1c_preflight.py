"""Strict read-only authority preflight for Storage v4 Phase 1C.

The preflight never creates the mission root. It authenticates the external
Golden and Phase 1B evidence and freezes the roadmap target before measurement.
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import shutil
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import cast

from hyperlab.paper.golden_v3 import GOLDEN_STREAM_NAMES, GoldenVerification, verify_golden_v3
from hyperlab.paper.golden_v3_certification import (
    GoldenCertificationVerification,
    verify_golden_v3_certification,
)
from hyperlab.paper.storage_v4.phase1b_certification import (
    GOLDEN_V3_EXPECTED_COMMITS,
    GOLDEN_V3_EXPECTED_ROWS,
    GOLDEN_V3_EXPECTED_STREAMS,
    PHASE1B_CERTIFICATION_FORMAT,
    PHASE1B_COMPLETE_FORMAT,
    PHASE1B_SUCCESS,
)

PHASE1C_PREFLIGHT_STATUS = "STORAGE_V4_PHASE_1C_PREFLIGHT_VERIFIED"
PHASE1C_POSTFLIGHT_STATUS = "STORAGE_V4_PHASE_1C_EXTERNAL_INPUTS_UNCHANGED"
PHASE1C_TARGET_PHRASE = "<0.20 GiB/h"
PHASE1C_TARGET_CONTEXT = ("Storage v4", "avec marge", "differential logique exact")
DEFAULT_MINIMUM_FREE_BYTES = 20 * 1024**3
_READ_CHUNK_BYTES = 1024 * 1024
_SHA256_LENGTH = 64
_WINDOWS_RESERVED_LEAVES = frozenset(
    {"AUX", "CON", "NUL", "PRN"}
    | {f"COM{index}" for index in range(1, 10)}
    | {f"LPT{index}" for index in range(1, 10)}
)


class Phase1CPreflightError(RuntimeError):
    """A Phase 1C authority, path, capacity, or immutability gate failed."""


def _require_digest(value: str, *, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be one lowercase SHA-256 digest")


def _require_absolute_path(value: Path, *, label: str) -> None:
    if not isinstance(value, Path):
        raise TypeError(f"{label} must be pathlib.Path")
    if not value.is_absolute():
        raise ValueError(f"{label} must be an explicit absolute path")
    if ".." in value.parts:
        raise ValueError(f"{label} must not contain path traversal")


@dataclass(frozen=True, slots=True)
class Phase1CGoldenExpectations:
    certification_root_hash: str
    golden_root_hash: str
    source_sha256: str
    run_id: str
    pin_sha256: str
    source_size_bytes: int = 2_014_072_832
    export_physical_bytes: int = 2_456_283_751
    commit_count: int = GOLDEN_V3_EXPECTED_COMMITS
    row_count: int = GOLDEN_V3_EXPECTED_ROWS
    stream_count: int = GOLDEN_V3_EXPECTED_STREAMS
    market_gap_count: int = 1

    def __post_init__(self) -> None:
        for label, digest_value in (
            ("certification_root_hash", self.certification_root_hash),
            ("golden_root_hash", self.golden_root_hash),
            ("source_sha256", self.source_sha256),
            ("run_id", self.run_id),
            ("pin_sha256", self.pin_sha256),
        ):
            _require_digest(digest_value, label=label)
        for label, count_value, minimum in (
            ("commit_count", self.commit_count, 1),
            ("row_count", self.row_count, 1),
            ("stream_count", self.stream_count, 1),
            ("market_gap_count", self.market_gap_count, 0),
            ("source_size_bytes", self.source_size_bytes, 1),
            ("export_physical_bytes", self.export_physical_bytes, 1),
        ):
            if type(count_value) is not int or count_value < minimum:
                raise ValueError(f"{label} must be an integer >= {minimum}")


@dataclass(frozen=True, slots=True)
class Phase1BProofExpectations:
    report_sha256: str
    manifest_root: str
    final_prefix_root: str
    storage_v4_store_bytes: int = 528_250_030
    anchor_bytes: int = 12_288
    compatibility_segment_bytes: int = 317_492_777
    status: str = PHASE1B_SUCCESS
    expected_leaf_name: str = "retry-02"

    def __post_init__(self) -> None:
        for digest_label, digest_value in (
            ("report_sha256", self.report_sha256),
            ("manifest_root", self.manifest_root),
            ("final_prefix_root", self.final_prefix_root),
        ):
            _require_digest(digest_value, label=digest_label)
        if not isinstance(self.status, str) or not self.status:
            raise ValueError("status must be a non-empty string")
        if (
            not isinstance(self.expected_leaf_name, str)
            or not self.expected_leaf_name
            or Path(self.expected_leaf_name).name != self.expected_leaf_name
        ):
            raise ValueError("expected_leaf_name must be one safe path component")
        for byte_label, byte_value in (
            ("storage_v4_store_bytes", self.storage_v4_store_bytes),
            ("anchor_bytes", self.anchor_bytes),
            ("compatibility_segment_bytes", self.compatibility_segment_bytes),
        ):
            if type(byte_value) is not int or byte_value <= 0:
                raise ValueError(f"{byte_label} must be a positive integer")


@dataclass(frozen=True, slots=True)
class Phase1CPreflightConfig:
    mission_root: Path
    allowed_parent: Path
    golden_certification_root: Path
    golden_export_root: Path
    golden_pin_path: Path
    phase1b_root: Path
    roadmap_path: Path
    golden: Phase1CGoldenExpectations
    phase1b: Phase1BProofExpectations
    minimum_free_bytes: int = DEFAULT_MINIMUM_FREE_BYTES
    expected_roadmap_sha256: str | None = None
    expected_target_line_number: int | None = None
    expected_canonical_target_occurrences: int = 3

    def __post_init__(self) -> None:
        for label, value in (
            ("mission_root", self.mission_root),
            ("allowed_parent", self.allowed_parent),
            ("golden_certification_root", self.golden_certification_root),
            ("golden_export_root", self.golden_export_root),
            ("golden_pin_path", self.golden_pin_path),
            ("phase1b_root", self.phase1b_root),
            ("roadmap_path", self.roadmap_path),
        ):
            _require_absolute_path(value, label=label)
        if type(self.golden) is not Phase1CGoldenExpectations:
            raise TypeError("golden must be Phase1CGoldenExpectations")
        if type(self.phase1b) is not Phase1BProofExpectations:
            raise TypeError("phase1b must be Phase1BProofExpectations")
        if type(self.minimum_free_bytes) is not int or self.minimum_free_bytes <= 0:
            raise ValueError("minimum_free_bytes must be a positive integer")
        if self.expected_roadmap_sha256 is not None:
            _require_digest(self.expected_roadmap_sha256, label="expected_roadmap_sha256")
        if self.expected_target_line_number is not None and (
            type(self.expected_target_line_number) is not int
            or self.expected_target_line_number <= 0
        ):
            raise ValueError("expected_target_line_number must be a positive integer or None")
        if (
            type(self.expected_canonical_target_occurrences) is not int
            or self.expected_canonical_target_occurrences <= 0
        ):
            raise ValueError("expected_canonical_target_occurrences must be a positive integer")


@dataclass(frozen=True, slots=True)
class FileByteWitness:
    path: Path
    size_bytes: int
    sha256: str

    def to_dict(self) -> dict[str, object]:
        return {"path": str(self.path), "sha256": self.sha256, "size_bytes": self.size_bytes}


@dataclass(frozen=True, slots=True)
class TreeByteWitness:
    root: Path
    file_count: int
    directory_count: int
    total_bytes: int
    tree_sha256: str

    def to_dict(self) -> dict[str, object]:
        return {
            "directory_count": self.directory_count,
            "file_count": self.file_count,
            "root": str(self.root),
            "total_bytes": self.total_bytes,
            "tree_sha256": self.tree_sha256,
        }


@dataclass(frozen=True, slots=True)
class GoldenAuthorityWitness:
    certification_root: Path
    certification_root_hash: str
    certification_tree: TreeByteWitness
    export_root: Path
    golden_root_hash: str
    export_tree: TreeByteWitness
    pin: FileByteWitness
    source_sha256: str
    source_size_bytes: int
    export_physical_bytes: int
    run_id: str
    commit_count: int
    row_count: int
    stream_count: int
    market_gap_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "certification_root": str(self.certification_root),
            "certification_root_hash": self.certification_root_hash,
            "certification_tree": self.certification_tree.to_dict(),
            "commit_count": self.commit_count,
            "export_root": str(self.export_root),
            "export_tree": self.export_tree.to_dict(),
            "golden_root_hash": self.golden_root_hash,
            "market_gap_count": self.market_gap_count,
            "pin": self.pin.to_dict(),
            "row_count": self.row_count,
            "run_id": self.run_id,
            "source_sha256": self.source_sha256,
            "source_size_bytes": self.source_size_bytes,
            "export_physical_bytes": self.export_physical_bytes,
            "stream_count": self.stream_count,
        }


@dataclass(frozen=True, slots=True)
class Phase1BProofWitness:
    root: Path
    root_tree: TreeByteWitness
    report: FileByteWitness
    complete: FileByteWitness
    status: str
    report_sha256: str
    manifest_root: str
    final_prefix_root: str
    storage_v4_store_bytes: int
    anchor_bytes: int
    compatibility_segment_bytes: int

    def to_dict(self) -> dict[str, object]:
        return {
            "complete": self.complete.to_dict(),
            "final_prefix_root": self.final_prefix_root,
            "storage_v4_store_bytes": self.storage_v4_store_bytes,
            "anchor_bytes": self.anchor_bytes,
            "store_plus_anchor_bytes": self.storage_v4_store_bytes + self.anchor_bytes,
            "compatibility_segment_bytes": self.compatibility_segment_bytes,
            "manifest_root": self.manifest_root,
            "report": self.report.to_dict(),
            "report_sha256": self.report_sha256,
            "root": str(self.root),
            "root_tree": self.root_tree.to_dict(),
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class RoadmapTargetLineWitness:
    line_number: int
    raw_line_text: str
    decoded_line_text: str

    def to_dict(self) -> dict[str, object]:
        return {
            "decoded_line_text": self.decoded_line_text,
            "line_number": self.line_number,
            "raw_line_text": self.raw_line_text,
        }


@dataclass(frozen=True, slots=True)
class CapacityTargetWitness:
    roadmap: FileByteWitness
    line_number: int
    raw_line_text: str
    decoded_line_text: str
    phrase: str
    comparator: str
    threshold_gib_per_hour: str
    margin_required: bool
    corroborating_lines: tuple[RoadmapTargetLineWitness, ...]
    consistent_numeric_target_line_numbers: tuple[int, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "comparator": self.comparator,
            "consistent_numeric_target_line_numbers": list(
                self.consistent_numeric_target_line_numbers
            ),
            "corroborating_lines": [line.to_dict() for line in self.corroborating_lines],
            "decoded_line_text": self.decoded_line_text,
            "line_number": self.line_number,
            "margin_required": self.margin_required,
            "phrase": self.phrase,
            "raw_line_text": self.raw_line_text,
            "roadmap": self.roadmap.to_dict(),
            "threshold_gib_per_hour": self.threshold_gib_per_hour,
        }


@dataclass(frozen=True, slots=True)
class Phase1CExternalWitness:
    golden: GoldenAuthorityWitness
    phase1b: Phase1BProofWitness
    capacity_target: CapacityTargetWitness

    def to_dict(self) -> dict[str, object]:
        return {
            "capacity_target": self.capacity_target.to_dict(),
            "golden": self.golden.to_dict(),
            "phase1b": self.phase1b.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class Phase1CPreflightWitness:
    mission_root: Path
    allowed_parent: Path
    mission_root_state: str
    minimum_free_bytes: int
    observed_free_bytes: int
    external: Phase1CExternalWitness
    status: str = PHASE1C_PREFLIGHT_STATUS

    def to_dict(self) -> dict[str, object]:
        return {
            "allowed_parent": str(self.allowed_parent),
            "external": self.external.to_dict(),
            "minimum_free_bytes": self.minimum_free_bytes,
            "mission_root": str(self.mission_root),
            "mission_root_state": self.mission_root_state,
            "observed_free_bytes": self.observed_free_bytes,
            "status": self.status,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class Phase1CPreflightResult:
    witness: Phase1CPreflightWitness
    golden_verification: GoldenVerification
    certification_verification: GoldenCertificationVerification

    def to_dict(self) -> dict[str, object]:
        return self.witness.to_dict()


@dataclass(frozen=True, slots=True)
class Phase1CPostflightVerification:
    mission_root: Path
    external: Phase1CExternalWitness
    unchanged: bool = True
    status: str = PHASE1C_POSTFLIGHT_STATUS

    def to_dict(self) -> dict[str, object]:
        return {
            "external": self.external.to_dict(),
            "mission_root": str(self.mission_root),
            "status": self.status,
            "unchanged": self.unchanged,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class _NodeIdentity:
    device: int
    inode: int
    mode: int
    size: int
    mtime_ns: int
    ctime_ns: int
    link_count: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> _NodeIdentity:
        return cls(
            device=int(value.st_dev),
            inode=int(value.st_ino),
            mode=int(value.st_mode),
            size=int(value.st_size),
            mtime_ns=int(value.st_mtime_ns),
            ctime_ns=int(value.st_ctime_ns),
            link_count=int(value.st_nlink),
        )

    def same_open_file(self, other: _NodeIdentity) -> bool:
        """Compare path/handle identity, excluding unreliable Windows handle ctime."""

        return (
            self.device,
            self.inode,
            self.mode,
            self.size,
            self.mtime_ns,
            self.link_count,
        ) == (
            other.device,
            other.inode,
            other.mode,
            other.size,
            other.mtime_ns,
            other.link_count,
        )


def _has_reparse_attribute(value: os.stat_result) -> bool:
    attribute = int(getattr(value, "st_file_attributes", 0))
    reparse = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))
    return bool(reparse and attribute & reparse)


def _reject_reparse_components(path: Path, *, label: str) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if not os.path.lexists(current):
            continue
        try:
            value = current.lstat()
        except OSError as error:
            raise Phase1CPreflightError(f"{label} could not be inspected: {current}") from error
        if stat.S_ISLNK(value.st_mode) or _has_reparse_attribute(value):
            raise Phase1CPreflightError(
                f"{label} contains a symlink, junction, or reparse component: {current}"
            )


def _resolved_existing_directory(path: Path, *, label: str) -> Path:
    _reject_reparse_components(path, label=label)
    try:
        value = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise Phase1CPreflightError(f"{label} does not exist: {path}") from error
    if not stat.S_ISDIR(value.st_mode) or path.is_symlink() or _has_reparse_attribute(value):
        raise Phase1CPreflightError(f"{label} is not a regular directory: {path}")
    return resolved


def _resolved_existing_file(path: Path, *, label: str) -> Path:
    _reject_reparse_components(path, label=label)
    try:
        value = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise Phase1CPreflightError(f"{label} does not exist: {path}") from error
    if not stat.S_ISREG(value.st_mode) or path.is_symlink() or _has_reparse_attribute(value):
        raise Phase1CPreflightError(f"{label} is not a regular file: {path}")
    if int(value.st_nlink) != 1:
        raise Phase1CPreflightError(f"{label} hardlinks are forbidden: {path}")
    return resolved


def _stable_file_read(
    path: Path,
    *,
    label: str,
    collect_bytes: bool,
) -> tuple[bytes | None, FileByteWitness]:
    resolved = _resolved_existing_file(path, label=label)
    try:
        before = _NodeIdentity.from_stat(resolved.stat(follow_symlinks=False))
        digest = hashlib.sha256()
        chunks: list[bytes] | None = [] if collect_bytes else None
        with resolved.open("rb") as handle:
            opened = _NodeIdentity.from_stat(os.fstat(handle.fileno()))
            if not before.same_open_file(opened):
                raise Phase1CPreflightError(f"{label} changed while it was opened")
            while chunk := handle.read(_READ_CHUNK_BYTES):
                digest.update(chunk)
                if chunks is not None:
                    chunks.append(chunk)
            after_handle = _NodeIdentity.from_stat(os.fstat(handle.fileno()))
        after_path = _NodeIdentity.from_stat(resolved.stat(follow_symlinks=False))
    except Phase1CPreflightError:
        raise
    except OSError as error:
        raise Phase1CPreflightError(f"{label} could not be read stably") from error
    if not before.same_open_file(after_handle) or before != after_path:
        raise Phase1CPreflightError(f"{label} changed while it was read")
    if before.link_count != 1:
        raise Phase1CPreflightError(f"{label} hardlinks are forbidden")
    payload = None if chunks is None else b"".join(chunks)
    if payload is not None and len(payload) != before.size:
        raise Phase1CPreflightError(f"{label} size changed while it was read")
    return payload, FileByteWitness(resolved, before.size, digest.hexdigest())


def _stable_file_bytes(path: Path, *, label: str) -> tuple[bytes, FileByteWitness]:
    payload, witness = _stable_file_read(path, label=label, collect_bytes=True)
    if payload is None:
        raise AssertionError("stable byte collection returned no bytes")
    return payload, witness


def _stable_file_witness(path: Path, *, label: str) -> FileByteWitness:
    _payload, witness = _stable_file_read(path, label=label, collect_bytes=False)
    return witness


def _frame_tree_item(kind: bytes, relative: str, size: int, digest: str = "") -> bytes:
    relative_bytes = relative.encode("utf-8", errors="strict")
    return b"".join(
        (
            kind,
            len(relative_bytes).to_bytes(8, "big"),
            relative_bytes,
            size.to_bytes(16, "big"),
            bytes.fromhex(digest) if digest else b"",
        )
    )


def _tree_byte_witness(path: Path, *, label: str) -> TreeByteWitness:
    root = _resolved_existing_directory(path, label=label)
    digest = hashlib.sha256()
    file_count = 0
    directory_count = 0
    total_bytes = 0

    def visit(directory: Path, relative: Path) -> None:
        nonlocal file_count, directory_count, total_bytes
        try:
            before = _NodeIdentity.from_stat(directory.stat(follow_symlinks=False))
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name.encode("utf-8"))
        except (OSError, UnicodeEncodeError) as error:
            raise Phase1CPreflightError(f"{label} could not be traversed stably") from error
        directory_count += 1
        relative_text = relative.as_posix() if relative.parts else "."
        digest.update(_frame_tree_item(b"D", relative_text, 0))
        for entry in entries:
            entry_path = directory / entry.name
            entry_relative = relative / entry.name
            try:
                entry_stat = entry.stat(follow_symlinks=False)
            except OSError as error:
                raise Phase1CPreflightError(f"{label} entry disappeared: {entry_path}") from error
            if stat.S_ISLNK(entry_stat.st_mode) or _has_reparse_attribute(entry_stat):
                raise Phase1CPreflightError(
                    f"{label} contains a symlink, junction, or reparse entry: {entry_path}"
                )
            if stat.S_ISDIR(entry_stat.st_mode):
                visit(entry_path, entry_relative)
                continue
            if not stat.S_ISREG(entry_stat.st_mode):
                raise Phase1CPreflightError(
                    f"{label} contains an unsupported filesystem object: {entry_path}"
                )
            witnessed = _stable_file_witness(entry_path, label=f"{label} file")
            file_count += 1
            total_bytes += witnessed.size_bytes
            digest.update(
                _frame_tree_item(
                    b"F",
                    entry_relative.as_posix(),
                    witnessed.size_bytes,
                    witnessed.sha256,
                )
            )
        try:
            after = _NodeIdentity.from_stat(directory.stat(follow_symlinks=False))
        except OSError as error:
            raise Phase1CPreflightError(f"{label} directory disappeared") from error
        if before != after:
            raise Phase1CPreflightError(f"{label} changed while it was traversed")

    visit(root, Path())
    return TreeByteWitness(
        root=root,
        file_count=file_count,
        directory_count=directory_count,
        total_bytes=total_bytes,
        tree_sha256=digest.hexdigest(),
    )


def _duplicate_rejecting_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise Phase1CPreflightError(f"canonical JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise Phase1CPreflightError(f"canonical JSON contains non-finite value {value}")


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8", errors="strict")
            + b"\n"
        )
    except (RecursionError, TypeError, UnicodeEncodeError, ValueError) as error:
        raise Phase1CPreflightError("value cannot be encoded as canonical JSON") from error


def _read_canonical_mapping(
    path: Path,
    *,
    label: str,
) -> tuple[Mapping[str, object], FileByteWitness]:
    payload, witness = _stable_file_bytes(path, label=label)
    try:
        decoded = payload.decode("utf-8", errors="strict")
        value = json.loads(
            decoded,
            object_pairs_hook=_duplicate_rejecting_object,
            parse_constant=_reject_json_constant,
        )
    except Phase1CPreflightError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise Phase1CPreflightError(f"{label} is not strict UTF-8 JSON") from error
    if not isinstance(value, Mapping):
        raise Phase1CPreflightError(f"{label} is not a JSON object")
    if payload != _canonical_json_bytes(value):
        raise Phase1CPreflightError(f"{label} is not canonical JSON")
    return cast(Mapping[str, object], value), witness


def _mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise Phase1CPreflightError(f"{label} is absent or malformed")
    return cast(Mapping[str, object], value)


def _integer(value: object, *, label: str) -> int:
    if type(value) is not int or value < 0:
        raise Phase1CPreflightError(f"{label} must be a non-negative integer")
    return value


def _resolved_manifest_entry(root: Path, value: object, *, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise Phase1CPreflightError(f"{label} is absent")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise Phase1CPreflightError(f"{label} contains an unsafe path")
    candidate = root / relative
    _reject_reparse_components(candidate, label=label)
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as error:
        raise Phase1CPreflightError(f"{label} escapes the certification root") from error
    return resolved


def _golden_counts(manifest: Mapping[str, object]) -> tuple[int, int, int, int]:
    census = _mapping(manifest.get("census"), label="Golden census")
    commit_count = _integer(census.get("commit_count"), label="Golden commit_count")
    alert_counts = _mapping(
        census.get("alert_code_counts"),
        label="Golden alert_code_counts",
    )
    market_gap_count = _integer(
        alert_counts.get("MARKET_GAP", 0),
        label="Golden MARKET_GAP count",
    )
    streams = _mapping(manifest.get("streams"), label="Golden streams")
    if set(streams) != set(GOLDEN_STREAM_NAMES):
        raise Phase1CPreflightError(
            "Golden stream names differ from the canonical 13-stream schema"
        )
    row_count = 0
    for name in GOLDEN_STREAM_NAMES:
        stream = _mapping(streams.get(name), label=f"Golden stream {name}")
        row_count += _integer(
            stream.get("row_count"),
            label=f"Golden stream {name} row_count",
        )
    return commit_count, row_count, len(streams), market_gap_count


def _golden_payload_physical_bytes(manifest: Mapping[str, object]) -> int:
    """Return authenticated shard bytes, excluding manifest/COMPLETE controls."""

    streams = _mapping(manifest.get("streams"), label="Golden streams")
    total_bytes = 0
    for name in GOLDEN_STREAM_NAMES:
        stream = _mapping(streams.get(name), label=f"Golden stream {name}")
        shards = stream.get("shards")
        if not isinstance(shards, list):
            raise Phase1CPreflightError(
                f"Golden stream {name} shards are absent or malformed"
            )
        for index, shard_value in enumerate(shards):
            shard = _mapping(
                shard_value,
                label=f"Golden stream {name} shard {index}",
            )
            physical_size = shard.get("physical_size")
            if type(physical_size) is not int or physical_size <= 0:
                raise Phase1CPreflightError(
                    f"Golden stream {name} shard {index} physical_size "
                    "must be a positive integer"
                )
            total_bytes += physical_size
    return total_bytes


def _golden_source_sha256(manifest: Mapping[str, object]) -> str:
    source = _mapping(manifest.get("source"), label="Golden source")
    value = source.get("sha256")
    if not isinstance(value, str):
        raise Phase1CPreflightError("Golden source SHA-256 is absent")
    return value


def _verify_golden_authority(
    config: Phase1CPreflightConfig,
) -> tuple[GoldenAuthorityWitness, GoldenVerification, GoldenCertificationVerification]:
    certification = verify_golden_v3_certification(config.golden_certification_root)
    golden = verify_golden_v3(config.golden_export_root, pin_path=config.golden_pin_path)
    expected = config.golden
    if certification.certification_root_hash != expected.certification_root_hash:
        raise Phase1CPreflightError("Golden certification root hash differs from the pinned value")
    if golden.root_hash != expected.golden_root_hash:
        raise Phase1CPreflightError("Golden export root hash differs from the pinned value")
    if golden.manifest.get("root_hash") != expected.golden_root_hash:
        raise Phase1CPreflightError("Golden manifest root differs from the pinned value")
    if golden.manifest.get("run_id") != expected.run_id:
        raise Phase1CPreflightError("Golden run id differs from the pinned value")
    source_sha256 = _golden_source_sha256(golden.manifest)
    if source_sha256 != expected.source_sha256:
        raise Phase1CPreflightError("Golden source SHA-256 differs from the pinned value")
    source = _mapping(golden.manifest.get("source"), label="Golden source")
    source_stat = _mapping(source.get("stat"), label="Golden source stat")
    source_size_bytes = _integer(source_stat.get("size"), label="Golden source size")
    if source_size_bytes != expected.source_size_bytes:
        raise Phase1CPreflightError("Golden source byte size differs from the pinned value")
    counts = _golden_counts(golden.manifest)
    expected_counts = (
        expected.commit_count,
        expected.row_count,
        expected.stream_count,
        expected.market_gap_count,
    )
    if counts != expected_counts:
        raise Phase1CPreflightError(
            "Golden census differs from the pinned commit/row/stream/MARKET_GAP counts"
        )
    payload_physical_bytes = _golden_payload_physical_bytes(golden.manifest)
    if payload_physical_bytes != expected.export_physical_bytes:
        raise Phase1CPreflightError(
            "Golden export payload physical bytes differ from the pinned value "
            f"(observed={payload_physical_bytes}, "
            f"expected={expected.export_physical_bytes})"
        )

    certification_manifest = certification.manifest
    if certification_manifest.get("run_id") != expected.run_id:
        raise Phase1CPreflightError("certification run id differs from the pinned value")
    certification_source = _mapping(
        certification_manifest.get("source"),
        label="certification source",
    )
    source_before = _mapping(
        certification_source.get("before"),
        label="certification source before",
    )
    source_after = _mapping(
        certification_source.get("after"),
        label="certification source after",
    )
    if (
        source_before.get("sha256") != expected.source_sha256
        or source_after.get("sha256") != expected.source_sha256
    ):
        raise Phase1CPreflightError("certification source SHA-256 differs from the pinned value")
    if (
        _integer(source_before.get("bytes"), label="certification source before bytes")
        != expected.source_size_bytes
        or _integer(source_after.get("bytes"), label="certification source after bytes")
        != expected.source_size_bytes
    ):
        raise Phase1CPreflightError("certification source byte size differs from the pinned value")
    certification_census = _mapping(
        certification_manifest.get("census"),
        label="certification census",
    )
    certification_alerts = _mapping(
        certification_census.get("alert_code_counts"),
        label="certification alert_code_counts",
    )
    if (
        _integer(
            certification_census.get("commit_count"),
            label="certification commit_count",
        )
        != expected.commit_count
        or _integer(
            certification_alerts.get("MARKET_GAP", 0),
            label="certification MARKET_GAP count",
        )
        != expected.market_gap_count
    ):
        raise Phase1CPreflightError("certification census differs from the pinned Golden census")

    certification_root = certification.candidate_root.resolve(strict=True)
    if certification_root != config.golden_certification_root.resolve(strict=True):
        raise Phase1CPreflightError("canonical verifier returned a different certification root")
    export_root = golden.export_root.resolve(strict=True)
    if export_root != config.golden_export_root.resolve(strict=True):
        raise Phase1CPreflightError("canonical verifier returned a different Golden export root")
    exports = _mapping(certification_manifest.get("exports"), label="certification exports")
    export_a = _mapping(exports.get("a"), label="certification export A")
    bound_export = _resolved_manifest_entry(
        certification_root,
        export_a.get("path"),
        label="certification export A path",
    )
    bound_pin = _resolved_manifest_entry(
        certification_root,
        export_a.get("pin"),
        label="certification export A pin",
    )
    if bound_export != export_root:
        raise Phase1CPreflightError("provided Golden export is not certified export A")
    if bound_pin != config.golden_pin_path.resolve(strict=True):
        raise Phase1CPreflightError("provided Golden pin is not certified export A pin")
    if export_a.get("root_hash") != expected.golden_root_hash:
        raise Phase1CPreflightError("certification export A root differs from the pinned value")

    pin_witness = _stable_file_witness(config.golden_pin_path, label="Golden export A pin")
    if pin_witness.sha256 != expected.pin_sha256:
        raise Phase1CPreflightError("Golden export A pin SHA-256 differs from the pinned value")
    artifacts = _mapping(
        certification_manifest.get("artifacts"),
        label="certification artifacts",
    )
    pin_relative = bound_pin.relative_to(certification_root).as_posix()
    pin_record = _mapping(
        artifacts.get(pin_relative),
        label="certification export A pin artifact",
    )
    if pin_record.get("sha256") != expected.pin_sha256:
        raise Phase1CPreflightError("certification pin artifact SHA-256 differs from the pinned value")

    certification_tree = _tree_byte_witness(
        certification_root,
        label="Golden certification tree",
    )
    export_tree = _tree_byte_witness(export_root, label="Golden export A tree")
    witness = GoldenAuthorityWitness(
        certification_root=certification_root,
        certification_root_hash=certification.certification_root_hash,
        certification_tree=certification_tree,
        export_root=export_root,
        golden_root_hash=golden.root_hash,
        export_tree=export_tree,
        pin=pin_witness,
        source_sha256=source_sha256,
        source_size_bytes=source_size_bytes,
        export_physical_bytes=payload_physical_bytes,
        run_id=expected.run_id,
        commit_count=counts[0],
        row_count=counts[1],
        stream_count=counts[2],
        market_gap_count=counts[3],
    )
    return witness, golden, certification


def _verify_phase1b_proof(config: Phase1CPreflightConfig) -> Phase1BProofWitness:
    expected = config.phase1b
    root = _resolved_existing_directory(config.phase1b_root, label="Phase 1B retry-02 root")
    if root.name != expected.expected_leaf_name:
        raise Phase1CPreflightError(
            f"Phase 1B proof root must be the pinned {expected.expected_leaf_name!r} result"
        )
    report, report_witness = _read_canonical_mapping(
        root / "report.json",
        label="Phase 1B report.json",
    )
    complete, complete_witness = _read_canonical_mapping(
        root / "COMPLETE",
        label="Phase 1B COMPLETE",
    )
    if report_witness.sha256 != expected.report_sha256:
        raise Phase1CPreflightError("Phase 1B report SHA-256 differs from the pinned value")
    if report.get("format") != PHASE1B_CERTIFICATION_FORMAT:
        raise Phase1CPreflightError("Phase 1B report format differs")
    if report.get("status") != expected.status:
        raise Phase1CPreflightError("Phase 1B report status differs")
    sizes = _mapping(report.get("sizes"), label="Phase 1B sizes")
    storage_v4_store_bytes = _integer(
        sizes.get("v3_compatibility_import_storage_v4_store_bytes"),
        label="Phase 1B Storage v4 store bytes",
    )
    anchor_bytes = _integer(sizes.get("anchor_bytes"), label="Phase 1B anchor bytes")
    compatibility_segment_bytes = _integer(
        sizes.get("v3_compatibility_import_segment_bytes"),
        label="Phase 1B compatibility segment bytes",
    )
    if (
        storage_v4_store_bytes != expected.storage_v4_store_bytes
        or anchor_bytes != expected.anchor_bytes
        or compatibility_segment_bytes != expected.compatibility_segment_bytes
    ):
        raise Phase1CPreflightError("Phase 1B authoritative byte census differs")
    audit = _mapping(report.get("audit"), label="Phase 1B audit")
    if audit.get("manifest_root") != expected.manifest_root:
        raise Phase1CPreflightError("Phase 1B manifest root differs")
    if audit.get("final_prefix_root") != expected.final_prefix_root:
        raise Phase1CPreflightError("Phase 1B final prefix root differs")
    golden = _mapping(report.get("golden"), label="Phase 1B Golden binding")
    pin = _mapping(golden.get("pin"), label="Phase 1B Golden pin binding")
    if (
        golden.get("root") != config.golden.golden_root_hash
        or golden.get("run_id") != config.golden.run_id
        or golden.get("source_sha256") != config.golden.source_sha256
        or pin.get("sha256") != config.golden.pin_sha256
    ):
        raise Phase1CPreflightError("Phase 1B report Golden authority binding differs")

    required_complete_keys = {
        "certifier_code_sha256",
        "certifier_configuration_sha256",
        "certifier_runtime_environment_sha256",
        "format",
        "golden_root",
        "golden_pin_sha256",
        "manifest_root",
        "report_sha256",
        "status",
    }
    if set(complete) != required_complete_keys:
        raise Phase1CPreflightError("Phase 1B COMPLETE schema is incomplete or unexpected")
    for key in (
        "certifier_code_sha256",
        "certifier_configuration_sha256",
        "certifier_runtime_environment_sha256",
    ):
        value = complete.get(key)
        if not isinstance(value, str):
            raise Phase1CPreflightError(f"Phase 1B COMPLETE {key} is absent")
        try:
            _require_digest(value, label=f"Phase 1B COMPLETE {key}")
        except ValueError as error:
            raise Phase1CPreflightError(str(error)) from error
    if (
        complete.get("format") != PHASE1B_COMPLETE_FORMAT
        or complete.get("status") != "COMPLETE"
        or complete.get("report_sha256") != expected.report_sha256
        or complete.get("manifest_root") != expected.manifest_root
        or complete.get("golden_root") != config.golden.golden_root_hash
        or complete.get("golden_pin_sha256") != config.golden.pin_sha256
    ):
        raise Phase1CPreflightError("Phase 1B COMPLETE does not authenticate the pinned report")
    root_tree = _tree_byte_witness(root, label="Phase 1B retry-02 tree")
    return Phase1BProofWitness(
        root=root,
        root_tree=root_tree,
        report=report_witness,
        complete=complete_witness,
        status=cast(str, report["status"]),
        report_sha256=report_witness.sha256,
        manifest_root=cast(str, audit["manifest_root"]),
        final_prefix_root=cast(str, audit["final_prefix_root"]),
        storage_v4_store_bytes=storage_v4_store_bytes,
        anchor_bytes=anchor_bytes,
        compatibility_segment_bytes=compatibility_segment_bytes,
    )


def _freeze_capacity_target(config: Phase1CPreflightConfig) -> CapacityTargetWitness:
    payload, roadmap_witness = _stable_file_bytes(config.roadmap_path, label="Phase 1C roadmap")
    if (
        config.expected_roadmap_sha256 is not None
        and roadmap_witness.sha256 != config.expected_roadmap_sha256
    ):
        raise Phase1CPreflightError("roadmap SHA-256 differs from the pinned value")
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise Phase1CPreflightError("roadmap is not strict UTF-8") from error

    canonical_lines: list[RoadmapTargetLineWitness] = []
    primary: list[RoadmapTargetLineWitness] = []
    numeric_target_lines: list[int] = []
    target_pattern = re.compile(r"<(?P<value>[0-9]+(?:\.[0-9]+)?) GiB/h")
    threshold = Decimal("0.20")
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        decoded_line = html.unescape(raw_line)
        canonical = PHASE1C_TARGET_PHRASE in decoded_line
        if canonical:
            line = RoadmapTargetLineWitness(line_number, raw_line, decoded_line)
            canonical_lines.append(line)
            if all(marker in decoded_line for marker in PHASE1C_TARGET_CONTEXT):
                primary.append(line)
        for match in target_pattern.finditer(decoded_line):
            numeric_target_lines.append(line_number)
            try:
                value = Decimal(match.group("value"))
            except ArithmeticError as error:
                raise Phase1CPreflightError("roadmap contains an invalid GiB/h target") from error
            if value != threshold:
                raise Phase1CPreflightError("roadmap contains inconsistent Storage v4 GiB/h targets")
    if len(canonical_lines) != config.expected_canonical_target_occurrences:
        raise Phase1CPreflightError(
            "roadmap canonical <0.20 GiB/h occurrence count differs from the pinned value"
        )
    if len(primary) != 1:
        raise Phase1CPreflightError(
            "roadmap must contain exactly one primary Storage v4 capacity target line"
        )
    selected = primary[0]
    if selected.decoded_line_text.count(PHASE1C_TARGET_PHRASE) != 1:
        raise Phase1CPreflightError("primary Storage v4 target line has an ambiguous threshold")
    if (
        config.expected_target_line_number is not None
        and selected.line_number != config.expected_target_line_number
    ):
        raise Phase1CPreflightError("roadmap target line number differs from the pinned value")
    if not numeric_target_lines:
        raise Phase1CPreflightError("roadmap has no numeric Storage v4 GiB/h target")
    if f"<{threshold:.2f} GiB/h" != PHASE1C_TARGET_PHRASE:
        raise AssertionError("internal Phase 1C target parser invariant failed")
    return CapacityTargetWitness(
        roadmap=roadmap_witness,
        line_number=selected.line_number,
        raw_line_text=selected.raw_line_text,
        decoded_line_text=selected.decoded_line_text,
        phrase=PHASE1C_TARGET_PHRASE,
        comparator="LT",
        threshold_gib_per_hour=f"{threshold:.2f}",
        margin_required=True,
        corroborating_lines=tuple(canonical_lines),
        consistent_numeric_target_line_numbers=tuple(numeric_target_lines),
    )


def _validate_mission_root(
    config: Phase1CPreflightConfig,
    *,
    require_absent: bool,
) -> tuple[Path, Path]:
    allowed_parent = _resolved_existing_directory(
        config.allowed_parent,
        label="Phase 1C allowed parent",
    )
    leaf = config.mission_root.name
    normalized_reserved_leaf = leaf.rstrip(" .").split(".", maxsplit=1)[0].upper()
    if (
        leaf in {"", ".", ".."}
        or ":" in leaf
        or "\x00" in leaf
        or leaf.endswith((" ", "."))
        or normalized_reserved_leaf in _WINDOWS_RESERVED_LEAVES
    ):
        raise Phase1CPreflightError("Phase 1C mission root has no safe leaf name")
    try:
        candidate_parent = config.mission_root.parent.resolve(strict=True)
    except OSError as error:
        raise Phase1CPreflightError("Phase 1C mission root parent does not exist") from error
    if candidate_parent != allowed_parent:
        raise Phase1CPreflightError("Phase 1C mission root must be a direct child of allowed_parent")
    mission_root = allowed_parent / leaf
    _reject_reparse_components(mission_root, label="Phase 1C mission root")
    if require_absent and os.path.lexists(mission_root):
        raise Phase1CPreflightError("Phase 1C mission root already exists or is ambiguous")
    if not require_absent and os.path.lexists(mission_root):
        _resolved_existing_directory(mission_root, label="Phase 1C mission root")
    return mission_root, allowed_parent


def _reject_output_input_overlap(config: Phase1CPreflightConfig, mission_root: Path) -> None:
    input_directories = (
        _resolved_existing_directory(
            config.golden_certification_root,
            label="Golden certification root",
        ),
        _resolved_existing_directory(config.golden_export_root, label="Golden export root"),
        _resolved_existing_directory(config.phase1b_root, label="Phase 1B root"),
    )
    for directory in input_directories:
        try:
            mission_root.relative_to(directory)
        except ValueError:
            continue
        raise Phase1CPreflightError("Phase 1C mission root would overlap a read-only input tree")


def _collect_external(
    config: Phase1CPreflightConfig,
) -> tuple[Phase1CExternalWitness, GoldenVerification, GoldenCertificationVerification]:
    golden_witness, golden, certification = _verify_golden_authority(config)
    phase1b_witness = _verify_phase1b_proof(config)
    target_witness = _freeze_capacity_target(config)
    return (
        Phase1CExternalWitness(
            golden=golden_witness,
            phase1b=phase1b_witness,
            capacity_target=target_witness,
        ),
        golden,
        certification,
    )


def run_phase1c_preflight(config: Phase1CPreflightConfig) -> Phase1CPreflightResult:
    """Verify all Phase 1C authorities without creating the mission root."""

    if type(config) is not Phase1CPreflightConfig:
        raise TypeError("config must be Phase1CPreflightConfig")
    mission_root, allowed_parent = _validate_mission_root(config, require_absent=True)
    _reject_output_input_overlap(config, mission_root)
    try:
        observed_free_bytes = int(shutil.disk_usage(allowed_parent).free)
    except OSError as error:
        raise Phase1CPreflightError("free space for Phase 1C allowed parent is unavailable") from error
    if observed_free_bytes < config.minimum_free_bytes:
        raise Phase1CPreflightError(
            "Phase 1C allowed parent has less free space than minimum_free_bytes"
        )
    external, golden, certification = _collect_external(config)
    if os.path.lexists(mission_root):
        raise Phase1CPreflightError("Phase 1C mission root appeared during preflight")
    witness = Phase1CPreflightWitness(
        mission_root=mission_root,
        allowed_parent=allowed_parent,
        mission_root_state="ABSENT_FRESH",
        minimum_free_bytes=config.minimum_free_bytes,
        observed_free_bytes=observed_free_bytes,
        external=external,
    )
    return Phase1CPreflightResult(
        witness=witness,
        golden_verification=golden,
        certification_verification=certification,
    )


def verify_phase1c_postflight(
    config: Phase1CPreflightConfig,
    baseline: Phase1CPreflightWitness,
) -> Phase1CPostflightVerification:
    """Reverify external authorities and prove their complete trees unchanged."""

    if type(config) is not Phase1CPreflightConfig:
        raise TypeError("config must be Phase1CPreflightConfig")
    if type(baseline) is not Phase1CPreflightWitness:
        raise TypeError("baseline must be Phase1CPreflightWitness")
    mission_root, allowed_parent = _validate_mission_root(config, require_absent=False)
    if mission_root != baseline.mission_root or allowed_parent != baseline.allowed_parent:
        raise Phase1CPreflightError("postflight mission path differs from the preflight witness")
    external, _golden, _certification = _collect_external(config)
    if external != baseline.external:
        raise Phase1CPreflightError(
            "Golden certification/export/pin, Phase 1B, or roadmap bytes changed after preflight"
        )
    return Phase1CPostflightVerification(mission_root=mission_root, external=external)


__all__ = [
    "DEFAULT_MINIMUM_FREE_BYTES",
    "PHASE1C_POSTFLIGHT_STATUS",
    "PHASE1C_PREFLIGHT_STATUS",
    "PHASE1C_TARGET_CONTEXT",
    "PHASE1C_TARGET_PHRASE",
    "CapacityTargetWitness",
    "FileByteWitness",
    "GoldenAuthorityWitness",
    "Phase1BProofExpectations",
    "Phase1BProofWitness",
    "Phase1CExternalWitness",
    "Phase1CGoldenExpectations",
    "Phase1CPostflightVerification",
    "Phase1CPreflightConfig",
    "Phase1CPreflightError",
    "Phase1CPreflightResult",
    "Phase1CPreflightWitness",
    "RoadmapTargetLineWitness",
    "TreeByteWitness",
    "run_phase1c_preflight",
    "verify_phase1c_postflight",
]
