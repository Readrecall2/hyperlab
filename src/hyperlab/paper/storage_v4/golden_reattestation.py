"""Strict read-only reattestation of a completed Golden native store.

``CURRENT`` is discovery-only. External anchors and authenticated manifest
chains remain authoritative, and this module never repairs or publishes into
the producer tree.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, NoReturn, cast

from hyperlab.paper.golden_v3 import (
    GOLDEN_STREAM_NAMES,
    GoldenVerification,
    iter_golden_stream,
)

from .anchor import AnchorRecord, LocalAnchor
from .candidate_tree import CandidateTreeWitness, witness_candidate_tree
from .canonical import canonical_json_bytes
from .checkpoint import checkpoint_state_sha256
from .contracts import RawLakeId, StorageMode
from .golden_native import (
    GOLDEN_NATIVE_INPUT_TYPE,
    GoldenNativeCheckpointWitness,
    GoldenNativeDifferentialResult,
    GoldenStreamFactory,
    compare_golden_native_checkpoint_witnesses_exact,
)
from .manifest import Manifest, manifest_from_bytes
from .native_journal import (
    NativeAuditExpectations,
    NativeAuditReport,
    NativeCheckpointBinding,
    NativeStreamExpectation,
    audit_native_frames,
    unbind_native_checkpoint_state,
)
from .overlay import OverlayState, OverlayThresholds, SQLiteOverlay
from .raw_manifest import RawManifest, raw_manifest_from_bytes
from .raw_store import (
    RAW_CURRENT_FORMAT_VERSION,
    DiskRawResolver,
    RawAuditReport,
    RawCurrentStatus,
    RawPendingStatus,
    RawStartupReport,
    RawStore,
    RawStoreConfig,
    RawStorePaths,
)
from .raw_store import (
    _current_bytes as _raw_current_bytes,
)
from .raw_store import (
    _parse_current as _parse_raw_current_bytes,
)
from .repository import (
    CHECKPOINT_SUFFIX,
    MANIFEST_SUFFIX,
    SEGMENT_SUFFIX,
    AuditReport,
    CurrentCacheStatus,
    CurrentRecord,
    RepositoryConfig,
    RepositoryPaths,
    StartupReport,
    StorageRepository,
    _overlay_identity,
)
from .repository import (
    _current_bytes as _paper_current_bytes,
)
from .repository import (
    _current_from_bytes as _parse_paper_current_bytes,
)
from .segment import CodecProfile
from .startup_trace import (
    StartupFileAccessTrace,
    StartupTracePaths,
    trace_startup_file_access,
)
from .types import Hash32, RunId, StoreId, StreamId

GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1 = "GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1"
GOLDEN_NATIVE_PRODUCER_STATUS = "STORAGE_V4_PHASE_1C_GOLDEN_NATIVE_EXACT"
GOLDEN_NATIVE_REATTESTATION_METRICS_STATUS = "REATTESTED_NOT_RECOVERED_ORIGINAL"

_MAX_CURRENT_BYTES = 64 * 1024
_MAX_MANIFEST_BYTES = 64 * 1024 * 1024
_MAX_LOG_BYTES = 256 * 1024 * 1024


class GoldenNativeReattestationError(RuntimeError):
    """The imported producer tree is ambiguous, mutable, or not Golden-exact."""


def _require_sha256(value: str, *, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be one lowercase SHA-256")
    return value


def _is_reparse(value: os.stat_result) -> bool:
    attributes = int(getattr(value, "st_file_attributes", 0))
    mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & mask)


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (
        int(value.st_dev),
        int(value.st_ino),
        int(value.st_size),
        int(value.st_mtime_ns),
    )


@dataclass(frozen=True, slots=True)
class ReattestedFileWitness:
    path: Path
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        if not self.path.is_absolute():
            raise ValueError("reattested file path must be absolute")
        if type(self.size_bytes) is not int or self.size_bytes < 0:
            raise ValueError("reattested file size must be a non-negative integer")
        _require_sha256(self.sha256, label="reattested file")

    def payload(self) -> dict[str, object]:
        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


def _read_regular_stable(
    path: Path,
    *,
    maximum_bytes: int,
) -> tuple[bytes, ReattestedFileWitness]:
    if not path.is_absolute():
        raise GoldenNativeReattestationError("reattested input path is not absolute")
    try:
        before = os.lstat(path)
    except OSError as error:
        raise GoldenNativeReattestationError(f"reattested input is missing: {path}") from error
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or _is_reparse(before)
        or int(before.st_size) > maximum_bytes
    ):
        raise GoldenNativeReattestationError(
            f"reattested input is unsafe or exceeds its bound: {path}"
        )
    flags = os.O_RDONLY
    for name in ("O_BINARY", "O_NOINHERIT", "O_NOFOLLOW"):
        flags |= int(getattr(os, name, 0))
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise GoldenNativeReattestationError(
            f"reattested input cannot be opened safely: {path}"
        ) from error
    chunks: list[bytes] = []
    try:
        opened_before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened_before.st_mode)
            or _is_reparse(opened_before)
            or not os.path.samestat(before, opened_before)
        ):
            raise GoldenNativeReattestationError(
                f"reattested descriptor differs from its path: {path}"
            )
        total = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            total += len(chunk)
            if total > maximum_bytes:
                raise GoldenNativeReattestationError(
                    f"reattested input grew beyond its bound: {path}"
                )
            chunks.append(chunk)
        opened_after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        after = os.lstat(path)
    except OSError as error:
        raise GoldenNativeReattestationError(
            f"reattested input disappeared during read: {path}"
        ) from error
    if (
        not stat.S_ISREG(after.st_mode)
        or stat.S_ISLNK(after.st_mode)
        or _is_reparse(after)
        or len(
            {
                _stat_identity(before),
                _stat_identity(opened_before),
                _stat_identity(opened_after),
                _stat_identity(after),
            }
        )
        != 1
    ):
        raise GoldenNativeReattestationError(f"reattested input changed during read: {path}")
    data = b"".join(chunks)
    return data, ReattestedFileWitness(
        path=path,
        size_bytes=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )


@dataclass(frozen=True, slots=True)
class GoldenNativeReattestationConfig:
    candidate_root: Path
    producer_stdout_log: Path
    producer_stderr_log: Path
    producer_stdout_sha256: str
    producer_stderr_sha256: str
    reattestor_code_identity: Hash32
    reattestor_runtime_identity: Hash32
    expected_commits: int = 252_262
    expected_rows: int = 1_011_362
    expected_streams: int = 13
    expected_market_gaps: int = 1

    def __post_init__(self) -> None:
        for label, path in (
            ("candidate_root", self.candidate_root),
            ("producer_stdout_log", self.producer_stdout_log),
            ("producer_stderr_log", self.producer_stderr_log),
        ):
            if not isinstance(path, Path) or not path.is_absolute():
                raise ValueError(f"{label} must be an absolute pathlib.Path")
        for log in (self.producer_stdout_log, self.producer_stderr_log):
            try:
                log.relative_to(self.candidate_root)
            except ValueError:
                pass
            else:
                raise ValueError("producer logs must remain outside the candidate tree")
        _require_sha256(self.producer_stdout_sha256, label="producer stdout")
        _require_sha256(self.producer_stderr_sha256, label="producer stderr")
        for identity in (self.reattestor_code_identity, self.reattestor_runtime_identity):
            if type(identity) is not Hash32:
                raise TypeError("reattestor identities must be Hash32")
        for label, value, minimum in (
            ("expected_commits", self.expected_commits, 1),
            ("expected_rows", self.expected_rows, 1),
            ("expected_streams", self.expected_streams, 1),
            ("expected_market_gaps", self.expected_market_gaps, 0),
        ):
            if type(value) is not int or value < minimum:
                raise ValueError(f"{label} is invalid")


@dataclass(frozen=True, slots=True)
class GoldenNativeProducerWitness:
    stdout: ReattestedFileWitness
    stderr: ReattestedFileWitness
    terminal_record: Mapping[str, object]
    paper_run_identity: Hash32
    paper_config_identity: Hash32
    producer_code_identity: Hash32
    producer_runtime_identity: Hash32
    inferred_batch_size: int

    def payload(self) -> dict[str, object]:
        return {
            "inferred_batch_size": self.inferred_batch_size,
            "logs": {
                "stderr": self.stderr.payload(),
                "stdout": self.stdout.payload(),
                "terminal_record": dict(self.terminal_record),
                "terminal_record_status": "UNIQUE_CANONICAL_PRODUCER_TERMINAL",
            },
            "manifest_identities": {
                "code_identity": self.producer_code_identity.hex(),
                "config_identity": self.paper_config_identity.hex(),
                "run_identity": self.paper_run_identity.hex(),
                "runtime_identity": self.producer_runtime_identity.hex(),
            },
        }


@dataclass(frozen=True, slots=True)
class GoldenNativeReattestationResult:
    candidate_root: Path
    candidate_tree_before: CandidateTreeWitness
    candidate_tree_after: CandidateTreeWitness
    raw_config: RawStoreConfig
    paper_config: RepositoryConfig
    producer: GoldenNativeProducerWitness
    reattestor_code_identity: Hash32
    reattestor_runtime_identity: Hash32
    raw_startup: RawStartupReport
    paper_startup: StartupReport
    startup_file_trace: StartupFileAccessTrace
    raw_audit: RawAuditReport
    paper_audit: AuditReport
    native_audit: NativeAuditReport
    checkpoint_witnesses: tuple[GoldenNativeCheckpointWitness, ...]
    differential: GoldenNativeDifferentialResult
    elapsed_ns: int

    def payload(self) -> dict[str, object]:
        return {
            "artifact": GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1,
            "candidate_root": str(self.candidate_root),
            "counts": {
                "audited_commits": self.native_audit.commit_count,
                "ingested_commits": 0,
                "prefix_reingested_commits": 0,
            },
            "differential": dict(self.differential.report),
            "integrity": {
                "candidate_tree": self.candidate_tree_before.payload(),
                "candidate_tree_unchanged": self.candidate_tree_before == self.candidate_tree_after,
                "checkpoint_witness_count": len(self.checkpoint_witnesses),
                "final_prefix_root": self.native_audit.final_prefix_root.hex(),
                "paper_manifest_root": self.paper_audit.manifest_root.hex(),
                "raw_manifest_root": self.checkpoint_witnesses[-1].raw_manifest_root.hex(),
            },
            "markers": [
                "PAPER_ONLY",
                "TECHNICAL_STORAGE_REPLAY_EVIDENCE",
                "NOT_ECONOMIC_EVIDENCE",
                "READ_ONLY_IMPORTED_CANDIDATE",
            ],
            "measurements": {
                "elapsed_ns": self.elapsed_ns,
                "status": GOLDEN_NATIVE_REATTESTATION_METRICS_STATUS,
            },
            "producer": self.producer.payload(),
            "reattestor": {
                "code_identity": self.reattestor_code_identity.hex(),
                "runtime_identity": self.reattestor_runtime_identity.hex(),
            },
            "startup": {
                "file_access_trace": self.startup_file_trace.payload(),
                "historical_segment_open_count": self.startup_file_trace.historical_segment_open_count,
                "paper_checkpoint_used": self.paper_startup.checkpoint_used,
                "paper_segments_read": self.paper_startup.segments_read,
                "paper_tail_entries_replayed": self.paper_startup.tail_entries_replayed,
                "raw_historical_segments_read": self.raw_startup.historical_segments_read,
            },
            "status": GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1,
        }


@dataclass(slots=True)
class _ReadOnlyLease:
    closed: bool = False

    def close(self) -> None:
        self.closed = True


class _ReadOnlyStorageRepository(StorageRepository):
    """StorageRepository reader whose startup validates but never repairs."""

    def startup(self) -> StartupReport:
        self._ensure_open()
        manifest = self._manifest
        checkpoint = self._checkpoint
        if manifest is None or checkpoint is None:
            raise GoldenNativeReattestationError(
                "read-only Golden startup requires anchored history"
            )
        expected_anchor = AnchorRecord(
            store_id=self._config.store_id,
            generation=manifest.generation,
            manifest_root=manifest.identity.root,
        )
        if self._anchor.read() != expected_anchor:
            raise GoldenNativeReattestationError("Paper anchor changed during read-only startup")
        observed_manifest = self._read_manifest(self._paths, expected_anchor)
        self._verify_manifest_config(observed_manifest, expected_anchor, self._config)
        observed_checkpoint = self._read_checkpoint(
            self._paths,
            observed_manifest,
            self._config,
        )
        expected_current = _paper_current_bytes(
            CurrentRecord(
                store_id=self._config.store_id,
                generation=observed_manifest.generation,
                manifest_root=observed_manifest.identity.root,
            )
        )
        current, _witness = _read_regular_stable(
            self._paths.current,
            maximum_bytes=_MAX_CURRENT_BYTES,
        )
        if current != expected_current:
            raise GoldenNativeReattestationError(
                "Paper CURRENT is not the exact anchored authority cache"
            )
        state = self._overlay.verify_integrity()
        _require_sealed_overlay(state, observed_manifest)
        if self._overlay.frames():
            raise GoldenNativeReattestationError(
                "imported Golden overlay contains an unpublished tail"
            )
        self._manifest = observed_manifest
        self._checkpoint = observed_checkpoint
        return self._build_startup_report(CurrentCacheStatus.EXACT)


@dataclass(frozen=True, slots=True)
class _DiscoveredAuthorities:
    raw_paths: RawStorePaths
    paper_paths: RepositoryPaths
    raw_config: RawStoreConfig
    paper_config: RepositoryConfig
    raw_manifest: RawManifest
    paper_manifest: Manifest
    inferred_batch_size: int


def _codec_from_profile_id(value: str) -> CodecProfile:
    if value == "raw-v1":
        return CodecProfile.raw()
    prefix = "zlib-v1-level-"
    if value.startswith(prefix):
        suffix = value[len(prefix) :]
        if suffix and suffix.isascii() and suffix.isdecimal():
            return CodecProfile.zlib(level=int(suffix))
    raise GoldenNativeReattestationError(
        f"unsupported producer codec profile {value!r}"
    )


def _exact_int(value: object, *, label: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise GoldenNativeReattestationError(f"{label} is invalid")
    return value


def _raw_current_value(data: bytes) -> dict[str, object]:
    try:
        value = _parse_raw_current_bytes(data)
    except (TypeError, UnicodeError, ValueError) as error:
        raise GoldenNativeReattestationError("raw CURRENT is invalid") from error
    if set(value) != {
        "config_identity",
        "format_version",
        "generation",
        "lake_id",
        "manifest_root",
        "store_id",
    } or value["format_version"] != RAW_CURRENT_FORMAT_VERSION:
        raise GoldenNativeReattestationError("raw CURRENT contract differs")
    return value


def _producer_config_identity(
    verification: GoldenVerification,
    *,
    batch_size: int,
    code_identity: Hash32,
    runtime_identity: Hash32,
) -> Hash32:
    material = canonical_json_bytes(
        {
            "batch_size": batch_size,
            "code_identity": code_identity.hex(),
            "format": "hyperlab.storage_v4.phase1c.golden_native_config.v1",
            "golden_root": verification.root_hash,
            "run_id": verification.manifest.get("run_id"),
            "runtime_identity": runtime_identity.hex(),
        }
    )
    return Hash32(hashlib.sha256(material).digest())


def _infer_batch_size(manifest: Manifest) -> int:
    sizes = tuple(int(descriptor.commit_count) for descriptor in manifest.segments)
    if not sizes or sizes[0] < 1:
        raise GoldenNativeReattestationError("Paper manifest has no producer batch size")
    batch_size = sizes[0]
    if any(size != batch_size for size in sizes[:-1]) or not 1 <= sizes[-1] <= batch_size:
        raise GoldenNativeReattestationError(
            "Paper segment boundaries do not identify one fixed producer batch size"
        )
    return batch_size


def _discover_authorities(
    config: GoldenNativeReattestationConfig,
    verification: GoldenVerification,
) -> _DiscoveredAuthorities:
    raw_paths = RawStorePaths.from_root(config.candidate_root / "raw")
    paper_paths = RepositoryPaths.from_root(config.candidate_root / "paper")
    raw_current, _raw_current_witness = _read_regular_stable(
        raw_paths.current,
        maximum_bytes=_MAX_CURRENT_BYTES,
    )
    raw_value = _raw_current_value(raw_current)
    try:
        raw_current_root = Hash32.from_hex(cast(str, raw_value["manifest_root"]))
        raw_store_id = StoreId(cast(str, raw_value["store_id"]))
        raw_lake_id = RawLakeId(cast(str, raw_value["lake_id"]))
        raw_config_identity = Hash32.from_hex(cast(str, raw_value["config_identity"]))
    except (TypeError, ValueError) as error:
        raise GoldenNativeReattestationError(
            "raw CURRENT typed discovery fields are invalid"
        ) from error
    raw_generation = _exact_int(
        raw_value["generation"],
        label="raw CURRENT generation",
        minimum=1,
    )
    raw_manifest_data, _raw_manifest_witness = _read_regular_stable(
        raw_paths.manifest_path(raw_current_root),
        maximum_bytes=_MAX_MANIFEST_BYTES,
    )
    try:
        raw_manifest = raw_manifest_from_bytes(raw_manifest_data)
    except (TypeError, ValueError) as error:
        raise GoldenNativeReattestationError(
            "raw CURRENT target manifest cannot be authenticated"
        ) from error
    if (
        raw_manifest.root != raw_current_root
        or raw_manifest.generation != raw_generation
        or raw_manifest.store_id != raw_store_id
        or raw_manifest.lake_id != raw_lake_id
        or raw_manifest.config_identity != raw_config_identity
    ):
        raise GoldenNativeReattestationError(
            "raw CURRENT discovery differs from its manifest"
        )
    raw_codecs = {descriptor.codec_profile for descriptor in raw_manifest.segments}
    if len(raw_codecs) != 1:
        raise GoldenNativeReattestationError("raw producer codec profile is ambiguous")
    raw_config = RawStoreConfig(
        store_id=raw_manifest.store_id,
        lake_id=raw_manifest.lake_id,
        config_identity=raw_manifest.config_identity,
        codec_profile=next(iter(raw_codecs)),
    )
    if raw_current != _raw_current_bytes(raw_config, raw_manifest):
        raise GoldenNativeReattestationError("raw CURRENT bytes are not exact")

    paper_current, _paper_current_witness = _read_regular_stable(
        paper_paths.current,
        maximum_bytes=_MAX_CURRENT_BYTES,
    )
    try:
        paper_record = _parse_paper_current_bytes(paper_current)
    except (TypeError, ValueError) as error:
        raise GoldenNativeReattestationError("Paper CURRENT is invalid") from error
    paper_manifest_data, _paper_manifest_witness = _read_regular_stable(
        paper_paths.manifest_path(paper_record.manifest_root),
        maximum_bytes=_MAX_MANIFEST_BYTES,
    )
    try:
        paper_manifest = manifest_from_bytes(paper_manifest_data)
    except (TypeError, ValueError) as error:
        raise GoldenNativeReattestationError(
            "Paper CURRENT target manifest cannot be authenticated"
        ) from error
    if (
        paper_manifest.identity.root != paper_record.manifest_root
        or paper_manifest.generation != paper_record.generation
        or paper_manifest.store_id != paper_record.store_id
    ):
        raise GoldenNativeReattestationError(
            "Paper CURRENT discovery differs from its manifest"
        )
    profile_ids = {descriptor.codec_profile for descriptor in paper_manifest.segments}
    if len(profile_ids) != 1:
        raise GoldenNativeReattestationError("Paper producer codec profile is ambiguous")
    paper_codec = _codec_from_profile_id(next(iter(profile_ids)))
    inferred_batch_size = _infer_batch_size(paper_manifest)
    run_value = verification.manifest.get("run_id")
    if type(run_value) is not str or paper_manifest.run_id != RunId(run_value):
        raise GoldenNativeReattestationError(
            "Paper producer run differs from the certified Golden manifest"
        )
    golden_root = Hash32.from_hex(verification.root_hash)
    if paper_manifest.start_prefix_root != golden_root:
        raise GoldenNativeReattestationError(
            "Paper producer start prefix differs from the certified Golden root"
        )
    expected_run_identity = Hash32(
        hashlib.sha256(b"HL4-GOLDEN-RUN\x00" + run_value.encode()).digest()
    )
    if paper_manifest.run_identity.digest != expected_run_identity:
        raise GoldenNativeReattestationError("Paper producer run identity differs")
    expected_config_identity = _producer_config_identity(
        verification,
        batch_size=inferred_batch_size,
        code_identity=paper_manifest.code_identity.digest,
        runtime_identity=paper_manifest.runtime_identity.digest,
    )
    if (
        paper_manifest.config_identity.digest != expected_config_identity
        or raw_config.config_identity != expected_config_identity
    ):
        raise GoldenNativeReattestationError(
            "producer config identity does not bind Golden/code/runtime/batch"
        )
    paper_config = RepositoryConfig(
        store_id=paper_manifest.store_id,
        run_id=paper_manifest.run_id,
        mode=StorageMode.V4_NATIVE,
        run_identity=paper_manifest.run_identity,
        config_identity=paper_manifest.config_identity,
        code_identity=paper_manifest.code_identity,
        runtime_identity=paper_manifest.runtime_identity,
        start_prefix_root=paper_manifest.start_prefix_root,
        thresholds=OverlayThresholds(),
        codec_profile=paper_codec,
    )
    if paper_current != _paper_current_bytes(paper_record):
        raise GoldenNativeReattestationError("Paper CURRENT bytes are not exact")
    return _DiscoveredAuthorities(
        raw_paths=raw_paths,
        paper_paths=paper_paths,
        raw_config=raw_config,
        paper_config=paper_config,
        raw_manifest=raw_manifest,
        paper_manifest=paper_manifest,
        inferred_batch_size=inferred_batch_size,
    )


def _require_sealed_overlay(state: OverlayState, manifest: Manifest) -> None:
    if (
        state.base_manifest_generation != manifest.generation
        or state.base_manifest_root != manifest.identity.root
        or state.base_commit_sequence != manifest.head.commit_sequence
        or state.base_prefix_root != manifest.head.prefix_root
        or state.head_commit_sequence != manifest.head.commit_sequence
        or state.head_prefix_root != manifest.head.prefix_root
        or state.tail_commit_count != 0
        or state.tail_row_count != 0
        or state.tail_bytes != 0
    ):
        raise GoldenNativeReattestationError(
            "Paper overlay is not the exact empty tail at anchored authority"
        )


def _assert_exact_entries(
    directory: Path,
    expected: Mapping[str, str],
    *,
    label: str,
) -> None:
    try:
        entries = tuple(directory.iterdir())
    except OSError as error:
        raise GoldenNativeReattestationError(f"{label} is unreadable") from error
    if {entry.name for entry in entries} != set(expected):
        raise GoldenNativeReattestationError(f"{label} contains a missing or extra entry")
    for entry in entries:
        try:
            value = os.lstat(entry)
        except OSError as error:
            raise GoldenNativeReattestationError(
                f"{label} entry cannot be inspected"
            ) from error
        kind = expected[entry.name]
        if (
            stat.S_ISLNK(value.st_mode)
            or _is_reparse(value)
            or (kind == "file" and not stat.S_ISREG(value.st_mode))
            or (kind == "directory" and not stat.S_ISDIR(value.st_mode))
        ):
            raise GoldenNativeReattestationError(f"{label} contains an unsafe entry")


def _assert_candidate_layout(
    config: GoldenNativeReattestationConfig,
    discovered: _DiscoveredAuthorities,
    raw_anchor: LocalAnchor,
    paper_anchor: LocalAnchor,
) -> None:
    root = config.candidate_root
    _assert_exact_entries(
        root,
        {
            "anchors": "directory",
            "paper": "directory",
            "raw": "directory",
            "staging": "directory",
        },
        label="Golden candidate root",
    )
    _assert_exact_entries(
        root / "anchors",
        {
            "paper.sqlite3": "file",
            "raw.sqlite3": "file",
            paper_anchor.writer_lease_path.name: "file",
            raw_anchor.writer_lease_path.name: "file",
        },
        label="Golden anchor namespace",
    )
    _assert_exact_entries(root / "staging", {}, label="Golden staging namespace")
    _assert_exact_entries(
        discovered.raw_paths.root,
        {"CURRENT": "file", "manifests": "directory", "segments": "directory"},
        label="Golden raw root",
    )
    _assert_exact_entries(
        discovered.paper_paths.root,
        {
            "CURRENT": "file",
            "checkpoints": "directory",
            "manifests": "directory",
            "overlay.sqlite3": "file",
            "segments": "directory",
            discovered.paper_paths.writer_lease.name: "file",
        },
        label="Golden Paper root",
    )


def _open_read_only_stores(
    config: GoldenNativeReattestationConfig,
    discovered: _DiscoveredAuthorities,
) -> tuple[RawStore, _ReadOnlyStorageRepository, LocalAnchor, LocalAnchor]:
    anchors = config.candidate_root / "anchors"
    raw_anchor = LocalAnchor.open_existing_read_only(
        anchors / "raw.sqlite3",
        store_id=discovered.raw_config.store_id,
    )
    paper_anchor = LocalAnchor.open_existing_read_only(
        anchors / "paper.sqlite3",
        store_id=discovered.paper_config.store_id,
    )
    _assert_candidate_layout(config, discovered, raw_anchor, paper_anchor)
    expected_raw_anchor = AnchorRecord(
        store_id=discovered.raw_config.store_id,
        generation=discovered.raw_manifest.generation,
        manifest_root=discovered.raw_manifest.root,
    )
    if raw_anchor.read() != expected_raw_anchor:
        raise GoldenNativeReattestationError(
            "raw anchor differs from the CURRENT-discovered manifest"
        )
    raw_manifest = RawStore._read_manifest(
        discovered.raw_paths,
        expected_raw_anchor.manifest_root,
        discovered.raw_config,
    )
    if raw_manifest != discovered.raw_manifest:
        raise GoldenNativeReattestationError("raw authority changed during startup")
    raw_current, _raw_current_witness = _read_regular_stable(
        discovered.raw_paths.current,
        maximum_bytes=_MAX_CURRENT_BYTES,
    )
    if raw_current != _raw_current_bytes(discovered.raw_config, raw_manifest):
        raise GoldenNativeReattestationError("raw CURRENT changed during startup")
    if discovered.raw_paths.pending.exists():
        raise GoldenNativeReattestationError(
            "read-only Golden candidate contains unresolved raw PENDING"
        )
    raw_store = RawStore(
        paths=discovered.raw_paths,
        anchor=raw_anchor,
        config=discovered.raw_config,
        manifest=raw_manifest,
        anchor_writer_lease=cast(Any, _ReadOnlyLease()),
        fault_hook=None,
        startup_report=RawStartupReport(
            generation=raw_manifest.generation,
            manifest_root=raw_manifest.root,
            current_status=RawCurrentStatus.EXACT,
            adopted_direct_successor=False,
            historical_segments_read=0,
            manifests_opened=1,
            manifest_namespace_entries_scanned=0,
            pending_status=RawPendingStatus.ABSENT,
        ),
    )
    expected_paper_anchor = AnchorRecord(
        store_id=discovered.paper_config.store_id,
        generation=discovered.paper_manifest.generation,
        manifest_root=discovered.paper_manifest.identity.root,
    )
    if paper_anchor.read() != expected_paper_anchor:
        raw_store.close()
        raise GoldenNativeReattestationError(
            "Paper anchor differs from the CURRENT-discovered manifest"
        )
    paper_manifest = StorageRepository._read_manifest(
        discovered.paper_paths,
        expected_paper_anchor,
    )
    StorageRepository._verify_manifest_config(
        paper_manifest,
        expected_paper_anchor,
        discovered.paper_config,
    )
    if paper_manifest != discovered.paper_manifest:
        raw_store.close()
        raise GoldenNativeReattestationError("Paper authority changed during startup")
    checkpoint = StorageRepository._read_checkpoint(
        discovered.paper_paths,
        paper_manifest,
        discovered.paper_config,
    )
    overlay = SQLiteOverlay.open_existing_read_only(
        discovered.paper_paths.overlay,
        expected_identity=_overlay_identity(discovered.paper_config),
    )
    try:
        paper = _ReadOnlyStorageRepository(
            paths=discovered.paper_paths,
            anchor=paper_anchor,
            config=discovered.paper_config,
            overlay=overlay,
            manifest=paper_manifest,
            checkpoint=checkpoint,
            anchor_writer_lease=cast(Any, _ReadOnlyLease()),
            writer_lease=cast(Any, _ReadOnlyLease()),
            fault_hook=None,
            current_cache_status=CurrentCacheStatus.EXACT,
        )
    except BaseException:
        overlay.close()
        raw_store.close()
        raise
    return raw_store, paper, raw_anchor, paper_anchor


def _assert_paper_namespaces(
    repository: _ReadOnlyStorageRepository,
    chain: tuple[Manifest, ...],
) -> None:
    latest = chain[-1]
    expected_manifests = {
        f"{manifest.identity.root.hex()}{MANIFEST_SUFFIX}": "file"
        for manifest in chain
    }
    expected_segments = {
        f"{descriptor.physical_sha256.hex()}{SEGMENT_SUFFIX}": "file"
        for descriptor in latest.segments
    }
    checkpoint_roots = tuple(
        manifest.segments[-1].checkpoint_root for manifest in chain
    )
    if any(root is None for root in checkpoint_roots):
        raise GoldenNativeReattestationError(
            "Paper manifest chain contains an uncheckpointed segment"
        )
    expected_checkpoints = {
        f"{cast(Hash32, root).hex()}{CHECKPOINT_SUFFIX}": "file"
        for root in checkpoint_roots
    }
    _assert_exact_entries(
        repository.paths.manifests,
        expected_manifests,
        label="Paper manifest namespace",
    )
    _assert_exact_entries(
        repository.paths.segments,
        expected_segments,
        label="Paper segment namespace",
    )
    _assert_exact_entries(
        repository.paths.checkpoints,
        expected_checkpoints,
        label="Paper checkpoint namespace",
    )


def _checkpoint_witnesses(
    paper: _ReadOnlyStorageRepository,
    paper_chain: tuple[Manifest, ...],
    raw_chain: tuple[RawManifest, ...],
) -> tuple[
    tuple[GoldenNativeCheckpointWitness, ...],
    NativeCheckpointBinding,
]:
    raw_by_root = {manifest.root: manifest for manifest in raw_chain}
    witnesses: list[GoldenNativeCheckpointWitness] = []
    prior_raw_generation = 0
    terminal_binding: NativeCheckpointBinding | None = None
    for manifest in paper_chain:
        checkpoint = paper._read_checkpoint(paper.paths, manifest, paper.config)
        descriptor = manifest.segments[-1]
        checkpoint_root = descriptor.checkpoint_root
        if checkpoint_root is None or checkpoint.root != checkpoint_root:
            raise GoldenNativeReattestationError(
                "Paper checkpoint root differs from its manifest"
            )
        unbound, binding = unbind_native_checkpoint_state(checkpoint.state)
        raw_manifest = raw_by_root.get(binding.raw_manifest_root)
        if raw_manifest is None:
            raise GoldenNativeReattestationError(
                "Paper checkpoint binds a raw manifest outside anchored history"
            )
        if (
            binding.raw_store_id != raw_manifest.store_id
            or binding.raw_lake_id != raw_manifest.lake_id
            or binding.raw_config_identity != raw_manifest.config_identity
            or binding.raw_generation != raw_manifest.generation
            or binding.raw_record_count != raw_manifest.total_record_count
            or binding.raw_last_record_id != raw_manifest.segments[-1].last_record_id
            or binding.raw_generation <= prior_raw_generation
            or raw_manifest.total_record_count != checkpoint.historical_commit_count
            or raw_manifest.segments[-1].last_arrival_sequence
            != int(checkpoint.covered_commit_sequence)
        ):
            raise GoldenNativeReattestationError(
                "Paper checkpoint/raw authority alignment differs"
            )
        witnesses.append(
            GoldenNativeCheckpointWitness(
                commit_sequence=checkpoint.covered_commit_sequence,
                checkpoint_root=checkpoint_root,
                bound_state_sha256=checkpoint_state_sha256(checkpoint.state),
                unbound_state_sha256=checkpoint_state_sha256(unbound),
                raw_manifest_root=binding.raw_manifest_root,
            )
        )
        prior_raw_generation = binding.raw_generation
        terminal_binding = binding
    if terminal_binding is None or terminal_binding.raw_manifest_root != raw_chain[-1].root:
        raise GoldenNativeReattestationError(
            "terminal Paper checkpoint does not bind terminal raw authority"
        )
    return tuple(witnesses), terminal_binding


def _default_stream_factory(
    verification: GoldenVerification,
    name: str,
) -> Iterable[Mapping[str, object]]:
    return cast(
        Iterable[Mapping[str, object]],
        iter_golden_stream(
            verification.export_root,
            name,
            verification=verification,
        ),
    )


def _native_expectations(
    config: GoldenNativeReattestationConfig,
    verification: GoldenVerification,
    paper_manifest: Manifest,
    raw_chain: tuple[RawManifest, ...],
    terminal_binding: NativeCheckpointBinding,
    stream_factory: GoldenStreamFactory,
) -> NativeAuditExpectations:
    descriptors = verification.manifest.get("streams")
    if type(descriptors) is not dict or set(descriptors) != set(GOLDEN_STREAM_NAMES):
        raise GoldenNativeReattestationError(
            "certified Golden manifest differs from the fixed 13-stream contract"
        )
    streams: list[NativeStreamExpectation] = []
    total_rows = 0
    for name in GOLDEN_STREAM_NAMES:
        descriptor = descriptors.get(name)
        if type(descriptor) is not dict:
            raise GoldenNativeReattestationError(
                f"certified Golden stream {name!r} is missing"
            )
        row_count = _exact_int(
            descriptor.get("row_count"),
            label=f"Golden {name} row count",
        )
        try:
            digest = Hash32.from_hex(cast(str, descriptor.get("logical_sha256")))
        except (TypeError, ValueError) as error:
            raise GoldenNativeReattestationError(
                f"certified Golden stream {name!r} digest is invalid"
            ) from error
        total_rows += row_count
        if row_count:
            streams.append(
                NativeStreamExpectation(
                    stream_id=StreamId(name),
                    row_count=row_count,
                    logical_sha256=digest,
                )
            )
    streams.sort(key=lambda item: item.stream_id.value.encode("utf-8"))
    commit_descriptor = cast(dict[str, object], descriptors["commits"])
    if (
        total_rows != config.expected_rows
        or len(descriptors) != config.expected_streams
        or commit_descriptor.get("row_count") != config.expected_commits
    ):
        raise GoldenNativeReattestationError(
            "certified Golden stream cardinalities differ from the mission pins"
        )
    market_gaps = 0
    for row in stream_factory(verification, "alerts"):
        if not isinstance(row, Mapping):
            raise GoldenNativeReattestationError(
                "certified Golden alerts emitted a non-mapping row"
            )
        if row.get("code") == "MARKET_GAP":
            market_gaps += 1
    if market_gaps != config.expected_market_gaps:
        raise GoldenNativeReattestationError(
            "certified Golden MARKET_GAP count differs from the mission pin"
        )
    if (
        int(paper_manifest.head.commit_sequence) != config.expected_commits
        or terminal_binding.raw_record_count != config.expected_commits
    ):
        raise GoldenNativeReattestationError(
            "authenticated native/raw commit counts differ from Golden"
        )
    return NativeAuditExpectations(
        run_id=paper_manifest.run_id,
        start_prefix_root=paper_manifest.start_prefix_root,
        commit_count=config.expected_commits,
        final_prefix_root=paper_manifest.head.prefix_root,
        streams=tuple(streams),
        market_gap_count=market_gaps,
        raw_reference_count=terminal_binding.raw_record_count,
        raw_manifest_roots=tuple(manifest.root for manifest in raw_chain),
        raw_last_record_id=terminal_binding.raw_last_record_id,
        raw_reference_prefix_root=terminal_binding.raw_reference_prefix_root,
    )


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"unsupported JSON constant {value!r}")


def _producer_terminal(
    config: GoldenNativeReattestationConfig,
    stdout_data: bytes,
    stdout_witness: ReattestedFileWitness,
    stderr_witness: ReattestedFileWitness,
    discovered: _DiscoveredAuthorities,
    raw_audit: RawAuditReport,
    paper_audit: AuditReport,
) -> GoldenNativeProducerWitness:
    terminal_candidates: list[tuple[bytes, dict[str, object]]] = []
    observed_framings: set[bytes] = set()
    for raw_line in stdout_data.splitlines(keepends=True):
        if raw_line.endswith(b"\r\n"):
            framing = b"\r\n"
        elif raw_line.endswith(b"\n"):
            framing = b"\n"
        else:
            raise GoldenNativeReattestationError("producer stdout JSONL framing differs")
        observed_framings.add(framing)
        body = raw_line[: -len(framing)]
        try:
            value = json.loads(
                body.decode("utf-8", errors="strict"),
                parse_constant=_reject_json_constant,
            )
        except (UnicodeError, ValueError) as error:
            raise GoldenNativeReattestationError(
                "producer stdout contains a malformed JSONL record"
            ) from error
        if type(value) is not dict:
            raise GoldenNativeReattestationError(
                "producer stdout contains a non-object JSONL record"
            )
        record = cast(dict[str, object], value)
        if (
            record.get("phase") == "golden_native_complete"
            or record.get("status") == GOLDEN_NATIVE_PRODUCER_STATUS
        ):
            terminal_candidates.append((raw_line, record))
    if len(terminal_candidates) != 1:
        raise GoldenNativeReattestationError(
            "producer stdout has no unique Golden native terminal record"
        )
    if len(observed_framings) != 1:
        raise GoldenNativeReattestationError(
            "producer stdout mixes LF and CRLF framing"
        )
    terminal_line, terminal = terminal_candidates[0]
    framing = next(iter(observed_framings))
    if terminal_line != canonical_json_bytes(terminal) + framing:
        raise GoldenNativeReattestationError(
            "producer Golden terminal record is not canonical JSONL"
        )
    expected_values: dict[str, object] = {
        "checkpoint_count": paper_audit.checkpoints_read,
        "commits_completed": config.expected_commits,
        "commits_total": config.expected_commits,
        "golden_root_hash": discovered.paper_config.start_prefix_root.hex(),
        "logical_rows_completed": config.expected_rows,
        "logical_rows_total": config.expected_rows,
        "paper_segment_count": paper_audit.segments_read,
        "phase": "golden_native_complete",
        "raw_segment_count": raw_audit.segments_read,
        "segment_count": raw_audit.segments_read + paper_audit.segments_read,
        "status": GOLDEN_NATIVE_PRODUCER_STATUS,
        "workload": "GOLDEN_V3_NATIVE",
        "workload_id": discovered.paper_config.start_prefix_root.hex(),
        "workload_profile": GOLDEN_NATIVE_INPUT_TYPE,
    }
    for name, expected in expected_values.items():
        if terminal.get(name) != expected:
            raise GoldenNativeReattestationError(
                f"producer Golden terminal field {name!r} differs"
            )
    for name in ("elapsed_ns", "cpu_ns", "workload_elapsed_ns"):
        _exact_int(terminal.get(name), label=f"producer terminal {name}")
    return GoldenNativeProducerWitness(
        stdout=stdout_witness,
        stderr=stderr_witness,
        terminal_record=MappingProxyType(dict(terminal)),
        paper_run_identity=discovered.paper_manifest.run_identity.digest,
        paper_config_identity=discovered.paper_manifest.config_identity.digest,
        producer_code_identity=discovered.paper_manifest.code_identity.digest,
        producer_runtime_identity=discovered.paper_manifest.runtime_identity.digest,
        inferred_batch_size=discovered.inferred_batch_size,
    )


def _reattest(
    config: GoldenNativeReattestationConfig,
    verification: GoldenVerification,
    *,
    stream_factory: GoldenStreamFactory,
) -> GoldenNativeReattestationResult:
    started = time.perf_counter_ns()
    tree_before = witness_candidate_tree(config.candidate_root)
    stdout_data, stdout_witness = _read_regular_stable(
        config.producer_stdout_log,
        maximum_bytes=_MAX_LOG_BYTES,
    )
    stderr_data, stderr_witness = _read_regular_stable(
        config.producer_stderr_log,
        maximum_bytes=_MAX_LOG_BYTES,
    )
    if (
        stdout_witness.sha256 != config.producer_stdout_sha256
        or stderr_witness.sha256 != config.producer_stderr_sha256
    ):
        raise GoldenNativeReattestationError(
            "producer stdout/stderr differs from the pinned provenance hashes"
        )
    discovered = _discover_authorities(config, verification)
    anchors = config.candidate_root / "anchors"
    raw_anchor_probe = LocalAnchor(
        path=anchors / "raw.sqlite3",
        store_id=discovered.raw_config.store_id,
        read_only=True,
    )
    paper_anchor_probe = LocalAnchor(
        path=anchors / "paper.sqlite3",
        store_id=discovered.paper_config.store_id,
        read_only=True,
    )
    trace_paths = StartupTracePaths(
        candidate_root=config.candidate_root,
        raw_root=discovered.raw_paths.root,
        paper_root=discovered.paper_paths.root,
        raw_anchor=raw_anchor_probe.path,
        paper_anchor=paper_anchor_probe.path,
        raw_anchor_writer_lease=raw_anchor_probe.writer_lease_path,
        paper_anchor_writer_lease=paper_anchor_probe.writer_lease_path,
        paper_writer_lease=discovered.paper_paths.writer_lease,
    )
    with trace_startup_file_access(trace_paths) as trace_recorder:
        raw, paper, _raw_anchor, _paper_anchor = _open_read_only_stores(
            config,
            discovered,
        )
        try:
            paper_startup = paper.startup()
            raw_startup = raw.startup_report
            trace_recorder.stop_observing()
        except BaseException:
            paper.close()
            raw.close()
            raise
    startup_trace = trace_recorder.result
    try:
        raw_audit = raw.full_audit()
        paper_chain = paper._load_manifest_chain(discovered.paper_manifest)
        paper._assert_no_manifest_forks()
        _assert_paper_namespaces(paper, paper_chain)
        paper_audit = paper.full_audit()
        raw_chain = raw._anchored_chain()
        checkpoint_witnesses, terminal_binding = _checkpoint_witnesses(
            paper,
            paper_chain,
            raw_chain,
        )
        resolver = DiskRawResolver(raw)
        expectations = _native_expectations(
            config,
            verification,
            discovered.paper_manifest,
            raw_chain,
            terminal_binding,
            stream_factory,
        )
        native_audit = audit_native_frames(
            paper.iter_historical_frames(),
            resolver,
            expectations,
        )
        differential = compare_golden_native_checkpoint_witnesses_exact(
            paper,
            resolver,
            verification,
            checkpoint_witnesses,
            stream_factory=stream_factory,
        )
    finally:
        paper.close()
        raw.close()
    if (
        raw_audit.records_read != config.expected_commits
        or paper_audit.commits_read != config.expected_commits
        or paper_audit.rows_read != config.expected_rows
        or native_audit.commit_count != config.expected_commits
        or differential.report.get("commits") != config.expected_commits
        or differential.report.get("rows") != config.expected_rows
        or differential.report.get("checkpoint_states_verified")
        != len(checkpoint_witnesses)
    ):
        raise GoldenNativeReattestationError(
            "terminal Golden reattestation cardinality gate differs"
        )
    if (
        raw_startup.historical_segments_read != 0
        or paper_startup.segments_read != 0
        or paper_startup.tail_entries_replayed != 0
        or not paper_startup.checkpoint_used
        or startup_trace.historical_segment_open_count != 0
    ):
        raise GoldenNativeReattestationError(
            "read-only Golden startup opened historical segments or replayed a tail"
        )
    producer = _producer_terminal(
        config,
        stdout_data,
        stdout_witness,
        stderr_witness,
        discovered,
        raw_audit,
        paper_audit,
    )
    final_stdout_data, final_stdout_witness = _read_regular_stable(
        config.producer_stdout_log,
        maximum_bytes=_MAX_LOG_BYTES,
    )
    final_stderr_data, final_stderr_witness = _read_regular_stable(
        config.producer_stderr_log,
        maximum_bytes=_MAX_LOG_BYTES,
    )
    if (
        final_stdout_data != stdout_data
        or final_stderr_data != stderr_data
        or final_stdout_witness != stdout_witness
        or final_stderr_witness != stderr_witness
    ):
        raise GoldenNativeReattestationError(
            "producer log provenance changed during reattestation"
        )
    tree_after = witness_candidate_tree(config.candidate_root)
    if tree_after != tree_before:
        raise GoldenNativeReattestationError(
            "Golden producer candidate changed across read-only reattestation"
        )
    return GoldenNativeReattestationResult(
        candidate_root=config.candidate_root,
        candidate_tree_before=tree_before,
        candidate_tree_after=tree_after,
        raw_config=discovered.raw_config,
        paper_config=discovered.paper_config,
        producer=producer,
        reattestor_code_identity=config.reattestor_code_identity,
        reattestor_runtime_identity=config.reattestor_runtime_identity,
        raw_startup=raw_startup,
        paper_startup=paper_startup,
        startup_file_trace=startup_trace,
        raw_audit=raw_audit,
        paper_audit=paper_audit,
        native_audit=native_audit,
        checkpoint_witnesses=checkpoint_witnesses,
        differential=differential,
        elapsed_ns=time.perf_counter_ns() - started,
    )


def reattest_golden_native_candidate(
    config: GoldenNativeReattestationConfig,
    verification: GoldenVerification,
    *,
    stream_factory: GoldenStreamFactory | None = None,
) -> GoldenNativeReattestationResult:
    """Reattest one completed Golden native producer tree without mutation."""

    if type(config) is not GoldenNativeReattestationConfig:
        raise TypeError("config must be GoldenNativeReattestationConfig")
    if type(verification) is not GoldenVerification:
        raise TypeError("verification must be GoldenVerification")
    selected_factory = _default_stream_factory if stream_factory is None else stream_factory
    if not callable(selected_factory):
        raise TypeError("stream_factory must be callable or None")
    try:
        return _reattest(config, verification, stream_factory=selected_factory)
    except GoldenNativeReattestationError:
        raise
    except Exception as error:
        raise GoldenNativeReattestationError(
            "Golden native imported reattestation failed closed"
        ) from error


__all__ = [
    "GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1",
    "GOLDEN_NATIVE_PRODUCER_STATUS",
    "GOLDEN_NATIVE_REATTESTATION_METRICS_STATUS",
    "GoldenNativeProducerWitness",
    "GoldenNativeReattestationConfig",
    "GoldenNativeReattestationError",
    "GoldenNativeReattestationResult",
    "ReattestedFileWitness",
    "reattest_golden_native_candidate",
]
