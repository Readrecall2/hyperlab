"""Durable content-addressed raw lake for Storage v4 native references.

The external :class:`~hyperlab.paper.storage_v4.anchor.Anchor` is authority.
Raw segments and cumulative manifests are immutable; ``CURRENT`` is only a
repairable cache.  Startup may adopt exactly one fully authenticated direct
manifest successor left between manifest publication and anchor CAS.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO, NoReturn, cast
from uuid import uuid4

from .anchor import (
    Anchor,
    AnchorError,
    AnchorErrorCode,
    AnchorRecord,
    AnchorWriterLease,
)
from .canonical import canonical_json_bytes
from .contracts import RawLakeId
from .durability import (
    ImmutableTargetConflict,
    atomic_rename_noreplace,
    atomic_write_mutable_cache,
    durable_publish_immutable,
    fsync_directory,
)
from .faults import FaultHook, FaultPoint, trigger_fault
from .phase1c_progress import (
    AUDIT_HEARTBEAT_MIN_SECONDS,
    AuditProgressCallback,
    BoundedAuditProgress,
)
from .raw_manifest import (
    RAW_MANIFEST_SUFFIX,
    RawManifest,
    RawManifestError,
    RawSegmentDescriptor,
    build_raw_manifest,
    raw_manifest_from_bytes,
    raw_manifest_to_bytes,
    verify_raw_manifest,
    verify_raw_manifest_transition,
)
from .raw_reference import RawSegmentRef, RawSegmentReferenceV2
from .raw_segment import (
    RAW_CODEC_VERSION,
    RawRecordLocator,
    RawSegmentArtifact,
    RawSegmentError,
    RawSegmentErrorCode,
    RawSegmentSummary,
    read_raw_payload,
    verify_raw_segment,
)
from .segment import CodecProfile
from .types import UINT64_MAX, Hash32, StoreId

RAW_SEGMENT_SUFFIX = ".hl4r"
RAW_CURRENT_FORMAT_VERSION = 1
RAW_PENDING_FORMAT_VERSION = 1
RAW_PENDING_CONTRACT = "hyperlab.storage_v4.raw_pending.v1"
_RAW_PENDING_DOMAIN = b"HL4-RAW-PENDING"
_RAW_PENDING_MAX_BYTES = 128 * 1024 * 1024
_COPY_CHUNK_SIZE = 1024 * 1024


class RawStoreErrorCode(StrEnum):
    ALREADY_EXISTS = "RAW_STORE_ALREADY_EXISTS"
    MISSING = "RAW_STORE_MISSING"
    PATH_LAYOUT = "RAW_STORE_PATH_LAYOUT_INVALID"
    CONFIG_MISMATCH = "RAW_STORE_CONFIG_MISMATCH"
    AUTHORITY_MISMATCH = "RAW_STORE_AUTHORITY_MISMATCH"
    MANIFEST_MISSING = "RAW_STORE_MANIFEST_MISSING"
    MANIFEST_MISMATCH = "RAW_STORE_MANIFEST_MISMATCH"
    MANIFEST_FORK = "RAW_STORE_MANIFEST_FORK"
    PENDING_MISMATCH = "RAW_STORE_PENDING_MISMATCH"
    SEGMENT_MISSING = "RAW_STORE_SEGMENT_MISSING"
    SEGMENT_MISMATCH = "RAW_STORE_SEGMENT_MISMATCH"
    SEGMENT_REPLACED = "RAW_STORE_SEGMENT_REPLACED"
    ORPHAN_REFERENCE = "RAW_STORE_ORPHAN_REFERENCE"
    WRONG_LAKE = "RAW_STORE_WRONG_LAKE"
    RANGE_INVALID = "RAW_STORE_RANGE_INVALID"
    PAYLOAD_MISMATCH = "RAW_STORE_PAYLOAD_MISMATCH"
    WRITER_LEASE_HELD = "RAW_STORE_WRITER_LEASE_HELD"
    WRITER_LEASE_FAILED = "RAW_STORE_WRITER_LEASE_FAILED"
    CLOSED = "RAW_STORE_CLOSED"


class RawStoreError(RuntimeError):
    """Fail-closed durable raw-store error with a stable code."""

    def __init__(self, code: RawStoreErrorCode, message: str) -> None:
        if type(code) is not RawStoreErrorCode:
            raise TypeError("raw store error code must be RawStoreErrorCode")
        self.code = code
        super().__init__(f"{code.value}: {message}")


def _error(code: RawStoreErrorCode, message: str) -> RawStoreError:
    return RawStoreError(code, message)


class RawCurrentStatus(StrEnum):
    EXACT = "RAW_CURRENT_EXACT"
    ABSENT_REPAIRED = "RAW_CURRENT_ABSENT_REPAIRED"
    STALE_REPAIRED = "RAW_CURRENT_STALE_REPAIRED"
    CORRUPT_REPAIRED = "RAW_CURRENT_CORRUPT_REPAIRED"
    GENESIS_ABSENT = "RAW_CURRENT_GENESIS_ABSENT"


class RawPendingStatus(StrEnum):
    ABSENT = "RAW_PENDING_ABSENT"
    DIRECT_SUCCESSOR_ADOPTED = "RAW_PENDING_DIRECT_SUCCESSOR_ADOPTED"
    COMMITTED_CLEARED = "RAW_PENDING_COMMITTED_CLEARED"


@dataclass(frozen=True, slots=True)
class RawStoreConfig:
    store_id: StoreId
    lake_id: RawLakeId
    config_identity: Hash32
    codec_profile: CodecProfile = field(
        default_factory=lambda: CodecProfile.zlib(level=6)
    )

    def __post_init__(self) -> None:
        if type(self.store_id) is not StoreId:
            raise TypeError("raw store store_id must be StoreId")
        if type(self.lake_id) is not RawLakeId:
            raise TypeError("raw store lake_id must be RawLakeId")
        if type(self.config_identity) is not Hash32:
            raise TypeError("raw store config_identity must be Hash32")
        if type(self.codec_profile) is not CodecProfile:
            raise TypeError("raw store codec_profile must be CodecProfile")


@dataclass(frozen=True, slots=True)
class RawStorePaths:
    root: Path
    segments: Path
    manifests: Path
    current: Path
    pending: Path

    @classmethod
    def from_root(cls, root: Path) -> RawStorePaths:
        if not isinstance(root, Path):
            raise TypeError("raw store root must be pathlib.Path")
        selected = root.absolute()
        return cls(
            root=selected,
            segments=selected / "segments",
            manifests=selected / "manifests",
            current=selected / "CURRENT",
            pending=selected / "PENDING",
        )

    def segment_path(self, physical_sha256: Hash32) -> Path:
        if type(physical_sha256) is not Hash32:
            raise TypeError("raw segment physical identity must be Hash32")
        return self.segments / f"{physical_sha256.hex()}{RAW_SEGMENT_SUFFIX}"

    def manifest_path(self, root: Hash32) -> Path:
        if type(root) is not Hash32:
            raise TypeError("raw manifest root must be Hash32")
        return self.manifests / f"{root.hex()}{RAW_MANIFEST_SUFFIX}"


@dataclass(frozen=True, slots=True)
class RawStartupReport:
    generation: int
    manifest_root: Hash32 | None
    current_status: RawCurrentStatus
    adopted_direct_successor: bool
    historical_segments_read: int
    manifests_opened: int = 0
    manifest_namespace_entries_scanned: int = 0
    pending_status: RawPendingStatus = RawPendingStatus.ABSENT


@dataclass(frozen=True, slots=True)
class RawSealResult:
    manifest: RawManifest
    descriptor: RawSegmentDescriptor
    segment_path: Path
    manifest_path: Path
    references: tuple[RawSegmentRef, ...]


@dataclass(frozen=True, slots=True)
class RawAuditReport:
    manifests_read: int
    segments_read: int
    records_read: int
    physical_segment_bytes: int
    logical_payload_bytes: int
    stored_payload_bytes: int


@dataclass(frozen=True, slots=True)
class RawSuffixReattestationReport:
    boundary_manifest_root: Hash32
    boundary_generation: int
    authority_manifest_root: Hash32
    authority_generation: int
    suffix_manifests_read: int
    suffix_segments_read: int
    suffix_records_read: int
    first_arrival_sequence: int | None
    last_arrival_sequence: int | None


@dataclass(frozen=True, slots=True)
class _Fingerprint:
    device: int
    inode: int
    size: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True, slots=True)
class _RawPending:
    expected_generation: int
    expected_manifest_root: Hash32 | None
    candidate: RawManifest
    root: Hash32


def _is_link_or_reparse_point(path: Path) -> bool:
    if path.is_symlink():
        return True
    if os.name != "nt":
        return False
    try:
        value = os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    attributes = int(getattr(value, "st_file_attributes", 0))
    mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & mask)


def _has_link_or_reparse_component(path: Path) -> bool:
    """Inspect every lexical ancestor without resolving through it."""

    current = path.absolute()
    while True:
        if _is_link_or_reparse_point(current):
            return True
        parent = current.parent
        if parent == current:
            return False
        current = parent


def _open_regular(path: Path) -> tuple[BinaryIO, _Fingerprint]:
    if _is_link_or_reparse_point(path):
        raise _error(RawStoreErrorCode.PATH_LAYOUT, "artifact is a link or reparse point")
    flags = os.O_RDONLY
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOINHERIT", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError as error:
        raise _error(RawStoreErrorCode.MISSING, "artifact is missing") from error
    except OSError as error:
        raise _error(RawStoreErrorCode.PATH_LAYOUT, "artifact cannot be opened safely") from error
    try:
        opened = os.fstat(descriptor)
        named = os.stat(path, follow_symlinks=False)
        if (
            _is_link_or_reparse_point(path)
            or not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(named.st_mode)
            or int(opened.st_nlink) != 1
            or int(named.st_nlink) != 1
            or not os.path.samestat(opened, named)
        ):
            raise _error(
                RawStoreErrorCode.PATH_LAYOUT,
                "artifact name does not identify the opened regular file",
            )
        fingerprint = _Fingerprint(
            device=int(opened.st_dev),
            inode=int(opened.st_ino),
            size=int(opened.st_size),
            modified_ns=int(opened.st_mtime_ns),
            changed_ns=int(opened.st_ctime_ns),
        )
        stream = cast(BinaryIO, os.fdopen(descriptor, "rb", buffering=0))
    except BaseException:
        os.close(descriptor)
        raise
    return stream, fingerprint


def _fingerprint(path: Path) -> _Fingerprint:
    stream, value = _open_regular(path)
    stream.close()
    return value


def _read_regular_bytes(path: Path, *, maximum: int, missing: RawStoreErrorCode) -> bytes:
    try:
        stream, fingerprint = _open_regular(path)
    except RawStoreError as error:
        if error.code is RawStoreErrorCode.MISSING:
            raise _error(missing, f"required artifact is missing: {path.name}") from error
        raise
    with stream:
        if fingerprint.size < 1 or fingerprint.size > maximum:
            raise _error(RawStoreErrorCode.MANIFEST_MISMATCH, "artifact size is invalid")
        value = stream.read(maximum + 1)
    if len(value) != fingerprint.size:
        raise _error(RawStoreErrorCode.MANIFEST_MISMATCH, "artifact changed while read")
    return value


def _acquire_anchor_lease(anchor: Anchor) -> AnchorWriterLease:
    try:
        return anchor.acquire_writer_lease()
    except AnchorError as error:
        code = (
            RawStoreErrorCode.WRITER_LEASE_HELD
            if error.code is AnchorErrorCode.WRITER_LEASE_HELD
            else RawStoreErrorCode.WRITER_LEASE_FAILED
        )
        raise _error(code, "external anchor writer lease is unavailable") from error


def _codec_name(profile: CodecProfile) -> str:
    return "raw" if profile.codec_id == 0 else "zlib"


def _descriptor_matches_summary(
    descriptor: RawSegmentDescriptor,
    summary: RawSegmentSummary,
) -> bool:
    return (
        descriptor.segment_identity == summary.segment_identity
        and descriptor.segment_root == summary.segment_root
        and descriptor.physical_sha256 == summary.physical_sha256
        and descriptor.physical_size == summary.physical_size
        and descriptor.record_count == summary.record_count
        and descriptor.logical_payload_bytes == summary.logical_payload_bytes
        and descriptor.stored_payload_bytes == summary.stored_payload_bytes
        and descriptor.first_arrival_sequence
        == int(summary.records[0].metadata.arrival_sequence)
        and descriptor.last_arrival_sequence
        == int(summary.records[-1].metadata.arrival_sequence)
        and descriptor.first_record_id == summary.records[0].metadata.record_id
        and descriptor.last_record_id == summary.records[-1].metadata.record_id
        and descriptor.codec_profile == summary.codec_profile
    )


def _current_bytes(config: RawStoreConfig, manifest: RawManifest) -> bytes:
    return canonical_json_bytes(
        {
            "config_identity": config.config_identity.hex(),
            "format_version": RAW_CURRENT_FORMAT_VERSION,
            "generation": manifest.generation,
            "lake_id": config.lake_id.value,
            "manifest_root": manifest.root.hex(),
            "store_id": config.store_id.value,
        }
    ) + b"\n"


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"unsupported JSON constant {value!r}")


def _parse_current(data: bytes) -> dict[str, object]:
    if not data.endswith(b"\n") or data.endswith(b"\n\n"):
        raise ValueError("CURRENT LF framing is invalid")
    body = data[:-1]
    decoded = json.loads(
        body.decode("utf-8", errors="strict"),
        parse_constant=_reject_json_constant,
    )
    if type(decoded) is not dict or canonical_json_bytes(decoded) != body:
        raise ValueError("CURRENT is not one canonical object")
    return cast(dict[str, object], decoded)


def _pending_body(
    config: RawStoreConfig,
    current: RawManifest | None,
    candidate: RawManifest,
) -> dict[str, object]:
    return {
        "candidate_manifest": candidate.canonical_value(),
        "config_identity": config.config_identity.hex(),
        "contract": RAW_PENDING_CONTRACT,
        "expected_generation": 0 if current is None else current.generation,
        "expected_manifest_root": None if current is None else current.root.hex(),
        "format_version": RAW_PENDING_FORMAT_VERSION,
        "lake_id": config.lake_id.value,
        "store_id": config.store_id.value,
    }


def _pending_root(body: dict[str, object]) -> Hash32:
    encoded = canonical_json_bytes(body)
    material = b"".join(
        (
            _RAW_PENDING_DOMAIN,
            len(encoded).to_bytes(8, "big", signed=False),
            encoded,
        )
    )
    return Hash32(hashlib.sha256(material).digest())


def _pending_bytes(
    config: RawStoreConfig,
    current: RawManifest | None,
    candidate: RawManifest,
) -> bytes:
    body = _pending_body(config, current, candidate)
    return canonical_json_bytes({**body, "pending_root": _pending_root(body).hex()}) + b"\n"


def _parse_pending(data: bytes, config: RawStoreConfig) -> _RawPending:
    if type(data) is not bytes:
        raise TypeError("raw pending reader requires exact bytes")
    if not data.endswith(b"\n") or data.endswith(b"\n\n"):
        raise ValueError("raw pending LF framing is invalid")
    body_bytes = data[:-1]
    decoded = json.loads(
        body_bytes.decode("utf-8", errors="strict"),
        parse_constant=_reject_json_constant,
    )
    keys = {
        "candidate_manifest",
        "config_identity",
        "contract",
        "expected_generation",
        "expected_manifest_root",
        "format_version",
        "lake_id",
        "pending_root",
        "store_id",
    }
    if (
        type(decoded) is not dict
        or set(decoded) != keys
        or canonical_json_bytes(decoded) != body_bytes
    ):
        raise ValueError("raw pending is not one canonical object")
    value = cast(dict[str, object], decoded)
    root_value = value["pending_root"]
    if type(root_value) is not str:
        raise ValueError("raw pending root is not text")
    root = Hash32.from_hex(root_value)
    unhashed = {key: item for key, item in value.items() if key != "pending_root"}
    if root != _pending_root(unhashed):
        raise ValueError("raw pending root differs")
    if (
        value["contract"] != RAW_PENDING_CONTRACT
        or value["format_version"] != RAW_PENDING_FORMAT_VERSION
        or value["store_id"] != config.store_id.value
        or value["lake_id"] != config.lake_id.value
        or value["config_identity"] != config.config_identity.hex()
    ):
        raise ValueError("raw pending configuration differs")
    expected_generation = value["expected_generation"]
    if (
        type(expected_generation) is not int
        or expected_generation < 0
        or expected_generation >= UINT64_MAX
    ):
        raise ValueError("raw pending expected generation is invalid")
    expected_root_value = value["expected_manifest_root"]
    expected_root: Hash32 | None
    if expected_generation == 0:
        if expected_root_value is not None:
            raise ValueError("raw genesis pending unexpectedly has a parent")
        expected_root = None
    else:
        if type(expected_root_value) is not str:
            raise ValueError("raw pending expected root is not text")
        expected_root = Hash32.from_hex(expected_root_value)
    candidate_value = value["candidate_manifest"]
    if type(candidate_value) is not dict:
        raise ValueError("raw pending candidate is not an object")
    candidate = raw_manifest_from_bytes(canonical_json_bytes(candidate_value) + b"\n")
    if (
        candidate.store_id != config.store_id
        or candidate.lake_id != config.lake_id
        or candidate.config_identity != config.config_identity
        or candidate.generation != expected_generation + 1
        or candidate.parent_manifest_root != expected_root
    ):
        raise ValueError("raw pending candidate is not the declared direct successor")
    return _RawPending(
        expected_generation=expected_generation,
        expected_manifest_root=expected_root,
        candidate=candidate,
        root=root,
    )


def _raw_fault(name: str, fallback: FaultPoint) -> FaultPoint:
    # The fallback keeps this module importable while deployments roll out the
    # Phase 1C enum extension; completed Phase 1C builds always select ``name``.
    return cast(FaultPoint, getattr(FaultPoint, name, fallback))


class RawStore:
    """Single-writer durable raw store bound to one monotone anchor."""

    def __init__(
        self,
        *,
        paths: RawStorePaths,
        anchor: Anchor,
        config: RawStoreConfig,
        manifest: RawManifest | None,
        anchor_writer_lease: AnchorWriterLease,
        fault_hook: FaultHook,
        startup_report: RawStartupReport,
    ) -> None:
        self._paths = paths
        self._anchor = anchor
        self._config = config
        self._manifest = manifest
        self._anchor_writer_lease = anchor_writer_lease
        self._fault_hook = fault_hook
        self._startup_report = startup_report
        self._closed = False

    @classmethod
    def create(
        cls,
        root: Path,
        *,
        anchor: Anchor,
        config: RawStoreConfig,
        fault_hook: FaultHook = None,
    ) -> RawStore:
        if type(config) is not RawStoreConfig:
            raise TypeError("raw store config must be RawStoreConfig")
        if anchor.store_id != config.store_id:
            raise _error(RawStoreErrorCode.CONFIG_MISMATCH, "anchor store ID differs")
        lease = _acquire_anchor_lease(anchor)
        try:
            if anchor.read() is not None:
                raise _error(
                    RawStoreErrorCode.AUTHORITY_MISMATCH,
                    "fresh raw store requires an empty anchor",
                )
            paths = RawStorePaths.from_root(root)
            if (
                paths.root.exists()
                or _is_link_or_reparse_point(paths.root)
                or _has_link_or_reparse_component(paths.root.parent)
            ):
                raise _error(RawStoreErrorCode.ALREADY_EXISTS, "raw store root exists")
            if not paths.root.parent.is_dir():
                raise _error(RawStoreErrorCode.PATH_LAYOUT, "raw store parent is missing")
            try:
                paths.root.mkdir(mode=0o700)
                paths.segments.mkdir(mode=0o700)
                paths.manifests.mkdir(mode=0o700)
                fsync_directory(paths.root)
                fsync_directory(paths.root.parent)
            except FileExistsError as error:
                raise _error(
                    RawStoreErrorCode.ALREADY_EXISTS,
                    "raw store was created concurrently",
                ) from error
            report = RawStartupReport(
                generation=0,
                manifest_root=None,
                current_status=RawCurrentStatus.GENESIS_ABSENT,
                adopted_direct_successor=False,
                historical_segments_read=0,
                manifests_opened=0,
                pending_status=RawPendingStatus.ABSENT,
            )
            return cls(
                paths=paths,
                anchor=anchor,
                config=config,
                manifest=None,
                anchor_writer_lease=lease,
                fault_hook=fault_hook,
                startup_report=report,
            )
        except BaseException:
            lease.close()
            raise

    @classmethod
    def open_existing(
        cls,
        root: Path,
        *,
        anchor: Anchor,
        config: RawStoreConfig,
        fault_hook: FaultHook = None,
    ) -> RawStore:
        if type(config) is not RawStoreConfig:
            raise TypeError("raw store config must be RawStoreConfig")
        if anchor.store_id != config.store_id:
            raise _error(RawStoreErrorCode.CONFIG_MISMATCH, "anchor store ID differs")
        lease = _acquire_anchor_lease(anchor)
        try:
            paths = RawStorePaths.from_root(root)
            cls._verify_paths(paths)
            anchored = anchor.read()
            manifest = None
            manifests_opened = 0
            if anchored is not None:
                if anchored.store_id != config.store_id:
                    raise _error(
                        RawStoreErrorCode.AUTHORITY_MISMATCH,
                        "anchor belongs to another store",
                    )
                manifest = cls._read_manifest(paths, anchored.manifest_root, config)
                manifests_opened = 1
                if manifest.generation != anchored.generation:
                    raise _error(
                        RawStoreErrorCode.MANIFEST_MISMATCH,
                        "anchored generation differs from manifest",
                    )
            temporary = cls(
                paths=paths,
                anchor=anchor,
                config=config,
                manifest=manifest,
                anchor_writer_lease=lease,
                fault_hook=fault_hook,
                startup_report=RawStartupReport(
                    generation=0 if manifest is None else manifest.generation,
                    manifest_root=None if manifest is None else manifest.root,
                    current_status=RawCurrentStatus.GENESIS_ABSENT,
                    adopted_direct_successor=False,
                    historical_segments_read=0,
                ),
            )
            (
                anchored,
                adopted,
                segments_read,
                recovery_manifests_opened,
                namespace_entries_scanned,
                pending_status,
            ) = temporary._recover_successor(anchored)
            current_status = temporary._repair_current(anchored)
            temporary._startup_report = RawStartupReport(
                generation=0 if temporary._manifest is None else temporary._manifest.generation,
                manifest_root=None if temporary._manifest is None else temporary._manifest.root,
                current_status=current_status,
                adopted_direct_successor=adopted,
                historical_segments_read=segments_read,
                manifests_opened=manifests_opened + recovery_manifests_opened,
                manifest_namespace_entries_scanned=namespace_entries_scanned,
                pending_status=pending_status,
            )
            return temporary
        except BaseException:
            lease.close()
            raise

    @staticmethod
    def _verify_paths(paths: RawStorePaths) -> None:
        if (
            _has_link_or_reparse_component(paths.root)
            or not paths.root.is_dir()
        ):
            raise _error(RawStoreErrorCode.MISSING, "raw store root is missing")
        for directory in (paths.segments, paths.manifests):
            if _is_link_or_reparse_point(directory) or not directory.is_dir():
                raise _error(
                    RawStoreErrorCode.PATH_LAYOUT,
                    f"raw store directory is invalid: {directory.name}",
                )

    @staticmethod
    def _read_manifest(
        paths: RawStorePaths,
        root: Hash32,
        config: RawStoreConfig,
    ) -> RawManifest:
        path = paths.manifest_path(root)
        data = _read_regular_bytes(
            path,
            maximum=64 * 1024 * 1024,
            missing=RawStoreErrorCode.MANIFEST_MISSING,
        )
        try:
            manifest = raw_manifest_from_bytes(data)
        except (RawManifestError, TypeError, ValueError) as error:
            raise _error(
                RawStoreErrorCode.MANIFEST_MISMATCH,
                "raw manifest cannot be authenticated",
            ) from error
        if manifest.root != root:
            raise _error(
                RawStoreErrorCode.MANIFEST_MISMATCH,
                "raw manifest filename differs from its root",
            )
        if (
            manifest.store_id != config.store_id
            or manifest.lake_id != config.lake_id
            or manifest.config_identity != config.config_identity
        ):
            raise _error(
                RawStoreErrorCode.CONFIG_MISMATCH,
                "raw manifest configuration differs",
            )
        return manifest

    def _read_pending(self) -> _RawPending | None:
        path = self._paths.pending
        if _is_link_or_reparse_point(path):
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "raw pending path is a link or reparse point",
            )
        if not path.exists():
            return None
        try:
            data = _read_regular_bytes(
                path,
                maximum=_RAW_PENDING_MAX_BYTES,
                missing=RawStoreErrorCode.PENDING_MISMATCH,
            )
            return _parse_pending(data, self._config)
        except RawStoreError as error:
            if error.code is RawStoreErrorCode.PENDING_MISMATCH:
                raise
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "raw pending artifact cannot be read safely",
            ) from error
        except (RawManifestError, TypeError, UnicodeError, ValueError) as error:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "raw pending artifact cannot be authenticated",
            ) from error

    def _publish_pending(self, candidate: RawManifest) -> _RawPending:
        if self._read_pending() is not None:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "an unresolved raw pending intent already exists",
            )
        data = _pending_bytes(self._config, self._manifest, candidate)

        def verify(data_to_verify: bytes) -> _RawPending:
            return _parse_pending(data_to_verify, self._config)

        atomic_write_mutable_cache(
            self._paths.pending,
            data,
            verifier=verify,
            fault_hook=self._fault_hook,
        )
        pending = self._read_pending()
        if pending is None or pending.candidate != candidate:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "published raw pending candidate differs",
            )
        return pending

    def _clear_pending(self, expected: _RawPending) -> None:
        observed = self._read_pending()
        if observed is None:
            return
        if observed != expected:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "raw pending changed before it could be cleared",
            )
        try:
            self._paths.pending.unlink()
            fsync_directory(self._paths.root)
        except OSError as error:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "raw pending could not be durably cleared",
            ) from error

    def _manifest_names(self) -> tuple[set[Hash32], int]:
        self._verify_paths(self._paths)
        try:
            entries = tuple(self._paths.manifests.iterdir())
        except OSError as error:
            raise _error(
                RawStoreErrorCode.PATH_LAYOUT,
                "manifest namespace is unreadable",
            ) from error
        roots: set[Hash32] = set()
        for path in entries:
            if path.name.startswith(".") and path.name.endswith(".tmp"):
                continue
            if (
                _is_link_or_reparse_point(path)
                or not path.is_file()
                or not path.name.endswith(RAW_MANIFEST_SUFFIX)
            ):
                raise _error(
                    RawStoreErrorCode.MANIFEST_FORK,
                    "manifest namespace contains an unexpected entry",
                )
            stem = path.name[: -len(RAW_MANIFEST_SUFFIX)]
            try:
                root = Hash32.from_hex(stem)
            except ValueError as error:
                raise _error(
                    RawStoreErrorCode.MANIFEST_FORK,
                    "manifest namespace contains a non-canonical name",
                ) from error
            if root in roots:
                raise _error(
                    RawStoreErrorCode.MANIFEST_FORK,
                    "manifest namespace contains a duplicate identity",
                )
            roots.add(root)
        return roots, len(entries)

    @staticmethod
    def _assert_exact_namespace(
        directory: Path,
        *,
        expected_names: set[str],
        suffix: str,
        missing_code: RawStoreErrorCode,
        mismatch_code: RawStoreErrorCode,
        label: str,
    ) -> None:
        try:
            entries = tuple(directory.iterdir())
        except OSError as error:
            raise _error(
                RawStoreErrorCode.PATH_LAYOUT,
                f"{label} namespace is unreadable",
            ) from error
        observed_names: set[str] = set()
        for path in entries:
            if (
                _is_link_or_reparse_point(path)
                or not path.is_file()
                or not path.name.endswith(suffix)
            ):
                raise _error(
                    mismatch_code,
                    f"{label} namespace contains an unexpected entry",
                )
            stem = path.name[: -len(suffix)]
            try:
                identity = Hash32.from_hex(stem)
            except ValueError as error:
                raise _error(
                    mismatch_code,
                    f"{label} namespace contains a non-canonical name",
                ) from error
            if path.name != f"{identity.hex()}{suffix}":
                raise _error(
                    mismatch_code,
                    f"{label} namespace contains a non-canonical name",
                )
            observed_names.add(path.name)
        missing = expected_names - observed_names
        if missing:
            raise _error(
                missing_code,
                f"{label} namespace is missing an anchored artifact",
            )
        if observed_names != expected_names:
            raise _error(
                mismatch_code,
                f"{label} namespace contains artifacts outside the anchored chain",
            )

    def _assert_exact_raw_namespaces(
        self,
        chain: tuple[RawManifest, ...],
    ) -> None:
        expected_manifests = {
            f"{manifest.root.hex()}{RAW_MANIFEST_SUFFIX}" for manifest in chain
        }
        latest = None if not chain else chain[-1]
        expected_segments = (
            set()
            if latest is None
            else {
                f"{descriptor.physical_sha256.hex()}{RAW_SEGMENT_SUFFIX}"
                for descriptor in latest.segments
            }
        )
        self._assert_exact_namespace(
            self._paths.manifests,
            expected_names=expected_manifests,
            suffix=RAW_MANIFEST_SUFFIX,
            missing_code=RawStoreErrorCode.MANIFEST_MISSING,
            mismatch_code=RawStoreErrorCode.MANIFEST_FORK,
            label="raw manifest",
        )
        self._assert_exact_namespace(
            self._paths.segments,
            expected_names=expected_segments,
            suffix=RAW_SEGMENT_SUFFIX,
            missing_code=RawStoreErrorCode.SEGMENT_MISSING,
            mismatch_code=RawStoreErrorCode.SEGMENT_MISMATCH,
            label="raw segment",
        )

    @property
    def paths(self) -> RawStorePaths:
        return self._paths

    @property
    def config(self) -> RawStoreConfig:
        return self._config

    @property
    def manifest(self) -> RawManifest | None:
        return self._manifest

    @property
    def startup_report(self) -> RawStartupReport:
        return self._startup_report

    def set_fault_hook(self, fault_hook: FaultHook) -> None:
        self._ensure_open()
        self._fault_hook = fault_hook

    def descriptor_for(self, artifact: RawSegmentArtifact) -> RawSegmentDescriptor:
        self._ensure_open()
        if type(artifact) is not RawSegmentArtifact:
            raise TypeError("raw store requires RawSegmentArtifact")
        return RawSegmentDescriptor.from_artifact(artifact)

    def _ensure_open(self) -> None:
        if self._closed:
            raise _error(RawStoreErrorCode.CLOSED, "raw store is closed")

    def _verify_artifact(self, artifact: RawSegmentArtifact) -> RawSegmentDescriptor:
        descriptor = self.descriptor_for(artifact)
        if artifact.lake_id != self._config.lake_id:
            raise _error(RawStoreErrorCode.WRONG_LAKE, "raw artifact belongs to another lake")
        if artifact.codec_profile != self._config.codec_profile:
            raise _error(
                RawStoreErrorCode.CONFIG_MISMATCH,
                "raw artifact codec differs from store configuration",
            )
        if self._manifest is not None:
            prior = self._manifest.segments[-1]
            if descriptor.first_arrival_sequence <= prior.last_arrival_sequence:
                if descriptor == prior:
                    return descriptor
                raise _error(
                    RawStoreErrorCode.MANIFEST_FORK,
                    "raw segment arrival range does not append",
                )
        return descriptor

    def _verify_published_segment(
        self,
        path: Path,
        descriptor: RawSegmentDescriptor,
        *,
        replacement: bool = False,
    ) -> RawSegmentSummary:
        try:
            before = _fingerprint(path)
            summary = verify_raw_segment(path)
            after = _fingerprint(path)
        except RawStoreError:
            raise
        except (OSError, RawSegmentError) as error:
            code = (
                RawStoreErrorCode.SEGMENT_REPLACED
                if replacement
                else RawStoreErrorCode.SEGMENT_MISMATCH
            )
            raise _error(code, "raw segment cannot be authenticated") from error
        if before != after:
            raise _error(RawStoreErrorCode.SEGMENT_REPLACED, "raw segment changed during read")
        if not _descriptor_matches_summary(descriptor, summary):
            raise _error(RawStoreErrorCode.SEGMENT_MISMATCH, "raw segment differs from manifest")
        return summary

    def _publish_segment(
        self,
        artifact: RawSegmentArtifact,
        descriptor: RawSegmentDescriptor,
    ) -> Path:
        target = self._paths.segment_path(descriptor.physical_sha256)
        if target.exists() or _is_link_or_reparse_point(target):
            self._verify_published_segment(target, descriptor)
            return target
        source_stream, source_before = _open_regular(artifact.path)
        temporary = self._paths.segments / f".{target.name}.{uuid4().hex}.tmp"
        try:
            with source_stream:
                trigger_fault(self._fault_hook, FaultPoint.BEFORE_RAW_SEGMENT_COPY)
                flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
                for name in ("O_BINARY", "O_NOINHERIT", "O_NOFOLLOW"):
                    flags |= int(getattr(os, name, 0))
                descriptor_fd = os.open(temporary, flags, 0o600)
                with os.fdopen(descriptor_fd, "wb") as output:
                    while True:
                        block = source_stream.read(_COPY_CHUNK_SIZE)
                        if not block:
                            break
                        written = output.write(block)
                        if written != len(block):
                            raise _error(
                                RawStoreErrorCode.SEGMENT_MISMATCH,
                                "raw segment publication copy was incomplete",
                            )
                    output.flush()
                    os.fsync(output.fileno())
                trigger_fault(self._fault_hook, FaultPoint.AFTER_RAW_SEGMENT_COPY)
            if _fingerprint(artifact.path) != source_before:
                raise _error(
                    RawStoreErrorCode.SEGMENT_REPLACED,
                    "raw staging segment changed during publication",
                )
            self._verify_published_segment(temporary, descriptor)
            trigger_fault(self._fault_hook, FaultPoint.BEFORE_RENAME)
            trigger_fault(self._fault_hook, FaultPoint.BEFORE_EXCLUSIVE_PUBLISH)
            try:
                atomic_rename_noreplace(temporary, target)
            except FileExistsError:
                self._verify_published_segment(target, descriptor)
                temporary.unlink(missing_ok=True)
            trigger_fault(self._fault_hook, FaultPoint.AFTER_EXCLUSIVE_PUBLISH)
            trigger_fault(self._fault_hook, FaultPoint.AFTER_RENAME)
            self._verify_published_segment(target, descriptor)
            fsync_directory(self._paths.segments)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        artifact.path.unlink(missing_ok=True)
        return target

    def _references(
        self,
        summary: RawSegmentSummary,
        manifest_root: Hash32,
    ) -> tuple[RawSegmentRef, ...]:
        codec_id = _codec_name(summary.codec_profile)
        return tuple(
            RawSegmentReferenceV2(
                raw_store_id=self._config.store_id,
                lake_id=summary.lake_id,
                source_id=record.metadata.source_id,
                venue_id=record.metadata.venue_id,
                segment_identity=summary.segment_identity,
                segment_root=summary.segment_root,
                raw_manifest_root=manifest_root,
                physical_sha256=summary.physical_sha256,
                record_id=record.metadata.record_id,
                byte_offset=record.byte_offset,
                stored_length=record.stored_length,
                stored_sha256=record.stored_sha256,
                logical_payload_length=record.logical_payload_length,
                logical_payload_sha256=record.logical_payload_sha256,
                input_type=record.metadata.input_type,
                source_stream_id=record.metadata.source_stream_id,
                source_first_sequence=record.metadata.source_first_sequence,
                source_last_sequence=record.metadata.source_last_sequence,
                arrival_sequence=record.metadata.arrival_sequence,
                source_timestamp=record.metadata.source_timestamp,
                received_timestamp=record.metadata.received_timestamp,
                codec_id=codec_id,
                codec_version=RAW_CODEC_VERSION,
            )
            for record in summary.records
        )

    def seal(self, artifact: RawSegmentArtifact) -> RawSealResult:
        self._ensure_open()
        if self._read_pending() is not None:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "raw store has an unresolved pending intent",
            )
        descriptor = self._verify_artifact(artifact)
        if self._manifest is not None and descriptor == self._manifest.segments[-1]:
            path = self._paths.segment_path(descriptor.physical_sha256)
            summary = self._verify_published_segment(path, descriptor)
            return RawSealResult(
                manifest=self._manifest,
                descriptor=descriptor,
                segment_path=path,
                manifest_path=self._paths.manifest_path(self._manifest.root),
                references=self._references(summary, self._manifest.root),
            )

        trigger_fault(
            self._fault_hook,
            _raw_fault("BEFORE_RAW_SEGMENT_PUBLICATION", FaultPoint.BEFORE_SEGMENT_PUBLICATION),
        )
        segment_path = self._publish_segment(artifact, descriptor)
        trigger_fault(
            self._fault_hook,
            _raw_fault("AFTER_RAW_SEGMENT_PUBLICATION", FaultPoint.AFTER_SEGMENT_PUBLICATION),
        )
        summary = self._verify_published_segment(segment_path, descriptor)
        generation = 1 if self._manifest is None else self._manifest.generation + 1
        segments = (
            (descriptor,)
            if self._manifest is None
            else (*self._manifest.segments, descriptor)
        )
        candidate = build_raw_manifest(
            store_id=self._config.store_id,
            lake_id=self._config.lake_id,
            config_identity=self._config.config_identity,
            generation=generation,
            parent_manifest_root=None if self._manifest is None else self._manifest.root,
            segments=segments,
        )
        manifest_path = self._paths.manifest_path(candidate.root)
        pending = self._publish_pending(candidate)
        trigger_fault(
            self._fault_hook,
            _raw_fault("BEFORE_RAW_MANIFEST_PUBLICATION", FaultPoint.BEFORE_MANIFEST_PUBLICATION),
        )
        try:
            durable_publish_immutable(
                manifest_path,
                raw_manifest_to_bytes(candidate),
                verifier=raw_manifest_from_bytes,
                fault_hook=self._fault_hook,
            )
        except ImmutableTargetConflict as error:
            raise _error(
                RawStoreErrorCode.MANIFEST_MISMATCH,
                "content-addressed raw manifest target is divergent",
            ) from error
        trigger_fault(
            self._fault_hook,
            _raw_fault("AFTER_RAW_MANIFEST_PUBLICATION", FaultPoint.AFTER_MANIFEST_PUBLICATION),
        )
        expected = (
            None
            if self._manifest is None
            else AnchorRecord(
                store_id=self._config.store_id,
                generation=self._manifest.generation,
                manifest_root=self._manifest.root,
            )
        )
        anchored = AnchorRecord(
            store_id=self._config.store_id,
            generation=candidate.generation,
            manifest_root=candidate.root,
        )
        trigger_fault(
            self._fault_hook,
            _raw_fault("BEFORE_RAW_ANCHOR_PUBLICATION", FaultPoint.BEFORE_ANCHOR_PUBLICATION),
        )
        try:
            self._anchor.compare_and_swap(expected, anchored)
        except AnchorError as error:
            raise _error(
                RawStoreErrorCode.AUTHORITY_MISMATCH,
                "raw anchor CAS refused the manifest successor",
            ) from error
        self._manifest = candidate
        trigger_fault(
            self._fault_hook,
            _raw_fault("AFTER_RAW_ANCHOR_PUBLICATION", FaultPoint.AFTER_ANCHOR_PUBLICATION),
        )
        self._clear_pending(pending)
        self._repair_current(anchored)
        return RawSealResult(
            manifest=candidate,
            descriptor=descriptor,
            segment_path=segment_path,
            manifest_path=manifest_path,
            references=self._references(summary, candidate.root),
        )

    def _recover_successor(
        self,
        anchored: AnchorRecord | None,
    ) -> tuple[AnchorRecord | None, bool, int, int, int, RawPendingStatus]:
        pending = self._read_pending()
        if pending is None:
            return anchored, False, 0, 0, 0, RawPendingStatus.ABSENT

        current_generation = 0 if self._manifest is None else self._manifest.generation
        current_root = None if self._manifest is None else self._manifest.root
        candidate = pending.candidate
        roots, namespace_entries_scanned = self._manifest_names()

        if (
            anchored is not None
            and anchored.generation == candidate.generation
            and anchored.manifest_root == candidate.root
        ):
            if self._manifest != candidate:
                raise _error(
                    RawStoreErrorCode.PENDING_MISMATCH,
                    "committed raw pending differs from anchored manifest",
                )
            if len(roots) != candidate.generation or candidate.root not in roots:
                raise _error(
                    RawStoreErrorCode.MANIFEST_FORK,
                    "committed raw pending has an ambiguous manifest namespace",
                )
            self._clear_pending(pending)
            return (
                anchored,
                False,
                0,
                0,
                namespace_entries_scanned,
                RawPendingStatus.COMMITTED_CLEARED,
            )

        if (
            pending.expected_generation != current_generation
            or pending.expected_manifest_root != current_root
        ):
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "raw pending expected authority differs from the anchor",
            )
        try:
            if self._manifest is None:
                verify_raw_manifest(candidate, expected_generation=1)
            else:
                verify_raw_manifest_transition(self._manifest, candidate)
        except RawManifestError as error:
            raise _error(
                RawStoreErrorCode.MANIFEST_FORK,
                "raw manifest successor is not an exact append",
            ) from error

        candidate_present = candidate.root in roots
        if candidate_present:
            if len(roots) != candidate.generation:
                raise _error(
                    RawStoreErrorCode.MANIFEST_FORK,
                    "raw pending has multiple manifest candidates",
                )
        elif len(roots) != pending.expected_generation:
            raise _error(
                RawStoreErrorCode.MANIFEST_FORK,
                "raw pending namespace differs from its expected generation",
            )

        manifest_path = self._paths.manifest_path(candidate.root)
        if not candidate_present:
            try:
                durable_publish_immutable(
                    manifest_path,
                    raw_manifest_to_bytes(candidate),
                    verifier=raw_manifest_from_bytes,
                    fault_hook=self._fault_hook,
                )
            except ImmutableTargetConflict as error:
                raise _error(
                    RawStoreErrorCode.MANIFEST_MISMATCH,
                    "recovered raw manifest target is divergent",
                ) from error
        published = self._read_manifest(self._paths, candidate.root, self._config)
        if published != candidate:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "published raw manifest differs from pending candidate",
            )

        added = candidate.segments[-1]
        path = self._paths.segment_path(added.physical_sha256)
        if not path.exists():
            raise _error(
                RawStoreErrorCode.SEGMENT_MISSING,
                "direct successor segment is missing",
            )
        self._verify_published_segment(path, added)
        record = AnchorRecord(
            store_id=self._config.store_id,
            generation=candidate.generation,
            manifest_root=candidate.root,
        )
        trigger_fault(
            self._fault_hook,
            _raw_fault("BEFORE_RAW_ANCHOR_PUBLICATION", FaultPoint.BEFORE_ANCHOR_PUBLICATION),
        )
        try:
            self._anchor.compare_and_swap(anchored, record)
        except AnchorError as error:
            raise _error(
                RawStoreErrorCode.AUTHORITY_MISMATCH,
                "anchor changed during raw successor recovery",
            ) from error
        self._manifest = candidate
        trigger_fault(
            self._fault_hook,
            _raw_fault("AFTER_RAW_ANCHOR_PUBLICATION", FaultPoint.AFTER_ANCHOR_PUBLICATION),
        )
        self._clear_pending(pending)
        return (
            record,
            True,
            1,
            1,
            namespace_entries_scanned,
            RawPendingStatus.DIRECT_SUCCESSOR_ADOPTED,
        )

    def _repair_current(self, anchored: AnchorRecord | None) -> RawCurrentStatus:
        if anchored is None or self._manifest is None:
            return RawCurrentStatus.GENESIS_ABSENT
        expected = _current_bytes(self._config, self._manifest)
        if _is_link_or_reparse_point(self._paths.current):
            status = RawCurrentStatus.CORRUPT_REPAIRED
            self._paths.current.unlink(missing_ok=True)
        elif not self._paths.current.exists():
            status = RawCurrentStatus.ABSENT_REPAIRED
        else:
            try:
                observed_bytes = _read_regular_bytes(
                    self._paths.current,
                    maximum=1024 * 1024,
                    missing=RawStoreErrorCode.MISSING,
                )
                if observed_bytes == expected:
                    return RawCurrentStatus.EXACT
                _parse_current(observed_bytes)
                status = RawCurrentStatus.STALE_REPAIRED
            except (OSError, UnicodeError, ValueError, RawStoreError):
                status = RawCurrentStatus.CORRUPT_REPAIRED
        trigger_fault(self._fault_hook, FaultPoint.BEFORE_CURRENT_PUBLICATION)
        atomic_write_mutable_cache(
            self._paths.current,
            expected,
            verifier=_parse_current,
            fault_hook=self._fault_hook,
        )
        trigger_fault(self._fault_hook, FaultPoint.AFTER_CURRENT_PUBLICATION)
        return status

    def _chain_to(self, root: Hash32) -> tuple[RawManifest, ...]:
        if self._manifest is None:
            raise _error(RawStoreErrorCode.ORPHAN_REFERENCE, "raw store has no authority")
        chain: list[RawManifest] = []
        child = self._manifest
        while True:
            chain.append(child)
            if child.root == root:
                return tuple(chain)
            if child.parent_manifest_root is None:
                raise _error(
                    RawStoreErrorCode.ORPHAN_REFERENCE,
                    "raw reference manifest is not in the anchored chain",
                )
            parent = self._read_manifest(self._paths, child.parent_manifest_root, self._config)
            try:
                verify_raw_manifest_transition(parent, child)
            except RawManifestError as error:
                raise _error(
                    RawStoreErrorCode.MANIFEST_FORK,
                    "authenticated raw manifest chain is not append-only",
                ) from error
            child = parent

    def _anchored_chain(self) -> tuple[RawManifest, ...]:
        if self._manifest is None:
            return ()
        descending: list[RawManifest] = []
        child = self._manifest
        while True:
            descending.append(child)
            if child.parent_manifest_root is None:
                break
            parent = self._read_manifest(
                self._paths,
                child.parent_manifest_root,
                self._config,
            )
            try:
                verify_raw_manifest_transition(parent, child)
            except RawManifestError as error:
                raise _error(
                    RawStoreErrorCode.MANIFEST_FORK,
                    "authenticated raw manifest chain is not append-only",
                ) from error
            child = parent
        chain = tuple(reversed(descending))
        if (
            not chain
            or chain[0].generation != 1
            or len(chain) != chain[-1].generation
        ):
            raise _error(
                RawStoreErrorCode.MANIFEST_FORK,
                "anchored raw manifest chain has a generation gap",
            )
        return chain

    def authenticated_manifest(self, root: Hash32) -> RawManifest:
        self._ensure_open()
        self._verify_paths(self._paths)
        return self._chain_to(root)[-1]

    def reattest_contiguous_suffix(
        self,
        *,
        boundary_manifest_root: Hash32,
        next_arrival_sequence: int,
    ) -> RawSuffixReattestationReport:
        """Authenticate only the raw segment suffix after one sealed boundary."""

        self._ensure_open()
        self._verify_paths(self._paths)
        if type(boundary_manifest_root) is not Hash32:
            raise TypeError("boundary_manifest_root must be Hash32")
        if (
            type(next_arrival_sequence) is not int
            or next_arrival_sequence < 0
            or next_arrival_sequence > UINT64_MAX
        ):
            raise ValueError("next_arrival_sequence must be an exact uint64")
        if self._read_pending() is not None:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "cannot reattest a suffix with an unresolved pending intent",
            )
        latest = self._manifest
        if latest is None:
            raise _error(
                RawStoreErrorCode.ORPHAN_REFERENCE,
                "raw suffix boundary has no published authority",
            )
        expected_anchor = AnchorRecord(
            store_id=self._config.store_id,
            generation=latest.generation,
            manifest_root=latest.root,
        )
        if self._anchor.read() != expected_anchor:
            raise _error(
                RawStoreErrorCode.AUTHORITY_MISMATCH,
                "raw anchor differs from the loaded suffix authority",
            )

        chain = self._anchored_chain()
        self._assert_exact_raw_namespaces(chain)
        boundary_index = next(
            (
                index
                for index, manifest in enumerate(chain)
                if manifest.root == boundary_manifest_root
            ),
            None,
        )
        if boundary_index is None:
            raise _error(
                RawStoreErrorCode.ORPHAN_REFERENCE,
                "raw suffix boundary is not in the anchored chain",
            )
        boundary = chain[boundary_index]
        boundary_last = boundary.segments[-1].last_arrival_sequence
        if boundary_last == UINT64_MAX or next_arrival_sequence != boundary_last + 1:
            raise _error(
                RawStoreErrorCode.RANGE_INVALID,
                "raw suffix does not start immediately after its boundary",
            )

        suffix_manifests = chain[boundary_index + 1 :]
        suffix_descriptors = latest.segments[len(boundary.segments) :]
        if len(suffix_manifests) != len(suffix_descriptors):
            raise _error(
                RawStoreErrorCode.MANIFEST_FORK,
                "raw suffix generations differ from appended segments",
            )

        cursor = next_arrival_sequence
        records_read = 0
        first_arrival: int | None = None
        last_arrival: int | None = None
        for descriptor in suffix_descriptors:
            span = descriptor.last_arrival_sequence - descriptor.first_arrival_sequence + 1
            if (
                descriptor.first_arrival_sequence != cursor
                or span != descriptor.record_count
            ):
                raise _error(
                    RawStoreErrorCode.RANGE_INVALID,
                    "raw suffix descriptor ranges are not strictly contiguous",
                )
            path = self._paths.segment_path(descriptor.physical_sha256)
            summary = self._verify_published_segment(path, descriptor)
            for ordinal, record in enumerate(summary.records):
                if int(record.metadata.arrival_sequence) != cursor + ordinal:
                    raise _error(
                        RawStoreErrorCode.RANGE_INVALID,
                        "raw suffix record arrivals are not strictly contiguous",
                    )
            if first_arrival is None:
                first_arrival = descriptor.first_arrival_sequence
            last_arrival = descriptor.last_arrival_sequence
            records_read += descriptor.record_count
            cursor = descriptor.last_arrival_sequence + 1

        self._assert_exact_raw_namespaces(chain)
        if self._read_pending() is not None:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "raw pending intent appeared during suffix reattestation",
            )
        if self._anchor.read() != expected_anchor:
            raise _error(
                RawStoreErrorCode.AUTHORITY_MISMATCH,
                "raw anchor changed during suffix reattestation",
            )
        return RawSuffixReattestationReport(
            boundary_manifest_root=boundary.root,
            boundary_generation=boundary.generation,
            authority_manifest_root=latest.root,
            authority_generation=latest.generation,
            suffix_manifests_read=len(suffix_manifests),
            suffix_segments_read=len(suffix_descriptors),
            suffix_records_read=records_read,
            first_arrival_sequence=first_arrival,
            last_arrival_sequence=last_arrival,
        )

    def authenticated_suffix_references(
        self,
        *,
        boundary_manifest_root: Hash32,
        next_arrival_sequence: int,
    ) -> tuple[RawSegmentRef, ...]:
        """Return only references from one fully reattested contiguous suffix."""

        report = self.reattest_contiguous_suffix(
            boundary_manifest_root=boundary_manifest_root,
            next_arrival_sequence=next_arrival_sequence,
        )
        if report.suffix_records_read == 0:
            return ()
        chain = self._anchored_chain()
        boundary_index = next(
            index
            for index, manifest in enumerate(chain)
            if manifest.root == boundary_manifest_root
        )
        references: list[RawSegmentRef] = []
        for manifest in chain[boundary_index + 1 :]:
            descriptor = manifest.segments[-1]
            summary = self._verify_published_segment(
                self._paths.segment_path(descriptor.physical_sha256),
                descriptor,
            )
            references.extend(self._references(summary, manifest.root))
        if (
            len(references) != report.suffix_records_read
            or int(references[0].arrival_sequence) != next_arrival_sequence
            or int(references[-1].arrival_sequence) != report.last_arrival_sequence
            or any(
                int(reference.arrival_sequence) != next_arrival_sequence + ordinal
                for ordinal, reference in enumerate(references)
            )
        ):
            raise _error(
                RawStoreErrorCode.RANGE_INVALID,
                "authenticated suffix references differ from the reattested range",
            )
        return tuple(references)

    def full_audit(
        self,
        *,
        progress: AuditProgressCallback | None = None,
        heartbeat_interval_seconds: float = AUDIT_HEARTBEAT_MIN_SECONDS,
    ) -> RawAuditReport:
        self._ensure_open()
        self._verify_paths(self._paths)
        if self._read_pending() is not None:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "cannot audit with an unresolved raw pending intent",
            )
        if self._manifest is None:
            if self._anchor.read() is not None:
                raise _error(
                    RawStoreErrorCode.AUTHORITY_MISMATCH,
                    "raw anchor is nonempty while the loaded store is at genesis",
                )
            self._assert_exact_raw_namespaces(())
            audit_progress = BoundedAuditProgress(
                phase="raw_full_audit",
                progress=progress,
                totals={"records": 0, "segments": 0},
                heartbeat_interval_seconds=heartbeat_interval_seconds,
            )
            report = RawAuditReport(0, 0, 0, 0, 0, 0)
            audit_progress.complete({"records": 0, "segments": 0})
            return report
        latest = self._manifest
        expected_anchor = AnchorRecord(
            store_id=self._config.store_id,
            generation=latest.generation,
            manifest_root=latest.root,
        )
        if self._anchor.read() != expected_anchor:
            raise _error(
                RawStoreErrorCode.AUTHORITY_MISMATCH,
                "raw anchor differs from the loaded audit authority",
            )
        audit_progress = BoundedAuditProgress(
            phase="raw_full_audit",
            progress=progress,
            totals={
                "records": latest.total_record_count,
                "segments": len(latest.segments),
            },
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )
        chain = self._anchored_chain()
        self._assert_exact_raw_namespaces(chain)
        records = 0
        physical = 0
        logical = 0
        stored = 0
        for segment_index, descriptor in enumerate(latest.segments, start=1):
            path = self._paths.segment_path(descriptor.physical_sha256)
            if not path.exists():
                raise _error(RawStoreErrorCode.SEGMENT_MISSING, "audited segment is missing")
            summary = self._verify_published_segment(path, descriptor)
            records += summary.record_count
            physical += summary.physical_size
            logical += summary.logical_payload_bytes
            stored += summary.stored_payload_bytes
            audit_progress.advance(
                {"records": records, "segments": segment_index},
            )
        self._assert_exact_raw_namespaces(chain)
        if self._read_pending() is not None:
            raise _error(
                RawStoreErrorCode.PENDING_MISMATCH,
                "raw pending intent appeared during full audit",
            )
        if self._anchor.read() != expected_anchor:
            raise _error(
                RawStoreErrorCode.AUTHORITY_MISMATCH,
                "raw anchor changed during full audit",
            )
        report = RawAuditReport(
            manifests_read=len(chain),
            segments_read=len(latest.segments),
            records_read=records,
            physical_segment_bytes=physical,
            logical_payload_bytes=logical,
            stored_payload_bytes=stored,
        )
        audit_progress.complete(
            {"records": records, "segments": len(latest.segments)},
        )
        return report

    def _oldest_root(self) -> Hash32:
        if self._manifest is None:
            raise _error(RawStoreErrorCode.MANIFEST_MISSING, "raw store is at genesis")
        child = self._manifest
        while child.parent_manifest_root is not None:
            parent = self._read_manifest(self._paths, child.parent_manifest_root, self._config)
            try:
                verify_raw_manifest_transition(parent, child)
            except RawManifestError as error:
                raise _error(RawStoreErrorCode.MANIFEST_FORK, "raw chain transition differs") from error
            child = parent
        return child.root

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._anchor_writer_lease.close()
        finally:
            self._closed = True

    def __enter__(self) -> RawStore:
        self._ensure_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class DiskRawResolver:
    """Resolve V2 references with one bounded current-segment hash cache."""

    def __init__(self, store: RawStore) -> None:
        if type(store) is not RawStore:
            raise TypeError("disk raw resolver requires RawStore")
        store._ensure_open()
        self._store = store
        self._authenticated_descriptors: dict[Hash32, RawSegmentDescriptor] = {}
        self._authenticated_authority_root: Hash32 | None = None
        self._verified: tuple[Hash32, _Fingerprint, RawSegmentSummary] | None = None
        self._physical_hash_passes = 0
        self._manifest_authentication_passes = 0

    @property
    def physical_hash_passes(self) -> int:
        return self._physical_hash_passes

    @property
    def manifest_authentication_passes(self) -> int:
        return self._manifest_authentication_passes

    @property
    def authenticated_manifest_count(self) -> int:
        return len(self._authenticated_descriptors)

    @property
    def cached_segment_count(self) -> int:
        return 0 if self._verified is None else 1

    def _authenticate_chain(self) -> None:
        latest = self._store.manifest
        if latest is None:
            raise _error(RawStoreErrorCode.ORPHAN_REFERENCE, "raw store has no authority")
        if self._authenticated_authority_root == latest.root:
            return
        descriptors: dict[Hash32, RawSegmentDescriptor] = {}
        child = latest
        while True:
            if child.root in descriptors:
                raise _error(RawStoreErrorCode.MANIFEST_FORK, "raw manifest chain cycles")
            descriptors[child.root] = child.segments[-1]
            if child.parent_manifest_root is None:
                break
            parent = self._store._read_manifest(
                self._store.paths,
                child.parent_manifest_root,
                self._store.config,
            )
            try:
                verify_raw_manifest_transition(parent, child)
            except RawManifestError as error:
                raise _error(
                    RawStoreErrorCode.MANIFEST_FORK,
                    "authenticated raw manifest chain is not append-only",
                ) from error
            child = parent
        self._authenticated_descriptors = descriptors
        self._authenticated_authority_root = latest.root
        self._verified = None
        self._manifest_authentication_passes += 1

    def _summary(
        self,
        descriptor: RawSegmentDescriptor,
    ) -> tuple[Path, RawSegmentSummary, _Fingerprint]:
        path = self._store.paths.segment_path(descriptor.physical_sha256)
        try:
            current = _fingerprint(path)
        except RawStoreError as error:
            if error.code is RawStoreErrorCode.MISSING:
                raise _error(RawStoreErrorCode.SEGMENT_MISSING, "referenced segment is missing") from error
            raise
        cached = self._verified
        if (
            cached is not None
            and cached[0] == descriptor.physical_sha256
            and cached[1] == current
        ):
            return path, cached[2], current
        try:
            summary = self._store._verify_published_segment(
                path,
                descriptor,
                replacement=cached is not None and cached[0] == descriptor.physical_sha256,
            )
        except RawStoreError as error:
            if (
                cached is not None
                and cached[0] == descriptor.physical_sha256
                and error.code is RawStoreErrorCode.SEGMENT_MISMATCH
            ):
                raise _error(
                    RawStoreErrorCode.SEGMENT_REPLACED,
                    "previously verified raw segment changed",
                ) from error
            raise
        after = _fingerprint(path)
        self._verified = (descriptor.physical_sha256, after, summary)
        self._physical_hash_passes += 1
        return path, summary, after

    @staticmethod
    def _locator(summary: RawSegmentSummary, reference: RawSegmentRef) -> RawRecordLocator:
        target = int(reference.arrival_sequence)
        lower = 0
        upper = len(summary.records)
        while lower < upper:
            midpoint = (lower + upper) // 2
            observed = int(summary.records[midpoint].metadata.arrival_sequence)
            if observed < target:
                lower = midpoint + 1
            else:
                upper = midpoint
        if lower == len(summary.records):
            raise _error(RawStoreErrorCode.ORPHAN_REFERENCE, "raw record ID is absent")
        locator = summary.records[lower]
        if (
            int(locator.metadata.arrival_sequence) != target
            or locator.metadata.record_id != reference.record_id
        ):
            raise _error(RawStoreErrorCode.ORPHAN_REFERENCE, "raw record ID is absent")
        if (
            reference.byte_offset != locator.byte_offset
            or reference.stored_length != locator.stored_length
        ):
            raise _error(RawStoreErrorCode.RANGE_INVALID, "raw byte range differs from index")
        metadata = locator.metadata
        if (
            reference.stored_sha256 != locator.stored_sha256
            or reference.logical_payload_length != locator.logical_payload_length
            or reference.logical_payload_sha256 != locator.logical_payload_sha256
            or reference.source_id != metadata.source_id
            or reference.venue_id != metadata.venue_id
            or reference.input_type != metadata.input_type
            or reference.source_stream_id != metadata.source_stream_id
            or reference.source_first_sequence != metadata.source_first_sequence
            or reference.source_last_sequence != metadata.source_last_sequence
            or reference.arrival_sequence != metadata.arrival_sequence
            or reference.source_timestamp != metadata.source_timestamp
            or reference.received_timestamp != metadata.received_timestamp
            or reference.codec_id != _codec_name(locator.codec_profile)
            or reference.codec_version != RAW_CODEC_VERSION
        ):
            raise _error(RawStoreErrorCode.PAYLOAD_MISMATCH, "raw reference metadata differs")
        return locator

    def resolve(self, reference: RawSegmentRef) -> bytes:
        self._store._ensure_open()
        self._store._verify_paths(self._store.paths)
        if type(reference) is not RawSegmentReferenceV2:
            raise TypeError("disk raw resolver requires RawSegmentReferenceV2")
        if reference.raw_store_id != self._store.config.store_id:
            raise _error(
                RawStoreErrorCode.AUTHORITY_MISMATCH,
                "raw reference belongs to another raw store",
            )
        if reference.lake_id != self._store.config.lake_id:
            raise _error(RawStoreErrorCode.WRONG_LAKE, "raw reference belongs to another lake")
        self._authenticate_chain()
        descriptor = self._authenticated_descriptors.get(reference.raw_manifest_root)
        if descriptor is None:
            raise _error(
                RawStoreErrorCode.ORPHAN_REFERENCE,
                "raw reference manifest is not in the anchored chain",
            )
        if (
            reference.physical_sha256 != descriptor.physical_sha256
            or reference.segment_identity != descriptor.segment_identity
            or reference.segment_root != descriptor.segment_root
        ):
            raise _error(
                RawStoreErrorCode.ORPHAN_REFERENCE,
                "raw segment is not the descriptor published by its manifest",
            )
        if (
            reference.byte_offset > descriptor.physical_size
            or reference.stored_length > descriptor.physical_size - reference.byte_offset
        ):
            raise _error(RawStoreErrorCode.RANGE_INVALID, "raw reference range exceeds segment")
        path, summary, fingerprint = self._summary(descriptor)
        locator = self._locator(summary, reference)
        try:
            logical = read_raw_payload(
                path,
                locator,
                expected_lake_id=self._store.config.lake_id,
            )
        except RawSegmentError as error:
            code = (
                RawStoreErrorCode.RANGE_INVALID
                if error.code is RawSegmentErrorCode.TRUNCATED
                else RawStoreErrorCode.PAYLOAD_MISMATCH
            )
            raise _error(code, "raw referenced payload cannot be authenticated") from error
        if _fingerprint(path) != fingerprint:
            self._verified = None
            raise _error(RawStoreErrorCode.SEGMENT_REPLACED, "raw segment changed during range read")
        return logical


__all__ = [
    "RAW_CURRENT_FORMAT_VERSION",
    "RAW_PENDING_CONTRACT",
    "RAW_PENDING_FORMAT_VERSION",
    "RAW_SEGMENT_SUFFIX",
    "DiskRawResolver",
    "RawAuditReport",
    "RawCurrentStatus",
    "RawPendingStatus",
    "RawSealResult",
    "RawStartupReport",
    "RawStore",
    "RawStoreConfig",
    "RawStoreError",
    "RawStoreErrorCode",
    "RawStorePaths",
    "RawSuffixReattestationReport",
]
