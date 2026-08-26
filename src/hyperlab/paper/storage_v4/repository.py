"""Checkpointed Storage v4 repository authority and recovery orchestration.

``CURRENT`` is a repairable cache; external anchor state is authoritative.
"""

from __future__ import annotations

import errno
import json
import os
import stat
import sys
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO, NoReturn, cast

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

from .anchor import (
    Anchor,
    AnchorError,
    AnchorErrorCode,
    AnchorRecord,
    AnchorWriterLease,
)
from .canonical import (
    PROTOCOL_VERSION,
    canonical_json_bytes,
    frame_bytes,
    frame_hash32,
    frame_optional_hash32,
    frame_text,
    frame_u32,
    frame_u64,
    framed_hash,
)
from .checkpoint import (
    Checkpoint,
    CheckpointState,
    CheckpointStateWitness,
    CumulativeStreamCount,
    build_checkpoint,
    checkpoint_from_bytes,
    checkpoint_state_sha256,
    checkpoint_to_bytes,
    verify_checkpoint,
)
from .contracts import StorageMode
from .durability import (
    PublishDisposition,
    atomic_write_mutable_cache,
    durable_publish_immutable,
    fsync_directory,
)
from .faults import FaultHook, FaultPoint, trigger_fault
from .manifest import (
    MANIFEST_FORMAT_VERSION,
    MANIFEST_MAGIC,
    Manifest,
    ManifestFormatError,
    ManifestReadLimits,
    OpaqueIdentity,
    SegmentDescriptor,
    build_manifest,
    manifest_from_bytes,
    manifest_to_bytes,
    verify_manifest,
    verify_manifest_transition,
)
from .overlay import (
    FAULT_AFTER_COMMIT,
    FAULT_BEFORE_COMMIT,
    FAULT_BEFORE_TRANSACTION,
    GENESIS_MANIFEST_GENERATION,
    GENESIS_MANIFEST_ROOT,
    OverlayIdentity,
    OverlayState,
    OverlayTailDiscardResult,
    OverlayThresholds,
    SQLiteOverlay,
)
from .phase1c_progress import (
    AUDIT_HEARTBEAT_MIN_SECONDS,
    AuditProgressCallback,
    BoundedAuditProgress,
)
from .segment import (
    CodecProfile,
    SegmentArtifact,
    SegmentFormatError,
    build_segment,
    read_segment,
)
from .types import (
    UINT32_MAX,
    UINT64_MAX,
    CommitFrame,
    CommitSequence,
    Hash32,
    LocalCount,
    RunId,
    StoreId,
    StreamId,
)

CURRENT_FORMAT_VERSION = 1
DOMAIN_CANDIDATE_SEGMENT_DESCRIPTORS = b"HL4-CANDIDATE-SEGMENT-DESCRIPTORS"
SEGMENT_SUFFIX = ".hl4s"
CHECKPOINT_SUFFIX = ".hl4c"
MANIFEST_SUFFIX = ".hl4m"
WRITER_LEASE_NAME = "WRITER.LEASE"


class RepositoryErrorCode(StrEnum):
    """Stable fail-closed reasons surfaced by repository integration."""

    ALREADY_EXISTS = "REPOSITORY_ALREADY_EXISTS"
    MISSING = "REPOSITORY_MISSING"
    PATH_LAYOUT = "REPOSITORY_PATH_LAYOUT_INVALID"
    CONFIG_MISMATCH = "REPOSITORY_CONFIG_MISMATCH"
    AUTHORITY_EMPTY = "REPOSITORY_AUTHORITY_EMPTY"
    AUTHORITY_MISSING = "REPOSITORY_AUTHORITY_ARTIFACT_MISSING"
    AUTHORITY_MISMATCH = "REPOSITORY_AUTHORITY_MISMATCH"
    CHECKPOINT_MISMATCH = "REPOSITORY_CHECKPOINT_MISMATCH"
    OVERLAY_AHEAD = "REPOSITORY_OVERLAY_AHEAD_OF_ANCHOR"
    OVERLAY_FORK = "REPOSITORY_OVERLAY_FORK"
    MANIFEST_FORK = "REPOSITORY_MANIFEST_FORK"
    EMPTY_SEAL = "REPOSITORY_EMPTY_SEAL"
    SNAPSHOT_MISMATCH = "REPOSITORY_SNAPSHOT_MISMATCH"
    COUNTER_OVERFLOW = "REPOSITORY_COUNTER_OVERFLOW"
    SEGMENT_MISSING = "REPOSITORY_SEGMENT_MISSING"
    SEGMENT_MISMATCH = "REPOSITORY_SEGMENT_MISMATCH"
    WRITER_LEASE_HELD = "REPOSITORY_WRITER_LEASE_HELD"
    WRITER_LEASE_FAILED = "REPOSITORY_WRITER_LEASE_FAILED"


class RepositoryError(RuntimeError):
    """One repository authority, recovery, or immutable-data invariant failed."""

    def __init__(self, code: RepositoryErrorCode, message: str) -> None:
        if type(code) is not RepositoryErrorCode:
            raise TypeError("repository error code must be RepositoryErrorCode")
        self.code = code
        super().__init__(f"{code.value}: {message}")


class CurrentCacheStatus(StrEnum):
    EXACT = "CURRENT_EXACT"
    ABSENT_REPAIRED = "CURRENT_ABSENT_REPAIRED"
    STALE_REPAIRED = "CURRENT_STALE_REPAIRED"
    CORRUPT_REPAIRED = "CURRENT_CORRUPT_REPAIRED"
    GENESIS_ABSENT = "CURRENT_GENESIS_ABSENT"


class StartupIntegrityStatus(StrEnum):
    GENESIS_OVERLAY_ONLY = "GENESIS_OVERLAY_ONLY"
    AUTHENTICATED_CHECKPOINT_PLUS_TAIL = "AUTHENTICATED_CHECKPOINT_PLUS_TAIL"


class AuditIntegrityStatus(StrEnum):
    FULL_HISTORY_AUTHENTICATED = "FULL_HISTORY_AUTHENTICATED"


def _is_link_or_reparse_point(path: Path) -> bool:
    """Reject symbolic links and Windows reparse points without following them."""

    if path.is_symlink():
        return True
    if os.name != "nt":
        return False
    try:
        path_stat = os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    attributes = int(getattr(path_stat, "st_file_attributes", 0))
    reparse_mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse_mask)


@dataclass(frozen=True, slots=True)
class RepositoryPaths:
    root: Path
    segments: Path
    checkpoints: Path
    manifests: Path
    overlay: Path
    current: Path
    writer_lease: Path

    @classmethod
    def from_root(cls, root: Path) -> RepositoryPaths:
        if not isinstance(root, Path):
            raise TypeError("repository root must be pathlib.Path")
        selected = root.absolute()
        return cls(
            root=selected,
            segments=selected / "segments",
            checkpoints=selected / "checkpoints",
            manifests=selected / "manifests",
            overlay=selected / "overlay.sqlite3",
            current=selected / "CURRENT",
            writer_lease=selected / WRITER_LEASE_NAME,
        )

    def segment_path(self, physical_sha256: Hash32) -> Path:
        if type(physical_sha256) is not Hash32:
            raise TypeError("segment physical identity must be Hash32")
        return self.segments / f"{physical_sha256.hex()}{SEGMENT_SUFFIX}"

    def checkpoint_path(self, root: Hash32) -> Path:
        if type(root) is not Hash32:
            raise TypeError("checkpoint root must be Hash32")
        return self.checkpoints / f"{root.hex()}{CHECKPOINT_SUFFIX}"

    def manifest_path(self, root: Hash32) -> Path:
        if type(root) is not Hash32:
            raise TypeError("manifest root must be Hash32")
        return self.manifests / f"{root.hex()}{MANIFEST_SUFFIX}"


@dataclass(frozen=True, slots=True)
class RepositoryConfig:
    store_id: StoreId
    run_id: RunId
    mode: StorageMode
    run_identity: OpaqueIdentity
    config_identity: OpaqueIdentity
    code_identity: OpaqueIdentity
    runtime_identity: OpaqueIdentity
    start_prefix_root: Hash32
    genesis_base_commit_sequence: CommitSequence = field(
        default_factory=lambda: CommitSequence(0)
    )
    thresholds: OverlayThresholds = field(default_factory=OverlayThresholds)
    codec_profile: CodecProfile = field(default_factory=CodecProfile.zlib)

    def __post_init__(self) -> None:
        if type(self.store_id) is not StoreId or type(self.run_id) is not RunId:
            raise TypeError("repository store and run IDs must be explicit identifiers")
        if type(self.mode) is not StorageMode:
            raise TypeError("repository mode must be StorageMode")
        for identity in (
            self.run_identity,
            self.config_identity,
            self.code_identity,
            self.runtime_identity,
        ):
            if type(identity) is not OpaqueIdentity:
                raise TypeError("repository identities must be OpaqueIdentity")
        if type(self.start_prefix_root) is not Hash32:
            raise TypeError("repository start prefix root must be Hash32")
        if type(self.genesis_base_commit_sequence) is not CommitSequence:
            raise TypeError("repository genesis base sequence must be CommitSequence")
        if type(self.thresholds) is not OverlayThresholds:
            raise TypeError("repository thresholds must be OverlayThresholds")
        if type(self.codec_profile) is not CodecProfile:
            raise TypeError("repository codec profile must be CodecProfile")


@dataclass(frozen=True, slots=True)
class CurrentRecord:
    store_id: StoreId
    generation: int
    manifest_root: Hash32


@dataclass(frozen=True, slots=True)
class _ManifestRecoveryHint:
    named_root: Hash32
    generation: int
    parent_manifest_root: Hash32 | None


@dataclass(frozen=True, slots=True)
class StartupReport:
    integrity_status: StartupIntegrityStatus
    manifest_generation: int
    manifest_root: Hash32
    checkpoint_root: Hash32 | None
    checkpoint_state: CheckpointState | None
    base_commit_sequence: CommitSequence
    base_prefix_root: Hash32
    tail_frames: tuple[CommitFrame, ...]
    tail_entries_replayed: int
    tail_rows_replayed: int
    segments_read: int
    historical_segments_not_read: int
    historical_commits_not_read: int
    historical_rows_not_read: int
    checkpoint_used: bool
    current_cache_status: CurrentCacheStatus

    @property
    def integrity_result(self) -> str:
        return self.integrity_status.value


@dataclass(frozen=True, slots=True)
class SealResult:
    manifest: Manifest
    checkpoint: Checkpoint
    segment: SegmentArtifact
    manifest_path: Path
    checkpoint_path: Path
    segment_path: Path
    manifest_disposition: PublishDisposition
    checkpoint_disposition: PublishDisposition
    segment_disposition: PublishDisposition


@dataclass(frozen=True, slots=True)
class AuditReport:
    integrity_status: AuditIntegrityStatus
    manifest_generation: int
    manifest_root: Hash32
    checkpoint_root: Hash32
    manifests_read: int
    checkpoints_read: int
    segments_read: int
    commits_read: int
    rows_read: int
    physical_segment_bytes: int
    cumulative_stream_counts: tuple[CumulativeStreamCount, ...]
    checkpoint_state_witnesses: tuple[CheckpointStateWitness, ...]

    @property
    def integrity_result(self) -> str:
        return self.integrity_status.value

    @property
    def segment_count(self) -> int:
        return self.segments_read

    @property
    def checkpoint_count(self) -> int:
        return self.checkpoints_read

    @property
    def commit_count(self) -> int:
        return self.commits_read

    @property
    def row_count(self) -> int:
        return self.rows_read

    @property
    def physical_bytes(self) -> int:
        return self.physical_segment_bytes


def _repository_error(code: RepositoryErrorCode, message: str) -> RepositoryError:
    return RepositoryError(code, message)


def _overlay_identity(config: RepositoryConfig) -> OverlayIdentity:
    return OverlayIdentity(
        store_id=config.store_id,
        run_id=config.run_id,
        mode=config.mode,
        run_identity=config.run_identity,
        config_identity=config.config_identity,
        code_identity=config.code_identity,
        runtime_identity=config.runtime_identity,
        codec_profile=config.codec_profile,
        base_manifest_generation=GENESIS_MANIFEST_GENERATION,
        base_manifest_root=GENESIS_MANIFEST_ROOT,
        base_commit_sequence=config.genesis_base_commit_sequence,
        base_prefix_root=config.start_prefix_root,
        thresholds=config.thresholds,
    )


def _acquire_anchor_writer_lease(anchor: Anchor) -> AnchorWriterLease:
    try:
        return anchor.acquire_writer_lease()
    except AnchorError as error:
        code = (
            RepositoryErrorCode.WRITER_LEASE_HELD
            if error.code == AnchorErrorCode.WRITER_LEASE_HELD
            else RepositoryErrorCode.WRITER_LEASE_FAILED
        )
        raise _repository_error(code, "external anchor writer lease is unavailable") from error


def _checked_add(left: int, right: int, *, label: str) -> int:
    value = left + right
    if value > UINT64_MAX:
        raise _repository_error(
            RepositoryErrorCode.COUNTER_OVERFLOW,
            f"{label} exceeds uint64",
        )
    return value


def _descriptor_material(
    descriptor: SegmentDescriptor,
    *,
    checkpoint_root: Hash32 | None,
) -> bytes:
    values = [
        frame_hash32(descriptor.identity.digest),
        frame_text(descriptor.run_id.value),
        frame_u64(int(descriptor.first_commit_sequence)),
        frame_u64(int(descriptor.last_commit_sequence)),
        frame_hash32(descriptor.previous_prefix_root),
        frame_hash32(descriptor.end_prefix_root),
        frame_hash32(descriptor.merkle_root),
        frame_hash32(descriptor.physical_sha256),
        frame_u64(descriptor.physical_size),
        frame_u64(descriptor.logical_size),
        frame_u32(int(descriptor.commit_count)),
        frame_u32(len(descriptor.counts_by_stream)),
    ]
    for stream_id, count in descriptor.counts_by_stream:
        values.extend((frame_text(stream_id.value), frame_u32(int(count))))
    values.extend(
        (
            frame_text(descriptor.codec_profile),
            frame_optional_hash32(checkpoint_root),
        )
    )
    return b"".join(values)


def candidate_segment_descriptors_digest(
    descriptors: Sequence[SegmentDescriptor],
) -> Hash32:
    """Bind descriptors while blanking only the new checkpoint's circular root."""

    selected = tuple(descriptors)
    if not selected:
        raise ValueError("candidate descriptor set cannot be empty")
    values = [frame_u32(len(selected))]
    last_index = len(selected) - 1
    for index, descriptor in enumerate(selected):
        if type(descriptor) is not SegmentDescriptor:
            raise TypeError("candidate descriptors must be SegmentDescriptor values")
        checkpoint_root = None if index == last_index else descriptor.checkpoint_root
        values.append(
            frame_bytes(
                _descriptor_material(
                    descriptor,
                    checkpoint_root=checkpoint_root,
                )
            )
        )
    return framed_hash(
        DOMAIN_CANDIDATE_SEGMENT_DESCRIPTORS,
        frame_bytes(b"".join(values)),
    )


def _aggregate_counts(
    initial: Sequence[CumulativeStreamCount],
    added: Sequence[tuple[StreamId, LocalCount]],
) -> tuple[CumulativeStreamCount, ...]:
    counts: dict[StreamId, int] = {}
    for stream_id, count in initial:
        if type(stream_id) is not StreamId or type(count) is not int or count < 1:
            raise _repository_error(
                RepositoryErrorCode.SNAPSHOT_MISMATCH,
                "existing cumulative stream counts are invalid",
            )
        if stream_id in counts:
            raise _repository_error(
                RepositoryErrorCode.SNAPSHOT_MISMATCH,
                "existing cumulative stream counts contain a duplicate",
            )
        counts[stream_id] = count
    for stream_id, raw_count in added:
        count = int(raw_count)
        counts[stream_id] = _checked_add(
            counts.get(stream_id, 0),
            count,
            label="cumulative stream count",
        )
    return tuple(
        sorted(counts.items(), key=lambda item: item[0].value.encode("utf-8"))
    )


def _current_bytes(record: CurrentRecord) -> bytes:
    return canonical_json_bytes(
        {
            "format_version": CURRENT_FORMAT_VERSION,
            "generation": record.generation,
            "manifest_root": record.manifest_root.hex(),
            "store_id": record.store_id.value,
        }
    ) + b"\n"


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"unsupported JSON constant {value!r}")


def _current_from_bytes(data: bytes) -> CurrentRecord:
    if type(data) is not bytes or not data.endswith(b"\n") or data.endswith(b"\n\n"):
        raise ValueError("CURRENT must contain one LF-terminated canonical object")
    body = data[:-1]
    try:
        decoded = json.loads(
            body.decode("utf-8", errors="strict"),
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError("CURRENT is not strict JSON") from error
    if type(decoded) is not dict or canonical_json_bytes(decoded) != body:
        raise ValueError("CURRENT is not a canonical object")
    if set(decoded) != {"format_version", "generation", "manifest_root", "store_id"}:
        raise ValueError("CURRENT field set differs from version one")
    if decoded["format_version"] != CURRENT_FORMAT_VERSION:
        raise ValueError("CURRENT format version is unsupported")
    generation = decoded["generation"]
    if type(generation) is not int or generation < 1 or generation > UINT64_MAX:
        raise ValueError("CURRENT generation is invalid")
    try:
        return CurrentRecord(
            store_id=StoreId(cast(str, decoded["store_id"])),
            generation=generation,
            manifest_root=Hash32.from_hex(cast(str, decoded["manifest_root"])),
        )
    except (TypeError, ValueError) as error:
        raise ValueError("CURRENT typed fields are invalid") from error


def _overlay_fault_injector(
    fault_hook: FaultHook,
) -> Callable[[str], None] | None:
    if fault_hook is None:
        return None

    def inject(point: str) -> None:
        if point == FAULT_BEFORE_TRANSACTION:
            trigger_fault(fault_hook, FaultPoint.BEFORE_OVERLAY_TRANSACTION)
        elif point == FAULT_BEFORE_COMMIT:
            trigger_fault(fault_hook, FaultPoint.BEFORE_OVERLAY_COMMIT)
        elif point == FAULT_AFTER_COMMIT:
            trigger_fault(fault_hook, FaultPoint.AFTER_OVERLAY_TRANSACTION)

    return inject


def _lock_writer_fd(fd: int) -> None:
    os.lseek(fd, 0, os.SEEK_SET)
    if sys.platform == "win32":
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


@dataclass(slots=True)
class _WriterLease:
    """One process-scoped writer authority released by closing its file handle."""

    path: Path
    stream: BinaryIO
    closed: bool = False

    @classmethod
    def acquire(cls, path: Path) -> _WriterLease:
        if not isinstance(path, Path):
            raise TypeError("writer lease path must be pathlib.Path")
        flags = os.O_CREAT | os.O_RDWR
        flags |= getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_NOINHERIT", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags, 0o600)
        except OSError as error:
            raise _repository_error(
                RepositoryErrorCode.WRITER_LEASE_FAILED,
                "writer lease file cannot be opened safely",
            ) from error

        try:
            descriptor_stat = os.fstat(fd)
            path_stat = os.stat(path, follow_symlinks=False)
            if (
                not stat.S_ISREG(descriptor_stat.st_mode)
                or not stat.S_ISREG(path_stat.st_mode)
                or not os.path.samestat(descriptor_stat, path_stat)
            ):
                raise _repository_error(
                    RepositoryErrorCode.WRITER_LEASE_FAILED,
                    "writer lease path is not the opened regular file",
                )
            if descriptor_stat.st_size < 1:
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, b"\x00")
                os.fsync(fd)
            try:
                _lock_writer_fd(fd)
            except OSError as error:
                if error.errno in {
                    errno.EACCES,
                    errno.EAGAIN,
                    errno.EDEADLK,
                }:
                    raise _repository_error(
                        RepositoryErrorCode.WRITER_LEASE_HELD,
                        "another repository writer holds the store lease",
                    ) from error
                raise _repository_error(
                    RepositoryErrorCode.WRITER_LEASE_FAILED,
                    "operating system refused the writer lease",
                ) from error
            fsync_directory(path.parent)
            stream = cast(BinaryIO, os.fdopen(fd, "r+b"))
        except BaseException:
            os.close(fd)
            raise
        return cls(path=path, stream=stream)

    def close(self) -> None:
        if self.closed:
            return
        try:
            self.stream.close()
        finally:
            self.closed = True


class StorageRepository:
    """One explicit single-run checkpointed Storage v4 repository."""

    def __init__(
        self,
        *,
        paths: RepositoryPaths,
        anchor: Anchor,
        config: RepositoryConfig,
        overlay: SQLiteOverlay,
        manifest: Manifest | None,
        checkpoint: Checkpoint | None,
        anchor_writer_lease: AnchorWriterLease,
        writer_lease: _WriterLease,
        fault_hook: FaultHook,
        current_cache_status: CurrentCacheStatus,
    ) -> None:
        self._paths = paths
        self._anchor = anchor
        self._config = config
        self._overlay = overlay
        self._manifest = manifest
        self._checkpoint = checkpoint
        self._anchor_writer_lease = anchor_writer_lease
        self._writer_lease = writer_lease
        self._fault_hook = fault_hook
        self._current_cache_status = current_cache_status
        self._startup_segments_read = 0
        self._startup_commits_read = 0
        self._startup_rows_read = 0
        self._closed = False

    @classmethod
    def create(
        cls,
        root: Path,
        *,
        anchor: Anchor,
        config: RepositoryConfig,
        fault_hook: FaultHook = None,
    ) -> StorageRepository:
        """Create a fresh empty repository and strict generation-zero overlay."""

        if type(config) is not RepositoryConfig:
            raise TypeError("repository config must be RepositoryConfig")
        if anchor.store_id != config.store_id:
            raise _repository_error(
                RepositoryErrorCode.CONFIG_MISMATCH,
                "anchor store ID differs from repository config",
            )
        anchor_writer_lease = _acquire_anchor_writer_lease(anchor)
        writer_lease: _WriterLease | None = None
        overlay: SQLiteOverlay | None = None
        try:
            if anchor.read() is not None:
                raise _repository_error(
                    RepositoryErrorCode.AUTHORITY_MISMATCH,
                    "fresh repository requires an empty external anchor",
                )
            paths = RepositoryPaths.from_root(root)
            if paths.root.exists():
                raise _repository_error(
                    RepositoryErrorCode.ALREADY_EXISTS,
                    "repository root already exists",
                )
            if not paths.root.parent.is_dir():
                raise _repository_error(
                    RepositoryErrorCode.PATH_LAYOUT,
                    "repository root parent is missing",
                )
            try:
                paths.root.mkdir()
            except FileExistsError as error:
                raise _repository_error(
                    RepositoryErrorCode.ALREADY_EXISTS,
                    "repository root was created concurrently",
                ) from error
            writer_lease = _WriterLease.acquire(paths.writer_lease)
            paths.segments.mkdir()
            paths.checkpoints.mkdir()
            paths.manifests.mkdir()
            fsync_directory(paths.root)
            fsync_directory(paths.root.parent)
            overlay = SQLiteOverlay.create(
                paths.overlay,
                identity=_overlay_identity(config),
                fault_injector=_overlay_fault_injector(fault_hook),
            )
            return cls(
                paths=paths,
                anchor=anchor,
                config=config,
                overlay=overlay,
                manifest=None,
                checkpoint=None,
                anchor_writer_lease=anchor_writer_lease,
                writer_lease=writer_lease,
                fault_hook=fault_hook,
                current_cache_status=CurrentCacheStatus.GENESIS_ABSENT,
            )
        except BaseException:
            try:
                if overlay is not None:
                    overlay.close()
            finally:
                try:
                    if writer_lease is not None:
                        writer_lease.close()
                finally:
                    anchor_writer_lease.close()
            raise

    @classmethod
    def open_existing(
        cls,
        root: Path,
        *,
        anchor: Anchor,
        config: RepositoryConfig,
        fault_hook: FaultHook = None,
    ) -> StorageRepository:
        """Open from anchor authority, then authenticate checkpoint and tail."""

        if type(config) is not RepositoryConfig:
            raise TypeError("repository config must be RepositoryConfig")
        if anchor.store_id != config.store_id:
            raise _repository_error(
                RepositoryErrorCode.CONFIG_MISMATCH,
                "anchor store ID differs from repository config",
            )
        anchor_writer_lease = _acquire_anchor_writer_lease(anchor)
        writer_lease: _WriterLease | None = None
        overlay: SQLiteOverlay | None = None
        try:
            paths = RepositoryPaths.from_root(root)
            cls._verify_root(paths)
            writer_lease = _WriterLease.acquire(paths.writer_lease)
            cls._verify_paths(paths)
            anchored = anchor.read()
            manifest: Manifest | None = None
            checkpoint: Checkpoint | None = None
            if anchored is not None:
                manifest = cls._read_manifest(paths, anchored)
                cls._verify_manifest_config(manifest, anchored, config)
                checkpoint = cls._read_checkpoint(paths, manifest, config)

            overlay = SQLiteOverlay.open_existing(
                paths.overlay,
                expected_identity=_overlay_identity(config),
                fault_injector=_overlay_fault_injector(fault_hook),
            )
            repository = cls(
                paths=paths,
                anchor=anchor,
                config=config,
                overlay=overlay,
                manifest=manifest,
                checkpoint=checkpoint,
                anchor_writer_lease=anchor_writer_lease,
                writer_lease=writer_lease,
                fault_hook=fault_hook,
                current_cache_status=CurrentCacheStatus.GENESIS_ABSENT,
            )
            repository._recover_overlay(anchored)
            anchored = repository._recover_unanchored_successor(anchored)
            repository._current_cache_status = repository._repair_current(anchored)
            repository._validate_tail_attachment()
            return repository
        except BaseException:
            try:
                if overlay is not None:
                    overlay.close()
            finally:
                try:
                    if writer_lease is not None:
                        writer_lease.close()
                finally:
                    anchor_writer_lease.close()
            raise

    @staticmethod
    def _verify_root(paths: RepositoryPaths) -> None:
        if _is_link_or_reparse_point(paths.root) or not paths.root.is_dir():
            raise _repository_error(
                RepositoryErrorCode.MISSING,
                "repository root is missing or not a regular directory",
            )

    @classmethod
    def _verify_paths(cls, paths: RepositoryPaths) -> None:
        cls._verify_root(paths)
        for directory in (paths.segments, paths.checkpoints, paths.manifests):
            if _is_link_or_reparse_point(directory) or not directory.is_dir():
                raise _repository_error(
                    RepositoryErrorCode.PATH_LAYOUT,
                    f"repository directory is invalid: {directory.name}",
                )
        if _is_link_or_reparse_point(paths.overlay) or not paths.overlay.is_file():
            raise _repository_error(
                RepositoryErrorCode.PATH_LAYOUT,
                "repository overlay is missing or not a regular file",
            )

    @property
    def paths(self) -> RepositoryPaths:
        return self._paths

    @property
    def config(self) -> RepositoryConfig:
        return self._config

    @property
    def overlay_state(self) -> OverlayState:
        """Return O(1) transactional counters for seal polling.

        The row is updated atomically with every append. Full tail decoding is
        deliberately retained at seal, startup/recovery, and full audit rather
        than repeated after every commit in the hot import loop.
        """

        self._ensure_open()
        return self._overlay.state

    @property
    def manifest(self) -> Manifest | None:
        return self._manifest

    @property
    def checkpoint(self) -> Checkpoint | None:
        return self._checkpoint

    @property
    def startup_report(self) -> StartupReport:
        self._ensure_open()
        return self._build_startup_report(self._current_cache_status)

    def set_fault_hook(self, fault_hook: FaultHook) -> None:
        self._ensure_open()
        self._fault_hook = fault_hook
        self._overlay.set_fault_injector(_overlay_fault_injector(fault_hook))

    def append(self, frame: CommitFrame) -> bool:
        self._ensure_open()
        return self._overlay.append(frame)

    def discard_unsealed_tail(
        self,
        *,
        expected_manifest_root: Hash32,
        expected_checkpoint_root: Hash32,
    ) -> OverlayTailDiscardResult:
        """Discard the complete overlay tail at an exact published boundary."""

        self._ensure_open()
        if type(expected_manifest_root) is not Hash32:
            raise TypeError("expected_manifest_root must be Hash32")
        if type(expected_checkpoint_root) is not Hash32:
            raise TypeError("expected_checkpoint_root must be Hash32")

        anchored = self._anchor.read()
        if anchored is None:
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_EMPTY,
                "cannot discard an unsealed tail without published authority",
            )
        if anchored.manifest_root != expected_manifest_root:
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_MISMATCH,
                "anchored manifest differs from discard authority",
            )
        manifest = self._read_manifest(self._paths, anchored)
        self._verify_manifest_config(manifest, anchored, self._config)
        checkpoint = self._read_checkpoint(self._paths, manifest, self._config)
        if checkpoint.root != expected_checkpoint_root:
            raise _repository_error(
                RepositoryErrorCode.CHECKPOINT_MISMATCH,
                "anchored checkpoint differs from discard authority",
            )
        self._assert_no_manifest_fork(manifest)

        expected_anchor = AnchorRecord(
            store_id=self._config.store_id,
            generation=manifest.generation,
            manifest_root=manifest.identity.root,
        )
        if self._anchor.read() != expected_anchor:
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_MISMATCH,
                "external anchor changed during discard reattestation",
            )
        expected_base = OverlayState(
            run_id=self._config.run_id,
            base_manifest_generation=manifest.generation,
            base_manifest_root=manifest.identity.root,
            base_commit_sequence=manifest.head.commit_sequence,
            base_prefix_root=manifest.head.prefix_root,
            thresholds=self._config.thresholds,
            tail_commit_count=0,
            tail_row_count=0,
            tail_bytes=0,
            head_commit_sequence=manifest.head.commit_sequence,
            head_prefix_root=manifest.head.prefix_root,
        )
        result = self._overlay.discard_unsealed_tail(expected_base=expected_base)
        if self._anchor.read() != expected_anchor:
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_MISMATCH,
                "external anchor changed during atomic tail discard",
            )
        self._manifest = manifest
        self._checkpoint = checkpoint
        self._validate_tail_attachment()
        return result

    @staticmethod
    def _read_manifest(
        paths: RepositoryPaths,
        anchored: AnchorRecord,
    ) -> Manifest:
        path = paths.manifest_path(anchored.manifest_root)
        if path.is_symlink() or not path.is_file():
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_MISSING,
                "anchored manifest artifact is missing",
            )
        try:
            manifest = manifest_from_bytes(path.read_bytes())
        except (OSError, ManifestFormatError) as error:
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_MISMATCH,
                "anchored manifest artifact failed authentication",
            ) from error
        if (
            manifest.identity.root != anchored.manifest_root
            or manifest.generation != anchored.generation
        ):
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_MISMATCH,
                "anchored manifest generation or root differs",
            )
        return manifest

    @staticmethod
    def _verify_manifest_config(
        manifest: Manifest,
        anchored: AnchorRecord,
        config: RepositoryConfig,
    ) -> None:
        if anchored.store_id != config.store_id:
            raise _repository_error(
                RepositoryErrorCode.CONFIG_MISMATCH,
                "anchored record belongs to another store",
            )
        if manifest.store_id != config.store_id or manifest.run_id != config.run_id:
            raise _repository_error(
                RepositoryErrorCode.CONFIG_MISMATCH,
                "manifest store or run differs from repository config",
            )
        expected_identities = (
            config.run_identity,
            config.config_identity,
            config.code_identity,
            config.runtime_identity,
        )
        actual_identities = (
            manifest.run_identity,
            manifest.config_identity,
            manifest.code_identity,
            manifest.runtime_identity,
        )
        if actual_identities != expected_identities:
            raise _repository_error(
                RepositoryErrorCode.CONFIG_MISMATCH,
                "manifest attested identities differ from repository config",
            )
        if manifest.start_prefix_root != config.start_prefix_root:
            raise _repository_error(
                RepositoryErrorCode.CONFIG_MISMATCH,
                "manifest starting prefix differs from repository config",
            )
        first = manifest.segments[0].first_commit_sequence
        base = int(config.genesis_base_commit_sequence)
        if base == UINT64_MAX or int(first) != base + 1:
            raise _repository_error(
                RepositoryErrorCode.CONFIG_MISMATCH,
                "manifest first commit does not follow the explicit genesis base",
            )

    @staticmethod
    def _manifest_counts(
        manifest: Manifest,
    ) -> tuple[CumulativeStreamCount, ...]:
        result: tuple[CumulativeStreamCount, ...] = ()
        for descriptor in manifest.segments:
            result = _aggregate_counts(result, descriptor.counts_by_stream)
        return result

    @classmethod
    def _read_checkpoint(
        cls,
        paths: RepositoryPaths,
        manifest: Manifest,
        config: RepositoryConfig,
    ) -> Checkpoint:
        descriptor = manifest.segments[-1]
        checkpoint_root = descriptor.checkpoint_root
        if checkpoint_root is None:
            raise _repository_error(
                RepositoryErrorCode.CHECKPOINT_MISMATCH,
                "anchored manifest head has no complete checkpoint",
            )
        path = paths.checkpoint_path(checkpoint_root)
        if path.is_symlink() or not path.is_file():
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_MISSING,
                "anchored checkpoint artifact is missing",
            )
        try:
            checkpoint = checkpoint_from_bytes(path.read_bytes())
            verify_checkpoint(
                checkpoint,
                expected_store_id=config.store_id,
                expected_run_id=config.run_id,
                expected_mode=config.mode.value,
                expected_target_manifest_generation=manifest.generation,
                expected_parent_manifest_root=manifest.parent_manifest_root,
                expected_start_prefix_root=config.start_prefix_root,
                expected_covered_commit_sequence=manifest.head.commit_sequence,
                expected_covered_prefix_root=manifest.head.prefix_root,
                expected_covered_segment_identity=manifest.head.segment_identity,
                expected_candidate_segment_descriptors_digest=(
                    candidate_segment_descriptors_digest(manifest.segments)
                ),
                expected_run_identity=config.run_identity,
                expected_config_identity=config.config_identity,
                expected_code_identity=config.code_identity,
                expected_runtime_identity=config.runtime_identity,
            )
        except (OSError, ValueError) as error:
            raise _repository_error(
                RepositoryErrorCode.CHECKPOINT_MISMATCH,
                "anchored checkpoint failed authentication or binding",
            ) from error
        historical_count = sum(
            int(item.commit_count) for item in manifest.segments
        )
        if (
            checkpoint.root != checkpoint_root
            or checkpoint.historical_commit_count != historical_count
            or checkpoint.cumulative_stream_counts != cls._manifest_counts(manifest)
        ):
            raise _repository_error(
                RepositoryErrorCode.CHECKPOINT_MISMATCH,
                "checkpoint cumulative history differs from manifest descriptors",
            )
        return checkpoint

    def _recover_overlay(self, anchored: AnchorRecord | None) -> None:
        state = self._overlay.verify_integrity()
        if state.thresholds != self._config.thresholds:
            raise _repository_error(
                RepositoryErrorCode.CONFIG_MISMATCH,
                "overlay thresholds differ from repository config",
            )
        if anchored is None:
            expected = (
                GENESIS_MANIFEST_GENERATION,
                GENESIS_MANIFEST_ROOT,
                self._config.genesis_base_commit_sequence,
                self._config.start_prefix_root,
            )
            actual = (
                state.base_manifest_generation,
                state.base_manifest_root,
                state.base_commit_sequence,
                state.base_prefix_root,
            )
            if actual != expected:
                raise _repository_error(
                    RepositoryErrorCode.OVERLAY_AHEAD,
                    "empty anchor cannot authorize a non-genesis overlay base",
                )
            return

        if self._manifest is None or self._checkpoint is None:
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_EMPTY,
                "nonempty anchor has no loaded manifest and checkpoint",
            )
        if state.base_manifest_generation > anchored.generation:
            raise _repository_error(
                RepositoryErrorCode.OVERLAY_AHEAD,
                "overlay base generation is ahead of the external anchor",
            )
        if state.base_manifest_generation == anchored.generation:
            if state.base_manifest_root != anchored.manifest_root:
                raise _repository_error(
                    RepositoryErrorCode.OVERLAY_FORK,
                    "overlay base forks the anchored generation",
                )
        else:
            self._verify_overlay_base_is_ancestor(state)
            self._overlay.advance_base(
                manifest_generation=anchored.generation,
                manifest_root=anchored.manifest_root,
                base_commit_sequence=self._manifest.head.commit_sequence,
                base_prefix_root=self._manifest.head.prefix_root,
            )
            state = self._overlay.verify_integrity()
        if (
            state.base_manifest_generation != anchored.generation
            or state.base_manifest_root != anchored.manifest_root
            or state.base_commit_sequence != self._manifest.head.commit_sequence
            or state.base_prefix_root != self._manifest.head.prefix_root
        ):
            raise _repository_error(
                RepositoryErrorCode.OVERLAY_FORK,
                "overlay authenticated base differs from anchored manifest head",
            )

    def _verify_overlay_base_is_ancestor(self, state: OverlayState) -> None:
        if self._manifest is None:
            raise AssertionError("anchored manifest must be loaded")
        if state.base_manifest_generation == GENESIS_MANIFEST_GENERATION:
            if (
                state.base_manifest_root != GENESIS_MANIFEST_ROOT
                or state.base_commit_sequence
                != self._config.genesis_base_commit_sequence
                or state.base_prefix_root != self._config.start_prefix_root
            ):
                raise _repository_error(
                    RepositoryErrorCode.OVERLAY_FORK,
                    "overlay genesis base differs from explicit repository genesis",
                )
            return
        chain = self._load_manifest_chain(self._manifest)
        ancestor = next(
            (
                item
                for item in chain
                if item.generation == state.base_manifest_generation
            ),
            None,
        )
        if ancestor is None or ancestor.identity.root != state.base_manifest_root:
            raise _repository_error(
                RepositoryErrorCode.OVERLAY_FORK,
                "overlay base is not an ancestor of the anchored manifest",
            )
        if (
            ancestor.head.commit_sequence != state.base_commit_sequence
            or ancestor.head.prefix_root != state.base_prefix_root
        ):
            raise _repository_error(
                RepositoryErrorCode.OVERLAY_FORK,
                "overlay base head differs from its authenticated ancestor",
            )

    def _repair_current(
        self,
        anchored: AnchorRecord | None,
    ) -> CurrentCacheStatus:
        if anchored is None:
            return CurrentCacheStatus.GENESIS_ABSENT
        expected = CurrentRecord(
            store_id=self._config.store_id,
            generation=anchored.generation,
            manifest_root=anchored.manifest_root,
        )
        if self._paths.current.is_symlink():
            status = CurrentCacheStatus.CORRUPT_REPAIRED
            self._paths.current.unlink()
        elif not self._paths.current.exists():
            status = CurrentCacheStatus.ABSENT_REPAIRED
        else:
            try:
                observed = _current_from_bytes(self._paths.current.read_bytes())
            except (OSError, ValueError):
                status = CurrentCacheStatus.CORRUPT_REPAIRED
            else:
                if observed == expected:
                    return CurrentCacheStatus.EXACT
                status = CurrentCacheStatus.STALE_REPAIRED
        trigger_fault(self._fault_hook, FaultPoint.BEFORE_CURRENT_PUBLICATION)
        atomic_write_mutable_cache(
            self._paths.current,
            _current_bytes(expected),
            verifier=_current_from_bytes,
            fault_hook=self._fault_hook,
        )
        trigger_fault(self._fault_hook, FaultPoint.AFTER_CURRENT_PUBLICATION)
        return status

    def _validate_tail_attachment(self) -> None:
        state = self._overlay.verify_integrity()
        frames = self._overlay.frames()
        if frames and (
            frames[0].previous_prefix_root != state.base_prefix_root
            or int(frames[0].commit_sequence)
            != int(state.base_commit_sequence) + 1
        ):
            raise _repository_error(
                RepositoryErrorCode.OVERLAY_FORK,
                "overlay tail does not attach to its authenticated base",
            )

    def _recover_unanchored_successor(
        self,
        anchored: AnchorRecord | None,
    ) -> AnchorRecord | None:
        """Adopt one exact interrupted-seal successor before admitting writes."""

        if self._manifest is None:
            parent_segments: tuple[SegmentDescriptor, ...] = ()
            current_generation = 0
            current_root = None
            expected_generation = 1
            expected_parent_root = None
        else:
            parent_segments = self._manifest.segments
            current_generation = self._manifest.generation
            current_root = self._manifest.identity.root
            expected_generation = self._manifest.generation + 1
            expected_parent_root = self._manifest.identity.root
        successors: list[Manifest] = []
        for path in self._paths.manifests.glob(f"*{MANIFEST_SUFFIX}"):
            hint = self._read_manifest_recovery_hint(path)
            if hint.generation < current_generation:
                continue
            if hint.generation == current_generation:
                if current_root is None or hint.named_root != current_root:
                    raise _repository_error(
                        RepositoryErrorCode.MANIFEST_FORK,
                        "manifest namespace contains a current-generation fork",
                    )
                continue
            if (
                hint.generation != expected_generation
                or hint.parent_manifest_root != expected_parent_root
            ):
                raise _repository_error(
                    RepositoryErrorCode.MANIFEST_FORK,
                    "manifest namespace contains a non-direct successor",
                )
            try:
                candidate = manifest_from_bytes(path.read_bytes())
            except (OSError, ManifestFormatError) as error:
                raise _repository_error(
                    RepositoryErrorCode.MANIFEST_FORK,
                    "unanchored manifest successor is unreadable",
                ) from error
            if candidate.identity.root != hint.named_root:
                raise _repository_error(
                    RepositoryErrorCode.MANIFEST_FORK,
                    "unanchored manifest filename differs from its authenticated root",
                )
            successors.append(candidate)
        if not successors:
            return anchored
        if len(successors) != 1:
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "manifest namespace has multiple unauthenticated successors",
            )
        candidate = successors[0]
        if (
            candidate.generation != expected_generation
            or candidate.parent_manifest_root != expected_parent_root
            or len(candidate.segments) != len(parent_segments) + 1
            or candidate.segments[: len(parent_segments)] != parent_segments
        ):
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "manifest namespace contains a non-direct successor",
            )
        try:
            if self._manifest is None:
                verify_manifest(candidate, expected_generation=1)
            else:
                verify_manifest_transition(self._manifest, candidate)
        except ManifestFormatError as error:
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "unanchored manifest successor is not append-only",
            ) from error

        candidate_record = AnchorRecord(
            store_id=self._config.store_id,
            generation=candidate.generation,
            manifest_root=candidate.identity.root,
        )
        self._verify_manifest_config(candidate, candidate_record, self._config)
        checkpoint = self._read_checkpoint(self._paths, candidate, self._config)
        added_descriptor = candidate.segments[-1]
        segment = self._read_segment_descriptor(added_descriptor)
        self._startup_segments_read = 1
        self._startup_commits_read = len(segment.commits)
        self._startup_rows_read = sum(len(frame.rows) for frame in segment.commits)
        tail = self._overlay.frames()
        covered = len(segment.commits)
        if covered > len(tail) or tuple(tail[:covered]) != segment.commits:
            raise _repository_error(
                RepositoryErrorCode.OVERLAY_FORK,
                "unanchored manifest successor does not cover an exact overlay prefix",
            )

        self._anchor.compare_and_swap(anchored, candidate_record)
        self._manifest = candidate
        self._checkpoint = checkpoint
        self._overlay.advance_base(
            manifest_generation=candidate.generation,
            manifest_root=candidate.identity.root,
            base_commit_sequence=candidate.head.commit_sequence,
            base_prefix_root=candidate.head.prefix_root,
        )
        return candidate_record

    def _read_manifest_recovery_hint(self, path: Path) -> _ManifestRecoveryHint:
        """Read only the bounded manifest prefix needed to find a direct successor."""

        if _is_link_or_reparse_point(path) or not path.is_file():
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "manifest namespace contains a non-regular artifact",
            )
        try:
            named_root = Hash32.from_hex(path.name.removesuffix(MANIFEST_SUFFIX))
        except ValueError as error:
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "manifest namespace contains an invalid content-addressed name",
            ) from error
        body_identity_prefix = b"".join(
            (
                PROTOCOL_VERSION.to_bytes(2, byteorder="big", signed=False),
                frame_text(self._config.store_id.value),
                frame_text(self._config.run_id.value),
            )
        )
        physical_prefix_size = len(MANIFEST_MAGIC) + 2 + 8
        required_size = physical_prefix_size + len(body_identity_prefix) + 8 + 1
        maximum_hint_size = required_size + 32
        limits = ManifestReadLimits()
        try:
            path_size = path.stat(follow_symlinks=False).st_size
            with path.open("rb") as handle:
                prefix = handle.read(maximum_hint_size)
        except OSError as error:
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "manifest recovery prefix cannot be read",
            ) from error
        if (
            path_size < required_size + 32
            or path_size > limits.max_physical_size
            or len(prefix) < required_size
        ):
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "manifest recovery candidate size is invalid",
            )
        if (
            prefix[: len(MANIFEST_MAGIC)] != MANIFEST_MAGIC
            or int.from_bytes(
                prefix[len(MANIFEST_MAGIC) : len(MANIFEST_MAGIC) + 2],
                byteorder="big",
                signed=False,
            )
            != MANIFEST_FORMAT_VERSION
        ):
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "manifest recovery candidate has an invalid physical header",
            )
        body_size_offset = len(MANIFEST_MAGIC) + 2
        body_size = int.from_bytes(
            prefix[body_size_offset : body_size_offset + 8],
            byteorder="big",
            signed=False,
        )
        if (
            body_size > limits.max_body_size
            or path_size != physical_prefix_size + body_size + 32
            or prefix[
                physical_prefix_size : physical_prefix_size
                + len(body_identity_prefix)
            ]
            != body_identity_prefix
        ):
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "manifest recovery candidate identity prefix is invalid",
            )
        generation_offset = physical_prefix_size + len(body_identity_prefix)
        generation = int.from_bytes(
            prefix[generation_offset : generation_offset + 8],
            byteorder="big",
            signed=False,
        )
        if generation < 1:
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "manifest recovery candidate generation is invalid",
            )
        parent_tag_offset = generation_offset + 8
        parent_tag = prefix[parent_tag_offset]
        if parent_tag == 0:
            parent = None
        elif parent_tag == 1 and len(prefix) >= parent_tag_offset + 33:
            parent = Hash32(prefix[parent_tag_offset + 1 : parent_tag_offset + 33])
        else:
            raise _repository_error(
                RepositoryErrorCode.MANIFEST_FORK,
                "manifest recovery candidate parent encoding is invalid",
            )
        return _ManifestRecoveryHint(
            named_root=named_root,
            generation=generation,
            parent_manifest_root=parent,
        )

    def _build_startup_report(
        self,
        current_status: CurrentCacheStatus,
    ) -> StartupReport:
        state = self._overlay.verify_integrity()
        frames = self._overlay.frames()
        tail_rows = sum(len(frame.rows) for frame in frames)
        if self._manifest is None:
            integrity = StartupIntegrityStatus.GENESIS_OVERLAY_ONLY
            generation = GENESIS_MANIFEST_GENERATION
            manifest_root = GENESIS_MANIFEST_ROOT
            checkpoint_root = None
            checkpoint_state = None
            historical_segments = 0
            historical_commits = 0
            historical_rows = 0
            checkpoint_used = False
        else:
            if self._checkpoint is None:
                raise AssertionError("anchored manifest requires checkpoint")
            integrity = StartupIntegrityStatus.AUTHENTICATED_CHECKPOINT_PLUS_TAIL
            generation = self._manifest.generation
            manifest_root = self._manifest.identity.root
            checkpoint_root = self._checkpoint.root
            checkpoint_state = self._checkpoint.state
            historical_segments = len(self._manifest.segments)
            historical_commits = self._checkpoint.historical_commit_count
            historical_rows = sum(
                count for _, count in self._checkpoint.cumulative_stream_counts
            )
            checkpoint_used = True
        if (
            self._startup_segments_read > historical_segments
            or self._startup_commits_read > historical_commits
            or self._startup_rows_read > historical_rows
        ):
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_MISMATCH,
                "startup recovery counters exceed authenticated history",
            )
        return StartupReport(
            integrity_status=integrity,
            manifest_generation=generation,
            manifest_root=manifest_root,
            checkpoint_root=checkpoint_root,
            checkpoint_state=checkpoint_state,
            base_commit_sequence=state.base_commit_sequence,
            base_prefix_root=state.base_prefix_root,
            tail_frames=frames,
            tail_entries_replayed=len(frames),
            tail_rows_replayed=tail_rows,
            segments_read=self._startup_segments_read,
            historical_segments_not_read=(
                historical_segments - self._startup_segments_read
            ),
            historical_commits_not_read=(
                historical_commits - self._startup_commits_read
            ),
            historical_rows_not_read=historical_rows - self._startup_rows_read,
            checkpoint_used=checkpoint_used,
            current_cache_status=current_status,
        )

    def startup(self) -> StartupReport:
        """Reattest anchor/checkpoint/tail without opening historical segments."""

        self._ensure_open()
        anchored = self._anchor.read()
        if anchored is None:
            if self._manifest is not None:
                raise _repository_error(
                    RepositoryErrorCode.AUTHORITY_MISMATCH,
                    "external anchor disappeared after repository open",
                )
        else:
            manifest = self._read_manifest(self._paths, anchored)
            self._verify_manifest_config(manifest, anchored, self._config)
            checkpoint = self._read_checkpoint(self._paths, manifest, self._config)
            self._manifest = manifest
            self._checkpoint = checkpoint
        self._recover_overlay(anchored)
        self._validate_tail_attachment()
        self._current_cache_status = self._repair_current(anchored)
        return self._build_startup_report(self._current_cache_status)

    def seal(
        self,
        *,
        checkpoint_state: CheckpointState,
        cumulative_stream_counts: tuple[CumulativeStreamCount, ...],
        historical_commit_count: int,
    ) -> SealResult:
        """Seal the complete tail and publish authority in one fixed order."""

        self._ensure_open()
        if type(checkpoint_state) is not CheckpointState:
            raise TypeError("seal checkpoint_state must be CheckpointState")
        if type(cumulative_stream_counts) is not tuple:
            raise TypeError("seal cumulative stream counts must be a tuple")
        if type(historical_commit_count) is not int:
            raise TypeError("seal historical commit count must be an exact integer")
        frames = self._overlay.frames()
        if not frames:
            raise _repository_error(
                RepositoryErrorCode.EMPTY_SEAL,
                "cannot seal an empty overlay tail",
            )
        if len(frames) > UINT32_MAX:
            raise _repository_error(
                RepositoryErrorCode.COUNTER_OVERFLOW,
                "one segment cannot contain more than uint32 commits",
            )
        expected_anchor = (
            None
            if self._manifest is None
            else AnchorRecord(
                store_id=self._config.store_id,
                generation=self._manifest.generation,
                manifest_root=self._manifest.identity.root,
            )
        )
        if self._anchor.read() != expected_anchor:
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_MISMATCH,
                "external anchor changed before sealing",
            )
        parent_checkpoint = self._checkpoint
        prior_counts = (
            () if parent_checkpoint is None else parent_checkpoint.cumulative_stream_counts
        )
        prior_commit_count = (
            0 if parent_checkpoint is None else parent_checkpoint.historical_commit_count
        )
        segment = build_segment(frames, codec=self._config.codec_profile)
        expected_counts = _aggregate_counts(prior_counts, segment.counts_by_stream)
        expected_commit_count = _checked_add(
            prior_commit_count,
            segment.commit_count,
            label="historical commit count",
        )
        if (
            cumulative_stream_counts != expected_counts
            or historical_commit_count != expected_commit_count
        ):
            raise _repository_error(
                RepositoryErrorCode.SNAPSHOT_MISMATCH,
                "seal counters differ from parent checkpoint plus authenticated tail",
            )

        segment_path = self._paths.segment_path(segment.physical_sha256)
        trigger_fault(self._fault_hook, FaultPoint.BEFORE_SEGMENT_PUBLICATION)
        segment_publication = durable_publish_immutable(
            segment_path,
            segment.data,
            verifier=lambda data: self._verify_segment_artifact(data, segment),
            fault_hook=self._fault_hook,
        )
        trigger_fault(self._fault_hook, FaultPoint.AFTER_SEGMENT_PUBLICATION)

        parent_descriptors = (
            () if self._manifest is None else self._manifest.segments
        )
        provisional_descriptor = SegmentDescriptor.from_segment(segment)
        provisional_descriptors = (*parent_descriptors, provisional_descriptor)
        generation = 1 if self._manifest is None else self._manifest.generation + 1
        parent_root = (
            None if self._manifest is None else self._manifest.identity.root
        )
        checkpoint = build_checkpoint(
            store_id=self._config.store_id,
            run_id=self._config.run_id,
            mode=self._config.mode.value,
            target_manifest_generation=generation,
            parent_manifest_root=parent_root,
            start_prefix_root=self._config.start_prefix_root,
            covered_commit_sequence=segment.last_commit_sequence,
            covered_prefix_root=segment.end_prefix_root,
            covered_segment_identity=segment.identity,
            candidate_segment_descriptors_digest=(
                candidate_segment_descriptors_digest(provisional_descriptors)
            ),
            run_identity=self._config.run_identity,
            config_identity=self._config.config_identity,
            code_identity=self._config.code_identity,
            runtime_identity=self._config.runtime_identity,
            historical_commit_count=historical_commit_count,
            cumulative_stream_counts=cumulative_stream_counts,
            state=checkpoint_state,
        )
        checkpoint_path = self._paths.checkpoint_path(checkpoint.root)
        checkpoint_bytes = checkpoint_to_bytes(checkpoint)
        trigger_fault(self._fault_hook, FaultPoint.BEFORE_CHECKPOINT_PUBLICATION)
        checkpoint_publication = durable_publish_immutable(
            checkpoint_path,
            checkpoint_bytes,
            verifier=lambda data: self._verify_checkpoint_artifact(data, checkpoint),
            fault_hook=self._fault_hook,
        )
        trigger_fault(self._fault_hook, FaultPoint.AFTER_CHECKPOINT_PUBLICATION)

        descriptor = replace(
            provisional_descriptor,
            checkpoint_root=checkpoint.root,
        )
        descriptors = (*parent_descriptors, descriptor)
        manifest = build_manifest(
            store_id=self._config.store_id,
            run_id=self._config.run_id,
            generation=generation,
            parent_manifest_root=parent_root,
            run_identity=self._config.run_identity,
            config_identity=self._config.config_identity,
            code_identity=self._config.code_identity,
            runtime_identity=self._config.runtime_identity,
            start_prefix_root=self._config.start_prefix_root,
            segments=descriptors,
        )
        if self._manifest is None:
            verify_manifest(
                manifest,
                expected_generation=1,
            )
        else:
            verify_manifest_transition(self._manifest, manifest)
        self._assert_no_manifest_fork(manifest)
        manifest_path = self._paths.manifest_path(manifest.identity.root)
        manifest_bytes = manifest_to_bytes(manifest)
        trigger_fault(self._fault_hook, FaultPoint.BEFORE_MANIFEST_PUBLICATION)
        manifest_publication = durable_publish_immutable(
            manifest_path,
            manifest_bytes,
            verifier=lambda data: self._verify_manifest_artifact(data, manifest),
            fault_hook=self._fault_hook,
        )
        trigger_fault(self._fault_hook, FaultPoint.AFTER_MANIFEST_PUBLICATION)

        anchored = AnchorRecord(
            store_id=self._config.store_id,
            generation=manifest.generation,
            manifest_root=manifest.identity.root,
        )
        trigger_fault(self._fault_hook, FaultPoint.BEFORE_ANCHOR_PUBLICATION)
        self._anchor.compare_and_swap(expected_anchor, anchored)
        trigger_fault(self._fault_hook, FaultPoint.AFTER_ANCHOR_PUBLICATION)
        self._current_cache_status = self._repair_current(anchored)
        self._overlay.advance_base(
            manifest_generation=manifest.generation,
            manifest_root=manifest.identity.root,
            base_commit_sequence=manifest.head.commit_sequence,
            base_prefix_root=manifest.head.prefix_root,
        )
        self._manifest = manifest
        self._checkpoint = checkpoint
        self._validate_tail_attachment()
        return SealResult(
            manifest=manifest,
            checkpoint=checkpoint,
            segment=segment,
            manifest_path=manifest_path,
            checkpoint_path=checkpoint_path,
            segment_path=segment_path,
            manifest_disposition=manifest_publication.disposition,
            checkpoint_disposition=checkpoint_publication.disposition,
            segment_disposition=segment_publication.disposition,
        )

    @staticmethod
    def _verify_segment_artifact(
        data: bytes,
        expected: SegmentArtifact,
    ) -> None:
        observed = read_segment(data)
        if observed != expected:
            raise _repository_error(
                RepositoryErrorCode.SEGMENT_MISMATCH,
                "published segment read-back differs",
            )

    @staticmethod
    def _verify_checkpoint_artifact(
        data: bytes,
        expected: Checkpoint,
    ) -> None:
        if checkpoint_from_bytes(data) != expected:
            raise _repository_error(
                RepositoryErrorCode.CHECKPOINT_MISMATCH,
                "published checkpoint read-back differs",
            )

    @staticmethod
    def _verify_manifest_artifact(
        data: bytes,
        expected: Manifest,
    ) -> None:
        if manifest_from_bytes(data) != expected:
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_MISMATCH,
                "published manifest read-back differs",
            )

    def _scan_manifest_namespace(self) -> tuple[Manifest, ...]:
        manifests: list[Manifest] = []
        for path in self._paths.manifests.glob(f"*{MANIFEST_SUFFIX}"):
            if _is_link_or_reparse_point(path) or not path.is_file():
                raise _repository_error(
                    RepositoryErrorCode.MANIFEST_FORK,
                    "manifest namespace contains a non-regular artifact",
                )
            try:
                observed = manifest_from_bytes(path.read_bytes())
            except (OSError, ManifestFormatError) as error:
                raise _repository_error(
                    RepositoryErrorCode.MANIFEST_FORK,
                    "manifest namespace contains an invalid immutable artifact",
                ) from error
            expected_name = f"{observed.identity.root.hex()}{MANIFEST_SUFFIX}"
            if path.name != expected_name:
                raise _repository_error(
                    RepositoryErrorCode.MANIFEST_FORK,
                    "manifest artifact name differs from its content address",
                )
            manifests.append(observed)
        return tuple(
            sorted(
                manifests,
                key=lambda manifest: (
                    manifest.generation,
                    bytes(manifest.identity.root),
                ),
            )
        )

    def _assert_no_manifest_fork(self, candidate: Manifest) -> None:
        for observed in self._scan_manifest_namespace():
            if (
                observed.generation == candidate.generation
                and observed.identity.root != candidate.identity.root
            ):
                raise _repository_error(
                    RepositoryErrorCode.MANIFEST_FORK,
                    "two manifest roots claim the same generation",
                )

    def _assert_no_manifest_forks(self) -> None:
        roots_by_generation: dict[int, Hash32] = {}
        for manifest in self._scan_manifest_namespace():
            prior = roots_by_generation.setdefault(
                manifest.generation,
                manifest.identity.root,
            )
            if prior != manifest.identity.root:
                raise _repository_error(
                    RepositoryErrorCode.MANIFEST_FORK,
                    "manifest namespace contains a same-generation fork",
                )

    def _load_manifest_chain(self, latest: Manifest) -> tuple[Manifest, ...]:
        descending = [latest]
        seen = {latest.identity.root}
        child = latest
        while child.generation > 1:
            parent_root = child.parent_manifest_root
            if parent_root is None or parent_root in seen:
                raise _repository_error(
                    RepositoryErrorCode.AUTHORITY_MISMATCH,
                    "manifest parent chain is missing or cyclic",
                )
            parent_record = AnchorRecord(
                store_id=self._config.store_id,
                generation=child.generation - 1,
                manifest_root=parent_root,
            )
            parent = self._read_manifest(self._paths, parent_record)
            self._verify_manifest_config(parent, parent_record, self._config)
            try:
                verify_manifest_transition(parent, child)
            except ManifestFormatError as error:
                raise _repository_error(
                    RepositoryErrorCode.AUTHORITY_MISMATCH,
                    "manifest transition is not append-only",
                ) from error
            descending.append(parent)
            seen.add(parent.identity.root)
            child = parent
        return tuple(reversed(descending))

    def _read_segment_descriptor(
        self,
        descriptor: SegmentDescriptor,
    ) -> SegmentArtifact:
        path = self._paths.segment_path(descriptor.physical_sha256)
        if path.is_symlink() or not path.is_file():
            raise _repository_error(
                RepositoryErrorCode.SEGMENT_MISSING,
                "manifest-declared segment is missing",
            )
        try:
            segment = read_segment(path.read_bytes())
        except (OSError, SegmentFormatError) as error:
            raise _repository_error(
                RepositoryErrorCode.SEGMENT_MISMATCH,
                "manifest-declared segment failed exhaustive verification",
            ) from error
        expected = SegmentDescriptor.from_segment(
            segment,
            checkpoint_root=descriptor.checkpoint_root,
        )
        if expected != descriptor:
            raise _repository_error(
                RepositoryErrorCode.SEGMENT_MISMATCH,
                "segment bytes differ from the manifest descriptor",
            )
        return segment

    def full_audit(
        self,
        *,
        progress: AuditProgressCallback | None = None,
        heartbeat_interval_seconds: float = AUDIT_HEARTBEAT_MIN_SECONDS,
    ) -> AuditReport:
        """Read and authenticate every manifest, checkpoint, and segment."""

        self._ensure_open()
        self.startup()
        if self._manifest is None or self._checkpoint is None:
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_EMPTY,
                "full audit requires at least one anchored seal",
            )
        audit_progress = BoundedAuditProgress(
            phase="paper_full_audit",
            progress=progress,
            totals={
                "commits": self._checkpoint.historical_commit_count,
                "rows": sum(
                    count for _, count in self._checkpoint.cumulative_stream_counts
                ),
                "segments": len(self._manifest.segments),
            },
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )
        chain = self._load_manifest_chain(self._manifest)
        self._assert_no_manifest_forks()
        checkpoints = tuple(
            self._read_checkpoint(self._paths, manifest, self._config)
            for manifest in chain
        )
        checkpoint_state_witnesses = tuple(
            CheckpointStateWitness(
                covered_commit_sequence=checkpoint.covered_commit_sequence,
                state_sha256=checkpoint_state_sha256(checkpoint.state),
            )
            for checkpoint in checkpoints
        )
        commits = 0
        rows = 0
        physical_bytes = 0
        observed_counts: tuple[CumulativeStreamCount, ...] = ()
        for segment_index, descriptor in enumerate(self._manifest.segments, start=1):
            segment = self._read_segment_descriptor(descriptor)
            commits = _checked_add(
                commits,
                segment.commit_count,
                label="audited commit count",
            )
            rows = _checked_add(
                rows,
                sum(len(frame.rows) for frame in segment.commits),
                label="audited row count",
            )
            physical_bytes = _checked_add(
                physical_bytes,
                segment.physical_size,
                label="audited physical bytes",
            )
            observed_counts = _aggregate_counts(
                observed_counts,
                segment.counts_by_stream,
            )
            audit_progress.advance(
                {
                    "commits": commits,
                    "rows": rows,
                    "segments": segment_index,
                }
            )
        if (
            commits != self._checkpoint.historical_commit_count
            or observed_counts != self._checkpoint.cumulative_stream_counts
            or rows != sum(count for _, count in observed_counts)
        ):
            raise _repository_error(
                RepositoryErrorCode.CHECKPOINT_MISMATCH,
                "full audit counters differ from the latest complete checkpoint",
            )
        checkpoint_root = self._manifest.segments[-1].checkpoint_root
        if checkpoint_root is None:
            raise AssertionError("latest manifest checkpoint was already verified")
        report = AuditReport(
            integrity_status=AuditIntegrityStatus.FULL_HISTORY_AUTHENTICATED,
            manifest_generation=self._manifest.generation,
            manifest_root=self._manifest.identity.root,
            checkpoint_root=checkpoint_root,
            manifests_read=len(chain),
            checkpoints_read=len(checkpoints),
            segments_read=len(self._manifest.segments),
            commits_read=commits,
            rows_read=rows,
            physical_segment_bytes=physical_bytes,
            cumulative_stream_counts=observed_counts,
            checkpoint_state_witnesses=checkpoint_state_witnesses,
        )
        audit_progress.complete(
            {
                "commits": commits,
                "rows": rows,
                "segments": len(self._manifest.segments),
            }
        )
        return report

    def iter_historical_frames(self) -> Iterator[CommitFrame]:
        """Yield all authenticated historical frames in commit order."""

        self._ensure_open()
        self.startup()
        if self._manifest is None:
            raise _repository_error(
                RepositoryErrorCode.AUTHORITY_EMPTY,
                "historical iteration requires at least one anchored seal",
            )
        self._load_manifest_chain(self._manifest)
        self._assert_no_manifest_forks()
        for descriptor in self._manifest.segments:
            yield from self._read_segment_descriptor(descriptor).commits

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._overlay.close()
        finally:
            try:
                self._writer_lease.close()
            finally:
                try:
                    self._anchor_writer_lease.close()
                finally:
                    self._closed = True

    def __enter__(self) -> StorageRepository:
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise _repository_error(
                RepositoryErrorCode.MISSING,
                "repository writer is closed",
            )


__all__ = [
    "CHECKPOINT_SUFFIX",
    "CURRENT_FORMAT_VERSION",
    "DOMAIN_CANDIDATE_SEGMENT_DESCRIPTORS",
    "MANIFEST_SUFFIX",
    "SEGMENT_SUFFIX",
    "WRITER_LEASE_NAME",
    "AuditIntegrityStatus",
    "AuditReport",
    "CurrentCacheStatus",
    "CurrentRecord",
    "RepositoryConfig",
    "RepositoryError",
    "RepositoryErrorCode",
    "RepositoryPaths",
    "SealResult",
    "StartupIntegrityStatus",
    "StartupReport",
    "StorageRepository",
    "candidate_segment_descriptors_digest",
]
