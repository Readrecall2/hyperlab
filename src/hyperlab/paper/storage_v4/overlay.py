from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Final, cast

from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.contracts import StorageMode
from hyperlab.paper.storage_v4.manifest import OpaqueIdentity
from hyperlab.paper.storage_v4.records import (
    RecordFormatError,
    RecordReadLimits,
    commit_frame_from_bytes,
    commit_frame_to_bytes,
)
from hyperlab.paper.storage_v4.segment import CodecProfile
from hyperlab.paper.storage_v4.types import (
    UINT64_MAX,
    CommitFrame,
    CommitLogical,
    CommitSequence,
    Hash32,
    RunId,
    StoreId,
)

OVERLAY_SCHEMA_VERSION: Final = 2
SEQUENCE_TEXT_WIDTH: Final = 20
GENESIS_MANIFEST_GENERATION: Final = 0
GENESIS_MANIFEST_ROOT: Final = Hash32(b"\x00" * 32)
FAULT_BEFORE_TRANSACTION: Final = "overlay.before_transaction"
FAULT_AFTER_BEGIN: Final = "overlay.after_begin"
FAULT_BEFORE_COMMIT: Final = "overlay.before_commit"
FAULT_AFTER_COMMIT: Final = "overlay.after_commit"

FaultInjector = Callable[[str], None]


class OverlayErrorCode(StrEnum):
    ALREADY_EXISTS = "OVERLAY_ALREADY_EXISTS"
    MISSING = "OVERLAY_MISSING"
    OPEN_FAILED = "OVERLAY_OPEN_FAILED"
    CLOSED = "OVERLAY_CLOSED"
    SCHEMA_MISMATCH = "OVERLAY_SCHEMA_MISMATCH"
    EXPECTED_STATE_MISMATCH = "OVERLAY_EXPECTED_STATE_MISMATCH"
    CORRUPT = "OVERLAY_CORRUPT"
    WRONG_RUN = "OVERLAY_WRONG_RUN"
    DUPLICATE_CONFLICT = "OVERLAY_DUPLICATE_CONFLICT"
    OVERLAP = "OVERLAY_SEQUENCE_OVERLAP"
    GAP = "OVERLAY_SEQUENCE_GAP"
    PREVIOUS_ROOT_MISMATCH = "OVERLAY_PREVIOUS_ROOT_MISMATCH"
    RECORD_LIMIT_EXCEEDED = "OVERLAY_RECORD_LIMIT_EXCEEDED"
    COUNTER_OVERFLOW = "OVERLAY_COUNTER_OVERFLOW"
    MANIFEST_ROLLBACK = "OVERLAY_MANIFEST_ROLLBACK"
    MANIFEST_CONFLICT = "OVERLAY_MANIFEST_CONFLICT"
    BASE_PREFIX_MISMATCH = "OVERLAY_BASE_PREFIX_MISMATCH"
    READ_ONLY = "OVERLAY_READ_ONLY"


class OverlayError(RuntimeError):
    """Structured, fail-closed overlay failure."""

    code: OverlayErrorCode
    details: Mapping[str, object]

    def __init__(
        self,
        code: OverlayErrorCode,
        message: str,
        **details: object,
    ) -> None:
        self.code = code
        self.details = MappingProxyType(dict(details))
        super().__init__(f"{code.value}: {message}")


@dataclass(frozen=True, slots=True)
class OverlayThresholds:
    """Seal the tail when either logical-row or stored-byte threshold is met."""

    seal_rows: int = 4_096
    seal_bytes: int = 16 * 1024 * 1024

    def __post_init__(self) -> None:
        _require_positive_u64(self.seal_rows, label="seal_rows")
        _require_positive_u64(self.seal_bytes, label="seal_bytes")


@dataclass(frozen=True, slots=True)
class OverlayIdentity:
    """Immutable creation identity required before any retained tail is read."""

    store_id: StoreId
    run_id: RunId
    mode: StorageMode
    run_identity: OpaqueIdentity
    config_identity: OpaqueIdentity
    code_identity: OpaqueIdentity
    runtime_identity: OpaqueIdentity
    codec_profile: CodecProfile
    base_manifest_generation: int
    base_manifest_root: Hash32
    base_commit_sequence: CommitSequence
    base_prefix_root: Hash32
    thresholds: OverlayThresholds

    def __post_init__(self) -> None:
        if type(self.store_id) is not StoreId:
            raise TypeError("overlay identity store_id must be StoreId")
        if type(self.run_id) is not RunId:
            raise TypeError("overlay identity run_id must be RunId")
        if type(self.mode) is not StorageMode:
            raise TypeError("overlay identity mode must be StorageMode")
        for label, identity in (
            ("run_identity", self.run_identity),
            ("config_identity", self.config_identity),
            ("code_identity", self.code_identity),
            ("runtime_identity", self.runtime_identity),
        ):
            if type(identity) is not OpaqueIdentity:
                raise TypeError(f"overlay identity {label} must be OpaqueIdentity")
        if type(self.codec_profile) is not CodecProfile:
            raise TypeError("overlay identity codec_profile must be CodecProfile")
        _validate_manifest_generation_root(
            self.base_manifest_generation,
            self.base_manifest_root,
            generation_label="overlay identity base_manifest_generation",
            root_label="overlay identity base_manifest_root",
        )
        if type(self.base_commit_sequence) is not CommitSequence:
            raise TypeError("overlay identity base_commit_sequence must be CommitSequence")
        if type(self.base_prefix_root) is not Hash32:
            raise TypeError("overlay identity base_prefix_root must be Hash32")
        if type(self.thresholds) is not OverlayThresholds:
            raise TypeError("overlay identity thresholds must be OverlayThresholds")


@dataclass(frozen=True, slots=True)
class OverlayState:
    run_id: RunId
    base_manifest_generation: int
    base_manifest_root: Hash32
    base_commit_sequence: CommitSequence
    base_prefix_root: Hash32
    thresholds: OverlayThresholds
    tail_commit_count: int
    tail_row_count: int
    tail_bytes: int
    head_commit_sequence: CommitSequence
    head_prefix_root: Hash32

    @property
    def seal_required(self) -> bool:
        return (
            self.tail_row_count >= self.thresholds.seal_rows
            or self.tail_bytes >= self.thresholds.seal_bytes
        )


@dataclass(frozen=True, slots=True)
class OverlayTailDiscardResult:
    before: OverlayState
    after: OverlayState
    discarded_commit_count: int
    discarded_row_count: int
    discarded_bytes: int

    @property
    def changed(self) -> bool:
        return self.discarded_commit_count != 0


@dataclass(frozen=True, slots=True)
class DurabilitySettings:
    journal_mode: str
    synchronous: int


@dataclass(frozen=True, slots=True)
class _Meta:
    identity: OverlayIdentity
    base_manifest_generation: int
    base_manifest_root: Hash32
    base_commit_sequence: CommitSequence
    base_prefix_root: Hash32
    tail_commit_count: int
    tail_row_count: int
    tail_bytes: int
    head_commit_sequence: CommitSequence
    head_prefix_root: Hash32

    @property
    def run_id(self) -> RunId:
        return self.identity.run_id

    @property
    def thresholds(self) -> OverlayThresholds:
        return self.identity.thresholds

    def public(self) -> OverlayState:
        return OverlayState(
            run_id=self.run_id,
            base_manifest_generation=self.base_manifest_generation,
            base_manifest_root=self.base_manifest_root,
            base_commit_sequence=self.base_commit_sequence,
            base_prefix_root=self.base_prefix_root,
            thresholds=self.thresholds,
            tail_commit_count=self.tail_commit_count,
            tail_row_count=self.tail_row_count,
            tail_bytes=self.tail_bytes,
            head_commit_sequence=self.head_commit_sequence,
            head_prefix_root=self.head_prefix_root,
        )


@dataclass(frozen=True, slots=True)
class _ValidatedCommit:
    frame: CommitFrame
    logical: CommitLogical
    record_size: int


_SCHEMA_STATEMENTS: Final[tuple[str, ...]] = (
    """
    CREATE TABLE overlay_meta (
        singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
        store_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        mode TEXT NOT NULL,
        run_identity BLOB NOT NULL CHECK (length(run_identity) = 32),
        config_identity BLOB NOT NULL CHECK (length(config_identity) = 32),
        code_identity BLOB NOT NULL CHECK (length(code_identity) = 32),
        runtime_identity BLOB NOT NULL CHECK (length(runtime_identity) = 32),
        codec_id INTEGER NOT NULL CHECK (codec_id >= 0),
        codec_level INTEGER NOT NULL CHECK (codec_level >= 0),
        codec_profile_id TEXT NOT NULL CHECK (length(codec_profile_id) > 0),
        genesis_manifest_generation TEXT NOT NULL CHECK (length(genesis_manifest_generation) = 20),
        genesis_manifest_root BLOB NOT NULL CHECK (length(genesis_manifest_root) = 32),
        genesis_commit_sequence TEXT NOT NULL CHECK (length(genesis_commit_sequence) = 20),
        genesis_prefix_root BLOB NOT NULL CHECK (length(genesis_prefix_root) = 32),
        base_manifest_generation TEXT NOT NULL CHECK (length(base_manifest_generation) = 20),
        base_manifest_root BLOB NOT NULL CHECK (length(base_manifest_root) = 32),
        base_commit_sequence TEXT NOT NULL CHECK (length(base_commit_sequence) = 20),
        base_prefix_root BLOB NOT NULL CHECK (length(base_prefix_root) = 32),
        seal_row_threshold TEXT NOT NULL CHECK (length(seal_row_threshold) = 20),
        seal_byte_threshold TEXT NOT NULL CHECK (length(seal_byte_threshold) = 20),
        tail_commit_count TEXT NOT NULL CHECK (length(tail_commit_count) = 20),
        tail_row_count TEXT NOT NULL CHECK (length(tail_row_count) = 20),
        tail_byte_count TEXT NOT NULL CHECK (length(tail_byte_count) = 20),
        head_commit_sequence TEXT NOT NULL CHECK (length(head_commit_sequence) = 20),
        head_prefix_root BLOB NOT NULL CHECK (length(head_prefix_root) = 32)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE overlay_commits (
        commit_sequence TEXT NOT NULL PRIMARY KEY CHECK (length(commit_sequence) = 20),
        record_bytes BLOB NOT NULL,
        commit_digest BLOB NOT NULL CHECK (length(commit_digest) = 32),
        prefix_root BLOB NOT NULL CHECK (length(prefix_root) = 32),
        previous_prefix_root BLOB NOT NULL CHECK (length(previous_prefix_root) = 32),
        row_count INTEGER NOT NULL CHECK (row_count >= 0),
        byte_count INTEGER NOT NULL CHECK (byte_count > 0)
    ) WITHOUT ROWID
    """,
)

_META_COLUMNS: Final[tuple[str, ...]] = (
    "singleton",
    "store_id",
    "run_id",
    "mode",
    "run_identity",
    "config_identity",
    "code_identity",
    "runtime_identity",
    "codec_id",
    "codec_level",
    "codec_profile_id",
    "genesis_manifest_generation",
    "genesis_manifest_root",
    "genesis_commit_sequence",
    "genesis_prefix_root",
    "base_manifest_generation",
    "base_manifest_root",
    "base_commit_sequence",
    "base_prefix_root",
    "seal_row_threshold",
    "seal_byte_threshold",
    "tail_commit_count",
    "tail_row_count",
    "tail_byte_count",
    "head_commit_sequence",
    "head_prefix_root",
)
_COMMIT_COLUMNS: Final[tuple[str, ...]] = (
    "commit_sequence",
    "record_bytes",
    "commit_digest",
    "prefix_root",
    "previous_prefix_root",
    "row_count",
    "byte_count",
)


def _require_u64(value: int, *, label: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{label} must be an exact integer")
    if value < 0 or value > UINT64_MAX:
        raise ValueError(f"{label} must be within uint64 bounds")


def _require_positive_u64(value: int, *, label: str) -> None:
    _require_u64(value, label=label)
    if value == 0:
        raise ValueError(f"{label} must be positive")


def _validate_manifest_generation_root(
    generation: int,
    root: Hash32,
    *,
    generation_label: str,
    root_label: str,
) -> None:
    _require_u64(generation, label=generation_label)
    if type(root) is not Hash32:
        raise TypeError(f"{root_label} must be Hash32")
    is_genesis_generation = generation == GENESIS_MANIFEST_GENERATION
    is_genesis_root = root == GENESIS_MANIFEST_ROOT
    if is_genesis_generation != is_genesis_root:
        raise ValueError(
            f"{generation_label} and {root_label} must use the genesis sentinel together"
        )


def _u64_text(value: int, *, label: str) -> str:
    _require_u64(value, label=label)
    return f"{value:0{SEQUENCE_TEXT_WIDTH}d}"


def _parse_u64_text(value: object, *, label: str) -> int:
    if type(value) is not str:
        raise OverlayError(OverlayErrorCode.CORRUPT, f"{label} is not text")
    if (
        len(value) != SEQUENCE_TEXT_WIDTH
        or not value.isascii()
        or not value.isdecimal()
    ):
        raise OverlayError(OverlayErrorCode.CORRUPT, f"{label} is not canonical uint64 text")
    parsed = int(value)
    if parsed > UINT64_MAX or value != f"{parsed:0{SEQUENCE_TEXT_WIDTH}d}":
        raise OverlayError(OverlayErrorCode.CORRUPT, f"{label} is outside canonical uint64")
    return parsed


def _blob_hash(value: object, *, label: str) -> Hash32:
    if type(value) is not bytes:
        raise OverlayError(OverlayErrorCode.CORRUPT, f"{label} is not a BLOB")
    try:
        return Hash32(value)
    except (TypeError, ValueError) as error:
        raise OverlayError(OverlayErrorCode.CORRUPT, f"{label} is not Hash32") from error


def _exact_text(value: object, *, label: str) -> str:
    if type(value) is not str:
        raise OverlayError(OverlayErrorCode.CORRUPT, f"{label} is not text")
    return value


def _exact_nonnegative_int(value: object, *, label: str, positive: bool = False) -> int:
    if type(value) is not int:
        raise OverlayError(OverlayErrorCode.CORRUPT, f"{label} is not an integer")
    minimum = 1 if positive else 0
    if value < minimum:
        raise OverlayError(OverlayErrorCode.CORRUPT, f"{label} is outside its bounds")
    return value


def _checked_add(left: int, right: int, *, label: str) -> int:
    result = left + right
    if result > UINT64_MAX:
        raise OverlayError(OverlayErrorCode.COUNTER_OVERFLOW, f"{label} exceeds uint64")
    return result


def _row_tuple(row: object, *, label: str) -> tuple[object, ...]:
    if type(row) is not tuple:
        raise OverlayError(OverlayErrorCode.CORRUPT, f"{label} has an invalid SQLite row")
    return cast(tuple[object, ...], row)


def _connect_rw(path: Path) -> sqlite3.Connection:
    if path.is_symlink() or not path.is_file():
        raise OverlayError(
            OverlayErrorCode.CORRUPT,
            "refusing a non-regular or symbolic-link overlay path",
            path=str(path),
        )
    uri = f"{path.absolute().as_uri()}?mode=rw"
    try:
        return sqlite3.connect(uri, uri=True, timeout=5.0, isolation_level=None)
    except sqlite3.Error as error:
        raise OverlayError(
            OverlayErrorCode.OPEN_FAILED,
            "cannot open the existing overlay read-write",
            path=str(path),
        ) from error


def _connect_ro(path: Path) -> sqlite3.Connection:
    if path.is_symlink() or not path.is_file():
        raise OverlayError(
            OverlayErrorCode.CORRUPT,
            "refusing a non-regular or symbolic-link overlay path",
            path=str(path),
        )
    uri = f"{path.absolute().as_uri()}?mode=ro"
    try:
        return sqlite3.connect(uri, uri=True, timeout=5.0, isolation_level=None)
    except sqlite3.Error as error:
        raise OverlayError(
            OverlayErrorCode.OPEN_FAILED,
            "cannot open the existing overlay read-only",
            path=str(path),
        ) from error


def _configure_durability(connection: sqlite3.Connection) -> DurabilitySettings:
    try:
        journal_row = connection.execute("PRAGMA journal_mode=DELETE").fetchone()
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        connection.execute("PRAGMA busy_timeout=5000")
        synchronous_row = connection.execute("PRAGMA synchronous").fetchone()
    except sqlite3.Error as error:
        raise OverlayError(
            OverlayErrorCode.OPEN_FAILED,
            "cannot configure durable SQLite pragmas",
        ) from error
    journal_values = _row_tuple(journal_row, label="journal_mode result")
    synchronous_values = _row_tuple(synchronous_row, label="synchronous result")
    if (
        len(journal_values) != 1
        or type(journal_values[0]) is not str
        or journal_values[0].lower() != "delete"
    ):
        raise OverlayError(OverlayErrorCode.OPEN_FAILED, "SQLite refused journal_mode=DELETE")
    if len(synchronous_values) != 1 or synchronous_values[0] != 2:
        raise OverlayError(OverlayErrorCode.OPEN_FAILED, "SQLite refused synchronous=FULL")
    return DurabilitySettings(journal_mode="delete", synchronous=2)


def _configure_read_only(connection: sqlite3.Connection) -> DurabilitySettings:
    """Enable query-only mode and verify the producer durability profile."""

    try:
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        connection.execute("PRAGMA query_only=ON")
        query_only_row = connection.execute("PRAGMA query_only").fetchone()
        journal_row = connection.execute("PRAGMA journal_mode").fetchone()
        synchronous_row = connection.execute("PRAGMA synchronous").fetchone()
    except sqlite3.Error as error:
        raise OverlayError(
            OverlayErrorCode.OPEN_FAILED,
            "cannot configure the SQLite read-only guard",
        ) from error
    query_only_values = _row_tuple(query_only_row, label="query_only result")
    journal_values = _row_tuple(journal_row, label="journal_mode result")
    synchronous_values = _row_tuple(synchronous_row, label="synchronous result")
    if query_only_values != (1,):
        raise OverlayError(OverlayErrorCode.OPEN_FAILED, "SQLite refused query_only=ON")
    if (
        len(journal_values) != 1
        or type(journal_values[0]) is not str
        or journal_values[0].lower() != "delete"
    ):
        raise OverlayError(
            OverlayErrorCode.OPEN_FAILED,
            "read-only overlay is not in journal_mode=DELETE",
        )
    if len(synchronous_values) != 1 or synchronous_values[0] != 2:
        raise OverlayError(
            OverlayErrorCode.OPEN_FAILED,
            "read-only overlay is not in synchronous=FULL",
        )
    return DurabilitySettings(journal_mode="delete", synchronous=2)


class SQLiteOverlay:
    """Bounded, single-writer durable tail after an authenticated manifest base."""

    def __init__(
        self,
        path: Path,
        connection: sqlite3.Connection,
        identity: OverlayIdentity,
        fault_injector: FaultInjector | None,
        *,
        read_only: bool = False,
    ) -> None:
        if type(read_only) is not bool:
            raise TypeError("read_only must be an exact bool")
        self._path = path
        self._connection = connection
        self._identity = identity
        self._fault_injector = fault_injector
        self._read_only = read_only
        self._closed = False

    @classmethod
    def create(
        cls,
        path: str | Path,
        *,
        identity: OverlayIdentity,
        fault_injector: FaultInjector | None = None,
    ) -> SQLiteOverlay:
        """Create a fresh overlay exclusively; never replace or adopt an existing path."""

        if type(identity) is not OverlayIdentity:
            raise TypeError("identity must be OverlayIdentity")

        selected_path = Path(path).absolute()
        if selected_path.is_symlink():
            raise OverlayError(
                OverlayErrorCode.ALREADY_EXISTS,
                "refusing to create an overlay through a symbolic link",
                path=str(selected_path),
            )
        try:
            with selected_path.open("xb"):
                pass
        except FileExistsError as error:
            raise OverlayError(
                OverlayErrorCode.ALREADY_EXISTS,
                "overlay path already exists",
                path=str(selected_path),
            ) from error
        except OSError as error:
            raise OverlayError(
                OverlayErrorCode.OPEN_FAILED,
                "cannot create the fresh overlay path",
                path=str(selected_path),
            ) from error

        connection = _connect_rw(selected_path)
        overlay = cls(selected_path, connection, identity, fault_injector)
        committed = False
        try:
            _configure_durability(connection)
            overlay._hit(FAULT_BEFORE_TRANSACTION)
            connection.execute("BEGIN IMMEDIATE")
            overlay._hit(FAULT_AFTER_BEGIN)
            for statement in _SCHEMA_STATEMENTS:
                connection.execute(statement)
            connection.execute(f"PRAGMA user_version={OVERLAY_SCHEMA_VERSION}")
            connection.execute(
                """
                INSERT INTO overlay_meta (
                    singleton, store_id, run_id, mode, run_identity, config_identity,
                    code_identity, runtime_identity, codec_id, codec_level,
                    codec_profile_id, genesis_manifest_generation,
                    genesis_manifest_root, genesis_commit_sequence,
                    genesis_prefix_root, base_manifest_generation, base_manifest_root,
                    base_commit_sequence, base_prefix_root, seal_row_threshold,
                    seal_byte_threshold, tail_commit_count, tail_row_count,
                    tail_byte_count, head_commit_sequence, head_prefix_root
                ) VALUES (
                    1, :store_id, :run_id, :mode, :run_identity, :config_identity,
                    :code_identity, :runtime_identity, :codec_id, :codec_level,
                    :codec_profile_id, :genesis_manifest_generation,
                    :genesis_manifest_root, :genesis_commit_sequence,
                    :genesis_prefix_root, :base_manifest_generation,
                    :base_manifest_root, :base_commit_sequence, :base_prefix_root,
                    :seal_row_threshold, :seal_byte_threshold, :tail_commit_count,
                    :tail_row_count, :tail_byte_count, :head_commit_sequence,
                    :head_prefix_root
                )
                """,
                {
                    "store_id": identity.store_id.value,
                    "run_id": identity.run_id.value,
                    "mode": identity.mode.value,
                    "run_identity": bytes(identity.run_identity.digest),
                    "config_identity": bytes(identity.config_identity.digest),
                    "code_identity": bytes(identity.code_identity.digest),
                    "runtime_identity": bytes(identity.runtime_identity.digest),
                    "codec_id": identity.codec_profile.codec_id,
                    "codec_level": identity.codec_profile.level,
                    "codec_profile_id": identity.codec_profile.profile_id,
                    "genesis_manifest_generation": _u64_text(
                        identity.base_manifest_generation,
                        label="genesis_manifest_generation",
                    ),
                    "genesis_manifest_root": bytes(identity.base_manifest_root),
                    "genesis_commit_sequence": _u64_text(
                        int(identity.base_commit_sequence),
                        label="genesis_commit_sequence",
                    ),
                    "genesis_prefix_root": bytes(identity.base_prefix_root),
                    "base_manifest_generation": _u64_text(
                        identity.base_manifest_generation,
                        label="base_manifest_generation",
                    ),
                    "base_manifest_root": bytes(identity.base_manifest_root),
                    "base_commit_sequence": _u64_text(
                        int(identity.base_commit_sequence),
                        label="base_commit_sequence",
                    ),
                    "base_prefix_root": bytes(identity.base_prefix_root),
                    "seal_row_threshold": _u64_text(
                        identity.thresholds.seal_rows,
                        label="seal_rows",
                    ),
                    "seal_byte_threshold": _u64_text(
                        identity.thresholds.seal_bytes,
                        label="seal_bytes",
                    ),
                    "tail_commit_count": _u64_text(0, label="tail_commit_count"),
                    "tail_row_count": _u64_text(0, label="tail_row_count"),
                    "tail_byte_count": _u64_text(0, label="tail_byte_count"),
                    "head_commit_sequence": _u64_text(
                        int(identity.base_commit_sequence),
                        label="head_commit_sequence",
                    ),
                    "head_prefix_root": bytes(identity.base_prefix_root),
                },
            )
            overlay._hit(FAULT_BEFORE_COMMIT)
            connection.commit()
            committed = True
            overlay._hit(FAULT_AFTER_COMMIT)
            overlay.verify_integrity()
            return overlay
        except Exception:
            if not committed and connection.in_transaction:
                connection.rollback()
            connection.close()
            overlay._closed = True
            raise

    @classmethod
    def open_existing(
        cls,
        path: str | Path,
        *,
        expected_identity: OverlayIdentity,
        fault_injector: FaultInjector | None = None,
    ) -> SQLiteOverlay:
        """Open and exhaustively validate an existing bounded tail without creating it."""

        if type(expected_identity) is not OverlayIdentity:
            raise TypeError("expected_identity must be OverlayIdentity")

        selected_path = Path(path).absolute()
        if selected_path.is_symlink():
            raise OverlayError(
                OverlayErrorCode.CORRUPT,
                "refusing to open an overlay through a symbolic link",
                path=str(selected_path),
            )
        if not selected_path.is_file():
            raise OverlayError(
                OverlayErrorCode.MISSING,
                "expected overlay file does not exist",
                path=str(selected_path),
            )
        connection = _connect_rw(selected_path)
        overlay = cls(selected_path, connection, expected_identity, fault_injector)
        try:
            _configure_durability(connection)
            overlay._verify_schema()
            meta = overlay._read_meta()
            overlay._expect_identity(meta.identity, expected_identity)
            overlay._validated_commits(meta)
            return overlay
        except Exception:
            connection.close()
            overlay._closed = True
            raise

    @classmethod
    def open_existing_read_only(
        cls,
        path: str | Path,
        *,
        expected_identity: OverlayIdentity,
    ) -> SQLiteOverlay:
        """Open and exhaustively validate through ``mode=ro`` and query-only."""

        if type(expected_identity) is not OverlayIdentity:
            raise TypeError("expected_identity must be OverlayIdentity")
        selected_path = Path(path).absolute()
        if selected_path.is_symlink():
            raise OverlayError(
                OverlayErrorCode.CORRUPT,
                "refusing to open an overlay through a symbolic link",
                path=str(selected_path),
            )
        if not selected_path.is_file():
            raise OverlayError(
                OverlayErrorCode.MISSING,
                "expected overlay file does not exist",
                path=str(selected_path),
            )
        connection = _connect_ro(selected_path)
        overlay = cls(
            selected_path,
            connection,
            expected_identity,
            None,
            read_only=True,
        )
        try:
            _configure_read_only(connection)
            overlay._verify_schema()
            meta = overlay._read_meta()
            overlay._expect_identity(meta.identity, expected_identity)
            overlay._validated_commits(meta)
            return overlay
        except Exception:
            connection.close()
            overlay._closed = True
            raise

    @property
    def path(self) -> Path:
        return self._path

    @property
    def identity(self) -> OverlayIdentity:
        return self._identity

    @property
    def read_only(self) -> bool:
        return self._read_only

    @property
    def state(self) -> OverlayState:
        self._ensure_open()
        meta = self._read_meta()
        self._assert_stored_identity(meta.identity)
        return meta.public()

    @property
    def seal_required(self) -> bool:
        return self.state.seal_required

    def durability_settings(self) -> DurabilitySettings:
        self._ensure_open()
        if self._read_only:
            return _configure_read_only(self._connection)
        return _configure_durability(self._connection)

    def set_fault_injector(self, fault_injector: FaultInjector | None) -> None:
        self._ensure_open()
        self._fault_injector = fault_injector

    def append(self, frame: CommitFrame) -> bool:
        """Atomically append one exact next commit; return False for an exact duplicate."""

        self._ensure_open()
        self._ensure_writable()
        if type(frame) is not CommitFrame:
            raise TypeError("append requires CommitFrame")
        if frame.run_id != self._identity.run_id:
            raise OverlayError(
                OverlayErrorCode.WRONG_RUN,
                "commit belongs to another run",
                expected=self._identity.run_id.value,
                actual=frame.run_id.value,
            )
        logical = build_commit_logical(frame)
        record = commit_frame_to_bytes(frame)
        default_record_limits = RecordReadLimits()
        if (
            len(frame.rows) > default_record_limits.max_rows
            or len(record) > default_record_limits.max_physical_size
        ):
            raise OverlayError(
                OverlayErrorCode.RECORD_LIMIT_EXCEEDED,
                "commit cannot be reconstructed within overlay record limits",
                rows=len(frame.rows),
                bytes=len(record),
            )
        sequence = int(frame.commit_sequence)
        key = _u64_text(sequence, label="commit_sequence")
        committed = False
        try:
            self._hit(FAULT_BEFORE_TRANSACTION)
            self._connection.execute("BEGIN IMMEDIATE")
            self._hit(FAULT_AFTER_BEGIN)
            meta = self._read_meta()
            self._assert_stored_identity(meta.identity)

            existing_raw = self._connection.execute(
                """
                SELECT record_bytes, commit_digest, prefix_root, previous_prefix_root,
                       row_count, byte_count
                FROM overlay_commits WHERE commit_sequence = ?
                """,
                (key,),
            ).fetchone()
            if existing_raw is not None:
                existing = _row_tuple(existing_raw, label="existing commit")
                expected = (
                    record,
                    bytes(logical.digest),
                    bytes(logical.prefix_root),
                    bytes(frame.previous_prefix_root),
                    len(frame.rows),
                    len(record),
                )
                if existing == expected:
                    self._connection.rollback()
                    return False
                raise OverlayError(
                    OverlayErrorCode.DUPLICATE_CONFLICT,
                    "commit sequence already contains different bytes or identity",
                    sequence=sequence,
                )

            base = int(meta.base_commit_sequence)
            head = int(meta.head_commit_sequence)
            if sequence <= base or sequence <= head:
                raise OverlayError(
                    OverlayErrorCode.OVERLAP,
                    "commit sequence overlaps the authenticated base or retained tail",
                    sequence=sequence,
                    base=base,
                    head=head,
                )
            if head == UINT64_MAX or sequence != head + 1:
                raise OverlayError(
                    OverlayErrorCode.GAP,
                    "commit sequence is not the exact next uint64 value",
                    expected=None if head == UINT64_MAX else head + 1,
                    actual=sequence,
                )
            if frame.previous_prefix_root != meta.head_prefix_root:
                raise OverlayError(
                    OverlayErrorCode.PREVIOUS_ROOT_MISMATCH,
                    "commit previous prefix root differs from overlay head",
                    sequence=sequence,
                )

            new_commit_count = _checked_add(
                meta.tail_commit_count,
                1,
                label="tail commit count",
            )
            new_row_count = _checked_add(
                meta.tail_row_count,
                len(frame.rows),
                label="tail row count",
            )
            new_byte_count = _checked_add(
                meta.tail_bytes,
                len(record),
                label="tail byte count",
            )
            self._connection.execute(
                """
                INSERT INTO overlay_commits (
                    commit_sequence, record_bytes, commit_digest, prefix_root,
                    previous_prefix_root, row_count, byte_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    record,
                    bytes(logical.digest),
                    bytes(logical.prefix_root),
                    bytes(frame.previous_prefix_root),
                    len(frame.rows),
                    len(record),
                ),
            )
            self._connection.execute(
                """
                UPDATE overlay_meta
                SET tail_commit_count = ?, tail_row_count = ?, tail_byte_count = ?,
                    head_commit_sequence = ?, head_prefix_root = ?
                WHERE singleton = 1
                """,
                (
                    _u64_text(new_commit_count, label="tail_commit_count"),
                    _u64_text(new_row_count, label="tail_row_count"),
                    _u64_text(new_byte_count, label="tail_byte_count"),
                    key,
                    bytes(logical.prefix_root),
                ),
            )
            self._hit(FAULT_BEFORE_COMMIT)
            self._connection.commit()
            committed = True
            self._hit(FAULT_AFTER_COMMIT)
            return True
        except sqlite3.Error as error:
            if not committed and self._connection.in_transaction:
                self._connection.rollback()
            raise OverlayError(
                OverlayErrorCode.CORRUPT,
                "SQLite rejected the atomic overlay append",
                sequence=sequence,
            ) from error
        except Exception:
            if not committed and self._connection.in_transaction:
                self._connection.rollback()
            raise

    def frames(self) -> tuple[CommitFrame, ...]:
        """Return the authenticated tail in uint64 numeric order."""

        self._ensure_open()
        meta = self._read_meta()
        self._assert_stored_identity(meta.identity)
        return tuple(item.frame for item in self._validated_commits(meta))

    def discard_unsealed_tail(
        self,
        *,
        expected_base: OverlayState,
    ) -> OverlayTailDiscardResult:
        """Atomically discard the whole tail only at one exact sealed base.

        The expected base is deliberately not a cutoff. It must describe a
        zero-tail state whose head equals its base, and every base field is
        compared under BEGIN IMMEDIATE before all retained commits are
        removed. This makes the operation an exact compare-and-swap rather
        than an arbitrary truncation primitive.
        """

        self._ensure_open()
        self._ensure_writable()
        if type(expected_base) is not OverlayState:
            raise TypeError("expected_base must be OverlayState")
        if type(expected_base.run_id) is not RunId:
            raise TypeError("expected_base run_id must be RunId")
        _validate_manifest_generation_root(
            expected_base.base_manifest_generation,
            expected_base.base_manifest_root,
            generation_label="expected_base base_manifest_generation",
            root_label="expected_base base_manifest_root",
        )
        if type(expected_base.base_commit_sequence) is not CommitSequence:
            raise TypeError("expected_base base_commit_sequence must be CommitSequence")
        if type(expected_base.base_prefix_root) is not Hash32:
            raise TypeError("expected_base base_prefix_root must be Hash32")
        if type(expected_base.thresholds) is not OverlayThresholds:
            raise TypeError("expected_base thresholds must be OverlayThresholds")
        for label, value in (
            ("tail_commit_count", expected_base.tail_commit_count),
            ("tail_row_count", expected_base.tail_row_count),
            ("tail_bytes", expected_base.tail_bytes),
        ):
            if type(value) is not int:
                raise TypeError(f"expected_base {label} must be an exact integer")
        if type(expected_base.head_commit_sequence) is not CommitSequence:
            raise TypeError("expected_base head_commit_sequence must be CommitSequence")
        if type(expected_base.head_prefix_root) is not Hash32:
            raise TypeError("expected_base head_prefix_root must be Hash32")
        if (
            expected_base.tail_commit_count != 0
            or expected_base.tail_row_count != 0
            or expected_base.tail_bytes != 0
            or expected_base.head_commit_sequence != expected_base.base_commit_sequence
            or expected_base.head_prefix_root != expected_base.base_prefix_root
        ):
            raise OverlayError(
                OverlayErrorCode.EXPECTED_STATE_MISMATCH,
                "discard authority must describe one exact sealed base",
            )

        committed = False
        try:
            self._hit(FAULT_BEFORE_TRANSACTION)
            self._connection.execute("BEGIN IMMEDIATE")
            self._hit(FAULT_AFTER_BEGIN)
            meta = self._read_meta()
            self._assert_stored_identity(meta.identity)
            commits = self._validated_commits(meta)
            actual_base = (
                meta.run_id,
                meta.base_manifest_generation,
                meta.base_manifest_root,
                meta.base_commit_sequence,
                meta.base_prefix_root,
                meta.thresholds,
            )
            authorized_base = (
                expected_base.run_id,
                expected_base.base_manifest_generation,
                expected_base.base_manifest_root,
                expected_base.base_commit_sequence,
                expected_base.base_prefix_root,
                expected_base.thresholds,
            )
            if actual_base != authorized_base:
                raise OverlayError(
                    OverlayErrorCode.EXPECTED_STATE_MISMATCH,
                    "overlay base differs from discard authority",
                )

            before = meta.public()
            after = OverlayState(
                run_id=meta.run_id,
                base_manifest_generation=meta.base_manifest_generation,
                base_manifest_root=meta.base_manifest_root,
                base_commit_sequence=meta.base_commit_sequence,
                base_prefix_root=meta.base_prefix_root,
                thresholds=meta.thresholds,
                tail_commit_count=0,
                tail_row_count=0,
                tail_bytes=0,
                head_commit_sequence=meta.base_commit_sequence,
                head_prefix_root=meta.base_prefix_root,
            )
            result = OverlayTailDiscardResult(
                before=before,
                after=after,
                discarded_commit_count=meta.tail_commit_count,
                discarded_row_count=meta.tail_row_count,
                discarded_bytes=meta.tail_bytes,
            )
            if not commits:
                self._connection.rollback()
                return result

            deleted = self._connection.execute("DELETE FROM overlay_commits")
            if deleted.rowcount != len(commits):
                raise OverlayError(
                    OverlayErrorCode.CORRUPT,
                    "whole-tail discard removed an unexpected commit count",
                )
            updated = self._connection.execute(
                """
                UPDATE overlay_meta
                SET tail_commit_count = ?, tail_row_count = ?, tail_byte_count = ?,
                    head_commit_sequence = ?, head_prefix_root = ?
                WHERE singleton = 1
                """,
                (
                    _u64_text(0, label="tail_commit_count"),
                    _u64_text(0, label="tail_row_count"),
                    _u64_text(0, label="tail_byte_count"),
                    _u64_text(
                        int(meta.base_commit_sequence),
                        label="head_commit_sequence",
                    ),
                    bytes(meta.base_prefix_root),
                ),
            )
            if updated.rowcount != 1:
                raise OverlayError(
                    OverlayErrorCode.CORRUPT,
                    "whole-tail discard did not update exactly one metadata row",
                )
            self._hit(FAULT_BEFORE_COMMIT)
            self._connection.commit()
            committed = True
            self._hit(FAULT_AFTER_COMMIT)
            return result
        except sqlite3.Error as error:
            if not committed and self._connection.in_transaction:
                self._connection.rollback()
            raise OverlayError(
                OverlayErrorCode.CORRUPT,
                "SQLite rejected the atomic whole-tail discard",
            ) from error
        except Exception:
            if not committed and self._connection.in_transaction:
                self._connection.rollback()
            raise

    def advance_base(
        self,
        *,
        manifest_generation: int,
        manifest_root: Hash32,
        base_commit_sequence: CommitSequence,
        base_prefix_root: Hash32,
    ) -> bool:
        """Atomically retire a covered prefix and preserve an authenticated tail.

        The exact current base is idempotent and returns False. A newer manifest
        generation may cover the same base sequence (reattest) or a contiguous
        prefix currently retained in the overlay.
        """

        self._ensure_open()
        self._ensure_writable()
        _validate_manifest_generation_root(
            manifest_generation,
            manifest_root,
            generation_label="manifest_generation",
            root_label="manifest_root",
        )
        if type(base_prefix_root) is not Hash32:
            raise TypeError("base_prefix_root must be Hash32")
        if type(base_commit_sequence) is not CommitSequence:
            raise TypeError("base_commit_sequence must be CommitSequence")
        new_base = int(base_commit_sequence)
        committed = False
        try:
            self._hit(FAULT_BEFORE_TRANSACTION)
            self._connection.execute("BEGIN IMMEDIATE")
            self._hit(FAULT_AFTER_BEGIN)
            meta = self._read_meta()
            self._assert_stored_identity(meta.identity)
            commits = self._validated_commits(meta)
            if manifest_generation < meta.base_manifest_generation:
                raise OverlayError(
                    OverlayErrorCode.MANIFEST_ROLLBACK,
                    "base manifest generation would move backwards",
                    current=meta.base_manifest_generation,
                    proposed=manifest_generation,
                )
            if manifest_generation == meta.base_manifest_generation:
                if (
                    manifest_root == meta.base_manifest_root
                    and base_commit_sequence == meta.base_commit_sequence
                    and base_prefix_root == meta.base_prefix_root
                ):
                    self._connection.rollback()
                    return False
                raise OverlayError(
                    OverlayErrorCode.MANIFEST_CONFLICT,
                    "same manifest generation proposes a different authenticated base",
                    generation=manifest_generation,
                )

            current_base = int(meta.base_commit_sequence)
            current_head = int(meta.head_commit_sequence)
            if new_base < current_base:
                raise OverlayError(
                    OverlayErrorCode.MANIFEST_ROLLBACK,
                    "base commit sequence would move backwards",
                    current=current_base,
                    proposed=new_base,
                )
            if new_base > current_head:
                raise OverlayError(
                    OverlayErrorCode.GAP,
                    "new base is beyond the retained overlay head",
                    head=current_head,
                    proposed=new_base,
                )

            if new_base == current_base:
                expected_prefix = meta.base_prefix_root
            else:
                covered = next(
                    (
                        item
                        for item in commits
                        if item.frame.commit_sequence == base_commit_sequence
                    ),
                    None,
                )
                if covered is None:
                    raise OverlayError(
                        OverlayErrorCode.CORRUPT,
                        "new base sequence is missing from the contiguous retained tail",
                        sequence=new_base,
                    )
                expected_prefix = covered.logical.prefix_root
            if base_prefix_root != expected_prefix:
                raise OverlayError(
                    OverlayErrorCode.BASE_PREFIX_MISMATCH,
                    "new base prefix root differs from authenticated commit identity",
                    sequence=new_base,
                )

            remaining = tuple(
                item for item in commits if int(item.frame.commit_sequence) > new_base
            )
            if remaining and remaining[0].frame.previous_prefix_root != base_prefix_root:
                raise OverlayError(
                    OverlayErrorCode.CORRUPT,
                    "preserved tail does not attach to the proposed base prefix",
                )
            tail_commit_count = len(remaining)
            tail_row_count = sum(len(item.frame.rows) for item in remaining)
            tail_byte_count = sum(item.record_size for item in remaining)
            for label, value in (
                ("tail commit count", tail_commit_count),
                ("tail row count", tail_row_count),
                ("tail byte count", tail_byte_count),
            ):
                if value > UINT64_MAX:
                    raise OverlayError(OverlayErrorCode.COUNTER_OVERFLOW, f"{label} exceeds uint64")

            self._connection.execute(
                "DELETE FROM overlay_commits WHERE commit_sequence <= ?",
                (_u64_text(new_base, label="base_commit_sequence"),),
            )
            if remaining:
                head_sequence = remaining[-1].frame.commit_sequence
                head_prefix = remaining[-1].logical.prefix_root
            else:
                head_sequence = base_commit_sequence
                head_prefix = base_prefix_root
            self._connection.execute(
                """
                UPDATE overlay_meta
                SET base_manifest_generation = ?, base_manifest_root = ?,
                    base_commit_sequence = ?, base_prefix_root = ?,
                    tail_commit_count = ?, tail_row_count = ?, tail_byte_count = ?,
                    head_commit_sequence = ?, head_prefix_root = ?
                WHERE singleton = 1
                """,
                (
                    _u64_text(manifest_generation, label="manifest_generation"),
                    bytes(manifest_root),
                    _u64_text(new_base, label="base_commit_sequence"),
                    bytes(base_prefix_root),
                    _u64_text(tail_commit_count, label="tail_commit_count"),
                    _u64_text(tail_row_count, label="tail_row_count"),
                    _u64_text(tail_byte_count, label="tail_byte_count"),
                    _u64_text(int(head_sequence), label="head_commit_sequence"),
                    bytes(head_prefix),
                ),
            )
            self._hit(FAULT_BEFORE_COMMIT)
            self._connection.commit()
            committed = True
            self._hit(FAULT_AFTER_COMMIT)
            return True
        except sqlite3.Error as error:
            if not committed and self._connection.in_transaction:
                self._connection.rollback()
            raise OverlayError(
                OverlayErrorCode.CORRUPT,
                "SQLite rejected the atomic overlay base advance",
                sequence=new_base,
            ) from error
        except Exception:
            if not committed and self._connection.in_transaction:
                self._connection.rollback()
            raise

    def verify_integrity(self) -> OverlayState:
        self._ensure_open()
        self._verify_schema()
        meta = self._read_meta()
        self._assert_stored_identity(meta.identity)
        self._validated_commits(meta)
        return meta.public()

    def close(self) -> None:
        if self._closed:
            return
        self._connection.close()
        self._closed = True

    def __enter__(self) -> SQLiteOverlay:
        self._ensure_open()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise OverlayError(OverlayErrorCode.CLOSED, "overlay connection is closed")

    def _ensure_writable(self) -> None:
        if self._read_only:
            raise OverlayError(
                OverlayErrorCode.READ_ONLY,
                "read-only overlay rejects mutation",
                path=str(self._path),
            )

    def _hit(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _verify_schema(self) -> None:
        try:
            version_raw = self._connection.execute("PRAGMA user_version").fetchone()
            table_rows = self._connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            ).fetchall()
        except sqlite3.Error as error:
            raise OverlayError(OverlayErrorCode.SCHEMA_MISMATCH, "cannot inspect overlay schema") from error
        version_values = _row_tuple(version_raw, label="user_version result")
        if len(version_values) == 1 and version_values[0] == 1:
            raise OverlayError(
                OverlayErrorCode.SCHEMA_MISMATCH,
                "overlay schema version 1 lacks the immutable genesis identity and cannot be adopted",
                expected=OVERLAY_SCHEMA_VERSION,
                actual=1,
                migration_supported=False,
            )
        if len(version_values) != 1 or version_values[0] != OVERLAY_SCHEMA_VERSION:
            raise OverlayError(
                OverlayErrorCode.SCHEMA_MISMATCH,
                "overlay user_version is unsupported",
                expected=OVERLAY_SCHEMA_VERSION,
                actual=version_values[0] if version_values else None,
            )
        tables: list[str] = []
        for raw in table_rows:
            values = _row_tuple(raw, label="schema table")
            if len(values) != 1 or type(values[0]) is not str:
                raise OverlayError(OverlayErrorCode.SCHEMA_MISMATCH, "invalid schema table metadata")
            tables.append(values[0])
        if tuple(tables) != ("overlay_commits", "overlay_meta"):
            raise OverlayError(
                OverlayErrorCode.SCHEMA_MISMATCH,
                "overlay table set differs from the versioned schema",
                tables=tuple(tables),
            )
        self._verify_columns("overlay_meta", _META_COLUMNS)
        self._verify_columns("overlay_commits", _COMMIT_COLUMNS)

    def _verify_columns(self, table: str, expected: tuple[str, ...]) -> None:
        try:
            rows = self._connection.execute(f"PRAGMA table_info({table})").fetchall()
        except sqlite3.Error as error:
            raise OverlayError(
                OverlayErrorCode.SCHEMA_MISMATCH,
                f"cannot inspect {table} columns",
            ) from error
        actual: list[str] = []
        for raw in rows:
            values = _row_tuple(raw, label=f"{table} column")
            if len(values) < 2 or type(values[1]) is not str:
                raise OverlayError(
                    OverlayErrorCode.SCHEMA_MISMATCH,
                    f"{table} has invalid column metadata",
                )
            actual.append(values[1])
        if tuple(actual) != expected:
            raise OverlayError(
                OverlayErrorCode.SCHEMA_MISMATCH,
                f"{table} columns differ from schema version {OVERLAY_SCHEMA_VERSION}",
                columns=tuple(actual),
            )

    def _read_meta(self) -> _Meta:
        try:
            rows = self._connection.execute(
                """
                SELECT store_id, run_id, mode, run_identity, config_identity,
                       code_identity, runtime_identity, codec_id, codec_level,
                       codec_profile_id, genesis_manifest_generation,
                       genesis_manifest_root, genesis_commit_sequence,
                       genesis_prefix_root, base_manifest_generation,
                       base_manifest_root, base_commit_sequence, base_prefix_root,
                       seal_row_threshold, seal_byte_threshold, tail_commit_count,
                       tail_row_count, tail_byte_count, head_commit_sequence,
                       head_prefix_root
                FROM overlay_meta WHERE singleton = 1
                """
            ).fetchall()
        except sqlite3.Error as error:
            raise OverlayError(OverlayErrorCode.CORRUPT, "cannot read overlay metadata") from error
        if len(rows) != 1:
            raise OverlayError(OverlayErrorCode.CORRUPT, "overlay must have exactly one metadata row")
        values = _row_tuple(rows[0], label="overlay metadata")
        if len(values) != 25:
            raise OverlayError(OverlayErrorCode.CORRUPT, "overlay metadata has the wrong width")
        try:
            codec_profile = CodecProfile(
                codec_id=_exact_nonnegative_int(values[7], label="codec_id"),
                level=_exact_nonnegative_int(values[8], label="codec_level"),
            )
            codec_profile_id = _exact_text(values[9], label="codec_profile_id")
            if codec_profile_id != codec_profile.profile_id:
                raise OverlayError(
                    OverlayErrorCode.CORRUPT,
                    "stored codec profile identity differs from its exact codec fields",
                )
            identity = OverlayIdentity(
                store_id=StoreId(_exact_text(values[0], label="store_id")),
                run_id=RunId(_exact_text(values[1], label="run_id")),
                mode=StorageMode(_exact_text(values[2], label="mode")),
                run_identity=OpaqueIdentity(_blob_hash(values[3], label="run_identity")),
                config_identity=OpaqueIdentity(
                    _blob_hash(values[4], label="config_identity")
                ),
                code_identity=OpaqueIdentity(_blob_hash(values[5], label="code_identity")),
                runtime_identity=OpaqueIdentity(
                    _blob_hash(values[6], label="runtime_identity")
                ),
                codec_profile=codec_profile,
                base_manifest_generation=_parse_u64_text(
                    values[10], label="genesis_manifest_generation"
                ),
                base_manifest_root=_blob_hash(
                    values[11], label="genesis_manifest_root"
                ),
                base_commit_sequence=CommitSequence(
                    _parse_u64_text(values[12], label="genesis_commit_sequence")
                ),
                base_prefix_root=_blob_hash(values[13], label="genesis_prefix_root"),
                thresholds=OverlayThresholds(
                    seal_rows=_parse_u64_text(values[18], label="seal_row_threshold"),
                    seal_bytes=_parse_u64_text(values[19], label="seal_byte_threshold"),
                ),
            )
            meta = _Meta(
                identity=identity,
                base_manifest_generation=_parse_u64_text(
                    values[14], label="base_manifest_generation"
                ),
                base_manifest_root=_blob_hash(values[15], label="base_manifest_root"),
                base_commit_sequence=CommitSequence(
                    _parse_u64_text(values[16], label="base_commit_sequence")
                ),
                base_prefix_root=_blob_hash(values[17], label="base_prefix_root"),
                tail_commit_count=_parse_u64_text(values[20], label="tail_commit_count"),
                tail_row_count=_parse_u64_text(values[21], label="tail_row_count"),
                tail_bytes=_parse_u64_text(values[22], label="tail_byte_count"),
                head_commit_sequence=CommitSequence(
                    _parse_u64_text(values[23], label="head_commit_sequence")
                ),
                head_prefix_root=_blob_hash(values[24], label="head_prefix_root"),
            )
            _validate_manifest_generation_root(
                meta.base_manifest_generation,
                meta.base_manifest_root,
                generation_label="stored base_manifest_generation",
                root_label="stored base_manifest_root",
            )
        except OverlayError:
            raise
        except (TypeError, ValueError) as error:
            raise OverlayError(OverlayErrorCode.CORRUPT, "overlay metadata is invalid") from error
        if int(meta.head_commit_sequence) < int(meta.base_commit_sequence):
            raise OverlayError(OverlayErrorCode.CORRUPT, "overlay head precedes its base")
        return meta

    def _validated_commits(self, meta: _Meta) -> tuple[_ValidatedCommit, ...]:
        try:
            rows = self._connection.execute(
                """
                SELECT commit_sequence, record_bytes, commit_digest, prefix_root,
                       previous_prefix_root, row_count, byte_count
                FROM overlay_commits ORDER BY commit_sequence
                """
            ).fetchall()
        except sqlite3.Error as error:
            raise OverlayError(OverlayErrorCode.CORRUPT, "cannot read retained overlay commits") from error
        result: list[_ValidatedCommit] = []
        expected_sequence = int(meta.base_commit_sequence)
        expected_prefix = meta.base_prefix_root
        total_rows = 0
        total_bytes = 0
        for raw in rows:
            values = _row_tuple(raw, label="overlay commit")
            if len(values) != 7:
                raise OverlayError(OverlayErrorCode.CORRUPT, "overlay commit row has wrong width")
            sequence = _parse_u64_text(values[0], label="commit_sequence")
            if expected_sequence == UINT64_MAX or sequence != expected_sequence + 1:
                relation = "overlap" if sequence <= expected_sequence else "gap"
                raise OverlayError(
                    OverlayErrorCode.CORRUPT,
                    f"retained overlay commit sequence has {relation}",
                    previous=expected_sequence,
                    actual=sequence,
                )
            if type(values[1]) is not bytes:
                raise OverlayError(OverlayErrorCode.CORRUPT, "commit record is not a BLOB")
            record = values[1]
            try:
                frame = commit_frame_from_bytes(record)
            except (RecordFormatError, TypeError, ValueError) as error:
                raise OverlayError(
                    OverlayErrorCode.CORRUPT,
                    "stored commit record is invalid",
                    sequence=sequence,
                ) from error
            logical = build_commit_logical(frame)
            if frame.run_id != meta.run_id:
                raise OverlayError(OverlayErrorCode.CORRUPT, "stored commit belongs to another run")
            if int(frame.commit_sequence) != sequence:
                raise OverlayError(OverlayErrorCode.CORRUPT, "stored commit sequence/key mismatch")
            if frame.previous_prefix_root != expected_prefix:
                raise OverlayError(OverlayErrorCode.CORRUPT, "stored commit prefix chain mismatch")
            if _blob_hash(values[2], label="commit_digest") != logical.digest:
                raise OverlayError(OverlayErrorCode.CORRUPT, "stored commit digest column mismatch")
            if _blob_hash(values[3], label="prefix_root") != logical.prefix_root:
                raise OverlayError(OverlayErrorCode.CORRUPT, "stored commit prefix column mismatch")
            if _blob_hash(values[4], label="previous_prefix_root") != frame.previous_prefix_root:
                raise OverlayError(OverlayErrorCode.CORRUPT, "stored previous prefix column mismatch")
            row_count = _exact_nonnegative_int(values[5], label="row_count")
            byte_count = _exact_nonnegative_int(values[6], label="byte_count", positive=True)
            if row_count != len(frame.rows) or byte_count != len(record):
                raise OverlayError(OverlayErrorCode.CORRUPT, "stored commit counters mismatch")
            total_rows = _checked_add(total_rows, row_count, label="validated tail row count")
            total_bytes = _checked_add(total_bytes, byte_count, label="validated tail byte count")
            result.append(_ValidatedCommit(frame=frame, logical=logical, record_size=byte_count))
            expected_sequence = sequence
            expected_prefix = logical.prefix_root

        if len(result) != meta.tail_commit_count:
            raise OverlayError(OverlayErrorCode.CORRUPT, "tail commit counter mismatch")
        if total_rows != meta.tail_row_count or total_bytes != meta.tail_bytes:
            raise OverlayError(OverlayErrorCode.CORRUPT, "tail row or byte counter mismatch")
        if expected_sequence != int(meta.head_commit_sequence) or expected_prefix != meta.head_prefix_root:
            raise OverlayError(OverlayErrorCode.CORRUPT, "overlay head metadata mismatch")
        return tuple(result)

    @staticmethod
    def _identity_checks(
        expected: OverlayIdentity,
        actual: OverlayIdentity,
    ) -> tuple[tuple[str, object, object], ...]:
        return (
            ("store_id", expected.store_id, actual.store_id),
            ("run_id", expected.run_id, actual.run_id),
            ("mode", expected.mode, actual.mode),
            ("run_identity", expected.run_identity, actual.run_identity),
            ("config_identity", expected.config_identity, actual.config_identity),
            ("code_identity", expected.code_identity, actual.code_identity),
            ("runtime_identity", expected.runtime_identity, actual.runtime_identity),
            ("codec_profile", expected.codec_profile, actual.codec_profile),
            (
                "base_manifest_generation",
                expected.base_manifest_generation,
                actual.base_manifest_generation,
            ),
            ("base_manifest_root", expected.base_manifest_root, actual.base_manifest_root),
            (
                "base_commit_sequence",
                expected.base_commit_sequence,
                actual.base_commit_sequence,
            ),
            ("base_prefix_root", expected.base_prefix_root, actual.base_prefix_root),
            ("seal_rows", expected.thresholds.seal_rows, actual.thresholds.seal_rows),
            ("seal_bytes", expected.thresholds.seal_bytes, actual.thresholds.seal_bytes),
        )

    def _expect_identity(
        self,
        actual: OverlayIdentity,
        expected: OverlayIdentity,
    ) -> None:
        for label, expected_value, actual_value in self._identity_checks(expected, actual):
            if expected_value != actual_value:
                code = (
                    OverlayErrorCode.WRONG_RUN
                    if label == "run_id"
                    else OverlayErrorCode.EXPECTED_STATE_MISMATCH
                )
                raise OverlayError(
                    code,
                    f"existing overlay has unexpected {label}",
                    field=label,
                )

    def _assert_stored_identity(self, actual: OverlayIdentity) -> None:
        for label, expected_value, actual_value in self._identity_checks(
            self._identity,
            actual,
        ):
            if expected_value != actual_value:
                raise OverlayError(
                    OverlayErrorCode.CORRUPT,
                    f"persisted overlay identity changed at {label}",
                    field=label,
                )


Overlay = SQLiteOverlay


__all__ = [
    "FAULT_AFTER_BEGIN",
    "FAULT_AFTER_COMMIT",
    "FAULT_BEFORE_COMMIT",
    "FAULT_BEFORE_TRANSACTION",
    "GENESIS_MANIFEST_GENERATION",
    "GENESIS_MANIFEST_ROOT",
    "OVERLAY_SCHEMA_VERSION",
    "SEQUENCE_TEXT_WIDTH",
    "DurabilitySettings",
    "FaultInjector",
    "Overlay",
    "OverlayError",
    "OverlayErrorCode",
    "OverlayIdentity",
    "OverlayState",
    "OverlayTailDiscardResult",
    "OverlayThresholds",
    "SQLiteOverlay",
]
