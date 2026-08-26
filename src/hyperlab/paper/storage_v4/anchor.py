"""Monotone external-anchor interface and durable local development witness.

``LocalAnchor`` is intentionally a small SQLite database configured in
``DELETE``/``FULL`` mode.  It is suitable for deterministic tests and local
development only; it does not claim Linux root ownership or resistance to a
compromised administrator.
"""

from __future__ import annotations

import errno
import hashlib
import os
import sqlite3
import stat
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO, Protocol, cast

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

from .durability import fsync_directory
from .faults import FaultHook, FaultPoint, trigger_fault
from .types import UINT64_MAX, Hash32, StoreId

ANCHOR_SCHEMA_VERSION = 1
ANCHOR_APPLICATION_ID = 0x484C3441  # ASCII ``HL4A``.
ANCHOR_WRITER_LEASE_MAGIC = b"HL4ANCHOR-WRITER-LEASE\x00\x01"
_ANCHOR_WRITER_LEASE_DOMAIN = b"HL4-ANCHOR-WRITER-LEASE-AUTHORITY"


class AnchorErrorCode(StrEnum):
    MISSING = "ANCHOR_MISSING"
    ALREADY_EXISTS = "ANCHOR_ALREADY_EXISTS"
    CORRUPT = "ANCHOR_CORRUPT"
    STORE_MISMATCH = "ANCHOR_STORE_MISMATCH"
    EXPECTED_MISMATCH = "ANCHOR_EXPECTED_MISMATCH"
    ROLLBACK = "ANCHOR_ROLLBACK"
    FORK = "ANCHOR_FORK"
    WRITER_LEASE_HELD = "ANCHOR_WRITER_LEASE_HELD"
    WRITER_LEASE_FAILED = "ANCHOR_WRITER_LEASE_FAILED"
    READ_ONLY = "ANCHOR_READ_ONLY"


class AnchorError(RuntimeError):
    """A fail-closed anchor rejection with a stable machine-readable code."""

    def __init__(self, code: AnchorErrorCode, message: str) -> None:
        if type(code) is not AnchorErrorCode:
            raise TypeError("anchor error code must be AnchorErrorCode")
        self.code = code
        super().__init__(f"{code.value}: {message}")


@dataclass(frozen=True, slots=True)
class AnchorRecord:
    store_id: StoreId
    generation: int
    manifest_root: Hash32

    def __post_init__(self) -> None:
        if type(self.store_id) is not StoreId:
            raise TypeError("anchor store_id must be StoreId")
        if type(self.generation) is not int:
            raise TypeError("anchor generation must be an exact integer")
        if self.generation < 1 or self.generation > UINT64_MAX:
            raise ValueError("anchor generation must be between 1 and uint64 maximum")
        if type(self.manifest_root) is not Hash32:
            raise TypeError("anchor manifest_root must be Hash32")


class AnchorWriterLease(Protocol):
    """Exclusive cooperative writer authority held until explicit close."""

    @property
    def closed(self) -> bool: ...

    def close(self) -> None: ...

    def __enter__(self) -> AnchorWriterLease: ...

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None: ...


class Anchor(Protocol):
    """Minimal anti-rollback witness consumed by repository startup."""

    @property
    def store_id(self) -> StoreId: ...

    def acquire_writer_lease(self) -> AnchorWriterLease: ...

    def read(self) -> AnchorRecord | None: ...

    def compare_and_swap(
        self,
        expected: AnchorRecord | None,
        new: AnchorRecord,
    ) -> AnchorRecord: ...

    def reattest(self, expected: AnchorRecord) -> AnchorRecord: ...


_CREATE_META_SQL = """
CREATE TABLE anchor_meta (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    store_id TEXT NOT NULL CHECK (typeof(store_id) = 'text' AND length(store_id) > 0)
) WITHOUT ROWID
"""

_CREATE_STATE_SQL = """
CREATE TABLE anchor_state (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    generation BLOB NOT NULL CHECK (typeof(generation) = 'blob' AND length(generation) = 8),
    manifest_root BLOB NOT NULL CHECK (typeof(manifest_root) = 'blob' AND length(manifest_root) = 32),
    FOREIGN KEY (singleton) REFERENCES anchor_meta(singleton)
) WITHOUT ROWID
"""


def _generation_bytes(generation: int) -> bytes:
    return generation.to_bytes(8, "big", signed=False)


def _anchor_error(code: AnchorErrorCode, message: str) -> AnchorError:
    return AnchorError(code, message)


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


def _writer_lease_spec(
    anchor_path: Path,
    store_id: StoreId,
) -> tuple[Path, bytes]:
    if _is_link_or_reparse_point(anchor_path):
        raise _anchor_error(
            AnchorErrorCode.WRITER_LEASE_FAILED,
            "writer lease anchor path is a symbolic link or reparse point",
        )
    try:
        canonical_anchor = anchor_path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise _anchor_error(
            AnchorErrorCode.WRITER_LEASE_FAILED,
            "writer lease anchor path cannot be resolved",
        ) from error
    if (
        _is_link_or_reparse_point(canonical_anchor)
        or not canonical_anchor.is_file()
    ):
        raise _anchor_error(
            AnchorErrorCode.WRITER_LEASE_FAILED,
            "writer lease anchor is not a regular file",
        )

    normalized_path = os.path.normcase(os.fspath(canonical_anchor))
    path_bytes = os.fsencode(normalized_path)
    store_bytes = store_id.value.encode("utf-8", errors="strict")
    identity_material = b"".join(
        (
            _ANCHOR_WRITER_LEASE_DOMAIN,
            len(path_bytes).to_bytes(8, "big", signed=False),
            path_bytes,
            len(store_bytes).to_bytes(8, "big", signed=False),
            store_bytes,
        )
    )
    identity = hashlib.sha256(identity_material).digest()
    path = canonical_anchor.parent / (
        f".hyperlab-anchor-writer-{identity.hex()}.lease"
    )
    return path, ANCHOR_WRITER_LEASE_MAGIC + identity


def _lock_writer_fd(fd: int) -> None:
    os.lseek(fd, 0, os.SEEK_SET)
    if sys.platform == "win32":
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _is_lock_contention(error: OSError) -> bool:
    return error.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK} or getattr(
        error,
        "winerror",
        None,
    ) in {32, 33}


def _write_all(fd: int, data: bytes) -> None:
    remaining = memoryview(data)
    while remaining:
        written = os.write(fd, remaining)
        if written <= 0:
            raise OSError("writer lease identity write made no progress")
        remaining = remaining[written:]


@dataclass(slots=True)
class _LocalAnchorWriterLease:
    path: Path
    _stream: BinaryIO
    _closed: bool = False

    @property
    def closed(self) -> bool:
        return self._closed or self._stream.closed

    def close(self) -> None:
        if self.closed:
            self._closed = True
            return
        try:
            self._stream.close()
        except OSError as error:
            self._closed = self._stream.closed
            raise _anchor_error(
                AnchorErrorCode.WRITER_LEASE_FAILED,
                "writer lease file handle could not be closed",
            ) from error
        self._closed = True

    def __enter__(self) -> _LocalAnchorWriterLease:
        if self.closed:
            raise _anchor_error(
                AnchorErrorCode.WRITER_LEASE_FAILED,
                "writer lease is already closed",
            )
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        self.close()


def _acquire_local_writer_lease(
    path: Path,
    expected_identity: bytes,
) -> AnchorWriterLease:
    if _is_link_or_reparse_point(path):
        raise _anchor_error(
            AnchorErrorCode.WRITER_LEASE_FAILED,
            "writer lease path is a symbolic link or reparse point",
        )
    flags = os.O_CREAT | os.O_RDWR
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOINHERIT", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as error:
        raise _anchor_error(
            AnchorErrorCode.WRITER_LEASE_FAILED,
            "writer lease file cannot be opened safely",
        ) from error

    try:
        descriptor_stat = os.fstat(fd)
        path_stat = os.stat(path, follow_symlinks=False)
        if (
            _is_link_or_reparse_point(path)
            or not stat.S_ISREG(descriptor_stat.st_mode)
            or not stat.S_ISREG(path_stat.st_mode)
            or not os.path.samestat(descriptor_stat, path_stat)
        ):
            raise _anchor_error(
                AnchorErrorCode.WRITER_LEASE_FAILED,
                "writer lease path is not the opened regular file",
            )
        if descriptor_stat.st_size < 1:
            os.lseek(fd, 0, os.SEEK_SET)
            _write_all(fd, b"\x00")
            os.fsync(fd)
        try:
            _lock_writer_fd(fd)
        except OSError as error:
            if _is_lock_contention(error):
                raise _anchor_error(
                    AnchorErrorCode.WRITER_LEASE_HELD,
                    "another writer holds the anchor/store lease",
                ) from error
            raise _anchor_error(
                AnchorErrorCode.WRITER_LEASE_FAILED,
                "operating system refused the anchor/store writer lease",
            ) from error

        locked_stat = os.fstat(fd)
        locked_path_stat = os.stat(path, follow_symlinks=False)
        if (
            _is_link_or_reparse_point(path)
            or not stat.S_ISREG(locked_stat.st_mode)
            or not stat.S_ISREG(locked_path_stat.st_mode)
            or not os.path.samestat(locked_stat, locked_path_stat)
        ):
            raise _anchor_error(
                AnchorErrorCode.WRITER_LEASE_FAILED,
                "writer lease path changed during acquisition",
            )

        os.lseek(fd, 0, os.SEEK_SET)
        observed = os.read(fd, len(expected_identity) + 1)
        if observed == b"\x00":
            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            _write_all(fd, expected_identity)
            os.fsync(fd)
        elif observed != expected_identity:
            raise _anchor_error(
                AnchorErrorCode.WRITER_LEASE_FAILED,
                "writer lease identity differs from its anchor/store authority",
            )
        fsync_directory(path.parent)
        stream = cast(BinaryIO, os.fdopen(fd, "r+b", buffering=0))
    except AnchorError:
        os.close(fd)
        raise
    except OSError as error:
        os.close(fd)
        raise _anchor_error(
            AnchorErrorCode.WRITER_LEASE_FAILED,
            "writer lease acquisition failed",
        ) from error
    except BaseException:
        os.close(fd)
        raise
    return _LocalAnchorWriterLease(path=path, _stream=stream)


def _configure_connection(connection: sqlite3.Connection, *, initialize: bool) -> None:
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA foreign_keys = ON")
    if initialize:
        journal = connection.execute("PRAGMA journal_mode = DELETE").fetchone()
    else:
        journal = connection.execute("PRAGMA journal_mode").fetchone()
    if journal is None or str(journal[0]).lower() != "delete":
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness is not in SQLite DELETE journal mode",
        )
    connection.execute("PRAGMA synchronous = FULL")
    synchronous = connection.execute("PRAGMA synchronous").fetchone()
    if synchronous is None or int(synchronous[0]) != 2:
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness could not enable SQLite FULL synchronous mode",
        )


def _configure_read_only_connection(connection: sqlite3.Connection) -> None:
    """Enable and prove SQLite's connection-scoped read-only guard."""

    try:
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA trusted_schema = OFF")
        connection.execute("PRAGMA query_only = ON")
        query_only = connection.execute("PRAGMA query_only").fetchone()
        journal = connection.execute("PRAGMA journal_mode").fetchone()
        synchronous = connection.execute("PRAGMA synchronous").fetchone()
    except sqlite3.Error as error:
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness could not enable SQLite read-only validation",
        ) from error
    if query_only != (1,):
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness SQLite query_only guard is not active",
        )
    if journal is None or str(journal[0]).lower() != "delete":
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness is not in SQLite DELETE journal mode",
        )
    if synchronous is None or int(synchronous[0]) != 2:
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness is not in SQLite FULL synchronous mode",
        )


def _table_columns(
    connection: sqlite3.Connection,
    table: str,
) -> tuple[tuple[object, ...], ...]:
    return tuple(tuple(row) for row in connection.execute(f"PRAGMA table_info({table})"))


def _validate_schema(connection: sqlite3.Connection, store_id: StoreId) -> None:
    application_id = connection.execute("PRAGMA application_id").fetchone()
    user_version = connection.execute("PRAGMA user_version").fetchone()
    if application_id != (ANCHOR_APPLICATION_ID,) or user_version != (
        ANCHOR_SCHEMA_VERSION,
    ):
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness schema identity is missing or unsupported",
        )

    integrity = tuple(connection.execute("PRAGMA integrity_check"))
    if integrity != (("ok",),):
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness failed SQLite integrity_check",
        )

    tables = tuple(
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema "
            "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    )
    if tables != ("anchor_meta", "anchor_state"):
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness has an unexpected table set",
        )
    meta_columns = tuple(row[1] for row in _table_columns(connection, "anchor_meta"))
    state_columns = tuple(row[1] for row in _table_columns(connection, "anchor_state"))
    if meta_columns != ("singleton", "schema_version", "store_id") or state_columns != (
        "singleton",
        "generation",
        "manifest_root",
    ):
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness columns differ from schema v1",
        )

    metadata = tuple(
        connection.execute(
            "SELECT singleton, schema_version, store_id FROM anchor_meta"
        )
    )
    if len(metadata) != 1:
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness must contain exactly one metadata row",
        )
    singleton, schema_version, stored_store_id = metadata[0]
    if singleton != 1 or schema_version != ANCHOR_SCHEMA_VERSION:
        raise _anchor_error(AnchorErrorCode.CORRUPT, "local witness metadata is invalid")
    if type(stored_store_id) is not str:
        raise _anchor_error(AnchorErrorCode.CORRUPT, "local witness store ID is not text")
    try:
        observed_store_id = StoreId(stored_store_id)
    except (TypeError, ValueError) as error:
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness store ID is malformed",
        ) from error
    if observed_store_id != store_id:
        raise _anchor_error(
            AnchorErrorCode.STORE_MISMATCH,
            "local witness belongs to a different store",
        )


def _read_record(
    connection: sqlite3.Connection,
    store_id: StoreId,
) -> AnchorRecord | None:
    rows = tuple(
        connection.execute(
            "SELECT generation, manifest_root FROM anchor_state WHERE singleton = 1"
        )
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "local witness contains more than one anchor row",
        )
    generation_value, root_value = rows[0]
    if type(generation_value) is not bytes or len(generation_value) != 8:
        raise _anchor_error(AnchorErrorCode.CORRUPT, "anchor generation is malformed")
    if type(root_value) is not bytes or len(root_value) != 32:
        raise _anchor_error(AnchorErrorCode.CORRUPT, "anchor manifest root is malformed")
    try:
        return AnchorRecord(
            store_id=store_id,
            generation=int.from_bytes(generation_value, "big", signed=False),
            manifest_root=Hash32(root_value),
        )
    except (TypeError, ValueError) as error:
        raise _anchor_error(
            AnchorErrorCode.CORRUPT,
            "anchor record violates schema v1 bounds",
        ) from error


@dataclass(frozen=True, slots=True)
class LocalAnchor:
    """Explicitly created/opened deterministic local witness.

    The object opens a fresh SQLite connection for every operation, so a crash
    injected after commit can be re-observed by a separate recovery instance.
    """

    path: Path
    store_id: StoreId
    fault_hook: FaultHook = None
    read_only: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            raise TypeError("local anchor path must be pathlib.Path")
        if type(self.store_id) is not StoreId:
            raise TypeError("local anchor store_id must be StoreId")
        if type(self.read_only) is not bool:
            raise TypeError("local anchor read_only must be an exact bool")

    @property
    def writer_lease_path(self) -> Path:
        """Return the stable sidecar authority for this anchor/store pair."""

        path, _identity = _writer_lease_spec(self.path, self.store_id)
        return path

    def acquire_writer_lease(self) -> AnchorWriterLease:
        """Acquire one non-blocking writer lease without locking SQLite reads/CAS."""

        if self.read_only:
            raise _anchor_error(
                AnchorErrorCode.READ_ONLY,
                "read-only local witness cannot acquire writer authority",
            )

        # Validate the witness before deriving writer authority. Repository
        # admission must still re-read the anchor after acquiring this lease.
        self.read()
        path, identity = _writer_lease_spec(self.path, self.store_id)
        return _acquire_local_writer_lease(path, identity)

    @classmethod
    def create(
        cls,
        path: Path,
        *,
        store_id: StoreId,
        fault_hook: FaultHook = None,
    ) -> LocalAnchor:
        """Create a new empty witness; never open or overwrite an existing path."""

        if not isinstance(path, Path):
            raise TypeError("local anchor path must be pathlib.Path")
        if type(store_id) is not StoreId:
            raise TypeError("local anchor store_id must be StoreId")
        absolute = path.absolute()
        if _is_link_or_reparse_point(path) or _is_link_or_reparse_point(
            absolute
        ):
            raise _anchor_error(
                AnchorErrorCode.ALREADY_EXISTS,
                "refusing to create local witness through a link or reparse point",
            )
        if not absolute.parent.is_dir():
            raise FileNotFoundError(f"anchor parent does not exist: {absolute.parent}")
        try:
            with absolute.open("xb"):
                pass
        except FileExistsError as error:
            raise _anchor_error(
                AnchorErrorCode.ALREADY_EXISTS,
                "local witness path already exists",
            ) from error

        try:
            connection = sqlite3.connect(
                absolute,
                isolation_level=None,
                timeout=5.0,
            )
            try:
                _configure_connection(connection, initialize=True)
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(_CREATE_META_SQL)
                    connection.execute(_CREATE_STATE_SQL)
                    connection.execute(f"PRAGMA application_id = {ANCHOR_APPLICATION_ID}")
                    connection.execute(f"PRAGMA user_version = {ANCHOR_SCHEMA_VERSION}")
                    connection.execute(
                        "INSERT INTO anchor_meta(singleton, schema_version, store_id) "
                        "VALUES(1, ?, ?)",
                        (ANCHOR_SCHEMA_VERSION, store_id.value),
                    )
                except BaseException:
                    connection.rollback()
                    raise
                connection.commit()
                _validate_schema(connection, store_id)
            finally:
                connection.close()
            with absolute.open("r+b") as stream:
                stream.flush()
                os.fsync(stream.fileno())
            fsync_directory(absolute.parent)
        except BaseException:
            for suffix in ("-journal", "-wal", "-shm"):
                Path(f"{absolute}{suffix}").unlink(missing_ok=True)
            absolute.unlink(missing_ok=True)
            raise
        return cls(path=absolute, store_id=store_id, fault_hook=fault_hook)

    @classmethod
    def open_existing(
        cls,
        path: Path,
        *,
        store_id: StoreId,
        fault_hook: FaultHook = None,
    ) -> LocalAnchor:
        """Open and validate an existing witness without implicit creation."""

        if not isinstance(path, Path):
            raise TypeError("local anchor path must be pathlib.Path")
        if type(store_id) is not StoreId:
            raise TypeError("local anchor store_id must be StoreId")
        absolute = path.absolute()
        if _is_link_or_reparse_point(path) or _is_link_or_reparse_point(
            absolute
        ):
            raise _anchor_error(
                AnchorErrorCode.CORRUPT,
                "refusing to open local witness through a link or reparse point",
            )
        if not absolute.exists():
            raise _anchor_error(AnchorErrorCode.MISSING, "local witness path is missing")
        if not absolute.is_file():
            raise _anchor_error(
                AnchorErrorCode.CORRUPT,
                "local witness path is not a regular file",
            )
        anchor = cls(path=absolute, store_id=store_id, fault_hook=fault_hook)
        with anchor._connection():
            pass
        return anchor

    @classmethod
    def open_existing_read_only(
        cls,
        path: Path,
        *,
        store_id: StoreId,
    ) -> LocalAnchor:
        """Open an existing witness through ``mode=ro`` plus ``query_only``.

        This path never creates a writer-lease sidecar and rejects both CAS and
        writer-authority acquisition on the returned object.
        """

        if not isinstance(path, Path):
            raise TypeError("local anchor path must be pathlib.Path")
        if type(store_id) is not StoreId:
            raise TypeError("local anchor store_id must be StoreId")
        absolute = path.absolute()
        if _is_link_or_reparse_point(path) or _is_link_or_reparse_point(absolute):
            raise _anchor_error(
                AnchorErrorCode.CORRUPT,
                "refusing to open local witness through a link or reparse point",
            )
        if not absolute.exists():
            raise _anchor_error(AnchorErrorCode.MISSING, "local witness path is missing")
        if not absolute.is_file():
            raise _anchor_error(
                AnchorErrorCode.CORRUPT,
                "local witness path is not a regular file",
            )
        anchor = cls(path=absolute, store_id=store_id, read_only=True)
        with anchor._connection():
            pass
        return anchor

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if _is_link_or_reparse_point(self.path):
            raise _anchor_error(
                AnchorErrorCode.CORRUPT,
                "local witness path was replaced by a link or reparse point",
            )
        if not self.path.exists():
            raise _anchor_error(AnchorErrorCode.MISSING, "local witness path is missing")
        if not self.path.is_file():
            raise _anchor_error(
                AnchorErrorCode.CORRUPT,
                "local witness path is not a regular file",
            )
        mode = "ro" if self.read_only else "rw"
        uri = f"{self.path.as_uri()}?mode={mode}"
        try:
            connection = sqlite3.connect(
                uri,
                uri=True,
                isolation_level=None,
                timeout=5.0,
            )
        except sqlite3.Error as error:
            raise _anchor_error(
                AnchorErrorCode.CORRUPT,
                f"local witness could not be opened {mode}",
            ) from error
        try:
            try:
                if self.read_only:
                    _configure_read_only_connection(connection)
                else:
                    _configure_connection(connection, initialize=False)
                _validate_schema(connection, self.store_id)
                yield connection
            except AnchorError:
                raise
            except sqlite3.Error as error:
                raise _anchor_error(
                    AnchorErrorCode.CORRUPT,
                    "local witness SQLite operation failed",
                ) from error
        finally:
            connection.close()

    def read(self) -> AnchorRecord | None:
        with self._connection() as connection:
            return _read_record(connection, self.store_id)

    def reattest(self, expected: AnchorRecord) -> AnchorRecord:
        """Require an exact durable record; never reinterpret a near match."""

        if type(expected) is not AnchorRecord:
            raise TypeError("reattest expected value must be AnchorRecord")
        if expected.store_id != self.store_id:
            raise _anchor_error(
                AnchorErrorCode.STORE_MISMATCH,
                "reattest record belongs to a different store",
            )
        observed = self.read()
        if observed is None:
            raise _anchor_error(
                AnchorErrorCode.EXPECTED_MISMATCH,
                "anchor is empty while an exact record was expected",
            )
        if observed == expected:
            return observed
        if expected.generation < observed.generation:
            code = AnchorErrorCode.ROLLBACK
            message = "reattest candidate is older than the durable anchor"
        elif expected.generation == observed.generation:
            code = AnchorErrorCode.FORK
            message = "reattest candidate forks the durable generation"
        else:
            code = AnchorErrorCode.EXPECTED_MISMATCH
            message = "reattest candidate is ahead of the durable anchor"
        raise _anchor_error(code, message)

    def compare_and_swap(
        self,
        expected: AnchorRecord | None,
        new: AnchorRecord,
    ) -> AnchorRecord:
        """Publish ``new`` only from the exact expected durable record.

        A byte-exact same-generation value is an idempotent reattestation.
        Same-generation divergent roots are forks and lower generations are
        rollbacks.  Jumps to a later verified manifest generation remain valid
        because manifest-chain continuity is checked by the repository layer.
        """

        if self.read_only:
            raise _anchor_error(
                AnchorErrorCode.READ_ONLY,
                "read-only local witness cannot compare-and-swap authority",
            )

        if expected is not None and type(expected) is not AnchorRecord:
            raise TypeError("anchor CAS expected value must be AnchorRecord or None")
        if type(new) is not AnchorRecord:
            raise TypeError("anchor CAS new value must be AnchorRecord")
        for label, record in (("expected", expected), ("new", new)):
            if record is not None and record.store_id != self.store_id:
                raise _anchor_error(
                    AnchorErrorCode.STORE_MISMATCH,
                    f"anchor CAS {label} record belongs to a different store",
                )

        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            committed = False
            try:
                current = _read_record(connection, self.store_id)
                if current != expected:
                    raise _anchor_error(
                        AnchorErrorCode.EXPECTED_MISMATCH,
                        "durable anchor differs from compare-and-swap expectation",
                    )
                if current is not None:
                    if new.generation < current.generation:
                        raise _anchor_error(
                            AnchorErrorCode.ROLLBACK,
                            "anchor generation cannot move backward",
                        )
                    if (
                        new.generation == current.generation
                        and new.manifest_root != current.manifest_root
                    ):
                        raise _anchor_error(
                            AnchorErrorCode.FORK,
                            "same anchor generation cannot attest a different root",
                        )
                trigger_fault(self.fault_hook, FaultPoint.BEFORE_ANCHOR_PUBLICATION)
                if current is None:
                    connection.execute(
                        "INSERT INTO anchor_state(singleton, generation, manifest_root) "
                        "VALUES(1, ?, ?)",
                        (_generation_bytes(new.generation), bytes(new.manifest_root)),
                    )
                elif new != current:
                    cursor = connection.execute(
                        "UPDATE anchor_state SET generation = ?, manifest_root = ? "
                        "WHERE singleton = 1 AND generation = ? AND manifest_root = ?",
                        (
                            _generation_bytes(new.generation),
                            bytes(new.manifest_root),
                            _generation_bytes(current.generation),
                            bytes(current.manifest_root),
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise _anchor_error(
                            AnchorErrorCode.CORRUPT,
                            "anchor singleton changed during locked publication",
                        )
                connection.commit()
                committed = True
            except BaseException:
                if not committed:
                    connection.rollback()
                raise

        # SQLite FULL supplies the file barrier.  The explicit directory flush
        # closes the local publication boundary and lets an idempotent retry heal
        # an interruption immediately after commit.
        fsync_directory(self.path.parent)
        trigger_fault(self.fault_hook, FaultPoint.AFTER_ANCHOR_PUBLICATION)
        return new


__all__ = [
    "ANCHOR_APPLICATION_ID",
    "ANCHOR_SCHEMA_VERSION",
    "ANCHOR_WRITER_LEASE_MAGIC",
    "Anchor",
    "AnchorError",
    "AnchorErrorCode",
    "AnchorRecord",
    "AnchorWriterLease",
    "LocalAnchor",
]
