"""Fail-closed local publication primitives for Storage v4 artifacts.

Immutable artifacts use a fresh UUID temporary file and an exclusive hard-link
publication.  The link operation is atomic and cannot replace an existing name.
An existing byte-identical artifact is an idempotent success; a divergent target
is always refused.  Mutable cache files (notably ``CURRENT``) use a separate
atomic replacement path and never become an authority by virtue of this helper.
"""

from __future__ import annotations

import ctypes
import errno
import os
import stat
import sys
from ctypes import wintypes
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO, Protocol
from uuid import uuid4

from .faults import FaultHook, FaultPoint, InjectedCrash, trigger_fault


class ArtifactVerifier(Protocol):
    """Validate exact read-back bytes or raise a domain-specific exception."""

    def __call__(self, data: bytes, /) -> object: ...


class DurabilityError(RuntimeError):
    """A durable publication invariant could not be established."""


class ImmutableTargetConflict(DurabilityError):
    """An immutable target already exists with different bytes."""


class PublishDisposition(StrEnum):
    CREATED = "created"
    ALREADY_PRESENT = "already_present"
    REPLACED = "replaced"


@dataclass(frozen=True, slots=True)
class PublishedArtifact:
    path: Path
    disposition: PublishDisposition


def _require_parent(path: Path) -> None:
    if not path.parent.exists():
        raise FileNotFoundError(f"publication parent does not exist: {path.parent}")
    if not path.parent.is_dir():
        raise NotADirectoryError(f"publication parent is not a directory: {path.parent}")
    if path.is_symlink():
        raise DurabilityError(f"refusing publication through symbolic link: {path}")


def _verify_bytes(data: bytes, verifier: ArtifactVerifier | None) -> None:
    if verifier is not None:
        verifier(data)


def _read_verified_exact_target(
    path: Path,
    expected: bytes,
    verifier: ArtifactVerifier | None,
) -> None:
    try:
        observed_stat = os.stat(path, follow_symlinks=False)
    except OSError as error:
        raise ImmutableTargetConflict(
            f"immutable target cannot be inspected safely: {path.name}"
        ) from error
    if path.is_symlink() or not stat.S_ISREG(observed_stat.st_mode):
        raise ImmutableTargetConflict(
            f"immutable target is not a regular file: {path.name}"
        )
    if observed_stat.st_nlink != 1:
        raise ImmutableTargetConflict(
            f"immutable target has a dangerous hardlink count: {path.name}"
        )
    observed = path.read_bytes()
    if observed != expected:
        raise ImmutableTargetConflict(
            f"refusing to overwrite divergent immutable target: {path.name}"
        )
    _verify_bytes(observed, verifier)


def flush_and_fsync(stream: BinaryIO, *, fault_hook: FaultHook = None) -> None:
    """Flush Python buffers and force one open regular file to stable storage."""

    trigger_fault(fault_hook, FaultPoint.BEFORE_FLUSH)
    stream.flush()
    trigger_fault(fault_hook, FaultPoint.AFTER_FLUSH)
    trigger_fault(fault_hook, FaultPoint.BEFORE_FILE_FSYNC)
    os.fsync(stream.fileno())
    trigger_fault(fault_hook, FaultPoint.AFTER_FILE_FSYNC)


def fsync_directory(path: Path) -> None:
    """Durably flush directory entries on POSIX and Windows or fail closed."""

    if os.name != "nt":
        descriptor = os.open(
            path,
            os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0)),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    flush_file_buffers = kernel32.FlushFileBuffers
    flush_file_buffers.argtypes = [wintypes.HANDLE]
    flush_file_buffers.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL

    handle = create_file(
        str(path),
        0x40000000,  # GENERIC_WRITE, required by FlushFileBuffers.
        0x00000007,  # FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE.
        None,
        3,  # OPEN_EXISTING.
        0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS permits directory handles.
        None,
    )
    invalid_handle = wintypes.HANDLE(-1).value
    if handle == invalid_handle:
        raise ctypes.WinError(ctypes.get_last_error())

    flushed = False
    flush_error = 0
    try:
        flushed = bool(flush_file_buffers(handle))
        flush_error = ctypes.get_last_error() if not flushed else 0
    finally:
        closed = bool(close_handle(handle))
        close_error = ctypes.get_last_error() if not closed else 0
    if not flushed:
        raise ctypes.WinError(flush_error)
    if not closed:
        raise ctypes.WinError(close_error)


def _directory_barrier(path: Path, fault_hook: FaultHook) -> None:
    trigger_fault(fault_hook, FaultPoint.BEFORE_DIRECTORY_FSYNC)
    fsync_directory(path)
    trigger_fault(fault_hook, FaultPoint.AFTER_DIRECTORY_FSYNC)


def _new_verified_temporary(
    target: Path,
    data: bytes,
    *,
    verifier: ArtifactVerifier | None,
    fault_hook: FaultHook,
) -> Path:
    for _attempt in range(16):
        temporary = target.parent / f".{target.name}.{uuid4().hex}.tmp"
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        for name in ("O_BINARY", "O_NOINHERIT", "O_NOFOLLOW"):
            flags |= int(getattr(os, name, 0))
        try:
            descriptor = os.open(temporary, flags, 0o600)
        except FileExistsError:
            continue
        try:
            stream = os.fdopen(descriptor, "wb")
            with stream:
                trigger_fault(fault_hook, FaultPoint.BEFORE_TEMP_WRITE)
                written = stream.write(data)
                if written != len(data):
                    raise DurabilityError("temporary artifact write was incomplete")
                trigger_fault(fault_hook, FaultPoint.AFTER_TEMP_WRITE)
                flush_and_fsync(stream, fault_hook=fault_hook)
            observed = temporary.read_bytes()
            if observed != data:
                raise DurabilityError("temporary artifact read-back differs from input")
            _verify_bytes(observed, verifier)
            return temporary
        except InjectedCrash:
            raise
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
    raise DurabilityError("could not allocate a fresh UUID temporary file")


def atomic_rename_noreplace(source: Path, target: Path) -> None:
    """Atomically rename ``source`` without replacing an existing target."""

    if os.name == "nt":
        # MoveFileEx without MOVEFILE_REPLACE_EXISTING is the behavior exposed
        # by os.rename on Windows.
        os.rename(source, target)
        return
    if not sys.platform.startswith("linux"):
        raise DurabilityError(
            "exclusive immutable rename is implemented only for Linux and Windows"
        )
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise DurabilityError("Linux libc does not expose renameat2")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    at_fdcwd = -100
    rename_noreplace = 1
    result = renameat2(
        at_fdcwd,
        os.fsencode(source),
        at_fdcwd,
        os.fsencode(target),
        rename_noreplace,
    )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number == errno.EEXIST:
        raise FileExistsError(error_number, os.strerror(error_number), target)
    raise OSError(error_number, os.strerror(error_number), target)


def durable_publish_immutable(
    target: Path,
    data: bytes,
    *,
    verifier: ArtifactVerifier | None = None,
    fault_hook: FaultHook = None,
) -> PublishedArtifact:
    """Publish immutable bytes exclusively, idempotently, and durably.

    An :class:`InjectedCrash` before rename leaves the fresh temporary file in
    place.  After rename the target has one link and no temporary alias.
    Recovery never treats a temporary orphan as published authority.
    """

    if not isinstance(target, Path):
        raise TypeError("immutable publication target must be pathlib.Path")
    if type(data) is not bytes:
        raise TypeError("immutable publication data must be exact bytes")
    _require_parent(target)

    if target.exists():
        _read_verified_exact_target(target, data, verifier)
        _directory_barrier(target.parent, fault_hook)
        return PublishedArtifact(target, PublishDisposition.ALREADY_PRESENT)

    temporary = _new_verified_temporary(
        target,
        data,
        verifier=verifier,
        fault_hook=fault_hook,
    )
    try:
        trigger_fault(fault_hook, FaultPoint.BEFORE_RENAME)
        trigger_fault(fault_hook, FaultPoint.BEFORE_EXCLUSIVE_PUBLISH)
        try:
            atomic_rename_noreplace(temporary, target)
        except FileExistsError:
            _read_verified_exact_target(target, data, verifier)
            _directory_barrier(target.parent, fault_hook)
            temporary.unlink(missing_ok=True)
            fsync_directory(target.parent)
            return PublishedArtifact(target, PublishDisposition.ALREADY_PRESENT)
        trigger_fault(fault_hook, FaultPoint.AFTER_EXCLUSIVE_PUBLISH)
        trigger_fault(fault_hook, FaultPoint.AFTER_RENAME)
        _read_verified_exact_target(target, data, None)
        _directory_barrier(target.parent, fault_hook)
        return PublishedArtifact(target, PublishDisposition.CREATED)
    except InjectedCrash:
        raise
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def atomic_write_mutable_cache(
    target: Path,
    data: bytes,
    *,
    verifier: ArtifactVerifier | None = None,
    fault_hook: FaultHook = None,
) -> PublishedArtifact:
    """Atomically replace a mutable cache file, then flush its directory entry."""

    if not isinstance(target, Path):
        raise TypeError("mutable cache target must be pathlib.Path")
    if type(data) is not bytes:
        raise TypeError("mutable cache data must be exact bytes")
    _require_parent(target)
    if target.exists():
        observed = os.stat(target, follow_symlinks=False)
        if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
            raise DurabilityError(
                f"refusing mutable cache replacement through unsafe target: {target}"
            )
    existed = target.exists()
    temporary = _new_verified_temporary(
        target,
        data,
        verifier=verifier,
        fault_hook=fault_hook,
    )
    try:
        trigger_fault(fault_hook, FaultPoint.BEFORE_RENAME)
        os.replace(temporary, target)
        trigger_fault(fault_hook, FaultPoint.AFTER_RENAME)
        _directory_barrier(target.parent, fault_hook)
        return PublishedArtifact(
            target,
            PublishDisposition.REPLACED if existed else PublishDisposition.CREATED,
        )
    except InjectedCrash:
        raise
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


# Short aliases for repository/sealer callers.
publish_immutable = durable_publish_immutable
write_mutable_cache = atomic_write_mutable_cache


__all__ = [
    "ArtifactVerifier",
    "DurabilityError",
    "ImmutableTargetConflict",
    "PublishDisposition",
    "PublishedArtifact",
    "atomic_rename_noreplace",
    "atomic_write_mutable_cache",
    "durable_publish_immutable",
    "flush_and_fsync",
    "fsync_directory",
    "publish_immutable",
    "write_mutable_cache",
]
