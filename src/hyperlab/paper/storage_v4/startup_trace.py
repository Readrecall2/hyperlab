"""Bounded Python-level startup file-access evidence for Storage v4 Phase 1C.

The tracer is deliberately narrow.  It observes the Python APIs used by the
prototype's synchronous normal-reopen path and restores every process-global
hook before returning.  It is not an operating-system or SQLite VFS trace.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import IO, Any, cast
from urllib.parse import unquote, urlsplit
from urllib.request import url2pathname

STARTUP_TRACE_STATUS = "EXACT_BOUNDED_PYTHON_LEVEL_STARTUP_FILE_ACCESS_TRACE"


class StartupTraceError(RuntimeError):
    """Raised when a startup trace cannot remain exact and safely bounded."""


class StartupFileCategory(StrEnum):
    RAW_MANIFEST = "RAW_MANIFEST"
    RAW_CURRENT_CACHE = "RAW_CURRENT_CACHE"
    RAW_PENDING_RECOVERY = "RAW_PENDING_RECOVERY"
    PAPER_MANIFEST = "PAPER_MANIFEST"
    PAPER_CHECKPOINT = "PAPER_CHECKPOINT"
    PAPER_CURRENT_CACHE = "PAPER_CURRENT_CACHE"
    PAPER_OVERLAY = "PAPER_OVERLAY"
    RAW_ANCHOR = "RAW_ANCHOR"
    PAPER_ANCHOR = "PAPER_ANCHOR"
    RAW_ANCHOR_WRITER_LEASE = "RAW_ANCHOR_WRITER_LEASE"
    PAPER_ANCHOR_WRITER_LEASE = "PAPER_ANCHOR_WRITER_LEASE"
    PAPER_WRITER_LEASE = "PAPER_WRITER_LEASE"
    RAW_HISTORICAL_SEGMENT = "RAW_HISTORICAL_SEGMENT"
    PAPER_HISTORICAL_SEGMENT = "PAPER_HISTORICAL_SEGMENT"


class StartupOpenApi(StrEnum):
    OS_OPEN = "OS_OPEN"
    PATH_OPEN = "PATH_OPEN"
    SQLITE_CONNECT = "SQLITE_CONNECT"


@dataclass(frozen=True, slots=True)
class StartupTracePaths:
    """Exact candidate paths that a normal reopen is allowed to request."""

    candidate_root: Path
    raw_root: Path
    paper_root: Path
    raw_anchor: Path
    paper_anchor: Path
    raw_anchor_writer_lease: Path
    paper_anchor_writer_lease: Path
    paper_writer_lease: Path

    def __post_init__(self) -> None:
        for label, value in (
            ("candidate_root", self.candidate_root),
            ("raw_root", self.raw_root),
            ("paper_root", self.paper_root),
            ("raw_anchor", self.raw_anchor),
            ("paper_anchor", self.paper_anchor),
            ("raw_anchor_writer_lease", self.raw_anchor_writer_lease),
            ("paper_anchor_writer_lease", self.paper_anchor_writer_lease),
            ("paper_writer_lease", self.paper_writer_lease),
        ):
            if not isinstance(value, Path) or not value.is_absolute():
                raise ValueError(f"{label} must be an absolute pathlib.Path")
        root = self.candidate_root
        if not root.is_dir() or _is_link_or_reparse_point(root):
            raise StartupTraceError("candidate_root must be an existing safe directory")
        for label, value in (
            ("raw_root", self.raw_root),
            ("paper_root", self.paper_root),
            ("raw_anchor", self.raw_anchor),
            ("paper_anchor", self.paper_anchor),
            ("raw_anchor_writer_lease", self.raw_anchor_writer_lease),
            ("paper_anchor_writer_lease", self.paper_anchor_writer_lease),
            ("paper_writer_lease", self.paper_writer_lease),
        ):
            try:
                value.relative_to(root)
            except ValueError as error:
                raise StartupTraceError(f"{label} escapes candidate_root") from error
        if self.raw_root == self.paper_root:
            raise StartupTraceError("raw_root and paper_root must be distinct")


@dataclass(frozen=True, slots=True)
class StartupFileOpen:
    sequence: int
    relative_path: str
    category: StartupFileCategory
    api: StartupOpenApi
    access: str
    size_bytes_after_scope: int
    sha256_after_scope: str

    def payload(self) -> dict[str, object]:
        return {
            "access": self.access,
            "api": self.api.value,
            "category": self.category.value,
            "relative_path": self.relative_path,
            "sequence": self.sequence,
            "sha256_after_scope": self.sha256_after_scope,
            "size_bytes_after_scope": self.size_bytes_after_scope,
        }


@dataclass(frozen=True, slots=True)
class StartupFileAccessTrace:
    """Immutable evidence for one synchronous normal-reopen scope."""

    candidate_root: Path
    opens: tuple[StartupFileOpen, ...]
    status: str = STARTUP_TRACE_STATUS

    def __post_init__(self) -> None:
        if not isinstance(self.candidate_root, Path) or not self.candidate_root.is_absolute():
            raise ValueError("candidate_root must be an absolute pathlib.Path")
        if type(self.opens) is not tuple or not self.opens:
            raise ValueError("startup trace must contain at least one observed open")
        if tuple(item.sequence for item in self.opens) != tuple(range(1, len(self.opens) + 1)):
            raise ValueError("startup trace sequences must be contiguous and ordered")
        if any(
            item.category
            in {
                StartupFileCategory.RAW_HISTORICAL_SEGMENT,
                StartupFileCategory.PAPER_HISTORICAL_SEGMENT,
            }
            for item in self.opens
        ):
            raise ValueError("successful startup trace cannot contain historical segments")
        if self.status != STARTUP_TRACE_STATUS:
            raise ValueError("startup trace status is not canonical")

    @property
    def historical_segment_open_count(self) -> int:
        return 0

    def payload(self) -> dict[str, object]:
        return {
            "candidate_root": str(self.candidate_root),
            "historical_segment_open_count": self.historical_segment_open_count,
            "historical_segment_paths_opened": [],
            "limitations": [
                "PYTHON_LEVEL_INTERCEPTION_NOT_AN_OS_KERNEL_OR_ETW_TRACE",
                "SQLITE_CONNECT_REQUESTS_ARE_OBSERVED_BUT_SQLITE_INTERNAL_IO_AND_SIDECARS_ARE_NOT",
                "ONLY_REQUESTED_PATHS_UNDER_THE_CANDIDATE_ROOT_ARE_RECORDED",
                "HASHES_AND_SIZES_ARE_POST_SCOPE_VALUES_FOR_PERSISTENT_REQUESTED_FILES",
                "PROCESS_GLOBAL_HOOKS_EXIST_ONLY_DURING_ONE_SYNCHRONOUS_NON_CONCURRENT_SCOPE",
            ],
            "opens": [item.payload() for item in self.opens],
            "ordered_relative_paths": [item.relative_path for item in self.opens],
            "scope": (
                "normal raw/Paper reopen and authority alignment only; exhaustive audits, "
                "differentials, and independent oracles are outside this trace"
            ),
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class _ObservedOpen:
    path: Path
    category: StartupFileCategory
    api: StartupOpenApi
    access: str


def _is_link_or_reparse_point(path: Path) -> bool:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(path_stat.st_mode):
        return True
    attributes = int(getattr(path_stat, "st_file_attributes", 0))
    reparse_mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse_mask)


def _sqlite_path(database: object) -> Path | None:
    if isinstance(database, bytes):
        selected = os.fsdecode(database)
    elif isinstance(database, (str, os.PathLike)):
        selected = os.fspath(database)
    else:
        return None
    if isinstance(selected, bytes):
        selected = os.fsdecode(selected)
    if selected == ":memory:":
        return None
    if selected.startswith("file:"):
        parsed = urlsplit(selected)
        if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
            raise StartupTraceError("SQLite URI is not a local file authority")
        selected = url2pathname(unquote(parsed.path))
    return Path(selected)


class _StartupTraceRecorder:
    def __init__(self, paths: StartupTracePaths) -> None:
        self._paths = paths
        self._observed: list[_ObservedOpen] = []
        self._result: StartupFileAccessTrace | None = None
        self._original_os_open = os.open
        self._original_path_open = Path.open
        self._original_sqlite_connect = sqlite3.connect
        self._observing = False

    @property
    def result(self) -> StartupFileAccessTrace:
        if self._result is None:
            raise StartupTraceError("startup trace has not completed successfully")
        return self._result

    def _absolute_candidate_path(self, value: object) -> Path | None:
        if isinstance(value, int):
            return None
        if isinstance(value, bytes):
            selected = os.fsdecode(value)
        elif isinstance(value, (str, os.PathLike)):
            selected = os.fspath(value)
        else:
            return None
        if isinstance(selected, bytes):
            selected = os.fsdecode(selected)
        path = Path(selected)
        absolute = path if path.is_absolute() else path.absolute()
        try:
            relative = absolute.relative_to(self._paths.candidate_root)
        except ValueError:
            return None
        current = self._paths.candidate_root
        if _is_link_or_reparse_point(current):
            raise StartupTraceError("candidate_root became a link or reparse point")
        for component in relative.parts:
            current = current / component
            if _is_link_or_reparse_point(current):
                raise StartupTraceError(
                    "startup access traversed a link or reparse point under candidate_root"
                )
        return absolute

    def _category(self, path: Path) -> StartupFileCategory:
        exact: Mapping[Path, StartupFileCategory] = {
            self._paths.raw_root / "CURRENT": StartupFileCategory.RAW_CURRENT_CACHE,
            self._paths.raw_root / "PENDING": StartupFileCategory.RAW_PENDING_RECOVERY,
            self._paths.paper_root / "CURRENT": StartupFileCategory.PAPER_CURRENT_CACHE,
            self._paths.paper_root / "overlay.sqlite3": StartupFileCategory.PAPER_OVERLAY,
            self._paths.raw_anchor: StartupFileCategory.RAW_ANCHOR,
            self._paths.paper_anchor: StartupFileCategory.PAPER_ANCHOR,
            self._paths.raw_anchor_writer_lease: (
                StartupFileCategory.RAW_ANCHOR_WRITER_LEASE
            ),
            self._paths.paper_anchor_writer_lease: (
                StartupFileCategory.PAPER_ANCHOR_WRITER_LEASE
            ),
            self._paths.paper_writer_lease: StartupFileCategory.PAPER_WRITER_LEASE,
        }
        if path in exact:
            return exact[path]
        for directory, category in (
            (self._paths.raw_root / "manifests", StartupFileCategory.RAW_MANIFEST),
            (self._paths.paper_root / "manifests", StartupFileCategory.PAPER_MANIFEST),
            (self._paths.paper_root / "checkpoints", StartupFileCategory.PAPER_CHECKPOINT),
            (
                self._paths.raw_root / "segments",
                StartupFileCategory.RAW_HISTORICAL_SEGMENT,
            ),
            (
                self._paths.paper_root / "segments",
                StartupFileCategory.PAPER_HISTORICAL_SEGMENT,
            ),
        ):
            try:
                relative = path.relative_to(directory)
            except ValueError:
                continue
            if len(relative.parts) != 1:
                raise StartupTraceError("startup access used a nested authority artifact path")
            return category
        raise StartupTraceError(
            "startup requested an unclassified file under candidate_root: "
            f"{path.relative_to(self._paths.candidate_root).as_posix()}"
        )

    def _prepare_observation(
        self,
        value: object,
        *,
        api: StartupOpenApi,
        access: str,
    ) -> _ObservedOpen | None:
        path = self._absolute_candidate_path(value)
        if path is None:
            return None
        category = self._category(path)
        if category in {
            StartupFileCategory.RAW_HISTORICAL_SEGMENT,
            StartupFileCategory.PAPER_HISTORICAL_SEGMENT,
        }:
            relative = path.relative_to(self._paths.candidate_root).as_posix()
            raise StartupTraceError(
                f"normal startup attempted to open historical segment path: {relative}"
            )
        return _ObservedOpen(path=path, category=category, api=api, access=access)

    def _record(self, observed: _ObservedOpen | None) -> None:
        if observed is not None:
            self._observed.append(observed)

    def install(self) -> None:
        if self._observing:
            raise StartupTraceError("startup trace hooks are already installed")
        original_os_open = self._original_os_open
        original_path_open = self._original_path_open
        original_sqlite_connect = self._original_sqlite_connect

        def traced_os_open(
            path: object,
            flags: int,
            mode: int = 0o777,
            *,
            dir_fd: int | None = None,
        ) -> int:
            if dir_fd is not None and not isinstance(path, int):
                candidate = self._absolute_candidate_path(path)
                if candidate is not None:
                    raise StartupTraceError(
                        "dir_fd-relative startup access cannot be attributed safely"
                    )
            observed = self._prepare_observation(
                path,
                api=StartupOpenApi.OS_OPEN,
                access=f"flags={flags}",
            )
            descriptor = cast(Any, original_os_open)(
                path,
                flags,
                mode,
                dir_fd=dir_fd,
            )
            self._record(observed)
            return cast(int, descriptor)

        def traced_path_open(
            path: Path,
            mode: str = "r",
            buffering: int = -1,
            encoding: str | None = None,
            errors: str | None = None,
            newline: str | None = None,
        ) -> IO[Any]:
            observed = self._prepare_observation(
                path,
                api=StartupOpenApi.PATH_OPEN,
                access=mode,
            )
            stream = cast(Any, original_path_open)(
                path,
                mode,
                buffering,
                encoding,
                errors,
                newline,
            )
            self._record(observed)
            return cast(IO[Any], stream)

        def traced_sqlite_connect(
            database: object,
            *args: object,
            **kwargs: object,
        ) -> sqlite3.Connection:
            sqlite_path = _sqlite_path(database)
            observed = (
                None
                if sqlite_path is None
                else self._prepare_observation(
                    sqlite_path,
                    api=StartupOpenApi.SQLITE_CONNECT,
                    access="sqlite_connect",
                )
            )
            connection = cast(Any, original_sqlite_connect)(database, *args, **kwargs)
            self._record(observed)
            return cast(sqlite3.Connection, connection)

        self._observing = True
        try:
            setattr(os, "open", traced_os_open)  # noqa: B010 - bounded instrumentation
            setattr(Path, "open", traced_path_open)  # noqa: B010 - bounded instrumentation
            setattr(  # noqa: B010 - bounded instrumentation
                sqlite3, "connect", traced_sqlite_connect
            )
        except BaseException:
            self.restore()
            raise

    def restore(self) -> None:
        if not self._observing:
            return
        setattr(  # noqa: B010 - restore exact original
            sqlite3, "connect", self._original_sqlite_connect
        )
        setattr(Path, "open", self._original_path_open)  # noqa: B010 - restore
        setattr(os, "open", self._original_os_open)  # noqa: B010 - restore
        self._observing = False

    def stop_observing(self) -> None:
        """Restore all hooks while retaining observations for later hashing."""

        if not self._observing:
            raise StartupTraceError("startup trace hooks are not active")
        self.restore()

    def finalize(self) -> None:
        if not self._observed:
            raise StartupTraceError("startup trace observed no candidate file opens")
        opens: list[StartupFileOpen] = []
        for sequence, observed in enumerate(self._observed, start=1):
            path = self._absolute_candidate_path(observed.path)
            if path is None or not path.is_file() or _is_link_or_reparse_point(path):
                raise StartupTraceError("observed startup artifact is no longer a regular file")
            before = os.stat(path, follow_symlinks=False)
            if not stat.S_ISREG(before.st_mode):
                raise StartupTraceError("observed startup artifact is not a regular file")
            digest = hashlib.sha256()
            size = 0
            with self._original_path_open(path, "rb") as stream:
                opened = os.fstat(stream.fileno())
                if not stat.S_ISREG(opened.st_mode) or not os.path.samestat(
                    before, opened
                ):
                    raise StartupTraceError(
                        "observed startup artifact changed before post-scope hashing"
                    )
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
            after = os.stat(path, follow_symlinks=False)
            before_signature = (
                int(before.st_size),
                int(before.st_mtime_ns),
                int(before.st_ctime_ns),
            )
            after_signature = (
                int(after.st_size),
                int(after.st_mtime_ns),
                int(after.st_ctime_ns),
            )
            opened_content_signature = (
                int(opened.st_size),
                int(opened.st_mtime_ns),
            )
            named_content_signature = (
                int(before.st_size),
                int(before.st_mtime_ns),
            )
            if (
                _is_link_or_reparse_point(path)
                or not stat.S_ISREG(after.st_mode)
                or not os.path.samestat(opened, after)
                or before_signature != after_signature
                or opened_content_signature != named_content_signature
                or size != int(opened.st_size)
            ):
                raise StartupTraceError(
                    "observed startup artifact changed during post-scope hashing"
                )
            opens.append(
                StartupFileOpen(
                    sequence=sequence,
                    relative_path=path.relative_to(
                        self._paths.candidate_root
                    ).as_posix(),
                    category=observed.category,
                    api=observed.api,
                    access=observed.access,
                    size_bytes_after_scope=size,
                    sha256_after_scope=digest.hexdigest(),
                )
            )
        self._result = StartupFileAccessTrace(
            candidate_root=self._paths.candidate_root,
            opens=tuple(opens),
        )


_TRACE_LOCK = threading.Lock()


@contextmanager
def trace_startup_file_access(
    paths: StartupTracePaths,
) -> Iterator[_StartupTraceRecorder]:
    """Observe one non-concurrent startup scope and always restore all hooks."""

    if not isinstance(paths, StartupTracePaths):
        raise TypeError("paths must be StartupTracePaths")
    if not _TRACE_LOCK.acquire(blocking=False):
        raise StartupTraceError("another process-local startup trace is already active")
    recorder = _StartupTraceRecorder(paths)
    try:
        recorder.install()
        try:
            yield recorder
        finally:
            recorder.restore()
        recorder.finalize()
    finally:
        recorder.restore()
        _TRACE_LOCK.release()


__all__ = [
    "STARTUP_TRACE_STATUS",
    "StartupFileAccessTrace",
    "StartupFileCategory",
    "StartupFileOpen",
    "StartupOpenApi",
    "StartupTraceError",
    "StartupTracePaths",
    "trace_startup_file_access",
]
