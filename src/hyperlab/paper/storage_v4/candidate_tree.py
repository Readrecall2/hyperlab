"""Canonical immutable-tree witnesses for offline Phase 1C candidates.

The witness hashes the same regular-file descriptors that it validates and
never follows links or reparse points.  A second enumeration and hash pass
closes additions, removals, replacements, and byte drift during collection.
"""

from __future__ import annotations

import hashlib
import math
import os
import stat
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .canonical import canonical_json_bytes

CANDIDATE_TREE_HEARTBEAT_MIN_SECONDS = 30.0
CANDIDATE_TREE_HEARTBEAT_MAX_SECONDS = 60.0

ProgressCallback = Callable[[Mapping[str, object]], None]


class CandidateTreeWitnessError(RuntimeError):
    """A candidate tree is unsafe, transient, or changed while witnessed."""


def _require_sha256(value: str, *, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def _safe_relative_path(value: str, *, label: str) -> str:
    pure = PurePosixPath(value)
    if (
        type(value) is not str
        or not value
        or "\\" in value
        or pure.is_absolute()
        or any(part in {"", ".", ".."} for part in pure.parts)
        or pure.as_posix() != value
    ):
        raise ValueError(f"{label} must be a canonical relative POSIX path")
    return value


def _is_reparse(value: os.stat_result) -> bool:
    attributes = int(getattr(value, "st_file_attributes", 0))
    mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & mask)


def _is_link_or_reparse(path: Path) -> bool:
    try:
        observed = os.lstat(path)
    except OSError:
        return True
    return stat.S_ISLNK(observed.st_mode) or _is_reparse(observed)


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (
        int(value.st_dev),
        int(value.st_ino),
        int(value.st_size),
        int(value.st_mtime_ns),
    )


@dataclass(frozen=True, slots=True)
class CandidateFileWitness:
    relative_path: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        _safe_relative_path(self.relative_path, label="candidate file path")
        if type(self.size_bytes) is not int or self.size_bytes < 0:
            raise ValueError("candidate file size must be a non-negative exact integer")
        _require_sha256(self.sha256, label="candidate file SHA-256")

    def payload(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class CandidateTreeWitness:
    root: Path
    files: tuple[CandidateFileWitness, ...]
    directories: tuple[str, ...]
    directory_count: int
    total_bytes: int
    tree_sha256: str

    def __post_init__(self) -> None:
        if not self.root.is_absolute() or not self.root.is_dir():
            raise ValueError("candidate tree root must be an existing absolute directory")
        file_paths = tuple(item.relative_path for item in self.files)
        if not self.files or file_paths != tuple(sorted(set(file_paths))):
            raise ValueError("candidate tree files must be non-empty, unique, and sorted")
        if self.directories != tuple(sorted(set(self.directories))):
            raise ValueError("candidate tree directories must be unique and sorted")
        for directory in self.directories:
            _safe_relative_path(directory, label="candidate directory path")
        if self.directory_count != len(self.directories) + 1:
            raise ValueError("candidate tree directory count differs from its manifest")
        if self.total_bytes != sum(item.size_bytes for item in self.files):
            raise ValueError("candidate tree total differs from its files")
        _require_sha256(self.tree_sha256, label="candidate tree SHA-256")
        if self.tree_sha256 != hashlib.sha256(
            canonical_json_bytes(self.payload_without_sha256())
        ).hexdigest():
            raise ValueError("candidate tree SHA-256 differs from its manifest")

    def payload_without_sha256(self) -> dict[str, object]:
        return {
            "directories": list(self.directories),
            "directory_count": self.directory_count,
            "file_count": len(self.files),
            "files": [item.payload() for item in self.files],
            "root": str(self.root),
            "total_bytes": self.total_bytes,
        }

    def payload(self) -> dict[str, object]:
        return {**self.payload_without_sha256(), "tree_sha256": self.tree_sha256}


def _validate_root(root: Path) -> None:
    if not isinstance(root, Path) or not root.is_absolute():
        raise ValueError("candidate root must be an absolute pathlib.Path")
    try:
        resolved = root.resolve(strict=True)
    except OSError as error:
        raise CandidateTreeWitnessError("candidate root is missing") from error
    if resolved != root or not root.is_dir():
        raise CandidateTreeWitnessError(
            "candidate root is not a direct regular directory"
        )
    cursor = root
    while True:
        if _is_link_or_reparse(cursor):
            raise CandidateTreeWitnessError(
                f"candidate ancestry contains a link/reparse point: {cursor}"
            )
        if cursor.parent == cursor:
            break
        cursor = cursor.parent


def _enumerate_tree(root: Path) -> tuple[tuple[str, ...], tuple[Path, ...]]:
    directories: list[str] = []
    files: list[Path] = []
    try:
        walker = os.walk(root, topdown=True, followlinks=False)
        for directory, names, filenames in walker:
            base = Path(directory)
            if _is_link_or_reparse(base):
                raise CandidateTreeWitnessError(
                    f"candidate tree contains a link/reparse point: {base}"
                )
            names.sort()
            filenames.sort()
            for name in names:
                candidate = base / name
                observed = os.lstat(candidate)
                if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
                    raise CandidateTreeWitnessError(
                        f"candidate tree contains a link/reparse point: {candidate}"
                    )
                if not stat.S_ISDIR(observed.st_mode):
                    raise CandidateTreeWitnessError(
                        f"candidate tree contains a non-directory entry: {candidate}"
                    )
                directories.append(candidate.relative_to(root).as_posix())
            for name in filenames:
                candidate = base / name
                observed = os.lstat(candidate)
                if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
                    raise CandidateTreeWitnessError(
                        f"candidate tree contains a link/reparse point: {candidate}"
                    )
                if not stat.S_ISREG(observed.st_mode):
                    raise CandidateTreeWitnessError(
                        f"candidate tree contains a non-regular entry: {candidate}"
                    )
                if name == "PENDING" or name.endswith(
                    (".tmp", "-journal", "-shm", "-wal")
                ):
                    raise CandidateTreeWitnessError(
                        f"candidate tree contains a transient sidecar: {candidate}"
                    )
                files.append(candidate)
    except CandidateTreeWitnessError:
        raise
    except OSError as error:
        raise CandidateTreeWitnessError("candidate tree enumeration failed") from error
    directories.sort()
    files.sort(key=lambda item: item.relative_to(root).as_posix())
    return tuple(directories), tuple(files)


def _hash_regular_file(path: Path) -> tuple[int, str]:
    try:
        before = os.lstat(path)
        if not stat.S_ISREG(before.st_mode) or _is_reparse(before):
            raise CandidateTreeWitnessError(f"candidate contains an unsafe file: {path}")
        flags = os.O_RDONLY
        for name in ("O_BINARY", "O_NOINHERIT", "O_NOFOLLOW"):
            flags |= int(getattr(os, name, 0))
        descriptor = os.open(path, flags)
    except CandidateTreeWitnessError:
        raise
    except OSError as error:
        raise CandidateTreeWitnessError(f"candidate file open failed: {path}") from error
    digest = hashlib.sha256()
    try:
        opened_before = os.fstat(descriptor)
        if not stat.S_ISREG(opened_before.st_mode) or _is_reparse(opened_before):
            raise CandidateTreeWitnessError(
                f"candidate descriptor is not a regular file: {path}"
            )
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        opened_after = os.fstat(descriptor)
    except OSError as error:
        raise CandidateTreeWitnessError(f"candidate file hash failed: {path}") from error
    finally:
        os.close(descriptor)
    try:
        after = os.lstat(path)
    except OSError as error:
        raise CandidateTreeWitnessError(f"candidate file disappeared: {path}") from error
    if (
        not stat.S_ISREG(after.st_mode)
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
        raise CandidateTreeWitnessError(f"candidate file changed while hashed: {path}")
    return int(before.st_size), digest.hexdigest()


def witness_candidate_tree(
    root: Path,
    *,
    progress: ProgressCallback | None = None,
    heartbeat_interval_seconds: float = CANDIDATE_TREE_HEARTBEAT_MIN_SECONDS,
) -> CandidateTreeWitness:
    """Hash one complete immutable candidate without following links."""

    _validate_root(root)
    if progress is not None and not callable(progress):
        raise TypeError("candidate hash progress callback must be callable or None")
    if (
        type(heartbeat_interval_seconds) not in (int, float)
        or not math.isfinite(float(heartbeat_interval_seconds))
        or not CANDIDATE_TREE_HEARTBEAT_MIN_SECONDS
        <= float(heartbeat_interval_seconds)
        <= CANDIDATE_TREE_HEARTBEAT_MAX_SECONDS
    ):
        raise ValueError("candidate hash heartbeat must be between 30 and 60 seconds")

    directories, paths = _enumerate_tree(root)
    if not paths:
        raise CandidateTreeWitnessError("candidate tree is empty")
    files: list[CandidateFileWitness] = []
    total_bytes = 0
    last_heartbeat = time.monotonic()
    for path in paths:
        size, digest = _hash_regular_file(path)
        total_bytes += size
        files.append(
            CandidateFileWitness(
                relative_path=path.relative_to(root).as_posix(),
                size_bytes=size,
                sha256=digest,
            )
        )
        now = time.monotonic()
        if now - last_heartbeat >= float(heartbeat_interval_seconds):
            if progress is not None:
                progress(
                    {
                        "bytes_hashed": total_bytes,
                        "candidate_root": str(root),
                        "files_completed": len(files),
                        "files_total": len(paths),
                        "phase": "phase1c_candidate_tree_hash",
                        "status": "RUNNING",
                    }
                )
            last_heartbeat = now

    verification_directories, verification_paths = _enumerate_tree(root)
    if (
        verification_directories != directories
        or tuple(path.relative_to(root).as_posix() for path in verification_paths)
        != tuple(item.relative_path for item in files)
    ):
        raise CandidateTreeWitnessError("candidate tree changed during enumeration")
    for path, witnessed in zip(verification_paths, files, strict=True):
        size, digest = _hash_regular_file(path)
        if size != witnessed.size_bytes or digest != witnessed.sha256:
            raise CandidateTreeWitnessError(
                f"candidate tree changed after its first hash pass: {path}"
            )
    material = {
        "directories": list(directories),
        "directory_count": len(directories) + 1,
        "file_count": len(files),
        "files": [item.payload() for item in files],
        "root": str(root),
        "total_bytes": total_bytes,
    }
    return CandidateTreeWitness(
        root=root,
        files=tuple(files),
        directories=directories,
        directory_count=len(directories) + 1,
        total_bytes=total_bytes,
        tree_sha256=hashlib.sha256(canonical_json_bytes(material)).hexdigest(),
    )


def compose_candidate_tree_witness(
    root: Path,
    children: Sequence[CandidateTreeWitness],
) -> CandidateTreeWitness:
    """Compose one parent witness from exact direct-child tree witnesses."""

    _validate_root(root)
    if not children:
        raise ValueError("candidate tree composition requires at least one child")
    files: list[CandidateFileWitness] = []
    directories: list[str] = []
    child_names: set[str] = set()
    for child in children:
        if not isinstance(child, CandidateTreeWitness):
            raise TypeError("candidate tree child must be CandidateTreeWitness")
        if child.root.parent != root:
            raise ValueError("candidate tree child must be a direct child of its parent")
        prefix = child.root.name
        _safe_relative_path(prefix, label="candidate tree child prefix")
        if prefix in child_names:
            raise ValueError("candidate tree child prefixes must be unique")
        child_names.add(prefix)
        directories.append(prefix)
        directories.extend(f"{prefix}/{directory}" for directory in child.directories)
        files.extend(
            CandidateFileWitness(
                relative_path=f"{prefix}/{item.relative_path}",
                size_bytes=item.size_bytes,
                sha256=item.sha256,
            )
            for item in child.files
        )
    files.sort(key=lambda item: item.relative_path)
    directories.sort()
    total_bytes = sum(item.size_bytes for item in files)
    material = {
        "directories": directories,
        "directory_count": len(directories) + 1,
        "file_count": len(files),
        "files": [item.payload() for item in files],
        "root": str(root),
        "total_bytes": total_bytes,
    }
    return CandidateTreeWitness(
        root=root,
        files=tuple(files),
        directories=tuple(directories),
        directory_count=len(directories) + 1,
        total_bytes=total_bytes,
        tree_sha256=hashlib.sha256(canonical_json_bytes(material)).hexdigest(),
    )


__all__ = [
    "CANDIDATE_TREE_HEARTBEAT_MAX_SECONDS",
    "CANDIDATE_TREE_HEARTBEAT_MIN_SECONDS",
    "CandidateFileWitness",
    "CandidateTreeWitness",
    "CandidateTreeWitnessError",
    "ProgressCallback",
    "compose_candidate_tree_witness",
    "witness_candidate_tree",
]
