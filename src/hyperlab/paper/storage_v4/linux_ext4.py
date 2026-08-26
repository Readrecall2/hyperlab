"""Linux/ext4 filesystem identity and fail-closed path attestations.

This module does not infer a filesystem from a path name.  It parses the
kernel's mountinfo view, selects the longest matching mount point, and records
the exact mount and superblock options used by the certification process.
Secure-tree attestations never follow links and reject multiply linked regular
files, ownership drift, permissive modes, transient publication sidecars, and
non-regular entries.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath


class FilesystemAttestationErrorCode(StrEnum):
    MOUNTINFO_INVALID = "MOUNTINFO_INVALID"
    MOUNT_NOT_FOUND = "MOUNT_NOT_FOUND"
    NOT_EXT4 = "NOT_EXT4"
    UNSAFE_MOUNT_OPTIONS = "UNSAFE_MOUNT_OPTIONS"
    PATH_UNSAFE = "PATH_UNSAFE"
    OWNER_MISMATCH = "OWNER_MISMATCH"
    PERMISSION_MISMATCH = "PERMISSION_MISMATCH"
    HARDLINK_REFUSED = "HARDLINK_REFUSED"
    TRANSIENT_ENTRY = "TRANSIENT_ENTRY"


class FilesystemAttestationError(RuntimeError):
    """Stable fail-closed filesystem or path-policy rejection."""

    def __init__(
        self,
        code: FilesystemAttestationErrorCode,
        message: str,
    ) -> None:
        if type(code) is not FilesystemAttestationErrorCode:
            raise TypeError("filesystem attestation error code is invalid")
        self.code = code
        super().__init__(f"{code.value}: {message}")


def _error(
    code: FilesystemAttestationErrorCode,
    message: str,
) -> FilesystemAttestationError:
    return FilesystemAttestationError(code, message)


def _decode_mountinfo_field(value: str) -> str:
    output: list[str] = []
    index = 0
    while index < len(value):
        if (
            value[index] == "\\"
            and index + 3 < len(value)
            and all(character in "01234567" for character in value[index + 1 : index + 4])
        ):
            output.append(chr(int(value[index + 1 : index + 4], 8)))
            index += 4
        else:
            output.append(value[index])
            index += 1
    return "".join(output)


def _options(value: str) -> tuple[str, ...]:
    return tuple(sorted(option for option in value.split(",") if option))


@dataclass(frozen=True, slots=True)
class MountInfoEntry:
    mount_id: int
    parent_id: int
    major_minor: str
    root: PurePosixPath
    mount_point: PurePosixPath
    mount_options: tuple[str, ...]
    optional_fields: tuple[str, ...]
    filesystem_type: str
    mount_source: str
    super_options: tuple[str, ...]

    def payload(self) -> dict[str, object]:
        return {
            "filesystem_type": self.filesystem_type,
            "major_minor": self.major_minor,
            "mount_id": self.mount_id,
            "mount_options": list(self.mount_options),
            "mount_point": str(self.mount_point),
            "mount_source": self.mount_source,
            "optional_fields": list(self.optional_fields),
            "parent_id": self.parent_id,
            "root": str(self.root),
            "super_options": list(self.super_options),
        }


def parse_mountinfo(text: str) -> tuple[MountInfoEntry, ...]:
    """Parse an exact ``/proc/*/mountinfo`` snapshot without guessing fields."""

    if type(text) is not str:
        raise TypeError("mountinfo text must be exact text")
    entries: list[MountInfoEntry] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line:
            continue
        before, separator, after = line.partition(" - ")
        left = before.split()
        right = after.split()
        if not separator or len(left) < 6 or len(right) != 3:
            raise _error(
                FilesystemAttestationErrorCode.MOUNTINFO_INVALID,
                f"mountinfo line {line_number} has an invalid field layout",
            )
        try:
            mount_id = int(left[0])
            parent_id = int(left[1])
        except ValueError as error:
            raise _error(
                FilesystemAttestationErrorCode.MOUNTINFO_INVALID,
                f"mountinfo line {line_number} has invalid numeric identities",
            ) from error
        root = PurePosixPath(_decode_mountinfo_field(left[3]))
        mount_point = PurePosixPath(_decode_mountinfo_field(left[4]))
        if not root.is_absolute() or not mount_point.is_absolute():
            raise _error(
                FilesystemAttestationErrorCode.MOUNTINFO_INVALID,
                f"mountinfo line {line_number} has a relative root or mount point",
            )
        entries.append(
            MountInfoEntry(
                mount_id=mount_id,
                parent_id=parent_id,
                major_minor=left[2],
                root=root,
                mount_point=mount_point,
                mount_options=_options(left[5]),
                optional_fields=tuple(left[6:]),
                filesystem_type=right[0],
                mount_source=_decode_mountinfo_field(right[1]),
                super_options=_options(right[2]),
            )
        )
    if not entries:
        raise _error(
            FilesystemAttestationErrorCode.MOUNTINFO_INVALID,
            "mountinfo snapshot is empty",
        )
    return tuple(entries)


def read_mountinfo(path: Path = Path("/proc/self/mountinfo")) -> tuple[MountInfoEntry, ...]:
    if not isinstance(path, Path):
        raise TypeError("mountinfo path must be pathlib.Path")
    try:
        text = path.read_text(encoding="utf-8", errors="strict")
    except (OSError, UnicodeError) as error:
        raise _error(
            FilesystemAttestationErrorCode.MOUNTINFO_INVALID,
            "kernel mountinfo could not be read exactly",
        ) from error
    return parse_mountinfo(text)


def detect_mount(path: Path | PurePosixPath, entries: Iterable[MountInfoEntry]) -> MountInfoEntry:
    """Select the longest mount point containing one absolute candidate path."""

    if not isinstance(path, (Path, PurePosixPath)):
        raise TypeError("mount detection path must be a pathlib path")
    linux_path = PurePosixPath(path.as_posix())
    if not linux_path.is_absolute():
        raise TypeError("mount detection path must be an absolute Linux path")
    selected: MountInfoEntry | None = None
    selected_depth = -1
    for entry in entries:
        if not isinstance(entry, MountInfoEntry):
            raise TypeError("mount entries must contain MountInfoEntry values")
        try:
            linux_path.relative_to(entry.mount_point)
        except ValueError:
            continue
        depth = len(entry.mount_point.parts)
        if depth > selected_depth:
            selected = entry
            selected_depth = depth
    if selected is None:
        raise _error(
            FilesystemAttestationErrorCode.MOUNT_NOT_FOUND,
            f"no mountinfo entry contains {linux_path}",
        )
    return selected


_UNSAFE_EXT4_OPTIONS = frozenset(
    {
        "barrier=0",
        "data=writeback",
        "dax",
        "dax=always",
        "journal_async_commit",
        "nobarrier",
        "ro",
    }
)


def require_ext4_mount(entry: MountInfoEntry) -> MountInfoEntry:
    """Require an ext4 read/write mount without known durability weakenings."""

    if not isinstance(entry, MountInfoEntry):
        raise TypeError("ext4 gate requires MountInfoEntry")
    if entry.filesystem_type != "ext4":
        raise _error(
            FilesystemAttestationErrorCode.NOT_EXT4,
            f"observed filesystem is {entry.filesystem_type!r}, not ext4",
        )
    combined = set(entry.mount_options) | set(entry.super_options)
    unsafe = tuple(sorted(combined & _UNSAFE_EXT4_OPTIONS))
    if "rw" not in entry.mount_options or unsafe:
        detail = ",".join(unsafe) if unsafe else "missing rw"
        raise _error(
            FilesystemAttestationErrorCode.UNSAFE_MOUNT_OPTIONS,
            f"ext4 mount options are not admitted: {detail}",
        )
    return entry


def _is_reparse(observed: os.stat_result) -> bool:
    attributes = int(getattr(observed, "st_file_attributes", 0))
    mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & mask)


def _lstat(path: Path) -> os.stat_result:
    try:
        return os.lstat(path)
    except OSError as error:
        raise _error(
            FilesystemAttestationErrorCode.PATH_UNSAFE,
            f"path cannot be inspected without following links: {path}",
        ) from error


def _require_direct_path(path: Path, *, directory: bool) -> os.stat_result:
    observed = _lstat(path)
    expected = stat.S_ISDIR(observed.st_mode) if directory else stat.S_ISREG(observed.st_mode)
    if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed) or not expected:
        raise _error(
            FilesystemAttestationErrorCode.PATH_UNSAFE,
            f"path is not a direct {'directory' if directory else 'regular file'}: {path}",
        )
    return observed


def _require_direct_ancestry(path: Path) -> None:
    cursor = path.absolute()
    while True:
        if cursor.exists():
            _require_direct_path(cursor, directory=True)
        if cursor.parent == cursor:
            return
        cursor = cursor.parent


def secure_mkdir(path: Path) -> Path:
    """Create one fresh private directory and verify its direct identity."""

    if not isinstance(path, Path):
        raise TypeError("secure directory path must be pathlib.Path")
    selected = path.absolute()
    if selected.exists() or selected.is_symlink():
        raise FileExistsError(f"secure directory already exists: {selected}")
    _require_direct_ancestry(selected.parent)
    selected.mkdir(mode=0o700)
    try:
        if os.name != "nt":
            selected.chmod(0o700)
        observed = _require_direct_path(selected, directory=True)
        if os.name != "nt" and stat.S_IMODE(observed.st_mode) != 0o700:
            raise _error(
                FilesystemAttestationErrorCode.PERMISSION_MISMATCH,
                f"secure directory mode is not 0700: {selected}",
            )
    except BaseException:
        selected.rmdir()
        raise
    return selected


@dataclass(frozen=True, slots=True)
class PathEntryAttestation:
    relative_path: str
    kind: str
    device: int
    inode: int
    uid: int
    gid: int
    mode: int
    link_count: int
    size_bytes: int

    def payload(self) -> dict[str, object]:
        return {
            "device": self.device,
            "gid": self.gid,
            "inode": self.inode,
            "kind": self.kind,
            "link_count": self.link_count,
            "mode_octal": f"{self.mode:04o}",
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
            "uid": self.uid,
        }


@dataclass(frozen=True, slots=True)
class SecureTreeAttestation:
    root: Path
    entries: tuple[PathEntryAttestation, ...]
    directory_count: int
    file_count: int
    hardlink_count: int
    total_bytes: int

    def payload(self) -> dict[str, object]:
        return {
            "directories": self.directory_count,
            "entries": [entry.payload() for entry in self.entries],
            "files": self.file_count,
            "hardlinks": self.hardlink_count,
            "root": str(self.root),
            "total_bytes": self.total_bytes,
        }


def _attest_entry(
    path: Path,
    *,
    root: Path,
    expected_uid: int,
    expected_gid: int,
    directory: bool,
) -> PathEntryAttestation:
    observed = _require_direct_path(path, directory=directory)
    if int(observed.st_uid) != expected_uid or int(observed.st_gid) != expected_gid:
        raise _error(
            FilesystemAttestationErrorCode.OWNER_MISMATCH,
            f"path ownership differs from the certification identity: {path}",
        )
    mode = stat.S_IMODE(observed.st_mode)
    expected_mode = 0o700 if directory else 0o600
    if mode != expected_mode:
        raise _error(
            FilesystemAttestationErrorCode.PERMISSION_MISMATCH,
            f"path mode {mode:04o} differs from {expected_mode:04o}: {path}",
        )
    link_count = int(observed.st_nlink)
    if not directory and link_count != 1:
        raise _error(
            FilesystemAttestationErrorCode.HARDLINK_REFUSED,
            f"regular file has {link_count} hardlinks: {path}",
        )
    relative = "." if path == root else path.relative_to(root).as_posix()
    return PathEntryAttestation(
        relative_path=relative,
        kind="directory" if directory else "regular_file",
        device=int(observed.st_dev),
        inode=int(observed.st_ino),
        uid=int(observed.st_uid),
        gid=int(observed.st_gid),
        mode=mode,
        link_count=link_count,
        size_bytes=0 if directory else int(observed.st_size),
    )


def attest_secure_tree(
    root: Path,
    *,
    expected_uid: int,
    expected_gid: int,
) -> SecureTreeAttestation:
    """Attest one private tree without following links or accepting hardlinks."""

    if not isinstance(root, Path) or not root.is_absolute():
        raise TypeError("secure tree root must be an absolute pathlib.Path")
    for label, value in (("uid", expected_uid), ("gid", expected_gid)):
        if type(value) is not int or value < 0:
            raise TypeError(f"expected {label} must be a non-negative exact integer")
    selected = root.absolute()
    _require_direct_ancestry(selected)
    entries: list[PathEntryAttestation] = [
        _attest_entry(
            selected,
            root=selected,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            directory=True,
        )
    ]
    transient_suffixes = (".tmp", "-journal", "-shm", "-wal")
    try:
        for directory, names, filenames in os.walk(selected, topdown=True, followlinks=False):
            base = Path(directory)
            names.sort()
            filenames.sort()
            for name in names:
                path = base / name
                entries.append(
                    _attest_entry(
                        path,
                        root=selected,
                        expected_uid=expected_uid,
                        expected_gid=expected_gid,
                        directory=True,
                    )
                )
            for name in filenames:
                path = base / name
                if name == "PENDING" or name.endswith(transient_suffixes):
                    raise _error(
                        FilesystemAttestationErrorCode.TRANSIENT_ENTRY,
                        f"tree contains an incomplete publication sidecar: {path}",
                    )
                entries.append(
                    _attest_entry(
                        path,
                        root=selected,
                        expected_uid=expected_uid,
                        expected_gid=expected_gid,
                        directory=False,
                    )
                )
    except FilesystemAttestationError:
        raise
    except OSError as error:
        raise _error(
            FilesystemAttestationErrorCode.PATH_UNSAFE,
            "secure tree enumeration failed",
        ) from error
    entries.sort(key=lambda item: (item.relative_path != ".", item.relative_path))
    directories = sum(entry.kind == "directory" for entry in entries)
    files = sum(entry.kind == "regular_file" for entry in entries)
    return SecureTreeAttestation(
        root=selected,
        entries=tuple(entries),
        directory_count=directories,
        file_count=files,
        hardlink_count=0,
        total_bytes=sum(entry.size_bytes for entry in entries),
    )


__all__ = [
    "FilesystemAttestationError",
    "FilesystemAttestationErrorCode",
    "MountInfoEntry",
    "PathEntryAttestation",
    "SecureTreeAttestation",
    "attest_secure_tree",
    "detect_mount",
    "parse_mountinfo",
    "read_mountinfo",
    "require_ext4_mount",
    "secure_mkdir",
]
