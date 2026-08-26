from __future__ import annotations

import os
from pathlib import Path, PurePosixPath

import pytest

from hyperlab.paper.storage_v4.linux_ext4 import (
    FilesystemAttestationError,
    FilesystemAttestationErrorCode,
    attest_secure_tree,
    detect_mount,
    parse_mountinfo,
    require_ext4_mount,
    secure_mkdir,
)


def _mountinfo(*lines: str) -> str:
    return "\n".join(lines) + "\n"


def test_mountinfo_parser_decodes_paths_and_selects_longest_mount() -> None:
    entries = parse_mountinfo(
        _mountinfo(
            "24 1 8:1 / / rw,relatime - ext4 /dev/vda1 rw,data=ordered",
            "25 24 8:2 / /srv/hyperlab\\040offline rw,nosuid,nodev - ext4 /dev/vdb1 rw,data=ordered",
        )
    )

    selected = detect_mount(PurePosixPath("/srv/hyperlab offline/phase1d/candidate"), entries)

    assert selected.mount_point == PurePosixPath("/srv/hyperlab offline")
    assert selected.filesystem_type == "ext4"
    assert selected.mount_options == ("nodev", "nosuid", "rw")
    assert selected.super_options == ("data=ordered", "rw")


@pytest.mark.parametrize(
    ("filesystem", "super_options", "code"),
    [
        ("xfs", "rw", FilesystemAttestationErrorCode.NOT_EXT4),
        ("ext4", "rw,data=writeback", FilesystemAttestationErrorCode.UNSAFE_MOUNT_OPTIONS),
        ("ext4", "rw,nobarrier", FilesystemAttestationErrorCode.UNSAFE_MOUNT_OPTIONS),
        ("ext4", "rw,dax", FilesystemAttestationErrorCode.UNSAFE_MOUNT_OPTIONS),
    ],
)
def test_ext4_gate_rejects_wrong_or_unsafe_filesystems(
    filesystem: str,
    super_options: str,
    code: FilesystemAttestationErrorCode,
) -> None:
    entry = parse_mountinfo(_mountinfo(f"24 1 8:1 / / rw,relatime - {filesystem} /dev/vda1 {super_options}"))[
        0
    ]

    with pytest.raises(FilesystemAttestationError) as caught:
        require_ext4_mount(entry)

    assert caught.value.code is code


@pytest.mark.skipif(os.name == "nt", reason="POSIX ownership and mode semantics required")
def test_secure_tree_attests_exact_private_modes_and_single_links(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    secure_mkdir(root)
    nested = root / "segments"
    secure_mkdir(nested)
    artifact = nested / "one.hl4"
    artifact.write_bytes(b"one")
    artifact.chmod(0o600)
    root_stat = os.lstat(root)

    report = attest_secure_tree(
        root,
        expected_uid=int(root_stat.st_uid),
        expected_gid=int(root_stat.st_gid),
    )

    assert report.root == root.absolute()
    assert report.directory_count == 2
    assert report.file_count == 1
    assert report.hardlink_count == 0
    assert {entry.mode for entry in report.entries} == {0o600, 0o700}


@pytest.mark.skipif(os.name == "nt", reason="POSIX ownership and mode semantics required")
def test_secure_tree_rejects_hardlinks_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    secure_mkdir(root)
    first = root / "first"
    first.write_bytes(b"same inode")
    first.chmod(0o600)
    os.link(first, root / "second")
    root_stat = os.lstat(root)

    with pytest.raises(FilesystemAttestationError) as caught:
        attest_secure_tree(
            root,
            expected_uid=int(root_stat.st_uid),
            expected_gid=int(root_stat.st_gid),
        )

    assert caught.value.code is FilesystemAttestationErrorCode.HARDLINK_REFUSED


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink semantics required")
def test_secure_tree_rejects_symlinks_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    secure_mkdir(root)
    target = root / "target"
    target.write_bytes(b"target")
    target.chmod(0o600)
    (root / "linked").symlink_to(target)
    root_stat = os.lstat(root)

    with pytest.raises(FilesystemAttestationError) as caught:
        attest_secure_tree(
            root,
            expected_uid=int(root_stat.st_uid),
            expected_gid=int(root_stat.st_gid),
        )

    assert caught.value.code is FilesystemAttestationErrorCode.PATH_UNSAFE


@pytest.mark.skipif(os.name == "nt", reason="POSIX ownership and mode semantics required")
def test_secure_tree_rejects_group_or_world_permissions(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    secure_mkdir(root)
    artifact = root / "unsafe"
    artifact.write_bytes(b"unsafe")
    artifact.chmod(0o640)
    root_stat = os.lstat(root)

    with pytest.raises(FilesystemAttestationError) as caught:
        attest_secure_tree(
            root,
            expected_uid=int(root_stat.st_uid),
            expected_gid=int(root_stat.st_gid),
        )

    assert caught.value.code is FilesystemAttestationErrorCode.PERMISSION_MISMATCH
