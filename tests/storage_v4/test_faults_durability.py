from __future__ import annotations

import os
from pathlib import Path

import pytest

import hyperlab.paper.storage_v4.durability as durability_module
from hyperlab.paper.storage_v4.durability import (
    ImmutableTargetConflict,
    PublishDisposition,
    atomic_write_mutable_cache,
    durable_publish_immutable,
    fsync_directory,
)
from hyperlab.paper.storage_v4.faults import (
    DeterministicFaultInjector,
    FaultPoint,
    InjectedCrash,
    trigger_fault,
)

SYNTHETIC_STORAGE_V4_WORKLOAD = True


def test_fault_points_cover_every_phase_1b_and_phase_1c_publication_boundary() -> None:
    expected = {
        f"{side}_{boundary}"
        for side in ("before", "after")
        for boundary in (
            "temp_write",
            "flush",
            "file_fsync",
            "rename",
            "exclusive_publish",
            "directory_fsync",
            "segment_publication",
            "checkpoint_publication",
            "manifest_publication",
            "current_publication",
            "anchor_publication",
            "overlay_transaction",
            "raw_segment_publication",
            "raw_manifest_publication",
            "raw_anchor_publication",
        )
    }
    expected.update(
        {
            "before_raw_segment_copy",
            "after_raw_segment_copy",
            "after_raw_before_paper_append",
            "before_overlay_commit",
        }
    )

    assert {point.value for point in FaultPoint} == expected


def test_deterministic_fault_injector_raises_once_at_selected_occurrence() -> None:
    injector = DeterministicFaultInjector(FaultPoint.BEFORE_FLUSH, occurrence=2)

    trigger_fault(injector, FaultPoint.AFTER_FLUSH)
    trigger_fault(injector, FaultPoint.BEFORE_FLUSH)
    with pytest.raises(InjectedCrash) as caught:
        trigger_fault(injector, FaultPoint.BEFORE_FLUSH)
    trigger_fault(injector, FaultPoint.BEFORE_FLUSH)

    assert caught.value.point is FaultPoint.BEFORE_FLUSH
    assert caught.value.occurrence == 2
    assert injector.seen == 2
    assert injector.triggered is True
    injector.reset()
    assert injector.seen == 0
    assert injector.triggered is False


def test_immutable_publication_is_verified_exclusive_and_idempotent(
    tmp_path: Path,
) -> None:
    target = tmp_path / "artifact.hl4"
    verified: list[bytes] = []

    first = durable_publish_immutable(target, b"authenticated", verifier=verified.append)
    second = durable_publish_immutable(target, b"authenticated", verifier=verified.append)

    assert first.disposition is PublishDisposition.CREATED
    assert second.disposition is PublishDisposition.ALREADY_PRESENT
    assert target.read_bytes() == b"authenticated"
    assert verified == [b"authenticated", b"authenticated"]
    assert not tuple(tmp_path.glob(".artifact.hl4.*.tmp"))

    with pytest.raises(ImmutableTargetConflict, match="divergent"):
        durable_publish_immutable(target, b"different")
    assert target.read_bytes() == b"authenticated"


@pytest.mark.parametrize(
    ("point", "published"),
    [
        (FaultPoint.BEFORE_TEMP_WRITE, False),
        (FaultPoint.AFTER_TEMP_WRITE, False),
        (FaultPoint.BEFORE_FLUSH, False),
        (FaultPoint.AFTER_FLUSH, False),
        (FaultPoint.BEFORE_FILE_FSYNC, False),
        (FaultPoint.AFTER_FILE_FSYNC, False),
        (FaultPoint.BEFORE_RENAME, False),
        (FaultPoint.BEFORE_EXCLUSIVE_PUBLISH, False),
        (FaultPoint.AFTER_EXCLUSIVE_PUBLISH, True),
        (FaultPoint.AFTER_RENAME, True),
        (FaultPoint.BEFORE_DIRECTORY_FSYNC, True),
        (FaultPoint.AFTER_DIRECTORY_FSYNC, True),
    ],
)
def test_injected_crash_leaves_safe_orphan_and_never_divergent_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    point: FaultPoint,
    published: bool,
) -> None:
    monkeypatch.setattr(durability_module, "fsync_directory", lambda _path: None)
    target = tmp_path / "crash.hl4"

    with pytest.raises(InjectedCrash) as caught:
        durable_publish_immutable(
            target,
            b"complete-bytes",
            fault_hook=DeterministicFaultInjector(point),
        )

    assert caught.value.point is point
    assert target.exists() is published
    if published:
        assert target.read_bytes() == b"complete-bytes"
        assert target.stat().st_nlink == 1
    orphans = tuple(tmp_path.glob(".crash.hl4.*.tmp"))
    assert len(orphans) == (0 if published else 1)

    recovered = durable_publish_immutable(target, b"complete-bytes")
    assert recovered.disposition is (
        PublishDisposition.ALREADY_PRESENT if published else PublishDisposition.CREATED
    )
    assert target.read_bytes() == b"complete-bytes"


def test_immutable_publication_refuses_an_existing_hardlinked_target(
    tmp_path: Path,
) -> None:
    authority = tmp_path / "authority.hl4"
    authority.write_bytes(b"authenticated")
    target = tmp_path / "artifact.hl4"
    os.link(authority, target)

    with pytest.raises(ImmutableTargetConflict, match="hardlink"):
        durable_publish_immutable(target, b"authenticated")

    assert authority.read_bytes() == b"authenticated"
    assert target.stat().st_nlink == 2


@pytest.mark.parametrize(
    ("point", "expected"),
    [
        (FaultPoint.BEFORE_RENAME, b"old"),
        (FaultPoint.AFTER_RENAME, b"new"),
        (FaultPoint.BEFORE_DIRECTORY_FSYNC, b"new"),
        (FaultPoint.AFTER_DIRECTORY_FSYNC, b"new"),
    ],
)
def test_mutable_cache_crash_boundary_is_separate_from_immutable_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    point: FaultPoint,
    expected: bytes,
) -> None:
    monkeypatch.setattr(durability_module, "fsync_directory", lambda _path: None)
    target = tmp_path / "CURRENT"
    target.write_bytes(b"old")

    with pytest.raises(InjectedCrash):
        atomic_write_mutable_cache(
            target,
            b"new",
            fault_hook=DeterministicFaultInjector(point),
        )

    assert target.read_bytes() == expected


def test_publication_orders_write_flush_file_sync_publish_and_directory_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    monkeypatch.setattr(
        durability_module,
        "fsync_directory",
        lambda _path: events.append("directory_call"),
    )

    def observe(point: FaultPoint) -> None:
        events.append(point.value)

    durable_publish_immutable(tmp_path / "ordered", b"value", fault_hook=observe)

    assert events == [
        "before_temp_write",
        "after_temp_write",
        "before_flush",
        "after_flush",
        "before_file_fsync",
        "after_file_fsync",
        "before_rename",
        "before_exclusive_publish",
        "after_exclusive_publish",
        "after_rename",
        "before_directory_fsync",
        "directory_call",
        "after_directory_fsync",
    ]


def test_directory_fsync_posix_uses_directory_descriptor_and_closes_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, object]] = []
    descriptor = 12345
    monkeypatch.setattr(durability_module.os, "name", "posix")
    monkeypatch.setattr(
        durability_module.os,
        "open",
        lambda path, flags: events.append(("open", (path, flags))) or descriptor,
    )
    monkeypatch.setattr(
        durability_module.os,
        "fsync",
        lambda value: events.append(("fsync", value)),
    )
    monkeypatch.setattr(
        durability_module.os,
        "close",
        lambda value: events.append(("close", value)),
    )

    fsync_directory(tmp_path)

    assert [name for name, _value in events] == ["open", "fsync", "close"]
    opened_path, opened_flags = events[0][1]
    assert opened_path == tmp_path
    assert opened_flags == os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0))


def test_verifier_failure_removes_non_authoritative_temporary(
    tmp_path: Path,
) -> None:
    target = tmp_path / "rejected"

    def reject(_data: bytes) -> None:
        raise ValueError("synthetic verifier refusal")

    with pytest.raises(ValueError, match="verifier refusal"):
        durable_publish_immutable(target, b"bad", verifier=reject)

    assert not target.exists()
    assert not tuple(tmp_path.iterdir())
