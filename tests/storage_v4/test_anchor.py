from __future__ import annotations

import multiprocessing
import sqlite3
from multiprocessing.queues import Queue
from pathlib import Path

import pytest

import hyperlab.paper.storage_v4.anchor as anchor_module
from hyperlab.paper.storage_v4.anchor import (
    AnchorError,
    AnchorErrorCode,
    AnchorRecord,
    LocalAnchor,
)
from hyperlab.paper.storage_v4.faults import (
    DeterministicFaultInjector,
    FaultPoint,
    InjectedCrash,
)
from hyperlab.paper.storage_v4.types import Hash32, StoreId

SYNTHETIC_STORAGE_V4_WORKLOAD = True
_STORE = StoreId("SYNTHETIC_STORAGE_V4_WORKLOAD/anchor-store")


def _record(generation: int, marker: int, *, store: StoreId = _STORE) -> AnchorRecord:
    return AnchorRecord(store, generation, Hash32(bytes([marker]) * 32))


def _child_acquire_anchor_writer(
    anchor_path: str,
    store_id: str,
    results: Queue[str],
) -> None:
    anchor = LocalAnchor.open_existing(Path(anchor_path), store_id=StoreId(store_id))
    try:
        lease = anchor.acquire_writer_lease()
    except AnchorError as error:
        results.put(error.code.value)
        return
    except BaseException as error:
        results.put(f"UNEXPECTED:{type(error).__name__}:{error}")
        return
    lease.close()
    results.put("ACQUIRED")


@pytest.fixture(autouse=True)
def _fast_directory_barrier(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(anchor_module, "fsync_directory", lambda _path: None)


def test_local_anchor_requires_explicit_create_or_open(tmp_path: Path) -> None:
    path = tmp_path / "anchor.sqlite3"
    with pytest.raises(AnchorError) as missing:
        LocalAnchor.open_existing(path, store_id=_STORE)
    assert missing.value.code is AnchorErrorCode.MISSING

    created = LocalAnchor.create(path, store_id=_STORE)
    assert created.read() is None
    with pytest.raises(AnchorError) as duplicate:
        LocalAnchor.create(path, store_id=_STORE)
    assert duplicate.value.code is AnchorErrorCode.ALREADY_EXISTS

    reopened = LocalAnchor.open_existing(path, store_id=_STORE)
    assert reopened.read() is None
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)
        assert connection.execute("PRAGMA synchronous").fetchone() == (2,)


def _symlink_or_skip(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symbolic links are unavailable: {error}")


def test_anchor_rejects_symlink_open_create_and_late_substitution(
    tmp_path: Path,
) -> None:
    alternate_path = tmp_path / "alternate.sqlite3"
    LocalAnchor.create(alternate_path, store_id=_STORE)
    link = tmp_path / "anchor-link.sqlite3"
    _symlink_or_skip(link, alternate_path)

    with pytest.raises(AnchorError) as created:
        LocalAnchor.create(link, store_id=_STORE)
    assert created.value.code is AnchorErrorCode.ALREADY_EXISTS
    with pytest.raises(AnchorError) as opened:
        LocalAnchor.open_existing(link, store_id=_STORE)
    assert opened.value.code is AnchorErrorCode.CORRUPT

    primary_path = tmp_path / "primary.sqlite3"
    primary = LocalAnchor.create(primary_path, store_id=_STORE)
    primary_path.unlink()
    _symlink_or_skip(primary_path, alternate_path)
    with pytest.raises(AnchorError) as substituted:
        primary.read()
    assert substituted.value.code is AnchorErrorCode.CORRUPT


def test_anchor_compare_and_swap_is_monotone_exact_and_idempotent(tmp_path: Path) -> None:
    anchor = LocalAnchor.create(tmp_path / "anchor.sqlite3", store_id=_STORE)
    first = _record(1, 1)
    later = _record(5, 5)

    assert anchor.compare_and_swap(None, first) == first
    assert anchor.compare_and_swap(first, first) == first
    assert anchor.reattest(first) == first
    assert anchor.compare_and_swap(first, later) == later
    assert anchor.read() == later

    reopened = LocalAnchor.open_existing(anchor.path, store_id=_STORE)
    assert reopened.read() == later
    assert reopened.compare_and_swap(later, later) == later


def test_anchor_writer_lease_blocks_sessions_and_processes_until_close(
    tmp_path: Path,
) -> None:
    anchor = LocalAnchor.create(tmp_path / "anchor.sqlite3", store_id=_STORE)
    session = LocalAnchor.open_existing(anchor.path, store_id=_STORE)
    lease = anchor.acquire_writer_lease()
    assert not lease.closed

    with pytest.raises(AnchorError) as same_process:
        session.acquire_writer_lease()
    assert same_process.value.code is AnchorErrorCode.WRITER_LEASE_HELD

    # The sidecar lock must not overlap SQLite's own locking bytes.
    first = _record(1, 1)
    assert session.read() is None
    assert session.compare_and_swap(None, first) == first
    assert anchor.read() == first

    context = multiprocessing.get_context("spawn")
    results: Queue[str] = context.Queue()
    process = context.Process(
        target=_child_acquire_anchor_writer,
        args=(str(anchor.path), _STORE.value, results),
    )
    observed: str | None = None
    try:
        process.start()
        process.join(timeout=20.0)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5.0)
            pytest.fail("child process did not return from non-blocking anchor lease")
        assert process.exitcode == 0
        observed = results.get(timeout=5.0)
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5.0)
        results.close()
        results.join_thread()
    assert observed == AnchorErrorCode.WRITER_LEASE_HELD.value

    lease.close()
    lease.close()
    assert lease.closed
    following = session.acquire_writer_lease()
    following.close()


def test_anchor_writer_leases_are_independent_for_distinct_anchors(
    tmp_path: Path,
) -> None:
    first = LocalAnchor.create(tmp_path / "first.sqlite3", store_id=_STORE)
    second = LocalAnchor.create(tmp_path / "second.sqlite3", store_id=_STORE)

    assert first.writer_lease_path != second.writer_lease_path
    with (
        first.acquire_writer_lease() as first_lease,
        second.acquire_writer_lease() as second_lease,
    ):
        assert not first_lease.closed
        assert not second_lease.closed
    assert first_lease.closed
    assert second_lease.closed


def test_anchor_writer_lease_rejects_symlink(
    tmp_path: Path,
) -> None:
    anchor = LocalAnchor.create(tmp_path / "anchor.sqlite3", store_id=_STORE)
    lease_path = anchor.writer_lease_path
    alternate = tmp_path / "alternate.lease"
    alternate.write_bytes(b"not-anchor-writer-authority")
    _symlink_or_skip(lease_path, alternate)

    with pytest.raises(AnchorError) as linked:
        anchor.acquire_writer_lease()
    assert linked.value.code is AnchorErrorCode.WRITER_LEASE_FAILED


def test_anchor_and_writer_lease_refuse_a_hardlinked_anchor(tmp_path: Path) -> None:
    anchor = LocalAnchor.create(tmp_path / "anchor.sqlite3", store_id=_STORE)
    sibling = tmp_path / "anchor-hardlink.sqlite3"
    sibling.hardlink_to(anchor.path)

    with pytest.raises(AnchorError) as reopened:
        LocalAnchor.open_existing(anchor.path, store_id=_STORE)
    assert reopened.value.code is AnchorErrorCode.CORRUPT

    with pytest.raises(AnchorError) as leased:
        anchor.acquire_writer_lease()
    assert leased.value.code is AnchorErrorCode.CORRUPT

    sibling.unlink()
    assert LocalAnchor.open_existing(anchor.path, store_id=_STORE).read() is None


def test_anchor_writer_lease_rejects_corrupt_identity(tmp_path: Path) -> None:
    anchor = LocalAnchor.create(tmp_path / "anchor.sqlite3", store_id=_STORE)
    lease_path = anchor.writer_lease_path
    lease_path.write_bytes(b"wrong-anchor-writer-identity")
    with pytest.raises(AnchorError) as corrupt:
        anchor.acquire_writer_lease()
    assert corrupt.value.code is AnchorErrorCode.WRITER_LEASE_FAILED


def test_anchor_writer_lease_rejects_windows_reparse_points_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    anchor = LocalAnchor.create(tmp_path / "anchor.sqlite3", store_id=_STORE)
    lease_path = anchor.writer_lease_path
    original = anchor_module._is_link_or_reparse_point

    def simulated_reparse(path: Path) -> bool:
        return path == lease_path or original(path)

    monkeypatch.setattr(
        anchor_module,
        "_is_link_or_reparse_point",
        simulated_reparse,
    )
    with pytest.raises(AnchorError) as reparse:
        anchor.acquire_writer_lease()
    assert reparse.value.code is AnchorErrorCode.WRITER_LEASE_FAILED


def test_anchor_rejects_expected_mismatch_rollback_and_same_generation_fork(
    tmp_path: Path,
) -> None:
    anchor = LocalAnchor.create(tmp_path / "anchor.sqlite3", store_id=_STORE)
    current = _record(4, 4)
    anchor.compare_and_swap(None, current)

    with pytest.raises(AnchorError) as mismatch:
        anchor.compare_and_swap(None, _record(5, 5))
    assert mismatch.value.code is AnchorErrorCode.EXPECTED_MISMATCH

    with pytest.raises(AnchorError) as rollback:
        anchor.compare_and_swap(current, _record(3, 3))
    assert rollback.value.code is AnchorErrorCode.ROLLBACK

    with pytest.raises(AnchorError) as fork:
        anchor.compare_and_swap(current, _record(4, 9))
    assert fork.value.code is AnchorErrorCode.FORK
    assert anchor.read() == current


def test_anchor_rejects_wrong_store_on_open_cas_and_reattest(tmp_path: Path) -> None:
    path = tmp_path / "anchor.sqlite3"
    anchor = LocalAnchor.create(path, store_id=_STORE)
    other = StoreId("SYNTHETIC_STORAGE_V4_WORKLOAD/other-store")

    with pytest.raises(AnchorError) as opened:
        LocalAnchor.open_existing(path, store_id=other)
    assert opened.value.code is AnchorErrorCode.STORE_MISMATCH
    with pytest.raises(AnchorError) as cas:
        anchor.compare_and_swap(None, _record(1, 1, store=other))
    assert cas.value.code is AnchorErrorCode.STORE_MISMATCH
    with pytest.raises(AnchorError) as reattest:
        anchor.reattest(_record(1, 1, store=other))
    assert reattest.value.code is AnchorErrorCode.STORE_MISMATCH


def test_anchor_fault_before_commit_rolls_back_and_after_commit_is_durable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "anchor.sqlite3"
    LocalAnchor.create(path, store_id=_STORE)
    first = _record(1, 1)

    before = LocalAnchor.open_existing(
        path,
        store_id=_STORE,
        fault_hook=DeterministicFaultInjector(FaultPoint.BEFORE_ANCHOR_PUBLICATION),
    )
    with pytest.raises(InjectedCrash):
        before.compare_and_swap(None, first)
    assert LocalAnchor.open_existing(path, store_id=_STORE).read() is None

    after = LocalAnchor.open_existing(
        path,
        store_id=_STORE,
        fault_hook=DeterministicFaultInjector(FaultPoint.AFTER_ANCHOR_PUBLICATION),
    )
    with pytest.raises(InjectedCrash):
        after.compare_and_swap(None, first)
    assert LocalAnchor.open_existing(path, store_id=_STORE).read() == first
    assert LocalAnchor.open_existing(path, store_id=_STORE).compare_and_swap(first, first) == first


def test_anchor_corruption_and_schema_tampering_fail_closed(tmp_path: Path) -> None:
    corrupt = tmp_path / "corrupt.sqlite3"
    corrupt.write_bytes(b"not a sqlite anchor")
    with pytest.raises(AnchorError) as unreadable:
        LocalAnchor.open_existing(corrupt, store_id=_STORE)
    assert unreadable.value.code is AnchorErrorCode.CORRUPT

    path = tmp_path / "tampered.sqlite3"
    LocalAnchor.create(path, store_id=_STORE)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA application_id = 0")
    with pytest.raises(AnchorError) as tampered:
        LocalAnchor.open_existing(path, store_id=_STORE)
    assert tampered.value.code is AnchorErrorCode.CORRUPT


def test_anchor_record_supports_full_uint64_without_sqlite_integer_coercion(
    tmp_path: Path,
) -> None:
    anchor = LocalAnchor.create(tmp_path / "anchor.sqlite3", store_id=_STORE)
    maximum = _record((1 << 64) - 1, 7)

    anchor.compare_and_swap(None, maximum)

    assert LocalAnchor.open_existing(anchor.path, store_id=_STORE).read() == maximum
    with pytest.raises((TypeError, ValueError)):
        AnchorRecord(_STORE, 0, Hash32(bytes(32)))
