from __future__ import annotations

import multiprocessing
from dataclasses import replace
from multiprocessing.queues import Queue
from pathlib import Path
from typing import Any

import pytest

import hyperlab.paper.storage_v4.repository as repository_module
from hyperlab.paper.storage_v4.anchor import AnchorRecord, LocalAnchor
from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.checkpoint import (
    CheckpointState,
    checkpoint_state_sha256,
)
from hyperlab.paper.storage_v4.contracts import StorageMode
from hyperlab.paper.storage_v4.faults import (
    DeterministicFaultInjector,
    FaultPoint,
    InjectedCrash,
)
from hyperlab.paper.storage_v4.manifest import (
    OpaqueIdentity,
    manifest_to_bytes,
)
from hyperlab.paper.storage_v4.overlay import (
    OverlayError,
    OverlayErrorCode,
    SQLiteOverlay,
)
from hyperlab.paper.storage_v4.repository import (
    AuditIntegrityStatus,
    CurrentCacheStatus,
    RepositoryConfig,
    RepositoryError,
    RepositoryErrorCode,
    SealResult,
    StartupIntegrityStatus,
    StorageRepository,
)
from hyperlab.paper.storage_v4.segment import CodecProfile
from hyperlab.paper.storage_v4.types import (
    CommitFrame,
    CommitOrdinal,
    CommitSequence,
    Hash32,
    LogicalRow,
    RunId,
    StoreId,
    StreamId,
)

SYNTHETIC_STORAGE_V4_WORKLOAD = True
_STORE = StoreId("SYNTHETIC_STORAGE_V4_WORKLOAD/repository-store")
_RUN = RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/repository-run")
_ZERO = Hash32(b"\x00" * 32)
_EVENTS = StreamId("events")


def _hash(marker: int) -> Hash32:
    return Hash32(bytes([marker]) * 32)


def _config() -> RepositoryConfig:
    return RepositoryConfig(
        store_id=_STORE,
        run_id=_RUN,
        mode=StorageMode.V4_NATIVE,
        run_identity=OpaqueIdentity(_hash(1)),
        config_identity=OpaqueIdentity(_hash(2)),
        code_identity=OpaqueIdentity(_hash(3)),
        runtime_identity=OpaqueIdentity(_hash(4)),
        start_prefix_root=_ZERO,
    )


def _child_open_writer(
    root: str,
    anchor_path: str,
    results: Queue[str],
) -> None:
    anchor = LocalAnchor.open_existing(Path(anchor_path), store_id=_STORE)
    try:
        repository = StorageRepository.open_existing(
            Path(root),
            anchor=anchor,
            config=_config(),
        )
    except RepositoryError as error:
        results.put(error.code.value)
        return
    except BaseException as error:
        results.put(f"UNEXPECTED:{type(error).__name__}:{error}")
        return
    repository.close()
    results.put("OPENED")


def _state(marker: int) -> CheckpointState:
    return CheckpointState(
        adapter={"marker": marker},
        ledger={"cash": str(100 - marker)},
        projection={"revision": marker},
        sessions={"closed": []},
        incidents={"open": []},
        cursors={"commit": marker},
        stream_heads={"events": marker},
    )


def _frame(sequence: int, previous: Hash32) -> CommitFrame:
    return CommitFrame(
        run_id=_RUN,
        commit_sequence=CommitSequence(sequence),
        previous_prefix_root=previous,
        rows=(
            LogicalRow(
                stream_id=_EVENTS,
                ordinal=CommitOrdinal(0),
                value={"marker": sequence, "synthetic": True},
            ),
        ),
    )


def _new_repository(
    tmp_path: Path,
) -> tuple[StorageRepository, LocalAnchor, RepositoryConfig]:
    config = _config()
    anchor = LocalAnchor.create(tmp_path / "anchor.sqlite3", store_id=_STORE)
    repository = StorageRepository.create(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    return repository, anchor, config


def _directory_symlink_or_skip(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("local Windows policy does not permit an unprivileged symlink")


def _append(repository: StorageRepository, count: int) -> tuple[CommitFrame, ...]:
    previous = repository.overlay_state.head_prefix_root
    first = int(repository.overlay_state.head_commit_sequence) + 1
    frames: list[CommitFrame] = []
    for sequence in range(first, first + count):
        frame = _frame(sequence, previous)
        assert repository.append(frame)
        frames.append(frame)
        previous = build_commit_logical(frame).prefix_root
    return tuple(frames)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("run_id", RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/other-run")),
        ("mode", StorageMode.V3_COMPATIBILITY_IMPORT),
        ("run_identity", OpaqueIdentity(_hash(21))),
        ("config_identity", OpaqueIdentity(_hash(22))),
        ("code_identity", OpaqueIdentity(_hash(23))),
        ("runtime_identity", OpaqueIdentity(_hash(24))),
        ("codec_profile", CodecProfile.zlib(level=1)),
        ("start_prefix_root", _hash(25)),
        (
            "thresholds",
            repository_module.OverlayThresholds(seal_rows=7, seal_bytes=11),
        ),
        ("genesis_base_commit_sequence", CommitSequence(3)),
    ],
)
def test_genesis_tail_rejects_repository_identity_drift_before_first_seal(
    tmp_path: Path,
    field_name: str,
    replacement: object,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    _append(repository, 1)
    repository.close()
    drifted = replace(config, **{field_name: replacement})

    with pytest.raises(OverlayError) as failure:
        StorageRepository.open_existing(
            tmp_path / "repository",
            anchor=anchor,
            config=drifted,
        )
    expected_code = (
        OverlayErrorCode.WRONG_RUN
        if field_name == "run_id"
        else OverlayErrorCode.EXPECTED_STATE_MISMATCH
    )
    assert failure.value.code == expected_code


def test_anchor_scoped_writer_lease_excludes_a_second_repository_root(
    tmp_path: Path,
) -> None:
    config = _config()
    anchor = LocalAnchor.create(tmp_path / "anchor.sqlite3", store_id=_STORE)
    first = StorageRepository.create(
        tmp_path / "repository-a",
        anchor=anchor,
        config=config,
    )

    with pytest.raises(RepositoryError) as failure:
        StorageRepository.create(
            tmp_path / "repository-b",
            anchor=anchor,
            config=config,
        )
    assert failure.value.code == RepositoryErrorCode.WRITER_LEASE_HELD
    assert not (tmp_path / "repository-b").exists()

    first.close()
    second = StorageRepository.create(
        tmp_path / "repository-b",
        anchor=anchor,
        config=config,
    )
    second.close()


def _seal(
    repository: StorageRepository,
    *,
    historical_count: int,
) -> SealResult:
    return repository.seal(
        checkpoint_state=_state(historical_count),
        cumulative_stream_counts=((_EVENTS, historical_count),),
        historical_commit_count=historical_count,
    )


def test_happy_path_seals_reopens_recovers_and_audits_without_false_genesis_commit(
    tmp_path: Path,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    genesis = repository.startup_report
    assert genesis.integrity_status == StartupIntegrityStatus.GENESIS_OVERLAY_ONLY
    assert genesis.manifest_generation == 0
    assert genesis.checkpoint_used is False
    assert genesis.tail_entries_replayed == 0
    assert genesis.segments_read == 0

    frames = _append(repository, 2)
    first_seal = _seal(repository, historical_count=2)
    assert first_seal.manifest.generation == 1
    assert first_seal.segment.commit_count == 2
    assert first_seal.segment_path.is_file()
    assert first_seal.checkpoint_path.is_file()
    assert first_seal.manifest_path.is_file()
    assert repository.overlay_state.tail_commit_count == 0
    assert anchor.read() == AnchorRecord(
        store_id=_STORE,
        generation=1,
        manifest_root=first_seal.manifest.identity.root,
    )

    startup = repository.startup_report
    assert startup.integrity_result == "AUTHENTICATED_CHECKPOINT_PLUS_TAIL"
    assert startup.checkpoint_used
    assert startup.historical_segments_not_read == 1
    assert startup.historical_commits_not_read == 2
    assert startup.historical_rows_not_read == 2
    assert startup.segments_read == 0
    assert startup.tail_frames == ()
    repository.close()

    reopened = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    assert reopened.startup_report.current_cache_status == CurrentCacheStatus.EXACT
    assert reopened.startup_report.segments_read == 0
    assert tuple(reopened.iter_historical_frames()) == frames
    audit = reopened.full_audit()
    assert audit.integrity_status == AuditIntegrityStatus.FULL_HISTORY_AUTHENTICATED
    assert audit.manifests_read == 1
    assert audit.checkpoints_read == 1
    assert audit.segments_read == 1
    assert len(audit.checkpoint_state_witnesses) == 1
    witness = audit.checkpoint_state_witnesses[0]
    assert int(witness.covered_commit_sequence) == 2
    assert witness.state_sha256 == checkpoint_state_sha256(_state(2))
    assert audit.commits_read == 2
    assert audit.rows_read == 2
    assert audit.physical_segment_bytes == first_seal.segment.physical_size

    third = _append(reopened, 1)[0]
    second_seal = _seal(reopened, historical_count=3)
    assert second_seal.manifest.generation == 2
    assert second_seal.manifest.parent_manifest_root == first_seal.manifest.identity.root
    assert tuple(reopened.iter_historical_frames()) == (*frames, third)
    assert reopened.full_audit().manifest_generation == 2
    reopened.close()


def test_overlay_state_polling_is_constant_time_until_seal_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, _anchor, _config = _new_repository(tmp_path)
    original = SQLiteOverlay._validated_commits
    validations = 0

    def counted(overlay: SQLiteOverlay, meta: Any) -> Any:
        nonlocal validations
        validations += 1
        return original(overlay, meta)

    monkeypatch.setattr(SQLiteOverlay, "_validated_commits", counted)
    previous = repository.overlay_state.head_prefix_root
    for sequence in range(1, 33):
        frame = _frame(sequence, previous)
        assert repository.append(frame)
        state = repository.overlay_state
        assert state.head_commit_sequence == CommitSequence(sequence)
        previous = build_commit_logical(frame).prefix_root

    assert validations == 0
    _seal(repository, historical_count=32)
    assert validations > 0
    repository.close()


def test_writer_lease_blocks_another_process_and_is_released_on_close(
    tmp_path: Path,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    context = multiprocessing.get_context("spawn")
    results: Queue[str] = context.Queue()
    process = context.Process(
        target=_child_open_writer,
        args=(str(repository.paths.root), str(anchor.path), results),
    )
    observed: str | None = None
    try:
        process.start()
        process.join(timeout=20.0)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5.0)
            pytest.fail("child process did not return from non-blocking lease attempt")
        assert process.exitcode == 0
        observed = results.get(timeout=5.0)
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5.0)
        results.close()
        results.join_thread()

    assert observed == RepositoryErrorCode.WRITER_LEASE_HELD.value
    _append(repository, 1)
    repository.close()

    reopened = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    assert reopened.overlay_state.tail_commit_count == 1
    reopened.close()


def test_failed_open_releases_writer_lease(tmp_path: Path) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    repository.close()
    wrong_run = replace(
        config,
        run_id=RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/wrong-run"),
    )

    with pytest.raises(OverlayError) as failure:
        StorageRepository.open_existing(
            tmp_path / "repository",
            anchor=anchor,
            config=wrong_run,
        )
    assert failure.value.code == OverlayErrorCode.WRONG_RUN

    reopened = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    reopened.close()


@pytest.mark.parametrize(
    "point",
    [
        FaultPoint.BEFORE_SEGMENT_PUBLICATION,
        FaultPoint.AFTER_SEGMENT_PUBLICATION,
        FaultPoint.BEFORE_CHECKPOINT_PUBLICATION,
        FaultPoint.AFTER_CHECKPOINT_PUBLICATION,
        FaultPoint.BEFORE_MANIFEST_PUBLICATION,
        FaultPoint.AFTER_MANIFEST_PUBLICATION,
        FaultPoint.BEFORE_ANCHOR_PUBLICATION,
        FaultPoint.AFTER_ANCHOR_PUBLICATION,
        FaultPoint.BEFORE_CURRENT_PUBLICATION,
        FaultPoint.AFTER_CURRENT_PUBLICATION,
        FaultPoint.BEFORE_OVERLAY_TRANSACTION,
        FaultPoint.BEFORE_OVERLAY_COMMIT,
        FaultPoint.AFTER_OVERLAY_TRANSACTION,
    ],
)
def test_fault_matrix_recovers_idempotently_at_every_repository_boundary(
    tmp_path: Path,
    point: FaultPoint,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    frame = _append(repository, 1)[0]
    injector = DeterministicFaultInjector(point)
    repository.set_fault_hook(injector)

    with pytest.raises(InjectedCrash) as interrupted:
        _seal(repository, historical_count=1)
    assert interrupted.value.point == point
    repository.close()

    recovered = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    if anchor.read() is None:
        report = recovered.startup_report
        assert report.integrity_status == StartupIntegrityStatus.GENESIS_OVERLAY_ONLY
        assert report.tail_frames == (frame,)
        _seal(recovered, historical_count=1)
    else:
        report = recovered.startup_report
        assert report.integrity_status == (
            StartupIntegrityStatus.AUTHENTICATED_CHECKPOINT_PLUS_TAIL
        )
        assert report.tail_frames == ()
        recovered_orphan = point in {
            FaultPoint.AFTER_MANIFEST_PUBLICATION,
            FaultPoint.BEFORE_ANCHOR_PUBLICATION,
        }
        assert report.segments_read == int(recovered_orphan)
        assert report.historical_commits_not_read == int(not recovered_orphan)
    assert tuple(recovered.iter_historical_frames()) == (frame,)
    assert recovered.full_audit().commits_read == 1
    recovered.close()
    repeated = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    repeated_report = repeated.startup_report
    assert repeated_report.manifest_generation == 1
    assert repeated_report.tail_frames == ()
    assert repeated_report.segments_read == 0
    repeated.close()


def test_manifest_publication_crash_is_adopted_before_append_and_reseal(
    tmp_path: Path,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    first = _append(repository, 1)[0]
    repository.set_fault_hook(
        DeterministicFaultInjector(FaultPoint.AFTER_MANIFEST_PUBLICATION)
    )
    with pytest.raises(InjectedCrash):
        _seal(repository, historical_count=1)
    repository.close()
    assert anchor.read() is None

    recovered = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    recovered_report = recovered.startup()
    assert recovered_report.manifest_generation == 1
    assert recovered_report.tail_frames == ()
    assert recovered_report.segments_read == 1
    assert recovered_report.historical_segments_not_read == 0
    assert recovered_report.historical_commits_not_read == 0
    assert recovered_report.historical_rows_not_read == 0
    second = _append(recovered, 1)[0]
    result = _seal(recovered, historical_count=2)
    assert result.manifest.generation == 2
    assert tuple(recovered.iter_historical_frames()) == (first, second)
    audit = recovered.full_audit()
    assert audit.commits_read == 2
    assert audit.manifests_read == 2
    assert audit.checkpoints_read == 2
    recovered.close()


def test_full_audit_refuses_a_hardlinked_paper_segment(tmp_path: Path) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    _append(repository, 1)
    _seal(repository, historical_count=1)
    repository.close()
    segment = next((tmp_path / "repository" / "segments").glob("*.hl4s"))
    sibling = tmp_path / "paper-segment-hardlink.hl4s"
    sibling.hardlink_to(segment)

    reopened = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    try:
        with pytest.raises(RepositoryError) as caught:
            reopened.full_audit()
        assert caught.value.code is RepositoryErrorCode.SEGMENT_MISSING
    finally:
        reopened.close()
        sibling.unlink()

    final = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    assert final.full_audit().commits_read == 1
    final.close()


def test_orphan_successor_adoption_preserves_an_existing_overlay_suffix(
    tmp_path: Path,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    first = _append(repository, 1)[0]
    repository.set_fault_hook(
        DeterministicFaultInjector(FaultPoint.AFTER_MANIFEST_PUBLICATION)
    )
    with pytest.raises(InjectedCrash):
        _seal(repository, historical_count=1)
    repository.set_fault_hook(None)
    second = _append(repository, 1)[0]
    repository.close()
    assert anchor.read() is None

    recovered = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    assert recovered.startup_report.manifest_generation == 1
    assert recovered.startup_report.tail_frames == (second,)
    _seal(recovered, historical_count=2)
    assert tuple(recovered.iter_historical_frames()) == (first, second)
    recovered.close()


def test_open_existing_rejects_a_repository_root_symlink(tmp_path: Path) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    repository.close()
    root_link = tmp_path / "repository-link"
    _directory_symlink_or_skip(root_link, tmp_path / "repository")

    with pytest.raises(RepositoryError) as caught:
        StorageRepository.open_existing(root_link, anchor=anchor, config=config)
    assert caught.value.code == RepositoryErrorCode.MISSING


def test_windows_reparse_attribute_is_treated_as_a_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ReparseStat:
        st_file_attributes = int(
            getattr(repository_module.stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        )

    monkeypatch.setattr(Path, "is_symlink", lambda _: False)
    monkeypatch.setattr(repository_module.os, "name", "nt")
    monkeypatch.setattr(repository_module.os, "lstat", lambda _: ReparseStat())

    assert repository_module._is_link_or_reparse_point(tmp_path)


@pytest.mark.parametrize(
    ("variant", "expected_status"),
    [
        ("absent", CurrentCacheStatus.ABSENT_REPAIRED),
        ("stale", CurrentCacheStatus.STALE_REPAIRED),
        ("corrupt", CurrentCacheStatus.CORRUPT_REPAIRED),
        ("dangling_symlink", CurrentCacheStatus.CORRUPT_REPAIRED),
    ],
)
def test_current_is_only_a_repairable_cache_and_symlinks_are_never_followed(
    tmp_path: Path,
    variant: str,
    expected_status: CurrentCacheStatus,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    _append(repository, 1)
    _seal(repository, historical_count=1)
    stale_bytes = repository.paths.current.read_bytes()
    _append(repository, 1)
    _seal(repository, historical_count=2)
    authoritative_bytes = repository.paths.current.read_bytes()
    current = repository.paths.current
    repository.close()

    if variant == "absent":
        current.unlink()
    elif variant == "stale":
        current.write_bytes(stale_bytes)
    elif variant == "corrupt":
        current.write_bytes(b"not-current")
    else:
        current.unlink()
        try:
            current.symlink_to(tmp_path / "must-not-be-read")
        except OSError:
            pytest.skip("local Windows policy does not permit an unprivileged symlink")

    reopened = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    assert reopened.startup_report.current_cache_status == expected_status
    assert current.read_bytes() == authoritative_bytes
    assert not current.is_symlink()
    reopened.close()


def test_normal_startup_never_calls_historical_segment_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    _append(repository, 2)
    _seal(repository, historical_count=2)
    repository.close()

    def forbidden(_: bytes, **__: object) -> object:
        raise AssertionError("normal startup opened a historical segment")

    def forbidden_chain(_: object, __: object) -> object:
        raise AssertionError("normal startup loaded the historical manifest chain")

    monkeypatch.setattr(repository_module, "read_segment", forbidden)
    monkeypatch.setattr(StorageRepository, "_load_manifest_chain", forbidden_chain)
    reopened = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    report = reopened.startup_report
    assert report.segments_read == 0
    assert report.historical_segments_not_read == 1
    assert report.historical_commits_not_read == 2
    reopened.close()


def test_truncated_historical_segment_is_deferred_to_full_audit_not_startup(
    tmp_path: Path,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    _append(repository, 1)
    sealed = _seal(repository, historical_count=1)
    damaged = sealed.segment_path.read_bytes()[:-1]
    repository.close()
    sealed.segment_path.write_bytes(damaged)

    reopened = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    assert reopened.startup_report.segments_read == 0
    with pytest.raises(RepositoryError) as failure:
        reopened.full_audit()
    assert failure.value.code == RepositoryErrorCode.SEGMENT_MISMATCH
    reopened.close()


def test_truncated_intermediate_checkpoint_is_deferred_to_full_audit(
    tmp_path: Path,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    _append(repository, 1)
    first = _seal(repository, historical_count=1)
    _append(repository, 1)
    _seal(repository, historical_count=2)
    repository.close()
    first.checkpoint_path.write_bytes(first.checkpoint_path.read_bytes()[:-1])

    reopened = StorageRepository.open_existing(
        tmp_path / "repository",
        anchor=anchor,
        config=config,
    )
    assert reopened.startup_report.manifest_generation == 2
    with pytest.raises(RepositoryError) as failure:
        reopened.full_audit()
    assert failure.value.code == RepositoryErrorCode.CHECKPOINT_MISMATCH
    reopened.close()


@pytest.mark.parametrize(
    ("artifact", "expected_code"),
    [
        ("checkpoint", RepositoryErrorCode.CHECKPOINT_MISMATCH),
        ("manifest", RepositoryErrorCode.AUTHORITY_MISMATCH),
    ],
)
def test_startup_rejects_truncated_authoritative_artifact(
    tmp_path: Path,
    artifact: str,
    expected_code: RepositoryErrorCode,
) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    _append(repository, 1)
    sealed = _seal(repository, historical_count=1)
    path = (
        sealed.checkpoint_path
        if artifact == "checkpoint"
        else sealed.manifest_path
    )
    damaged = path.read_bytes()[:-1]
    repository.close()
    path.write_bytes(damaged)

    with pytest.raises(RepositoryError) as failure:
        StorageRepository.open_existing(
            tmp_path / "repository",
            anchor=anchor,
            config=config,
        )
    assert failure.value.code == expected_code


def test_anchor_ahead_of_available_manifest_fails_closed(tmp_path: Path) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    _append(repository, 1)
    _seal(repository, historical_count=1)
    repository.close()
    current = anchor.read()
    assert current is not None
    anchor.compare_and_swap(
        current,
        AnchorRecord(
            store_id=_STORE,
            generation=2,
            manifest_root=_hash(99),
        ),
    )

    with pytest.raises(RepositoryError) as failure:
        StorageRepository.open_existing(
            tmp_path / "repository",
            anchor=anchor,
            config=config,
        )
    assert failure.value.code == RepositoryErrorCode.AUTHORITY_MISSING


def test_overlay_ahead_of_anchor_is_not_silently_rolled_back(tmp_path: Path) -> None:
    repository, anchor, config = _new_repository(tmp_path)
    _append(repository, 1)
    _seal(repository, historical_count=1)
    state = repository.overlay_state
    overlay_path = repository.paths.overlay
    repository.close()

    overlay = SQLiteOverlay.open_existing(
        overlay_path,
        expected_identity=repository_module._overlay_identity(config),
    )
    overlay.advance_base(
        manifest_generation=2,
        manifest_root=_hash(98),
        base_commit_sequence=state.base_commit_sequence,
        base_prefix_root=state.base_prefix_root,
    )
    overlay.close()
    with pytest.raises(RepositoryError) as failure:
        StorageRepository.open_existing(
            tmp_path / "repository",
            anchor=anchor,
            config=config,
        )
    assert failure.value.code == RepositoryErrorCode.OVERLAY_AHEAD


def test_full_audit_detects_same_generation_manifest_fork(tmp_path: Path) -> None:
    repository, _, _ = _new_repository(tmp_path)
    _append(repository, 1)
    sealed = _seal(repository, historical_count=1)
    fork = replace(
        sealed.manifest,
        runtime_identity=OpaqueIdentity(_hash(88)),
    )
    fork_path = repository.paths.manifest_path(fork.identity.root)
    fork_path.write_bytes(manifest_to_bytes(fork))

    with pytest.raises(RepositoryError) as failure:
        repository.full_audit()
    assert failure.value.code == RepositoryErrorCode.MANIFEST_FORK
    repository.close()


def test_repository_discards_only_the_tail_after_exact_manifest_checkpoint_boundary(
    tmp_path: Path,
) -> None:
    repository, _, _ = _new_repository(tmp_path)
    _append(repository, 2)
    sealed = _seal(repository, historical_count=2)
    tail = _append(repository, 3)
    before = repository.overlay_state

    result = repository.discard_unsealed_tail(
        expected_manifest_root=sealed.manifest.identity.root,
        expected_checkpoint_root=sealed.checkpoint.root,
    )

    assert result.changed is True
    assert result.before == before
    assert result.discarded_commit_count == len(tail)
    assert repository.overlay_state == result.after
    assert repository.overlay_state.tail_commit_count == 0
    assert repository.manifest == sealed.manifest
    assert repository.checkpoint == sealed.checkpoint

    repeated = repository.discard_unsealed_tail(
        expected_manifest_root=sealed.manifest.identity.root,
        expected_checkpoint_root=sealed.checkpoint.root,
    )
    assert repeated.changed is False
    repository.close()


def test_repository_discard_refuses_wrong_published_authority_without_mutation(
    tmp_path: Path,
) -> None:
    repository, _, _ = _new_repository(tmp_path)
    _append(repository, 1)
    sealed = _seal(repository, historical_count=1)
    tail = _append(repository, 2)
    before = repository.overlay_state

    with pytest.raises(RepositoryError) as manifest_failure:
        repository.discard_unsealed_tail(
            expected_manifest_root=_hash(91),
            expected_checkpoint_root=sealed.checkpoint.root,
        )
    assert manifest_failure.value.code is RepositoryErrorCode.AUTHORITY_MISMATCH
    assert repository.overlay_state == before

    with pytest.raises(RepositoryError) as checkpoint_failure:
        repository.discard_unsealed_tail(
            expected_manifest_root=sealed.manifest.identity.root,
            expected_checkpoint_root=_hash(92),
        )
    assert checkpoint_failure.value.code is RepositoryErrorCode.CHECKPOINT_MISMATCH
    assert repository.overlay_state == before
    assert repository.startup_report.tail_frames == tail
    repository.close()
