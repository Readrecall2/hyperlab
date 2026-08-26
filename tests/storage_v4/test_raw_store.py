from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

import hyperlab.paper.storage_v4.raw_store as raw_store_module
from hyperlab.paper.storage_v4.anchor import LocalAnchor
from hyperlab.paper.storage_v4.contracts import RawLakeId
from hyperlab.paper.storage_v4.faults import DeterministicFaultInjector, FaultPoint, InjectedCrash
from hyperlab.paper.storage_v4.raw_manifest import build_raw_manifest, raw_manifest_to_bytes
from hyperlab.paper.storage_v4.raw_segment import RawRecordMetadata, RawSegmentWriter
from hyperlab.paper.storage_v4.raw_store import (
    DiskRawResolver,
    RawCurrentStatus,
    RawPendingStatus,
    RawStore,
    RawStoreConfig,
    RawStoreError,
    RawStoreErrorCode,
)
from hyperlab.paper.storage_v4.types import EventSequence, Hash32, StoreId, StreamId

_STORE_ID = StoreId("synthetic-raw-store")
_LAKE_ID = RawLakeId("synthetic-raw-lake")


def _config() -> RawStoreConfig:
    return RawStoreConfig(
        store_id=_STORE_ID,
        lake_id=_LAKE_ID,
        config_identity=Hash32(b"\x44" * 32),
    )


def _anchor(tmp_path: Path) -> LocalAnchor:
    return LocalAnchor.create(tmp_path / "raw-anchor.sqlite3", store_id=_STORE_ID)


def _artifact(tmp_path: Path, sequence: int, payload: bytes = b'{"raw":true}'):
    staging = tmp_path / f"staging-{sequence}"
    staging.mkdir(exist_ok=True)
    writer = RawSegmentWriter(staging, lake_id=_LAKE_ID)
    writer.append(
        payload,
        RawRecordMetadata(
            record_id=f"input-{sequence}",
            source_id="synthetic-source",
            venue_id="SYNTHETIC",
            input_type="PUBLIC_MARKET_EVENT",
            source_stream_id=StreamId("wire"),
            source_first_sequence=EventSequence(sequence),
            source_last_sequence=EventSequence(sequence),
            arrival_sequence=EventSequence(sequence),
            source_timestamp=f"2026-01-01T00:00:{sequence:02d}Z",
            received_timestamp=f"2026-01-01T00:00:{sequence:02d}Z",
        ),
    )
    return writer.seal()


def _new_store(tmp_path: Path) -> tuple[RawStore, LocalAnchor, RawStoreConfig]:
    anchor = _anchor(tmp_path)
    config = _config()
    return RawStore.create(tmp_path / "raw", anchor=anchor, config=config), anchor, config


@pytest.mark.parametrize(
    "fault_point",
    (
        FaultPoint.BEFORE_RAW_SEGMENT_PUBLICATION,
        FaultPoint.BEFORE_RAW_SEGMENT_COPY,
        FaultPoint.AFTER_RAW_SEGMENT_COPY,
        FaultPoint.AFTER_RAW_SEGMENT_PUBLICATION,
        FaultPoint.BEFORE_RAW_MANIFEST_PUBLICATION,
        FaultPoint.AFTER_RAW_MANIFEST_PUBLICATION,
        FaultPoint.BEFORE_RAW_ANCHOR_PUBLICATION,
        FaultPoint.AFTER_RAW_ANCHOR_PUBLICATION,
    ),
    ids=lambda point: point.value,
)
def test_raw_publication_crash_matrix_recovers_exactly_and_idempotently(
    tmp_path: Path,
    fault_point: FaultPoint,
) -> None:
    store, anchor, config = _new_store(tmp_path)
    artifact = _artifact(tmp_path, 1)
    injector = DeterministicFaultInjector(fault_point)
    store.set_fault_hook(injector)

    with pytest.raises(InjectedCrash) as caught:
        store.seal(artifact)

    assert caught.value.point is fault_point
    assert injector.triggered is True
    before_manifest = {
        FaultPoint.BEFORE_RAW_SEGMENT_PUBLICATION,
        FaultPoint.BEFORE_RAW_SEGMENT_COPY,
        FaultPoint.AFTER_RAW_SEGMENT_COPY,
        FaultPoint.AFTER_RAW_SEGMENT_PUBLICATION,
    }
    staging_survives = {
        FaultPoint.BEFORE_RAW_SEGMENT_PUBLICATION,
        FaultPoint.BEFORE_RAW_SEGMENT_COPY,
        FaultPoint.AFTER_RAW_SEGMENT_COPY,
    }
    manifest_is_published = {
        FaultPoint.AFTER_RAW_MANIFEST_PUBLICATION,
        FaultPoint.BEFORE_RAW_ANCHOR_PUBLICATION,
        FaultPoint.AFTER_RAW_ANCHOR_PUBLICATION,
    }
    expected_generation = 0 if fault_point in before_manifest else 1
    assert artifact.path.exists() is (fault_point in staging_survives)
    assert len(tuple(store.paths.segments.glob("*.hl4r"))) == (
        0 if fault_point in staging_survives else 1
    )
    assert len(tuple(store.paths.segments.glob(".*.tmp"))) == 0
    assert len(tuple(store.paths.manifests.glob("*.hl4rm"))) == (
        1 if fault_point in manifest_is_published else 0
    )
    assert store.paths.pending.exists() is (fault_point not in before_manifest)
    anchored_after_crash = anchor.read()
    assert (anchored_after_crash is not None) is (
        fault_point is FaultPoint.AFTER_RAW_ANCHOR_PUBLICATION
    )
    store.close()

    recovered = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert recovered.startup_report.generation == expected_generation
    assert (recovered.manifest is not None) is (expected_generation == 1)
    assert not recovered.paths.pending.exists()
    if expected_generation == 0:
        assert recovered.startup_report.pending_status is RawPendingStatus.ABSENT
        assert recovered.startup_report.current_status is RawCurrentStatus.GENESIS_ABSENT
    elif fault_point is FaultPoint.AFTER_RAW_ANCHOR_PUBLICATION:
        assert recovered.startup_report.pending_status is RawPendingStatus.COMMITTED_CLEARED
        assert recovered.startup_report.current_status is RawCurrentStatus.ABSENT_REPAIRED
    else:
        assert (
            recovered.startup_report.pending_status
            is RawPendingStatus.DIRECT_SUCCESSOR_ADOPTED
        )
        assert recovered.startup_report.current_status is RawCurrentStatus.ABSENT_REPAIRED
    if recovered.manifest is not None:
        assert recovered.full_audit().records_read == 1
    recovered.close()

    repeated = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert repeated.startup_report.generation == expected_generation
    assert repeated.startup_report.adopted_direct_successor is False
    assert repeated.startup_report.pending_status is RawPendingStatus.ABSENT
    assert repeated.startup_report.current_status is (
        RawCurrentStatus.GENESIS_ABSENT
        if expected_generation == 0
        else RawCurrentStatus.EXACT
    )
    repeated.close()


@pytest.mark.parametrize(
    ("fault_point", "expected_first_status"),
    (
        (FaultPoint.BEFORE_CURRENT_PUBLICATION, RawCurrentStatus.ABSENT_REPAIRED),
        (FaultPoint.AFTER_CURRENT_PUBLICATION, RawCurrentStatus.EXACT),
    ),
    ids=("before_raw_current", "after_raw_current"),
)
def test_raw_current_crash_boundary_is_non_authoritative_and_idempotent(
    tmp_path: Path,
    fault_point: FaultPoint,
    expected_first_status: RawCurrentStatus,
) -> None:
    store, anchor, config = _new_store(tmp_path)
    injector = DeterministicFaultInjector(fault_point)
    store.set_fault_hook(injector)

    with pytest.raises(InjectedCrash) as caught:
        store.seal(_artifact(tmp_path, 1))

    assert caught.value.point is fault_point
    assert anchor.read() is not None
    assert not store.paths.pending.exists()
    assert store.paths.current.exists() is (
        fault_point is FaultPoint.AFTER_CURRENT_PUBLICATION
    )
    store.close()

    recovered = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert recovered.startup_report.generation == 1
    assert recovered.startup_report.current_status is expected_first_status
    assert recovered.full_audit().records_read == 1
    recovered.close()

    repeated = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert repeated.startup_report.current_status is RawCurrentStatus.EXACT
    assert repeated.startup_report.adopted_direct_successor is False
    repeated.close()


def test_raw_copy_hooks_bracket_the_true_staging_plus_temp_peak(
    tmp_path: Path,
) -> None:
    store, _, _ = _new_store(tmp_path)
    artifact = _artifact(tmp_path, 1, b"scratch-census")
    observed: dict[FaultPoint, tuple[bool, tuple[tuple[str, int], ...]]] = {}

    def observe(point: FaultPoint) -> None:
        if point not in {
            FaultPoint.BEFORE_RAW_SEGMENT_COPY,
            FaultPoint.AFTER_RAW_SEGMENT_COPY,
        }:
            return
        entries = tuple(
            sorted(
                (path.name, path.stat().st_size)
                for path in store.paths.segments.iterdir()
            )
        )
        observed[point] = (artifact.path.exists(), entries)

    store.set_fault_hook(observe)
    sealed = store.seal(artifact)

    assert observed[FaultPoint.BEFORE_RAW_SEGMENT_COPY] == (True, ())
    staging_present, after_entries = observed[FaultPoint.AFTER_RAW_SEGMENT_COPY]
    assert staging_present is True
    assert len(after_entries) == 1
    temporary_name, temporary_size = after_entries[0]
    assert temporary_name.startswith(".")
    assert temporary_name.endswith(".tmp")
    assert temporary_size == sealed.segment_path.stat().st_size
    assert not artifact.path.exists()
    store.close()


def test_store_seals_idempotently_resolves_ranges_and_full_audits(tmp_path: Path) -> None:
    store, anchor, config = _new_store(tmp_path)
    artifact = _artifact(tmp_path, 1, b'{"payload":"exact"}')
    first = store.seal(artifact)
    repeated = store.seal(artifact)

    assert first.manifest.generation == 1
    assert repeated.manifest.root == first.manifest.root
    assert len(first.references) == 1
    resolver = DiskRawResolver(store)
    assert resolver.resolve(first.references[0]) == b'{"payload":"exact"}'
    assert resolver.physical_hash_passes == 1
    assert resolver.resolve(first.references[0]) == b'{"payload":"exact"}'
    assert resolver.physical_hash_passes == 1
    audit = store.full_audit()
    assert audit.segments_read == 1
    assert audit.records_read == 1
    store.close()

    reopened = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert reopened.startup_report.historical_segments_read == 0
    assert reopened.startup_report.current_status is RawCurrentStatus.EXACT
    reopened.close()


def test_resolver_authenticates_each_manifest_root_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, _, _ = _new_store(tmp_path)
    first = store.seal(_artifact(tmp_path, 1, b"first"))
    store.seal(_artifact(tmp_path, 2, b"second"))
    calls = 0
    original = RawStore._read_manifest

    def counted(paths, root: Hash32, config: RawStoreConfig):
        nonlocal calls
        calls += 1
        return original(paths, root, config)

    monkeypatch.setattr(RawStore, "_read_manifest", staticmethod(counted))
    resolver = DiskRawResolver(store)
    assert resolver.resolve(first.references[0]) == b"first"
    assert resolver.resolve(first.references[0]) == b"first"
    assert calls == 1
    assert resolver.manifest_authentication_passes == 1
    assert resolver.authenticated_manifest_count == 2
    assert resolver.cached_segment_count == 1
    assert resolver.physical_hash_passes == 1
    store.close()


def test_resolver_keeps_only_current_segment_summary(tmp_path: Path) -> None:
    store, _, _ = _new_store(tmp_path)
    sealed = [store.seal(_artifact(tmp_path, sequence, f"payload-{sequence}".encode())) for sequence in range(1, 5)]
    resolver = DiskRawResolver(store)

    for result in sealed:
        assert resolver.resolve(result.references[0]).startswith(b"payload-")
        assert resolver.cached_segment_count == 1

    assert resolver.authenticated_manifest_count == 4
    assert resolver.manifest_authentication_passes == 1
    assert resolver.physical_hash_passes == 4
    store.close()


def test_resolver_rejects_wrong_store_lake_hash_bounds_and_replacement(
    tmp_path: Path,
) -> None:
    store, _, _ = _new_store(tmp_path)
    sealed = store.seal(_artifact(tmp_path, 1, b"payload"))
    reference = sealed.references[0]
    resolver = DiskRawResolver(store)

    variants = (
        replace(reference, raw_store_id=StoreId("other-raw-store")),
        replace(reference, lake_id=RawLakeId("other-lake")),
        replace(reference, physical_sha256=Hash32(b"\x55" * 32)),
        replace(reference, byte_offset=reference.byte_offset + 1),
        replace(reference, logical_payload_sha256=Hash32(b"\x66" * 32)),
    )
    expected = {
        RawStoreErrorCode.AUTHORITY_MISMATCH,
        RawStoreErrorCode.WRONG_LAKE,
        RawStoreErrorCode.ORPHAN_REFERENCE,
        RawStoreErrorCode.RANGE_INVALID,
        RawStoreErrorCode.PAYLOAD_MISMATCH,
    }
    observed = set()
    for variant in variants:
        with pytest.raises(RawStoreError) as caught:
            resolver.resolve(variant)
        observed.add(caught.value.code)
    assert observed == expected

    segment = sealed.segment_path
    segment.write_bytes(segment.read_bytes()[:-1])
    with pytest.raises(RawStoreError) as replaced_error:
        DiskRawResolver(store).resolve(reference)
    assert replaced_error.value.code in {
        RawStoreErrorCode.SEGMENT_MISMATCH,
        RawStoreErrorCode.SEGMENT_REPLACED,
    }
    store.close()


def test_missing_manifest_fails_closed(tmp_path: Path) -> None:
    store, anchor, config = _new_store(tmp_path)
    sealed = store.seal(_artifact(tmp_path, 1))
    store.close()
    sealed.manifest_path.unlink()

    with pytest.raises(RawStoreError) as caught:
        RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert caught.value.code is RawStoreErrorCode.MANIFEST_MISSING


def test_second_writer_is_refused_until_close(tmp_path: Path) -> None:
    store, anchor, config = _new_store(tmp_path)
    with pytest.raises(RawStoreError) as caught:
        RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert caught.value.code is RawStoreErrorCode.WRITER_LEASE_HELD
    store.close()

    reopened = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    reopened.close()


def test_direct_successor_is_adopted_and_recovery_is_idempotent(tmp_path: Path) -> None:
    store, anchor, config = _new_store(tmp_path)
    store.set_fault_hook(
        DeterministicFaultInjector(FaultPoint.AFTER_RAW_MANIFEST_PUBLICATION)
    )
    with pytest.raises(InjectedCrash):
        store.seal(_artifact(tmp_path, 1))
    store.close()
    assert anchor.read() is None

    recovered = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert recovered.manifest is not None
    assert recovered.manifest.generation == 1
    assert recovered.startup_report.adopted_direct_successor is True
    assert recovered.startup_report.manifests_opened == 1
    assert recovered.startup_report.manifest_namespace_entries_scanned == 1
    assert (
        recovered.startup_report.pending_status
        is RawPendingStatus.DIRECT_SUCCESSOR_ADOPTED
    )
    assert not recovered.paths.pending.exists()
    recovered.close()

    repeated = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert repeated.startup_report.adopted_direct_successor is False
    repeated.close()


def test_pending_reconstructs_manifest_after_prepublication_crash(tmp_path: Path) -> None:
    store, anchor, config = _new_store(tmp_path)
    store.set_fault_hook(
        DeterministicFaultInjector(FaultPoint.BEFORE_RAW_MANIFEST_PUBLICATION)
    )
    with pytest.raises(InjectedCrash):
        store.seal(_artifact(tmp_path, 1))
    assert store.paths.pending.is_file()
    assert not tuple(store.paths.manifests.glob("*.hl4rm"))
    store.close()

    recovered = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert recovered.manifest is not None
    assert recovered.manifest.generation == 1
    assert recovered.startup_report.adopted_direct_successor is True
    assert recovered.startup_report.manifests_opened == 1
    assert recovered.startup_report.manifest_namespace_entries_scanned == 0
    assert not recovered.paths.pending.exists()
    recovered.close()


def test_committed_pending_is_cleared_without_reading_a_successor(
    tmp_path: Path,
) -> None:
    store, anchor, config = _new_store(tmp_path)
    store.set_fault_hook(
        DeterministicFaultInjector(FaultPoint.AFTER_RAW_ANCHOR_PUBLICATION)
    )
    with pytest.raises(InjectedCrash):
        store.seal(_artifact(tmp_path, 1))
    assert store.paths.pending.is_file()
    assert anchor.read() is not None
    store.close()

    recovered = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert recovered.startup_report.adopted_direct_successor is False
    assert recovered.startup_report.manifests_opened == 1
    assert recovered.startup_report.manifest_namespace_entries_scanned == 1
    assert recovered.startup_report.pending_status is RawPendingStatus.COMMITTED_CLEARED
    assert not recovered.paths.pending.exists()
    recovered.close()


def test_corrupt_pending_fails_closed(tmp_path: Path) -> None:
    store, anchor, config = _new_store(tmp_path)
    store.set_fault_hook(
        DeterministicFaultInjector(FaultPoint.BEFORE_RAW_MANIFEST_PUBLICATION)
    )
    with pytest.raises(InjectedCrash):
        store.seal(_artifact(tmp_path, 1))
    store.paths.pending.write_bytes(b"corrupt")
    store.close()

    with pytest.raises(RawStoreError) as caught:
        RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert caught.value.code is RawStoreErrorCode.PENDING_MISMATCH


def test_live_writer_cannot_overwrite_unresolved_pending(tmp_path: Path) -> None:
    store, _, _ = _new_store(tmp_path)
    store.set_fault_hook(
        DeterministicFaultInjector(FaultPoint.BEFORE_RAW_MANIFEST_PUBLICATION)
    )
    with pytest.raises(InjectedCrash):
        store.seal(_artifact(tmp_path, 1))
    store.set_fault_hook(None)
    second = _artifact(tmp_path, 2)

    with pytest.raises(RawStoreError) as caught:
        store.seal(second)
    assert caught.value.code is RawStoreErrorCode.PENDING_MISMATCH
    assert second.path.is_file()
    store.close()


def test_startup_opens_only_latest_manifest_across_many_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, anchor, config = _new_store(tmp_path)
    for sequence in range(1, 25):
        store.seal(_artifact(tmp_path, sequence, f"payload-{sequence}".encode()))
    assert store.manifest is not None
    latest_path = store.paths.manifest_path(store.manifest.root)
    store.close()

    opened_manifests: list[Path] = []
    original = raw_store_module._open_regular

    def counted(path: Path):
        if path.suffix == ".hl4rm":
            opened_manifests.append(path)
        return original(path)

    monkeypatch.setattr(raw_store_module, "_open_regular", counted)
    reopened = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert opened_manifests == [latest_path]
    assert reopened.startup_report.manifests_opened == 1
    assert reopened.startup_report.manifest_namespace_entries_scanned == 0
    assert reopened.startup_report.pending_status is RawPendingStatus.ABSENT
    reopened.close()


def test_ambiguous_unanchored_successor_is_refused(tmp_path: Path) -> None:
    store, anchor, config = _new_store(tmp_path)
    store.set_fault_hook(
        DeterministicFaultInjector(FaultPoint.AFTER_RAW_MANIFEST_PUBLICATION)
    )
    with pytest.raises(InjectedCrash):
        store.seal(_artifact(tmp_path, 1))
    store.set_fault_hook(None)

    alternate = _artifact(tmp_path, 2, b"alternate")
    alternate_manifest = build_raw_manifest(
        store_id=config.store_id,
        lake_id=config.lake_id,
        config_identity=config.config_identity,
        generation=1,
        parent_manifest_root=None,
        segments=(store.descriptor_for(alternate),),
    )
    alternate_path = store.paths.manifest_path(alternate_manifest.root)
    alternate_path.write_bytes(raw_manifest_to_bytes(alternate_manifest))
    store.close()

    with pytest.raises(RawStoreError) as caught:
        RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert caught.value.code is RawStoreErrorCode.MANIFEST_FORK


@pytest.mark.parametrize(
    ("variant", "expected"),
    [
        ("absent", RawCurrentStatus.ABSENT_REPAIRED),
        ("stale", RawCurrentStatus.STALE_REPAIRED),
        ("corrupt", RawCurrentStatus.CORRUPT_REPAIRED),
    ],
)
def test_current_is_non_authoritative_and_repaired(
    tmp_path: Path,
    variant: str,
    expected: RawCurrentStatus,
) -> None:
    store, anchor, config = _new_store(tmp_path)
    store.seal(_artifact(tmp_path, 1))
    current = store.paths.current
    exact = current.read_bytes()
    store.close()
    if variant == "absent":
        current.unlink()
    elif variant == "stale":
        current.write_bytes(b'{"generation":0}\n')
    else:
        current.write_bytes(b"corrupt")

    reopened = RawStore.open_existing(tmp_path / "raw", anchor=anchor, config=config)
    assert reopened.startup_report.current_status is expected
    assert current.read_bytes() == exact
    reopened.close()


def test_segment_symlink_is_never_followed(tmp_path: Path) -> None:
    store, _, _ = _new_store(tmp_path)
    sealed = store.seal(_artifact(tmp_path, 1, b"payload"))
    original = sealed.segment_path
    replacement = tmp_path / "replacement.hl4r"
    replacement.write_bytes(original.read_bytes())
    original.unlink()
    try:
        original.symlink_to(replacement)
    except OSError:
        store.close()
        pytest.skip("local Windows policy does not permit an unprivileged symlink")

    with pytest.raises(RawStoreError) as caught:
        DiskRawResolver(store).resolve(sealed.references[0])
    assert caught.value.code is RawStoreErrorCode.PATH_LAYOUT
    store.close()


def test_raw_store_refuses_a_link_or_reparse_ancestor(tmp_path: Path) -> None:
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked-parent"
    try:
        linked_parent.symlink_to(real_parent, target_is_directory=True)
    except OSError:
        pytest.skip("local Windows policy does not permit an unprivileged symlink")
    anchor = _anchor(tmp_path)

    with pytest.raises(RawStoreError) as caught:
        RawStore.create(linked_parent / "raw", anchor=anchor, config=_config())
    assert caught.value.code in {
        RawStoreErrorCode.ALREADY_EXISTS,
        RawStoreErrorCode.PATH_LAYOUT,
    }


def test_post_open_segments_directory_link_fails_all_read_paths(tmp_path: Path) -> None:
    store, _, _ = _new_store(tmp_path)
    sealed = store.seal(_artifact(tmp_path, 1, b"payload"))
    segments = store.paths.segments
    relocated = store.paths.root / "segments-relocated"
    segments.rename(relocated)
    try:
        segments.symlink_to(relocated, target_is_directory=True)
    except OSError:
        relocated.rename(segments)
        store.close()
        pytest.skip("local policy does not permit a directory symlink or junction")

    operations = (
        lambda: store.authenticated_manifest(sealed.manifest.root),
        store.full_audit,
        lambda: DiskRawResolver(store).resolve(sealed.references[0]),
    )
    for operation in operations:
        with pytest.raises(RawStoreError) as caught:
            operation()
        assert caught.value.code is RawStoreErrorCode.PATH_LAYOUT
    store.close()


def test_reference_contains_no_path_locator(tmp_path: Path) -> None:
    store, _, _ = _new_store(tmp_path)
    reference = store.seal(_artifact(tmp_path, 1)).references[0]
    names = set(reference.__dataclass_fields__)
    assert not names.intersection({"path", "filename", "relative_path", "locator"})
    assert "physical_sha256" in names
    store.close()


def test_reattest_contiguous_suffix_reads_only_multi_generation_suffix_segments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, _, _ = _new_store(tmp_path)
    sealed = tuple(
        store.seal(_artifact(tmp_path, sequence, f"payload-{sequence}".encode()))
        for sequence in range(1, 5)
    )
    boundary = sealed[1].manifest
    verified: list[Hash32] = []
    original = RawStore._verify_published_segment

    def counted(
        selected: RawStore,
        path: Path,
        descriptor,
        *,
        replacement: bool = False,
    ):
        verified.append(descriptor.physical_sha256)
        return original(
            selected,
            path,
            descriptor,
            replacement=replacement,
        )

    monkeypatch.setattr(RawStore, "_verify_published_segment", counted)
    report = store.reattest_contiguous_suffix(
        boundary_manifest_root=boundary.root,
        next_arrival_sequence=3,
    )

    assert report.boundary_generation == 2
    assert report.authority_generation == 4
    assert report.suffix_manifests_read == 2
    assert report.suffix_segments_read == 2
    assert report.suffix_records_read == 2
    assert report.first_arrival_sequence == 3
    assert report.last_arrival_sequence == 4
    assert verified == [
        sealed[2].descriptor.physical_sha256,
        sealed[3].descriptor.physical_sha256,
    ]

    verified.clear()
    references = store.authenticated_suffix_references(
        boundary_manifest_root=boundary.root,
        next_arrival_sequence=3,
    )
    assert tuple(int(reference.arrival_sequence) for reference in references) == (3, 4)
    assert tuple(reference.raw_manifest_root for reference in references) == (
        sealed[2].manifest.root,
        sealed[3].manifest.root,
    )
    assert verified == [
        sealed[2].descriptor.physical_sha256,
        sealed[3].descriptor.physical_sha256,
        sealed[2].descriptor.physical_sha256,
        sealed[3].descriptor.physical_sha256,
    ]

    verified.clear()
    aligned = store.reattest_contiguous_suffix(
        boundary_manifest_root=sealed[3].manifest.root,
        next_arrival_sequence=5,
    )
    assert aligned.suffix_manifests_read == 0
    assert aligned.suffix_segments_read == 0
    assert aligned.suffix_records_read == 0
    assert aligned.first_arrival_sequence is None
    assert aligned.last_arrival_sequence is None
    assert verified == []
    store.close()


def test_reattest_contiguous_suffix_rejects_an_arrival_gap(tmp_path: Path) -> None:
    store, _, _ = _new_store(tmp_path)
    boundary = store.seal(_artifact(tmp_path, 1)).manifest
    store.seal(_artifact(tmp_path, 3))

    with pytest.raises(RawStoreError) as failure:
        store.reattest_contiguous_suffix(
            boundary_manifest_root=boundary.root,
            next_arrival_sequence=2,
        )

    assert failure.value.code is RawStoreErrorCode.RANGE_INVALID
    store.close()


@pytest.mark.parametrize(
    ("namespace", "suffix", "expected_code"),
    (
        ("manifests", ".hl4rm", RawStoreErrorCode.MANIFEST_FORK),
        ("segments", ".hl4r", RawStoreErrorCode.SEGMENT_MISMATCH),
    ),
)
def test_full_raw_audit_rejects_every_extra_namespace_artifact(
    tmp_path: Path,
    namespace: str,
    suffix: str,
    expected_code: RawStoreErrorCode,
) -> None:
    store, _, _ = _new_store(tmp_path)
    store.seal(_artifact(tmp_path, 1))
    directory = getattr(store.paths, namespace)
    directory.joinpath(f"{'aa' * 32}{suffix}").write_bytes(b"extra")

    with pytest.raises(RawStoreError) as failure:
        store.full_audit()

    assert failure.value.code is expected_code
    store.close()
