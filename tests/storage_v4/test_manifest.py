from __future__ import annotations

from dataclasses import replace

import pytest

from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.manifest import (
    Manifest,
    ManifestFormatError,
    ManifestHead,
    ManifestReadLimits,
    OpaqueIdentity,
    SegmentDescriptor,
    build_manifest,
    manifest_from_bytes,
    manifest_to_bytes,
    verify_manifest,
    verify_manifest_segments,
)
from hyperlab.paper.storage_v4.segment import (
    CodecProfile,
    SegmentArtifact,
    build_segment,
)
from hyperlab.paper.storage_v4.types import (
    CommitFrame,
    CommitOrdinal,
    CommitSequence,
    Hash32,
    LocalCount,
    LogicalRow,
    RunId,
    SegmentIdentity,
    StoreId,
    StreamId,
)

SYNTHETIC_STORAGE_V4_WORKLOAD = True
_STORE_ID = StoreId("SYNTHETIC_STORAGE_V4_WORKLOAD/store")
_RUN_ID = RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/run-manifest")
_ZERO = Hash32(b"\x00" * 32)
_MANIFEST_V1_FROZEN_HEX = """
484c344d414e00010001000000000000026c00010000002a53594e5448455449435f53544f524147455f56345f574f52
4b4c4f41442f73746f72652d766563746f720000002d53594e5448455449435f53544f524147455f56345f574f524b4c
4f41442f6d616e69666573742d766563746f720000000000000001000101010101010101010101010101010101010101
010101010101010101010101020202020202020202020202020202020202020202020202020202020202020203030303
030303030303030303030303030303030303030303030303030303030404040404040404040404040404040404040404
040404040404040404040404000000000000000000000000000000000000000000000000000000000000000000000001
0000011211111111111111111111111111111111111111111111111111111111111111110000002d53594e5448455449
435f53544f524147455f56345f574f524b4c4f41442f6d616e69666573742d766563746f720000000000000001000000
000000000100000000000000000000000000000000000000000000000000000000000000002222222222222222222222
222222222222222222222222222222222222222222333333333333333333333333333333333333333333333333333333
33333333334444444444444444444444444444444444444444444444444444444444444444000000000000007b000000
000000002d0000000100000001000000066576656e747300000001000000067261772d76310000000000000000012222
222222222222222222222222222222222222222222222222222222222222111111111111111111111111111111111111
111111111111111111111111111156f09c5ca9ffd42606375db6f1f39843229022f69d28cf5fe9f5d211586fa5f2
"""


def _frames() -> tuple[CommitFrame, ...]:
    previous = _ZERO
    result: list[CommitFrame] = []
    for sequence in range(1, 7):
        frame = CommitFrame(
            run_id=_RUN_ID,
            commit_sequence=CommitSequence(sequence),
            previous_prefix_root=previous,
            rows=(
                LogicalRow(
                    stream_id=StreamId("events" if sequence % 2 else "inputs"),
                    ordinal=CommitOrdinal(0),
                    value={"sequence": sequence},
                ),
            ),
        )
        result.append(frame)
        previous = build_commit_logical(frame).prefix_root
    return tuple(result)


def _artifacts() -> tuple[SegmentArtifact, ...]:
    frames = _frames()
    first = build_segment(frames[:2], codec=CodecProfile.raw())
    second = build_segment(frames[2:4], codec=CodecProfile.zlib(level=1))
    third = build_segment(frames[4:], codec=CodecProfile.zlib(level=9))
    return first, second, third


def _descriptors() -> tuple[SegmentDescriptor, ...]:
    return tuple(SegmentDescriptor.from_segment(item) for item in _artifacts())


def _identity(byte: int) -> OpaqueIdentity:
    return OpaqueIdentity(Hash32(bytes([byte]) * 32))


def _manifest(
    *,
    descriptors: tuple[SegmentDescriptor, ...] | None = None,
    generation: int = 1,
    parent: Hash32 | None = None,
) -> Manifest:
    return build_manifest(
        store_id=_STORE_ID,
        run_id=_RUN_ID,
        generation=generation,
        parent_manifest_root=parent,
        run_identity=_identity(1),
        config_identity=_identity(2),
        code_identity=_identity(3),
        runtime_identity=_identity(4),
        start_prefix_root=_ZERO,
        segments=_descriptors() if descriptors is None else descriptors,
    )


def test_manifest_chain_and_serialization_are_stable() -> None:
    parent = _manifest()
    parent_bytes = manifest_to_bytes(parent)
    child = _manifest(
        generation=2,
        parent=parent.identity.root,
    )

    verify_manifest(parent)
    verify_manifest(
        child,
        expected_parent_root=parent.identity.root,
        expected_generation=2,
    )
    assert manifest_to_bytes(parent) == parent_bytes
    assert manifest_from_bytes(parent_bytes) == parent
    assert manifest_to_bytes(manifest_from_bytes(parent_bytes)) == parent_bytes


def test_manifest_matches_frozen_protocol_v1_file_and_root_vector() -> None:
    run_id = RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/manifest-vector")
    descriptor = SegmentDescriptor(
        identity=SegmentIdentity(Hash32(b"\x11" * 32)),
        run_id=run_id,
        first_commit_sequence=CommitSequence(1),
        last_commit_sequence=CommitSequence(1),
        previous_prefix_root=_ZERO,
        end_prefix_root=Hash32(b"\x22" * 32),
        merkle_root=Hash32(b"\x33" * 32),
        physical_sha256=Hash32(b"\x44" * 32),
        physical_size=123,
        logical_size=45,
        commit_count=LocalCount(1),
        counts_by_stream=((StreamId("events"), LocalCount(1)),),
        codec_profile="raw-v1",
    )
    manifest = build_manifest(
        store_id=StoreId("SYNTHETIC_STORAGE_V4_WORKLOAD/store-vector"),
        run_id=run_id,
        generation=1,
        parent_manifest_root=None,
        run_identity=_identity(1),
        config_identity=_identity(2),
        code_identity=_identity(3),
        runtime_identity=_identity(4),
        start_prefix_root=_ZERO,
        segments=(descriptor,),
    )
    expected = bytes.fromhex(_MANIFEST_V1_FROZEN_HEX)

    assert len(expected) == 670
    assert manifest.identity.root == Hash32.from_hex(
        "56f09c5ca9ffd42606375db6f1f39843229022f69d28cf5fe9f5d211586fa5f2"
    )
    assert manifest_to_bytes(manifest) == expected
    assert manifest_from_bytes(expected) == manifest


def test_manifest_rejects_invalid_generation_or_parent() -> None:
    with pytest.raises((TypeError, ValueError), match="generation"):
        _manifest(generation=0)
    with pytest.raises((TypeError, ValueError), match="parent"):
        _manifest(generation=1, parent=Hash32(b"\x01" * 32))

    parent = _manifest()
    child = _manifest(generation=2, parent=parent.identity.root)
    with pytest.raises(ManifestFormatError, match="parent"):
        verify_manifest(child, expected_parent_root=Hash32(b"\xff" * 32))


@pytest.mark.parametrize("case", ("gap", "overlap", "reordered", "duplicate"))
def test_manifest_rejects_noncontiguous_or_duplicate_segments(case: str) -> None:
    first, second, third = _descriptors()
    if case == "gap":
        descriptors = (first, third)
    elif case == "overlap":
        descriptors = (
            first,
            replace(second, first_commit_sequence=first.last_commit_sequence),
            third,
        )
    elif case == "reordered":
        descriptors = (second, first, third)
    else:
        descriptors = (first, second, second)

    with pytest.raises(ManifestFormatError):
        _manifest(descriptors=descriptors)


def test_manifest_rejects_run_or_prefix_root_incompatibility() -> None:
    first, second, third = _descriptors()
    wrong_run = replace(
        second,
        run_id=RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/wrong-run"),
    )
    wrong_prefix = replace(second, previous_prefix_root=Hash32(b"\x88" * 32))

    with pytest.raises(ManifestFormatError, match="run"):
        _manifest(descriptors=(first, wrong_run, third))
    with pytest.raises(ManifestFormatError, match="prefix"):
        _manifest(descriptors=(first, wrong_prefix, third))


def test_manifest_root_changes_when_physical_hash_changes() -> None:
    descriptors = _descriptors()
    original = _manifest(descriptors=descriptors)
    changed_descriptor = replace(
        descriptors[1],
        physical_sha256=Hash32(b"\x77" * 32),
    )
    changed = _manifest(
        descriptors=(descriptors[0], changed_descriptor, descriptors[2])
    )

    assert changed.identity != original.identity
    assert manifest_to_bytes(changed) != manifest_to_bytes(original)


def test_manifest_rejects_missing_segments_and_incompatible_head() -> None:
    with pytest.raises(ManifestFormatError, match="segment"):
        _manifest(descriptors=())

    valid = _manifest()
    bad_head = replace(
        valid,
        head=ManifestHead(
            commit_sequence=valid.head.commit_sequence,
            prefix_root=Hash32(b"\x66" * 32),
            segment_identity=valid.head.segment_identity,
        ),
    )
    with pytest.raises(ManifestFormatError, match="head"):
        verify_manifest(bad_head)


def test_manifest_rejects_malformed_checkpoint_root_and_truncation() -> None:
    descriptors = _descriptors()
    with pytest.raises((TypeError, ValueError)):
        replace(descriptors[0], checkpoint_root=b"short")

    encoded = manifest_to_bytes(_manifest(descriptors=descriptors))
    with pytest.raises(ManifestFormatError):
        manifest_from_bytes(encoded[:-1])


def test_manifest_crosschecks_every_descriptor_against_verified_segment_bytes() -> None:
    artifacts = _artifacts()
    descriptors = tuple(SegmentDescriptor.from_segment(item) for item in artifacts)
    manifest = _manifest(descriptors=descriptors)

    verify_manifest_segments(manifest, artifacts)
    with pytest.raises(ManifestFormatError, match="segment artifact count"):
        verify_manifest_segments(manifest, artifacts[:-1])

    wrong_merkle = replace(
        descriptors[1],
        merkle_root=Hash32(b"\x55" * 32),
    )
    manifest_with_wrong_root = _manifest(
        descriptors=(descriptors[0], wrong_merkle, descriptors[2])
    )
    with pytest.raises(ManifestFormatError, match="descriptor"):
        verify_manifest_segments(manifest_with_wrong_root, artifacts)

    wrong_physical = replace(
        descriptors[1],
        physical_sha256=Hash32(b"\x44" * 32),
    )
    manifest_with_wrong_physical = _manifest(
        descriptors=(descriptors[0], wrong_physical, descriptors[2])
    )
    with pytest.raises(ManifestFormatError, match="descriptor"):
        verify_manifest_segments(manifest_with_wrong_physical, artifacts)


def test_manifest_optional_checkpoint_root_roundtrips_as_raw_hash32() -> None:
    descriptors = _descriptors()
    checkpoint = Hash32(b"\xaa" * 32)
    with_checkpoint = replace(descriptors[0], checkpoint_root=checkpoint)
    manifest = _manifest(
        descriptors=(with_checkpoint, descriptors[1], descriptors[2])
    )

    decoded = manifest_from_bytes(manifest_to_bytes(manifest))

    assert decoded.segments[0].checkpoint_root == checkpoint


def test_segment_descriptor_rejects_untyped_or_zero_stream_counts() -> None:
    descriptor = _descriptors()[0]

    with pytest.raises(TypeError, match="pairs"):
        replace(
            descriptor,
            counts_by_stream=(
                [StreamId("events"), LocalCount(1)],  # type: ignore[arg-type]
            ),
        )
    with pytest.raises(ValueError, match="zero"):
        replace(
            descriptor,
            counts_by_stream=((StreamId("events"), LocalCount(0)),),
        )


def test_manifest_reader_applies_limits_before_large_copies_or_object_counts() -> None:
    encoded = manifest_to_bytes(_manifest())
    body_size_offset = 8 + 2
    body_size = int.from_bytes(
        encoded[body_size_offset : body_size_offset + 8],
        "big",
    )

    with pytest.raises(ManifestFormatError, match="physical size exceeds"):
        manifest_from_bytes(
            encoded,
            limits=ManifestReadLimits(max_physical_size=len(encoded) - 1),
        )
    with pytest.raises(ManifestFormatError, match="body size exceeds"):
        manifest_from_bytes(
            encoded,
            limits=ManifestReadLimits(max_body_size=body_size - 1),
        )
    with pytest.raises(ManifestFormatError, match="segment count exceeds"):
        manifest_from_bytes(
            encoded,
            limits=ManifestReadLimits(max_segments=2),
        )
    with pytest.raises(ManifestFormatError, match="descriptor size exceeds"):
        manifest_from_bytes(
            encoded,
            limits=ManifestReadLimits(max_descriptor_size=1),
        )

    for invalid in (0, -1, True):
        with pytest.raises((TypeError, ValueError)):
            ManifestReadLimits(max_segments=invalid)  # type: ignore[arg-type]
