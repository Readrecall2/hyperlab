from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from hyperlab.paper.storage_v4.contracts import RawLakeId
from hyperlab.paper.storage_v4.raw_manifest import (
    RawManifestError,
    RawManifestErrorCode,
    RawSegmentDescriptor,
    build_raw_manifest,
    raw_manifest_from_bytes,
    raw_manifest_to_bytes,
    verify_raw_manifest,
    verify_raw_manifest_transition,
)
from hyperlab.paper.storage_v4.raw_segment import RawRecordMetadata, RawSegmentWriter
from hyperlab.paper.storage_v4.types import EventSequence, Hash32, StoreId, StreamId

_STORE = StoreId("synthetic-raw-store")
_LAKE = RawLakeId("synthetic-raw-lake")
_CONFIG = Hash32(b"\x42" * 32)


def _descriptor(tmp_path: Path, sequence: int) -> RawSegmentDescriptor:
    directory = tmp_path / f"segment-{sequence}"
    directory.mkdir()
    writer = RawSegmentWriter(directory, lake_id=_LAKE)
    writer.append(
        f'{{"sequence":{sequence}}}'.encode(),
        RawRecordMetadata(
            record_id=f"input-{sequence}",
            source_id="synthetic",
            venue_id=None,
            input_type="PUBLIC_MARKET_EVENT",
            source_stream_id=StreamId("wire"),
            source_first_sequence=EventSequence(sequence),
            source_last_sequence=EventSequence(sequence),
            arrival_sequence=EventSequence(sequence),
            received_timestamp=f"2026-01-01T00:00:{sequence:02d}Z",
        ),
    )
    return RawSegmentDescriptor.from_artifact(writer.seal())


def test_manifest_roundtrip_and_append_only_transition(tmp_path: Path) -> None:
    first_descriptor = _descriptor(tmp_path, 1)
    first = build_raw_manifest(
        store_id=_STORE,
        lake_id=_LAKE,
        config_identity=_CONFIG,
        generation=1,
        parent_manifest_root=None,
        segments=(first_descriptor,),
    )
    encoded = raw_manifest_to_bytes(first)
    assert raw_manifest_from_bytes(encoded) == first
    verify_raw_manifest(first, expected_generation=1)

    second = build_raw_manifest(
        store_id=_STORE,
        lake_id=_LAKE,
        config_identity=_CONFIG,
        generation=2,
        parent_manifest_root=first.root,
        segments=(*first.segments, _descriptor(tmp_path, 2)),
    )
    verify_raw_manifest_transition(first, second)
    assert second.total_record_count == 2
    assert second.total_logical_payload_bytes > first.total_logical_payload_bytes


def test_manifest_transition_rejects_fork_rewrite_and_wrong_parent(tmp_path: Path) -> None:
    descriptor = _descriptor(tmp_path, 1)
    parent = build_raw_manifest(
        store_id=_STORE,
        lake_id=_LAKE,
        config_identity=_CONFIG,
        generation=1,
        parent_manifest_root=None,
        segments=(descriptor,),
    )
    candidate = build_raw_manifest(
        store_id=_STORE,
        lake_id=_LAKE,
        config_identity=_CONFIG,
        generation=2,
        parent_manifest_root=parent.root,
        segments=(descriptor, _descriptor(tmp_path, 2)),
    )

    variants = (
        replace(candidate, parent_manifest_root=Hash32(b"\x99" * 32)),
        replace(candidate, segments=(replace(descriptor, physical_size=descriptor.physical_size + 1), candidate.segments[1])),
        replace(candidate, generation=3),
    )
    for variant in variants:
        with pytest.raises(RawManifestError) as caught:
            verify_raw_manifest_transition(parent, variant)
        assert caught.value.code is RawManifestErrorCode.TRANSITION


def test_manifest_reader_rejects_trailing_truncated_and_noncanonical_bytes(
    tmp_path: Path,
) -> None:
    manifest = build_raw_manifest(
        store_id=_STORE,
        lake_id=_LAKE,
        config_identity=_CONFIG,
        generation=1,
        parent_manifest_root=None,
        segments=(_descriptor(tmp_path, 1),),
    )
    encoded = raw_manifest_to_bytes(manifest)

    for damaged in (encoded[:-1], encoded + b"x", encoded.replace(b'"generation":1', b'"generation":01')):
        with pytest.raises(RawManifestError):
            raw_manifest_from_bytes(damaged)

