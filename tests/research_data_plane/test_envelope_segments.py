from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import hyperlab.research_data.segments as segments_module
from hyperlab.research_data.canonical import CanonicalDataError, canonical_json_bytes
from hyperlab.research_data.derived import paper_references
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    PublicDataEnvelope,
    SessionEnvelopeFactory,
    Venue,
)
from hyperlab.research_data.segments import (
    MANIFEST_SUFFIX,
    SEGMENT_SUFFIX,
    ResearchDataCapacityError,
    ResearchDataIntegrityError,
    ResearchSegmentReader,
    ResearchSegmentWriter,
    UnsafeAuthorityPathError,
    WriterAlreadyActiveError,
    build_segment,
    decode_segment,
)


class SimulatedCrash(RuntimeError):
    pass


def _provenance(collection_id: str = "fixture-collection") -> CaptureProvenance:
    return CaptureProvenance(
        collection_id=collection_id,
        source_url="fixture://research-data-plane-v1",
        transport="FIXTURE",
        fixture_label=SYNTHETIC_FIXTURE_LABEL,
    )


def _factory(collection_id: str = "fixture-collection") -> SessionEnvelopeFactory:
    return SessionEnvelopeFactory(
        venue=Venue.HYPERLIQUID,
        collector_identity="fixture-collector-v1",
        session_identity="fixture-session",
        source_metadata_version="fixture-metadata-v1",
        provenance=_provenance(collection_id),
    )


def _envelopes(count: int = 3, *, collection_id: str = "fixture-collection") -> tuple[PublicDataEnvelope, ...]:
    factory = _factory(collection_id)
    values: list[PublicDataEnvelope] = []
    for index in range(count):
        raw = canonical_json_bytes(
            {
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                "index": index,
                "price": f"{100 + index}.0000",
            }
        )
        values.append(
            factory.make(
                feed_type="bbo",
                instrument_id="HL:BTC:perp",
                market_id=None,
                source_timestamp_ns=1_700_000_000_000_000_000 + index,
                receive_timestamp_utc_ns=1_700_000_000_100_000_000 + index,
                receive_monotonic_ns=10_000 + index,
                raw_payload=raw,
                source_sequence=None,
                source_event_id=f"fixture-event-{index}",
            )
        )
    return tuple(values)


def _writer(root: Path, *, fault=None, rotation_seconds: float = 60.0) -> ResearchSegmentWriter:
    return ResearchSegmentWriter(
        root,
        collection_id="fixture-collection",
        max_segment_bytes=1_000_000,
        rotation_seconds=rotation_seconds,
        max_total_bytes=10_000_000,
        fault_injector=fault,
    )


def test_canonicalization_and_envelope_round_trip_are_byte_stable() -> None:
    assert canonical_json_bytes({"b": "2", "a": 1}) == b'{"a":1,"b":"2"}'
    with pytest.raises(CanonicalDataError, match="binary floats"):
        canonical_json_bytes({"price": 0.5})

    envelope = _envelopes(1)[0]
    encoded = envelope.canonical_bytes()
    decoded = PublicDataEnvelope.from_canonical_bytes(encoded)

    assert decoded == envelope
    assert decoded.raw_payload == envelope.raw_payload
    assert decoded.canonical_bytes() == encoded
    assert decoded.content_sha256 == hashlib.sha256(envelope.raw_payload).hexdigest()
    assert decoded.source_sequence is None

    opaque_cursor = _factory().make(
        feed_type="bbo",
        instrument_id="HL:BTC:perp",
        market_id=None,
        source_timestamp_ns=None,
        receive_timestamp_utc_ns=1,
        receive_monotonic_ns=1,
        raw_payload=b'{"fixture_label":"SYNTHETIC/FIXTURE"}',
        source_cursor="cursor/value==",
    )
    assert PublicDataEnvelope.from_canonical_bytes(
        opaque_cursor.canonical_bytes()
    ).source_cursor == "cursor/value=="

    malformed = json.loads(encoded)
    malformed["state"]["duplicate"] = "false"
    with pytest.raises(ValueError, match="must be a boolean"):
        PublicDataEnvelope.from_canonical_bytes(canonical_json_bytes(malformed))


def test_arrival_gap_duplicate_reconnect_and_absent_source_sequence_are_explicit() -> None:
    factory = _factory()
    common = {
        "feed_type": "test_feed",
        "instrument_id": "HL:BTC:perp",
        "market_id": None,
        "source_timestamp_ns": 10,
        "receive_timestamp_utc_ns": 20,
        "raw_payload": b'{"fixture_label":"SYNTHETIC/FIXTURE"}',
    }
    first = factory.make(
        **common,
        receive_monotonic_ns=100,
        source_sequence=1,
        source_event_id="one",
    )
    gap = factory.make(
        **common,
        receive_monotonic_ns=101,
        source_sequence=3,
        source_event_id="three",
    )
    duplicate = factory.make(
        **common,
        receive_monotonic_ns=102,
        source_sequence=3,
        source_event_id="three",
    )
    factory.begin_reconnect()
    reconnect = factory.make(
        **common,
        receive_monotonic_ns=103,
        source_sequence=None,
        source_event_id="after-reconnect",
    )

    assert [item.arrival_sequence for item in (first, gap, duplicate, reconnect)] == [1, 2, 3, 4]
    assert not first.state.gap_detected
    assert gap.state.gap_detected and gap.state.reason == "SOURCE_SEQUENCE_GAP"
    assert duplicate.state.duplicate and duplicate.state.reason == "DUPLICATE_SOURCE_EVENT"
    assert reconnect.state.reconnect and reconnect.source_sequence is None
    assert reconnect.session_identity.endswith(":1")


def test_segment_round_trip_rotation_and_replay_are_deterministic(tmp_path: Path) -> None:
    envelopes = _envelopes(3)
    artifact = build_segment(
        envelopes,
        segment_index=0,
        previous_segment_sha256=None,
        collection_id="fixture-collection",
    )
    assert decode_segment(
        artifact.physical_bytes,
        expected_physical_sha256=artifact.descriptor.physical_sha256,
    ).envelopes == envelopes

    manifests = []
    for name in ("first", "second"):
        root = tmp_path / name
        writer = _writer(root, rotation_seconds=0.000000001)
        for envelope in envelopes:
            writer.append(envelope)
        manifest = writer.close()
        assert manifest is not None
        manifests.append(manifest)
        assert ResearchSegmentReader(root, manifest_sha256=manifest.manifest_sha256).replay() == envelopes
        assert len(manifest.segments) == 3

    assert manifests[0].manifest_sha256 == manifests[1].manifest_sha256
    assert [item.physical_sha256 for item in manifests[0].segments] == [
        item.physical_sha256 for item in manifests[1].segments
    ]


@pytest.mark.parametrize("mutation", ["truncate", "corrupt"])
def test_truncation_and_corruption_are_refused(tmp_path: Path, mutation: str) -> None:
    artifact = build_segment(
        _envelopes(1),
        segment_index=0,
        previous_segment_sha256=None,
        collection_id="fixture-collection",
    )
    damaged = bytearray(artifact.physical_bytes)
    if mutation == "truncate":
        damaged = damaged[:-7]
    else:
        damaged[len(damaged) // 2] ^= 0x01
    with pytest.raises(ResearchDataIntegrityError):
        decode_segment(bytes(damaged))


def test_missing_manifest_and_segment_are_refused(tmp_path: Path) -> None:
    root = tmp_path / "missing"
    writer = _writer(root)
    writer.append(_envelopes(1)[0])
    manifest = writer.close()
    assert manifest is not None
    manifest_path = root / "manifests" / f"{manifest.manifest_sha256}{MANIFEST_SUFFIX}"
    segment = manifest.segments[0]
    segment_path = root / "segments" / f"{segment.physical_sha256}{SEGMENT_SUFFIX}"

    manifest_bytes = manifest_path.read_bytes()
    manifest_path.unlink()
    with pytest.raises(ResearchDataIntegrityError, match="manifest is missing"):
        ResearchSegmentReader(root, manifest_sha256=manifest.manifest_sha256)
    manifest_path.write_bytes(manifest_bytes)
    segment_path.unlink()
    with pytest.raises(ResearchDataIntegrityError, match="segment is missing"):
        ResearchSegmentReader(root, manifest_sha256=manifest.manifest_sha256)


def test_recovery_before_segment_publication_discards_only_staging_tmp(tmp_path: Path) -> None:
    root = tmp_path / "before"

    def fault(point: str) -> None:
        if point == "before_segment_publish":
            raise SimulatedCrash(point)

    writer = _writer(root, fault=fault)
    writer.append(_envelopes(1)[0])
    with pytest.raises(SimulatedCrash):
        writer.flush()
    writer.abort()
    recovered = _writer(root)
    assert recovered.frame_count == 0
    assert list((root / "staging").iterdir()) == []
    assert recovered.close() is None


@pytest.mark.parametrize("fault_point", ["after_segment_publish", "after_manifest_publish"])
def test_recovery_after_atomic_publication_is_exact(tmp_path: Path, fault_point: str) -> None:
    root = tmp_path / fault_point
    envelope = _envelopes(1)[0]

    def fault(point: str) -> None:
        if point == fault_point:
            raise SimulatedCrash(point)

    writer = _writer(root, fault=fault)
    writer.append(envelope)
    with pytest.raises(SimulatedCrash):
        writer.flush()
    writer.abort()

    recovered = _writer(root)
    manifest = recovered.close()
    assert manifest is not None
    assert ResearchSegmentReader(root, manifest_sha256=manifest.manifest_sha256).replay() == (
        envelope,
    )


def test_second_writer_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "lease"
    first = _writer(root)
    try:
        with pytest.raises(WriterAlreadyActiveError):
            _writer(root)
    finally:
        first.abort()


def test_authoritative_reparse_or_symlink_path_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "unsafe"
    original = segments_module._is_reparse

    def marked(path: Path) -> bool:
        return path == root.absolute() or original(path)

    monkeypatch.setattr(segments_module, "_is_reparse", marked)
    with pytest.raises(UnsafeAuthorityPathError):
        _writer(root)


def test_raw_derived_and_paper_boundaries_do_not_commit_per_tick(tmp_path: Path) -> None:
    root = tmp_path / "paper-boundary"
    envelopes = _envelopes(25)
    writer = _writer(root)
    for envelope in envelopes:
        writer.append(envelope)
    manifest = writer.close()
    assert manifest is not None
    references = paper_references(manifest)

    assert len(manifest.segments) == 1
    assert len(references) == 1
    assert references[0].frame_count == 25
    assert not hasattr(references[0], "raw_payload")
    source = Path(segments_module.__file__).read_text(encoding="utf-8")
    assert "hyperlab.paper" not in source


def test_oversize_single_frame_is_refused_before_admission(tmp_path: Path) -> None:
    envelope = _envelopes(1)[0]
    writer = ResearchSegmentWriter(
        tmp_path / "strict-capacity",
        collection_id="fixture-collection",
        max_segment_bytes=128,
        rotation_seconds=60,
        max_total_bytes=10_000_000,
    )
    with pytest.raises(ResearchDataCapacityError, match="exceeds max_segment_bytes"):
        writer.append(envelope)
    assert writer.frame_count == 0
    assert writer.close() is None
