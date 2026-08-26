from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from hyperlab.paper.storage_v4.canonical import canonical_json_bytes
from hyperlab.paper.storage_v4.contracts import (
    RAW_REFERENCE_CONTRACT_MARKER,
    RawLakeId,
    RawReferenceError,
    RawReferenceRegistrationError,
    RawReferenceResolutionError,
    RawSegmentReference,
    native_reference_row,
    raw_reference_from_row,
)
from hyperlab.paper.storage_v4.raw_reference import (
    RAW_REFERENCE_CONTRACT_MARKER_V2,
    RAW_REFERENCE_FORMAT_VERSION_V2,
    RAW_REFERENCE_RAW_CODEC_ID,
    RAW_REFERENCE_RAW_CODEC_VERSION,
    DeterministicRawLakeV2Emulator,
    RawReferenceResolverV2,
    RawSegmentRef,
    RawSegmentReferenceV2,
    native_reference_v2_row,
    raw_reference_v2_from_row,
    resolve_native_reference_v2_row,
    verify_and_resolve_raw_reference_v2,
)
from hyperlab.paper.storage_v4.types import (
    UINT64_MAX,
    CommitOrdinal,
    EventSequence,
    Hash32,
    LogicalRow,
    SegmentIdentity,
    StoreId,
    StreamId,
)


def _sha256(value: bytes) -> Hash32:
    return Hash32(hashlib.sha256(value).digest())


def _reference(
    segment: bytes,
    *,
    offset: int,
    length: int,
    codec_id: str = RAW_REFERENCE_RAW_CODEC_ID,
    codec_version: str = RAW_REFERENCE_RAW_CODEC_VERSION,
) -> RawSegmentRef:
    payload = segment[offset : offset + length]
    return RawSegmentRef(
        raw_store_id=StoreId("synthetic-public-raw-store"),
        lake_id=RawLakeId("synthetic-public-lake"),
        source_id="hyperliquid-public",
        venue_id="hyperliquid",
        segment_identity=SegmentIdentity(_sha256(b"segment-identity")),
        segment_root=_sha256(b"segment-root"),
        raw_manifest_root=_sha256(b"raw-manifest-root"),
        physical_sha256=_sha256(segment),
        record_id="record-40-42",
        byte_offset=offset,
        stored_length=length,
        stored_sha256=_sha256(payload),
        logical_payload_length=length,
        logical_payload_sha256=_sha256(payload),
        input_type="PUBLIC_MARKET_DATA",
        source_stream_id=StreamId("public-market-wire"),
        source_first_sequence=EventSequence(40),
        source_last_sequence=EventSequence(42),
        arrival_sequence=EventSequence(99),
        source_timestamp="2026-08-25T12:00:00.000000Z",
        received_timestamp="2026-08-25T12:00:00.000001Z",
        codec_id=codec_id,
        codec_version=codec_version,
    )


def test_raw_segment_reference_v2_has_one_exact_roundtrip_representation() -> None:
    segment = b"header|payload|footer"
    reference = _reference(segment, offset=7, length=7)
    row = native_reference_v2_row(
        reference,
        StreamId("paper_inputs"),
        CommitOrdinal(1),
    )

    assert RawSegmentRef is RawSegmentReferenceV2
    assert reference.byte_end == 14
    assert row.value == {
        "arrival_sequence": 99,
        "byte_offset": 7,
        "codec_id": "raw",
        "codec_version": "1",
        "contract": RAW_REFERENCE_CONTRACT_MARKER_V2,
        "format_version": RAW_REFERENCE_FORMAT_VERSION_V2,
        "input_type": "PUBLIC_MARKET_DATA",
        "lake_id": "synthetic-public-lake",
        "logical_payload_length": 7,
        "logical_payload_sha256": hashlib.sha256(b"payload").hexdigest(),
        "mode": "V4_NATIVE",
        "physical_sha256": hashlib.sha256(segment).hexdigest(),
        "raw_manifest_root": hashlib.sha256(b"raw-manifest-root").hexdigest(),
        "raw_store_id": "synthetic-public-raw-store",
        "received_timestamp": "2026-08-25T12:00:00.000001Z",
        "record_id": "record-40-42",
        "segment_identity": reference.segment_identity.digest.hex(),
        "segment_root": hashlib.sha256(b"segment-root").hexdigest(),
        "source_first_sequence": 40,
        "source_id": "hyperliquid-public",
        "source_last_sequence": 42,
        "source_stream_id": "public-market-wire",
        "source_timestamp": "2026-08-25T12:00:00.000000Z",
        "stored_length": 7,
        "stored_sha256": hashlib.sha256(b"payload").hexdigest(),
        "venue_id": "hyperliquid",
    }
    assert canonical_json_bytes(row.value) == row.canonical_bytes
    assert raw_reference_v2_from_row(row) == reference
    assert reference.to_logical_row(StreamId("paper_inputs"), CommitOrdinal(1)) == row


def test_raw_segment_reference_v2_roundtrips_optional_metadata_as_null() -> None:
    reference = replace(
        _reference(b"payload", offset=0, length=7),
        venue_id=None,
        source_timestamp=None,
        received_timestamp=None,
    )
    row = reference.to_logical_row(StreamId("paper_inputs"), CommitOrdinal(0))

    assert row.value["venue_id"] is None
    assert row.value["source_timestamp"] is None
    assert row.value["received_timestamp"] is None
    assert raw_reference_v2_from_row(row) == reference


def test_existing_v1_reference_decode_remains_exact_and_unchanged() -> None:
    segment = b"header|payload|footer"
    payload = b"payload"
    reference = RawSegmentReference(
        lake_id=RawLakeId("synthetic-public-lake"),
        segment_identity=SegmentIdentity(_sha256(b"v1-segment-identity")),
        physical_sha256=_sha256(segment),
        byte_offset=7,
        byte_length=7,
        payload_sha256=_sha256(payload),
        stream_id=StreamId("public-market-wire"),
        source_first_sequence=EventSequence(40),
        source_last_sequence=EventSequence(42),
    )
    row = native_reference_row(reference, StreamId("paper_inputs"), CommitOrdinal(0))

    decoded = raw_reference_from_row(row)

    assert row.value["contract"] == RAW_REFERENCE_CONTRACT_MARKER
    assert "format_version" not in row.value
    assert type(decoded) is RawSegmentReference
    assert decoded == reference


def test_raw_segment_reference_v2_is_typed_and_range_checked() -> None:
    reference = _reference(b"payload", offset=0, length=7)

    with pytest.raises(TypeError, match="RawLakeId"):
        replace(reference, lake_id="lake")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="StoreId"):
        replace(reference, raw_store_id="store")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="exact integers"):
        replace(reference, stored_length=True)
    with pytest.raises(ValueError, match="positive"):
        replace(reference, logical_payload_length=0)
    with pytest.raises(ValueError, match="exceeds uint64"):
        replace(reference, byte_offset=UINT64_MAX, stored_length=1)
    with pytest.raises(ValueError, match="reversed"):
        replace(
            reference,
            source_first_sequence=EventSequence(43),
            source_last_sequence=EventSequence(42),
        )
    with pytest.raises(ValueError, match="non-empty"):
        replace(reference, source_id="")
    with pytest.raises(TypeError, match="venue_id"):
        replace(reference, venue_id=False)  # type: ignore[arg-type]


def test_v2_parser_rejects_malformed_ambiguous_or_extra_fields() -> None:
    reference = _reference(b"header|payload|footer", offset=7, length=7)
    value = reference.canonical_value()
    variants: list[dict[str, object]] = []

    for key, replacement in (
        ("contract", RAW_REFERENCE_CONTRACT_MARKER),
        ("format_version", 1),
        ("stored_length", True),
        ("venue_id", False),
        ("source_first_sequence", 43),
        ("codec_id", ""),
        ("segment_root", "A" * 64),
    ):
        variant: dict[str, object] = dict(value)
        variant[key] = replacement
        variants.append(variant)
    missing = dict(value)
    del missing["raw_manifest_root"]
    variants.append(missing)
    extra = dict(value)
    extra["physical_path"] = "untrusted/location"
    variants.append(extra)

    for variant in variants:
        row = LogicalRow(StreamId("paper_inputs"), CommitOrdinal(0), variant)
        with pytest.raises(RawReferenceError):
            raw_reference_v2_from_row(row)


def test_v2_emulator_resolves_raw_codec_and_authenticates_reference() -> None:
    segment = b"head\nPAYLOAD\ntail"
    payload = b"PAYLOAD"
    reference = _reference(
        segment,
        offset=segment.index(payload),
        length=len(payload),
    )
    emulator = DeterministicRawLakeV2Emulator()
    emulator.register_v2(reference, segment)
    row = native_reference_v2_row(
        reference,
        StreamId("paper_inputs"),
        CommitOrdinal(0),
    )

    assert isinstance(emulator, RawReferenceResolverV2)
    assert emulator.segment_count == 1
    assert emulator.resolve(reference) == payload
    assert verify_and_resolve_raw_reference_v2(reference, emulator) == payload
    assert resolve_native_reference_v2_row(row, emulator) == payload

    with pytest.raises(RawReferenceResolutionError, match="stored SHA-256"):
        emulator.resolve(replace(reference, stored_sha256=_sha256(b"wrong")))
    with pytest.raises(RawReferenceResolutionError, match="logical payload SHA-256"):
        emulator.resolve(
            replace(reference, logical_payload_sha256=_sha256(b"wrong"))
        )
    with pytest.raises(RawReferenceResolutionError, match="segment root"):
        emulator.resolve(replace(reference, segment_root=_sha256(b"wrong")))
    with pytest.raises(RawReferenceResolutionError, match="manifest root"):
        emulator.resolve(replace(reference, raw_manifest_root=_sha256(b"wrong")))


def test_v2_emulator_fails_closed_for_unsupported_codec() -> None:
    segment = b"head|compressed-looking-bytes|tail"
    reference = _reference(
        segment,
        offset=5,
        length=len(b"compressed-looking-bytes"),
        codec_id="zstd",
        codec_version="1",
    )

    with pytest.raises(RawReferenceRegistrationError, match="codec"):
        DeterministicRawLakeV2Emulator().register_v2(reference, segment)


class _WrongPayloadResolver:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def resolve(self, reference: RawSegmentRef) -> bytes:
        del reference
        return self._payload


def test_v2_verify_helper_independently_checks_logical_length_and_hash() -> None:
    reference = _reference(b"header|payload|footer", offset=7, length=7)

    with pytest.raises(RawReferenceResolutionError, match="logical payload length"):
        verify_and_resolve_raw_reference_v2(
            reference,
            _WrongPayloadResolver(b"short"),
        )
    with pytest.raises(RawReferenceResolutionError, match="logical payload SHA-256"):
        verify_and_resolve_raw_reference_v2(
            reference,
            _WrongPayloadResolver(b"PAYLOAD"),
        )
