from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from hyperlab.paper.storage_v4.canonical import CanonicalizationError, canonical_json_bytes
from hyperlab.paper.storage_v4.contracts import (
    COMPATIBILITY_CONTRACT_MARKER,
    RAW_REFERENCE_CONTRACT_MARKER,
    CompatibilityRecord,
    CompatibilityRecordError,
    DeterministicRawLakeEmulator,
    RawLakeId,
    RawReferenceError,
    RawReferenceRegistrationError,
    RawReferenceResolutionError,
    RawReferenceResolver,
    RawSegmentReference,
    StorageMode,
    compatibility_record_from_row,
    native_reference_row,
    raw_reference_from_row,
    rematerialize_compatibility_record,
    resolve_native_reference_row,
    verify_and_resolve_raw_reference,
)
from hyperlab.paper.storage_v4.types import (
    UINT64_MAX,
    CommitOrdinal,
    EventSequence,
    Hash32,
    LogicalRow,
    SegmentIdentity,
    StreamId,
)

SYNTHETIC_RAW_LAKE_EMULATOR = "SYNTHETIC_RAW_LAKE_EMULATOR"


def _sha256(value: bytes) -> Hash32:
    return Hash32(hashlib.sha256(value).digest())


def _segment_identity(byte: int = 1) -> SegmentIdentity:
    return SegmentIdentity(Hash32(bytes([byte]) * 32))


def _reference(
    segment: bytes,
    *,
    offset: int,
    length: int,
    identity: SegmentIdentity | None = None,
    lake_id: RawLakeId | None = None,
) -> RawSegmentReference:
    payload = segment[offset : offset + length]
    return RawSegmentReference(
        lake_id=lake_id or RawLakeId("synthetic-public-lake"),
        segment_identity=identity or _segment_identity(),
        physical_sha256=_sha256(segment),
        byte_offset=offset,
        byte_length=length,
        payload_sha256=_sha256(payload),
        stream_id=StreamId("public-market-wire"),
        source_first_sequence=EventSequence(40),
        source_last_sequence=EventSequence(42),
    )


def test_storage_modes_are_exact_and_deliberately_distinct() -> None:
    assert StorageMode.V3_COMPATIBILITY_IMPORT.value == "V3_COMPATIBILITY_IMPORT"
    assert StorageMode.V4_NATIVE.value == "V4_NATIVE"
    assert str(StorageMode.V3_COMPATIBILITY_IMPORT) == "V3_COMPATIBILITY_IMPORT"
    assert tuple(StorageMode) == (
        StorageMode.V3_COMPATIBILITY_IMPORT,
        StorageMode.V4_NATIVE,
    )


def test_compatibility_record_preserves_actual_finite_float_bytes_without_weakening_v4() -> None:
    golden_object = {
        "large": 1.2345678901234567e100,
        "negative_zero": -0.0,
        "smallest": 5e-324,
        "text": "café 漢字",
        "value": 1.25,
    }
    golden = json.dumps(
        golden_object,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    record = CompatibilityRecord.from_bytes(golden, lf_terminated=False)
    row = record.to_logical_row(StreamId("paper_events"), CommitOrdinal(3))

    assert record.canonical_json_bytes is golden
    assert record.canonical_json_text == golden.decode("utf-8")
    assert record.canonical_sha256 == _sha256(golden)
    assert row.value == {
        "canonical_json": golden.decode("utf-8"),
        "canonical_sha256": hashlib.sha256(golden).hexdigest(),
        "contract": COMPATIBILITY_CONTRACT_MARKER,
        "mode": "V3_COMPATIBILITY_IMPORT",
    }
    assert canonical_json_bytes(row.value) == row.canonical_bytes
    assert compatibility_record_from_row(row) == record
    assert rematerialize_compatibility_record(row) == golden + b"\n"

    with pytest.raises(CanonicalizationError, match="float"):
        canonical_json_bytes(golden_object)


def test_compatibility_record_requires_explicit_exact_lf_handling() -> None:
    canonical = b'{"a":1}'

    assert CompatibilityRecord.from_jsonl_bytes(canonical + b"\n").jsonl_bytes == (
        canonical + b"\n"
    )
    assert CompatibilityRecord.from_bytes(
        canonical + b"\n", lf_terminated=True
    ).canonical_json_bytes == canonical

    with pytest.raises(CompatibilityRecordError, match="missing"):
        CompatibilityRecord.from_bytes(canonical, lf_terminated=True)
    with pytest.raises(CompatibilityRecordError, match="explicitly"):
        CompatibilityRecord.from_bytes(canonical + b"\n", lf_terminated=False)
    with pytest.raises(CompatibilityRecordError):
        CompatibilityRecord.from_jsonl_bytes(canonical + b"\r\n")
    with pytest.raises(CompatibilityRecordError):
        CompatibilityRecord.from_jsonl_bytes(canonical + b"\n\n")
    with pytest.raises(TypeError, match="bool"):
        CompatibilityRecord.from_bytes(canonical, lf_terminated=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="exact bytes"):
        CompatibilityRecord.from_bytes(
            bytearray(canonical),  # type: ignore[arg-type]
            lf_terminated=True,
        )


@pytest.mark.parametrize(
    "invalid",
    [
        b'{"b":1,"a":2}',
        b'{"a": 1}',
        b'{"a":"\\u00e9"}',
        b'{"a":1} ',
        b'{"a":1,"a":1}',
        b'{"a":1}\n{"b":2}',
        b"[]",
        b"null",
        b"\xef\xbb\xbf{\"a\":1}",
        b'{"a":"\xff"}',
    ],
)
def test_compatibility_record_rejects_noncanonical_nonobject_or_non_utf8_bytes(
    invalid: bytes,
) -> None:
    with pytest.raises(CompatibilityRecordError):
        CompatibilityRecord(invalid)


@pytest.mark.parametrize(
    "invalid",
    [
        b'{"value":NaN}',
        b'{"value":Infinity}',
        b'{"value":-Infinity}',
    ],
)
def test_compatibility_record_rejects_nonfinite_golden_numbers(invalid: bytes) -> None:
    with pytest.raises(CompatibilityRecordError, match="cannot contain"):
        CompatibilityRecord(invalid)


def test_compatibility_unwrap_rejects_corrupt_or_noncanonical_envelopes() -> None:
    record = CompatibilityRecord(b'{"price":1.5}')
    envelope = record.envelope()
    variants: list[dict[str, object]] = []

    for key, value in (
        ("contract", "wrong-contract"),
        ("mode", StorageMode.V4_NATIVE.value),
        ("canonical_sha256", "0" * 64),
        ("canonical_sha256", record.canonical_sha256.hex().upper()),
        ("canonical_json", '{"price": 1.5}'),
    ):
        variant: dict[str, object] = dict(envelope)
        variant[key] = value
        variants.append(variant)
    missing = dict(envelope)
    del missing["canonical_sha256"]
    variants.append(missing)
    extra = dict(envelope)
    extra["unbound"] = "field"
    variants.append(extra)

    for variant in variants:
        row = LogicalRow(StreamId("paper_inputs"), CommitOrdinal(0), variant)
        with pytest.raises(CompatibilityRecordError):
            rematerialize_compatibility_record(row)


def test_raw_segment_reference_is_typed_immutable_and_range_checked() -> None:
    segment = b"0123456789"
    reference = _reference(segment, offset=2, length=4)

    assert reference.byte_end == 6
    assert reference.source_first_sequence == EventSequence(40)
    assert reference.source_last_sequence == EventSequence(42)

    with pytest.raises(TypeError, match="RawLakeId"):
        replace(reference, lake_id="lake")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="exact integers"):
        replace(reference, byte_offset=True)
    with pytest.raises(ValueError, match="byte_offset"):
        replace(reference, byte_offset=-1)
    with pytest.raises(ValueError, match="positive"):
        replace(reference, byte_length=0)
    with pytest.raises(ValueError, match="exceeds uint64"):
        replace(reference, byte_offset=UINT64_MAX, byte_length=1)
    with pytest.raises(ValueError, match="reversed"):
        replace(
            reference,
            source_first_sequence=EventSequence(43),
            source_last_sequence=EventSequence(42),
        )


def test_native_reference_has_one_strict_canonical_row_representation() -> None:
    segment = b"header|payload|footer"
    reference = _reference(segment, offset=7, length=7)
    row = native_reference_row(reference, StreamId("paper_inputs"), CommitOrdinal(0))

    assert row.value == {
        "byte_length": 7,
        "byte_offset": 7,
        "contract": RAW_REFERENCE_CONTRACT_MARKER,
        "lake_id": "synthetic-public-lake",
        "mode": "V4_NATIVE",
        "payload_sha256": hashlib.sha256(b"payload").hexdigest(),
        "physical_sha256": hashlib.sha256(segment).hexdigest(),
        "segment_identity": reference.segment_identity.digest.hex(),
        "source_first_sequence": 40,
        "source_last_sequence": 42,
        "stream_id": "public-market-wire",
    }
    assert canonical_json_bytes(row.value) == row.canonical_bytes
    assert raw_reference_from_row(row) == reference
    assert reference.to_logical_row(StreamId("paper_inputs"), CommitOrdinal(0)) == row


def test_raw_lake_emulator_resolves_exact_bytes_and_helpers_reverify_payload() -> None:
    assert SYNTHETIC_RAW_LAKE_EMULATOR.startswith("SYNTHETIC_")
    segment = b"head\nPAYLOAD\ntail"
    payload = b"PAYLOAD"
    offset = segment.index(payload)
    reference = _reference(segment, offset=offset, length=len(payload))
    emulator = DeterministicRawLakeEmulator()

    physical_hash = emulator.register(
        reference.lake_id,
        reference.segment_identity,
        segment,
    )
    row = native_reference_row(reference, StreamId("paper_inputs"), CommitOrdinal(0))

    assert isinstance(emulator, RawReferenceResolver)
    assert physical_hash == reference.physical_sha256
    assert emulator.segment_count == 1
    assert emulator.resolve(reference) == payload
    assert verify_and_resolve_raw_reference(reference, emulator) == payload
    assert resolve_native_reference_row(row, emulator) == payload


def test_raw_lake_emulator_fails_closed_on_missing_range_physical_or_payload_mismatch() -> None:
    segment = b"head|payload|tail"
    reference = _reference(segment, offset=5, length=7)
    emulator = DeterministicRawLakeEmulator()
    emulator.register_segment(reference.lake_id, reference.segment_identity, segment)

    variants = (
        replace(reference, segment_identity=_segment_identity(9)),
        replace(reference, byte_offset=len(segment) - 1, byte_length=4),
        replace(reference, physical_sha256=Hash32(bytes([8]) * 32)),
        replace(reference, payload_sha256=Hash32(bytes([7]) * 32)),
    )
    expected = ("missing", "exceeds segment size", "physical", "payload")

    for variant, message in zip(variants, expected, strict=True):
        with pytest.raises(RawReferenceResolutionError, match=message):
            emulator.resolve(variant)


def test_raw_lake_emulator_rejects_duplicate_conflict_and_ambiguous_alias() -> None:
    lake_id = RawLakeId("synthetic-public-lake")
    identity = _segment_identity()
    segment = b"immutable-segment"
    emulator = DeterministicRawLakeEmulator()
    emulator.register(lake_id, identity, segment)

    with pytest.raises(RawReferenceRegistrationError, match="duplicate"):
        emulator.register(lake_id, identity, segment)
    with pytest.raises(RawReferenceRegistrationError, match="conflicting"):
        emulator.register(lake_id, identity, b"changed-segment")
    with pytest.raises(RawReferenceRegistrationError, match="ambiguous"):
        emulator.register(lake_id, _segment_identity(2), segment)
    with pytest.raises(RawReferenceRegistrationError, match="empty"):
        DeterministicRawLakeEmulator().register(lake_id, identity, b"")
    with pytest.raises(TypeError, match="exact bytes"):
        DeterministicRawLakeEmulator().register(
            lake_id,
            identity,
            bytearray(segment),  # type: ignore[arg-type]
        )


def test_native_reference_parser_rejects_corrupt_ambiguous_or_extra_fields() -> None:
    reference = _reference(b"header|payload|footer", offset=7, length=7)
    value = reference.canonical_value()
    variants: list[dict[str, object]] = []

    for key, replacement in (
        ("contract", "wrong-contract"),
        ("mode", StorageMode.V3_COMPATIBILITY_IMPORT.value),
        ("segment_identity", "A" * 64),
        ("physical_sha256", "0" * 63),
        ("byte_length", True),
        ("source_first_sequence", 43),
    ):
        variant: dict[str, object] = dict(value)
        variant[key] = replacement
        variants.append(variant)
    missing = dict(value)
    del missing["payload_sha256"]
    variants.append(missing)
    extra = dict(value)
    extra["physical_path"] = "untrusted/location"
    variants.append(extra)

    for variant in variants:
        row = LogicalRow(StreamId("paper_inputs"), CommitOrdinal(0), variant)
        with pytest.raises(RawReferenceError):
            raw_reference_from_row(row)


class _WrongPayloadResolver:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def resolve(self, reference: RawSegmentReference) -> bytes:
        del reference
        return self._payload


def test_verify_helper_does_not_blindly_trust_a_protocol_implementation() -> None:
    segment = b"header|payload|footer"
    reference = _reference(segment, offset=7, length=7)

    with pytest.raises(RawReferenceResolutionError, match="length"):
        verify_and_resolve_raw_reference(reference, _WrongPayloadResolver(b"short"))
    with pytest.raises(RawReferenceResolutionError, match="SHA-256"):
        verify_and_resolve_raw_reference(reference, _WrongPayloadResolver(b"PAYLOAD"))
