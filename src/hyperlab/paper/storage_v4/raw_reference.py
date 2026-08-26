"""Canonical authenticated V4_NATIVE raw-reference contract, format V2.

The V1 contract remains implemented in :mod:`contracts` without modification.
This module gives Phase 1C a separate, strict V2 envelope that distinguishes
physical stored bytes from the logical payload returned to replay consumers.

The deterministic emulator is an in-memory contract test double.  It supports
only the identity ``raw`` codec and does not prove filesystem durability,
external manifest authority, or resistance to a compromised administrator.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .contracts import (
    RawLakeId,
    RawReferenceError,
    RawReferenceRegistrationError,
    RawReferenceResolutionError,
    StorageMode,
)
from .types import (
    UINT32_MAX,
    UINT64_MAX,
    CanonicalObject,
    CommitOrdinal,
    EventSequence,
    Hash32,
    LogicalRow,
    SegmentIdentity,
    StoreId,
    StreamId,
)

RAW_REFERENCE_FORMAT_VERSION_V2 = 2
RAW_REFERENCE_CONTRACT_MARKER_V2 = "hyperlab.storage_v4.raw_segment_reference.v2"
RAW_REFERENCE_RAW_CODEC_ID = "raw"
RAW_REFERENCE_RAW_CODEC_VERSION = "1"

_RAW_REFERENCE_V2_KEYS = frozenset(
    {
        "arrival_sequence",
        "byte_offset",
        "codec_id",
        "codec_version",
        "contract",
        "format_version",
        "input_type",
        "lake_id",
        "logical_payload_length",
        "logical_payload_sha256",
        "mode",
        "physical_sha256",
        "raw_manifest_root",
        "raw_store_id",
        "received_timestamp",
        "record_id",
        "segment_identity",
        "segment_root",
        "source_first_sequence",
        "source_id",
        "source_last_sequence",
        "source_stream_id",
        "source_timestamp",
        "stored_length",
        "stored_sha256",
        "venue_id",
    }
)


def _validate_text(value: object, *, label: str, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if type(value) is not str:
        suffix = " or null" if optional else ""
        raise TypeError(f"raw reference {label} must be exact text{suffix}")
    if not value:
        raise ValueError(f"raw reference {label} must be non-empty")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise ValueError(f"raw reference {label} must be strict UTF-8 text") from error
    if len(encoded) > UINT32_MAX:
        raise ValueError(f"raw reference {label} exceeds uint32 UTF-8 framing")
    return value


@dataclass(frozen=True, slots=True)
class RawSegmentReferenceV2:
    """Canonical raw record locator with physical and logical authentication."""

    raw_store_id: StoreId
    lake_id: RawLakeId
    source_id: str
    venue_id: str | None
    segment_identity: SegmentIdentity
    segment_root: Hash32
    raw_manifest_root: Hash32
    physical_sha256: Hash32
    record_id: str
    byte_offset: int
    stored_length: int
    stored_sha256: Hash32
    logical_payload_length: int
    logical_payload_sha256: Hash32
    input_type: str
    source_stream_id: StreamId
    source_first_sequence: EventSequence
    source_last_sequence: EventSequence
    arrival_sequence: EventSequence
    source_timestamp: str | None
    received_timestamp: str | None
    codec_id: str
    codec_version: str

    def __post_init__(self) -> None:
        if type(self.raw_store_id) is not StoreId or type(self.lake_id) is not RawLakeId:
            raise TypeError("raw reference store/lake identity must use StoreId and RawLakeId")
        if type(self.segment_identity) is not SegmentIdentity:
            raise TypeError("raw reference segment_identity must be SegmentIdentity")
        for label, value in (
            ("segment_root", self.segment_root),
            ("raw_manifest_root", self.raw_manifest_root),
            ("physical_sha256", self.physical_sha256),
            ("stored_sha256", self.stored_sha256),
            ("logical_payload_sha256", self.logical_payload_sha256),
        ):
            if type(value) is not Hash32:
                raise TypeError(f"raw reference {label} must be Hash32")
        if type(self.byte_offset) is not int or type(self.stored_length) is not int:
            raise TypeError("raw reference byte offset and stored length must be exact integers")
        if type(self.logical_payload_length) is not int:
            raise TypeError("raw reference logical payload length must be an exact integer")
        if self.byte_offset < 0 or self.byte_offset > UINT64_MAX:
            raise ValueError("raw reference byte_offset is outside uint64")
        if self.stored_length < 1 or self.stored_length > UINT64_MAX:
            raise ValueError("raw reference stored_length must be positive and within uint64")
        if self.logical_payload_length < 1 or self.logical_payload_length > UINT64_MAX:
            raise ValueError(
                "raw reference logical_payload_length must be positive and within uint64"
            )
        if self.byte_offset > UINT64_MAX - self.stored_length:
            raise ValueError("raw reference stored byte interval exceeds uint64")
        if type(self.source_stream_id) is not StreamId:
            raise TypeError("raw reference source_stream_id must be StreamId")
        if (
            type(self.source_first_sequence) is not EventSequence
            or type(self.source_last_sequence) is not EventSequence
            or type(self.arrival_sequence) is not EventSequence
        ):
            raise TypeError("raw reference source and arrival sequences must use EventSequence")
        if int(self.source_first_sequence) > int(self.source_last_sequence):
            raise ValueError("raw reference source range is reversed")

        _validate_text(self.source_id, label="source_id")
        _validate_text(self.venue_id, label="venue_id", optional=True)
        _validate_text(self.record_id, label="record_id")
        _validate_text(self.input_type, label="input_type")
        _validate_text(self.source_timestamp, label="source_timestamp", optional=True)
        _validate_text(self.received_timestamp, label="received_timestamp", optional=True)
        _validate_text(self.codec_id, label="codec_id")
        _validate_text(self.codec_version, label="codec_version")

    @property
    def format_version(self) -> int:
        return RAW_REFERENCE_FORMAT_VERSION_V2

    @property
    def byte_end(self) -> int:
        """Return the exclusive stored-byte endpoint checked against uint64."""

        return self.byte_offset + self.stored_length

    def canonical_value(self) -> CanonicalObject:
        return {
            "arrival_sequence": int(self.arrival_sequence),
            "byte_offset": self.byte_offset,
            "codec_id": self.codec_id,
            "codec_version": self.codec_version,
            "contract": RAW_REFERENCE_CONTRACT_MARKER_V2,
            "format_version": RAW_REFERENCE_FORMAT_VERSION_V2,
            "input_type": self.input_type,
            "lake_id": self.lake_id.value,
            "logical_payload_length": self.logical_payload_length,
            "logical_payload_sha256": self.logical_payload_sha256.hex(),
            "mode": StorageMode.V4_NATIVE.value,
            "physical_sha256": self.physical_sha256.hex(),
            "raw_manifest_root": self.raw_manifest_root.hex(),
            "raw_store_id": self.raw_store_id.value,
            "received_timestamp": self.received_timestamp,
            "record_id": self.record_id,
            "segment_identity": self.segment_identity.digest.hex(),
            "segment_root": self.segment_root.hex(),
            "source_first_sequence": int(self.source_first_sequence),
            "source_id": self.source_id,
            "source_last_sequence": int(self.source_last_sequence),
            "source_stream_id": self.source_stream_id.value,
            "source_timestamp": self.source_timestamp,
            "stored_length": self.stored_length,
            "stored_sha256": self.stored_sha256.hex(),
            "venue_id": self.venue_id,
        }

    def to_logical_row(self, stream_id: StreamId, ordinal: CommitOrdinal) -> LogicalRow:
        return native_reference_v2_row(self, stream_id, ordinal)


RawSegmentRef = RawSegmentReferenceV2


def native_reference_v2_row(
    reference: RawSegmentRef,
    stream_id: StreamId,
    ordinal: CommitOrdinal,
) -> LogicalRow:
    """Represent one V2 raw reference as an exact canonical logical row."""

    if type(reference) is not RawSegmentReferenceV2:
        raise TypeError("native_reference_v2_row reference must be RawSegmentRef")
    return LogicalRow(stream_id=stream_id, ordinal=ordinal, value=reference.canonical_value())


def _require_exact_object(value: object) -> dict[str, object]:
    if type(value) is not dict:
        raise RawReferenceError("V2 native raw reference must be a canonical JSON object")
    typed_value: dict[str, object] = value
    actual_keys = frozenset(typed_value)
    if actual_keys != _RAW_REFERENCE_V2_KEYS:
        missing = sorted(_RAW_REFERENCE_V2_KEYS - actual_keys)
        extra = sorted(actual_keys - _RAW_REFERENCE_V2_KEYS)
        raise RawReferenceError(
            "V2 native raw reference keys differ from the contract; "
            f"missing={missing!r}, extra={extra!r}"
        )
    return typed_value


def _required_text(value: object, *, label: str) -> str:
    if type(value) is not str:
        raise RawReferenceError(f"{label} must be exact text")
    return value


def _optional_text(value: object, *, label: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, label=label)


def _required_integer(value: object, *, label: str) -> int:
    if type(value) is not int:
        raise RawReferenceError(f"{label} must be an exact integer, not bool")
    return value


def _required_hash(value: object, *, label: str) -> Hash32:
    try:
        return Hash32.from_hex(_required_text(value, label=label))
    except (RawReferenceError, TypeError, ValueError) as error:
        raise RawReferenceError(f"{label} must be lowercase SHA-256") from error


def raw_reference_v2_from_row(row: LogicalRow) -> RawSegmentRef:
    """Parse one exact V2 envelope without weakening the separate V1 decoder."""

    if type(row) is not LogicalRow:
        raise TypeError("raw_reference_v2_from_row requires LogicalRow")
    try:
        value = _require_exact_object(row.value)
        if _required_text(value["contract"], label="raw reference contract") != (
            RAW_REFERENCE_CONTRACT_MARKER_V2
        ):
            raise RawReferenceError("V2 native raw reference contract marker is invalid")
        if _required_integer(value["format_version"], label="format_version") != (
            RAW_REFERENCE_FORMAT_VERSION_V2
        ):
            raise RawReferenceError("V2 native raw reference format version is invalid")
        if _required_text(value["mode"], label="raw reference mode") != (
            StorageMode.V4_NATIVE.value
        ):
            raise RawReferenceError("V2 native raw reference mode is invalid")
        return RawSegmentReferenceV2(
            raw_store_id=StoreId(
                _required_text(value["raw_store_id"], label="raw_store_id")
            ),
            lake_id=RawLakeId(_required_text(value["lake_id"], label="lake_id")),
            source_id=_required_text(value["source_id"], label="source_id"),
            venue_id=_optional_text(value["venue_id"], label="venue_id"),
            segment_identity=SegmentIdentity(
                _required_hash(value["segment_identity"], label="segment_identity")
            ),
            segment_root=_required_hash(value["segment_root"], label="segment_root"),
            raw_manifest_root=_required_hash(
                value["raw_manifest_root"],
                label="raw_manifest_root",
            ),
            physical_sha256=_required_hash(
                value["physical_sha256"],
                label="physical_sha256",
            ),
            record_id=_required_text(value["record_id"], label="record_id"),
            byte_offset=_required_integer(value["byte_offset"], label="byte_offset"),
            stored_length=_required_integer(value["stored_length"], label="stored_length"),
            stored_sha256=_required_hash(value["stored_sha256"], label="stored_sha256"),
            logical_payload_length=_required_integer(
                value["logical_payload_length"],
                label="logical_payload_length",
            ),
            logical_payload_sha256=_required_hash(
                value["logical_payload_sha256"],
                label="logical_payload_sha256",
            ),
            input_type=_required_text(value["input_type"], label="input_type"),
            source_stream_id=StreamId(
                _required_text(value["source_stream_id"], label="source_stream_id")
            ),
            source_first_sequence=EventSequence(
                _required_integer(
                    value["source_first_sequence"],
                    label="source_first_sequence",
                )
            ),
            source_last_sequence=EventSequence(
                _required_integer(
                    value["source_last_sequence"],
                    label="source_last_sequence",
                )
            ),
            arrival_sequence=EventSequence(
                _required_integer(value["arrival_sequence"], label="arrival_sequence")
            ),
            source_timestamp=_optional_text(
                value["source_timestamp"],
                label="source_timestamp",
            ),
            received_timestamp=_optional_text(
                value["received_timestamp"],
                label="received_timestamp",
            ),
            codec_id=_required_text(value["codec_id"], label="codec_id"),
            codec_version=_required_text(value["codec_version"], label="codec_version"),
        )
    except RawReferenceError:
        raise
    except (TypeError, UnicodeError, ValueError) as error:
        raise RawReferenceError("V2 native raw reference is malformed") from error


@runtime_checkable
class RawReferenceResolverV2(Protocol):
    """Resolve one V2 reference to its exact logical payload bytes."""

    def resolve(self, reference: RawSegmentRef) -> bytes:
        """Return logical bytes or raise ``RawReferenceResolutionError``."""


@dataclass(frozen=True, slots=True)
class _RegisteredRawSegmentV2:
    physical_bytes: bytes
    physical_sha256: Hash32
    segment_root: Hash32
    raw_manifest_root: Hash32


class DeterministicRawLakeV2Emulator:
    """One-shot in-memory V2 resolver supporting only ``raw`` codec V1."""

    def __init__(self) -> None:
        self._segments: dict[
            tuple[StoreId, RawLakeId, SegmentIdentity],
            _RegisteredRawSegmentV2,
        ] = {}
        self._physical_owners: dict[
            tuple[StoreId, RawLakeId, Hash32], SegmentIdentity
        ] = {}

    @property
    def segment_count(self) -> int:
        return len(self._segments)

    @staticmethod
    def _supports_codec(reference: RawSegmentRef) -> bool:
        return (
            reference.codec_id == RAW_REFERENCE_RAW_CODEC_ID
            and reference.codec_version == RAW_REFERENCE_RAW_CODEC_VERSION
        )

    def register_v2(self, reference: RawSegmentRef, physical_bytes: bytes) -> Hash32:
        """Register and authenticate one immutable physical segment exactly once."""

        if type(reference) is not RawSegmentReferenceV2:
            raise TypeError("raw V2 emulator register_v2 requires RawSegmentRef")
        if not self._supports_codec(reference):
            raise RawReferenceRegistrationError("raw V2 emulator codec is unsupported")
        if type(physical_bytes) is not bytes:
            raise TypeError("raw V2 emulator physical_bytes must be exact bytes")
        if not physical_bytes:
            raise RawReferenceRegistrationError("raw V2 emulator segment cannot be empty")
        if len(physical_bytes) > UINT64_MAX:
            raise RawReferenceRegistrationError("raw V2 emulator segment exceeds uint64 size")

        physical_sha256 = Hash32(hashlib.sha256(physical_bytes).digest())
        if physical_sha256 != reference.physical_sha256:
            raise RawReferenceRegistrationError(
                "raw V2 reference physical SHA-256 does not match registered bytes"
            )
        if reference.byte_end > len(physical_bytes):
            raise RawReferenceRegistrationError(
                "raw V2 reference byte interval exceeds registered segment size"
            )
        stored = physical_bytes[reference.byte_offset : reference.byte_end]
        if Hash32(hashlib.sha256(stored).digest()) != reference.stored_sha256:
            raise RawReferenceRegistrationError(
                "raw V2 reference stored SHA-256 does not match registered bytes"
            )
        if len(stored) != reference.logical_payload_length:
            raise RawReferenceRegistrationError(
                "raw codec logical payload length does not match stored bytes"
            )
        if Hash32(hashlib.sha256(stored).digest()) != reference.logical_payload_sha256:
            raise RawReferenceRegistrationError(
                "raw codec logical payload SHA-256 does not match stored bytes"
            )

        logical_key = (
            reference.raw_store_id,
            reference.lake_id,
            reference.segment_identity,
        )
        existing = self._segments.get(logical_key)
        if existing is not None:
            if existing.physical_bytes == physical_bytes:
                raise RawReferenceRegistrationError(
                    "raw V2 emulator rejects duplicate immutable segment registration"
                )
            raise RawReferenceRegistrationError(
                "raw V2 emulator rejects conflicting registered segment bytes"
            )
        physical_key = (reference.raw_store_id, reference.lake_id, physical_sha256)
        existing_owner = self._physical_owners.get(physical_key)
        if existing_owner is not None and existing_owner != reference.segment_identity:
            raise RawReferenceRegistrationError(
                "raw V2 emulator rejects ambiguous physical segment aliases"
            )

        self._segments[logical_key] = _RegisteredRawSegmentV2(
            physical_bytes=physical_bytes,
            physical_sha256=physical_sha256,
            segment_root=reference.segment_root,
            raw_manifest_root=reference.raw_manifest_root,
        )
        self._physical_owners[physical_key] = reference.segment_identity
        return physical_sha256

    def resolve(self, reference: RawSegmentRef) -> bytes:
        if type(reference) is not RawSegmentReferenceV2:
            raise TypeError("raw V2 emulator resolve requires RawSegmentRef")
        if not self._supports_codec(reference):
            raise RawReferenceResolutionError("raw V2 emulator codec is unsupported")
        registered = self._segments.get(
            (
                reference.raw_store_id,
                reference.lake_id,
                reference.segment_identity,
            )
        )
        if registered is None:
            raise RawReferenceResolutionError("referenced raw V2 segment is missing")

        actual_physical_sha256 = Hash32(hashlib.sha256(registered.physical_bytes).digest())
        if actual_physical_sha256 != registered.physical_sha256:
            raise RawReferenceResolutionError("registered raw V2 segment changed")
        if reference.physical_sha256 != actual_physical_sha256:
            raise RawReferenceResolutionError("raw V2 physical SHA-256 does not match")
        if reference.segment_root != registered.segment_root:
            raise RawReferenceResolutionError("raw V2 segment root does not match authority")
        if reference.raw_manifest_root != registered.raw_manifest_root:
            raise RawReferenceResolutionError("raw V2 manifest root does not match authority")
        if reference.byte_end > len(registered.physical_bytes):
            raise RawReferenceResolutionError("raw V2 byte interval exceeds segment size")

        stored = registered.physical_bytes[reference.byte_offset : reference.byte_end]
        if len(stored) != reference.stored_length:
            raise RawReferenceResolutionError("raw V2 resolver returned truncated stored bytes")
        if Hash32(hashlib.sha256(stored).digest()) != reference.stored_sha256:
            raise RawReferenceResolutionError("raw V2 stored SHA-256 does not match")

        logical_payload = stored
        if len(logical_payload) != reference.logical_payload_length:
            raise RawReferenceResolutionError("raw V2 logical payload length does not match")
        if Hash32(hashlib.sha256(logical_payload).digest()) != (
            reference.logical_payload_sha256
        ):
            raise RawReferenceResolutionError("raw V2 logical payload SHA-256 does not match")
        return logical_payload


def verify_and_resolve_raw_reference_v2(
    reference: RawSegmentRef,
    resolver: RawReferenceResolverV2,
) -> bytes:
    """Resolve V2 and independently authenticate the logical replay payload."""

    if type(reference) is not RawSegmentReferenceV2:
        raise TypeError("verify_and_resolve_raw_reference_v2 requires RawSegmentRef")
    if not isinstance(resolver, RawReferenceResolverV2):
        raise TypeError("resolver must implement RawReferenceResolverV2")
    logical_payload = resolver.resolve(reference)
    if type(logical_payload) is not bytes:
        raise RawReferenceResolutionError("raw V2 resolver must return exact bytes")
    if len(logical_payload) != reference.logical_payload_length:
        raise RawReferenceResolutionError("raw V2 logical payload length does not match")
    if Hash32(hashlib.sha256(logical_payload).digest()) != reference.logical_payload_sha256:
        raise RawReferenceResolutionError("raw V2 logical payload SHA-256 does not match")
    return logical_payload


def resolve_native_reference_v2_row(
    row: LogicalRow,
    resolver: RawReferenceResolverV2,
) -> bytes:
    """Parse, resolve, and independently authenticate one V2 reference row."""

    return verify_and_resolve_raw_reference_v2(raw_reference_v2_from_row(row), resolver)


__all__ = [
    "RAW_REFERENCE_CONTRACT_MARKER_V2",
    "RAW_REFERENCE_FORMAT_VERSION_V2",
    "RAW_REFERENCE_RAW_CODEC_ID",
    "RAW_REFERENCE_RAW_CODEC_VERSION",
    "DeterministicRawLakeV2Emulator",
    "RawReferenceResolverV2",
    "RawSegmentRef",
    "RawSegmentReferenceV2",
    "native_reference_v2_row",
    "raw_reference_v2_from_row",
    "resolve_native_reference_v2_row",
    "verify_and_resolve_raw_reference_v2",
]
