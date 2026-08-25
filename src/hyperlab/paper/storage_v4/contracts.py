"""Public Storage v4 import and raw-segment-reference contracts.

``V3_COMPATIBILITY_IMPORT`` preserves one Golden V3 JSON object as exact
canonical UTF-8 text inside a strict, float-free V4 logical row.
``V4_NATIVE`` stores authenticated references to immutable raw segment bytes.

The deterministic raw-lake emulator in this module is an in-memory contract
test double.  It does not prove filesystem durability, Linux root ownership,
or resistance to a compromised administrator.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import NoReturn, Protocol, runtime_checkable

from .types import (
    UINT32_MAX,
    UINT64_MAX,
    CanonicalObject,
    CommitOrdinal,
    EventSequence,
    Hash32,
    LogicalRow,
    SegmentIdentity,
    StreamId,
)

COMPATIBILITY_CONTRACT_MARKER = "hyperlab.storage_v4.v3_compatibility_record.v1"
RAW_REFERENCE_CONTRACT_MARKER = "hyperlab.storage_v4.raw_segment_reference.v1"

_COMPATIBILITY_KEYS = frozenset(
    {"canonical_json", "canonical_sha256", "contract", "mode"}
)
_RAW_REFERENCE_KEYS = frozenset(
    {
        "byte_length",
        "byte_offset",
        "contract",
        "lake_id",
        "mode",
        "payload_sha256",
        "physical_sha256",
        "segment_identity",
        "source_first_sequence",
        "source_last_sequence",
        "stream_id",
    }
)


class StorageMode(StrEnum):
    """The two deliberately separate Storage v4 logical contracts."""

    V3_COMPATIBILITY_IMPORT = "V3_COMPATIBILITY_IMPORT"
    V4_NATIVE = "V4_NATIVE"


class StorageContractError(ValueError):
    """A public Storage v4 contract is malformed or fails authentication."""


class CompatibilityRecordError(StorageContractError):
    """A Golden V3 compatibility record is not exact and canonical."""


class RawReferenceError(StorageContractError):
    """A native raw-segment reference is malformed or cannot be resolved."""


class RawReferenceRegistrationError(RawReferenceError):
    """An immutable emulator registration is duplicate or conflicting."""


class RawReferenceResolutionError(RawReferenceError):
    """A native raw-segment reference fails closed during resolution."""


def _reject_nonfinite_json(constant: str) -> NoReturn:
    raise CompatibilityRecordError(f"Golden V3 JSON cannot contain {constant}")


def _validated_golden_object_bytes(value: bytes) -> bytes:
    if type(value) is not bytes:
        raise TypeError("Golden V3 canonical record must be exact bytes")
    if not value:
        raise CompatibilityRecordError("Golden V3 canonical record cannot be empty")
    try:
        text = value.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise CompatibilityRecordError("Golden V3 canonical record must be strict UTF-8") from error
    try:
        decoded = json.loads(text, parse_constant=_reject_nonfinite_json)
    except CompatibilityRecordError:
        raise
    except (json.JSONDecodeError, RecursionError, UnicodeError, ValueError) as error:
        raise CompatibilityRecordError(
            "Golden V3 canonical record must contain exactly one JSON value"
        ) from error
    if type(decoded) is not dict:
        raise CompatibilityRecordError("Golden V3 canonical record must be one JSON object")
    try:
        canonical_text = json.dumps(
            decoded,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        canonical = canonical_text.encode("utf-8", errors="strict")
    except (RecursionError, TypeError, UnicodeEncodeError, ValueError) as error:
        raise CompatibilityRecordError(
            "Golden V3 JSON object cannot be rematerialized canonically"
        ) from error
    if canonical != value:
        raise CompatibilityRecordError(
            "Golden V3 JSON bytes are not the exact canonical json.dumps representation"
        )
    return value


@dataclass(frozen=True, slots=True, init=False)
class CompatibilityRecord:
    """One exact Golden V3 canonical JSON object, stored without its JSONL LF.

    Direct construction and ``from_bytes(..., lf_terminated=False)`` accept no
    line terminator.  ``from_jsonl_bytes`` and
    ``from_bytes(..., lf_terminated=True)`` require exactly one trailing LF.
    CRLF, a missing LF, or multiple trailing LFs are rejected rather than
    normalized.
    """

    _canonical_json_bytes: bytes

    def __init__(self, canonical_json_bytes: bytes) -> None:
        canonical = _validated_golden_object_bytes(canonical_json_bytes)
        object.__setattr__(self, "_canonical_json_bytes", canonical)

    @classmethod
    def from_bytes(cls, value: bytes, *, lf_terminated: bool) -> CompatibilityRecord:
        if type(value) is not bytes:
            raise TypeError("Golden V3 canonical record must be exact bytes")
        if type(lf_terminated) is not bool:
            raise TypeError("lf_terminated must be an exact bool")
        if lf_terminated:
            if not value.endswith(b"\n"):
                raise CompatibilityRecordError("LF-terminated Golden V3 record is missing its LF")
            value = value[:-1]
        elif value.endswith(b"\n"):
            raise CompatibilityRecordError(
                "Golden V3 record has an LF; pass lf_terminated=True explicitly"
            )
        return cls(value)

    @classmethod
    def from_jsonl_bytes(cls, value: bytes) -> CompatibilityRecord:
        return cls.from_bytes(value, lf_terminated=True)

    @classmethod
    def from_logical_row(cls, row: LogicalRow) -> CompatibilityRecord:
        return compatibility_record_from_row(row)

    @property
    def canonical_json_bytes(self) -> bytes:
        return self._canonical_json_bytes

    @property
    def canonical_json_text(self) -> str:
        return self._canonical_json_bytes.decode("utf-8")

    @property
    def canonical_sha256(self) -> Hash32:
        return Hash32(hashlib.sha256(self._canonical_json_bytes).digest())

    @property
    def jsonl_bytes(self) -> bytes:
        return self._canonical_json_bytes + b"\n"

    def envelope(self) -> CanonicalObject:
        """Return the strict float-free V4 object that carries this V3 record."""

        return {
            "canonical_json": self.canonical_json_text,
            "canonical_sha256": self.canonical_sha256.hex(),
            "contract": COMPATIBILITY_CONTRACT_MARKER,
            "mode": StorageMode.V3_COMPATIBILITY_IMPORT.value,
        }

    def to_logical_row(self, stream_id: StreamId, ordinal: CommitOrdinal) -> LogicalRow:
        return LogicalRow(stream_id=stream_id, ordinal=ordinal, value=self.envelope())

    def as_logical_row(self, stream_id: StreamId, ordinal: CommitOrdinal) -> LogicalRow:
        return self.to_logical_row(stream_id, ordinal)


def _require_exact_object(
    value: object,
    *,
    keys: frozenset[str],
    label: str,
) -> dict[str, object]:
    if type(value) is not dict:
        raise StorageContractError(f"{label} must be a canonical JSON object")
    typed_value: dict[str, object] = value
    actual_keys = frozenset(typed_value)
    if actual_keys != keys:
        missing = sorted(keys - actual_keys)
        extra = sorted(actual_keys - keys)
        raise StorageContractError(
            f"{label} keys differ from the contract; missing={missing!r}, extra={extra!r}"
        )
    return typed_value


def _required_text(value: object, *, label: str) -> str:
    if type(value) is not str:
        raise StorageContractError(f"{label} must be exact text")
    return value


def _required_integer(value: object, *, label: str) -> int:
    if type(value) is not int:
        raise StorageContractError(f"{label} must be an exact integer, not bool")
    return value


def compatibility_record_from_row(row: LogicalRow) -> CompatibilityRecord:
    """Authenticate and unwrap one strict V4 compatibility envelope."""

    if type(row) is not LogicalRow:
        raise TypeError("compatibility_record_from_row requires LogicalRow")
    try:
        envelope = _require_exact_object(
            row.value,
            keys=_COMPATIBILITY_KEYS,
            label="V3 compatibility envelope",
        )
        if _required_text(envelope["contract"], label="compatibility contract") != (
            COMPATIBILITY_CONTRACT_MARKER
        ):
            raise CompatibilityRecordError("V3 compatibility contract marker is invalid")
        if _required_text(envelope["mode"], label="compatibility mode") != (
            StorageMode.V3_COMPATIBILITY_IMPORT.value
        ):
            raise CompatibilityRecordError("V3 compatibility mode is invalid")
        canonical_text = _required_text(
            envelope["canonical_json"], label="compatibility canonical_json"
        )
        declared_hash = Hash32.from_hex(
            _required_text(
                envelope["canonical_sha256"], label="compatibility canonical_sha256"
            )
        )
        record = CompatibilityRecord(canonical_text.encode("utf-8", errors="strict"))
    except CompatibilityRecordError:
        raise
    except (StorageContractError, TypeError, UnicodeEncodeError, ValueError) as error:
        raise CompatibilityRecordError("V3 compatibility envelope is malformed") from error
    if record.canonical_sha256 != declared_hash:
        raise CompatibilityRecordError("V3 compatibility canonical SHA-256 does not match")
    return record


def rematerialize_compatibility_record(row: LogicalRow) -> bytes:
    """Return the authenticated exact Golden V3 JSON object plus one LF."""

    return compatibility_record_from_row(row).jsonl_bytes


@dataclass(frozen=True, slots=True, order=True)
class RawLakeId:
    """A typed strict-UTF-8 namespace for an external immutable raw lake."""

    value: str

    def __post_init__(self) -> None:
        if type(self.value) is not str:
            raise TypeError("RawLakeId value must be text")
        if not self.value:
            raise ValueError("RawLakeId value cannot be empty")
        try:
            encoded = self.value.encode("utf-8", errors="strict")
        except UnicodeEncodeError as error:
            raise ValueError("RawLakeId value must be strict UTF-8 text") from error
        if len(encoded) > UINT32_MAX:
            raise ValueError("RawLakeId UTF-8 value exceeds uint32 framing")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class RawSegmentReference:
    """Authenticated byte range and source identity for one native raw payload."""

    lake_id: RawLakeId
    segment_identity: SegmentIdentity
    physical_sha256: Hash32
    byte_offset: int
    byte_length: int
    payload_sha256: Hash32
    stream_id: StreamId
    source_first_sequence: EventSequence
    source_last_sequence: EventSequence

    def __post_init__(self) -> None:
        if type(self.lake_id) is not RawLakeId:
            raise TypeError("raw reference lake_id must be RawLakeId")
        if type(self.segment_identity) is not SegmentIdentity:
            raise TypeError("raw reference segment_identity must be SegmentIdentity")
        if type(self.physical_sha256) is not Hash32:
            raise TypeError("raw reference physical_sha256 must be Hash32")
        if type(self.payload_sha256) is not Hash32:
            raise TypeError("raw reference payload_sha256 must be Hash32")
        if type(self.byte_offset) is not int or type(self.byte_length) is not int:
            raise TypeError("raw reference byte offset and length must be exact integers")
        if self.byte_offset < 0 or self.byte_offset > UINT64_MAX:
            raise ValueError("raw reference byte_offset is outside uint64")
        if self.byte_length < 1 or self.byte_length > UINT64_MAX:
            raise ValueError("raw reference byte_length must be positive and within uint64")
        if self.byte_offset > UINT64_MAX - self.byte_length:
            raise ValueError("raw reference byte interval exceeds uint64")
        if type(self.stream_id) is not StreamId:
            raise TypeError("raw reference stream_id must be StreamId")
        if (
            type(self.source_first_sequence) is not EventSequence
            or type(self.source_last_sequence) is not EventSequence
        ):
            raise TypeError("raw reference source range must use EventSequence")
        if int(self.source_first_sequence) > int(self.source_last_sequence):
            raise ValueError("raw reference source range is reversed")

    @property
    def byte_end(self) -> int:
        """Return the exclusive byte endpoint, already checked against uint64."""

        return self.byte_offset + self.byte_length

    def canonical_value(self) -> CanonicalObject:
        return {
            "byte_length": self.byte_length,
            "byte_offset": self.byte_offset,
            "contract": RAW_REFERENCE_CONTRACT_MARKER,
            "lake_id": self.lake_id.value,
            "mode": StorageMode.V4_NATIVE.value,
            "payload_sha256": self.payload_sha256.hex(),
            "physical_sha256": self.physical_sha256.hex(),
            "segment_identity": self.segment_identity.digest.hex(),
            "source_first_sequence": int(self.source_first_sequence),
            "source_last_sequence": int(self.source_last_sequence),
            "stream_id": self.stream_id.value,
        }

    def to_logical_row(self, stream_id: StreamId, ordinal: CommitOrdinal) -> LogicalRow:
        return native_reference_row(self, stream_id, ordinal)


def native_reference_row(
    reference: RawSegmentReference,
    stream_id: StreamId,
    ordinal: CommitOrdinal,
) -> LogicalRow:
    """Represent one native raw reference as a strict canonical V4 row."""

    if type(reference) is not RawSegmentReference:
        raise TypeError("native_reference_row reference must be RawSegmentReference")
    return LogicalRow(stream_id=stream_id, ordinal=ordinal, value=reference.canonical_value())


def _hash_from_contract(value: object, *, label: str) -> Hash32:
    try:
        return Hash32.from_hex(_required_text(value, label=label))
    except (StorageContractError, TypeError, ValueError) as error:
        raise RawReferenceError(f"{label} must be lowercase SHA-256") from error


def raw_reference_from_row(row: LogicalRow) -> RawSegmentReference:
    """Parse and strictly verify the canonical representation of a raw reference."""

    if type(row) is not LogicalRow:
        raise TypeError("raw_reference_from_row requires LogicalRow")
    try:
        value = _require_exact_object(
            row.value,
            keys=_RAW_REFERENCE_KEYS,
            label="native raw reference",
        )
        if _required_text(value["contract"], label="raw reference contract") != (
            RAW_REFERENCE_CONTRACT_MARKER
        ):
            raise RawReferenceError("native raw reference contract marker is invalid")
        if _required_text(value["mode"], label="raw reference mode") != StorageMode.V4_NATIVE.value:
            raise RawReferenceError("native raw reference mode is invalid")
        return RawSegmentReference(
            lake_id=RawLakeId(_required_text(value["lake_id"], label="raw reference lake_id")),
            segment_identity=SegmentIdentity(
                _hash_from_contract(value["segment_identity"], label="segment_identity")
            ),
            physical_sha256=_hash_from_contract(
                value["physical_sha256"], label="physical_sha256"
            ),
            byte_offset=_required_integer(value["byte_offset"], label="byte_offset"),
            byte_length=_required_integer(value["byte_length"], label="byte_length"),
            payload_sha256=_hash_from_contract(
                value["payload_sha256"], label="payload_sha256"
            ),
            stream_id=StreamId(_required_text(value["stream_id"], label="raw stream_id")),
            source_first_sequence=EventSequence(
                _required_integer(value["source_first_sequence"], label="source_first_sequence")
            ),
            source_last_sequence=EventSequence(
                _required_integer(value["source_last_sequence"], label="source_last_sequence")
            ),
        )
    except RawReferenceError:
        raise
    except (StorageContractError, TypeError, ValueError) as error:
        raise RawReferenceError("native raw reference is malformed") from error


@runtime_checkable
class RawReferenceResolver(Protocol):
    """Resolve and authenticate one exact reference against an immutable raw lake."""

    def resolve(self, reference: RawSegmentReference) -> bytes:
        """Return exact referenced bytes or raise ``RawReferenceResolutionError``."""


@dataclass(frozen=True, slots=True)
class _RegisteredRawSegment:
    physical_bytes: bytes
    physical_sha256: Hash32


class DeterministicRawLakeEmulator:
    """In-memory immutable resolver for tests and local development only.

    Registration is one-shot.  A repeated logical key, whether byte-identical
    or conflicting, is rejected.  A second logical identity for the same
    lake/physical hash is also rejected as an ambiguous alias.
    """

    def __init__(self) -> None:
        self._segments: dict[tuple[RawLakeId, SegmentIdentity], _RegisteredRawSegment] = {}
        self._physical_owners: dict[tuple[RawLakeId, Hash32], SegmentIdentity] = {}

    @property
    def segment_count(self) -> int:
        return len(self._segments)

    def register(
        self,
        lake_id: RawLakeId,
        segment_identity: SegmentIdentity,
        physical_bytes: bytes,
    ) -> Hash32:
        """Register one immutable segment exactly once and return its physical SHA-256."""

        if type(lake_id) is not RawLakeId:
            raise TypeError("raw emulator lake_id must be RawLakeId")
        if type(segment_identity) is not SegmentIdentity:
            raise TypeError("raw emulator segment_identity must be SegmentIdentity")
        if type(physical_bytes) is not bytes:
            raise TypeError("raw emulator physical_bytes must be exact bytes")
        if not physical_bytes:
            raise RawReferenceRegistrationError("raw emulator segment cannot be empty")
        if len(physical_bytes) > UINT64_MAX:
            raise RawReferenceRegistrationError("raw emulator segment exceeds uint64 size")

        physical_sha256 = Hash32(hashlib.sha256(physical_bytes).digest())
        logical_key = (lake_id, segment_identity)
        existing = self._segments.get(logical_key)
        if existing is not None:
            if existing.physical_bytes == physical_bytes:
                raise RawReferenceRegistrationError(
                    "raw emulator rejects duplicate immutable segment registration"
                )
            raise RawReferenceRegistrationError(
                "raw emulator rejects conflicting bytes for a registered segment identity"
            )

        physical_key = (lake_id, physical_sha256)
        existing_owner = self._physical_owners.get(physical_key)
        if existing_owner is not None and existing_owner != segment_identity:
            raise RawReferenceRegistrationError(
                "raw emulator rejects ambiguous logical aliases for one physical segment"
            )

        self._segments[logical_key] = _RegisteredRawSegment(
            physical_bytes=physical_bytes,
            physical_sha256=physical_sha256,
        )
        self._physical_owners[physical_key] = segment_identity
        return physical_sha256

    def register_segment(
        self,
        lake_id: RawLakeId,
        segment_identity: SegmentIdentity,
        physical_bytes: bytes,
    ) -> Hash32:
        return self.register(lake_id, segment_identity, physical_bytes)

    def resolve(self, reference: RawSegmentReference) -> bytes:
        if type(reference) is not RawSegmentReference:
            raise TypeError("raw emulator resolve requires RawSegmentReference")
        registered = self._segments.get((reference.lake_id, reference.segment_identity))
        if registered is None:
            raise RawReferenceResolutionError("referenced raw segment is missing")

        actual_physical_sha256 = Hash32(hashlib.sha256(registered.physical_bytes).digest())
        if actual_physical_sha256 != registered.physical_sha256:
            raise RawReferenceResolutionError("registered raw segment changed after registration")
        if reference.physical_sha256 != actual_physical_sha256:
            raise RawReferenceResolutionError("raw segment physical SHA-256 does not match")
        if reference.byte_end > len(registered.physical_bytes):
            raise RawReferenceResolutionError("raw reference byte interval exceeds segment size")

        payload = registered.physical_bytes[reference.byte_offset : reference.byte_end]
        if len(payload) != reference.byte_length:
            raise RawReferenceResolutionError("raw resolver returned a truncated byte interval")
        if Hash32(hashlib.sha256(payload).digest()) != reference.payload_sha256:
            raise RawReferenceResolutionError("raw reference payload SHA-256 does not match")
        return payload


def verify_and_resolve_raw_reference(
    reference: RawSegmentReference,
    resolver: RawReferenceResolver,
) -> bytes:
    """Resolve a typed reference and independently verify payload size and hash."""

    if type(reference) is not RawSegmentReference:
        raise TypeError("verify_and_resolve_raw_reference requires RawSegmentReference")
    if not isinstance(resolver, RawReferenceResolver):
        raise TypeError("resolver must implement RawReferenceResolver")
    payload = resolver.resolve(reference)
    if type(payload) is not bytes:
        raise RawReferenceResolutionError("raw resolver must return exact bytes")
    if len(payload) != reference.byte_length:
        raise RawReferenceResolutionError("raw resolver payload length does not match")
    if Hash32(hashlib.sha256(payload).digest()) != reference.payload_sha256:
        raise RawReferenceResolutionError("raw resolver payload SHA-256 does not match")
    return payload


def resolve_native_reference_row(
    row: LogicalRow,
    resolver: RawReferenceResolver,
) -> bytes:
    """Parse a native reference row, resolve it, and fail closed on any mismatch."""

    return verify_and_resolve_raw_reference(raw_reference_from_row(row), resolver)


__all__ = [
    "COMPATIBILITY_CONTRACT_MARKER",
    "RAW_REFERENCE_CONTRACT_MARKER",
    "CompatibilityRecord",
    "CompatibilityRecordError",
    "DeterministicRawLakeEmulator",
    "RawLakeId",
    "RawReferenceError",
    "RawReferenceRegistrationError",
    "RawReferenceResolutionError",
    "RawReferenceResolver",
    "RawSegmentReference",
    "StorageContractError",
    "StorageMode",
    "compatibility_record_from_row",
    "native_reference_row",
    "raw_reference_from_row",
    "rematerialize_compatibility_record",
    "resolve_native_reference_row",
    "verify_and_resolve_raw_reference",
]
