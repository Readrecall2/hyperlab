from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import NoReturn, cast

from hyperlab.paper.storage_v4.canonical import (
    PROTOCOL_VERSION,
    build_commit_logical,
    frame_bytes,
    frame_hash32,
    frame_text,
    frame_u32,
    frame_u64,
    row_hash,
)
from hyperlab.paper.storage_v4.types import (
    UINT32_MAX,
    UINT64_MAX,
    CanonicalValue,
    CommitFrame,
    CommitOrdinal,
    CommitSequence,
    Hash32,
    LogicalRow,
    RunId,
    StreamId,
)

LOGICAL_ROW_RECORD_MAGIC = b"HL4ROW\x00\x01"
COMMIT_FRAME_RECORD_MAGIC = b"HL4REC\x00\x01"
RECORD_FORMAT_VERSION = 1
_OUTER_FIXED_SIZE = 8 + 2 + 8 + 32


class RecordFormatError(ValueError):
    """A logical row or commit record is malformed or unauthenticated."""


@dataclass(frozen=True, slots=True)
class RecordReadLimits:
    """Allocation bounds for one untrusted overlay record."""

    max_physical_size: int = 64 * 1024 * 1024
    max_rows: int = 1_000_000
    max_identifier_size: int = 1024 * 1024
    max_json_size: int = 32 * 1024 * 1024

    def __post_init__(self) -> None:
        for label, value, maximum in (
            ("max_physical_size", self.max_physical_size, UINT64_MAX),
            ("max_rows", self.max_rows, UINT32_MAX),
            ("max_identifier_size", self.max_identifier_size, UINT32_MAX),
            ("max_json_size", self.max_json_size, UINT32_MAX),
        ):
            if type(value) is not int:
                raise TypeError(f"{label} must be an exact integer")
            if value < 1 or value > maximum:
                raise ValueError(f"{label} must be positive and within its framing width")


@dataclass(slots=True)
class _Cursor:
    data: bytes
    offset: int = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.offset

    def take(self, size: int, *, label: str) -> bytes:
        if type(size) is not int or size < 0:
            raise RecordFormatError(f"{label} has an invalid size")
        end = self.offset + size
        if end > len(self.data):
            raise RecordFormatError(f"{label} is truncated")
        value = self.data[self.offset : end]
        self.offset = end
        return value

    def u16(self, *, label: str) -> int:
        return int.from_bytes(self.take(2, label=label), "big", signed=False)

    def u32(self, *, label: str) -> int:
        return int.from_bytes(self.take(4, label=label), "big", signed=False)

    def u64(self, *, label: str) -> int:
        return int.from_bytes(self.take(8, label=label), "big", signed=False)

    def bytes_u32(self, *, label: str, maximum: int) -> bytes:
        size = self.u32(label=f"{label} length")
        if size > maximum:
            raise RecordFormatError(f"{label} exceeds its read limit")
        return self.take(size, label=label)

    def text(self, *, label: str, maximum: int) -> str:
        raw = self.bytes_u32(label=label, maximum=maximum)
        try:
            return raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise RecordFormatError(f"{label} is not strict UTF-8") from error

    def hash32(self, *, label: str) -> Hash32:
        return Hash32(self.take(32, label=label))

    def require_end(self, *, label: str) -> None:
        if self.remaining:
            raise RecordFormatError(f"{label} has {self.remaining} trailing bytes")


def _u16(value: int) -> bytes:
    if type(value) is not int or value < 0 or value > 0xFFFF:
        raise ValueError("uint16 framing value is invalid")
    return value.to_bytes(2, "big", signed=False)


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"non-canonical JSON constant {value!r}")


def _decode_json(value: bytes, *, label: str) -> CanonicalValue:
    try:
        decoded = json.loads(
            value.decode("utf-8", errors="strict"),
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise RecordFormatError(f"{label} is not canonical JSON") from error
    return cast(CanonicalValue, decoded)


def _wrap(magic: bytes, body: bytes) -> bytes:
    if len(magic) != 8:
        raise AssertionError("record magic must be eight bytes")
    return b"".join(
        (
            magic,
            _u16(RECORD_FORMAT_VERSION),
            frame_u64(len(body)),
            body,
            hashlib.sha256(body).digest(),
        )
    )


def _unwrap(
    data: bytes,
    *,
    magic: bytes,
    limits: RecordReadLimits,
    label: str,
) -> bytes:
    if type(data) is not bytes:
        raise TypeError(f"{label} reader requires exact bytes")
    if len(data) > limits.max_physical_size:
        raise RecordFormatError(f"{label} exceeds its physical read limit")
    if len(data) < _OUTER_FIXED_SIZE:
        raise RecordFormatError(f"{label} is truncated")
    cursor = _Cursor(data)
    if cursor.take(8, label=f"{label} magic") != magic:
        raise RecordFormatError(f"invalid {label} magic")
    if cursor.u16(label=f"{label} format version") != RECORD_FORMAT_VERSION:
        raise RecordFormatError(f"unsupported {label} format version")
    body_size = cursor.u64(label=f"{label} body length")
    if body_size > limits.max_physical_size:
        raise RecordFormatError(f"{label} body exceeds its read limit")
    body = cursor.take(body_size, label=f"{label} body")
    expected_sha256 = cursor.take(32, label=f"{label} body SHA-256")
    cursor.require_end(label=label)
    if hashlib.sha256(body).digest() != expected_sha256:
        raise RecordFormatError(f"{label} body SHA-256 mismatch")
    return body


def _logical_row_body(row: LogicalRow) -> bytes:
    if type(row) is not LogicalRow:
        raise TypeError("logical row serializer requires LogicalRow")
    return b"".join(
        (
            _u16(PROTOCOL_VERSION),
            frame_text(row.stream_id.value),
            frame_u32(int(row.ordinal)),
            frame_bytes(row.canonical_bytes),
            frame_hash32(row_hash(row)),
        )
    )


def logical_row_to_bytes(row: LogicalRow) -> bytes:
    """Serialize one row deterministically, including its authenticated identity."""

    return _wrap(LOGICAL_ROW_RECORD_MAGIC, _logical_row_body(row))


def logical_row_from_bytes(
    data: bytes,
    *,
    limits: RecordReadLimits | None = None,
) -> LogicalRow:
    """Parse one row and reject non-canonical JSON, corruption, and trailing bytes."""

    selected = limits if limits is not None else RecordReadLimits()
    body = _unwrap(data, magic=LOGICAL_ROW_RECORD_MAGIC, limits=selected, label="logical row")
    cursor = _Cursor(body)
    if cursor.u16(label="logical row protocol version") != PROTOCOL_VERSION:
        raise RecordFormatError("unsupported logical row protocol version")
    try:
        stream_id = StreamId(
            cursor.text(label="logical row stream ID", maximum=selected.max_identifier_size)
        )
        ordinal = CommitOrdinal(cursor.u32(label="logical row ordinal"))
        canonical = cursor.bytes_u32(
            label="logical row JSON",
            maximum=selected.max_json_size,
        )
        stored_hash = cursor.hash32(label="logical row hash")
        cursor.require_end(label="logical row body")
        row = LogicalRow(stream_id=stream_id, ordinal=ordinal, value=_decode_json(canonical, label="row"))
    except RecordFormatError:
        raise
    except (TypeError, ValueError) as error:
        raise RecordFormatError("logical row violates its typed contract") from error
    if row.canonical_bytes != canonical:
        raise RecordFormatError("logical row JSON is not in canonical byte form")
    if row_hash(row) != stored_hash:
        raise RecordFormatError("logical row hash mismatch")
    return row


def _optional_hash(value: Hash32 | None) -> bytes:
    if value is None:
        return b"\x00"
    if type(value) is not Hash32:
        raise TypeError("optional record hash must be Hash32 or None")
    return b"\x01" + bytes(value)


def _read_optional_hash(cursor: _Cursor, *, label: str) -> Hash32 | None:
    marker = cursor.take(1, label=f"{label} marker")
    if marker == b"\x00":
        return None
    if marker == b"\x01":
        return cursor.hash32(label=label)
    raise RecordFormatError(f"{label} marker is invalid")


def _commit_frame_body(frame: CommitFrame) -> bytes:
    if type(frame) is not CommitFrame:
        raise TypeError("commit frame serializer requires CommitFrame")
    logical = build_commit_logical(frame)
    values = [
        _u16(PROTOCOL_VERSION),
        frame_text(frame.run_id.value),
        frame_u64(int(frame.commit_sequence)),
        frame_hash32(frame.previous_prefix_root),
        _optional_hash(frame.legacy_v3_identity),
        frame_u32(len(frame.rows)),
    ]
    values.extend(frame_bytes(logical_row_to_bytes(row)) for row in frame.rows)
    values.extend((frame_hash32(logical.digest), frame_hash32(logical.prefix_root)))
    return b"".join(values)


def commit_frame_to_bytes(frame: CommitFrame) -> bytes:
    """Serialize a complete commit without depending on SQLite representation."""

    return _wrap(COMMIT_FRAME_RECORD_MAGIC, _commit_frame_body(frame))


def commit_frame_from_bytes(
    data: bytes,
    *,
    limits: RecordReadLimits | None = None,
) -> CommitFrame:
    """Parse and authenticate a complete commit frame strictly."""

    selected = limits if limits is not None else RecordReadLimits()
    body = _unwrap(data, magic=COMMIT_FRAME_RECORD_MAGIC, limits=selected, label="commit frame")
    cursor = _Cursor(body)
    if cursor.u16(label="commit frame protocol version") != PROTOCOL_VERSION:
        raise RecordFormatError("unsupported commit frame protocol version")
    try:
        run_id = RunId(cursor.text(label="commit frame run ID", maximum=selected.max_identifier_size))
        sequence = CommitSequence(cursor.u64(label="commit frame sequence"))
        previous = cursor.hash32(label="commit frame previous prefix root")
        legacy = _read_optional_hash(cursor, label="commit frame legacy V3 identity")
        row_count = cursor.u32(label="commit frame row count")
        if row_count > selected.max_rows:
            raise RecordFormatError("commit frame row count exceeds its read limit")
        rows: list[LogicalRow] = []
        for index in range(row_count):
            row_data = cursor.bytes_u32(
                label=f"commit frame row {index}",
                maximum=selected.max_physical_size,
            )
            rows.append(logical_row_from_bytes(row_data, limits=selected))
        stored_digest = cursor.hash32(label="commit frame logical digest")
        stored_prefix = cursor.hash32(label="commit frame prefix root")
        cursor.require_end(label="commit frame body")
        frame = CommitFrame(
            run_id=run_id,
            commit_sequence=sequence,
            previous_prefix_root=previous,
            rows=tuple(rows),
            legacy_v3_identity=legacy,
        )
        logical = build_commit_logical(frame)
    except RecordFormatError:
        raise
    except (TypeError, ValueError) as error:
        raise RecordFormatError("commit frame violates its typed contract") from error
    if logical.digest != stored_digest:
        raise RecordFormatError("commit frame logical digest mismatch")
    if logical.prefix_root != stored_prefix:
        raise RecordFormatError("commit frame prefix root mismatch")
    return frame


# Short aliases for overlay and adapter call sites.
frame_to_bytes = commit_frame_to_bytes
frame_from_bytes = commit_frame_from_bytes


__all__ = [
    "COMMIT_FRAME_RECORD_MAGIC",
    "LOGICAL_ROW_RECORD_MAGIC",
    "RECORD_FORMAT_VERSION",
    "RecordFormatError",
    "RecordReadLimits",
    "commit_frame_from_bytes",
    "commit_frame_to_bytes",
    "frame_from_bytes",
    "frame_to_bytes",
    "logical_row_from_bytes",
    "logical_row_to_bytes",
]
