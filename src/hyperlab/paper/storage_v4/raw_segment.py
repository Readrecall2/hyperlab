"""Bounded streaming immutable raw-segment format for Storage v4 native inputs."""

from __future__ import annotations

import hashlib
import os
import stat
import zlib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO, cast
from uuid import uuid4

from .canonical import canonical_json_bytes, frame_hash32, frame_text, frame_u32, frame_u64, framed_hash
from .contracts import RawLakeId
from .durability import flush_and_fsync
from .faults import FaultHook, FaultPoint, InjectedCrash, trigger_fault
from .segment import CodecProfile
from .types import UINT32_MAX, UINT64_MAX, EventSequence, Hash32, SegmentIdentity, StreamId

RAW_SEGMENT_MAGIC = b"HL4RAW\x00\x01"
RAW_RECORD_MAGIC = b"HL4RRC\x00\x01"
RAW_FOOTER_MAGIC = b"HL4RFT\x00\x01"
RAW_SEGMENT_END_MAGIC = b"HL4REND1"
RAW_SEGMENT_FORMAT_VERSION = 1
RAW_RECORD_FORMAT_VERSION = 1
RAW_FOOTER_FORMAT_VERSION = 1
RAW_CODEC_VERSION = "1"
RAW_RECORD_DOMAIN = b"HL4-RAW-RECORD"
RAW_PREFIX_DOMAIN = b"HL4-RAW-PREFIX"
RAW_SEGMENT_DOMAIN = b"HL4-RAW-SEGMENT"

_RECORD_FIXED_SIZE = 8 + 2 + 4 + 8 + 8 + 32 + 32 + 32 + 32
_INDEX_ENTRY_SIZE = 8 + 8 + 8 + 32 + 32 + 32 + 32
_FOOTER_FIXED_SIZE = 8 + 2 + 4 + 8 + 8 + 32 + 32 + 4 + 32 + 8 + 8


class RawSegmentErrorCode(StrEnum):
    EMPTY = "RAW_SEGMENT_EMPTY"
    TYPE = "RAW_SEGMENT_TYPE_INVALID"
    LIMIT = "RAW_SEGMENT_LIMIT_EXCEEDED"
    THRESHOLD_REACHED = "RAW_SEGMENT_THRESHOLD_REACHED"
    DUPLICATE_RECORD = "RAW_SEGMENT_DUPLICATE_RECORD"
    RECORD_ORDER = "RAW_SEGMENT_RECORD_ORDER_INVALID"
    TRUNCATED = "RAW_SEGMENT_TRUNCATED"
    CORRUPT = "RAW_SEGMENT_CORRUPT"
    HASH_MISMATCH = "RAW_SEGMENT_HASH_MISMATCH"
    PAYLOAD_MISMATCH = "RAW_SEGMENT_PAYLOAD_MISMATCH"
    WRONG_LAKE = "RAW_SEGMENT_WRONG_LAKE"
    CODEC = "RAW_SEGMENT_CODEC_INVALID"
    CLOSED = "RAW_SEGMENT_WRITER_CLOSED"


class RawSegmentError(RuntimeError):
    def __init__(self, code: RawSegmentErrorCode, message: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {message}")


def _error(code: RawSegmentErrorCode, message: str) -> RawSegmentError:
    return RawSegmentError(code, message)


def _is_link_or_reparse_point(path: Path) -> bool:
    try:
        value = path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    if stat.S_ISLNK(value.st_mode):
        return True
    attributes = int(getattr(value, "st_file_attributes", 0))
    reparse = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse)


def _has_link_or_reparse_component(path: Path) -> bool:
    current = path.absolute()
    while True:
        if _is_link_or_reparse_point(current):
            return True
        parent = current.parent
        if parent == current:
            return False
        current = parent


def _open_regular_for_read(path: Path) -> tuple[BinaryIO, int]:
    """Open one stable regular file without following a link/reparse name."""

    if not isinstance(path, Path):
        raise TypeError("raw segment path must be pathlib.Path")
    if _has_link_or_reparse_component(path):
        raise _error(RawSegmentErrorCode.CORRUPT, "raw segment path traverses a link")
    flags = os.O_RDONLY
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOINHERIT", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError as error:
        raise _error(RawSegmentErrorCode.TRUNCATED, "raw segment is missing") from error
    except OSError as error:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw segment cannot be opened safely") from error
    try:
        opened = os.fstat(descriptor)
        named = os.stat(path, follow_symlinks=False)
        if (
            _is_link_or_reparse_point(path)
            or not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(named.st_mode)
            or not os.path.samestat(opened, named)
        ):
            raise _error(
                RawSegmentErrorCode.CORRUPT,
                "raw segment name does not identify the opened regular file",
            )
        stream = cast(BinaryIO, os.fdopen(descriptor, "rb", buffering=0))
    except BaseException:
        os.close(descriptor)
        raise
    return stream, int(opened.st_size)


def _sha256_stream(stream: BinaryIO, size: int) -> Hash32:
    digest = hashlib.sha256()
    stream.seek(0)
    remaining = size
    while remaining:
        block = stream.read(min(1024 * 1024, remaining))
        if not block:
            raise _error(RawSegmentErrorCode.TRUNCATED, "raw segment changed while hashed")
        digest.update(block)
        remaining -= len(block)
    if stream.read(1):
        raise _error(RawSegmentErrorCode.CORRUPT, "raw segment grew while hashed")
    return Hash32(digest.digest())


def _exact_text(value: object, *, label: str, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if type(value) is not str or not value:
        raise _error(RawSegmentErrorCode.TYPE, f"{label} must be nonempty exact text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise _error(RawSegmentErrorCode.TYPE, f"{label} must be strict UTF-8") from error
    return value


@dataclass(frozen=True, slots=True)
class RawSegmentThresholds:
    max_records: int = 50_000
    max_logical_payload_bytes: int = 64 * 1024 * 1024
    max_physical_bytes: int = 128 * 1024 * 1024
    max_single_payload_bytes: int = 32 * 1024 * 1024

    def __post_init__(self) -> None:
        for label, value, maximum in (
            ("max_records", self.max_records, UINT32_MAX),
            ("max_logical_payload_bytes", self.max_logical_payload_bytes, UINT64_MAX),
            ("max_physical_bytes", self.max_physical_bytes, UINT64_MAX),
            ("max_single_payload_bytes", self.max_single_payload_bytes, UINT64_MAX),
        ):
            if type(value) is not int or value < 1 or value > maximum:
                raise ValueError(f"{label} must be a positive bounded exact integer")
        if self.max_single_payload_bytes > self.max_logical_payload_bytes:
            raise ValueError("single raw payload limit exceeds segment logical limit")


@dataclass(frozen=True, slots=True)
class RawSegmentReadLimits:
    max_records: int = 100_000
    max_metadata_bytes: int = 1024 * 1024
    max_stored_payload_bytes: int = 64 * 1024 * 1024
    max_logical_payload_bytes: int = 64 * 1024 * 1024
    max_physical_bytes: int = 512 * 1024 * 1024

    def __post_init__(self) -> None:
        for label, value in (
            ("max_records", self.max_records),
            ("max_metadata_bytes", self.max_metadata_bytes),
            ("max_stored_payload_bytes", self.max_stored_payload_bytes),
            ("max_logical_payload_bytes", self.max_logical_payload_bytes),
            ("max_physical_bytes", self.max_physical_bytes),
        ):
            if type(value) is not int or value < 1:
                raise ValueError(f"{label} must be a positive exact integer")


@dataclass(frozen=True, slots=True)
class RawRecordMetadata:
    record_id: str
    source_id: str
    venue_id: str | None
    input_type: str
    source_stream_id: StreamId
    source_first_sequence: EventSequence
    source_last_sequence: EventSequence
    arrival_sequence: EventSequence
    source_timestamp: str | None = None
    received_timestamp: str | None = None

    def __post_init__(self) -> None:
        for label, value in (
            ("record_id", self.record_id),
            ("source_id", self.source_id),
            ("input_type", self.input_type),
        ):
            _exact_text(value, label=label)
        _exact_text(self.venue_id, label="venue_id", optional=True)
        _exact_text(self.source_timestamp, label="source_timestamp", optional=True)
        _exact_text(self.received_timestamp, label="received_timestamp", optional=True)
        if type(self.source_stream_id) is not StreamId:
            raise TypeError("source_stream_id must be StreamId")
        for sequence in (
            self.source_first_sequence,
            self.source_last_sequence,
            self.arrival_sequence,
        ):
            if type(sequence) is not EventSequence:
                raise TypeError("raw source and arrival sequences must be EventSequence")
        if int(self.source_first_sequence) > int(self.source_last_sequence):
            raise ValueError("raw source sequence range is reversed")

    def canonical_value(self) -> dict[str, object]:
        return {
            "arrival_sequence": int(self.arrival_sequence),
            "input_type": self.input_type,
            "received_timestamp": self.received_timestamp,
            "record_id": self.record_id,
            "source_first_sequence": int(self.source_first_sequence),
            "source_id": self.source_id,
            "source_last_sequence": int(self.source_last_sequence),
            "source_stream_id": self.source_stream_id.value,
            "source_timestamp": self.source_timestamp,
            "venue_id": self.venue_id,
        }


@dataclass(frozen=True, slots=True)
class RawRecordLocator:
    ordinal: int
    metadata: RawRecordMetadata
    byte_offset: int
    stored_length: int
    stored_sha256: Hash32
    logical_payload_length: int
    logical_payload_sha256: Hash32
    metadata_sha256: Hash32
    record_digest: Hash32
    codec_profile: CodecProfile


@dataclass(frozen=True, slots=True)
class RawSegmentSummary:
    lake_id: RawLakeId
    codec_profile: CodecProfile
    segment_identity: SegmentIdentity
    segment_root: Hash32
    physical_sha256: Hash32
    physical_size: int
    record_count: int
    logical_payload_bytes: int
    stored_payload_bytes: int
    records: tuple[RawRecordLocator, ...]


@dataclass(frozen=True, slots=True)
class RawSegmentArtifact:
    path: Path
    summary: RawSegmentSummary

    @property
    def lake_id(self) -> RawLakeId:
        return self.summary.lake_id

    @property
    def codec_profile(self) -> CodecProfile:
        return self.summary.codec_profile

    @property
    def segment_identity(self) -> SegmentIdentity:
        return self.summary.segment_identity

    @property
    def segment_root(self) -> Hash32:
        return self.summary.segment_root

    @property
    def physical_sha256(self) -> Hash32:
        return self.summary.physical_sha256

    @property
    def physical_size(self) -> int:
        return self.summary.physical_size

    @property
    def record_count(self) -> int:
        return self.summary.record_count

    @property
    def logical_payload_bytes(self) -> int:
        return self.summary.logical_payload_bytes

    @property
    def stored_payload_bytes(self) -> int:
        return self.summary.stored_payload_bytes

    @property
    def records(self) -> tuple[RawRecordLocator, ...]:
        return self.summary.records


def _codec_id(codec: CodecProfile) -> str:
    return "raw" if codec.codec_id == 0 else "zlib"


def _encode_payload(payload: bytes, codec: CodecProfile) -> bytes:
    if codec.codec_id == 0:
        return payload
    if codec.codec_id == 1:
        return zlib.compress(payload, level=codec.level)
    raise _error(RawSegmentErrorCode.CODEC, "unsupported raw segment codec")


def _decode_payload(stored: bytes, codec: CodecProfile, logical_size: int) -> bytes:
    if codec.codec_id == 0:
        decoded = stored
    elif codec.codec_id == 1:
        try:
            decompressor = zlib.decompressobj()
            decoded = decompressor.decompress(stored, logical_size + 1)
            if decompressor.unconsumed_tail or len(decoded) > logical_size:
                raise _error(RawSegmentErrorCode.CORRUPT, "raw payload exceeds declared size")
            decoded += decompressor.flush(logical_size + 1 - len(decoded))
        except zlib.error as error:
            raise _error(RawSegmentErrorCode.CORRUPT, "raw zlib payload is invalid") from error
        if not decompressor.eof or decompressor.unused_data:
            raise _error(RawSegmentErrorCode.CORRUPT, "raw zlib payload has trailing or truncated data")
    else:
        raise _error(RawSegmentErrorCode.CODEC, "unsupported raw segment codec")
    if len(decoded) != logical_size:
        raise _error(RawSegmentErrorCode.PAYLOAD_MISMATCH, "raw logical payload length differs")
    return decoded


def _segment_prefix(lake_id: RawLakeId, codec: CodecProfile) -> bytes:
    return b"".join(
        (
            RAW_SEGMENT_MAGIC,
            RAW_SEGMENT_FORMAT_VERSION.to_bytes(2, "big"),
            bytes((codec.codec_id, codec.level)),
            frame_text(lake_id.value),
        )
    )


def _record_digest(
    metadata_bytes: bytes,
    logical_length: int,
    logical_sha256: Hash32,
) -> Hash32:
    return framed_hash(
        RAW_RECORD_DOMAIN,
        metadata_bytes,
        frame_u64(logical_length),
        frame_hash32(logical_sha256),
    )


def _next_root(root: Hash32, record_digest: Hash32) -> Hash32:
    return framed_hash(RAW_PREFIX_DOMAIN, frame_hash32(root), frame_hash32(record_digest))


def _segment_identity(lake_id: RawLakeId, count: int, root: Hash32) -> SegmentIdentity:
    return SegmentIdentity(
        framed_hash(
            RAW_SEGMENT_DOMAIN,
            frame_text(lake_id.value),
            frame_u32(count),
            frame_hash32(root),
        )
    )


def _footer_size(record_count: int) -> int:
    return _FOOTER_FIXED_SIZE + record_count * _INDEX_ENTRY_SIZE


def raw_footer_index_physical_bytes(record_count: int) -> int:
    """Return the exact physical bytes occupied by one raw footer and its index."""

    if type(record_count) is not int:
        raise _error(
            RawSegmentErrorCode.TYPE,
            "raw footer/index record count must be an exact integer",
        )
    if record_count < 1 or record_count > UINT32_MAX:
        raise _error(
            RawSegmentErrorCode.LIMIT,
            "raw footer/index record count must be positive and uint32-bounded",
        )
    physical_bytes = _footer_size(record_count)
    if physical_bytes > UINT64_MAX:
        raise _error(
            RawSegmentErrorCode.LIMIT,
            "raw footer/index physical bytes exceed uint64",
        )
    return physical_bytes


def _encode_footer(
    records: tuple[RawRecordLocator, ...],
    *,
    logical_bytes: int,
    stored_bytes: int,
    segment_root: Hash32,
    identity: SegmentIdentity,
) -> bytes:
    index = b"".join(
        b"".join(
            (
                frame_u64(record.byte_offset),
                frame_u64(record.stored_length),
                frame_u64(record.logical_payload_length),
                frame_hash32(record.metadata_sha256),
                frame_hash32(record.stored_sha256),
                frame_hash32(record.logical_payload_sha256),
                frame_hash32(record.record_digest),
            )
        )
        for record in records
    )
    core = b"".join(
        (
            RAW_FOOTER_MAGIC,
            RAW_FOOTER_FORMAT_VERSION.to_bytes(2, "big"),
            frame_u32(len(records)),
            frame_u64(logical_bytes),
            frame_u64(stored_bytes),
            frame_hash32(segment_root),
            frame_hash32(identity.digest),
            frame_u32(len(records)),
            index,
        )
    )
    footer_size = len(core) + 32 + 8 + len(RAW_SEGMENT_END_MAGIC)
    tail = frame_u64(footer_size) + RAW_SEGMENT_END_MAGIC
    return core + hashlib.sha256(core + tail).digest() + tail


def _sha256_path(path: Path, *, chunk_size: int = 1024 * 1024) -> Hash32:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return Hash32(digest.digest())


def _write_all(stream: BinaryIO, value: bytes) -> None:
    written = stream.write(value)
    if written != len(value):
        raise _error(RawSegmentErrorCode.CORRUPT, "raw temporary write was incomplete")


class RawSegmentWriter:
    """Stream individual payloads to one bounded fresh temporary segment."""

    def __init__(
        self,
        directory: Path,
        *,
        lake_id: RawLakeId,
        codec_profile: CodecProfile | None = None,
        thresholds: RawSegmentThresholds | None = None,
        fault_hook: FaultHook = None,
    ) -> None:
        if not isinstance(directory, Path) or not directory.is_dir():
            raise NotADirectoryError("raw segment staging directory is missing")
        if type(lake_id) is not RawLakeId:
            raise TypeError("raw segment lake_id must be RawLakeId")
        self._lake_id = lake_id
        self._codec = CodecProfile.zlib(level=6) if codec_profile is None else codec_profile
        self._thresholds = RawSegmentThresholds() if thresholds is None else thresholds
        if type(self._codec) is not CodecProfile:
            raise TypeError("raw codec profile must be CodecProfile")
        if type(self._thresholds) is not RawSegmentThresholds:
            raise TypeError("raw thresholds must be RawSegmentThresholds")
        self._fault_hook = fault_hook
        self._path = directory / f".raw-segment.{uuid4().hex}.tmp"
        self._stream = self._path.open("xb")
        self._records: list[RawRecordLocator] = []
        self._record_ids: set[str] = set()
        self._logical_bytes = 0
        self._stored_bytes = 0
        self._root = framed_hash(RAW_PREFIX_DOMAIN, b"")
        self._last_arrival: int | None = None
        self._sealed = False
        self._closed = False
        _write_all(self._stream, _segment_prefix(lake_id, self._codec))

    @property
    def record_count(self) -> int:
        return len(self._records)

    @property
    def should_seal(self) -> bool:
        return (
            len(self._records) >= self._thresholds.max_records
            or self._logical_bytes >= self._thresholds.max_logical_payload_bytes
            or self._stream.tell() + _footer_size(len(self._records))
            >= self._thresholds.max_physical_bytes
        )

    def append(self, payload: bytes, metadata: RawRecordMetadata) -> RawRecordLocator:
        if self._closed or self._sealed:
            raise _error(RawSegmentErrorCode.CLOSED, "raw segment writer is closed")
        if type(payload) is not bytes or type(metadata) is not RawRecordMetadata:
            raise _error(RawSegmentErrorCode.TYPE, "raw append requires exact bytes and metadata")
        if not payload:
            raise _error(RawSegmentErrorCode.EMPTY, "raw payload cannot be empty")
        if len(payload) > self._thresholds.max_single_payload_bytes:
            raise _error(RawSegmentErrorCode.LIMIT, "raw payload exceeds the single-record limit")
        arrival = int(metadata.arrival_sequence)
        if metadata.record_id in self._record_ids:
            raise _error(RawSegmentErrorCode.DUPLICATE_RECORD, "raw record_id is duplicated")
        if self._last_arrival is not None and arrival <= self._last_arrival:
            raise _error(RawSegmentErrorCode.RECORD_ORDER, "raw arrival sequence did not increase")

        metadata_bytes = canonical_json_bytes(metadata.canonical_value())
        stored = _encode_payload(payload, self._codec)
        projected_count = len(self._records) + 1
        projected_logical = self._logical_bytes + len(payload)
        projected_physical = (
            self._stream.tell()
            + _RECORD_FIXED_SIZE
            + len(metadata_bytes)
            + len(stored)
            + _footer_size(projected_count)
        )
        exceeds = (
            projected_count > self._thresholds.max_records
            or projected_logical > self._thresholds.max_logical_payload_bytes
            or projected_physical > self._thresholds.max_physical_bytes
        )
        if exceeds:
            code = (
                RawSegmentErrorCode.THRESHOLD_REACHED
                if self._records
                else RawSegmentErrorCode.LIMIT
            )
            raise _error(code, "raw segment threshold would be exceeded")

        metadata_sha = Hash32(hashlib.sha256(metadata_bytes).digest())
        stored_sha = Hash32(hashlib.sha256(stored).digest())
        logical_sha = Hash32(hashlib.sha256(payload).digest())
        record_digest = _record_digest(metadata_bytes, len(payload), logical_sha)
        record_start = self._stream.tell()
        stored_offset = record_start + _RECORD_FIXED_SIZE + len(metadata_bytes)
        fixed = b"".join(
            (
                RAW_RECORD_MAGIC,
                RAW_RECORD_FORMAT_VERSION.to_bytes(2, "big"),
                frame_u32(len(metadata_bytes)),
                frame_u64(len(stored)),
                frame_u64(len(payload)),
                frame_hash32(metadata_sha),
                frame_hash32(stored_sha),
                frame_hash32(logical_sha),
                frame_hash32(record_digest),
            )
        )
        trigger_fault(self._fault_hook, FaultPoint.BEFORE_TEMP_WRITE)
        _write_all(self._stream, fixed)
        _write_all(self._stream, metadata_bytes)
        _write_all(self._stream, stored)
        trigger_fault(self._fault_hook, FaultPoint.AFTER_TEMP_WRITE)
        locator = RawRecordLocator(
            ordinal=len(self._records),
            metadata=metadata,
            byte_offset=stored_offset,
            stored_length=len(stored),
            stored_sha256=stored_sha,
            logical_payload_length=len(payload),
            logical_payload_sha256=logical_sha,
            metadata_sha256=metadata_sha,
            record_digest=record_digest,
            codec_profile=self._codec,
        )
        self._records.append(locator)
        self._record_ids.add(metadata.record_id)
        self._logical_bytes = projected_logical
        self._stored_bytes += len(stored)
        self._root = _next_root(self._root, record_digest)
        self._last_arrival = arrival
        return locator

    def seal(self) -> RawSegmentArtifact:
        if self._closed or self._sealed:
            raise _error(RawSegmentErrorCode.CLOSED, "raw segment writer is already closed")
        if not self._records:
            raise _error(RawSegmentErrorCode.EMPTY, "cannot seal an empty raw segment")
        identity = _segment_identity(self._lake_id, len(self._records), self._root)
        footer = _encode_footer(
            tuple(self._records),
            logical_bytes=self._logical_bytes,
            stored_bytes=self._stored_bytes,
            segment_root=self._root,
            identity=identity,
        )
        _write_all(self._stream, footer)
        try:
            flush_and_fsync(self._stream, fault_hook=self._fault_hook)
        except InjectedCrash:
            raise
        self._stream.close()
        self._closed = True
        physical_size = self._path.stat().st_size
        if physical_size > self._thresholds.max_physical_bytes:
            raise _error(RawSegmentErrorCode.LIMIT, "sealed raw segment exceeds physical limit")
        physical_sha = _sha256_path(self._path)
        expected = RawSegmentSummary(
            lake_id=self._lake_id,
            codec_profile=self._codec,
            segment_identity=identity,
            segment_root=self._root,
            physical_sha256=physical_sha,
            physical_size=physical_size,
            record_count=len(self._records),
            logical_payload_bytes=self._logical_bytes,
            stored_payload_bytes=self._stored_bytes,
            records=tuple(self._records),
        )
        observed = verify_raw_segment(self._path)
        if observed != expected:
            raise _error(RawSegmentErrorCode.CORRUPT, "raw segment read-back differs")
        self._sealed = True
        return RawSegmentArtifact(path=self._path, summary=expected)

    def close(self) -> None:
        if not self._closed:
            self._stream.close()
            self._closed = True
        if not self._sealed:
            self._path.unlink(missing_ok=True)

    def __enter__(self) -> RawSegmentWriter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _read_exact(stream: BinaryIO, size: int, *, label: str) -> bytes:
    if size < 0:
        raise _error(RawSegmentErrorCode.CORRUPT, f"{label} has negative length")
    value = stream.read(size)
    if len(value) != size:
        raise _error(RawSegmentErrorCode.TRUNCATED, f"{label} is truncated")
    return value


def _u16(value: bytes) -> int:
    return int.from_bytes(value, "big", signed=False)


def _u32(value: bytes) -> int:
    return int.from_bytes(value, "big", signed=False)


def _u64(value: bytes) -> int:
    return int.from_bytes(value, "big", signed=False)


def _read_prefix(stream: BinaryIO, limits: RawSegmentReadLimits) -> tuple[RawLakeId, CodecProfile, int]:
    if _read_exact(stream, 8, label="raw segment magic") != RAW_SEGMENT_MAGIC:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw segment magic is invalid")
    if _u16(_read_exact(stream, 2, label="raw segment version")) != RAW_SEGMENT_FORMAT_VERSION:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw segment version is unsupported")
    codec_bytes = _read_exact(stream, 2, label="raw codec")
    try:
        codec = CodecProfile(codec_id=codec_bytes[0], level=codec_bytes[1])
    except ValueError as error:
        raise _error(RawSegmentErrorCode.CODEC, "raw segment codec is unsupported") from error
    lake_size = _u32(_read_exact(stream, 4, label="raw lake length"))
    if lake_size < 1 or lake_size > limits.max_metadata_bytes:
        raise _error(RawSegmentErrorCode.LIMIT, "raw lake identifier exceeds read limit")
    try:
        lake = RawLakeId(_read_exact(stream, lake_size, label="raw lake").decode("utf-8"))
    except (UnicodeDecodeError, TypeError, ValueError) as error:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw lake identifier is invalid") from error
    return lake, codec, stream.tell()


def _metadata_from_bytes(value: bytes) -> RawRecordMetadata:
    import json

    try:
        decoded = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw record metadata is invalid JSON") from error
    if type(decoded) is not dict or canonical_json_bytes(decoded) != value:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw record metadata is not canonical")
    expected = {
        "arrival_sequence",
        "input_type",
        "received_timestamp",
        "record_id",
        "source_first_sequence",
        "source_id",
        "source_last_sequence",
        "source_stream_id",
        "source_timestamp",
        "venue_id",
    }
    if set(decoded) != expected:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw record metadata field set differs")
    try:
        return RawRecordMetadata(
            record_id=decoded["record_id"],
            source_id=decoded["source_id"],
            venue_id=decoded["venue_id"],
            input_type=decoded["input_type"],
            source_stream_id=StreamId(decoded["source_stream_id"]),
            source_first_sequence=EventSequence(decoded["source_first_sequence"]),
            source_last_sequence=EventSequence(decoded["source_last_sequence"]),
            arrival_sequence=EventSequence(decoded["arrival_sequence"]),
            source_timestamp=decoded["source_timestamp"],
            received_timestamp=decoded["received_timestamp"],
        )
    except (TypeError, ValueError) as error:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw record metadata violates its schema") from error


def _read_footer(
    stream: BinaryIO,
    size: int,
    limits: RawSegmentReadLimits,
) -> tuple[int, int, int, Hash32, SegmentIdentity, tuple[tuple[int, int, int, Hash32, Hash32, Hash32, Hash32], ...], int]:
    if size < 16:
        raise _error(RawSegmentErrorCode.TRUNCATED, "raw segment lacks footer trailer")
    stream.seek(size - 16)
    footer_size = _u64(_read_exact(stream, 8, label="raw footer size"))
    if _read_exact(stream, 8, label="raw end magic") != RAW_SEGMENT_END_MAGIC:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw segment end magic is invalid")
    if footer_size < _FOOTER_FIXED_SIZE or footer_size > size:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw footer size is invalid")
    footer_offset = size - footer_size
    stream.seek(footer_offset)
    footer = _read_exact(stream, footer_size, label="raw footer")
    core = footer[:-48]
    expected_hash = footer[-48:-16]
    tail = footer[-16:]
    if hashlib.sha256(core + tail).digest() != expected_hash:
        raise _error(RawSegmentErrorCode.HASH_MISMATCH, "raw footer SHA-256 differs")
    offset = 0

    def take(length: int) -> bytes:
        nonlocal offset
        end = offset + length
        if end > len(core):
            raise _error(RawSegmentErrorCode.TRUNCATED, "raw footer core is truncated")
        value = core[offset:end]
        offset = end
        return value

    if take(8) != RAW_FOOTER_MAGIC or _u16(take(2)) != RAW_FOOTER_FORMAT_VERSION:
        raise _error(RawSegmentErrorCode.CORRUPT, "raw footer header is invalid")
    count = _u32(take(4))
    logical_bytes = _u64(take(8))
    stored_bytes = _u64(take(8))
    root = Hash32(take(32))
    identity = SegmentIdentity(Hash32(take(32)))
    index_count = _u32(take(4))
    if count != index_count or count < 1 or count > limits.max_records:
        raise _error(RawSegmentErrorCode.LIMIT, "raw footer record count is invalid")
    entries = []
    for _ in range(count):
        entries.append(
            (
                _u64(take(8)),
                _u64(take(8)),
                _u64(take(8)),
                Hash32(take(32)),
                Hash32(take(32)),
                Hash32(take(32)),
                Hash32(take(32)),
            )
        )
    if offset != len(core):
        raise _error(RawSegmentErrorCode.CORRUPT, "raw footer has trailing bytes")
    return count, logical_bytes, stored_bytes, root, identity, tuple(entries), footer_offset


def verify_raw_segment(
    path: Path,
    *,
    limits: RawSegmentReadLimits | None = None,
) -> RawSegmentSummary:
    selected = RawSegmentReadLimits() if limits is None else limits
    stream, physical_size = _open_regular_for_read(path)
    with stream:
        if physical_size < 1 or physical_size > selected.max_physical_bytes:
            raise _error(RawSegmentErrorCode.LIMIT, "raw segment physical size exceeds read limit")
        physical_sha = _sha256_stream(stream, physical_size)
        stream.seek(0)
        lake, codec, records_offset = _read_prefix(stream, selected)
        count, logical_total, stored_total, declared_root, declared_identity, index, footer_offset = (
            _read_footer(stream, physical_size, selected)
        )
        stream.seek(records_offset)
        records: list[RawRecordLocator] = []
        observed_root = framed_hash(RAW_PREFIX_DOMAIN, b"")
        observed_logical = 0
        observed_stored = 0
        prior_arrival: int | None = None
        record_ids: set[str] = set()
        for ordinal, expected in enumerate(index):
            if stream.tell() >= footer_offset:
                raise _error(RawSegmentErrorCode.TRUNCATED, "raw record set ended early")
            fixed = _read_exact(stream, _RECORD_FIXED_SIZE, label="raw record header")
            if fixed[:8] != RAW_RECORD_MAGIC or _u16(fixed[8:10]) != RAW_RECORD_FORMAT_VERSION:
                raise _error(RawSegmentErrorCode.CORRUPT, "raw record header is invalid")
            metadata_size = _u32(fixed[10:14])
            stored_length = _u64(fixed[14:22])
            logical_length = _u64(fixed[22:30])
            metadata_sha = Hash32(fixed[30:62])
            stored_sha = Hash32(fixed[62:94])
            logical_sha = Hash32(fixed[94:126])
            record_digest = Hash32(fixed[126:158])
            if metadata_size > selected.max_metadata_bytes:
                raise _error(RawSegmentErrorCode.LIMIT, "raw metadata exceeds read limit")
            if stored_length > selected.max_stored_payload_bytes:
                raise _error(RawSegmentErrorCode.LIMIT, "stored raw payload exceeds read limit")
            if logical_length > selected.max_logical_payload_bytes:
                raise _error(RawSegmentErrorCode.LIMIT, "logical raw payload exceeds read limit")
            metadata_bytes = _read_exact(stream, metadata_size, label="raw metadata")
            stored_offset = stream.tell()
            stored = _read_exact(stream, stored_length, label="stored raw payload")
            if Hash32(hashlib.sha256(metadata_bytes).digest()) != metadata_sha:
                raise _error(RawSegmentErrorCode.HASH_MISMATCH, "raw metadata SHA-256 differs")
            if Hash32(hashlib.sha256(stored).digest()) != stored_sha:
                raise _error(RawSegmentErrorCode.HASH_MISMATCH, "stored raw SHA-256 differs")
            metadata = _metadata_from_bytes(metadata_bytes)
            logical = _decode_payload(stored, codec, logical_length)
            if Hash32(hashlib.sha256(logical).digest()) != logical_sha:
                raise _error(RawSegmentErrorCode.PAYLOAD_MISMATCH, "logical raw SHA-256 differs")
            if _record_digest(metadata_bytes, logical_length, logical_sha) != record_digest:
                raise _error(RawSegmentErrorCode.HASH_MISMATCH, "raw record digest differs")
            locator = RawRecordLocator(
                ordinal=ordinal,
                metadata=metadata,
                byte_offset=stored_offset,
                stored_length=stored_length,
                stored_sha256=stored_sha,
                logical_payload_length=logical_length,
                logical_payload_sha256=logical_sha,
                metadata_sha256=metadata_sha,
                record_digest=record_digest,
                codec_profile=codec,
            )
            expected_locator = (
                locator.byte_offset,
                locator.stored_length,
                locator.logical_payload_length,
                locator.metadata_sha256,
                locator.stored_sha256,
                locator.logical_payload_sha256,
                locator.record_digest,
            )
            if expected_locator != expected:
                raise _error(RawSegmentErrorCode.CORRUPT, "raw footer index differs from record")
            arrival = int(metadata.arrival_sequence)
            if prior_arrival is not None and arrival <= prior_arrival:
                raise _error(RawSegmentErrorCode.RECORD_ORDER, "raw arrival order regressed")
            if metadata.record_id in record_ids:
                raise _error(RawSegmentErrorCode.DUPLICATE_RECORD, "raw record ID is duplicated")
            prior_arrival = arrival
            record_ids.add(metadata.record_id)
            observed_root = _next_root(observed_root, record_digest)
            observed_logical += logical_length
            observed_stored += stored_length
            records.append(locator)
        if stream.tell() != footer_offset:
            raise _error(RawSegmentErrorCode.CORRUPT, "raw records do not end at footer")
    identity = _segment_identity(lake, count, observed_root)
    if (
        count != len(records)
        or observed_root != declared_root
        or identity != declared_identity
        or observed_logical != logical_total
        or observed_stored != stored_total
    ):
        raise _error(RawSegmentErrorCode.CORRUPT, "raw footer aggregates differ")
    return RawSegmentSummary(
        lake_id=lake,
        codec_profile=codec,
        segment_identity=identity,
        segment_root=observed_root,
        physical_sha256=physical_sha,
        physical_size=physical_size,
        record_count=count,
        logical_payload_bytes=logical_total,
        stored_payload_bytes=stored_total,
        records=tuple(records),
    )


def read_raw_payload(
    path: Path,
    locator: RawRecordLocator,
    *,
    expected_lake_id: RawLakeId,
    limits: RawSegmentReadLimits | None = None,
) -> bytes:
    selected = RawSegmentReadLimits() if limits is None else limits
    if type(locator) is not RawRecordLocator:
        raise TypeError("raw payload locator must be RawRecordLocator")
    stream, size = _open_regular_for_read(path)
    with stream:
        lake, codec, _ = _read_prefix(stream, selected)
        if lake != expected_lake_id:
            raise _error(RawSegmentErrorCode.WRONG_LAKE, "raw segment belongs to another lake")
        if codec != locator.codec_profile:
            raise _error(RawSegmentErrorCode.CODEC, "raw reference codec differs from segment")
        if (
            locator.byte_offset < 0
            or locator.stored_length < 1
            or locator.byte_offset > size
            or locator.stored_length > size - locator.byte_offset
        ):
            raise _error(RawSegmentErrorCode.TRUNCATED, "raw reference range exceeds segment")
        stream.seek(locator.byte_offset)
        stored = _read_exact(stream, locator.stored_length, label="referenced raw payload")
    if Hash32(hashlib.sha256(stored).digest()) != locator.stored_sha256:
        raise _error(RawSegmentErrorCode.HASH_MISMATCH, "referenced stored SHA-256 differs")
    logical = _decode_payload(stored, codec, locator.logical_payload_length)
    if Hash32(hashlib.sha256(logical).digest()) != locator.logical_payload_sha256:
        raise _error(RawSegmentErrorCode.PAYLOAD_MISMATCH, "referenced logical SHA-256 differs")
    return logical


__all__ = [
    "RAW_CODEC_VERSION",
    "RAW_SEGMENT_FORMAT_VERSION",
    "RawRecordLocator",
    "RawRecordMetadata",
    "RawSegmentArtifact",
    "RawSegmentError",
    "RawSegmentErrorCode",
    "RawSegmentReadLimits",
    "RawSegmentSummary",
    "RawSegmentThresholds",
    "RawSegmentWriter",
    "raw_footer_index_physical_bytes",
    "read_raw_payload",
    "verify_raw_segment",
]
