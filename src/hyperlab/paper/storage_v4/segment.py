"""Versioned immutable Storage v4 segments.

The physical framing is intentionally independent from the logical identities.
Only complete commit frames are admitted to blocks. Raw and stdlib zlib are the
only Phase 1A codecs.
"""

from __future__ import annotations

import hashlib
import json
import zlib as _zlib
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise
from typing import NoReturn, cast

from .canonical import (
    DOMAIN_SEGMENT,
    PROTOCOL_VERSION,
    CanonicalizationError,
    build_commit_logical,
    canonical_json_bytes,
    frame_bytes,
    frame_hash32,
    frame_optional_hash32,
    frame_text,
    frame_u32,
    frame_u64,
    framed_hash,
)
from .merkle import merkle_root
from .types import (
    UINT32_MAX,
    UINT64_MAX,
    CanonicalValue,
    CommitFrame,
    CommitLogical,
    CommitOrdinal,
    CommitSequence,
    Hash32,
    LocalCount,
    LogicalRow,
    RunId,
    SegmentIdentity,
    StreamCount,
    StreamId,
)

SEGMENT_MAGIC = b"HL4SEG\x00\x01"
BLOCK_MAGIC = b"HL4B"
FOOTER_MAGIC = b"HL4FTR\x00\x01"
SEGMENT_END_MAGIC = b"HL4END\x00\x01"
COMMIT_FRAME_MAGIC = b"HL4C"

SEGMENT_FORMAT_VERSION = 1
SEGMENT_HEADER_VERSION = 1
SEGMENT_FOOTER_VERSION = 1
COMMIT_FRAME_VERSION = 1

_CODEC_RAW = 0
_CODEC_ZLIB = 1
_SEGMENT_PREFIX_SIZE = 40
_BLOCK_HEADER_SIZE = 76
_FOOTER_FIXED_SIZE = 148


class SegmentFormatError(ValueError):
    """A segment is truncated, corrupt, non-canonical, or internally inconsistent."""


@dataclass(frozen=True, slots=True)
class CodecProfile:
    """Versioned physical codec profile excluded from logical identity."""

    codec_id: int
    level: int

    def __post_init__(self) -> None:
        if type(self.codec_id) is not int or type(self.level) is not int:
            raise TypeError("codec ID and level must be exact integers")
        if self.codec_id == _CODEC_RAW:
            if self.level != 0:
                raise ValueError("raw codec level must be zero")
            return
        if self.codec_id == _CODEC_ZLIB:
            if self.level < 1 or self.level > 9:
                raise ValueError("zlib level must be between 1 and 9")
            return
        raise ValueError(f"unsupported Storage v4 codec ID {self.codec_id}")

    @classmethod
    def raw(cls) -> CodecProfile:
        return cls(codec_id=_CODEC_RAW, level=0)

    @classmethod
    def zlib(cls, *, level: int = 6) -> CodecProfile:
        return cls(codec_id=_CODEC_ZLIB, level=level)

    @property
    def profile_id(self) -> str:
        if self.codec_id == _CODEC_RAW:
            return "raw-v1"
        return f"zlib-v1-level-{self.level}"


@dataclass(frozen=True, slots=True)
class SegmentReadLimits:
    """Fail-closed allocation limits for untrusted Phase 1A segment bytes."""

    max_physical_size: int = 256 * 1024 * 1024
    max_logical_size: int = 256 * 1024 * 1024
    max_header_size: int = 16 * 1024 * 1024
    max_block_physical_size: int = 128 * 1024 * 1024
    max_block_logical_size: int = 128 * 1024 * 1024
    max_blocks: int = 65_536
    max_commits: int = 1_000_000

    def __post_init__(self) -> None:
        for label, value, maximum in (
            ("max_physical_size", self.max_physical_size, UINT64_MAX),
            ("max_logical_size", self.max_logical_size, UINT64_MAX),
            ("max_header_size", self.max_header_size, UINT32_MAX),
            ("max_block_physical_size", self.max_block_physical_size, UINT64_MAX),
            ("max_block_logical_size", self.max_block_logical_size, UINT64_MAX),
            ("max_blocks", self.max_blocks, UINT32_MAX),
            ("max_commits", self.max_commits, UINT32_MAX),
        ):
            if type(value) is not int:
                raise TypeError(f"{label} must be an exact integer")
            if value < 1 or value > maximum:
                raise ValueError(f"{label} must be positive and within its framing width")


@dataclass(frozen=True, slots=True)
class BlockLayout:
    offset: int
    payload_sha256_offset: int
    payload_offset: int
    payload_size: int


@dataclass(frozen=True, slots=True)
class SegmentLayout:
    header_offset: int
    header_size: int
    blocks: tuple[BlockLayout, ...]
    footer_offset: int
    footer_size: int


@dataclass(frozen=True, slots=True)
class BlockInfo:
    index: int
    first_commit_sequence: CommitSequence
    last_commit_sequence: CommitSequence
    commit_count: LocalCount
    logical_size: int
    physical_size: int
    payload_sha256: Hash32


@dataclass(frozen=True, slots=True)
class SegmentArtifact:
    """Verified physical bytes and separate logical and physical identities."""

    data: bytes
    identity: SegmentIdentity
    physical_sha256: Hash32
    codec_profile: CodecProfile
    run_id: RunId
    first_commit_sequence: CommitSequence
    last_commit_sequence: CommitSequence
    previous_prefix_root: Hash32
    end_prefix_root: Hash32
    merkle_root: Hash32
    counts_by_stream: tuple[StreamCount, ...]
    commit_digests: tuple[Hash32, ...]
    commits: tuple[CommitFrame, ...]
    blocks: tuple[BlockInfo, ...]
    logical_size: int
    layout: SegmentLayout

    @property
    def block_count(self) -> int:
        return len(self.blocks)

    @property
    def commit_count(self) -> int:
        return len(self.commits)

    @property
    def physical_size(self) -> int:
        return len(self.data)


@dataclass(slots=True)
class _Cursor:
    data: bytes
    offset: int = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.offset

    def take(self, size: int, *, label: str) -> bytes:
        if type(size) is not int or size < 0:
            raise SegmentFormatError(f"invalid {label} size")
        end = self.offset + size
        if end > len(self.data):
            raise SegmentFormatError(f"truncated {label}")
        value = self.data[self.offset : end]
        self.offset = end
        return value

    def u8(self, *, label: str) -> int:
        return self.take(1, label=label)[0]

    def u16(self, *, label: str) -> int:
        return int.from_bytes(self.take(2, label=label), "big", signed=False)

    def u32(self, *, label: str) -> int:
        return int.from_bytes(self.take(4, label=label), "big", signed=False)

    def u64(self, *, label: str) -> int:
        return int.from_bytes(self.take(8, label=label), "big", signed=False)

    def hash32(self, *, label: str) -> Hash32:
        return Hash32(self.take(32, label=label))

    def text(self, *, label: str) -> str:
        encoded = self.take(self.u32(label=f"{label} length"), label=label)
        try:
            value = encoded.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise SegmentFormatError(f"{label} is not strict UTF-8") from error
        if not value:
            raise SegmentFormatError(f"{label} cannot be empty")
        return value

    def bytes_u32(self, *, label: str) -> bytes:
        return self.take(self.u32(label=f"{label} length"), label=label)

    def optional_hash32(self, *, label: str) -> Hash32 | None:
        tag = self.u8(label=f"{label} tag")
        if tag == 0:
            return None
        if tag == 1:
            return self.hash32(label=label)
        raise SegmentFormatError(f"{label} has invalid optional tag {tag}")

    def require_end(self, *, label: str) -> None:
        if self.remaining != 0:
            raise SegmentFormatError(f"{label} has {self.remaining} trailing bytes")


def _frame_u16(value: int) -> bytes:
    if type(value) is not int:
        raise TypeError("uint16 framing requires an exact integer")
    if value < 0 or value > 0xFFFF:
        raise ValueError("uint16 framing value is out of range")
    return value.to_bytes(2, "big", signed=False)


def _sha256(value: bytes) -> Hash32:
    return Hash32(hashlib.sha256(value).digest())


def _require_positive_u32(value: int, *, label: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{label} must be an exact integer")
    if value < 1 or value > UINT32_MAX:
        raise ValueError(f"{label} must be between 1 and uint32 maximum")


def _require_positive_u64(value: int, *, label: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{label} must be an exact integer")
    if value < 1 or value > UINT64_MAX:
        raise ValueError(f"{label} must be between 1 and uint64 maximum")


def _reject_json_constant(value: str) -> NoReturn:
    raise SegmentFormatError(f"non-finite JSON constant {value!r} is forbidden")


def _decode_canonical_json(value: bytes) -> CanonicalValue:
    try:
        text = value.decode("utf-8", errors="strict")
        decoded = json.loads(text, parse_constant=_reject_json_constant)
        canonical = cast(CanonicalValue, decoded)
        if canonical_json_bytes(canonical) != value:
            raise SegmentFormatError("logical row JSON is not byte-canonical")
        return canonical
    except SegmentFormatError:
        raise
    except (
        CanonicalizationError,
        RecursionError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise SegmentFormatError("logical row JSON is invalid") from error


def _encode_counts(counts: tuple[StreamCount, ...]) -> bytes:
    fields = [frame_u32(len(counts))]
    prior: bytes | None = None
    for stream_id, count in counts:
        if type(stream_id) is not StreamId or type(count) is not LocalCount:
            raise TypeError("stream counts require StreamId and LocalCount")
        key = stream_id.value.encode("utf-8")
        if prior is not None and key <= prior:
            raise ValueError("stream counts must be unique and UTF-8 sorted")
        if int(count) == 0:
            raise ValueError("encoded stream counts cannot contain zero")
        prior = key
        fields.extend((frame_text(stream_id.value), frame_u32(int(count))))
    return b"".join(fields)


def _decode_counts(cursor: _Cursor, *, label: str) -> tuple[StreamCount, ...]:
    result: list[StreamCount] = []
    prior: bytes | None = None
    for _ in range(cursor.u32(label=f"{label} entry count")):
        stream_id = StreamId(cursor.text(label=f"{label} stream ID"))
        key = stream_id.value.encode("utf-8")
        if prior is not None and key <= prior:
            raise SegmentFormatError(f"{label} must be unique and UTF-8 sorted")
        count = LocalCount(cursor.u32(label=f"{label} count"))
        if int(count) == 0:
            raise SegmentFormatError(f"{label} cannot contain a zero count")
        prior = key
        result.append((stream_id, count))
    return tuple(result)


def _aggregate_counts(logicals: Sequence[CommitLogical]) -> tuple[StreamCount, ...]:
    values: dict[StreamId, int] = {}
    for logical in logicals:
        for stream_id, count in logical.counts_by_stream:
            values[stream_id] = values.get(stream_id, 0) + int(count)
    return tuple(
        (stream_id, LocalCount(values[stream_id]))
        for stream_id in sorted(values, key=lambda item: item.value.encode("utf-8"))
    )


def _encode_commit_frame(frame: CommitFrame, logical: CommitLogical | None = None) -> bytes:
    derived = build_commit_logical(frame) if logical is None else logical
    if derived.frame != frame or build_commit_logical(frame) != derived:
        raise ValueError("commit logical identity does not match its frame")

    fields = [
        frame_u64(int(frame.commit_sequence)),
        frame_hash32(frame.previous_prefix_root),
        frame_optional_hash32(frame.legacy_v3_identity),
        _encode_counts(derived.counts_by_stream),
        frame_u32(len(frame.rows)),
    ]
    for row, row_digest in zip(frame.rows, derived.row_hashes, strict=True):
        fields.extend(
            (
                frame_text(row.stream_id.value),
                frame_u32(int(row.ordinal)),
                frame_hash32(row_digest),
                frame_bytes(row.canonical_bytes),
            )
        )
    fields.extend((frame_hash32(derived.digest), frame_hash32(derived.prefix_root)))
    body = b"".join(fields)
    return COMMIT_FRAME_MAGIC + _frame_u16(COMMIT_FRAME_VERSION) + frame_u64(len(body)) + body


def logical_frame_size(frame: CommitFrame) -> int:
    """Return the exact uncompressed physical-v1 size of one indivisible commit."""

    if type(frame) is not CommitFrame:
        raise TypeError("logical_frame_size requires CommitFrame")
    return len(_encode_commit_frame(frame))


def _decode_commit_frame(
    cursor: _Cursor,
    *,
    run_id: RunId,
) -> tuple[CommitFrame, CommitLogical, int]:
    start = cursor.offset
    if cursor.take(len(COMMIT_FRAME_MAGIC), label="commit frame magic") != COMMIT_FRAME_MAGIC:
        raise SegmentFormatError("invalid commit frame magic")
    if cursor.u16(label="commit frame version") != COMMIT_FRAME_VERSION:
        raise SegmentFormatError("unsupported commit frame version")
    body = _Cursor(cursor.take(cursor.u64(label="commit frame body size"), label="commit frame body"))

    sequence = CommitSequence(body.u64(label="commit sequence"))
    previous = body.hash32(label="commit previous prefix root")
    legacy = body.optional_hash32(label="commit legacy V3 identity")
    stored_counts = _decode_counts(body, label="commit stream counts")
    row_count = body.u32(label="commit row count")
    rows: list[LogicalRow] = []
    stored_row_hashes: list[Hash32] = []
    for row_index in range(row_count):
        stream_id = StreamId(body.text(label=f"row {row_index} stream ID"))
        ordinal = CommitOrdinal(body.u32(label=f"row {row_index} ordinal"))
        stored_row_hashes.append(body.hash32(label=f"row {row_index} hash"))
        value = _decode_canonical_json(body.bytes_u32(label=f"row {row_index} value"))
        rows.append(LogicalRow(stream_id=stream_id, ordinal=ordinal, value=value))
    stored_digest = body.hash32(label="commit digest")
    stored_prefix = body.hash32(label="commit prefix root")
    body.require_end(label="commit frame body")

    try:
        frame = CommitFrame(
            run_id=run_id,
            commit_sequence=sequence,
            previous_prefix_root=previous,
            rows=tuple(rows),
            legacy_v3_identity=legacy,
        )
        logical = build_commit_logical(frame)
    except (TypeError, ValueError) as error:
        raise SegmentFormatError("commit frame violates the logical contract") from error

    if logical.counts_by_stream != stored_counts:
        raise SegmentFormatError("commit stream counts do not match rows")
    if logical.row_hashes != tuple(stored_row_hashes):
        raise SegmentFormatError("commit row hash mismatch")
    if logical.digest != stored_digest:
        raise SegmentFormatError("commit digest mismatch")
    if logical.prefix_root != stored_prefix:
        raise SegmentFormatError("commit prefix root mismatch")
    return frame, logical, cursor.offset - start


def _validate_logicals(frames: Sequence[CommitFrame]) -> tuple[tuple[CommitFrame, ...], tuple[CommitLogical, ...]]:
    commits = tuple(frames)
    if not commits:
        raise ValueError("a segment must contain at least one complete commit")
    if len(commits) > UINT32_MAX:
        raise ValueError("segment commit count exceeds uint32")
    if any(type(frame) is not CommitFrame for frame in commits):
        raise TypeError("segment commits must all be CommitFrame values")

    run_id = commits[0].run_id
    logicals: list[CommitLogical] = []
    prior_sequence: int | None = None
    prior_prefix: Hash32 | None = None
    for frame in commits:
        if frame.run_id != run_id:
            raise ValueError("a segment must contain commits from a single run")
        sequence = int(frame.commit_sequence)
        if prior_sequence is not None and sequence != prior_sequence + 1:
            raise ValueError("segment commit sequences must be strictly contiguous")
        if prior_prefix is not None and frame.previous_prefix_root != prior_prefix:
            raise ValueError("segment commit has wrong previous prefix root")
        logical = build_commit_logical(frame)
        logicals.append(logical)
        prior_sequence = sequence
        prior_prefix = logical.prefix_root
    return commits, tuple(logicals)


def _segment_identity(
    *,
    run_id: RunId,
    first: CommitSequence,
    last: CommitSequence,
    previous_prefix_root: Hash32,
    end_prefix_root: Hash32,
    root: Hash32,
    counts: tuple[StreamCount, ...],
    digests: tuple[Hash32, ...],
) -> SegmentIdentity:
    fields = [
        frame_text(run_id.value),
        frame_u64(int(first)),
        frame_u64(int(last)),
        frame_hash32(previous_prefix_root),
        frame_hash32(end_prefix_root),
        frame_hash32(root),
        frame_u32(len(digests)),
        _encode_counts(counts),
        frame_u32(len(digests)),
    ]
    fields.extend(frame_hash32(digest) for digest in digests)
    return SegmentIdentity(framed_hash(DOMAIN_SEGMENT, *fields))


def _encode_segment_header(
    *,
    run_id: RunId,
    first: CommitSequence,
    last: CommitSequence,
    previous_prefix_root: Hash32,
    end_prefix_root: Hash32,
    root: Hash32,
    identity: SegmentIdentity,
    commit_count: int,
    counts: tuple[StreamCount, ...],
) -> bytes:
    return b"".join(
        (
            _frame_u16(PROTOCOL_VERSION),
            frame_text(run_id.value),
            frame_u64(int(first)),
            frame_u64(int(last)),
            frame_hash32(previous_prefix_root),
            frame_hash32(end_prefix_root),
            frame_hash32(root),
            frame_hash32(identity.digest),
            frame_u32(commit_count),
            _encode_counts(counts),
        )
    )


@dataclass(frozen=True, slots=True)
class _SegmentHeader:
    run_id: RunId
    first: CommitSequence
    last: CommitSequence
    previous_prefix_root: Hash32
    end_prefix_root: Hash32
    merkle_root: Hash32
    identity: SegmentIdentity
    commit_count: int
    counts: tuple[StreamCount, ...]


def _decode_segment_header(value: bytes) -> _SegmentHeader:
    cursor = _Cursor(value)
    if cursor.u16(label="header protocol version") != PROTOCOL_VERSION:
        raise SegmentFormatError("unsupported segment logical protocol version")
    try:
        result = _SegmentHeader(
            run_id=RunId(cursor.text(label="segment run ID")),
            first=CommitSequence(cursor.u64(label="segment first commit sequence")),
            last=CommitSequence(cursor.u64(label="segment last commit sequence")),
            previous_prefix_root=cursor.hash32(label="segment previous prefix root"),
            end_prefix_root=cursor.hash32(label="segment end prefix root"),
            merkle_root=cursor.hash32(label="segment Merkle root"),
            identity=SegmentIdentity(cursor.hash32(label="segment logical identity")),
            commit_count=cursor.u32(label="segment commit count"),
            counts=_decode_counts(cursor, label="segment stream counts"),
        )
    except (TypeError, ValueError) as error:
        raise SegmentFormatError("invalid segment header metadata") from error
    cursor.require_end(label="segment header")
    return result


def _encode_payload(value: bytes, codec: CodecProfile) -> bytes:
    if codec.codec_id == _CODEC_RAW:
        return value
    return _zlib.compress(value, level=codec.level)


def _decode_payload(value: bytes, *, codec: CodecProfile, logical_size: int) -> bytes:
    if codec.codec_id == _CODEC_RAW:
        if len(value) != logical_size:
            raise SegmentFormatError("raw block physical and logical sizes differ")
        return value
    try:
        decompressor = _zlib.decompressobj()
        decoded = decompressor.decompress(value, logical_size + 1)
        if decompressor.unconsumed_tail:
            raise SegmentFormatError("zlib block exceeds declared logical size")
    except _zlib.error as error:
        raise SegmentFormatError("zlib block cannot be decoded") from error
    if not decompressor.eof or decompressor.unused_data:
        raise SegmentFormatError("zlib block has a truncated stream or trailing data")
    if len(decoded) != logical_size:
        raise SegmentFormatError("decoded block size differs from declared logical size")
    return decoded


@dataclass(frozen=True, slots=True)
class _BlockBuild:
    logicals: tuple[CommitLogical, ...]
    logical_bytes: bytes


def _partition_blocks(
    logicals: tuple[CommitLogical, ...],
    encoded_frames: tuple[bytes, ...],
    *,
    max_commits: int,
    max_logical_bytes: int,
) -> tuple[_BlockBuild, ...]:
    blocks: list[_BlockBuild] = []
    current_logicals: list[CommitLogical] = []
    current_frames: list[bytes] = []
    current_size = 0

    def flush() -> None:
        nonlocal current_size
        if not current_logicals:
            return
        blocks.append(
            _BlockBuild(
                logicals=tuple(current_logicals),
                logical_bytes=b"".join(current_frames),
            )
        )
        current_logicals.clear()
        current_frames.clear()
        current_size = 0

    for logical, encoded in zip(logicals, encoded_frames, strict=True):
        would_exceed_count = len(current_logicals) >= max_commits
        would_exceed_bytes = current_size + len(encoded) > max_logical_bytes
        if current_logicals and (would_exceed_count or would_exceed_bytes):
            flush()
        current_logicals.append(logical)
        current_frames.append(encoded)
        current_size += len(encoded)
    flush()
    return tuple(blocks)


def _encode_segment_prefix(
    *,
    codec: CodecProfile,
    header_size: int,
    block_count: int,
    logical_size: int,
    physical_size: int,
) -> bytes:
    value = b"".join(
        (
            SEGMENT_MAGIC,
            _frame_u16(SEGMENT_FORMAT_VERSION),
            _frame_u16(SEGMENT_HEADER_VERSION),
            bytes((codec.codec_id, codec.level)),
            _frame_u16(0),
            frame_u32(header_size),
            frame_u32(block_count),
            frame_u64(logical_size),
            frame_u64(physical_size),
        )
    )
    if len(value) != _SEGMENT_PREFIX_SIZE:
        raise AssertionError("segment prefix size drifted")
    return value


def _encode_block(
    block: _BlockBuild,
    *,
    index: int,
    codec: CodecProfile,
) -> tuple[bytes, BlockInfo]:
    payload = _encode_payload(block.logical_bytes, codec)
    payload_digest = _sha256(payload)
    first = block.logicals[0].frame.commit_sequence
    last = block.logicals[-1].frame.commit_sequence
    header = b"".join(
        (
            BLOCK_MAGIC,
            frame_u32(index),
            frame_u64(int(first)),
            frame_u64(int(last)),
            frame_u32(len(block.logicals)),
            frame_u64(len(block.logical_bytes)),
            frame_u64(len(payload)),
            frame_hash32(payload_digest),
        )
    )
    if len(header) != _BLOCK_HEADER_SIZE:
        raise AssertionError("block header size drifted")
    info = BlockInfo(
        index=index,
        first_commit_sequence=first,
        last_commit_sequence=last,
        commit_count=LocalCount(len(block.logicals)),
        logical_size=len(block.logical_bytes),
        physical_size=len(payload),
        payload_sha256=payload_digest,
    )
    return header + payload, info


def _encode_footer(
    *,
    block_offsets: tuple[int, ...],
    logical_size: int,
    physical_size: int,
    header_sha256: Hash32,
    body_sha256: Hash32,
) -> bytes:
    core = b"".join(
        (
            FOOTER_MAGIC,
            _frame_u16(SEGMENT_FOOTER_VERSION),
            _frame_u16(0),
            frame_u32(len(block_offsets)),
            frame_u64(logical_size),
            frame_u64(physical_size),
            frame_hash32(header_sha256),
            frame_hash32(body_sha256),
            frame_u32(len(block_offsets)),
            *(frame_u64(offset) for offset in block_offsets),
        )
    )
    footer_size = len(core) + 32 + 8 + len(SEGMENT_END_MAGIC)
    expected_size = _FOOTER_FIXED_SIZE + 8 * len(block_offsets)
    if footer_size != expected_size:
        raise AssertionError("segment footer size drifted")
    size_and_end = frame_u64(footer_size) + SEGMENT_END_MAGIC
    checksum = _sha256(core + size_and_end)
    return core + frame_hash32(checksum) + size_and_end


def build_segment(
    frames: Sequence[CommitFrame],
    *,
    codec: CodecProfile | None = None,
    max_commits_per_block: int = 4096,
    max_logical_bytes_per_block: int = 16 * 1024 * 1024,
) -> SegmentArtifact:
    """Build and self-verify one immutable, single-run, contiguous segment."""

    selected_codec = CodecProfile.raw() if codec is None else codec
    if type(selected_codec) is not CodecProfile:
        raise TypeError("codec must be CodecProfile")
    _require_positive_u32(max_commits_per_block, label="maximum commits per block")
    _require_positive_u64(
        max_logical_bytes_per_block,
        label="maximum logical bytes per block",
    )
    commits, logicals = _validate_logicals(frames)
    encoded_frames = tuple(
        _encode_commit_frame(frame, logical)
        for frame, logical in zip(commits, logicals, strict=True)
    )
    logical_size = sum(len(value) for value in encoded_frames)
    if logical_size > UINT64_MAX:
        raise ValueError("segment logical size exceeds uint64")

    digests = tuple(logical.digest for logical in logicals)
    counts = _aggregate_counts(logicals)
    root = merkle_root(digests)
    first = commits[0].commit_sequence
    last = commits[-1].commit_sequence
    previous = commits[0].previous_prefix_root
    end = logicals[-1].prefix_root
    identity = _segment_identity(
        run_id=commits[0].run_id,
        first=first,
        last=last,
        previous_prefix_root=previous,
        end_prefix_root=end,
        root=root,
        counts=counts,
        digests=digests,
    )
    header = _encode_segment_header(
        run_id=commits[0].run_id,
        first=first,
        last=last,
        previous_prefix_root=previous,
        end_prefix_root=end,
        root=root,
        identity=identity,
        commit_count=len(commits),
        counts=counts,
    )
    if len(header) > UINT32_MAX:
        raise ValueError("segment header exceeds uint32")

    block_builds = _partition_blocks(
        logicals,
        encoded_frames,
        max_commits=max_commits_per_block,
        max_logical_bytes=max_logical_bytes_per_block,
    )
    encoded_blocks: list[bytes] = []
    block_infos: list[BlockInfo] = []
    for index, block in enumerate(block_builds):
        encoded, info = _encode_block(block, index=index, codec=selected_codec)
        encoded_blocks.append(encoded)
        block_infos.append(info)

    footer_size = _FOOTER_FIXED_SIZE + 8 * len(encoded_blocks)
    physical_size = (
        _SEGMENT_PREFIX_SIZE
        + len(header)
        + sum(len(block) for block in encoded_blocks)
        + footer_size
    )
    if physical_size > UINT64_MAX:
        raise ValueError("segment physical size exceeds uint64")
    prefix = _encode_segment_prefix(
        codec=selected_codec,
        header_size=len(header),
        block_count=len(encoded_blocks),
        logical_size=logical_size,
        physical_size=physical_size,
    )

    block_offsets: list[int] = []
    block_layouts: list[BlockLayout] = []
    offset = len(prefix) + len(header)
    for encoded in encoded_blocks:
        block_offsets.append(offset)
        payload_size = len(encoded) - _BLOCK_HEADER_SIZE
        block_layouts.append(
            BlockLayout(
                offset=offset,
                payload_sha256_offset=offset + 44,
                payload_offset=offset + _BLOCK_HEADER_SIZE,
                payload_size=payload_size,
            )
        )
        offset += len(encoded)
    footer_offset = offset
    body = prefix + header + b"".join(encoded_blocks)
    footer = _encode_footer(
        block_offsets=tuple(block_offsets),
        logical_size=logical_size,
        physical_size=physical_size,
        header_sha256=_sha256(header),
        body_sha256=_sha256(body),
    )
    data = body + footer
    if len(data) != physical_size or len(footer) != footer_size:
        raise AssertionError("segment physical size calculation drifted")

    artifact = SegmentArtifact(
        data=data,
        identity=identity,
        physical_sha256=_sha256(data),
        codec_profile=selected_codec,
        run_id=commits[0].run_id,
        first_commit_sequence=first,
        last_commit_sequence=last,
        previous_prefix_root=previous,
        end_prefix_root=end,
        merkle_root=root,
        counts_by_stream=counts,
        commit_digests=digests,
        commits=commits,
        blocks=tuple(block_infos),
        logical_size=logical_size,
        layout=SegmentLayout(
            header_offset=_SEGMENT_PREFIX_SIZE,
            header_size=len(header),
            blocks=tuple(block_layouts),
            footer_offset=footer_offset,
            footer_size=len(footer),
        ),
    )
    verified = read_segment(data)
    if verified != artifact:
        raise AssertionError("newly built segment failed independent self-verification")
    return artifact


@dataclass(frozen=True, slots=True)
class _Footer:
    block_offsets: tuple[int, ...]
    header_sha256: Hash32
    body_sha256: Hash32


def _decode_footer(
    data: bytes,
    *,
    expected_block_count: int,
    expected_logical_size: int,
    expected_physical_size: int,
) -> tuple[int, _Footer]:
    if len(data) < 16 or data[-len(SEGMENT_END_MAGIC) :] != SEGMENT_END_MAGIC:
        raise SegmentFormatError("segment end magic is missing")
    footer_size = int.from_bytes(
        data[-16 : -len(SEGMENT_END_MAGIC)],
        "big",
        signed=False,
    )
    if footer_size < _FOOTER_FIXED_SIZE or footer_size > len(data):
        raise SegmentFormatError("segment footer size is invalid")
    expected_footer_size = _FOOTER_FIXED_SIZE + 8 * expected_block_count
    if footer_size != expected_footer_size:
        raise SegmentFormatError("segment footer size disagrees with block count")
    footer_offset = len(data) - footer_size
    cursor = _Cursor(data[footer_offset:])
    if cursor.take(len(FOOTER_MAGIC), label="footer magic") != FOOTER_MAGIC:
        raise SegmentFormatError("invalid segment footer magic")
    if cursor.u16(label="footer version") != SEGMENT_FOOTER_VERSION:
        raise SegmentFormatError("unsupported segment footer version")
    if cursor.u16(label="footer flags") != 0:
        raise SegmentFormatError("segment footer has unsupported flags")
    block_count = cursor.u32(label="footer block count")
    if block_count != expected_block_count:
        raise SegmentFormatError("segment footer block count disagrees with prefix")
    logical_size = cursor.u64(label="footer logical size")
    physical_size = cursor.u64(label="footer physical size")
    header_sha256 = cursor.hash32(label="footer header SHA-256")
    body_sha256 = cursor.hash32(label="footer body SHA-256")
    index_count = cursor.u32(label="footer index count")
    if index_count != expected_block_count:
        raise SegmentFormatError("segment footer index count disagrees with prefix")
    offsets = tuple(cursor.u64(label="footer block offset") for _ in range(index_count))
    checksum_offset = cursor.offset
    checksum = cursor.hash32(label="footer checksum")
    declared_size = cursor.u64(label="trailing footer size")
    end_magic = cursor.take(len(SEGMENT_END_MAGIC), label="segment end magic")
    cursor.require_end(label="segment footer")

    if (
        logical_size != expected_logical_size
        or physical_size != expected_physical_size
        or declared_size != footer_size
        or end_magic != SEGMENT_END_MAGIC
    ):
        raise SegmentFormatError("segment footer disagrees with prefix")
    checksum_input = (
        cursor.data[:checksum_offset]
        + frame_u64(declared_size)
        + SEGMENT_END_MAGIC
    )
    if _sha256(checksum_input) != checksum:
        raise SegmentFormatError("segment footer checksum mismatch")
    if any(right <= left for left, right in pairwise(offsets)):
        raise SegmentFormatError("segment footer block offsets are not strictly ordered")
    return footer_offset, _Footer(
        block_offsets=offsets,
        header_sha256=header_sha256,
        body_sha256=body_sha256,
    )


def _read_segment(data: bytes, *, limits: SegmentReadLimits) -> SegmentArtifact:
    if len(data) > limits.max_physical_size:
        raise SegmentFormatError("segment physical size exceeds decode limit")
    if len(data) < _SEGMENT_PREFIX_SIZE + _FOOTER_FIXED_SIZE:
        raise SegmentFormatError("segment is shorter than its minimum framing")
    cursor = _Cursor(data)
    if cursor.take(len(SEGMENT_MAGIC), label="segment magic") != SEGMENT_MAGIC:
        raise SegmentFormatError("invalid segment magic")
    if cursor.u16(label="segment format version") != SEGMENT_FORMAT_VERSION:
        raise SegmentFormatError("unsupported segment format version")
    if cursor.u16(label="segment header version") != SEGMENT_HEADER_VERSION:
        raise SegmentFormatError("unsupported segment header version")
    codec_id = cursor.u8(label="segment codec ID")
    codec_level = cursor.u8(label="segment codec level")
    try:
        codec = CodecProfile(codec_id=codec_id, level=codec_level)
    except (TypeError, ValueError) as error:
        raise SegmentFormatError("unsupported segment codec profile") from error
    if cursor.u16(label="segment flags") != 0:
        raise SegmentFormatError("segment prefix has unsupported flags")
    header_size = cursor.u32(label="segment header size")
    block_count = cursor.u32(label="segment block count")
    logical_size = cursor.u64(label="segment logical size")
    physical_size = cursor.u64(label="segment physical size")
    if cursor.offset != _SEGMENT_PREFIX_SIZE:
        raise AssertionError("segment prefix reader size drifted")
    if block_count < 1:
        raise SegmentFormatError("segment must describe at least one block")
    if header_size > limits.max_header_size:
        raise SegmentFormatError("segment header size exceeds decode limit")
    if block_count > limits.max_blocks:
        raise SegmentFormatError("segment block count exceeds decode limit")
    if logical_size > limits.max_logical_size:
        raise SegmentFormatError("segment logical size exceeds decode limit")
    if physical_size != len(data):
        raise SegmentFormatError("segment physical size mismatch")

    footer_offset, footer = _decode_footer(
        data,
        expected_block_count=block_count,
        expected_logical_size=logical_size,
        expected_physical_size=physical_size,
    )
    header_end = _SEGMENT_PREFIX_SIZE + header_size
    if header_end > footer_offset:
        raise SegmentFormatError("segment header overlaps footer")
    header_bytes = data[_SEGMENT_PREFIX_SIZE:header_end]
    if _sha256(header_bytes) != footer.header_sha256:
        raise SegmentFormatError("segment header SHA-256 mismatch")
    if _sha256(data[:footer_offset]) != footer.body_sha256:
        raise SegmentFormatError("segment body SHA-256 mismatch")
    header = _decode_segment_header(header_bytes)
    if header.commit_count < 1:
        raise SegmentFormatError("segment header has no commits")
    if header.commit_count > limits.max_commits:
        raise SegmentFormatError("segment commit count exceeds decode limit")
    if len(footer.block_offsets) != block_count:
        raise SegmentFormatError("segment block index count mismatch")
    if footer.block_offsets[0] != header_end:
        raise SegmentFormatError("first segment block offset does not follow header")

    cursor.offset = header_end
    all_frames: list[CommitFrame] = []
    all_logicals: list[CommitLogical] = []
    block_infos: list[BlockInfo] = []
    block_layouts: list[BlockLayout] = []
    decoded_logical_size = 0
    for expected_index, expected_offset in enumerate(footer.block_offsets):
        if cursor.offset != expected_offset:
            raise SegmentFormatError("segment block offset index mismatch")
        block_offset = cursor.offset
        if cursor.take(len(BLOCK_MAGIC), label="block magic") != BLOCK_MAGIC:
            raise SegmentFormatError("invalid segment block magic")
        block_index = cursor.u32(label="block index")
        first = CommitSequence(cursor.u64(label="block first commit sequence"))
        last = CommitSequence(cursor.u64(label="block last commit sequence"))
        commit_count = cursor.u32(label="block commit count")
        block_logical_size = cursor.u64(label="block logical size")
        block_physical_size = cursor.u64(label="block physical size")
        if block_logical_size > limits.max_block_logical_size:
            raise SegmentFormatError("block logical size exceeds decode limit")
        if block_logical_size > logical_size - decoded_logical_size:
            raise SegmentFormatError("block logical sizes exceed segment total")
        if block_physical_size > limits.max_block_physical_size:
            raise SegmentFormatError("block physical size exceeds decode limit")
        payload_sha256_offset = cursor.offset
        payload_sha256 = cursor.hash32(label="block payload SHA-256")
        payload_offset = cursor.offset
        payload = cursor.take(block_physical_size, label="block payload")

        if block_index != expected_index:
            raise SegmentFormatError("segment block index is not contiguous")
        if commit_count < 1:
            raise SegmentFormatError("segment block cannot be empty")
        if commit_count > header.commit_count - len(all_frames):
            raise SegmentFormatError("block commit count exceeds segment header total")
        if int(last) < int(first) or int(last) - int(first) + 1 != commit_count:
            raise SegmentFormatError("segment block range disagrees with commit count")
        if _sha256(payload) != payload_sha256:
            raise SegmentFormatError("segment block payload SHA-256 mismatch")
        decoded = _decode_payload(
            payload,
            codec=codec,
            logical_size=block_logical_size,
        )
        decoded_cursor = _Cursor(decoded)
        block_frames: list[CommitFrame] = []
        block_logicals: list[CommitLogical] = []
        for _ in range(commit_count):
            frame, logical, _ = _decode_commit_frame(
                decoded_cursor,
                run_id=header.run_id,
            )
            block_frames.append(frame)
            block_logicals.append(logical)
        decoded_cursor.require_end(label="decoded block")
        if (
            block_frames[0].commit_sequence != first
            or block_frames[-1].commit_sequence != last
        ):
            raise SegmentFormatError("segment block range does not match decoded commits")

        all_frames.extend(block_frames)
        all_logicals.extend(block_logicals)
        decoded_logical_size += block_logical_size
        block_infos.append(
            BlockInfo(
                index=block_index,
                first_commit_sequence=first,
                last_commit_sequence=last,
                commit_count=LocalCount(commit_count),
                logical_size=block_logical_size,
                physical_size=block_physical_size,
                payload_sha256=payload_sha256,
            )
        )
        block_layouts.append(
            BlockLayout(
                offset=block_offset,
                payload_sha256_offset=payload_sha256_offset,
                payload_offset=payload_offset,
                payload_size=block_physical_size,
            )
        )

    if cursor.offset != footer_offset:
        raise SegmentFormatError("segment blocks do not end at footer")
    if decoded_logical_size != logical_size:
        raise SegmentFormatError("segment logical size differs from block total")

    commits, logicals = _validate_logicals(tuple(all_frames))
    if tuple(all_logicals) != logicals:
        raise SegmentFormatError("decoded commit identities are inconsistent")
    if len(commits) != header.commit_count:
        raise SegmentFormatError("segment header commit count mismatch")
    if (
        commits[0].commit_sequence != header.first
        or commits[-1].commit_sequence != header.last
    ):
        raise SegmentFormatError("segment header commit range mismatch")
    if commits[0].previous_prefix_root != header.previous_prefix_root:
        raise SegmentFormatError("segment starting prefix root mismatch")
    if logicals[-1].prefix_root != header.end_prefix_root:
        raise SegmentFormatError("segment ending prefix root mismatch")

    counts = _aggregate_counts(logicals)
    digests = tuple(logical.digest for logical in logicals)
    root = merkle_root(digests)
    identity = _segment_identity(
        run_id=header.run_id,
        first=header.first,
        last=header.last,
        previous_prefix_root=header.previous_prefix_root,
        end_prefix_root=header.end_prefix_root,
        root=root,
        counts=counts,
        digests=digests,
    )
    if counts != header.counts:
        raise SegmentFormatError("segment aggregate stream counts mismatch")
    if root != header.merkle_root:
        raise SegmentFormatError("segment Merkle root mismatch")
    if identity != header.identity:
        raise SegmentFormatError("segment logical identity mismatch")

    return SegmentArtifact(
        data=data,
        identity=identity,
        physical_sha256=_sha256(data),
        codec_profile=codec,
        run_id=header.run_id,
        first_commit_sequence=header.first,
        last_commit_sequence=header.last,
        previous_prefix_root=header.previous_prefix_root,
        end_prefix_root=header.end_prefix_root,
        merkle_root=root,
        counts_by_stream=counts,
        commit_digests=digests,
        commits=commits,
        blocks=tuple(block_infos),
        logical_size=logical_size,
        layout=SegmentLayout(
            header_offset=_SEGMENT_PREFIX_SIZE,
            header_size=header_size,
            blocks=tuple(block_layouts),
            footer_offset=footer_offset,
            footer_size=len(data) - footer_offset,
        ),
    )


def read_segment(
    data: bytes,
    *,
    limits: SegmentReadLimits | None = None,
) -> SegmentArtifact:
    """Parse and exhaustively verify one complete segment from independent bytes."""

    if type(data) is not bytes:
        raise TypeError("segment reader requires exact bytes")
    selected_limits = SegmentReadLimits() if limits is None else limits
    if type(selected_limits) is not SegmentReadLimits:
        raise TypeError("segment read limits must be SegmentReadLimits")
    try:
        return _read_segment(data, limits=selected_limits)
    except SegmentFormatError:
        raise
    except (OverflowError, TypeError, ValueError) as error:
        raise SegmentFormatError("segment violates the Storage v4 contract") from error


__all__ = [
    "BLOCK_MAGIC",
    "COMMIT_FRAME_MAGIC",
    "COMMIT_FRAME_VERSION",
    "FOOTER_MAGIC",
    "SEGMENT_END_MAGIC",
    "SEGMENT_FORMAT_VERSION",
    "SEGMENT_HEADER_VERSION",
    "SEGMENT_MAGIC",
    "BlockInfo",
    "BlockLayout",
    "CodecProfile",
    "SegmentArtifact",
    "SegmentFormatError",
    "SegmentLayout",
    "SegmentReadLimits",
    "build_segment",
    "logical_frame_size",
    "read_segment",
]
