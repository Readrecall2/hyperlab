from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import cast

from hyperlab.paper.storage_v4.types import (
    UINT32_MAX,
    UINT64_MAX,
    CommitFrame,
    CommitLogical,
    Hash32,
    LocalCount,
    LogicalRow,
    StreamCount,
    StreamId,
)

PROTOCOL_VERSION = 1

DOMAIN_ROW = b"HL4-ROW"
DOMAIN_COMMIT = b"HL4-COMMIT"
DOMAIN_PREFIX = b"HL4-PREFIX"
DOMAIN_MERKLE_LEAF = b"HL4-MERKLE-LEAF"
DOMAIN_MERKLE_NODE = b"HL4-MERKLE-NODE"
DOMAIN_MERKLE_ROOT = b"HL4-MERKLE-ROOT"
DOMAIN_SEGMENT = b"HL4-SEGMENT"
DOMAIN_MANIFEST = b"HL4-MANIFEST"

HASH_DOMAINS = frozenset(
    {
        DOMAIN_ROW,
        DOMAIN_COMMIT,
        DOMAIN_PREFIX,
        DOMAIN_MERKLE_LEAF,
        DOMAIN_MERKLE_NODE,
        DOMAIN_MERKLE_ROOT,
        DOMAIN_SEGMENT,
        DOMAIN_MANIFEST,
    }
)


class CanonicalizationError(ValueError):
    """A logical value has no permitted Storage v4 canonical representation."""


def _snapshot_canonical_value(value: object, *, path: str, active: set[int]) -> object:
    value_type = type(value)
    if value is None or value_type is bool or value_type is int:
        return value
    if value_type is str:
        try:
            cast(str, value).encode("utf-8", errors="strict")
        except UnicodeEncodeError as error:
            raise CanonicalizationError(f"{path} must be strict UTF-8 text") from error
        return value
    if value_type is float:
        raise CanonicalizationError(f"{path} float values are forbidden, including NaN and Infinity")
    if isinstance(value, Decimal):
        raise CanonicalizationError(
            f"{path} Decimal values require an explicit canonical decimal string"
        )
    if value_type is list:
        identity = id(value)
        if identity in active:
            raise CanonicalizationError(f"{path} contains a cyclic list")
        active.add(identity)
        snapshot: list[object] = []
        try:
            for index, item in enumerate(cast(list[object], value)):
                snapshot.append(
                    _snapshot_canonical_value(
                        item,
                        path=f"{path}[{index}]",
                        active=active,
                    )
                )
        finally:
            active.remove(identity)
        return snapshot
    if value_type is dict:
        identity = id(value)
        if identity in active:
            raise CanonicalizationError(f"{path} contains a cyclic object")
        active.add(identity)
        snapshot_object: dict[str, object] = {}
        try:
            for key, item in cast(dict[object, object], value).items():
                if type(key) is not str:
                    raise CanonicalizationError(f"{path} object keys must be text")
                try:
                    key.encode("utf-8", errors="strict")
                except UnicodeEncodeError as error:
                    raise CanonicalizationError(f"{path} object keys must be strict UTF-8") from error
                snapshot_object[key] = _snapshot_canonical_value(
                    item,
                    path=f"{path}.{key}",
                    active=active,
                )
        finally:
            active.remove(identity)
        return snapshot_object
    raise CanonicalizationError(
        f"{path} type {value_type.__module__}.{value_type.__qualname__} is not canonical"
    )


def canonical_json_bytes(value: object) -> bytes:
    """Encode strict logical JSON as deterministic UTF-8 bytes.

    Only exact ``dict``, ``list``, ``str``, ``int``, ``bool`` and ``None``
    values are accepted. In particular, floats, implicit ``Decimal`` values,
    untyped bytes, tuples, non-text keys and locale-dependent encodings fail.
    No Unicode normalization or economic-number normalization is performed.
    """

    try:
        snapshot = _snapshot_canonical_value(value, path="$", active=set())
    except (RecursionError, RuntimeError) as error:
        raise CanonicalizationError(
            "logical value changed or exceeded recursion while being snapshotted"
        ) from error
    try:
        text = json.dumps(
            snapshot,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return text.encode("utf-8", errors="strict")
    except (RecursionError, TypeError, UnicodeEncodeError, ValueError) as error:
        raise CanonicalizationError("logical value could not be encoded as canonical UTF-8 JSON") from error


def canonical_json_text(value: object) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def frame_u32(value: int) -> bytes:
    """Encode an exact Python integer as unsigned 32-bit big-endian bytes."""

    if type(value) is not int:
        raise TypeError("uint32 framing requires an integer, not bool")
    if value < 0 or value > UINT32_MAX:
        raise ValueError("uint32 framing value is out of range")
    return value.to_bytes(4, byteorder="big", signed=False)


def frame_u64(value: int) -> bytes:
    """Encode an exact Python integer as unsigned 64-bit big-endian bytes."""

    if type(value) is not int:
        raise TypeError("uint64 framing requires an integer, not bool")
    if value < 0 or value > UINT64_MAX:
        raise ValueError("uint64 framing value is out of range")
    return value.to_bytes(8, byteorder="big", signed=False)


def frame_bytes(value: bytes) -> bytes:
    """Encode required bytes as ``uint32 length || bytes``."""

    if type(value) is not bytes:
        raise TypeError("byte framing requires exact bytes")
    if len(value) > UINT32_MAX:
        raise ValueError("byte value exceeds uint32 framing")
    return frame_u32(len(value)) + value


def frame_text(value: str) -> bytes:
    """Encode required strict UTF-8 text as ``uint32 byte length || bytes``."""

    if type(value) is not str:
        raise TypeError("text framing requires exact str")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise ValueError("text framing requires strict UTF-8") from error
    return frame_bytes(encoded)


def frame_hash32(value: Hash32) -> bytes:
    """Encode a required fixed-size logical hash without a redundant length."""

    if type(value) is not Hash32:
        raise TypeError("hash framing requires Hash32")
    return bytes(value)


def frame_optional_bytes(value: bytes | None) -> bytes:
    """Encode ``absent=0x00`` or ``present=0x01 || framed bytes``."""

    if value is None:
        return b"\x00"
    return b"\x01" + frame_bytes(value)


def frame_optional_hash32(value: Hash32 | None) -> bytes:
    """Encode ``absent=0x00`` or ``present=0x01 || exact 32-byte hash``."""

    if value is None:
        return b"\x00"
    return b"\x01" + frame_hash32(value)


def frame_domain(domain: bytes) -> bytes:
    """Encode ``uint32 domain length || ASCII domain || uint16 protocol``."""

    if type(domain) is not bytes:
        raise TypeError("hash domain must be exact bytes")
    if not domain:
        raise ValueError("hash domain cannot be empty")
    try:
        domain.decode("ascii", errors="strict")
    except UnicodeDecodeError as error:
        raise ValueError("hash domain must be ASCII") from error
    return frame_bytes(domain) + PROTOCOL_VERSION.to_bytes(2, byteorder="big", signed=False)


def framed_preimage(domain: bytes, *framed_fields: bytes) -> bytes:
    """Build the sole domain/version grammar for an ordered field sequence.

    The field count and every field byte length are encoded here, even when a
    schema has already framed its own contents. Thus no public call can confuse
    two adjacent variable fields or confuse no field with one empty field.
    """

    for field in framed_fields:
        if type(field) is not bytes:
            raise TypeError("framed preimage fields must be exact bytes")
    return (
        frame_domain(domain)
        + frame_u32(len(framed_fields))
        + b"".join(frame_bytes(field) for field in framed_fields)
    )


def framed_hash(domain: bytes, *framed_fields: bytes) -> Hash32:
    """Hash the common unambiguous domain/version field grammar."""

    return Hash32(hashlib.sha256(framed_preimage(domain, *framed_fields)).digest())


def row_preimage(row: LogicalRow) -> bytes:
    """Return the exact ``HL4-ROW`` protocol-v1 logical preimage."""

    if type(row) is not LogicalRow:
        raise TypeError("row_preimage requires LogicalRow")
    return framed_preimage(
        DOMAIN_ROW,
        frame_text(row.stream_id.value),
        frame_u32(int(row.ordinal)),
        frame_bytes(row.canonical_bytes),
    )


def row_hash(row: LogicalRow) -> Hash32:
    return Hash32(hashlib.sha256(row_preimage(row)).digest())


def _counts_by_stream(rows: tuple[LogicalRow, ...]) -> tuple[StreamCount, ...]:
    counts: dict[StreamId, int] = {}
    for row in rows:
        counts[row.stream_id] = counts.get(row.stream_id, 0) + 1
    return tuple(
        (stream_id, LocalCount(counts[stream_id]))
        for stream_id in sorted(counts, key=lambda item: item.value.encode("utf-8"))
    )


def _commit_preimage_components(
    frame: CommitFrame,
    row_hashes: tuple[Hash32, ...],
    counts_by_stream: tuple[StreamCount, ...],
) -> bytes:
    fields = [
        frame_text(frame.run_id.value),
        frame_u64(int(frame.commit_sequence)),
        frame_u32(len(counts_by_stream)),
    ]
    for stream_id, count in counts_by_stream:
        fields.append(frame_text(stream_id.value))
        fields.append(frame_u32(int(count)))
    fields.append(frame_u32(len(row_hashes)))
    fields.extend(frame_hash32(digest) for digest in row_hashes)
    fields.append(frame_hash32(frame.previous_prefix_root))
    fields.append(frame_optional_hash32(frame.legacy_v3_identity))
    return framed_preimage(DOMAIN_COMMIT, *fields)


def commit_preimage(commit: CommitLogical) -> bytes:
    """Return the exact ``HL4-COMMIT`` protocol-v1 logical preimage."""

    if type(commit) is not CommitLogical:
        raise TypeError("commit_preimage requires CommitLogical")
    return _commit_preimage_components(
        commit.frame,
        commit.row_hashes,
        commit.counts_by_stream,
    )


def commit_digest(
    frame: CommitFrame,
    row_hashes: tuple[Hash32, ...],
    counts_by_stream: tuple[StreamCount, ...],
) -> Hash32:
    if type(frame) is not CommitFrame:
        raise TypeError("commit_digest frame must be CommitFrame")
    if type(row_hashes) is not tuple or any(
        type(digest) is not Hash32 for digest in row_hashes
    ):
        raise TypeError("commit_digest row_hashes must be an exact tuple of Hash32")
    if len(row_hashes) != len(frame.rows):
        raise ValueError("commit_digest row hash count differs from frame")
    if row_hashes != tuple(row_hash(row) for row in frame.rows):
        raise ValueError("commit_digest row hashes do not match the frame rows")
    if type(counts_by_stream) is not tuple:
        raise TypeError("commit_digest counts_by_stream must be an exact tuple")
    for item in counts_by_stream:
        if type(item) is not tuple or len(item) != 2:
            raise TypeError("commit_digest stream counts must be exact pairs")
        stream_id, count = item
        if type(stream_id) is not StreamId or type(count) is not LocalCount:
            raise TypeError("commit_digest stream counts require StreamId and LocalCount")
    if counts_by_stream != _counts_by_stream(frame.rows):
        raise ValueError("commit_digest stream counts do not match the frame rows")
    return Hash32(
        hashlib.sha256(
            _commit_preimage_components(frame, row_hashes, counts_by_stream)
        ).digest()
    )


def _prefix_preimage_components(frame: CommitFrame, digest: Hash32) -> bytes:
    return framed_preimage(
        DOMAIN_PREFIX,
        frame_text(frame.run_id.value),
        frame_u64(int(frame.commit_sequence)),
        frame_hash32(frame.previous_prefix_root),
        frame_hash32(digest),
        frame_optional_hash32(frame.legacy_v3_identity),
    )


def prefix_preimage(commit: CommitLogical) -> bytes:
    """Return the exact ``HL4-PREFIX`` protocol-v1 chained preimage."""

    if type(commit) is not CommitLogical:
        raise TypeError("prefix_preimage requires CommitLogical")
    return _prefix_preimage_components(commit.frame, commit.digest)


def prefix_root(frame: CommitFrame, digest: Hash32) -> Hash32:
    if type(frame) is not CommitFrame:
        raise TypeError("prefix_root frame must be CommitFrame")
    if type(digest) is not Hash32:
        raise TypeError("prefix_root digest must be Hash32")
    return Hash32(hashlib.sha256(_prefix_preimage_components(frame, digest)).digest())


def build_commit_logical(frame: CommitFrame) -> CommitLogical:
    """Validate and derive all row, commit and chained-prefix identities."""

    if type(frame) is not CommitFrame:
        raise TypeError("build_commit_logical requires CommitFrame")
    row_hashes = tuple(row_hash(row) for row in frame.rows)
    counts_by_stream = _counts_by_stream(frame.rows)
    digest = commit_digest(frame, row_hashes, counts_by_stream)
    root = prefix_root(frame, digest)
    return CommitLogical(
        frame=frame,
        row_hashes=row_hashes,
        counts_by_stream=counts_by_stream,
        digest=digest,
        prefix_root=root,
    )


def verify_commit_logical(commit: CommitLogical) -> bool:
    """Return whether a supplied derived commit exactly matches its frame."""

    if type(commit) is not CommitLogical:
        raise TypeError("verify_commit_logical requires CommitLogical")
    return build_commit_logical(commit.frame) == commit


__all__ = [
    "DOMAIN_COMMIT",
    "DOMAIN_MANIFEST",
    "DOMAIN_MERKLE_LEAF",
    "DOMAIN_MERKLE_NODE",
    "DOMAIN_MERKLE_ROOT",
    "DOMAIN_PREFIX",
    "DOMAIN_ROW",
    "DOMAIN_SEGMENT",
    "HASH_DOMAINS",
    "PROTOCOL_VERSION",
    "CanonicalizationError",
    "build_commit_logical",
    "canonical_json_bytes",
    "canonical_json_text",
    "commit_digest",
    "commit_preimage",
    "frame_bytes",
    "frame_domain",
    "frame_hash32",
    "frame_optional_bytes",
    "frame_optional_hash32",
    "frame_text",
    "frame_u32",
    "frame_u64",
    "framed_hash",
    "framed_preimage",
    "prefix_preimage",
    "prefix_root",
    "row_hash",
    "row_preimage",
    "verify_commit_logical",
]
