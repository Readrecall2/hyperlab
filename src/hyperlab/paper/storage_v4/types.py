from __future__ import annotations

import json
from dataclasses import dataclass
from typing import ClassVar, TypeAlias, cast

UINT32_MAX = (1 << 32) - 1
UINT64_MAX = (1 << 64) - 1

CanonicalScalar: TypeAlias = bool | int | str | None
CanonicalValue: TypeAlias = CanonicalScalar | list["CanonicalValue"] | dict[str, "CanonicalValue"]
CanonicalObject: TypeAlias = dict[str, CanonicalValue]


@dataclass(frozen=True, slots=True)
class Hash32:
    """One logical SHA-256 value stored as exactly 32 immutable bytes."""

    value: bytes

    def __post_init__(self) -> None:
        if type(self.value) is not bytes:
            raise TypeError("Hash32 value must be exact bytes")
        if len(self.value) != 32:
            raise ValueError("Hash32 value must contain exactly 32 bytes")

    def __bytes__(self) -> bytes:
        return self.value

    def hex(self) -> str:
        """Return the lowercase external view; internal storage remains bytes."""

        return self.value.hex()

    @classmethod
    def from_hex(cls, value: str) -> Hash32:
        if type(value) is not str:
            raise TypeError("Hash32 hexadecimal view must be text")
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("Hash32 hexadecimal view must be 64 lowercase hexadecimal characters")
        return cls(bytes.fromhex(value))


@dataclass(frozen=True, slots=True, order=True)
class _TextIdentifier:
    value: str

    def __post_init__(self) -> None:
        if type(self.value) is not str:
            raise TypeError(f"{type(self).__name__} value must be text")
        if not self.value:
            raise ValueError(f"{type(self).__name__} value cannot be empty")
        try:
            encoded = self.value.encode("utf-8", errors="strict")
        except UnicodeEncodeError as error:
            raise ValueError(f"{type(self).__name__} value must be strict UTF-8 text") from error
        if len(encoded) > UINT32_MAX:
            raise ValueError(f"{type(self).__name__} UTF-8 value exceeds uint32 framing")

    def __str__(self) -> str:
        return self.value


class StoreId(_TextIdentifier):
    __slots__ = ()


class RunId(_TextIdentifier):
    __slots__ = ()


class StreamId(_TextIdentifier):
    __slots__ = ()


@dataclass(frozen=True, slots=True, order=True)
class _BoundedUnsigned:
    value: int

    maximum: ClassVar[int]

    def __post_init__(self) -> None:
        if type(self.value) is not int:
            raise TypeError(f"{type(self).__name__} value must be an integer, not bool")
        if self.value < 0 or self.value > self.maximum:
            raise ValueError(
                f"{type(self).__name__} value must be between 0 and {self.maximum} inclusive"
            )

    def __int__(self) -> int:
        return self.value

    def __index__(self) -> int:
        return self.value


class CommitSequence(_BoundedUnsigned):
    __slots__ = ()
    maximum = UINT64_MAX


class EventSequence(_BoundedUnsigned):
    __slots__ = ()
    maximum = UINT64_MAX


class CommitOrdinal(_BoundedUnsigned):
    __slots__ = ()
    maximum = UINT32_MAX


class LocalCount(_BoundedUnsigned):
    __slots__ = ()
    maximum = UINT32_MAX


@dataclass(frozen=True, slots=True)
class SegmentIdentity:
    digest: Hash32

    def __post_init__(self) -> None:
        if type(self.digest) is not Hash32:
            raise TypeError("SegmentIdentity digest must be Hash32")


@dataclass(frozen=True, slots=True)
class ManifestIdentity:
    digest: Hash32

    def __post_init__(self) -> None:
        if type(self.digest) is not Hash32:
            raise TypeError("ManifestIdentity digest must be Hash32")

    @property
    def root(self) -> Hash32:
        return self.digest


def _canonicalize_logical_value(value: CanonicalValue) -> bytes:
    # The local import keeps the type layer usable by canonical.py while making
    # every constructed logical record strict immediately, before it is hashed.
    from hyperlab.paper.storage_v4.canonical import canonical_json_bytes

    return canonical_json_bytes(value)


def _decode_logical_value(value: bytes) -> CanonicalValue:
    return cast(CanonicalValue, json.loads(value))


@dataclass(frozen=True, slots=True, init=False)
class LogicalRow:
    """One canonical JSON value in one typed stream and commit-local ordinal."""

    stream_id: StreamId
    ordinal: CommitOrdinal
    _canonical_bytes: bytes

    def __init__(
        self,
        stream_id: StreamId,
        ordinal: CommitOrdinal,
        value: CanonicalValue,
    ) -> None:
        if type(stream_id) is not StreamId:
            raise TypeError("LogicalRow stream_id must be StreamId")
        if type(ordinal) is not CommitOrdinal:
            raise TypeError("LogicalRow ordinal must be CommitOrdinal")
        canonical = _canonicalize_logical_value(value)
        object.__setattr__(self, "stream_id", stream_id)
        object.__setattr__(self, "ordinal", ordinal)
        object.__setattr__(self, "_canonical_bytes", canonical)

    @property
    def value(self) -> CanonicalValue:
        return _decode_logical_value(self._canonical_bytes)

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes


@dataclass(frozen=True, slots=True, init=False)
class _LogicalPayload:
    _canonical_bytes: bytes

    def __init__(self, value: CanonicalValue) -> None:
        object.__setattr__(self, "_canonical_bytes", _canonicalize_logical_value(value))

    @property
    def value(self) -> CanonicalValue:
        return _decode_logical_value(self._canonical_bytes)

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    def as_row(self, stream_id: StreamId, ordinal: CommitOrdinal) -> LogicalRow:
        return LogicalRow(stream_id=stream_id, ordinal=ordinal, value=self.value)


class InputLogical(_LogicalPayload):
    __slots__ = ()


class EventLogical(_LogicalPayload):
    __slots__ = ()


class LedgerTransactionLogical(_LogicalPayload):
    __slots__ = ()


class LedgerEntryLogical(_LogicalPayload):
    __slots__ = ()


class AlertLogical(_LogicalPayload):
    __slots__ = ()


class ProjectionDeltaLogical(_LogicalPayload):
    __slots__ = ()


@dataclass(frozen=True, slots=True)
class CommitFrame:
    """Complete logical input for one Storage v4 commit digest."""

    run_id: RunId
    commit_sequence: CommitSequence
    previous_prefix_root: Hash32
    rows: tuple[LogicalRow, ...]
    legacy_v3_identity: Hash32 | None = None

    def __post_init__(self) -> None:
        if type(self.run_id) is not RunId:
            raise TypeError("CommitFrame run_id must be RunId")
        if type(self.commit_sequence) is not CommitSequence:
            raise TypeError("CommitFrame commit_sequence must be CommitSequence")
        if type(self.previous_prefix_root) is not Hash32:
            raise TypeError("CommitFrame previous_prefix_root must be Hash32")
        if type(self.rows) is not tuple or any(type(row) is not LogicalRow for row in self.rows):
            raise TypeError("CommitFrame rows must be a tuple of LogicalRow values")
        if self.legacy_v3_identity is not None and type(self.legacy_v3_identity) is not Hash32:
            raise TypeError("CommitFrame legacy_v3_identity must be Hash32 or None")

        ordinals_by_stream: dict[StreamId, set[int]] = {}
        for row in self.rows:
            ordinals = ordinals_by_stream.setdefault(row.stream_id, set())
            ordinal = int(row.ordinal)
            if ordinal in ordinals:
                raise ValueError(
                    f"CommitFrame has duplicate ordinal {ordinal} for stream {row.stream_id.value!r}"
                )
            ordinals.add(ordinal)
        for stream_id, ordinals in ordinals_by_stream.items():
            for expected, actual in enumerate(sorted(ordinals)):
                if actual != expected:
                    raise ValueError(
                        "CommitFrame ordinals must be contiguous from zero within stream "
                        f"{stream_id.value!r}"
                    )


StreamCount: TypeAlias = tuple[StreamId, LocalCount]


@dataclass(frozen=True, slots=True)
class CommitLogical:
    """Derived authenticated identity of a validated complete commit frame."""

    frame: CommitFrame
    row_hashes: tuple[Hash32, ...]
    counts_by_stream: tuple[StreamCount, ...]
    digest: Hash32
    prefix_root: Hash32

    def __post_init__(self) -> None:
        if type(self.frame) is not CommitFrame:
            raise TypeError("CommitLogical frame must be CommitFrame")
        if type(self.row_hashes) is not tuple or any(
            type(row_digest) is not Hash32 for row_digest in self.row_hashes
        ):
            raise TypeError("CommitLogical row_hashes must be a tuple of Hash32 values")
        if len(self.row_hashes) != len(self.frame.rows):
            raise ValueError("CommitLogical row hash count differs from frame row count")
        if type(self.counts_by_stream) is not tuple:
            raise TypeError("CommitLogical counts_by_stream must be a tuple")
        prior_key: bytes | None = None
        total = 0
        for item in self.counts_by_stream:
            if type(item) is not tuple or len(item) != 2:
                raise TypeError("CommitLogical stream counts must be (StreamId, LocalCount) pairs")
            stream_id, count = item
            if type(stream_id) is not StreamId or type(count) is not LocalCount:
                raise TypeError("CommitLogical stream counts must use StreamId and LocalCount")
            key = stream_id.value.encode("utf-8")
            if prior_key is not None and key <= prior_key:
                raise ValueError("CommitLogical stream counts must be unique and UTF-8 sorted")
            prior_key = key
            total += int(count)
        if total != len(self.frame.rows):
            raise ValueError("CommitLogical stream counts differ from frame row count")
        if type(self.digest) is not Hash32 or type(self.prefix_root) is not Hash32:
            raise TypeError("CommitLogical digest and prefix_root must be Hash32")

    def count_for(self, stream_id: StreamId) -> LocalCount:
        if type(stream_id) is not StreamId:
            raise TypeError("count_for stream_id must be StreamId")
        for candidate, count in self.counts_by_stream:
            if candidate == stream_id:
                return count
        return LocalCount(0)


# Concise compatibility aliases for callers that use adjective-first naming.
LogicalInput = InputLogical
LogicalEvent = EventLogical
LogicalLedgerTransaction = LedgerTransactionLogical
LogicalLedgerEntry = LedgerEntryLogical
LogicalAlert = AlertLogical
LogicalProjectionDelta = ProjectionDeltaLogical
CommitSeq = CommitSequence
EventSeq = EventSequence
Ordinal = CommitOrdinal
Count = LocalCount


__all__ = [
    "UINT32_MAX",
    "UINT64_MAX",
    "AlertLogical",
    "CanonicalObject",
    "CanonicalScalar",
    "CanonicalValue",
    "CommitFrame",
    "CommitLogical",
    "CommitOrdinal",
    "CommitSeq",
    "CommitSequence",
    "Count",
    "EventLogical",
    "EventSeq",
    "EventSequence",
    "Hash32",
    "InputLogical",
    "LedgerEntryLogical",
    "LedgerTransactionLogical",
    "LocalCount",
    "LogicalAlert",
    "LogicalEvent",
    "LogicalInput",
    "LogicalLedgerEntry",
    "LogicalLedgerTransaction",
    "LogicalProjectionDelta",
    "LogicalRow",
    "ManifestIdentity",
    "Ordinal",
    "ProjectionDeltaLogical",
    "RunId",
    "SegmentIdentity",
    "StoreId",
    "StreamCount",
    "StreamId",
]
