"""Native V4 journal composition, checkpoint binding, and streaming audit.

The helpers in this module preserve the original Paper adapter snapshot while
binding raw-store authority into ``CheckpointState.adapter``.  Native inbox
references rematerialize to the exact canonical JSONL bytes that Paper replay
consumes; all audit paths process one frame and one row at a time.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from .canonical import (
    build_commit_logical,
    canonical_json_bytes,
    frame_bytes,
    frame_hash32,
    frame_text,
    frame_u32,
    frame_u64,
    framed_hash,
)
from .checkpoint import CheckpointState
from .contracts import (
    COMPATIBILITY_CONTRACT_MARKER,
    CompatibilityRecord,
    CompatibilityRecordError,
    RawLakeId,
    RawReferenceError,
    RawReferenceResolutionError,
    rematerialize_compatibility_record,
)
from .raw_reference import (
    RAW_REFERENCE_CONTRACT_MARKER_V2,
    RawReferenceResolverV2,
    RawSegmentRef,
    RawSegmentReferenceV2,
    raw_reference_v2_from_row,
    verify_and_resolve_raw_reference_v2,
)
from .raw_store import RawStoreError
from .types import (
    UINT64_MAX,
    CanonicalObject,
    CommitFrame,
    CommitOrdinal,
    Hash32,
    LogicalRow,
    RunId,
    StoreId,
    StreamId,
)

NATIVE_CHECKPOINT_BINDING_FORMAT_VERSION = 2
NATIVE_CHECKPOINT_BINDING_CONTRACT = "hyperlab.storage_v4.native_checkpoint_binding.v2"
NATIVE_CHECKPOINT_ADAPTER_CONTRACT = "hyperlab.storage_v4.native_checkpoint_adapter.v1"
NATIVE_RAW_REFERENCE_PREFIX_DOMAIN = b"HL4-NATIVE-RAW-REFERENCE-PREFIX\x00\x01"

_BINDING_KEYS = frozenset(
    {
        "contract",
        "format_version",
        "raw_generation",
        "raw_config_identity",
        "raw_last_record_id",
        "raw_lake_id",
        "raw_manifest_root",
        "raw_record_count",
        "raw_reference_prefix_root",
        "raw_store_id",
    }
)
_ADAPTER_KEYS = frozenset(
    {"contract", "format_version", "native_binding", "paper_adapter"}
)


class NativeJournalErrorCode(StrEnum):
    TYPE_INVALID = "NATIVE_JOURNAL_TYPE_INVALID"
    CHECKPOINT_BINDING_INVALID = "NATIVE_CHECKPOINT_BINDING_INVALID"
    CHECKPOINT_BINDING_MISMATCH = "NATIVE_CHECKPOINT_BINDING_MISMATCH"
    RAW_REFERENCE_INVALID = "NATIVE_RAW_REFERENCE_INVALID"
    RAW_REFERENCE_UNRESOLVED = "NATIVE_RAW_REFERENCE_UNRESOLVED"
    REMATERIALIZATION_INVALID = "NATIVE_REMATERIALIZATION_INVALID"
    COMMIT_SEQUENCE_INVALID = "NATIVE_COMMIT_SEQUENCE_INVALID"
    PREFIX_DIVERGENCE = "NATIVE_PREFIX_DIVERGENCE"
    RUN_MISMATCH = "NATIVE_RUN_MISMATCH"
    OUTER_OWNERSHIP_MISMATCH = "NATIVE_OUTER_OWNERSHIP_MISMATCH"
    ARRIVAL_MISMATCH = "NATIVE_ARRIVAL_MISMATCH"
    DUPLICATE_RECORD_REFERENCE = "NATIVE_DUPLICATE_RECORD_REFERENCE"
    ORPHAN_REFERENCE = "NATIVE_ORPHAN_REFERENCE"
    RAW_PAYLOAD_MISMATCH = "NATIVE_RAW_PAYLOAD_MISMATCH"
    EXPECTATION_MISMATCH = "NATIVE_AUDIT_EXPECTATION_MISMATCH"


class NativeJournalError(ValueError):
    def __init__(self, code: NativeJournalErrorCode, message: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {message}")


def _error(code: NativeJournalErrorCode, message: str) -> NativeJournalError:
    return NativeJournalError(code, message)


def _exact_object(
    value: object,
    *,
    keys: frozenset[str],
    label: str,
) -> dict[str, object]:
    if type(value) is not dict:
        raise _error(NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID, f"{label} is not an object")
    typed: dict[str, object] = value
    actual = frozenset(typed)
    if actual != keys:
        raise _error(
            NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
            f"{label} fields differ; missing={sorted(keys - actual)!r}, "
            f"extra={sorted(actual - keys)!r}",
        )
    return typed


def _binding_hash(value: object, *, label: str) -> Hash32:
    if type(value) is not str:
        raise _error(
            NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
            f"{label} must be lowercase SHA-256 text",
        )
    try:
        return Hash32.from_hex(value)
    except (TypeError, ValueError) as error:
        raise _error(
            NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
            f"{label} must be lowercase SHA-256 text",
        ) from error


@dataclass(frozen=True, slots=True)
class NativeCheckpointBinding:
    raw_store_id: StoreId
    raw_lake_id: RawLakeId
    raw_config_identity: Hash32
    raw_generation: int
    raw_manifest_root: Hash32
    raw_record_count: int
    raw_last_record_id: str
    raw_reference_prefix_root: Hash32

    def __post_init__(self) -> None:
        if type(self.raw_store_id) is not StoreId or type(self.raw_lake_id) is not RawLakeId:
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "raw store and lake identities must use their strict types",
            )
        if type(self.raw_config_identity) is not Hash32:
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "raw config identity must be Hash32",
            )
        if type(self.raw_generation) is not int or not 1 <= self.raw_generation <= UINT64_MAX:
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "raw generation must be a positive uint64",
            )
        if type(self.raw_manifest_root) is not Hash32:
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "raw manifest root must be Hash32",
            )
        if type(self.raw_record_count) is not int or not 1 <= self.raw_record_count <= UINT64_MAX:
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "raw record count must be a positive uint64",
            )
        if type(self.raw_last_record_id) is not str or not self.raw_last_record_id:
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "raw last record ID must be nonempty text",
            )
        try:
            self.raw_last_record_id.encode("utf-8", errors="strict")
        except UnicodeEncodeError as error:
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "raw last record ID must be strict UTF-8",
            ) from error
        if type(self.raw_reference_prefix_root) is not Hash32:
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "raw reference prefix root must be Hash32",
            )

    def canonical_value(self) -> CanonicalObject:
        return {
            "contract": NATIVE_CHECKPOINT_BINDING_CONTRACT,
            "format_version": NATIVE_CHECKPOINT_BINDING_FORMAT_VERSION,
            "raw_config_identity": self.raw_config_identity.hex(),
            "raw_generation": self.raw_generation,
            "raw_last_record_id": self.raw_last_record_id,
            "raw_lake_id": self.raw_lake_id.value,
            "raw_manifest_root": self.raw_manifest_root.hex(),
            "raw_record_count": self.raw_record_count,
            "raw_reference_prefix_root": self.raw_reference_prefix_root.hex(),
            "raw_store_id": self.raw_store_id.value,
        }

    @classmethod
    def from_value(cls, value: object) -> NativeCheckpointBinding:
        item = _exact_object(value, keys=_BINDING_KEYS, label="native checkpoint binding")
        if item["contract"] != NATIVE_CHECKPOINT_BINDING_CONTRACT:
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "native checkpoint binding contract differs",
            )
        if (
            type(item["format_version"]) is not int
            or item["format_version"] != NATIVE_CHECKPOINT_BINDING_FORMAT_VERSION
        ):
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "native checkpoint binding version differs",
            )
        try:
            return cls(
                raw_store_id=StoreId(cast(str, item["raw_store_id"])),
                raw_lake_id=RawLakeId(cast(str, item["raw_lake_id"])),
                raw_config_identity=_binding_hash(
                    item["raw_config_identity"],
                    label="raw config identity",
                ),
                raw_generation=cast(int, item["raw_generation"]),
                raw_manifest_root=_binding_hash(
                    item["raw_manifest_root"],
                    label="raw manifest root",
                ),
                raw_record_count=cast(int, item["raw_record_count"]),
                raw_last_record_id=cast(str, item["raw_last_record_id"]),
                raw_reference_prefix_root=_binding_hash(
                    item["raw_reference_prefix_root"],
                    label="raw reference prefix root",
                ),
            )
        except NativeJournalError:
            raise
        except (TypeError, ValueError) as error:
            raise _error(
                NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
                "native checkpoint binding values are malformed",
            ) from error


def _copy_state(state: CheckpointState, *, adapter: CanonicalObject) -> CheckpointState:
    return CheckpointState(
        adapter=adapter,
        ledger=state.ledger,
        projection=state.projection,
        sessions=state.sessions,
        incidents=state.incidents,
        cursors=state.cursors,
        stream_heads=state.stream_heads,
    )


def bind_native_checkpoint_state(
    state: CheckpointState,
    binding: NativeCheckpointBinding,
) -> CheckpointState:
    """Wrap ``state.adapter`` with an authenticated native raw-store binding."""

    if type(state) is not CheckpointState or type(binding) is not NativeCheckpointBinding:
        raise _error(
            NativeJournalErrorCode.TYPE_INVALID,
            "checkpoint binding requires CheckpointState and NativeCheckpointBinding",
        )
    adapter = cast(
        CanonicalObject,
        {
            "contract": NATIVE_CHECKPOINT_ADAPTER_CONTRACT,
            "format_version": NATIVE_CHECKPOINT_BINDING_FORMAT_VERSION,
            "native_binding": binding.canonical_value(),
            "paper_adapter": state.adapter,
        },
    )
    return _copy_state(state, adapter=adapter)


def unbind_native_checkpoint_state(
    state: CheckpointState,
    *,
    expected_binding: NativeCheckpointBinding | None = None,
) -> tuple[CheckpointState, NativeCheckpointBinding]:
    """Authenticate the wrapper and restore the exact original Paper adapter."""

    if type(state) is not CheckpointState:
        raise _error(NativeJournalErrorCode.TYPE_INVALID, "checkpoint unbind requires CheckpointState")
    if expected_binding is not None and type(expected_binding) is not NativeCheckpointBinding:
        raise _error(
            NativeJournalErrorCode.TYPE_INVALID,
            "expected native checkpoint binding has the wrong type",
        )
    adapter = _exact_object(
        state.adapter,
        keys=_ADAPTER_KEYS,
        label="native checkpoint adapter",
    )
    if adapter["contract"] != NATIVE_CHECKPOINT_ADAPTER_CONTRACT:
        raise _error(
            NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
            "native checkpoint adapter contract differs",
        )
    if (
        type(adapter["format_version"]) is not int
        or adapter["format_version"] != NATIVE_CHECKPOINT_BINDING_FORMAT_VERSION
    ):
        raise _error(
            NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
            "native checkpoint adapter version differs",
        )
    paper_adapter = adapter["paper_adapter"]
    if type(paper_adapter) is not dict:
        raise _error(
            NativeJournalErrorCode.CHECKPOINT_BINDING_INVALID,
            "preserved Paper adapter is not a canonical object",
        )
    binding = NativeCheckpointBinding.from_value(adapter["native_binding"])
    if expected_binding is not None and binding != expected_binding:
        raise _error(
            NativeJournalErrorCode.CHECKPOINT_BINDING_MISMATCH,
            "native checkpoint raw binding differs from expectation",
        )
    return _copy_state(state, adapter=cast(CanonicalObject, paper_adapter)), binding


def _contract_marker(row: LogicalRow) -> object:
    value = row.value
    return value.get("contract") if type(value) is dict else None


def _raw_reference_from_row(row: LogicalRow) -> RawSegmentRef:
    try:
        return raw_reference_v2_from_row(row)
    except (RawReferenceError, TypeError, ValueError) as error:
        raise _error(
            NativeJournalErrorCode.RAW_REFERENCE_INVALID,
            "native raw-reference row is malformed",
        ) from error


def rematerialize_native_row(
    row: LogicalRow,
    resolver: RawReferenceResolverV2,
) -> bytes:
    """Return one row's exact canonical JSONL replay representation."""

    if type(row) is not LogicalRow:
        raise _error(NativeJournalErrorCode.TYPE_INVALID, "native rematerialization requires LogicalRow")
    marker = _contract_marker(row)
    if marker == RAW_REFERENCE_CONTRACT_MARKER_V2 or (
        type(marker) is str
        and marker.startswith("hyperlab.storage_v4.raw_segment_reference.")
    ):
        reference = _raw_reference_from_row(row)
        try:
            payload = verify_and_resolve_raw_reference_v2(reference, resolver)
        except (
            RawReferenceError,
            RawReferenceResolutionError,
            RawStoreError,
            TypeError,
            ValueError,
        ) as error:
            raise _error(
                NativeJournalErrorCode.RAW_REFERENCE_UNRESOLVED,
                "native raw reference cannot be resolved exactly",
            ) from error
        try:
            canonical = CompatibilityRecord.from_jsonl_bytes(payload).jsonl_bytes
        except (CompatibilityRecordError, TypeError, ValueError) as error:
            raise _error(
                NativeJournalErrorCode.REMATERIALIZATION_INVALID,
                "resolved raw payload is not one exact canonical JSONL object",
            ) from error
        if canonical != payload:
            raise _error(
                NativeJournalErrorCode.REMATERIALIZATION_INVALID,
                "resolved raw payload changed during canonical validation",
            )
        return payload
    if marker == COMPATIBILITY_CONTRACT_MARKER:
        try:
            return rematerialize_compatibility_record(row)
        except (CompatibilityRecordError, TypeError, ValueError) as error:
            raise _error(
                NativeJournalErrorCode.REMATERIALIZATION_INVALID,
                "compatibility row cannot be rematerialized exactly",
            ) from error
    return row.canonical_bytes + b"\n"


def _jsonl_object(line: bytes) -> dict[str, object]:
    try:
        value = json.loads(line[:-1].decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise _error(
            NativeJournalErrorCode.REMATERIALIZATION_INVALID,
            "rematerialized row is not JSON",
        ) from error
    if type(value) is not dict:
        raise _error(
            NativeJournalErrorCode.OUTER_OWNERSHIP_MISMATCH,
            "owned Paper row is not an object",
        )
    return cast(dict[str, object], value)


def _validate_outer_ownership(
    *,
    frame: CommitFrame,
    row: LogicalRow,
    reference: RawSegmentRef,
    payload: bytes,
) -> None:
    if row.stream_id != StreamId("inbox") or int(row.ordinal) != 0:
        raise _error(
            NativeJournalErrorCode.OUTER_OWNERSHIP_MISMATCH,
            "native raw reference must own inbox ordinal zero",
        )
    sequence = int(frame.commit_sequence)
    if int(reference.arrival_sequence) != sequence:
        raise _error(
            NativeJournalErrorCode.ARRIVAL_MISMATCH,
            "raw arrival sequence differs from outer commit sequence",
        )
    value = _jsonl_object(payload)
    if type(value.get("commit_sequence")) is not int or value["commit_sequence"] != sequence:
        raise _error(
            NativeJournalErrorCode.OUTER_OWNERSHIP_MISMATCH,
            "rematerialized input belongs to another commit",
        )
    if value.get("input_id") != reference.record_id:
        raise _error(
            NativeJournalErrorCode.OUTER_OWNERSHIP_MISMATCH,
            "rematerialized input identity differs from raw record ID",
        )
    arrival = value.get("arrival_sequence")
    if arrival is not None and (type(arrival) is not int or arrival != sequence):
        raise _error(
            NativeJournalErrorCode.ARRIVAL_MISMATCH,
            "rematerialized input arrival differs from outer commit",
        )
    run_id = value.get("run_id")
    if run_id is not None and run_id != frame.run_id.value:
        raise _error(
            NativeJournalErrorCode.OUTER_OWNERSHIP_MISMATCH,
            "rematerialized input belongs to another run",
        )


def native_raw_reference_prefix_seed() -> Hash32:
    return framed_hash(NATIVE_RAW_REFERENCE_PREFIX_DOMAIN, b"")


def advance_native_raw_reference_prefix(
    previous: Hash32,
    reference: RawSegmentRef,
    stream_id: StreamId,
    ordinal: CommitOrdinal,
) -> Hash32:
    """Extend the authenticated ordered prefix of native raw references."""

    if (
        type(previous) is not Hash32
        or type(reference) is not RawSegmentReferenceV2
        or type(stream_id) is not StreamId
        or type(ordinal) is not CommitOrdinal
    ):
        raise _error(
            NativeJournalErrorCode.TYPE_INVALID,
            "raw reference prefix extension received a wrong typed value",
        )
    return framed_hash(
        NATIVE_RAW_REFERENCE_PREFIX_DOMAIN,
        frame_hash32(previous),
        frame_u64(int(reference.arrival_sequence)),
        frame_text(stream_id.value),
        frame_u32(int(ordinal)),
        frame_bytes(canonical_json_bytes(reference.canonical_value())),
    )


def rechain_native_frames(
    frames: Iterable[CommitFrame],
    replacements: Mapping[int, RawSegmentRef],
    resolver: RawReferenceResolverV2,
    *,
    start_prefix_root: Hash32,
) -> Iterator[CommitFrame]:
    """Stream source frames, replace selected inbox rows, and rechain exactly."""

    if not isinstance(frames, Iterable) or not isinstance(replacements, Mapping):
        raise _error(
            NativeJournalErrorCode.TYPE_INVALID,
            "native rechaining requires an iterable and a replacement mapping",
        )
    if type(start_prefix_root) is not Hash32:
        raise _error(NativeJournalErrorCode.TYPE_INVALID, "start prefix root must be Hash32")
    typed_replacements: dict[int, RawSegmentRef] = {}
    for sequence, replacement in replacements.items():
        if (
            type(sequence) is not int
            or sequence < 1
            or type(replacement) is not RawSegmentReferenceV2
        ):
            raise _error(
                NativeJournalErrorCode.TYPE_INVALID,
                "replacement keys and values must be positive ints and RawSegmentRef",
            )
        typed_replacements[sequence] = replacement

    expected_sequence = 1
    source_prefix = start_prefix_root
    native_prefix = start_prefix_root
    run_id: RunId | None = None
    matched: set[int] = set()
    seen_records: set[tuple[str, str, str]] = set()
    for frame in frames:
        if type(frame) is not CommitFrame:
            raise _error(NativeJournalErrorCode.TYPE_INVALID, "native rechaining requires CommitFrame")
        sequence = int(frame.commit_sequence)
        if sequence != expected_sequence:
            raise _error(
                NativeJournalErrorCode.COMMIT_SEQUENCE_INVALID,
                "source commit sequence has a gap or reorder",
            )
        if frame.previous_prefix_root != source_prefix:
            raise _error(
                NativeJournalErrorCode.PREFIX_DIVERGENCE,
                "source commit prefix chain diverges",
            )
        if run_id is None:
            run_id = frame.run_id
        elif frame.run_id != run_id:
            raise _error(NativeJournalErrorCode.RUN_MISMATCH, "source frame run identity changed")
        source_prefix = build_commit_logical(frame).prefix_root

        rows = list(frame.rows)
        reference = typed_replacements.get(sequence)
        if reference is not None:
            candidates = [
                (index, row)
                for index, row in enumerate(rows)
                if row.stream_id.value == "inbox"
            ]
            if len(candidates) != 1:
                raise _error(
                    NativeJournalErrorCode.OUTER_OWNERSHIP_MISMATCH,
                    "selected commit must contain exactly one inbox row",
                )
            index, original = candidates[0]
            if _contract_marker(original) != COMPATIBILITY_CONTRACT_MARKER:
                raise _error(
                    NativeJournalErrorCode.OUTER_OWNERSHIP_MISMATCH,
                    "selected inbox row is not a V3 compatibility record",
                )
            record_key = (
                reference.raw_store_id.value,
                reference.lake_id.value,
                reference.record_id,
            )
            if record_key in seen_records:
                raise _error(
                    NativeJournalErrorCode.DUPLICATE_RECORD_REFERENCE,
                    "raw record is referenced by more than one commit",
                )
            seen_records.add(record_key)
            original_payload = rematerialize_native_row(original, resolver)
            replacement_row = reference.to_logical_row(original.stream_id, original.ordinal)
            replacement_payload = rematerialize_native_row(replacement_row, resolver)
            _validate_outer_ownership(
                frame=frame,
                row=replacement_row,
                reference=reference,
                payload=replacement_payload,
            )
            if replacement_payload != original_payload:
                raise _error(
                    NativeJournalErrorCode.RAW_PAYLOAD_MISMATCH,
                    "native raw reference does not rematerialize the selected inbox row",
                )
            rows[index] = replacement_row
            matched.add(sequence)

        native_frame = CommitFrame(
            run_id=frame.run_id,
            commit_sequence=frame.commit_sequence,
            previous_prefix_root=native_prefix,
            rows=tuple(rows),
            legacy_v3_identity=frame.legacy_v3_identity,
        )
        native_prefix = build_commit_logical(native_frame).prefix_root
        expected_sequence += 1
        yield native_frame

    orphaned = sorted(set(typed_replacements) - matched)
    if orphaned:
        raise _error(
            NativeJournalErrorCode.ORPHAN_REFERENCE,
            f"replacement references have no source commit: {orphaned!r}",
        )


@dataclass(frozen=True, slots=True)
class NativeStreamExpectation:
    stream_id: StreamId
    row_count: int
    logical_sha256: Hash32

    def __post_init__(self) -> None:
        if type(self.stream_id) is not StreamId or type(self.logical_sha256) is not Hash32:
            raise _error(
                NativeJournalErrorCode.TYPE_INVALID,
                "stream expectation identities have wrong types",
            )
        if type(self.row_count) is not int or not 1 <= self.row_count <= UINT64_MAX:
            raise _error(
                NativeJournalErrorCode.TYPE_INVALID,
                "stream expectation count must be a positive uint64",
            )


@dataclass(frozen=True, slots=True)
class NativeAuditExpectations:
    run_id: RunId
    start_prefix_root: Hash32
    commit_count: int
    final_prefix_root: Hash32
    streams: tuple[NativeStreamExpectation, ...]
    market_gap_count: int
    raw_reference_count: int
    raw_manifest_roots: tuple[Hash32, ...]
    raw_last_record_id: str | None
    raw_reference_prefix_root: Hash32

    def __post_init__(self) -> None:
        if type(self.run_id) is not RunId:
            raise _error(NativeJournalErrorCode.TYPE_INVALID, "audit run_id must be RunId")
        for root in (
            self.start_prefix_root,
            self.final_prefix_root,
            self.raw_reference_prefix_root,
        ):
            if type(root) is not Hash32:
                raise _error(NativeJournalErrorCode.TYPE_INVALID, "audit roots must be Hash32")
        for label, value, minimum in (
            ("commit_count", self.commit_count, 1),
            ("market_gap_count", self.market_gap_count, 0),
            ("raw_reference_count", self.raw_reference_count, 0),
        ):
            if type(value) is not int or value < minimum or value > UINT64_MAX:
                raise _error(
                    NativeJournalErrorCode.TYPE_INVALID,
                    f"audit {label} is outside uint64 bounds",
                )
        if type(self.streams) is not tuple or any(
            type(item) is not NativeStreamExpectation for item in self.streams
        ):
            raise _error(
                NativeJournalErrorCode.TYPE_INVALID,
                "audit streams must be a tuple of NativeStreamExpectation",
            )
        stream_keys = tuple(item.stream_id.value.encode("utf-8") for item in self.streams)
        if stream_keys != tuple(sorted(stream_keys)) or len(set(stream_keys)) != len(stream_keys):
            raise _error(
                NativeJournalErrorCode.TYPE_INVALID,
                "audit stream expectations must be unique and UTF-8 sorted",
            )
        if type(self.raw_manifest_roots) is not tuple or any(
            type(root) is not Hash32 for root in self.raw_manifest_roots
        ):
            raise _error(
                NativeJournalErrorCode.TYPE_INVALID,
                "audit raw manifest roots must be a tuple of Hash32",
            )
        if len(set(self.raw_manifest_roots)) != len(self.raw_manifest_roots):
            raise _error(
                NativeJournalErrorCode.TYPE_INVALID,
                "audit raw manifest roots must be unique",
            )
        if self.raw_reference_count == 0:
            if self.raw_manifest_roots or self.raw_last_record_id is not None:
                raise _error(
                    NativeJournalErrorCode.TYPE_INVALID,
                    "zero raw references require no roots and no last record",
                )
        elif (
            not self.raw_manifest_roots
            or type(self.raw_last_record_id) is not str
            or not self.raw_last_record_id
        ):
            raise _error(
                NativeJournalErrorCode.TYPE_INVALID,
                "nonzero raw references require roots and a last record ID",
            )


@dataclass(frozen=True, slots=True)
class NativeAuditReport:
    commit_count: int
    final_prefix_root: Hash32
    streams: tuple[NativeStreamExpectation, ...]
    market_gap_count: int
    raw_reference_count: int
    raw_manifest_roots: tuple[Hash32, ...]
    raw_last_record_id: str | None
    raw_reference_prefix_root: Hash32


def audit_native_frames(
    frames: Iterable[CommitFrame],
    resolver: RawReferenceResolverV2,
    expectations: NativeAuditExpectations,
) -> NativeAuditReport:
    """Exhaustively audit frames while retaining only counters and digests."""

    if not isinstance(frames, Iterable) or type(expectations) is not NativeAuditExpectations:
        raise _error(
            NativeJournalErrorCode.TYPE_INVALID,
            "native audit requires a frame iterable and NativeAuditExpectations",
        )
    expected_sequence = 1
    previous_prefix = expectations.start_prefix_root
    counts: dict[StreamId, int] = {}
    hashes: dict[StreamId, object] = {}
    market_gap_count = 0
    raw_reference_count = 0
    raw_manifest_roots: list[Hash32] = []
    observed_manifest_roots: set[Hash32] = set()
    allowed_manifest_roots = set(expectations.raw_manifest_roots)
    raw_last_record_id: str | None = None
    raw_reference_prefix = native_raw_reference_prefix_seed()
    # Exact duplicate detection retains fixed-size identities, never rows or payload bytes.
    seen_reference_ids: set[Hash32] = set()

    for frame in frames:
        if type(frame) is not CommitFrame:
            raise _error(NativeJournalErrorCode.TYPE_INVALID, "native audit requires CommitFrame")
        if frame.run_id != expectations.run_id:
            raise _error(NativeJournalErrorCode.RUN_MISMATCH, "audited frame belongs to another run")
        if int(frame.commit_sequence) != expected_sequence:
            raise _error(
                NativeJournalErrorCode.COMMIT_SEQUENCE_INVALID,
                "audited commit sequence has a gap or reorder",
            )
        if frame.previous_prefix_root != previous_prefix:
            raise _error(
                NativeJournalErrorCode.PREFIX_DIVERGENCE,
                "audited commit prefix chain diverges",
            )

        for row in frame.rows:
            line = rematerialize_native_row(row, resolver)
            counts[row.stream_id] = counts.get(row.stream_id, 0) + 1
            digest = hashes.setdefault(row.stream_id, hashlib.sha256())
            digest.update(line)  # type: ignore[attr-defined]
            value = _jsonl_object(line)
            if row.stream_id.value == "alerts" and value.get("code") == "MARKET_GAP":
                market_gap_count += 1

            marker = _contract_marker(row)
            if marker == RAW_REFERENCE_CONTRACT_MARKER_V2:
                reference = _raw_reference_from_row(row)
                reference_id = Hash32(
                    hashlib.sha256(
                        frame_text(reference.lake_id.value)
                        + frame_text(reference.record_id)
                    ).digest()
                )
                if reference_id in seen_reference_ids:
                    raise _error(
                        NativeJournalErrorCode.DUPLICATE_RECORD_REFERENCE,
                        "raw record is referenced more than once",
                    )
                seen_reference_ids.add(reference_id)
                if reference.raw_manifest_root not in allowed_manifest_roots:
                    raise _error(
                        NativeJournalErrorCode.ORPHAN_REFERENCE,
                        "raw reference manifest root is not expected",
                    )
                if reference.raw_manifest_root not in observed_manifest_roots:
                    observed_manifest_roots.add(reference.raw_manifest_root)
                    raw_manifest_roots.append(reference.raw_manifest_root)
                _validate_outer_ownership(
                    frame=frame,
                    row=row,
                    reference=reference,
                    payload=line,
                )
                raw_reference_prefix = advance_native_raw_reference_prefix(
                    raw_reference_prefix,
                    reference,
                    row.stream_id,
                    row.ordinal,
                )
                raw_reference_count += 1
                raw_last_record_id = reference.record_id

        previous_prefix = build_commit_logical(frame).prefix_root
        expected_sequence += 1

    commit_count = expected_sequence - 1
    observed_streams = tuple(
        NativeStreamExpectation(
            stream_id=stream_id,
            row_count=counts[stream_id],
            logical_sha256=Hash32(hashes[stream_id].digest()),  # type: ignore[attr-defined]
        )
        for stream_id in sorted(counts, key=lambda item: item.value.encode("utf-8"))
    )
    report = NativeAuditReport(
        commit_count=commit_count,
        final_prefix_root=previous_prefix,
        streams=observed_streams,
        market_gap_count=market_gap_count,
        raw_reference_count=raw_reference_count,
        raw_manifest_roots=tuple(raw_manifest_roots),
        raw_last_record_id=raw_last_record_id,
        raw_reference_prefix_root=raw_reference_prefix,
    )

    if report.commit_count != expectations.commit_count:
        raise _error(NativeJournalErrorCode.EXPECTATION_MISMATCH, "commit count differs")
    if report.final_prefix_root != expectations.final_prefix_root:
        raise _error(NativeJournalErrorCode.PREFIX_DIVERGENCE, "final prefix root differs")
    if report.streams != expectations.streams:
        raise _error(
            NativeJournalErrorCode.EXPECTATION_MISMATCH,
            "per-stream ordered counts or SHA-256 differ",
        )
    if report.market_gap_count != expectations.market_gap_count:
        raise _error(NativeJournalErrorCode.EXPECTATION_MISMATCH, "MARKET_GAP count differs")
    if report.raw_reference_count != expectations.raw_reference_count:
        raise _error(NativeJournalErrorCode.EXPECTATION_MISMATCH, "raw reference count differs")
    if report.raw_manifest_roots != expectations.raw_manifest_roots:
        raise _error(NativeJournalErrorCode.EXPECTATION_MISMATCH, "raw manifest roots differ")
    if report.raw_last_record_id != expectations.raw_last_record_id:
        raise _error(NativeJournalErrorCode.EXPECTATION_MISMATCH, "last raw record differs")
    if report.raw_reference_prefix_root != expectations.raw_reference_prefix_root:
        raise _error(
            NativeJournalErrorCode.PREFIX_DIVERGENCE,
            "raw reference prefix root differs",
        )
    return report


__all__ = [
    "NATIVE_CHECKPOINT_ADAPTER_CONTRACT",
    "NATIVE_CHECKPOINT_BINDING_CONTRACT",
    "NATIVE_CHECKPOINT_BINDING_FORMAT_VERSION",
    "NATIVE_RAW_REFERENCE_PREFIX_DOMAIN",
    "NativeAuditExpectations",
    "NativeAuditReport",
    "NativeCheckpointBinding",
    "NativeJournalError",
    "NativeJournalErrorCode",
    "NativeStreamExpectation",
    "advance_native_raw_reference_prefix",
    "audit_native_frames",
    "bind_native_checkpoint_state",
    "native_raw_reference_prefix_seed",
    "rechain_native_frames",
    "rematerialize_native_row",
    "unbind_native_checkpoint_state",
]
