"""Deterministic offline orchestration for Storage V4 Phase 1C.

The coordinator deliberately keeps the two authorities separate.  A raw batch
is durably published before any Paper frame can contain a reference to it.  A
Paper checkpoint then binds the latest raw authority and the ordered prefix of
all references.  Normal reopen authenticates only the raw head and the Paper
checkpoint/tail; exhaustive raw resolution is deferred to certification.

This module contains no CLI, network access, live-order path, or benchmark
policy.  Golden and synthetic-capacity runners supply source frames and raw
metadata through the public batch contracts below.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from itertools import chain
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from .canonical import build_commit_logical
from .capacity import (
    CapacityWorkloadHasher,
    CapacityWorkloadManifest,
    SyntheticCapacityCommit,
)
from .checkpoint import CheckpointState
from .contracts import CompatibilityRecord, CompatibilityRecordError, StorageMode
from .faults import FaultHook, FaultPoint, trigger_fault
from .native_journal import (
    NativeAuditExpectations,
    NativeAuditReport,
    NativeCheckpointBinding,
    NativeJournalError,
    NativeStreamExpectation,
    advance_native_raw_reference_prefix,
    audit_native_frames,
    bind_native_checkpoint_state,
    native_raw_reference_prefix_seed,
    rematerialize_native_row,
    unbind_native_checkpoint_state,
)
from .overlay import OverlayTailDiscardResult
from .raw_reference import (
    RAW_REFERENCE_CONTRACT_MARKER_V2,
    RawReferenceResolverV2,
    RawSegmentRef,
    raw_reference_v2_from_row,
)
from .raw_segment import (
    RawRecordMetadata,
    RawSegmentError,
    RawSegmentErrorCode,
    RawSegmentThresholds,
    RawSegmentWriter,
)
from .raw_store import (
    DiskRawResolver,
    RawAuditReport,
    RawSealResult,
    RawStartupReport,
    RawStore,
    RawStoreConfig,
    RawStoreError,
    RawSuffixReattestationReport,
)
from .repository import (
    AuditReport,
    RepositoryConfig,
    RepositoryError,
    SealResult,
    StartupReport,
    StorageRepository,
)
from .types import CommitFrame, Hash32, LogicalRow, RunId, StreamId


class Phase1CPipelineErrorCode(StrEnum):
    """Stable fail-closed categories exposed to offline runners."""

    TYPE_INVALID = "PHASE1C_TYPE_INVALID"
    STATE_INVALID = "PHASE1C_STATE_INVALID"
    EMPTY_BATCH = "PHASE1C_EMPTY_BATCH"
    SOURCE_DIVERGENCE = "PHASE1C_SOURCE_DIVERGENCE"
    RAW_INPUT_INVALID = "PHASE1C_RAW_INPUT_INVALID"
    RAW_PUBLICATION_FAILED = "PHASE1C_RAW_PUBLICATION_FAILED"
    NATIVE_RECHAIN_FAILED = "PHASE1C_NATIVE_RECHAIN_FAILED"
    PAPER_APPEND_FAILED = "PHASE1C_PAPER_APPEND_FAILED"
    PAPER_SEAL_FAILED = "PHASE1C_PAPER_SEAL_FAILED"
    CHECKPOINT_BINDING_INVALID = "PHASE1C_CHECKPOINT_BINDING_INVALID"
    PAPER_WITHOUT_RAW_AUTHORITY = "PHASE1C_PAPER_WITHOUT_RAW_AUTHORITY"
    AUTHORITY_MISALIGNED = "PHASE1C_AUTHORITY_MISALIGNED"
    STARTUP_SCOPE_VIOLATION = "PHASE1C_STARTUP_SCOPE_VIOLATION"
    AUDIT_FAILED = "PHASE1C_AUDIT_FAILED"
    CAPACITY_WORKLOAD_DIVERGENCE = "PHASE1C_CAPACITY_WORKLOAD_DIVERGENCE"
    RESUME_INVALID = "PHASE1C_RESUME_INVALID"


class Phase1CPipelineError(RuntimeError):
    """Structured Phase 1C failure with bounded diagnostic details."""

    def __init__(
        self,
        code: Phase1CPipelineErrorCode,
        message: str,
        **details: object,
    ) -> None:
        if type(code) is not Phase1CPipelineErrorCode:
            raise TypeError("pipeline error code must be Phase1CPipelineErrorCode")
        self.code = code
        self.details = MappingProxyType(dict(details))
        super().__init__(f"{code.value}: {message}")


def _error(
    code: Phase1CPipelineErrorCode,
    message: str,
    **details: object,
) -> Phase1CPipelineError:
    return Phase1CPipelineError(code, message, **details)


@dataclass(frozen=True, slots=True)
class NativeRawRecord:
    """Exact canonical JSONL payload selected for one native inbox reference."""

    commit_sequence: int
    payload: bytes
    metadata: RawRecordMetadata

    def __post_init__(self) -> None:
        if type(self.commit_sequence) is not int or self.commit_sequence < 1:
            raise _error(
                Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
                "raw record commit sequence must be a positive exact integer",
            )
        if type(self.payload) is not bytes or type(self.metadata) is not RawRecordMetadata:
            raise _error(
                Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
                "raw record requires exact bytes and RawRecordMetadata",
            )
        if int(self.metadata.arrival_sequence) != self.commit_sequence:
            raise _error(
                Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
                "raw arrival sequence differs from its selected commit",
                commit_sequence=self.commit_sequence,
            )
        try:
            CompatibilityRecord.from_jsonl_bytes(self.payload)
        except (CompatibilityRecordError, TypeError, ValueError) as exc:
            raise _error(
                Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
                "raw native payload is not one exact canonical JSONL object",
                commit_sequence=self.commit_sequence,
            ) from exc


@dataclass(frozen=True, slots=True)
class Phase1CBatch:
    """One bounded source-frame batch and the subset published to the raw lake."""

    source_frames: tuple[CommitFrame, ...]
    raw_records: tuple[NativeRawRecord, ...] = ()

    def __post_init__(self) -> None:
        if type(self.source_frames) is not tuple or any(
            type(frame) is not CommitFrame for frame in self.source_frames
        ):
            raise _error(
                Phase1CPipelineErrorCode.TYPE_INVALID,
                "batch source_frames must be a tuple of CommitFrame",
            )
        if not self.source_frames:
            raise _error(Phase1CPipelineErrorCode.EMPTY_BATCH, "Phase 1C batch is empty")
        if type(self.raw_records) is not tuple or any(
            type(record) is not NativeRawRecord for record in self.raw_records
        ):
            raise _error(
                Phase1CPipelineErrorCode.TYPE_INVALID,
                "batch raw_records must be a tuple of NativeRawRecord",
            )
        sequences = tuple(record.commit_sequence for record in self.raw_records)
        if tuple(sorted(sequences)) != sequences or len(set(sequences)) != len(sequences):
            raise _error(
                Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
                "batch raw records must be unique and ordered by commit sequence",
            )


@dataclass(frozen=True, slots=True)
class NativeBatchCursor:
    """Serializable state needed to rechain independently published raw batches."""

    run_id: RunId
    next_commit_sequence: int
    source_prefix_root: Hash32
    native_prefix_root: Hash32
    raw_reference_prefix_root: Hash32
    raw_reference_count: int
    raw_last_record_id: str | None
    raw_manifest_roots: tuple[Hash32, ...]

    def __post_init__(self) -> None:
        if type(self.run_id) is not RunId:
            raise _error(Phase1CPipelineErrorCode.TYPE_INVALID, "cursor run_id is invalid")
        if type(self.next_commit_sequence) is not int or self.next_commit_sequence < 1:
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "cursor next sequence must be a positive exact integer",
            )
        for value in (
            self.source_prefix_root,
            self.native_prefix_root,
            self.raw_reference_prefix_root,
        ):
            if type(value) is not Hash32:
                raise _error(Phase1CPipelineErrorCode.TYPE_INVALID, "cursor root is invalid")
        if type(self.raw_reference_count) is not int or self.raw_reference_count < 0:
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "cursor raw reference count is invalid",
            )
        if self.raw_reference_count == 0:
            if self.raw_last_record_id is not None or self.raw_manifest_roots:
                raise _error(
                    Phase1CPipelineErrorCode.STATE_INVALID,
                    "empty raw cursor cannot carry raw authority",
                )
        elif not self.raw_last_record_id or not self.raw_manifest_roots:
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "nonempty raw cursor requires roots and last record ID",
            )
        if type(self.raw_manifest_roots) is not tuple or any(
            type(root) is not Hash32 for root in self.raw_manifest_roots
        ):
            raise _error(
                Phase1CPipelineErrorCode.TYPE_INVALID,
                "cursor raw manifest roots are invalid",
            )
        if len(set(self.raw_manifest_roots)) != len(self.raw_manifest_roots):
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "cursor raw manifest roots must be unique",
            )

    @classmethod
    def genesis(cls, *, run_id: RunId, start_prefix_root: Hash32) -> NativeBatchCursor:
        return cls(
            run_id=run_id,
            next_commit_sequence=1,
            source_prefix_root=start_prefix_root,
            native_prefix_root=start_prefix_root,
            raw_reference_prefix_root=native_raw_reference_prefix_seed(),
            raw_reference_count=0,
            raw_last_record_id=None,
            raw_manifest_roots=(),
        )


@dataclass(frozen=True, slots=True)
class Phase1CBatchResult:
    native_frames: tuple[CommitFrame, ...]
    raw_seals: tuple[RawSealResult, ...]
    cursor: NativeBatchCursor

    @property
    def raw_seal(self) -> RawSealResult | None:
        """Return the terminal raw seal for backwards-compatible callers."""

        return None if not self.raw_seals else self.raw_seals[-1]


@dataclass(frozen=True, slots=True)
class Phase1CSealResult:
    paper_seal: SealResult
    binding: NativeCheckpointBinding
    expectations: NativeAuditExpectations


@dataclass(frozen=True, slots=True)
class Phase1CResumeResult:
    writer: Phase1CWriter
    checkpoint_state: CheckpointState
    binding: NativeCheckpointBinding
    boundary_audit: NativeAuditReport
    raw_suffix: RawSuffixReattestationReport
    suffix_references: tuple[RawSegmentRef, ...]
    discarded_tail: OverlayTailDiscardResult


class Phase1CAuthorityStatus(StrEnum):
    EMPTY = "PHASE1C_EMPTY"
    PAPER_TAIL_WITHOUT_RAW = "PHASE1C_PAPER_TAIL_WITHOUT_RAW"
    RAW_VALID_PAPER_ABSENT = "PHASE1C_RAW_VALID_PAPER_ABSENT"
    RAW_VALID_PAPER_TAIL = "PHASE1C_RAW_VALID_PAPER_TAIL"
    RAW_AHEAD_OF_PAPER = "PHASE1C_RAW_AHEAD_OF_PAPER"
    ALIGNED = "PHASE1C_RAW_PAPER_ALIGNED"


@dataclass(frozen=True, slots=True)
class Phase1CAuthorityReport:
    status: Phase1CAuthorityStatus
    raw_generation: int
    raw_manifest_root: Hash32 | None
    raw_record_count: int
    paper_generation: int
    paper_manifest_root: Hash32 | None
    paper_tail_commit_count: int
    binding: NativeCheckpointBinding | None


@dataclass(frozen=True, slots=True)
class Phase1CCertificationReport:
    raw_startup: RawStartupReport
    paper_startup: StartupReport
    alignment: Phase1CAuthorityReport
    raw_audit: RawAuditReport
    paper_audit: AuditReport
    native_audit: NativeAuditReport
    raw_resolver_physical_hash_passes: int


@dataclass(slots=True)
class _StreamAccumulator:
    count: int = 0
    digest: Any = field(default_factory=hashlib.sha256)


def _rehydrate_authenticated_boundary(
    raw_store: RawStore,
    paper_repository: StorageRepository,
    *,
    binding: NativeCheckpointBinding,
    source_prefix_root: Hash32,
) -> tuple[
    NativeBatchCursor,
    dict[StreamId, _StreamAccumulator],
    int,
    NativeAuditReport,
]:
    checkpoint = paper_repository.checkpoint
    if checkpoint is None:
        raise _error(
            Phase1CPipelineErrorCode.RESUME_INVALID,
            "resume requires a published Paper checkpoint",
        )
    resolver = DiskRawResolver(raw_store)
    streams: dict[StreamId, _StreamAccumulator] = {}
    market_gap_count = 0
    expected_sequence = 1
    previous_prefix = paper_repository.config.start_prefix_root
    reference_prefix = native_raw_reference_prefix_seed()
    reference_count = 0
    last_record_id: str | None = None
    manifest_roots: list[Hash32] = []
    manifest_root_set: set[Hash32] = set()

    for frame in paper_repository.iter_historical_frames():
        sequence = int(frame.commit_sequence)
        if (
            frame.run_id != paper_repository.config.run_id
            or sequence != expected_sequence
            or frame.previous_prefix_root != previous_prefix
        ):
            raise _error(
                Phase1CPipelineErrorCode.RESUME_INVALID,
                "published Paper prefix is not one contiguous authenticated run",
            )
        for row in frame.rows:
            try:
                line = rematerialize_native_row(row, resolver)
            except (NativeJournalError, RawStoreError, TypeError, ValueError) as exc:
                raise _error(
                    Phase1CPipelineErrorCode.RESUME_INVALID,
                    "published native row cannot be rematerialized during resume",
                ) from exc
            accumulator = streams.setdefault(row.stream_id, _StreamAccumulator())
            accumulator.count += 1
            accumulator.digest.update(line)
            value = _payload_object(line)
            if row.stream_id == StreamId("alerts") and value.get("code") == "MARKET_GAP":
                market_gap_count += 1
            if type(row.value) is dict and row.value.get("contract") == (
                RAW_REFERENCE_CONTRACT_MARKER_V2
            ):
                try:
                    reference = raw_reference_v2_from_row(row)
                except (TypeError, ValueError) as exc:
                    raise _error(
                        Phase1CPipelineErrorCode.RESUME_INVALID,
                        "published raw reference is malformed during resume",
                    ) from exc
                reference_prefix = advance_native_raw_reference_prefix(
                    reference_prefix,
                    reference,
                    row.stream_id,
                    row.ordinal,
                )
                reference_count += 1
                last_record_id = reference.record_id
                if reference.raw_manifest_root not in manifest_root_set:
                    manifest_root_set.add(reference.raw_manifest_root)
                    manifest_roots.append(reference.raw_manifest_root)
        previous_prefix = build_commit_logical(frame).prefix_root
        expected_sequence += 1

    commit_count = expected_sequence - 1
    expectations = NativeAuditExpectations(
        run_id=paper_repository.config.run_id,
        start_prefix_root=paper_repository.config.start_prefix_root,
        commit_count=commit_count,
        final_prefix_root=previous_prefix,
        streams=tuple(
            NativeStreamExpectation(
                stream_id=stream_id,
                row_count=accumulator.count,
                logical_sha256=Hash32(accumulator.digest.digest()),
            )
            for stream_id, accumulator in sorted(
                streams.items(), key=lambda item: item[0].value.encode("utf-8")
            )
        ),
        market_gap_count=market_gap_count,
        raw_reference_count=reference_count,
        raw_manifest_roots=tuple(manifest_roots),
        raw_last_record_id=last_record_id,
        raw_reference_prefix_root=reference_prefix,
    )
    checkpoint_counts = tuple(
        (item.stream_id, item.row_count) for item in expectations.streams
    )
    if (
        commit_count != int(checkpoint.covered_commit_sequence)
        or previous_prefix != checkpoint.covered_prefix_root
        or checkpoint_counts != checkpoint.cumulative_stream_counts
        or reference_count != binding.raw_record_count
        or last_record_id != binding.raw_last_record_id
        or reference_prefix != binding.raw_reference_prefix_root
        or not manifest_roots
        or manifest_roots[-1] != binding.raw_manifest_root
    ):
        raise _error(
            Phase1CPipelineErrorCode.RESUME_INVALID,
            "checkpoint, raw binding, and rehydrated Paper prefix diverge",
        )
    try:
        audit = audit_native_frames(
            paper_repository.iter_historical_frames(),
            resolver,
            expectations,
        )
    except (NativeJournalError, RawStoreError, TypeError, ValueError) as exc:
        raise _error(
            Phase1CPipelineErrorCode.RESUME_INVALID,
            "independent native boundary audit failed during resume",
        ) from exc
    cursor = NativeBatchCursor(
        run_id=paper_repository.config.run_id,
        next_commit_sequence=commit_count + 1,
        source_prefix_root=source_prefix_root,
        native_prefix_root=previous_prefix,
        raw_reference_prefix_root=reference_prefix,
        raw_reference_count=reference_count,
        raw_last_record_id=last_record_id,
        raw_manifest_roots=tuple(manifest_roots),
    )
    return cursor, streams, market_gap_count, audit


def _payload_object(payload: bytes) -> dict[str, object]:
    try:
        value = json.loads(payload[:-1].decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _error(
            Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
            "raw payload cannot be decoded after canonical validation",
        ) from exc
    if type(value) is not dict:
        raise _error(
            Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
            "raw payload must own one JSON object",
        )
    return value


def _validate_payload_ownership(
    *,
    frame: CommitFrame,
    record: NativeRawRecord,
) -> None:
    value = _payload_object(record.payload)
    sequence = int(frame.commit_sequence)
    if value.get("commit_sequence") != sequence or type(value.get("commit_sequence")) is not int:
        raise _error(
            Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
            "raw payload belongs to another commit",
            commit_sequence=sequence,
        )
    if value.get("input_id") != record.metadata.record_id:
        raise _error(
            Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
            "raw payload input ID differs from metadata",
            commit_sequence=sequence,
        )
    arrival = value.get("arrival_sequence")
    if arrival is not None and (type(arrival) is not int or arrival != sequence):
        raise _error(
            Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
            "raw payload arrival sequence differs from outer commit",
            commit_sequence=sequence,
        )
    run_id = value.get("run_id")
    if run_id is not None and run_id != frame.run_id.value:
        raise _error(
            Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
            "raw payload run identity differs from outer frame",
            commit_sequence=sequence,
        )


def _compatibility_inbox(frame: CommitFrame) -> tuple[int, LogicalRow, bytes]:
    candidates = [(index, row) for index, row in enumerate(frame.rows) if row.stream_id == StreamId("inbox")]
    if len(candidates) != 1:
        raise _error(
            Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
            "selected commit must contain exactly one inbox row",
            commit_sequence=int(frame.commit_sequence),
        )
    index, row = candidates[0]
    if int(row.ordinal) != 0:
        raise _error(
            Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
            "selected inbox row must own ordinal zero",
            commit_sequence=int(frame.commit_sequence),
        )
    try:
        payload = CompatibilityRecord.from_logical_row(row).jsonl_bytes
    except (CompatibilityRecordError, TypeError, ValueError) as exc:
        raise _error(
            Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
            "selected inbox row is not an exact compatibility record",
            commit_sequence=int(frame.commit_sequence),
        ) from exc
    return index, row, payload


def _preflight_batch(batch: Phase1CBatch, cursor: NativeBatchCursor) -> None:
    records = {record.commit_sequence: record for record in batch.raw_records}
    expected = cursor.next_commit_sequence
    prefix = cursor.source_prefix_root
    seen: set[tuple[str, str]] = set()
    for frame in batch.source_frames:
        sequence = int(frame.commit_sequence)
        if frame.run_id != cursor.run_id or sequence != expected:
            raise _error(
                Phase1CPipelineErrorCode.SOURCE_DIVERGENCE,
                "source frames changed run or lost contiguous commit order",
                expected_sequence=expected,
                observed_sequence=sequence,
            )
        if frame.previous_prefix_root != prefix:
            raise _error(
                Phase1CPipelineErrorCode.SOURCE_DIVERGENCE,
                "source frame prefix chain diverges",
                commit_sequence=sequence,
            )
        record = records.get(sequence)
        if record is not None:
            _, _, source_payload = _compatibility_inbox(frame)
            if source_payload != record.payload:
                raise _error(
                    Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
                    "raw payload differs from selected source inbox bytes",
                    commit_sequence=sequence,
                )
            _validate_payload_ownership(frame=frame, record=record)
            key = (record.metadata.source_id, record.metadata.record_id)
            if key in seen:
                raise _error(
                    Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
                    "raw source and record identity is duplicated",
                    commit_sequence=sequence,
                )
            seen.add(key)
        prefix = build_commit_logical(frame).prefix_root
        expected += 1
    orphaned = sorted(set(records) - {int(frame.commit_sequence) for frame in batch.source_frames})
    if orphaned:
        raise _error(
            Phase1CPipelineErrorCode.RAW_INPUT_INVALID,
            "raw records have no source commit in this batch",
            orphaned_sequences=orphaned,
        )


def _rechain_batch(
    *,
    batch: Phase1CBatch,
    references: dict[int, RawSegmentRef],
    resolver: RawReferenceResolverV2,
    cursor: NativeBatchCursor,
) -> tuple[tuple[CommitFrame, ...], NativeBatchCursor]:
    expected = cursor.next_commit_sequence
    source_prefix = cursor.source_prefix_root
    native_prefix = cursor.native_prefix_root
    reference_prefix = cursor.raw_reference_prefix_root
    reference_count = cursor.raw_reference_count
    last_record = cursor.raw_last_record_id
    roots = list(cursor.raw_manifest_roots)
    root_set = set(roots)
    seen: set[tuple[str, str]] = set()
    output: list[CommitFrame] = []
    matched: set[int] = set()

    for frame in batch.source_frames:
        sequence = int(frame.commit_sequence)
        if sequence != expected or frame.previous_prefix_root != source_prefix:
            raise _error(
                Phase1CPipelineErrorCode.SOURCE_DIVERGENCE,
                "source changed after raw publication",
                expected_sequence=expected,
                observed_sequence=sequence,
            )
        source_prefix = build_commit_logical(frame).prefix_root
        rows = list(frame.rows)
        reference = references.get(sequence)
        if reference is not None:
            index, original, original_payload = _compatibility_inbox(frame)
            replacement = reference.to_logical_row(original.stream_id, original.ordinal)
            try:
                replacement_payload = rematerialize_native_row(replacement, resolver)
            except (NativeJournalError, RawStoreError, TypeError, ValueError) as exc:
                raise _error(
                    Phase1CPipelineErrorCode.NATIVE_RECHAIN_FAILED,
                    "published raw reference cannot be rematerialized",
                    commit_sequence=sequence,
                ) from exc
            if replacement_payload != original_payload:
                raise _error(
                    Phase1CPipelineErrorCode.NATIVE_RECHAIN_FAILED,
                    "published raw reference differs from source inbox bytes",
                    commit_sequence=sequence,
                )
            if int(reference.arrival_sequence) != sequence:
                raise _error(
                    Phase1CPipelineErrorCode.NATIVE_RECHAIN_FAILED,
                    "published raw reference has wrong arrival ownership",
                    commit_sequence=sequence,
                )
            value = _payload_object(replacement_payload)
            if value.get("input_id") != reference.record_id:
                raise _error(
                    Phase1CPipelineErrorCode.NATIVE_RECHAIN_FAILED,
                    "published reference record ID differs from payload",
                    commit_sequence=sequence,
                )
            key = (reference.source_id, reference.record_id)
            if key in seen:
                raise _error(
                    Phase1CPipelineErrorCode.NATIVE_RECHAIN_FAILED,
                    "published raw reference is duplicated",
                    commit_sequence=sequence,
                )
            seen.add(key)
            rows[index] = replacement
            reference_prefix = advance_native_raw_reference_prefix(
                reference_prefix,
                reference,
                replacement.stream_id,
                replacement.ordinal,
            )
            reference_count += 1
            last_record = reference.record_id
            if reference.raw_manifest_root not in root_set:
                root_set.add(reference.raw_manifest_root)
                roots.append(reference.raw_manifest_root)
            matched.add(sequence)

        native = CommitFrame(
            run_id=frame.run_id,
            commit_sequence=frame.commit_sequence,
            previous_prefix_root=native_prefix,
            rows=tuple(rows),
            legacy_v3_identity=frame.legacy_v3_identity,
        )
        native_prefix = build_commit_logical(native).prefix_root
        output.append(native)
        expected += 1

    if matched != set(references):
        raise _error(
            Phase1CPipelineErrorCode.NATIVE_RECHAIN_FAILED,
            "published references do not map one-to-one to source frames",
        )
    return tuple(output), NativeBatchCursor(
        run_id=cursor.run_id,
        next_commit_sequence=expected,
        source_prefix_root=source_prefix,
        native_prefix_root=native_prefix,
        raw_reference_prefix_root=reference_prefix,
        raw_reference_count=reference_count,
        raw_last_record_id=last_record,
        raw_manifest_roots=tuple(roots),
    )


class Phase1CWriter:
    """Fresh-run streaming coordinator with raw-before-Paper ordering."""

    def __init__(
        self,
        *,
        raw_store: RawStore,
        paper_repository: StorageRepository,
        staging_directory: Path,
        raw_thresholds: RawSegmentThresholds | None = None,
        fault_hook: FaultHook = None,
    ) -> None:
        if type(raw_store) is not RawStore or type(paper_repository) is not StorageRepository:
            raise _error(
                Phase1CPipelineErrorCode.TYPE_INVALID,
                "writer requires RawStore and StorageRepository",
            )
        if not isinstance(staging_directory, Path) or not staging_directory.is_dir():
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "raw staging directory is missing",
            )
        if raw_thresholds is not None and type(raw_thresholds) is not RawSegmentThresholds:
            raise _error(
                Phase1CPipelineErrorCode.TYPE_INVALID,
                "raw thresholds must be RawSegmentThresholds",
            )
        paper_config = paper_repository.config
        if paper_config.mode is not StorageMode.V4_NATIVE:
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "Paper repository is not configured for V4_NATIVE",
            )
        if raw_store.config.config_identity != paper_config.config_identity.digest:
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "raw and Paper configuration identities differ",
            )
        overlay = paper_repository.overlay_state
        if (
            raw_store.manifest is not None
            or paper_repository.manifest is not None
            or overlay.tail_commit_count != 0
        ):
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "Phase 1C writer requires fresh empty authorities",
            )
        self._raw_store = raw_store
        self._paper = paper_repository
        self._staging = staging_directory
        self._thresholds = raw_thresholds
        self._fault_hook = fault_hook
        self._cursor = NativeBatchCursor.genesis(
            run_id=paper_config.run_id,
            start_prefix_root=paper_config.start_prefix_root,
        )
        self._streams: dict[StreamId, _StreamAccumulator] = {}
        self._market_gap_count = 0
        self._poisoned = False

    @classmethod
    def resume_from_authenticated_checkpoint(
        cls,
        *,
        raw_store: RawStore,
        paper_repository: StorageRepository,
        staging_directory: Path,
        source_prefix_root: Hash32,
        raw_thresholds: RawSegmentThresholds | None = None,
        fault_hook: FaultHook = None,
    ) -> Phase1CResumeResult:
        """Resume at the last sealed boundary and expose any raw-only suffix."""

        if type(raw_store) is not RawStore or type(paper_repository) is not StorageRepository:
            raise _error(
                Phase1CPipelineErrorCode.TYPE_INVALID,
                "resume requires RawStore and StorageRepository",
            )
        if not isinstance(staging_directory, Path) or not staging_directory.is_dir():
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "resume raw staging directory is missing",
            )
        if type(source_prefix_root) is not Hash32:
            raise _error(
                Phase1CPipelineErrorCode.TYPE_INVALID,
                "resume source prefix must be Hash32",
            )
        if raw_thresholds is not None and type(raw_thresholds) is not RawSegmentThresholds:
            raise _error(
                Phase1CPipelineErrorCode.TYPE_INVALID,
                "resume raw thresholds must be RawSegmentThresholds",
            )
        paper_config = paper_repository.config
        if (
            paper_config.mode is not StorageMode.V4_NATIVE
            or raw_store.config.config_identity != paper_config.config_identity.digest
        ):
            raise _error(
                Phase1CPipelineErrorCode.RESUME_INVALID,
                "resume raw and Paper authorities have incompatible identities",
            )
        paper_manifest = paper_repository.manifest
        paper_checkpoint = paper_repository.checkpoint
        if paper_manifest is None or paper_checkpoint is None:
            raise _error(
                Phase1CPipelineErrorCode.RESUME_INVALID,
                "resume requires an authenticated Paper manifest and checkpoint",
            )
        try:
            checkpoint_state, binding = unbind_native_checkpoint_state(
                paper_checkpoint.state
            )
        except (NativeJournalError, TypeError, ValueError) as exc:
            raise _error(
                Phase1CPipelineErrorCode.RESUME_INVALID,
                "resume checkpoint has no valid native binding",
            ) from exc
        if (
            binding.raw_store_id != raw_store.config.store_id
            or binding.raw_lake_id != raw_store.config.lake_id
            or binding.raw_config_identity != raw_store.config.config_identity
        ):
            raise _error(
                Phase1CPipelineErrorCode.RESUME_INVALID,
                "resume checkpoint raw identities differ",
            )
        try:
            boundary_manifest = raw_store.authenticated_manifest(binding.raw_manifest_root)
            next_arrival = boundary_manifest.segments[-1].last_arrival_sequence + 1
            raw_suffix = raw_store.reattest_contiguous_suffix(
                boundary_manifest_root=binding.raw_manifest_root,
                next_arrival_sequence=next_arrival,
            )
            suffix_references = raw_store.authenticated_suffix_references(
                boundary_manifest_root=binding.raw_manifest_root,
                next_arrival_sequence=next_arrival,
            )
        except (RawStoreError, TypeError, ValueError) as exc:
            raise _error(
                Phase1CPipelineErrorCode.RESUME_INVALID,
                "resume raw boundary or suffix failed authentication",
            ) from exc
        if len(suffix_references) != raw_suffix.suffix_records_read:
            raise _error(
                Phase1CPipelineErrorCode.RESUME_INVALID,
                "resume raw suffix reference count differs",
            )
        try:
            discarded_tail = paper_repository.discard_unsealed_tail(
                expected_manifest_root=paper_manifest.identity.root,
                expected_checkpoint_root=paper_checkpoint.root,
            )
            cursor, streams, market_gap_count, boundary_audit = (
                _rehydrate_authenticated_boundary(
                    raw_store,
                    paper_repository,
                    binding=binding,
                    source_prefix_root=source_prefix_root,
                )
            )
        except (NativeJournalError, RawStoreError, RepositoryError, TypeError, ValueError) as exc:
            if isinstance(exc, Phase1CPipelineError):
                raise
            raise _error(
                Phase1CPipelineErrorCode.RESUME_INVALID,
                "resume Paper boundary failed reattestation",
            ) from exc

        writer = cls.__new__(cls)
        writer._raw_store = raw_store
        writer._paper = paper_repository
        writer._staging = staging_directory
        writer._thresholds = raw_thresholds
        writer._fault_hook = fault_hook
        writer._cursor = cursor
        writer._streams = streams
        writer._market_gap_count = market_gap_count
        writer._poisoned = False
        return Phase1CResumeResult(
            writer=writer,
            checkpoint_state=checkpoint_state,
            binding=binding,
            boundary_audit=boundary_audit,
            raw_suffix=raw_suffix,
            suffix_references=suffix_references,
            discarded_tail=discarded_tail,
        )

    @property
    def cursor(self) -> NativeBatchCursor:
        return self._cursor

    def _ensure_writable(self) -> None:
        if self._poisoned:
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "Phase 1C writer failed and cannot continue",
            )

    def audit_expectations(self) -> NativeAuditExpectations:
        """Snapshot exact native expectations without sealing the Paper tail."""

        self._ensure_writable()
        raw_manifest = self._raw_store.manifest
        if raw_manifest is None or self._cursor.raw_reference_count == 0:
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "native expectations require at least one published raw reference",
            )
        if raw_manifest.total_record_count != self._cursor.raw_reference_count:
            raise _error(
                Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
                "raw authority contains records not owned by the Paper prefix",
            )
        last_record = self._cursor.raw_last_record_id
        if last_record is None or raw_manifest.segments[-1].last_record_id != last_record:
            raise _error(
                Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
                "raw authority last record differs from the Paper reference prefix",
            )
        return NativeAuditExpectations(
            run_id=self._cursor.run_id,
            start_prefix_root=self._paper.config.start_prefix_root,
            commit_count=self._cursor.next_commit_sequence - 1,
            final_prefix_root=self._cursor.native_prefix_root,
            streams=tuple(
                NativeStreamExpectation(
                    stream_id=stream_id,
                    row_count=accumulator.count,
                    logical_sha256=Hash32(accumulator.digest.digest()),
                )
                for stream_id, accumulator in sorted(
                    self._streams.items(), key=lambda item: item[0].value.encode("utf-8")
                )
            ),
            market_gap_count=self._market_gap_count,
            raw_reference_count=self._cursor.raw_reference_count,
            raw_manifest_roots=self._cursor.raw_manifest_roots,
            raw_last_record_id=last_record,
            raw_reference_prefix_root=self._cursor.raw_reference_prefix_root,
        )

    def _observe_native_frames(self, native_frames: tuple[CommitFrame, ...]) -> None:
        resolver = DiskRawResolver(self._raw_store)
        for frame in native_frames:
            for row in frame.rows:
                try:
                    line = rematerialize_native_row(row, resolver)
                except (NativeJournalError, RawStoreError, TypeError, ValueError) as exc:
                    self._poisoned = True
                    raise _error(
                        Phase1CPipelineErrorCode.NATIVE_RECHAIN_FAILED,
                        "native row failed post-append rematerialization",
                        commit_sequence=int(frame.commit_sequence),
                    ) from exc
                accumulator = self._streams.setdefault(row.stream_id, _StreamAccumulator())
                accumulator.count += 1
                accumulator.digest.update(line)
                value = _payload_object(line)
                if row.stream_id == StreamId("alerts") and value.get("code") == "MARKET_GAP":
                    self._market_gap_count += 1

    def append_batch(self, batch: Phase1CBatch) -> Phase1CBatchResult:
        """Publish raw bytes, resolve/rechain them, then append Paper frames."""

        self._ensure_writable()
        if type(batch) is not Phase1CBatch:
            raise _error(Phase1CPipelineErrorCode.TYPE_INVALID, "append requires Phase1CBatch")
        _preflight_batch(batch, self._cursor)

        raw_seals: list[RawSealResult] = []
        references: dict[int, RawSegmentRef] = {}
        resolver = DiskRawResolver(self._raw_store)
        if batch.raw_records:
            try:
                segment_writer = RawSegmentWriter(
                    self._staging,
                    lake_id=self._raw_store.config.lake_id,
                    codec_profile=self._raw_store.config.codec_profile,
                    thresholds=self._thresholds,
                )
                try:
                    for record in batch.raw_records:
                        try:
                            segment_writer.append(record.payload, record.metadata)
                        except RawSegmentError as exc:
                            if (
                                exc.code is not RawSegmentErrorCode.THRESHOLD_REACHED
                                or segment_writer.record_count == 0
                            ):
                                raise
                            raw_seals.append(self._raw_store.seal(segment_writer.seal()))
                            next_writer = RawSegmentWriter(
                                self._staging,
                                lake_id=self._raw_store.config.lake_id,
                                codec_profile=self._raw_store.config.codec_profile,
                                thresholds=self._thresholds,
                            )
                            segment_writer = next_writer
                            segment_writer.append(record.payload, record.metadata)
                    raw_seals.append(self._raw_store.seal(segment_writer.seal()))
                finally:
                    segment_writer.close()
            except (OSError, RawSegmentError, RawStoreError, TypeError, ValueError) as exc:
                self._poisoned = True
                raise _error(
                    Phase1CPipelineErrorCode.RAW_PUBLICATION_FAILED,
                    "raw batch publication did not complete",
                    first_commit=int(batch.source_frames[0].commit_sequence),
                    last_commit=int(batch.source_frames[-1].commit_sequence),
                ) from exc
            published_reference_count = sum(len(raw_seal.references) for raw_seal in raw_seals)
            references = {
                int(reference.arrival_sequence): reference
                for raw_seal in raw_seals
                for reference in raw_seal.references
            }
            if len(references) != published_reference_count or published_reference_count != len(
                batch.raw_records
            ):
                self._poisoned = True
                raise _error(
                    Phase1CPipelineErrorCode.NATIVE_RECHAIN_FAILED,
                    "raw seals returned duplicate or missing arrival references",
                )

        try:
            native_frames, next_cursor = _rechain_batch(
                batch=batch,
                references=references,
                resolver=resolver,
                cursor=self._cursor,
            )
        except Phase1CPipelineError:
            self._poisoned = True
            raise

        try:
            trigger_fault(self._fault_hook, FaultPoint.AFTER_RAW_BEFORE_PAPER_APPEND)
        except BaseException:
            self._poisoned = True
            raise

        try:
            for frame in native_frames:
                if not self._paper.append(frame):
                    raise _error(
                        Phase1CPipelineErrorCode.PAPER_APPEND_FAILED,
                        "Paper append was unexpectedly idempotent",
                        commit_sequence=int(frame.commit_sequence),
                    )
        except (Phase1CPipelineError, RepositoryError, TypeError, ValueError) as exc:
            self._poisoned = True
            if isinstance(exc, Phase1CPipelineError):
                raise
            raise _error(
                Phase1CPipelineErrorCode.PAPER_APPEND_FAILED,
                "native Paper batch append failed after raw publication",
                raw_generation=(0 if not raw_seals else raw_seals[-1].manifest.generation),
            ) from exc

        self._observe_native_frames(native_frames)

        self._cursor = next_cursor
        return Phase1CBatchResult(
            native_frames=native_frames,
            raw_seals=tuple(raw_seals),
            cursor=next_cursor,
        )

    def append_presealed_batch(
        self,
        batch: Phase1CBatch,
        references: tuple[RawSegmentRef, ...],
    ) -> Phase1CBatchResult:
        """Rebuild Paper tail from an authenticated raw suffix without raw writes."""

        self._ensure_writable()
        if type(batch) is not Phase1CBatch or type(references) is not tuple or any(
            type(reference) is not RawSegmentRef for reference in references
        ):
            raise _error(
                Phase1CPipelineErrorCode.TYPE_INVALID,
                "presealed append requires Phase1CBatch and exact raw references",
            )
        _preflight_batch(batch, self._cursor)
        records = {record.commit_sequence: record for record in batch.raw_records}
        reference_map = {int(reference.arrival_sequence): reference for reference in references}
        if (
            len(records) != len(batch.raw_records)
            or len(reference_map) != len(references)
            or set(records) != set(reference_map)
        ):
            raise _error(
                Phase1CPipelineErrorCode.RESUME_INVALID,
                "presealed raw references do not map one-to-one to source records",
            )
        for sequence, record in records.items():
            reference = reference_map[sequence]
            metadata = record.metadata
            if (
                reference.raw_store_id != self._raw_store.config.store_id
                or reference.lake_id != self._raw_store.config.lake_id
                or reference.source_id != metadata.source_id
                or reference.venue_id != metadata.venue_id
                or reference.record_id != metadata.record_id
                or reference.input_type != metadata.input_type
                or reference.source_stream_id != metadata.source_stream_id
                or reference.source_first_sequence != metadata.source_first_sequence
                or reference.source_last_sequence != metadata.source_last_sequence
                or reference.arrival_sequence != metadata.arrival_sequence
                or reference.source_timestamp != metadata.source_timestamp
                or reference.received_timestamp != metadata.received_timestamp
                or reference.logical_payload_length != len(record.payload)
                or reference.logical_payload_sha256
                != Hash32(hashlib.sha256(record.payload).digest())
            ):
                raise _error(
                    Phase1CPipelineErrorCode.RESUME_INVALID,
                    "presealed raw reference differs from deterministic source metadata",
                    commit_sequence=sequence,
                )
        resolver = DiskRawResolver(self._raw_store)
        try:
            native_frames, next_cursor = _rechain_batch(
                batch=batch,
                references=reference_map,
                resolver=resolver,
                cursor=self._cursor,
            )
            trigger_fault(self._fault_hook, FaultPoint.AFTER_RAW_BEFORE_PAPER_APPEND)
            for frame in native_frames:
                if not self._paper.append(frame):
                    raise _error(
                        Phase1CPipelineErrorCode.PAPER_APPEND_FAILED,
                        "presealed Paper rebuild was unexpectedly idempotent",
                    )
        except (Phase1CPipelineError, RepositoryError, TypeError, ValueError) as exc:
            self._poisoned = True
            if isinstance(exc, Phase1CPipelineError):
                raise
            raise _error(
                Phase1CPipelineErrorCode.PAPER_APPEND_FAILED,
                "presealed Paper suffix reconstruction failed",
            ) from exc
        self._observe_native_frames(native_frames)
        self._cursor = next_cursor
        return Phase1CBatchResult(
            native_frames=native_frames,
            raw_seals=(),
            cursor=next_cursor,
        )

    def seal(self, checkpoint_state: CheckpointState) -> Phase1CSealResult:
        """Bind the complete raw prefix and publish the Paper checkpoint."""

        self._ensure_writable()
        if type(checkpoint_state) is not CheckpointState:
            raise _error(
                Phase1CPipelineErrorCode.TYPE_INVALID,
                "seal requires CheckpointState",
            )
        raw_manifest = self._raw_store.manifest
        if raw_manifest is None or self._cursor.raw_reference_count == 0:
            raise _error(
                Phase1CPipelineErrorCode.STATE_INVALID,
                "native seal requires at least one published raw reference",
            )
        if raw_manifest.total_record_count != self._cursor.raw_reference_count:
            raise _error(
                Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
                "raw authority contains records not owned by the Paper prefix",
            )
        last_record = self._cursor.raw_last_record_id
        if last_record is None or raw_manifest.segments[-1].last_record_id != last_record:
            raise _error(
                Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
                "raw authority last record differs from the Paper reference prefix",
            )
        binding = NativeCheckpointBinding(
            raw_store_id=self._raw_store.config.store_id,
            raw_lake_id=self._raw_store.config.lake_id,
            raw_config_identity=self._raw_store.config.config_identity,
            raw_generation=raw_manifest.generation,
            raw_manifest_root=raw_manifest.root,
            raw_record_count=raw_manifest.total_record_count,
            raw_last_record_id=last_record,
            raw_reference_prefix_root=self._cursor.raw_reference_prefix_root,
        )
        counts = tuple(
            (stream_id, accumulator.count)
            for stream_id, accumulator in sorted(
                self._streams.items(), key=lambda item: item[0].value.encode("utf-8")
            )
        )
        commit_count = self._cursor.next_commit_sequence - 1
        overlay = self._paper.overlay_state
        if (
            overlay.tail_commit_count == 0
            or int(overlay.base_commit_sequence) + overlay.tail_commit_count != commit_count
        ):
            raise _error(
                Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
                "Paper checkpoint tail differs from the native cursor",
            )
        try:
            paper_seal = self._paper.seal(
                checkpoint_state=bind_native_checkpoint_state(checkpoint_state, binding),
                cumulative_stream_counts=counts,
                historical_commit_count=commit_count,
            )
        except (NativeJournalError, RepositoryError, TypeError, ValueError) as exc:
            self._poisoned = True
            raise _error(
                Phase1CPipelineErrorCode.PAPER_SEAL_FAILED,
                "Paper seal failed after raw authority was published",
                raw_generation=raw_manifest.generation,
            ) from exc

        expectations = self.audit_expectations()
        return Phase1CSealResult(
            paper_seal=paper_seal,
            binding=binding,
            expectations=expectations,
        )


def inspect_phase1c_alignment(
    raw_store: RawStore,
    paper_repository: StorageRepository,
) -> Phase1CAuthorityReport:
    """Classify authenticated raw/Paper authority without opening raw segments."""

    if type(raw_store) is not RawStore or type(paper_repository) is not StorageRepository:
        raise _error(
            Phase1CPipelineErrorCode.TYPE_INVALID,
            "alignment inspection requires open raw and Paper stores",
        )
    raw_manifest = raw_store.manifest
    paper_manifest = paper_repository.manifest
    paper_checkpoint = paper_repository.checkpoint
    tail_count = paper_repository.overlay_state.tail_commit_count
    raw_generation = 0 if raw_manifest is None else raw_manifest.generation
    raw_root = None if raw_manifest is None else raw_manifest.root
    raw_count = 0 if raw_manifest is None else raw_manifest.total_record_count
    paper_generation = 0 if paper_manifest is None else paper_manifest.generation
    paper_root = None if paper_manifest is None else paper_manifest.identity.root

    if raw_manifest is None:
        if paper_checkpoint is not None or paper_manifest is not None:
            raise _error(
                Phase1CPipelineErrorCode.PAPER_WITHOUT_RAW_AUTHORITY,
                "Paper authority exists without a raw authority",
            )
        status = (
            Phase1CAuthorityStatus.EMPTY if tail_count == 0 else Phase1CAuthorityStatus.PAPER_TAIL_WITHOUT_RAW
        )
        return Phase1CAuthorityReport(
            status=status,
            raw_generation=0,
            raw_manifest_root=None,
            raw_record_count=0,
            paper_generation=0,
            paper_manifest_root=None,
            paper_tail_commit_count=tail_count,
            binding=None,
        )

    if paper_checkpoint is None or paper_manifest is None:
        status = (
            Phase1CAuthorityStatus.RAW_VALID_PAPER_ABSENT
            if tail_count == 0
            else Phase1CAuthorityStatus.RAW_VALID_PAPER_TAIL
        )
        return Phase1CAuthorityReport(
            status=status,
            raw_generation=raw_generation,
            raw_manifest_root=raw_root,
            raw_record_count=raw_count,
            paper_generation=paper_generation,
            paper_manifest_root=paper_root,
            paper_tail_commit_count=tail_count,
            binding=None,
        )

    try:
        _, binding = unbind_native_checkpoint_state(paper_checkpoint.state)
    except (NativeJournalError, TypeError, ValueError) as exc:
        raise _error(
            Phase1CPipelineErrorCode.CHECKPOINT_BINDING_INVALID,
            "Paper checkpoint has no valid native raw binding",
        ) from exc
    if (
        binding.raw_store_id != raw_store.config.store_id
        or binding.raw_lake_id != raw_store.config.lake_id
        or binding.raw_config_identity != raw_store.config.config_identity
    ):
        raise _error(
            Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
            "Paper checkpoint raw store, lake, or config identity differs",
        )
    try:
        bound_manifest = raw_store.authenticated_manifest(binding.raw_manifest_root)
    except (RawStoreError, TypeError, ValueError) as exc:
        raise _error(
            Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
            "Paper checkpoint references an unauthenticated raw manifest",
        ) from exc
    if (
        bound_manifest.generation != binding.raw_generation
        or bound_manifest.total_record_count != binding.raw_record_count
        or bound_manifest.segments[-1].last_record_id != binding.raw_last_record_id
    ):
        raise _error(
            Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
            "Paper checkpoint raw counters differ from raw authority",
        )
    if raw_manifest.root == binding.raw_manifest_root:
        status = Phase1CAuthorityStatus.ALIGNED
    elif raw_manifest.generation > binding.raw_generation:
        status = Phase1CAuthorityStatus.RAW_AHEAD_OF_PAPER
    else:
        raise _error(
            Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
            "raw authority is older than or forked from the Paper binding",
        )
    return Phase1CAuthorityReport(
        status=status,
        raw_generation=raw_generation,
        raw_manifest_root=raw_root,
        raw_record_count=raw_count,
        paper_generation=paper_generation,
        paper_manifest_root=paper_root,
        paper_tail_commit_count=tail_count,
        binding=binding,
    )


def certify_phase1c_reopen(
    *,
    raw_root: Path,
    raw_anchor: object,
    raw_config: RawStoreConfig,
    paper_root: Path,
    paper_anchor: object,
    paper_config: RepositoryConfig,
    binding: NativeCheckpointBinding,
    expectations: NativeAuditExpectations,
    expected_tail_entries: int = 0,
    checkpoint_expectations: NativeAuditExpectations | None = None,
) -> Phase1CCertificationReport:
    """Reopen cheaply, then audit checkpoint history and its bounded native tail."""

    if type(expected_tail_entries) is not int or expected_tail_entries < 0:
        raise _error(
            Phase1CPipelineErrorCode.TYPE_INVALID,
            "expected tail entries must be a non-negative exact integer",
        )
    if checkpoint_expectations is None:
        checkpoint_expectations = expectations
    if type(checkpoint_expectations) is not NativeAuditExpectations:
        raise _error(
            Phase1CPipelineErrorCode.TYPE_INVALID,
            "checkpoint expectations must be NativeAuditExpectations",
        )
    expected_historical_commits = expectations.commit_count - expected_tail_entries
    if (
        expected_historical_commits < 1
        or checkpoint_expectations.commit_count != expected_historical_commits
        or checkpoint_expectations.raw_reference_count != binding.raw_record_count
        or checkpoint_expectations.raw_last_record_id != binding.raw_last_record_id
        or checkpoint_expectations.raw_reference_prefix_root != binding.raw_reference_prefix_root
        or binding.raw_manifest_root not in checkpoint_expectations.raw_manifest_roots
    ):
        raise _error(
            Phase1CPipelineErrorCode.CHECKPOINT_BINDING_INVALID,
            "checkpoint expectations differ from the requested tail boundary",
        )

    # Anchor protocols are structural at runtime; RawStore/StorageRepository
    # perform the authoritative validation and expose stable underlying errors.
    try:
        raw = RawStore.open_existing(raw_root, anchor=raw_anchor, config=raw_config)  # type: ignore[arg-type]
    except (RawStoreError, TypeError, ValueError, OSError) as exc:
        raise _error(
            Phase1CPipelineErrorCode.AUDIT_FAILED,
            "raw authority could not be reopened",
        ) from exc
    try:
        with raw:
            try:
                paper = StorageRepository.open_existing(
                    paper_root,
                    anchor=paper_anchor,  # type: ignore[arg-type]
                    config=paper_config,
                )
            except (RepositoryError, TypeError, ValueError, OSError) as exc:
                raise _error(
                    Phase1CPipelineErrorCode.AUDIT_FAILED,
                    "Paper authority could not be reopened",
                ) from exc
            with paper:
                raw_startup = raw.startup_report
                paper_startup = paper.startup_report
                if raw_startup.historical_segments_read != 0:
                    raise _error(
                        Phase1CPipelineErrorCode.STARTUP_SCOPE_VIOLATION,
                        "normal raw startup opened historical segments",
                    )
                if (
                    paper_startup.segments_read != 0
                    or not paper_startup.checkpoint_used
                    or paper_startup.tail_entries_replayed != expected_tail_entries
                ):
                    raise _error(
                        Phase1CPipelineErrorCode.STARTUP_SCOPE_VIOLATION,
                        "normal Paper startup differs from checkpoint plus bounded tail",
                    )
                alignment = inspect_phase1c_alignment(raw, paper)
                allowed_alignment = (
                    (Phase1CAuthorityStatus.ALIGNED,)
                    if expected_tail_entries == 0
                    else (
                        Phase1CAuthorityStatus.ALIGNED,
                        Phase1CAuthorityStatus.RAW_AHEAD_OF_PAPER,
                    )
                )
                if (
                    alignment.status not in allowed_alignment
                    or alignment.paper_tail_commit_count != expected_tail_entries
                ):
                    raise _error(
                        Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
                        "terminal certification found an invalid raw/Paper tail alignment",
                        status=alignment.status.value,
                    )
                if alignment.binding != binding:
                    raise _error(
                        Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
                        "reopened checkpoint binding differs from the sealed binding",
                    )
                try:
                    raw_audit = raw.full_audit()
                    paper_audit = paper.full_audit()
                    resolver = DiskRawResolver(raw)
                    checkpoint_audit = audit_native_frames(
                        paper.iter_historical_frames(),
                        resolver,
                        checkpoint_expectations,
                    )
                    native_audit = (
                        checkpoint_audit
                        if expected_tail_entries == 0
                        else audit_native_frames(
                            chain(
                                paper.iter_historical_frames(),
                                paper_startup.tail_frames,
                            ),
                            resolver,
                            expectations,
                        )
                    )
                except (
                    NativeJournalError,
                    RawStoreError,
                    RepositoryError,
                    TypeError,
                    ValueError,
                    OSError,
                ) as exc:
                    raise _error(
                        Phase1CPipelineErrorCode.AUDIT_FAILED,
                        "exhaustive raw/Paper rematerialization audit failed",
                    ) from exc
                if (
                    raw_audit.records_read != expectations.raw_reference_count
                    or alignment.raw_record_count != expectations.raw_reference_count
                    or paper_audit.commits_read != expected_historical_commits
                    or paper_audit.rows_read + paper_startup.tail_rows_replayed
                    != sum(stream.row_count for stream in expectations.streams)
                    or native_audit.raw_reference_prefix_root != expectations.raw_reference_prefix_root
                ):
                    raise _error(
                        Phase1CPipelineErrorCode.AUTHORITY_MISALIGNED,
                        "exhaustive audit counters differ from checkpoint plus tail",
                    )
                return Phase1CCertificationReport(
                    raw_startup=raw_startup,
                    paper_startup=paper_startup,
                    alignment=alignment,
                    raw_audit=raw_audit,
                    paper_audit=paper_audit,
                    native_audit=native_audit,
                    raw_resolver_physical_hash_passes=resolver.physical_hash_passes,
                )
    except Phase1CPipelineError:
        raise


@runtime_checkable
class Phase1CCapacityBatchAdapter(Protocol):
    """Translate a bounded synthetic chunk into one source/native batch."""

    def build_phase1c_batch(
        self,
        commits: tuple[SyntheticCapacityCommit, ...],
    ) -> Phase1CBatch: ...


@dataclass(frozen=True, slots=True)
class Phase1CCapacityIngestReport:
    workload_manifest_sha256: str
    observed_workload_sha256: str
    commit_count: int
    logical_row_count: int
    batch_count: int
    cursor: NativeBatchCursor


def ingest_capacity_workload(
    *,
    writer: Phase1CWriter,
    manifest: CapacityWorkloadManifest,
    commits: Iterable[SyntheticCapacityCommit],
    adapter: Phase1CCapacityBatchAdapter,
    batch_size: int,
) -> Phase1CCapacityIngestReport:
    """Stream and authenticate one capacity workload through Phase 1C batches."""

    if type(writer) is not Phase1CWriter or not isinstance(manifest, CapacityWorkloadManifest):
        raise _error(
            Phase1CPipelineErrorCode.TYPE_INVALID,
            "capacity ingestion requires Phase1CWriter and CapacityWorkloadManifest",
        )
    if not isinstance(adapter, Phase1CCapacityBatchAdapter):
        raise _error(
            Phase1CPipelineErrorCode.TYPE_INVALID,
            "capacity adapter does not satisfy Phase1CCapacityBatchAdapter",
        )
    if type(batch_size) is not int or batch_size < 1:
        raise _error(
            Phase1CPipelineErrorCode.TYPE_INVALID,
            "capacity batch_size must be a positive exact integer",
        )
    hasher = CapacityWorkloadHasher()
    pending: list[SyntheticCapacityCommit] = []
    batch_count = 0
    try:
        for commit in commits:
            hasher.update(commit)
            pending.append(commit)
            if len(pending) == batch_size:
                writer.append_batch(adapter.build_phase1c_batch(tuple(pending)))
                pending.clear()
                batch_count += 1
        if pending:
            writer.append_batch(adapter.build_phase1c_batch(tuple(pending)))
            batch_count += 1
        observed = hasher.finalize()
    except Phase1CPipelineError:
        raise
    except (TypeError, ValueError, RuntimeError) as exc:
        raise _error(
            Phase1CPipelineErrorCode.CAPACITY_WORKLOAD_DIVERGENCE,
            "capacity adapter or workload stream diverged",
        ) from exc
    if (
        observed.commit_count != manifest.commit_count
        or observed.logical_row_count != manifest.logical_row_count
        or observed.sha256 != manifest.workload_sha256
    ):
        raise _error(
            Phase1CPipelineErrorCode.CAPACITY_WORKLOAD_DIVERGENCE,
            "observed capacity workload differs from its frozen manifest",
        )
    return Phase1CCapacityIngestReport(
        workload_manifest_sha256=manifest.sha256,
        observed_workload_sha256=observed.sha256,
        commit_count=observed.commit_count,
        logical_row_count=observed.logical_row_count,
        batch_count=batch_count,
        cursor=writer.cursor,
    )


__all__ = [
    "NativeBatchCursor",
    "NativeRawRecord",
    "Phase1CAuthorityReport",
    "Phase1CAuthorityStatus",
    "Phase1CBatch",
    "Phase1CBatchResult",
    "Phase1CCapacityBatchAdapter",
    "Phase1CCapacityIngestReport",
    "Phase1CCertificationReport",
    "Phase1CPipelineError",
    "Phase1CPipelineErrorCode",
    "Phase1CResumeResult",
    "Phase1CSealResult",
    "Phase1CWriter",
    "certify_phase1c_reopen",
    "ingest_capacity_workload",
    "inspect_phase1c_alignment",
]
