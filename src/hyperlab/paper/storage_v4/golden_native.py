"""Streaming Golden V3 ingestion through the Storage V4 native raw path.

Golden V3 authenticates canonical public-input JSONL, not the exchange's
original wire representation.  This adapter preserves that distinction in
every raw-record metadata value.  It stores exactly one certified canonical
inbox line per commit, replaces only that inbox row with a native raw
reference, and leaves the remaining twelve logical streams on the Paper side.

The module deliberately contains no filesystem discovery, CLI, network
access, or evidence publication.  Callers must supply an already verified
Golden export, fresh raw/Paper authorities, and an explicit fixed batch size.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast, runtime_checkable

from hyperlab.paper.golden_v3 import GOLDEN_STREAM_NAMES, GoldenVerification, iter_golden_stream

from .canonical import build_commit_logical
from .checkpoint import CheckpointState, checkpoint_state_sha256
from .contracts import CompatibilityRecord, CompatibilityRecordError
from .golden_import import AssembledGoldenCommit, GoldenCommitAssembler, GoldenImportError
from .native_journal import rematerialize_native_row, unbind_native_checkpoint_state
from .phase1b_certification import (
    Phase1BCertificationError,
    _compare_all_streams,
)
from .phase1c_pipeline import (
    NativeRawRecord,
    Phase1CBatch,
    Phase1CBatchResult,
    Phase1CSealResult,
    Phase1CWriter,
)
from .raw_reference import RawReferenceResolverV2
from .raw_segment import RawRecordMetadata
from .types import CommitFrame, CommitSequence, EventSequence, Hash32, StreamId

GOLDEN_NATIVE_SOURCE_ID = "golden-v3-certified-canonical-jsonl"
GOLDEN_NATIVE_INPUT_TYPE = "CERTIFIED_CANONICAL_JSONL_NOT_ORIGINAL_WIRE"
GOLDEN_NATIVE_SOURCE_STREAM = StreamId("inbox")

GoldenStreamFactory = Callable[
    [GoldenVerification, str], Iterable[Mapping[str, object]]
]


class GoldenNativeError(RuntimeError):
    """The Golden/native boundary is malformed or no longer exact."""


@dataclass(frozen=True, slots=True)
class GoldenNativeBatch:
    """One bounded native ingest unit and its exact unbound Golden state."""

    batch: Phase1CBatch
    checkpoint_state: CheckpointState
    boundary_commit_sequence: CommitSequence

    def __post_init__(self) -> None:
        if type(self.batch) is not Phase1CBatch or type(self.checkpoint_state) is not CheckpointState:
            raise TypeError("Golden native boundary requires Phase1CBatch and CheckpointState")
        if type(self.boundary_commit_sequence) is not CommitSequence:
            raise TypeError("Golden native boundary requires CommitSequence")
        if int(self.batch.source_frames[-1].commit_sequence) != int(self.boundary_commit_sequence):
            raise ValueError("Golden native checkpoint boundary differs from its final frame")


GoldenNativeIngestProgress = Callable[
    [GoldenNativeBatch, Phase1CBatchResult, Phase1CSealResult],
    None,
]


@dataclass(frozen=True, slots=True)
class GoldenNativeCheckpointWitness:
    """Persisted bound state plus the exact Golden state recovered from it."""

    commit_sequence: CommitSequence
    checkpoint_root: Hash32
    bound_state_sha256: Hash32
    unbound_state_sha256: Hash32
    raw_manifest_root: Hash32

    def __post_init__(self) -> None:
        if type(self.commit_sequence) is not CommitSequence:
            raise TypeError("Golden native witness sequence must be CommitSequence")
        for value in (
            self.checkpoint_root,
            self.bound_state_sha256,
            self.unbound_state_sha256,
            self.raw_manifest_root,
        ):
            if type(value) is not Hash32:
                raise TypeError("Golden native witness digests must be Hash32")


@dataclass(frozen=True, slots=True)
class GoldenNativeIngestResult:
    """Bounded evidence retained after all source batches have been released."""

    checkpoint_witnesses: tuple[GoldenNativeCheckpointWitness, ...]
    terminal_batch_result: Phase1CBatchResult
    terminal_seal_result: Phase1CSealResult
    commit_count: int

    def __post_init__(self) -> None:
        if not self.checkpoint_witnesses:
            raise ValueError("Golden native ingestion requires at least one checkpoint witness")
        if type(self.terminal_batch_result) is not Phase1CBatchResult:
            raise TypeError("Golden native terminal batch result is invalid")
        if type(self.terminal_seal_result) is not Phase1CSealResult:
            raise TypeError("Golden native terminal seal result is invalid")
        if type(self.commit_count) is not int or self.commit_count < 1:
            raise ValueError("Golden native commit count must be positive")
        sequences = tuple(int(witness.commit_sequence) for witness in self.checkpoint_witnesses)
        if tuple(sorted(sequences)) != sequences or len(set(sequences)) != len(sequences):
            raise ValueError("Golden native checkpoint witnesses must be unique and ordered")
        if sequences[-1] != self.commit_count:
            raise ValueError("Golden native terminal witness differs from the commit count")

    @property
    def unbound_checkpoint_state_witnesses(self) -> dict[int, str]:
        """Return the exact mapping consumed by the independent Phase 1B comparator."""

        return {
            int(witness.commit_sequence): witness.unbound_state_sha256.hex()
            for witness in self.checkpoint_witnesses
        }


def _checkpoint_witness_mapping(
    witnesses: tuple[GoldenNativeCheckpointWitness, ...],
) -> dict[int, str]:
    if type(witnesses) is not tuple or not witnesses or any(
        type(witness) is not GoldenNativeCheckpointWitness for witness in witnesses
    ):
        raise TypeError(
            "Golden native comparison requires a nonempty exact checkpoint witness tuple"
        )
    sequences = tuple(int(witness.commit_sequence) for witness in witnesses)
    if sequences != tuple(sorted(sequences)) or len(set(sequences)) != len(sequences):
        raise GoldenNativeError("Golden native checkpoint witnesses are not unique and ordered")
    return {
        int(witness.commit_sequence): witness.unbound_state_sha256.hex()
        for witness in witnesses
    }


@dataclass(frozen=True, slots=True)
class GoldenNativeDifferentialResult:
    report: dict[str, object]
    terminal_unbound_checkpoint_state: CheckpointState


def _checkpoint_state(commit: AssembledGoldenCommit) -> CheckpointState:
    sections = commit.build_checkpoint_sections()
    return CheckpointState(
        adapter=sections.adapter_state,
        ledger=sections.ledger_state,
        projection=sections.projection_state,
        sessions=sections.sessions_state,
        incidents=sections.incidents_state,
        cursors=sections.cursors,
        stream_heads=sections.stream_heads,
    )


def _inbox_raw_record(commit: AssembledGoldenCommit) -> NativeRawRecord:
    frame = commit.frame
    inbox = tuple(row for row in frame.rows if row.stream_id == GOLDEN_NATIVE_SOURCE_STREAM)
    if len(inbox) != 1 or int(inbox[0].ordinal) != 0:
        raise GoldenNativeError("Golden commit must own exactly one ordinal-zero inbox row")
    try:
        record = CompatibilityRecord.from_logical_row(inbox[0])
        value = json.loads(record.canonical_json_text)
    except (CompatibilityRecordError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise GoldenNativeError("Golden inbox row is not exact canonical JSON") from error
    if type(value) is not dict:
        raise GoldenNativeError("Golden inbox canonical value is not an object")
    input_id = value.get("input_id")
    if type(input_id) is not str or not input_id:
        raise GoldenNativeError("Golden inbox input_id is missing")
    sequence = int(frame.commit_sequence)
    metadata = RawRecordMetadata(
        record_id=input_id,
        source_id=GOLDEN_NATIVE_SOURCE_ID,
        venue_id=None,
        input_type=GOLDEN_NATIVE_INPUT_TYPE,
        source_stream_id=GOLDEN_NATIVE_SOURCE_STREAM,
        source_first_sequence=EventSequence(sequence),
        source_last_sequence=EventSequence(sequence),
        arrival_sequence=EventSequence(sequence),
        source_timestamp=None,
        received_timestamp=None,
    )
    return NativeRawRecord(
        commit_sequence=sequence,
        payload=record.jsonl_bytes,
        metadata=metadata,
    )


def iter_golden_native_batches(
    commits: GoldenCommitAssembler | Iterable[AssembledGoldenCommit],
    *,
    batch_size: int,
    expected_commit_count: int | None = None,
) -> Iterator[GoldenNativeBatch]:
    """Yield fixed-size batches while snapshots are still valid.

    ``GoldenCommitAssembler`` invalidates an earlier commit's lazy checkpoint
    factory as soon as iteration advances.  The boundary state is therefore
    materialized before this generator yields and before it requests the next
    source commit.
    """

    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("Golden native batch_size must be a positive exact integer")
    if type(commits) is GoldenCommitAssembler:
        certified_count = commits.expectations.commit_count
        if expected_commit_count is None:
            expected_commit_count = certified_count
        elif expected_commit_count != certified_count:
            raise ValueError(
                "Golden native expected_commit_count differs from assembler expectations"
            )
    elif expected_commit_count is None:
        raise ValueError(
            "Golden native generic iterables require expected_commit_count"
        )
    if type(expected_commit_count) is not int or expected_commit_count < 1:
        raise ValueError("Golden native expected_commit_count must be a positive exact integer")
    source_frames: list[CommitFrame] = []
    raw_records: list[NativeRawRecord] = []
    observed = 0
    try:
        for commit in commits:
            if type(commit) is not AssembledGoldenCommit:
                raise TypeError("Golden native iterator requires AssembledGoldenCommit values")
            observed += 1
            if observed > expected_commit_count:
                raise GoldenNativeError(
                    "Golden native iterator exceeded expected_commit_count"
                )
            source_frames.append(commit.frame)
            raw_records.append(_inbox_raw_record(commit))
            if len(source_frames) == batch_size or observed == expected_commit_count:
                state = _checkpoint_state(commit)
                yield GoldenNativeBatch(
                    batch=Phase1CBatch(tuple(source_frames), tuple(raw_records)),
                    checkpoint_state=state,
                    boundary_commit_sequence=commit.frame.commit_sequence,
                )
                source_frames = []
                raw_records = []
    except GoldenImportError as error:
        raise GoldenNativeError("Golden assembly failed during native batching") from error
    if observed != expected_commit_count:
        raise GoldenNativeError(
            "Golden native iterator ended before expected_commit_count"
        )
    if source_frames or raw_records:
        raise AssertionError("Golden native terminal batch was not yielded")


def ingest_golden_native_batches(
    writer: Phase1CWriter,
    batches: Iterable[GoldenNativeBatch],
    *,
    progress: GoldenNativeIngestProgress | None = None,
) -> GoldenNativeIngestResult:
    """Append then seal every boundary, retaining witnesses and terminal results only."""

    if type(writer) is not Phase1CWriter:
        raise TypeError("Golden native ingestion requires Phase1CWriter")
    if progress is not None and not callable(progress):
        raise TypeError("Golden native ingestion progress must be callable or None")
    witnesses: list[GoldenNativeCheckpointWitness] = []
    terminal_batch: Phase1CBatchResult | None = None
    terminal_seal: Phase1CSealResult | None = None
    prior_boundary = 0
    for boundary in batches:
        if type(boundary) is not GoldenNativeBatch:
            raise TypeError("Golden native ingestion requires GoldenNativeBatch values")
        sequence = int(boundary.boundary_commit_sequence)
        first_sequence = int(boundary.batch.source_frames[0].commit_sequence)
        if first_sequence != prior_boundary + 1 or sequence < first_sequence:
            raise GoldenNativeError("Golden native batch boundaries are not contiguous")
        terminal_batch = writer.append_batch(boundary.batch)
        terminal_seal = writer.seal(boundary.checkpoint_state)
        persisted = terminal_seal.paper_seal.checkpoint
        if int(persisted.covered_commit_sequence) != sequence:
            raise GoldenNativeError("persisted native checkpoint covers another boundary")
        unbound, persisted_binding = unbind_native_checkpoint_state(
            persisted.state,
            expected_binding=terminal_seal.binding,
        )
        if persisted_binding != terminal_seal.binding or unbound != boundary.checkpoint_state:
            raise GoldenNativeError("persisted native checkpoint does not recover the Golden state")
        witnesses.append(
            GoldenNativeCheckpointWitness(
                commit_sequence=boundary.boundary_commit_sequence,
                checkpoint_root=persisted.root,
                bound_state_sha256=checkpoint_state_sha256(persisted.state),
                unbound_state_sha256=checkpoint_state_sha256(unbound),
                raw_manifest_root=terminal_seal.binding.raw_manifest_root,
            )
        )
        prior_boundary = sequence
        if progress is not None:
            progress(boundary, terminal_batch, terminal_seal)
    if terminal_batch is None or terminal_seal is None:
        raise GoldenNativeError("Golden native ingestion received no batches")
    return GoldenNativeIngestResult(
        checkpoint_witnesses=tuple(witnesses),
        terminal_batch_result=terminal_batch,
        terminal_seal_result=terminal_seal,
        commit_count=prior_boundary,
    )


@runtime_checkable
class GoldenNativeRepository(Protocol):
    """Minimal authenticated historical reader accepted by the differential."""

    @property
    def overlay_state(self) -> Any: ...

    @property
    def startup_report(self) -> Any: ...

    def append(self, frame: Any) -> bool: ...

    def seal(
        self,
        *,
        checkpoint_state: CheckpointState,
        cumulative_stream_counts: tuple[tuple[StreamId, int], ...],
        historical_commit_count: int,
    ) -> Any: ...

    def startup(self) -> Any: ...

    def full_audit(self) -> Any: ...

    def iter_historical_frames(self) -> Iterator[CommitFrame]: ...

    def close(self) -> None: ...


class RematerializedGoldenRepository:
    """Read-only compatibility view over authenticated native historical frames."""

    def __init__(
        self,
        repository: GoldenNativeRepository,
        resolver: RawReferenceResolverV2,
        *,
        golden_export_root: Hash32,
    ) -> None:
        if not isinstance(repository, GoldenNativeRepository):
            raise TypeError("native compatibility view requires an authenticated reader")
        if not isinstance(resolver, RawReferenceResolverV2):
            raise TypeError("native compatibility view requires RawReferenceResolverV2")
        if type(golden_export_root) is not Hash32:
            raise TypeError("native compatibility view requires a Golden Hash32 root")
        self._repository = repository
        self._resolver = resolver
        self._golden_export_root = golden_export_root

    @property
    def overlay_state(self) -> Any:
        return self._repository.overlay_state

    @property
    def startup_report(self) -> Any:
        return self._repository.startup_report

    def append(self, frame: Any) -> bool:
        del frame
        raise GoldenNativeError("rematerialized Golden repository is read-only")

    def seal(
        self,
        *,
        checkpoint_state: CheckpointState,
        cumulative_stream_counts: tuple[tuple[StreamId, int], ...],
        historical_commit_count: int,
    ) -> Any:
        del checkpoint_state, cumulative_stream_counts, historical_commit_count
        raise GoldenNativeError("rematerialized Golden repository is read-only")

    def startup(self) -> Any:
        return self._repository.startup()

    def full_audit(self) -> Any:
        return self._repository.full_audit()

    def close(self) -> None:
        self._repository.close()

    def iter_historical_frames(self) -> Iterator[CommitFrame]:
        previous_prefix = self._golden_export_root
        for native in self._repository.iter_historical_frames():
            rows = []
            for row in native.rows:
                payload = rematerialize_native_row(row, self._resolver)
                compatibility = CompatibilityRecord.from_jsonl_bytes(payload)
                rows.append(compatibility.to_logical_row(row.stream_id, row.ordinal))
            frame = CommitFrame(
                run_id=native.run_id,
                commit_sequence=native.commit_sequence,
                previous_prefix_root=previous_prefix,
                rows=tuple(rows),
                legacy_v3_identity=native.legacy_v3_identity,
            )
            previous_prefix = build_commit_logical(frame).prefix_root
            yield frame


def _default_stream_factory(
    verification: GoldenVerification,
    name: str,
) -> Iterable[Mapping[str, object]]:
    return cast(
        Iterable[Mapping[str, object]],
        iter_golden_stream(
            verification.export_root,
            name,
            verification=verification,
        ),
    )


def _expected_rows(verification: GoldenVerification) -> int:
    raw_streams = verification.manifest.get("streams")
    if type(raw_streams) is not dict or set(raw_streams) != set(GOLDEN_STREAM_NAMES):
        raise GoldenNativeError("Golden manifest differs from the fixed 13-stream contract")
    total = 0
    for name in GOLDEN_STREAM_NAMES:
        descriptor = raw_streams.get(name)
        if type(descriptor) is not dict:
            raise GoldenNativeError(f"Golden stream {name!r} descriptor is missing")
        count = descriptor.get("row_count")
        if type(count) is not int or count < 0:
            raise GoldenNativeError(f"Golden stream {name!r} row count is invalid")
        total += count
    return total


def compare_golden_native_checkpoint_witnesses_exact(
    repository: GoldenNativeRepository,
    resolver: RawReferenceResolverV2,
    verification: GoldenVerification,
    checkpoint_witnesses: tuple[GoldenNativeCheckpointWitness, ...],
    *,
    stream_factory: GoldenStreamFactory = _default_stream_factory,
) -> GoldenNativeDifferentialResult:
    """Compare native history from persisted checkpoint witnesses only."""

    if not isinstance(repository, GoldenNativeRepository) or type(verification) is not GoldenVerification:
        raise TypeError("Golden native comparison requires a reader and GoldenVerification")
    if not isinstance(resolver, RawReferenceResolverV2):
        raise TypeError("Golden native comparison requires RawReferenceResolverV2")
    if not callable(stream_factory):
        raise TypeError("Golden native comparison inputs are invalid")
    checkpoint_state_witnesses = _checkpoint_witness_mapping(checkpoint_witnesses)
    market_gap_rows = 0
    for row in stream_factory(verification, "alerts"):
        if not isinstance(row, Mapping):
            raise GoldenNativeError("Golden alert stream emitted a non-mapping row")
        if row.get("code") == "MARKET_GAP":
            market_gap_rows += 1
    view = RematerializedGoldenRepository(
        repository,
        resolver,
        golden_export_root=Hash32.from_hex(verification.root_hash),
    )
    try:
        compared = _compare_all_streams(
            view,
            verification,
            stream_factory,
            checkpoint_state_witnesses,
            expected_rows=_expected_rows(verification),
            expected_market_gap_rows=market_gap_rows,
        )
    except Phase1BCertificationError as error:
        raise GoldenNativeError("native/Golden exact differential failed") from error
    terminal_witness = checkpoint_witnesses[-1].unbound_state_sha256
    if checkpoint_state_sha256(compared.checkpoint_state) != terminal_witness:
        raise GoldenNativeError("independent terminal Golden checkpoint differs from native state")
    return GoldenNativeDifferentialResult(
        report=compared.report,
        terminal_unbound_checkpoint_state=compared.checkpoint_state,
    )


def compare_golden_native_exact(
    repository: GoldenNativeRepository,
    resolver: RawReferenceResolverV2,
    verification: GoldenVerification,
    ingestion: GoldenNativeIngestResult,
    *,
    stream_factory: GoldenStreamFactory = _default_stream_factory,
) -> GoldenNativeDifferentialResult:
    """Run the independent comparator for a freshly ingested native result."""

    if type(ingestion) is not GoldenNativeIngestResult:
        raise TypeError("Golden native comparison requires GoldenNativeIngestResult")
    return compare_golden_native_checkpoint_witnesses_exact(
        repository,
        resolver,
        verification,
        ingestion.checkpoint_witnesses,
        stream_factory=stream_factory,
    )


__all__ = [
    "GOLDEN_NATIVE_INPUT_TYPE",
    "GOLDEN_NATIVE_SOURCE_ID",
    "GOLDEN_NATIVE_SOURCE_STREAM",
    "GoldenNativeBatch",
    "GoldenNativeCheckpointWitness",
    "GoldenNativeDifferentialResult",
    "GoldenNativeError",
    "GoldenNativeIngestProgress",
    "GoldenNativeIngestResult",
    "GoldenNativeRepository",
    "GoldenStreamFactory",
    "RematerializedGoldenRepository",
    "compare_golden_native_checkpoint_witnesses_exact",
    "compare_golden_native_exact",
    "ingest_golden_native_batches",
    "iter_golden_native_batches",
]
