"""Bounded synthetic-capacity adapter for the Storage V4 Phase 1C pipeline.

The workload generator emits payload recipes rather than retaining a corpus in
memory.  This adapter materializes only one caller-bounded tuple at a time.  The
first synthetic row of each commit becomes the sole compatibility ``inbox``
record selected for native raw storage; every remaining synthetic row stays a
direct Paper-owned logical row.  Consequently the source frame row count is
exactly the workload row count, while each commit owns exactly one raw record.

The mutable state is deliberately bounded by configured batch and stream-count
limits.  It retains counters and stream heads, never per-commit identity sets or
payloads, and can therefore produce repeated checkpoint states without growing
with corpus length.
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from enum import StrEnum

from .canonical import build_commit_logical, canonical_json_bytes
from .capacity import (
    CAPACITY_MARKERS,
    CapacityWorkloadDigest,
    SyntheticCapacityCommit,
    SyntheticCapacityRow,
)
from .checkpoint import CheckpointState
from .contracts import CompatibilityRecord
from .phase1c_pipeline import NativeRawRecord, Phase1CBatch
from .raw_segment import RawRecordMetadata
from .types import (
    CanonicalObject,
    CanonicalValue,
    CommitFrame,
    CommitOrdinal,
    CommitSequence,
    EventSequence,
    Hash32,
    LogicalRow,
    RunId,
    StreamId,
)

SYNTHETIC_CAPACITY_ADAPTER_CONTRACT = (
    "hyperlab.storage_v4.synthetic_capacity_adapter.v1"
)
SYNTHETIC_CAPACITY_ROW_CONTRACT = "hyperlab.storage_v4.synthetic_capacity_row.v1"
SYNTHETIC_CAPACITY_SOURCE_ID = "hyperlab.synthetic.capacity.v1"
SYNTHETIC_CAPACITY_VENUE_ID = "SYNTHETIC"
RAW_NATIVE_INBOX_OWNERSHIP = "RAW_NATIVE_INBOX"
PAPER_DIRECT_OWNERSHIP = "PAPER_DIRECT"

DEFAULT_MAX_BATCH_COMMITS = 10_000
DEFAULT_MAX_TRACKED_STREAMS = 64
_GENESIS_PREFIX_ROOT = Hash32(b"\x00" * 32)
_RECORD_ID_DOMAIN = b"HL4-SYNTHETIC-CAPACITY-RAW-RECORD-V1\x00"


class SyntheticCapacityAdapterErrorCode(StrEnum):
    """Stable fail-closed categories for synthetic source adaptation."""

    TYPE_INVALID = "SYNTHETIC_CAPACITY_ADAPTER_TYPE_INVALID"
    EMPTY_BATCH = "SYNTHETIC_CAPACITY_ADAPTER_EMPTY_BATCH"
    BATCH_LIMIT = "SYNTHETIC_CAPACITY_ADAPTER_BATCH_LIMIT"
    COMMIT_DIVERGENCE = "SYNTHETIC_CAPACITY_ADAPTER_COMMIT_DIVERGENCE"
    ROW_OWNERSHIP = "SYNTHETIC_CAPACITY_ADAPTER_ROW_OWNERSHIP"
    STREAM_DIVERGENCE = "SYNTHETIC_CAPACITY_ADAPTER_STREAM_DIVERGENCE"
    STREAM_LIMIT = "SYNTHETIC_CAPACITY_ADAPTER_STREAM_LIMIT"
    RESUME_INVALID = "SYNTHETIC_CAPACITY_ADAPTER_RESUME_INVALID"


class SyntheticCapacityAdapterError(ValueError):
    """One bounded synthetic workload cannot be adapted without divergence."""

    def __init__(self, code: SyntheticCapacityAdapterErrorCode, message: str) -> None:
        if type(code) is not SyntheticCapacityAdapterErrorCode:
            raise TypeError("adapter error code must be SyntheticCapacityAdapterErrorCode")
        self.code = code
        super().__init__(f"{code.value}: {message}")


def _error(
    code: SyntheticCapacityAdapterErrorCode,
    message: str,
) -> SyntheticCapacityAdapterError:
    return SyntheticCapacityAdapterError(code, message)


def _require_text(value: str, *, label: str) -> str:
    if type(value) is not str or not value:
        raise TypeError(f"{label} must be non-empty exact text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise ValueError(f"{label} must be strict UTF-8 text") from error
    return value


def _timestamp_text(logical_time_ns: int | None) -> str | None:
    """Return an exact, timezone-unambiguous epoch-nanosecond timestamp."""

    if logical_time_ns is None:
        return None
    return f"unix-ns:{logical_time_ns}"


def _record_id(run_id: RunId, row: SyntheticCapacityRow) -> str:
    digest = hashlib.sha256(_RECORD_ID_DOMAIN)
    encoded_run = run_id.value.encode("utf-8", errors="strict")
    digest.update(len(encoded_run).to_bytes(4, "big"))
    digest.update(encoded_run)
    descriptor = canonical_json_bytes(row.descriptor())
    digest.update(len(descriptor).to_bytes(8, "big"))
    digest.update(descriptor)
    return f"synthetic-capacity-v1:{digest.hexdigest()}"


def _row_value(
    row: SyntheticCapacityRow,
    *,
    ownership: str,
) -> CanonicalObject:
    payload = row.payload.to_bytes()
    payload_value: CanonicalObject = {
        "algorithm": row.payload.algorithm,
        "content_base64": base64.b64encode(payload).decode("ascii"),
        "content_sha256": hashlib.sha256(payload).hexdigest(),
        "key_sha256": row.payload.key_sha256,
        "size_bytes": row.payload.size_bytes,
    }
    markers: list[CanonicalValue] = [marker for marker in CAPACITY_MARKERS]
    return {
        "code": row.code,
        "commit_sequence": row.commit_sequence,
        "contract": SYNTHETIC_CAPACITY_ROW_CONTRACT,
        "logical_time_ns": row.logical_time_ns,
        "markers": markers,
        "ownership": ownership,
        "payload": payload_value,
        "record_type": row.record_type,
        "row_ordinal": row.row_ordinal,
        "source_sequence": row.source_sequence,
        "strategy": row.strategy,
        "stream": row.stream,
    }


@dataclass(frozen=True, slots=True)
class _StreamHead:
    row_count: int
    last_source_sequence: int
    last_commit_sequence: int

    def canonical_value(self) -> CanonicalObject:
        return {
            "last_commit_sequence": self.last_commit_sequence,
            "last_source_sequence": self.last_source_sequence,
            "row_count": self.row_count,
        }


def _resume_int(value: object, *, label: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise _error(
            SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
            f"{label} is not an exact integer >= {minimum}",
        )
    return value


def _resume_object(value: object, *, label: str) -> dict[str, object]:
    if type(value) is not dict:
        raise _error(
            SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
            f"{label} is not an exact object",
        )
    return value


def _validate_resume_section(value: CanonicalObject, *, label: str) -> None:
    if (
        value.get("contract") != SYNTHETIC_CAPACITY_ADAPTER_CONTRACT
        or value.get("markers") != list(CAPACITY_MARKERS)
    ):
        raise _error(
            SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
            f"{label} contract or markers differ",
        )


class SyntheticCapacityPhase1CAdapter:
    """Stateful bounded adapter implementing ``Phase1CCapacityBatchAdapter``."""

    __slots__ = (
        "_commit_count",
        "_last_record_id",
        "_logical_row_count",
        "_market_gap_count",
        "_max_batch_commits",
        "_max_tracked_streams",
        "_next_commit_sequence",
        "_paper_stream_counts",
        "_raw_record_count",
        "_run_id",
        "_source_id",
        "_source_prefix_root",
        "_source_stream_heads",
        "_start_prefix_root",
        "_venue_id",
    )

    def __init__(
        self,
        *,
        run_id: RunId,
        start_prefix_root: Hash32 = _GENESIS_PREFIX_ROOT,
        source_id: str = SYNTHETIC_CAPACITY_SOURCE_ID,
        venue_id: str = SYNTHETIC_CAPACITY_VENUE_ID,
        max_batch_commits: int = DEFAULT_MAX_BATCH_COMMITS,
        max_tracked_streams: int = DEFAULT_MAX_TRACKED_STREAMS,
    ) -> None:
        if type(run_id) is not RunId or type(start_prefix_root) is not Hash32:
            raise _error(
                SyntheticCapacityAdapterErrorCode.TYPE_INVALID,
                "run_id and start_prefix_root require exact Storage V4 identifiers",
            )
        _require_text(source_id, label="source_id")
        _require_text(venue_id, label="venue_id")
        for label, value in (
            ("max_batch_commits", max_batch_commits),
            ("max_tracked_streams", max_tracked_streams),
        ):
            if type(value) is not int or value < 1:
                raise _error(
                    SyntheticCapacityAdapterErrorCode.TYPE_INVALID,
                    f"{label} must be a positive exact integer",
                )
        self._run_id = run_id
        self._start_prefix_root = start_prefix_root
        self._source_prefix_root = start_prefix_root
        self._source_id = source_id
        self._venue_id = venue_id
        self._max_batch_commits = max_batch_commits
        self._max_tracked_streams = max_tracked_streams
        self._next_commit_sequence = 1
        self._commit_count = 0
        self._logical_row_count = 0
        self._raw_record_count = 0
        self._market_gap_count = 0
        self._last_record_id: str | None = None
        self._source_stream_heads: dict[str, _StreamHead] = {}
        self._paper_stream_counts: dict[str, int] = {}

    @property
    def run_id(self) -> RunId:
        return self._run_id

    @property
    def next_commit_sequence(self) -> int:
        return self._next_commit_sequence

    @property
    def source_prefix_root(self) -> Hash32:
        return self._source_prefix_root

    @property
    def commit_count(self) -> int:
        return self._commit_count

    @property
    def logical_row_count(self) -> int:
        return self._logical_row_count

    @property
    def raw_record_count(self) -> int:
        return self._raw_record_count

    @property
    def tracked_stream_count(self) -> int:
        return len(self._source_stream_heads)

    @property
    def source_stream_sequences(self) -> dict[str, int]:
        """Return bounded authenticated source counters for sequence-addressable resume."""

        return {
            stream: head.last_source_sequence
            for stream, head in self._source_stream_heads.items()
        }

    @classmethod
    def resume_from_checkpoint(
        cls,
        state: CheckpointState,
        *,
        expected_run_id: RunId,
        expected_start_prefix_root: Hash32,
        max_batch_commits: int = DEFAULT_MAX_BATCH_COMMITS,
        max_tracked_streams: int = DEFAULT_MAX_TRACKED_STREAMS,
    ) -> tuple[SyntheticCapacityPhase1CAdapter, CapacityWorkloadDigest]:
        """Restore only a fully authenticated, internally coherent sealed state."""

        if type(state) is not CheckpointState:
            raise _error(
                SyntheticCapacityAdapterErrorCode.TYPE_INVALID,
                "resume requires an unbound CheckpointState",
            )
        for label, section in (
            ("adapter", state.adapter),
            ("ledger", state.ledger),
            ("projection", state.projection),
            ("sessions", state.sessions),
            ("incidents", state.incidents),
            ("cursors", state.cursors),
            ("stream_heads", state.stream_heads),
        ):
            _validate_resume_section(section, label=label)

        adapter_state = state.adapter
        cursors = state.cursors
        incidents = state.incidents
        stream_state = state.stream_heads
        if adapter_state.get("run_id") != expected_run_id.value:
            raise _error(
                SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                "checkpoint run identity differs",
            )
        if adapter_state.get("start_prefix_root") != expected_start_prefix_root.hex():
            raise _error(
                SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                "checkpoint start prefix differs",
            )
        source_prefix_value = adapter_state.get("source_prefix_root")
        if type(source_prefix_value) is not str:
            raise _error(
                SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                "checkpoint source prefix is invalid",
            )
        try:
            source_prefix_root = Hash32.from_hex(source_prefix_value)
        except (TypeError, ValueError) as error:
            raise _error(
                SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                "checkpoint source prefix is invalid",
            ) from error

        commit_count = _resume_int(adapter_state.get("commit_count"), label="commit_count")
        logical_row_count = _resume_int(
            adapter_state.get("logical_row_count"),
            label="logical_row_count",
        )
        covered = _resume_int(
            cursors.get("covered_commit_sequence"),
            label="covered_commit_sequence",
        )
        next_sequence = _resume_int(
            cursors.get("next_commit_sequence"),
            label="next_commit_sequence",
            minimum=1,
        )
        raw_record_count = _resume_int(
            cursors.get("raw_record_count"),
            label="raw_record_count",
        )
        market_gap_count = _resume_int(
            incidents.get("market_gap_count"),
            label="market_gap_count",
        )
        last_record_id = cursors.get("last_raw_record_id")
        if (
            covered != commit_count
            or next_sequence != commit_count + 1
            or raw_record_count != commit_count
            or logical_row_count < commit_count
            or market_gap_count > logical_row_count
        ):
            raise _error(
                SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                "checkpoint adapter counters are not mutually coherent",
            )
        if commit_count == 0:
            if last_record_id is not None:
                raise _error(
                    SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                    "empty checkpoint cannot carry a last raw record ID",
                )
            restored_last_record_id: str | None = None
        else:
            if type(last_record_id) is not str or not last_record_id:
                raise _error(
                    SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                    "non-empty checkpoint requires a last raw record ID",
                )
            restored_last_record_id = last_record_id

        source_values = _resume_object(
            stream_state.get("source_stream_heads"),
            label="source_stream_heads",
        )
        paper_values = _resume_object(
            stream_state.get("paper_stream_counts"),
            label="paper_stream_counts",
        )
        source_heads: dict[str, _StreamHead] = {}
        for stream, raw_head in source_values.items():
            _require_text(stream, label="source stream")
            head = _resume_object(raw_head, label=f"source stream head {stream}")
            if frozenset(head) != {
                "last_commit_sequence",
                "last_source_sequence",
                "row_count",
            }:
                raise _error(
                    SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                    "source stream head fields differ",
                )
            row_count = _resume_int(head["row_count"], label="source row_count", minimum=1)
            last_source = _resume_int(
                head["last_source_sequence"],
                label="last_source_sequence",
                minimum=1,
            )
            last_commit = _resume_int(
                head["last_commit_sequence"],
                label="last_commit_sequence",
                minimum=1,
            )
            if last_source != row_count or last_commit > covered:
                raise _error(
                    SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                    "source stream head is not contiguous within the sealed prefix",
                )
            source_heads[stream] = _StreamHead(row_count, last_source, last_commit)

        paper_counts: dict[str, int] = {}
        for stream, value in paper_values.items():
            _require_text(stream, label="Paper stream")
            paper_counts[stream] = _resume_int(
                value,
                label="Paper stream count",
                minimum=1,
            )
        if (
            len(source_heads) > max_tracked_streams
            or sum(head.row_count for head in source_heads.values()) != logical_row_count
            or sum(paper_counts.values()) != logical_row_count
        ):
            raise _error(
                SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                "checkpoint stream counts differ from the logical row total",
            )

        workload_value = _resume_object(
            cursors.get("workload_prefix"),
            label="workload_prefix",
        )
        if frozenset(workload_value) != {"commit_count", "logical_row_count", "sha256"}:
            raise _error(
                SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                "workload prefix fields differ",
            )
        workload_commit_count = _resume_int(
            workload_value["commit_count"],
            label="workload commit_count",
        )
        workload_logical_rows = _resume_int(
            workload_value["logical_row_count"],
            label="workload logical_row_count",
        )
        workload_sha256 = workload_value["sha256"]
        if type(workload_sha256) is not str:
            raise _error(
                SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                "workload prefix SHA-256 is not exact text",
            )
        try:
            workload_prefix = CapacityWorkloadDigest(
                commit_count=workload_commit_count,
                logical_row_count=workload_logical_rows,
                sha256=workload_sha256,
            )
        except (TypeError, ValueError) as error:
            raise _error(
                SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                "workload prefix digest is malformed",
            ) from error
        if (
            workload_prefix.commit_count != commit_count
            or workload_prefix.logical_row_count != logical_row_count
        ):
            raise _error(
                SyntheticCapacityAdapterErrorCode.RESUME_INVALID,
                "workload prefix counters differ from adapter counters",
            )

        restored = cls(
            run_id=expected_run_id,
            start_prefix_root=expected_start_prefix_root,
            max_batch_commits=max_batch_commits,
            max_tracked_streams=max_tracked_streams,
        )
        restored._source_prefix_root = source_prefix_root
        restored._next_commit_sequence = next_sequence
        restored._commit_count = commit_count
        restored._logical_row_count = logical_row_count
        restored._raw_record_count = raw_record_count
        restored._market_gap_count = market_gap_count
        restored._last_record_id = restored_last_record_id
        restored._source_stream_heads = source_heads
        restored._paper_stream_counts = paper_counts
        return restored, workload_prefix

    def _validate_and_advance_stream(
        self,
        *,
        row: SyntheticCapacityRow,
        heads: dict[str, _StreamHead],
    ) -> None:
        head = heads.get(row.stream)
        expected_source_sequence = 1 if head is None else head.last_source_sequence + 1
        if row.source_sequence != expected_source_sequence:
            raise _error(
                SyntheticCapacityAdapterErrorCode.STREAM_DIVERGENCE,
                "synthetic source sequence is not contiguous within its declared stream",
            )
        if head is None and len(heads) >= self._max_tracked_streams:
            raise _error(
                SyntheticCapacityAdapterErrorCode.STREAM_LIMIT,
                "synthetic workload exceeds the configured bounded stream count",
            )
        heads[row.stream] = _StreamHead(
            row_count=1 if head is None else head.row_count + 1,
            last_source_sequence=row.source_sequence,
            last_commit_sequence=row.commit_sequence,
        )

    def _adapt_commit(
        self,
        *,
        commit: SyntheticCapacityCommit,
        expected_sequence: int,
        previous_prefix_root: Hash32,
        heads: dict[str, _StreamHead],
        paper_counts: dict[str, int],
    ) -> tuple[CommitFrame, NativeRawRecord, Hash32, int]:
        sequence = commit.sequence
        if sequence != expected_sequence:
            raise _error(
                SyntheticCapacityAdapterErrorCode.COMMIT_DIVERGENCE,
                "synthetic commit sequence differs from adapter state",
            )

        for ordinal, row in enumerate(commit.rows):
            if (
                type(row) is not SyntheticCapacityRow
                or row.commit_sequence != sequence
                or row.row_ordinal != ordinal
                or row.strategy != commit.strategy
                or row.logical_time_ns != commit.logical_time_ns
            ):
                raise _error(
                    SyntheticCapacityAdapterErrorCode.ROW_OWNERSHIP,
                    "synthetic row ownership differs from its enclosing commit",
                )
            self._validate_and_advance_stream(row=row, heads=heads)

        primary = commit.rows[0]
        record_id = _record_id(self._run_id, primary)
        raw_value = _row_value(primary, ownership=RAW_NATIVE_INBOX_OWNERSHIP)
        raw_value.update(
            {
                "arrival_sequence": sequence,
                "input_id": record_id,
                "run_id": self._run_id.value,
            }
        )
        raw_payload = canonical_json_bytes(raw_value) + b"\n"
        inbox = CompatibilityRecord.from_jsonl_bytes(raw_payload).to_logical_row(
            StreamId("inbox"), CommitOrdinal(0)
        )
        rows: list[LogicalRow] = [inbox]
        local_ordinals: dict[str, int] = {"inbox": 1}
        paper_counts["inbox"] = paper_counts.get("inbox", 0) + 1
        market_gap_count = 1 if primary.code == "MARKET_GAP" else 0

        for row in commit.rows[1:]:
            ordinal = local_ordinals.get(row.stream, 0)
            rows.append(
                LogicalRow(
                    stream_id=StreamId(row.stream),
                    ordinal=CommitOrdinal(ordinal),
                    value=_row_value(row, ownership=PAPER_DIRECT_OWNERSHIP),
                )
            )
            local_ordinals[row.stream] = ordinal + 1
            paper_counts[row.stream] = paper_counts.get(row.stream, 0) + 1
            if row.code == "MARKET_GAP":
                market_gap_count += 1

        frame = CommitFrame(
            run_id=self._run_id,
            commit_sequence=CommitSequence(sequence),
            previous_prefix_root=previous_prefix_root,
            rows=tuple(rows),
        )
        next_prefix = build_commit_logical(frame).prefix_root
        timestamp = _timestamp_text(commit.logical_time_ns)
        metadata = RawRecordMetadata(
            record_id=record_id,
            source_id=self._source_id,
            venue_id=self._venue_id,
            input_type=primary.record_type,
            source_stream_id=StreamId(primary.stream),
            source_first_sequence=EventSequence(primary.source_sequence),
            source_last_sequence=EventSequence(primary.source_sequence),
            arrival_sequence=EventSequence(sequence),
            source_timestamp=timestamp,
            received_timestamp=timestamp,
        )
        return (
            frame,
            NativeRawRecord(
                commit_sequence=sequence,
                payload=raw_payload,
                metadata=metadata,
            ),
            next_prefix,
            market_gap_count,
        )

    def build_phase1c_batch(
        self,
        commits: tuple[SyntheticCapacityCommit, ...],
    ) -> Phase1CBatch:
        """Materialize and chain one bounded tuple, then atomically advance state."""

        if type(commits) is not tuple or any(
            type(commit) is not SyntheticCapacityCommit for commit in commits
        ):
            raise _error(
                SyntheticCapacityAdapterErrorCode.TYPE_INVALID,
                "commits must be an exact tuple of SyntheticCapacityCommit values",
            )
        if not commits:
            raise _error(
                SyntheticCapacityAdapterErrorCode.EMPTY_BATCH,
                "synthetic capacity batch cannot be empty",
            )
        if len(commits) > self._max_batch_commits:
            raise _error(
                SyntheticCapacityAdapterErrorCode.BATCH_LIMIT,
                "synthetic capacity batch exceeds its configured memory bound",
            )

        expected = self._next_commit_sequence
        for commit in commits:
            if commit.sequence != expected:
                raise _error(
                    SyntheticCapacityAdapterErrorCode.COMMIT_DIVERGENCE,
                    "synthetic commits must be contiguous across adapter calls",
                )
            expected += 1

        heads = dict(self._source_stream_heads)
        paper_counts = dict(self._paper_stream_counts)
        previous = self._source_prefix_root
        frames: list[CommitFrame] = []
        raw_records: list[NativeRawRecord] = []
        market_gaps = 0

        for offset, commit in enumerate(commits):
            frame, record, previous, commit_market_gaps = self._adapt_commit(
                commit=commit,
                expected_sequence=self._next_commit_sequence + offset,
                previous_prefix_root=previous,
                heads=heads,
                paper_counts=paper_counts,
            )
            frames.append(frame)
            raw_records.append(record)
            market_gaps += commit_market_gaps

        batch = Phase1CBatch(source_frames=tuple(frames), raw_records=tuple(raw_records))
        self._source_stream_heads = heads
        self._paper_stream_counts = paper_counts
        self._source_prefix_root = previous
        self._next_commit_sequence += len(commits)
        self._commit_count += len(commits)
        self._logical_row_count += sum(len(commit.rows) for commit in commits)
        self._raw_record_count += len(commits)
        self._market_gap_count += market_gaps
        self._last_record_id = raw_records[-1].metadata.record_id
        return batch

    def checkpoint_state(
        self,
        *,
        workload_prefix: CapacityWorkloadDigest | None = None,
    ) -> CheckpointState:
        """Return a repeatable O(stream-count) state suitable for Paper seals."""

        covered = self._next_commit_sequence - 1
        source_heads: CanonicalObject = {
            stream: head.canonical_value()
            for stream, head in sorted(self._source_stream_heads.items())
        }
        paper_counts: CanonicalObject = {
            stream: count for stream, count in sorted(self._paper_stream_counts.items())
        }
        markers: list[CanonicalValue] = [marker for marker in CAPACITY_MARKERS]
        common: CanonicalObject = {
            "contract": SYNTHETIC_CAPACITY_ADAPTER_CONTRACT,
            "markers": markers,
        }

        def rows_for(*streams: str) -> int:
            return sum(
                self._source_stream_heads[stream].row_count
                for stream in streams
                if stream in self._source_stream_heads
            )

        return CheckpointState(
            adapter={
                **common,
                "commit_count": self._commit_count,
                "logical_row_count": self._logical_row_count,
                "run_id": self._run_id.value,
                "source_prefix_root": self._source_prefix_root.hex(),
                "start_prefix_root": self._start_prefix_root.hex(),
            },
            ledger={
                **common,
                "row_count": rows_for("ledger", "ledger_entries", "ledger_transactions"),
            },
            projection={
                **common,
                "row_count": rows_for("projection_history", "projections"),
            },
            sessions={
                **common,
                "row_count": rows_for("runtime_sessions", "sessions"),
            },
            incidents={
                **common,
                "market_gap_count": self._market_gap_count,
                "row_count": rows_for("incidents"),
            },
            cursors={
                **common,
                "covered_commit_sequence": covered,
                "last_raw_record_id": self._last_record_id,
                "next_commit_sequence": self._next_commit_sequence,
                "raw_record_count": self._raw_record_count,
                "workload_prefix": (
                    None
                    if workload_prefix is None
                    else {
                        "commit_count": workload_prefix.commit_count,
                        "logical_row_count": workload_prefix.logical_row_count,
                        "sha256": workload_prefix.sha256,
                    }
                ),
            },
            stream_heads={
                **common,
                "paper_stream_counts": paper_counts,
                "source_stream_heads": source_heads,
            },
        )


SyntheticCapacityAdapter = SyntheticCapacityPhase1CAdapter


__all__ = [
    "DEFAULT_MAX_BATCH_COMMITS",
    "DEFAULT_MAX_TRACKED_STREAMS",
    "PAPER_DIRECT_OWNERSHIP",
    "RAW_NATIVE_INBOX_OWNERSHIP",
    "SYNTHETIC_CAPACITY_ADAPTER_CONTRACT",
    "SYNTHETIC_CAPACITY_ROW_CONTRACT",
    "SYNTHETIC_CAPACITY_SOURCE_ID",
    "SYNTHETIC_CAPACITY_VENUE_ID",
    "SyntheticCapacityAdapter",
    "SyntheticCapacityAdapterError",
    "SyntheticCapacityAdapterErrorCode",
    "SyntheticCapacityPhase1CAdapter",
]
