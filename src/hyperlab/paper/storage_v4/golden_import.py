"""Streaming Golden V3 to Storage v4 compatibility commit assembly."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, TypeAlias, cast

from hyperlab.paper.golden_v3 import (
    GOLDEN_STREAM_NAMES,
    GoldenVerification,
    iter_golden_stream,
)
from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.contracts import CompatibilityRecord
from hyperlab.paper.storage_v4.exact_decimal import ExactDecimalSum
from hyperlab.paper.storage_v4.types import (
    CanonicalObject,
    CommitFrame,
    CommitOrdinal,
    CommitSequence,
    Hash32,
    RunId,
    StreamId,
)

GoldenRow: TypeAlias = Mapping[str, object]
GoldenStreams: TypeAlias = Mapping[str, Iterable[GoldenRow]]


class GoldenImportError(ValueError):
    """The certified logical streams cannot form an exact compatibility history."""


@dataclass(frozen=True, slots=True)
class GoldenImportExpectations:
    run_id: RunId
    export_root: Hash32
    commit_count: int
    row_count: int
    stream_row_counts: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        if type(self.run_id) is not RunId or type(self.export_root) is not Hash32:
            raise TypeError("Golden import expectations require RunId and Hash32")
        if type(self.commit_count) is not int or self.commit_count < 1:
            raise ValueError("Golden import commit_count must be positive")
        if type(self.row_count) is not int or self.row_count < self.commit_count:
            raise ValueError("Golden import row_count is incompatible")
        if tuple(name for name, _ in self.stream_row_counts) != GOLDEN_STREAM_NAMES:
            raise ValueError("Golden import stream counts must use the fixed 13-stream order")
        if any(type(count) is not int or count < 0 for _, count in self.stream_row_counts):
            raise ValueError("Golden import stream row counts must be nonnegative")
        if sum(count for _, count in self.stream_row_counts) != self.row_count:
            raise ValueError("Golden import stream counts do not sum to row_count")

    @classmethod
    def from_verification(cls, verification: GoldenVerification) -> GoldenImportExpectations:
        if type(verification) is not GoldenVerification:
            raise TypeError("verification must be GoldenVerification")
        manifest = verification.manifest
        raw_streams = manifest.get("streams")
        if not isinstance(raw_streams, dict):
            raise GoldenImportError("verified Golden manifest has no stream descriptors")
        counts: list[tuple[str, int]] = []
        for name in GOLDEN_STREAM_NAMES:
            descriptor = raw_streams.get(name)
            if not isinstance(descriptor, dict):
                raise GoldenImportError(f"verified Golden stream {name!r} is missing")
            count = descriptor.get("row_count")
            if type(count) is not int or count < 0:
                raise GoldenImportError(f"verified Golden stream {name!r} has invalid row_count")
            counts.append((name, count))
        run_id = manifest.get("run_id")
        if type(run_id) is not str:
            raise GoldenImportError("verified Golden run_id is invalid")
        commit_count = dict(counts)["commits"]
        return cls(
            run_id=RunId(run_id),
            export_root=Hash32.from_hex(verification.root_hash),
            commit_count=commit_count,
            row_count=sum(count for _, count in counts),
            stream_row_counts=tuple(counts),
        )


@dataclass(frozen=True, slots=True)
class CompatibilityCheckpointSections:
    adapter_state: CanonicalObject
    ledger_state: CanonicalObject
    projection_state: CanonicalObject
    sessions_state: CanonicalObject
    incidents_state: CanonicalObject
    cursors: CanonicalObject
    stream_heads: CanonicalObject


@dataclass(frozen=True, slots=True)
class AssembledGoldenCommit:
    """One frame plus a checkpoint builder valid until iteration advances.

    Building the complete checkpoint is deliberately lazy: the projection
    snapshot and stream-head table are only serialized when the overlay is
    actually sealed.  The assembler invalidates the builder before it starts
    mutating state for the next commit, so a stale caller fails closed instead
    of receiving a later state under an earlier frame identity.
    """

    frame: CommitFrame
    cumulative_rows: int
    cumulative_stream_counts: tuple[tuple[str, int], ...]
    checkpoint_sections_factory: Callable[[], CompatibilityCheckpointSections]

    def build_checkpoint_sections(self) -> CompatibilityCheckpointSections:
        sections = self.checkpoint_sections_factory()
        if type(sections) is not CompatibilityCheckpointSections:
            raise GoldenImportError(
                "Golden checkpoint factory returned an invalid section set"
            )
        return sections


@dataclass(slots=True)
class _CheckpointBuildCursor:
    active_sequence: int | None = None


class _Peekable:
    def __init__(self, values: Iterable[GoldenRow]) -> None:
        self._iterator = iter(values)
        self._next: GoldenRow | None = None
        self._loaded = False

    def peek(self) -> GoldenRow | None:
        if not self._loaded:
            self._next = next(self._iterator, None)
            self._loaded = True
        return self._next

    def pop(self) -> GoldenRow:
        value = self.peek()
        if value is None:
            raise GoldenImportError("Golden stream ended before its declared boundary")
        self._loaded = False
        self._next = None
        return value

    def require_end(self, *, stream: str) -> None:
        if self.peek() is not None:
            raise GoldenImportError(f"Golden stream {stream!r} has unconsumed rows")


def _canonical_jsonl(row: GoldenRow) -> bytes:
    try:
        encoded = json.dumps(
            dict(row),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8", errors="strict")
    except (RecursionError, TypeError, UnicodeEncodeError, ValueError) as error:
        raise GoldenImportError("Golden row cannot be rematerialized canonically") from error
    return encoded + b"\n"


def _integer(row: GoldenRow, key: str, *, minimum: int = 0) -> int:
    value = row.get(key)
    if type(value) is not int or value < minimum:
        raise GoldenImportError(f"Golden field {key!r} must be an integer >= {minimum}")
    return value


def _optional_integer(row: GoldenRow, key: str, *, minimum: int = 0) -> int | None:
    value = row.get(key)
    if value is None:
        return None
    if type(value) is not int or value < minimum:
        raise GoldenImportError(f"Golden field {key!r} must be null or an integer")
    return value


def _text(row: GoldenRow, key: str) -> str:
    value = row.get(key)
    if type(value) is not str or not value:
        raise GoldenImportError(f"Golden field {key!r} must be nonempty text")
    return value


def _digest_text(row: GoldenRow, key: str) -> str:
    value = _text(row, key)
    try:
        Hash32.from_hex(value)
    except ValueError as error:
        raise GoldenImportError(f"Golden field {key!r} is not lowercase SHA-256") from error
    return value


def _ledger_amount(row: GoldenRow) -> tuple[str, str, str]:
    account = _text(row, "account")
    unit = _text(row, "unit")
    amount_text = _text(row, "amount_text")
    try:
        ExactDecimalSum.from_text(amount_text)
    except ValueError as error:
        raise GoldenImportError("Golden ledger amount_text is not exact Decimal") from error
    return account, unit, amount_text


def _lazy_checkpoint_sections_factory(
    *,
    cursor: _CheckpointBuildCursor,
    expected: GoldenImportExpectations,
    sequence: int,
    cumulative_rows: int,
    commit_hash: str,
    projection: GoldenRow,
    ledger_balances: Mapping[str, Mapping[str, ExactDecimalSum]],
    counts: Mapping[str, int],
    hashes: Mapping[str, Any],
    last_line_hash: Mapping[str, str | None],
    sessions: list[str],
    incidents: list[str],
    last_entry_hash: str | None,
    last_transaction_hash: str | None,
) -> Callable[[], CompatibilityCheckpointSections]:
    def build_checkpoint_sections() -> CompatibilityCheckpointSections:
        if cursor.active_sequence != sequence:
            raise GoldenImportError(
                "checkpoint snapshot requested after assembler advanced"
            )
        return CompatibilityCheckpointSections(
            adapter_state=cast(
                CanonicalObject,
                {
                    "contract": "hyperlab.storage_v4.golden_import.v1",
                    "export_root": expected.export_root.hex(),
                    "last_v3_commit_hash": commit_hash,
                    "processed_commits": sequence,
                    "processed_rows": cumulative_rows,
                },
            ),
            ledger_state=cast(
                CanonicalObject,
                {
                    "balances": {
                        account: {
                            unit: amount.text
                            for unit, amount in sorted(by_unit.items())
                        }
                        for account, by_unit in sorted(ledger_balances.items())
                    },
                    "entry_count": counts["ledger_entries"],
                    "last_entry_hash": last_entry_hash,
                    "last_transaction_hash": last_transaction_hash,
                    "transaction_count": counts["ledger_transactions"],
                },
            ),
            projection_state=cast(
                CanonicalObject,
                {
                    "canonical_json": _canonical_jsonl(projection)[:-1].decode(
                        "utf-8"
                    ),
                    "projection_hash": _digest_text(projection, "projection_hash"),
                    "revision": sequence,
                },
            ),
            sessions_state=cast(CanonicalObject, {"records": list(sessions)}),
            incidents_state=cast(CanonicalObject, {"records": list(incidents)}),
            cursors=cast(
                CanonicalObject,
                {
                    "stream_row_counts": {
                        name: counts[name] for name in GOLDEN_STREAM_NAMES
                    }
                },
            ),
            stream_heads=cast(
                CanonicalObject,
                {
                    "streams": {
                        name: {
                            "last_line_sha256": last_line_hash[name],
                            "logical_sha256": hashes[name].copy().hexdigest(),
                            "row_count": counts[name],
                        }
                        for name in GOLDEN_STREAM_NAMES
                    }
                },
            ),
        )

    return build_checkpoint_sections


def _validate_run(row: GoldenRow, run_id: RunId) -> None:
    value = row.get("run_id")
    if value is not None and value != run_id.value:
        raise GoldenImportError("Golden row belongs to another run")


def _one(values: Iterable[GoldenRow], *, stream: str) -> GoldenRow:
    rows = tuple(values)
    if len(rows) != 1:
        raise GoldenImportError(f"Golden stream {stream!r} must contain exactly one row")
    return rows[0]


class _CommittedStreamCursor:
    """Drain only rows owned by the current commit from one ordered stream."""

    def __init__(
        self,
        values: Iterable[GoldenRow],
        *,
        stream: str,
        run_id: RunId,
    ) -> None:
        self._values = _Peekable(values)
        self._stream = stream
        self._run_id = run_id
        self._prior_sequence = 0

    def pop_for(self, sequence: int) -> tuple[GoldenRow, ...]:
        rows: list[GoldenRow] = []
        while (row := self._values.peek()) is not None:
            _validate_run(row, self._run_id)
            row_sequence = _optional_integer(row, "commit_sequence", minimum=1)
            if row_sequence is None:
                raise GoldenImportError(
                    "certified compatibility importer refuses uncommitted "
                    f"{self._stream} rows"
                )
            if row_sequence < self._prior_sequence:
                raise GoldenImportError(
                    f"Golden stream {self._stream!r} commit order regressed"
                )
            if row_sequence < sequence:
                raise GoldenImportError(
                    f"Golden stream {self._stream!r} row missed its commit boundary"
                )
            if row_sequence > sequence:
                break
            rows.append(self._values.pop())
            self._prior_sequence = row_sequence
        return tuple(rows)

    def require_end(self) -> None:
        if self._values.peek() is not None:
            raise GoldenImportError(
                f"Golden committed {self._stream} row lies outside commit range"
            )


class GoldenCommitAssembler:
    """Assemble exactly one Storage v4 frame for each Golden V3 commit."""

    def __init__(
        self,
        streams: GoldenStreams,
        expectations: GoldenImportExpectations,
    ) -> None:
        if set(streams) != set(GOLDEN_STREAM_NAMES):
            raise GoldenImportError("Golden stream set differs from the fixed 13-stream contract")
        self._expectations = expectations
        self._streams = streams

    @classmethod
    def from_verification(
        cls,
        verification: GoldenVerification,
    ) -> GoldenCommitAssembler:
        expectations = GoldenImportExpectations.from_verification(verification)
        streams: dict[str, Iterable[GoldenRow]] = {
            name: cast(
                Iterable[GoldenRow],
                iter_golden_stream(
                    verification.export_root,
                    name,
                    verification=verification,
                ),
            )
            for name in GOLDEN_STREAM_NAMES
        }
        return cls(streams, expectations)

    def __iter__(self) -> Iterator[AssembledGoldenCommit]:
        return self.iter_commits()

    def iter_commits(self) -> Iterator[AssembledGoldenCommit]:
        expected = self._expectations
        run_id = expected.run_id

        schema = tuple(self._streams["schema"])
        run = _one(self._streams["run"], stream="run")
        _validate_run(run, run_id)
        projection_current = _one(
            self._streams["projection_current"], stream="projection_current"
        )
        heads = _one(self._streams["heads"], stream="heads")
        _validate_run(projection_current, run_id)
        _validate_run(heads, run_id)

        committed = {
            name: _CommittedStreamCursor(
                self._streams[name],
                stream=name,
                run_id=run_id,
            )
            for name in (
                "ledger_transactions",
                "ledger_entries",
                "alerts",
                "runtime_sessions",
                "incidents",
            )
        }
        inbox = _Peekable(self._streams["inbox"])
        events = _Peekable(self._streams["events"])
        commits = _Peekable(self._streams["commits"])
        projections = _Peekable(self._streams["projection_history"])

        counts = {name: 0 for name in GOLDEN_STREAM_NAMES}
        hashes = {name: hashlib.sha256() for name in GOLDEN_STREAM_NAMES}
        last_line_hash: dict[str, str | None] = {name: None for name in GOLDEN_STREAM_NAMES}
        sessions: list[str] = []
        incidents: list[str] = []
        previous_prefix = expected.export_root
        cumulative_rows = 0
        last_transaction_hash: str | None = None
        last_entry_hash: str | None = None
        ledger_balances: dict[str, dict[str, ExactDecimalSum]] = {}
        latest_projection: GoldenRow | None = None
        checkpoint_cursor = _CheckpointBuildCursor()

        for sequence in range(1, expected.commit_count + 1):
            inbox_row = inbox.pop()
            commit_row = commits.pop()
            _validate_run(inbox_row, run_id)
            _validate_run(commit_row, run_id)
            if _integer(inbox_row, "commit_sequence", minimum=1) != sequence:
                raise GoldenImportError("Golden inbox sequence has a gap or reorder")
            if _integer(commit_row, "commit_sequence", minimum=1) != sequence:
                raise GoldenImportError("Golden commit sequence has a gap or reorder")
            if _text(inbox_row, "input_id") != _text(commit_row, "input_id"):
                raise GoldenImportError("Golden inbox and commit input identities differ")
            commit_hash = _digest_text(commit_row, "commit_hash")
            if _digest_text(inbox_row, "commit_hash") != commit_hash:
                raise GoldenImportError("Golden inbox and commit hashes differ")

            event_rows: list[GoldenRow] = []
            first_event = _optional_integer(commit_row, "first_event_sequence", minimum=1)
            last_event = _optional_integer(commit_row, "last_event_sequence", minimum=1)
            raw_hashes = commit_row.get("event_hashes")
            if type(raw_hashes) is not list or any(type(value) is not str for value in raw_hashes):
                raise GoldenImportError("Golden commit event_hashes must be a string array")
            event_hashes = cast(list[str], raw_hashes)
            if first_event is None or last_event is None:
                if first_event is not None or last_event is not None or event_hashes:
                    raise GoldenImportError("Golden empty-event commit boundaries are inconsistent")
            else:
                if last_event < first_event or len(event_hashes) != last_event - first_event + 1:
                    raise GoldenImportError("Golden event range and hash count differ")
                for expected_event_sequence, expected_hash in zip(
                    range(first_event, last_event + 1), event_hashes, strict=True
                ):
                    event = events.pop()
                    _validate_run(event, run_id)
                    if _integer(event, "sequence", minimum=1) != expected_event_sequence:
                        raise GoldenImportError("Golden event sequence differs from commit range")
                    if _text(event, "input_id") != _text(commit_row, "input_id"):
                        raise GoldenImportError("Golden event input identity differs from commit")
                    if _digest_text(event, "event_hash") != expected_hash:
                        raise GoldenImportError("Golden event hash differs from commit witness")
                    event_rows.append(event)

            projection_rows: list[GoldenRow] = []
            expected_revisions = (0, 1) if sequence == 1 else (sequence,)
            for revision in expected_revisions:
                projection = projections.pop()
                _validate_run(projection, run_id)
                if _integer(projection, "revision") != revision:
                    raise GoldenImportError("Golden projection history has a gap or reorder")
                projection_rows.append(projection)
                latest_projection = projection
            if _integer(commit_row, "projection_revision") != sequence:
                raise GoldenImportError("Golden commit projection revision differs")
            if latest_projection is None or _digest_text(
                latest_projection, "projection_hash"
            ) != _digest_text(commit_row, "projection_hash"):
                raise GoldenImportError("Golden commit projection hash differs from history")

            rows_by_stream: dict[str, tuple[GoldenRow, ...]] = {
                "schema": schema if sequence == 1 else (),
                "run": (run,) if sequence == expected.commit_count else (),
                "inbox": (inbox_row,),
                "events": tuple(event_rows),
                "ledger_transactions": committed["ledger_transactions"].pop_for(sequence),
                "ledger_entries": committed["ledger_entries"].pop_for(sequence),
                "alerts": committed["alerts"].pop_for(sequence),
                "commits": (commit_row,),
                "projection_history": tuple(projection_rows),
                "projection_current": (
                    (projection_current,) if sequence == expected.commit_count else ()
                ),
                "runtime_sessions": committed["runtime_sessions"].pop_for(sequence),
                "incidents": committed["incidents"].pop_for(sequence),
                "heads": (heads,) if sequence == expected.commit_count else (),
            }

            logical_rows = []
            for stream_name in GOLDEN_STREAM_NAMES:
                stream_rows = rows_by_stream[stream_name]
                for ordinal, row in enumerate(stream_rows):
                    _validate_run(row, run_id)
                    line = _canonical_jsonl(row)
                    record = CompatibilityRecord.from_jsonl_bytes(line)
                    logical_rows.append(
                        record.to_logical_row(
                            StreamId(stream_name),
                            CommitOrdinal(ordinal),
                        )
                    )
                    counts[stream_name] += 1
                    cumulative_rows += 1
                    hashes[stream_name].update(line)
                    last_line_hash[stream_name] = hashlib.sha256(line).hexdigest()
                    if stream_name == "runtime_sessions":
                        sessions.append(record.canonical_json_text)
                    elif stream_name == "incidents":
                        incidents.append(record.canonical_json_text)

            transactions = rows_by_stream["ledger_transactions"]
            entries = rows_by_stream["ledger_entries"]
            if transactions:
                last_transaction_hash = _digest_text(transactions[-1], "transaction_hash")
            if entries:
                last_entry_hash = _digest_text(entries[-1], "entry_hash")
                for entry in entries:
                    account, unit, amount_text = _ledger_amount(entry)
                    by_unit = ledger_balances.setdefault(account, {})
                    by_unit[unit] = by_unit.get(unit, ExactDecimalSum()).add_text(
                        amount_text
                    )

            frame = CommitFrame(
                run_id=run_id,
                commit_sequence=CommitSequence(sequence),
                previous_prefix_root=previous_prefix,
                rows=tuple(logical_rows),
                legacy_v3_identity=Hash32.from_hex(commit_hash),
            )
            previous_prefix = build_commit_logical(frame).prefix_root
            if latest_projection is None:
                raise GoldenImportError(
                    "Golden commit lacks a projection checkpoint snapshot"
                )
            checkpoint_cursor.active_sequence = sequence
            build_checkpoint_sections = _lazy_checkpoint_sections_factory(
                cursor=checkpoint_cursor,
                expected=expected,
                sequence=sequence,
                cumulative_rows=cumulative_rows,
                commit_hash=commit_hash,
                projection=latest_projection,
                ledger_balances=ledger_balances,
                counts=counts,
                hashes=hashes,
                last_line_hash=last_line_hash,
                sessions=sessions,
                incidents=incidents,
                last_entry_hash=last_entry_hash,
                last_transaction_hash=last_transaction_hash,
            )

            yield AssembledGoldenCommit(
                frame=frame,
                cumulative_rows=cumulative_rows,
                cumulative_stream_counts=tuple(
                    (name, counts[name]) for name in GOLDEN_STREAM_NAMES
                ),
                checkpoint_sections_factory=build_checkpoint_sections,
            )
            checkpoint_cursor.active_sequence = None

        inbox.require_end(stream="inbox")
        events.require_end(stream="events")
        commits.require_end(stream="commits")
        projections.require_end(stream="projection_history")
        for cursor in committed.values():
            cursor.require_end()
        actual_counts = tuple((name, counts[name]) for name in GOLDEN_STREAM_NAMES)
        if actual_counts != expected.stream_row_counts:
            raise GoldenImportError("assembled Golden per-stream counts differ from manifest")
        if cumulative_rows != expected.row_count:
            raise GoldenImportError("assembled Golden row count differs from manifest")


def golden_streams_from_verification(
    verification: GoldenVerification,
) -> dict[str, Iterable[GoldenRow]]:
    """Build 13 iterables that reuse one already-exhaustive Golden verification."""

    return {
        name: cast(
            Iterable[GoldenRow],
            iter_golden_stream(
                verification.export_root,
                name,
                verification=verification,
            ),
        )
        for name in GOLDEN_STREAM_NAMES
    }


__all__ = [
    "AssembledGoldenCommit",
    "CompatibilityCheckpointSections",
    "GoldenCommitAssembler",
    "GoldenImportError",
    "GoldenImportExpectations",
    "GoldenRow",
    "GoldenStreams",
    "golden_streams_from_verification",
]
