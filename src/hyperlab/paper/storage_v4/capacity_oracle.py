"""Independent logical oracle for reopened synthetic V4_NATIVE capacity stores."""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from itertools import chain, zip_longest
from typing import Protocol

from .canonical import build_commit_logical, canonical_json_bytes
from .capacity import (
    CAPACITY_MARKERS,
    CapacityWorkloadHasher,
    CapacityWorkloadManifest,
    SyntheticCapacityRow,
    iter_capacity_commits,
)
from .capacity_adapter import (
    PAPER_DIRECT_OWNERSHIP,
    RAW_NATIVE_INBOX_OWNERSHIP,
    SYNTHETIC_CAPACITY_ROW_CONTRACT,
)
from .native_journal import NativeJournalError, rematerialize_native_row
from .phase1c_progress import (
    AUDIT_HEARTBEAT_MIN_SECONDS,
    AuditProgressCallback,
    BoundedAuditProgress,
)
from .raw_reference import RawReferenceResolverV2
from .repository import StorageRepository
from .types import CanonicalObject, CanonicalValue, RunId, StreamId

_RECORD_ID_DOMAIN = b"HL4-SYNTHETIC-CAPACITY-RAW-RECORD-V1\x00"


class CapacityOracleError(RuntimeError):
    """The reopened physical store differs from the generator manifest."""


class _Hasher(Protocol):
    def update(self, data: bytes, /) -> None: ...

    def hexdigest(self) -> str: ...


@dataclass(frozen=True, slots=True)
class CapacityOracleStream:
    stream_id: str
    row_count: int
    logical_sha256: str


@dataclass(frozen=True, slots=True)
class CapacityOracleReport:
    commit_count: int
    logical_row_count: int
    workload_sha256: str
    final_prefix_root: str
    market_gap_count: int
    streams: tuple[CapacityOracleStream, ...]


def _record_id(run_id: RunId, row: SyntheticCapacityRow) -> str:
    digest = hashlib.sha256(_RECORD_ID_DOMAIN)
    encoded_run = run_id.value.encode("utf-8", errors="strict")
    digest.update(len(encoded_run).to_bytes(4, "big"))
    digest.update(encoded_run)
    descriptor = canonical_json_bytes(row.descriptor())
    digest.update(len(descriptor).to_bytes(8, "big"))
    digest.update(descriptor)
    return f"synthetic-capacity-v1:{digest.hexdigest()}"


def _row_value(row: SyntheticCapacityRow, *, ownership: str) -> CanonicalObject:
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


def compare_capacity_native_exact(
    repository: StorageRepository,
    resolver: RawReferenceResolverV2,
    manifest: CapacityWorkloadManifest,
    *,
    run_id: RunId,
    include_tail: bool = False,
    progress: AuditProgressCallback | None = None,
    heartbeat_interval_seconds: float = AUDIT_HEARTBEAT_MIN_SECONDS,
) -> CapacityOracleReport:
    """Compare every reopened logical row against a fresh generator pass.

    Historical iteration intentionally excludes the mutable overlay.  Tail
    matrix certification opts in to the already authenticated startup tail;
    the default remains sealed-history-only for existing capacity callers.
    """

    if type(repository) is not StorageRepository:
        raise TypeError("capacity oracle requires StorageRepository")
    if not isinstance(resolver, RawReferenceResolverV2):
        raise TypeError("capacity oracle requires RawReferenceResolverV2")
    if not isinstance(manifest, CapacityWorkloadManifest) or type(run_id) is not RunId:
        raise TypeError("capacity oracle requires manifest and RunId")
    if type(include_tail) is not bool:
        raise TypeError("include_tail must be an exact bool")

    audit_progress = BoundedAuditProgress(
        phase="capacity_oracle_full_audit",
        progress=progress,
        totals={
            "commits": manifest.commit_count,
            "rows": manifest.logical_row_count,
        },
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )
    expected_commits = iter_capacity_commits(manifest.config)
    actual_frames = (
        chain(
            repository.iter_historical_frames(),
            repository.startup_report.tail_frames,
        )
        if include_tail
        else repository.iter_historical_frames()
    )
    hasher = CapacityWorkloadHasher()
    stream_counts: dict[str, int] = {}
    stream_hashes: dict[str, _Hasher] = {}
    market_gaps = 0
    rows_total = 0
    commits_total = 0
    expected_prefix = repository.config.start_prefix_root

    for actual, expected in zip_longest(actual_frames, expected_commits):
        if actual is None or expected is None:
            raise CapacityOracleError("capacity commit cardinality differs")
        hasher.update(expected)
        sequence = expected.sequence
        if (
            actual.run_id != run_id
            or int(actual.commit_sequence) != sequence
            or actual.previous_prefix_root != expected_prefix
            or len(actual.rows) != len(expected.rows)
        ):
            raise CapacityOracleError(f"capacity frame ownership differs at commit {sequence}")

        expected_rows: list[tuple[StreamId, int, bytes]] = []
        primary = expected.rows[0]
        raw_value = _row_value(primary, ownership=RAW_NATIVE_INBOX_OWNERSHIP)
        raw_value.update(
            {
                "arrival_sequence": sequence,
                "input_id": _record_id(run_id, primary),
                "run_id": run_id.value,
            }
        )
        expected_rows.append((StreamId("inbox"), 0, canonical_json_bytes(raw_value) + b"\n"))
        local_ordinals: dict[str, int] = {"inbox": 1}
        for row in expected.rows[1:]:
            ordinal = local_ordinals.get(row.stream, 0)
            expected_rows.append(
                (
                    StreamId(row.stream),
                    ordinal,
                    canonical_json_bytes(_row_value(row, ownership=PAPER_DIRECT_OWNERSHIP)) + b"\n",
                )
            )
            local_ordinals[row.stream] = ordinal + 1

        for actual_row, (stream_id, ordinal, expected_line) in zip(
            actual.rows,
            expected_rows,
            strict=True,
        ):
            if actual_row.stream_id != stream_id or int(actual_row.ordinal) != ordinal:
                raise CapacityOracleError(f"capacity stream ownership differs at commit {sequence}")
            try:
                actual_line = rematerialize_native_row(actual_row, resolver)
            except (NativeJournalError, TypeError, ValueError) as error:
                raise CapacityOracleError(
                    f"capacity row cannot be rematerialized at commit {sequence}"
                ) from error
            if actual_line != expected_line:
                raise CapacityOracleError(f"capacity logical bytes differ at commit {sequence}")
            stream = stream_id.value
            stream_counts[stream] = stream_counts.get(stream, 0) + 1
            digest = stream_hashes.setdefault(stream, hashlib.sha256())
            digest.update(actual_line)
            rows_total += 1
        market_gaps += sum(1 for row in expected.rows if row.code == "MARKET_GAP")
        expected_prefix = build_commit_logical(actual).prefix_root
        commits_total += 1
        if commits_total % 256 == 0 or commits_total == manifest.commit_count:
            audit_progress.advance(
                {"commits": commits_total, "rows": rows_total}
            )

    observed = hasher.finalize()
    if (
        observed.sha256 != manifest.workload_sha256
        or observed.commit_count != manifest.commit_count
        or observed.logical_row_count != manifest.logical_row_count
        or commits_total != manifest.commit_count
        or rows_total != manifest.logical_row_count
    ):
        raise CapacityOracleError("capacity workload digest or counts differ")
    report = CapacityOracleReport(
        commit_count=commits_total,
        logical_row_count=rows_total,
        workload_sha256=observed.sha256,
        final_prefix_root=expected_prefix.hex(),
        market_gap_count=market_gaps,
        streams=tuple(
            CapacityOracleStream(
                stream_id=stream,
                row_count=stream_counts[stream],
                logical_sha256=stream_hashes[stream].hexdigest(),
            )
            for stream in sorted(stream_counts)
        ),
    )
    audit_progress.complete(
        {
            "commits": report.commit_count,
            "rows": report.logical_row_count,
        }
    )
    return report


__all__ = [
    "CapacityOracleError",
    "CapacityOracleReport",
    "CapacityOracleStream",
    "compare_capacity_native_exact",
]
