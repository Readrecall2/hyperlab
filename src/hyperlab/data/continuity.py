from __future__ import annotations

import hashlib
import json
import re
import time
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path

import pyarrow as pa

from hyperlab.data.continuity_store import (
    BoundedRowStore,
    SpilledSequence,
    SpilledTimestampSeries,
    StreamingMetrics,
    configured_scratch_parent,
)
from hyperlab.data.lake import (
    Gap,
    InventoryReport,
    PartitionKey,
    PartitionManifest,
    PartitionValidationError,
    iter_hashed_batches,
    validate_partition,
)
from hyperlab.data.schema import BREAKING_SCHEMA_TRANSITIONS, RecordType, schema_for

PHASE_10_STATUS = "BLOCKED_PRECONDITION_NOT_MET"
BINANCE = "binance_usdm"
HYPERLIQUID = "hyperliquid"
PHASE_10_ASSETS = frozenset({"BTC", "ETH"})
DEFAULT_STATE_TTL = timedelta(seconds=30)
MAX_CLOCK_SAMPLING_INTERVAL_MS = 10_000
MAX_CLOCK_AGE_MS = 15_000
MAX_CLOCK_UNCERTAINTY_MS = Decimal("50")
MAX_CONSECUTIVE_REJECTED_CLOCK_PROBES = 1
_REQUIRED_WIRE_FORENSIC_SAMPLE_LIMIT = 32

_REQUIRED_TYPES = frozenset(
    {
        RecordType.BBO,
        RecordType.L2_BOOK_STATE,
        RecordType.L2_SNAPSHOT,
        RecordType.TRADE,
        RecordType.WIRE_MESSAGE,
        RecordType.CLOCK_SYNC,
        RecordType.CONNECTION_EVENT,
    }
)
_FAIL_CLOSED_EVENTS = frozenset({"disconnect", "gap"})
_CANONICAL_PARQUET = re.compile(r"^part-[0-9a-f]{64}\.parquet$")
_CANONICAL_MANIFEST = re.compile(r"^part-[0-9a-f]{64}\.manifest\.json$")
_FUNDING_BUCKET_TOLERANCE_NS = 60 * 1_000_000_000
_L2_INTEGRITY_METADATA = {
    RecordType.L2_SNAPSHOT: (
        "snapshot_id",
        (
            "book_epoch_id",
            "connection_id",
            "last_sequence",
            "event_time",
            "exchange_time",
            "received_time",
            "source_sequence",
        ),
        "snapshot",
    ),
    RecordType.L2_DELTA: (
        "update_id",
        (
            "book_epoch_id",
            "connection_id",
            "first_sequence",
            "last_sequence",
            "event_time",
            "exchange_time",
            "received_time",
            "source_sequence",
        ),
        "delta",
    ),
}


@dataclass(frozen=True, slots=True, order=True)
class Interval:
    """A half-open interval whose lineage tag may never be interpolated away."""

    start: datetime
    end: datetime
    tag: str

    def __post_init__(self) -> None:
        start = _utc(self.start)
        end = _utc(self.end)
        if end <= start:
            raise ValueError("continuity interval end must be after start")
        if not self.tag:
            raise ValueError("continuity interval tag cannot be empty")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)


@dataclass(frozen=True, slots=True)
class _LoadedLake:
    inventory: InventoryReport
    inventory_partition_count: int
    store: BoundedRowStore
    wire_identities: frozenset[tuple[str, str, int, str]]
    relevant_gaps: SpilledSequence[tuple[str, Gap, bool]]
    metrics: StreamingMetrics


@dataclass(frozen=True, slots=True)
class _ConnectionLineage:
    identities: Mapping[tuple[str, int, str], tuple[str, datetime]]
    observed_captures: frozenset[str]
    eligible_captures: frozenset[str]
    rejected_identity_count: int
    rejected_captures: frozenset[str]
    unbound_connect_events: int


@dataclass(frozen=True, slots=True)
class _ClockSample:
    interval: Interval
    drift_ms: Decimal
    uncertainty_ms: Decimal


@dataclass(frozen=True, slots=True)
class _ClockAudit:
    intervals: Mapping[str, tuple[Interval, ...]]
    legacy_samples: int
    valid_samples: int
    invalid_samples: int
    rejected_probe_samples: int
    hard_invalid_samples: int
    failure_events: int
    policy_rejections: int
    identity_rejections: int
    unbound_invalid_events: int
    in_window_invalid_events: int
    in_window_rejected_probe_events: int
    in_window_hard_invalid_events: int
    in_window_failure_events: int
    consecutive_rejection_violations: int
    consecutive_rejection_violation_captures: frozenset[str]
    consecutive_rejection_outages: Mapping[str, tuple[Interval, ...]]
    max_consecutive_rejected_probes: int
    spacing_violations: int
    spacing_violation_captures: frozenset[str]
    offset_discontinuities: int
    offset_discontinuity_captures: frozenset[str]
    max_sample_gap_ms: float | None
    forensics: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _EventOutageAudit:
    intervals: tuple[Interval, ...]
    unbound_fail_closed_events: int
    unbound_resync_events: int
    active_event_captures: frozenset[str]
    in_window_gap_events: int
    unclean_in_window_disconnect_events: int
    clean_terminal_roles: Mapping[str, frozenset[str]]
    failure_events_by_capture: Mapping[str, tuple[Mapping[str, object], ...]]


@dataclass(frozen=True, slots=True)
class _RequiredWireAudit:
    counts: Mapping[str, int]
    by_kind_asset: Mapping[str, Mapping[str, int]]
    total: int
    samples: tuple[Mapping[str, object], ...]


@dataclass(slots=True)
class _StreamingNumericSummary:
    count: int = 0
    total: Decimal = Decimal(0)
    minimum: Decimal | None = None
    maximum: Decimal | None = None

    def observe(self, value: object) -> None:
        try:
            numeric = Decimal(str(value))
        except (ArithmeticError, TypeError, ValueError):
            return
        if not numeric.is_finite():
            return
        self.count += 1
        self.total += numeric
        self.minimum = numeric if self.minimum is None else min(self.minimum, numeric)
        self.maximum = numeric if self.maximum is None else max(self.maximum, numeric)

    def as_dict(self) -> dict[str, object]:
        return {
            "count": self.count,
            "min": None if self.minimum is None else float(self.minimum),
            "mean": None if self.count == 0 else float(self.total / self.count),
            "max": None if self.maximum is None else float(self.maximum),
        }


def _clock_forensic_value(value: object) -> object:
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Decimal):
        return float(value)
    return value


class _ClockForensicAccumulator:
    _OUTCOMES = ("valid", "rejected", "hard_invalid")

    def __init__(self) -> None:
        self.schema_versions: dict[int, int] = defaultdict(int)
        self.outcomes = {outcome: 0 for outcome in self._OUTCOMES}
        self.field_coverage = {
            outcome: {
                field: {"present": 0, "null": 0}
                for field in _CLOCK_FORENSIC_FIELDS
            }
            for outcome in self._OUTCOMES
        }
        self.timing_summaries = {
            outcome: {
                field: _StreamingNumericSummary()
                for field in _CLOCK_FORENSIC_TIMING_FIELDS
            }
            for outcome in self._OUTCOMES
        }
        self.counter_summaries = {
            outcome: {
                field: _StreamingNumericSummary()
                for field in _CLOCK_FORENSIC_COUNTER_FIELDS
            }
            for outcome in self._OUTCOMES
        }
        self.boolean_counts = {
            outcome: {
                field: {"true": 0, "false": 0, "null": 0}
                for field in _CLOCK_FORENSIC_BOOLEAN_FIELDS
            }
            for outcome in self._OUTCOMES
        }
        self.rejected_samples_seen = 0
        self.rejected_samples: list[
            tuple[tuple[str, str, str], Mapping[str, object]]
        ] = []
        self.consecutive_pairs_seen = 0
        self.consecutive_pairs: list[
            tuple[tuple[str, str, str], Mapping[str, object]]
        ] = []
        self.last_rejected_by_capture: dict[str, Mapping[str, object]] = {}

    @staticmethod
    def _sample_key(sample: Mapping[str, object]) -> tuple[str, str, str]:
        return (
            str(sample.get("capture_epoch_id") or ""),
            str(sample.get("request_sent_time") or ""),
            str(sample.get("observation_id") or ""),
        )

    @staticmethod
    def _retain(
        candidates: list[
            tuple[tuple[str, str, str], Mapping[str, object]]
        ],
        sample: Mapping[str, object],
        *,
        limit: int,
    ) -> None:
        candidates.append((_ClockForensicAccumulator._sample_key(sample), sample))
        candidates.sort(key=lambda candidate: candidate[0])
        if len(candidates) > limit:
            candidates.pop()

    @staticmethod
    def _sample(row: Mapping[str, object]) -> Mapping[str, object]:
        fields = (
            "schema_version",
            "observation_id",
            "capture_epoch_id",
            "connection_id",
            "connection_epoch",
            "request_sent_time",
            "response_received_time",
            "server_time",
            "received_time",
            "causal_valid_from",
            "causal_valid_until",
            "round_trip_latency_ms",
            "estimated_clock_drift_ms",
            "drift_uncertainty_ms",
            "sample_status",
            "invalid_reason",
            "sampling_interval_ms",
            "max_age_ms",
            "max_uncertainty_ms",
            *_CLOCK_FORENSIC_FIELDS,
        )
        return {
            field: _clock_forensic_value(row.get(field))
            for field in fields
        }

    def observe(
        self,
        row: Mapping[str, object],
        outcome: str,
    ) -> Mapping[str, object]:
        if outcome not in self.outcomes:
            raise ValueError(f"unsupported clock forensic outcome: {outcome}")
        version = int(str(row.get("schema_version", 0)))
        self.schema_versions[version] += 1
        self.outcomes[outcome] += 1
        for field in _CLOCK_FORENSIC_FIELDS:
            coverage = self.field_coverage[outcome][field]
            value = row.get(field)
            coverage["null" if value is None else "present"] += 1
        for field in _CLOCK_FORENSIC_TIMING_FIELDS:
            value = row.get(field)
            if value is not None:
                self.timing_summaries[outcome][field].observe(value)
        for field in _CLOCK_FORENSIC_COUNTER_FIELDS:
            value = row.get(field)
            if value is not None:
                self.counter_summaries[outcome][field].observe(value)
        for field in _CLOCK_FORENSIC_BOOLEAN_FIELDS:
            value = row.get(field)
            counts = self.boolean_counts[outcome][field]
            if value is True:
                counts["true"] += 1
            elif value is False:
                counts["false"] += 1
            else:
                counts["null"] += 1
        sample = self._sample(row)
        if outcome == "rejected":
            self.rejected_samples_seen += 1
            self._retain(
                self.rejected_samples,
                sample,
                limit=_CLOCK_FORENSIC_REJECTED_SAMPLE_LIMIT,
            )
        return sample

    def observe_rejection_streak(
        self,
        capture: str,
        streak: int,
        sample: Mapping[str, object],
    ) -> None:
        previous = self.last_rejected_by_capture.get(capture)
        if streak == MAX_CONSECUTIVE_REJECTED_CLOCK_PROBES + 1:
            self.consecutive_pairs_seen += 1
            pair: Mapping[str, object] = {
                "capture_epoch_id": capture,
                "rejections": [previous, sample],
            }
            self._retain(
                self.consecutive_pairs,
                sample={**sample, "pair": pair},
                limit=_CLOCK_FORENSIC_CONSECUTIVE_PAIR_LIMIT,
            )
        self.last_rejected_by_capture[capture] = sample

    def reset_rejection_streak(self, capture: str) -> None:
        self.last_rejected_by_capture.pop(capture, None)

    def as_dict(self) -> dict[str, object]:
        pair_payloads = [
            candidate["pair"]
            for _, candidate in self.consecutive_pairs
        ]
        return {
            "classification": "evidence_only_no_gate_override",
            "population": (
                "v2_plus_in_window_and_retained_boundary_rows_"
                "excluding_cadence_only_successors"
            ),
            "diagnostic_fields_projected": list(_CLOCK_FORENSIC_FIELDS),
            "schema_version_counts": {
                str(version): count
                for version, count in sorted(self.schema_versions.items())
            },
            "outcomes": dict(self.outcomes),
            "field_coverage_by_outcome": self.field_coverage,
            "timing_summaries_by_outcome": {
                outcome: {
                    field: summary.as_dict()
                    for field, summary in summaries.items()
                }
                for outcome, summaries in self.timing_summaries.items()
            },
            "counter_summaries_by_outcome": {
                outcome: {
                    field: summary.as_dict()
                    for field, summary in summaries.items()
                }
                for outcome, summaries in self.counter_summaries.items()
            },
            "boolean_counts_by_outcome": self.boolean_counts,
            "rejected_evidence": {
                "sample_limit": _CLOCK_FORENSIC_REJECTED_SAMPLE_LIMIT,
                "total": self.rejected_samples_seen,
                "retained": len(self.rejected_samples),
                "truncated": max(
                    self.rejected_samples_seen - len(self.rejected_samples),
                    0,
                ),
                "samples": [sample for _, sample in self.rejected_samples],
            },
            "consecutive_rejection_evidence": {
                "pair_limit": _CLOCK_FORENSIC_CONSECUTIVE_PAIR_LIMIT,
                "total": self.consecutive_pairs_seen,
                "retained": len(pair_payloads),
                "truncated": max(
                    self.consecutive_pairs_seen - len(pair_payloads),
                    0,
                ),
                "pairs": pair_payloads,
            },
        }


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("continuity timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{label} must be a timestamp")
    return _utc(value)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _seconds(delta: timedelta) -> float:
    return round(max(delta.total_seconds(), 0.0), 6)


def _merge(intervals: Iterable[Interval], *, preserve_tags: bool = True) -> tuple[Interval, ...]:
    ordered = sorted(
        intervals,
        key=(
            (lambda item: (item.tag, item.start, item.end))
            if preserve_tags
            else (lambda item: (item.start, item.end, item.tag))
        ),
    )
    merged: list[Interval] = []
    for item in ordered:
        if merged and (not preserve_tags or merged[-1].tag == item.tag) and item.start <= merged[-1].end:
            previous = merged[-1]
            merged[-1] = Interval(
                previous.start,
                max(previous.end, item.end),
                previous.tag if preserve_tags else "union",
            )
        else:
            merged.append(item if preserve_tags else Interval(item.start, item.end, "union"))
    return tuple(sorted(merged, key=lambda item: (item.start, item.end, item.tag)))


def _intersect(
    left: Iterable[Interval],
    right: Iterable[Interval],
    *,
    require_same_tag: bool = False,
) -> tuple[Interval, ...]:
    result: list[Interval] = []
    for first in left:
        for second in right:
            if require_same_tag and first.tag != second.tag:
                continue
            start = max(first.start, second.start)
            end = min(first.end, second.end)
            if start < end:
                tag = first.tag if require_same_tag else f"{first.tag}|{second.tag}"
                result.append(Interval(start, end, tag))
    return _merge(result)


def _subtract(intervals: Iterable[Interval], outages: Iterable[Interval]) -> tuple[Interval, ...]:
    result: list[Interval] = []
    by_tag: dict[str, list[Interval]] = defaultdict(list)
    for outage in outages:
        by_tag[outage.tag].append(outage)
    for item in intervals:
        fragments = [(item.start, item.end)]
        for outage in sorted(by_tag.get(item.tag, ()), key=lambda value: value.start):
            next_fragments: list[tuple[datetime, datetime]] = []
            for start, end in fragments:
                if outage.end <= start or outage.start >= end:
                    next_fragments.append((start, end))
                    continue
                if start < outage.start:
                    next_fragments.append((start, outage.start))
                if outage.end < end:
                    next_fragments.append((outage.end, end))
            fragments = next_fragments
        result.extend(Interval(start, end, item.tag) for start, end in fragments if start < end)
    return _merge(result)


def _clip(intervals: Iterable[Interval], start: datetime, end: datetime) -> tuple[Interval, ...]:
    result: list[Interval] = []
    for item in intervals:
        left = max(item.start, start)
        right = min(item.end, end)
        if left < right:
            result.append(Interval(left, right, item.tag))
    return _merge(result)


def _duration(intervals: Iterable[Interval]) -> timedelta:
    union = _merge(intervals, preserve_tags=False)
    return sum((item.end - item.start for item in union), start=timedelta())


def _covered_without_gaps(
    intervals: Iterable[Interval], start: datetime, end: datetime
) -> tuple[bool, int, timedelta]:
    if end <= start:
        return False, 0, timedelta()
    union = _clip(_merge(intervals, preserve_tags=False), start, end)
    cursor = start
    gaps = 0
    uncovered = timedelta()
    for item in union:
        if item.start > cursor:
            gaps += 1
            uncovered += item.start - cursor
        cursor = max(cursor, item.end)
    if cursor < end:
        gaps += 1
        uncovered += end - cursor
    return gaps == 0, gaps, uncovered


def _row_in_window(row: Mapping[str, object], start: datetime, end: datetime) -> bool:
    value = row.get("received_time")
    return isinstance(value, datetime) and start <= _utc(value) < end


def _schema_compatibility_family(record_type: RecordType, version: int) -> int:
    breaking_candidates = [
        candidate
        for (transition_type, _previous, candidate) in BREAKING_SCHEMA_TRANSITIONS
        if transition_type == record_type and candidate <= version
    ]
    return max(breaking_candidates, default=1)


def _normalized_integrity_order(
    row: Mapping[str, object],
    columns: Sequence[str],
) -> tuple[tuple[int, object], ...]:
    return tuple((1, "") if row.get(column) is None else (0, row[column]) for column in columns)


def _timestamp_iso_ns(epoch_ns: int) -> str:
    seconds, nanoseconds = divmod(epoch_ns, 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{nanoseconds:09d}Z"


def _funding_bucket(epoch_ns: int, interval_ns: int) -> int:
    tolerance_ns = min(
        _FUNDING_BUCKET_TOLERANCE_NS,
        max(interval_ns // 2 - 1, 0),
    )
    return (epoch_ns + tolerance_ns) // interval_ns


def _gap_is_relevant(
    key: PartitionKey,
    gap: Gap,
    assets: frozenset[str],
    start: datetime,
    end: datetime,
) -> bool:
    allowed_assets = {*assets, "GLOBAL", "CONNECTION"}
    first_date = start.date()
    last_date = (end - timedelta(microseconds=1)).date()
    key_date = key.date if isinstance(key.date, datetime) else datetime.fromisoformat(str(key.date)).date()
    if (
        key.venue not in {BINANCE, HYPERLIQUID}
        or key.record_type not in _REQUIRED_TYPES
        or key.asset not in allowed_assets
        or not first_date <= key_date <= last_date
        or gap.kind not in {"time", "funding_bucket"}
    ):
        return False
    try:
        left = _utc(datetime.fromisoformat(gap.start.replace("Z", "+00:00")))
        right = _utc(datetime.fromisoformat(gap.end.replace("Z", "+00:00")))
    except ValueError:
        return False
    return left < end and right > start


def _batch_timestamp_ns(batch: pa.RecordBatch, column: str) -> list[int]:
    index = batch.schema.get_field_index(column)
    if index < 0:
        raise ValueError(f"continuity integrity scan requires {column}")
    values = batch.column(index).cast(pa.int64()).to_pylist()
    if any(value is None for value in values):
        raise ValueError(f"continuity integrity scan found null {column}")
    return [int(value) for value in values]


_CLOCK_FORENSIC_TIMING_FIELDS = (
    "clock_schedule_overdue_ms",
    "single_flight_blocked_ms",
    "executor_submit_to_worker_start_ms",
    "worker_completion_to_supervisor_drain_ms",
    "transport_lock_wait_ms",
    "requests_adapter_header_elapsed_ms",
    "session_get_total_ms",
    "json_decode_ms",
    "diagnostic_prepare_ms",
    "diagnostic_finalize_ms",
    "request_boundary_monotonic_elapsed_ms",
    "request_boundary_thread_cpu_ms",
    "request_boundary_thread_runqueue_wait_ms",
    "urllib3_connection_observed_age_ms",
    "tls_socket_observed_age_ms",
)
_CLOCK_FORENSIC_COUNTER_FIELDS = (
    "request_boundary_thread_timeslice_delta",
    "requests_session_request_ordinal",
)
_CLOCK_FORENSIC_BOOLEAN_FIELDS = (
    "new_urllib3_connection_object_created",
    "requests_session_reused",
    "urllib3_connection_reused",
    "tls_socket_reused",
    "tls_session_reused",
    "post_request_observation_current",
)
_CLOCK_FORENSIC_CONTEXT_FIELDS = (
    "urllib3_connection_identity",
    "tls_socket_identity",
    "peer_ip",
    "peer_port",
    "socket_family",
    "response_cloudfront_pop",
    "response_cache",
)
_CLOCK_FORENSIC_FIELDS = (
    *_CLOCK_FORENSIC_TIMING_FIELDS,
    *_CLOCK_FORENSIC_COUNTER_FIELDS,
    *_CLOCK_FORENSIC_BOOLEAN_FIELDS,
    *_CLOCK_FORENSIC_CONTEXT_FIELDS,
)
_CLOCK_FORENSIC_REJECTED_SAMPLE_LIMIT = 64
_CLOCK_FORENSIC_CONSECUTIVE_PAIR_LIMIT = 32


_PROJECTED_ROW_COLUMNS = frozenset(
    {
        "schema_version",
        "record_type",
        "venue",
        "asset",
        "event_time",
        "exchange_time",
        "received_time",
        "source_sequence",
        "connection_id",
        "connection_epoch",
        "capture_epoch_id",
        "message_asset",
        "channel",
        "raw_message",
        "arrival_sequence",
        "snapshot_id",
        "book_epoch_id",
        "first_sequence",
        "last_sequence",
        "bid_level_count",
        "ask_level_count",
        "side",
        "level",
        "price",
        "quantity",
        "order_count",
        "bid_price",
        "bid_quantity",
        "ask_price",
        "ask_quantity",
        "update_id",
        "trade_id",
        "quote_quantity",
        "aggressor_side",
        "is_liquidation",
        "event_kind",
        "socket_role",
        "reason",
        "resync_snapshot_id",
        "observation_id",
        "request_sent_time",
        "response_received_time",
        "server_time",
        "round_trip_latency_ms",
        "estimated_clock_drift_ms",
        "drift_uncertainty_ms",
        "causal_valid_from",
        "causal_valid_until",
        "sample_status",
        "invalid_reason",
        "sampling_interval_ms",
        "max_age_ms",
        "max_uncertainty_ms",
        "payload_sha256",
        *_CLOCK_FORENSIC_FIELDS,
    }
)


@dataclass(frozen=True, slots=True)
class _BoundaryCandidate:
    map_key: tuple[str, RecordType, str]
    row: dict[str, object]
    manifest_order: int
    row_order: int
    columns: tuple[str, ...]


def _manifest_received_bounds(
    manifest: PartitionManifest,
) -> tuple[datetime | None, datetime | None]:
    bounds = manifest.timestamp_bounds.get("received_time", {})

    def parse(value: str | None) -> datetime | None:
        if value is None:
            return None
        return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))

    return parse(bounds.get("min")), parse(bounds.get("max"))


def _manifest_needed_for_scan(
    manifest: PartitionManifest,
    assets: frozenset[str],
    start: datetime,
    end: datetime,
) -> bool:
    key = manifest.partition
    record_type = RecordType(key.record_type)
    if key.venue not in {BINANCE, HYPERLIQUID} or record_type not in _REQUIRED_TYPES:
        return False
    if (
        record_type
        in {
            RecordType.BBO,
            RecordType.L2_BOOK_STATE,
            RecordType.L2_SNAPSHOT,
            RecordType.TRADE,
        }
        and key.asset not in assets
    ):
        return False
    minimum, maximum = _manifest_received_bounds(manifest)
    if record_type in {
        RecordType.BBO,
        RecordType.L2_BOOK_STATE,
        RecordType.L2_SNAPSHOT,
    }:
        return not ((maximum is not None and maximum < start) or (minimum is not None and minimum >= end))
    if record_type == RecordType.CONNECTION_EVENT:
        return minimum is None or minimum < end
    # Wire/trade need exact predecessor and successor rows. Clock cadence also
    # admits a request sent before end whose response arrives after end.
    return True


def _discover_partition_paths_bounded(
    root: Path,
    store: BoundedRowStore,
) -> Iterator[Path]:
    if not root.exists():
        store.metrics.manifest_files_discovered = 0
        return
    canonical_root = root.resolve()
    scratch = store.directory.resolve()
    try:
        scratch.relative_to(canonical_root)
    except ValueError:
        pass
    else:
        raise ValueError("continuity scratch directory must be outside the immutable lake")
    pending: list[tuple[str, str, str]] = []
    first_outside: str | None = None
    first_noncanonical_parquet: str | None = None
    first_noncanonical_manifest: str | None = None
    for artifact in root.rglob("*"):
        relative = artifact.relative_to(root).as_posix()
        try:
            artifact.resolve().relative_to(canonical_root)
        except ValueError:
            if first_outside is None or relative < first_outside:
                first_outside = relative
        name = artifact.name
        kind: str | None = None
        stem: str | None = None
        if name.endswith(".parquet"):
            if _CANONICAL_PARQUET.fullmatch(name) is None:
                if first_noncanonical_parquet is None or relative < first_noncanonical_parquet:
                    first_noncanonical_parquet = relative
                continue
            kind = "parquet"
            stem = name.removesuffix(".parquet")
        elif name.endswith(".manifest.json"):
            if _CANONICAL_MANIFEST.fullmatch(name) is None:
                if first_noncanonical_manifest is None or relative < first_noncanonical_manifest:
                    first_noncanonical_manifest = relative
                continue
            kind = "manifest"
            stem = name.removesuffix(".manifest.json")
        if kind is None or stem is None:
            continue
        parent = Path(relative).parent.as_posix()
        pending.append((relative, f"{parent}/{stem}", kind))
        if len(pending) >= 512:
            store.add_artifacts(pending)
            pending.clear()
    if pending:
        store.add_artifacts(pending)
    if first_outside is not None:
        raise PartitionValidationError(
            f"discovered artifact resolves outside data lake root: {first_outside}"
        )
    if first_noncanonical_parquet is not None:
        raise PartitionValidationError(f"non-canonical Parquet file: {first_noncanonical_parquet}")
    if first_noncanonical_manifest is not None:
        raise PartitionValidationError(f"non-canonical manifest file: {first_noncanonical_manifest}")
    orphan = store.orphan_parquet_path()
    if orphan is not None:
        raise PartitionValidationError(f"orphan Parquet without manifest: {orphan}")
    store.metrics.manifest_files_discovered = store.manifest_artifact_count()
    for relative in store.iter_manifest_relative_paths():
        yield root / Path(relative)


def _load_lake(
    root: Path,
    start: datetime,
    end: datetime,
    assets: Sequence[str],
) -> _LoadedLake:
    metrics = StreamingMetrics()
    canonical_root = root.resolve()
    configured_parent = configured_scratch_parent()
    if configured_parent is not None:
        try:
            configured_parent.relative_to(canonical_root)
        except ValueError:
            pass
        else:
            raise ValueError("HYPERLAB_CONTINUITY_SCRATCH must be outside the immutable lake")
    scratch_parent = canonical_root.parent if configured_parent is None else configured_parent
    store = BoundedRowStore(
        metrics,
        scratch_parent=scratch_parent,
    )
    predecessors: dict[tuple[object, ...], _BoundaryCandidate] = {}
    successors: dict[tuple[object, ...], _BoundaryCandidate] = {}
    clock_cadence_successors: dict[tuple[object, ...], _BoundaryCandidate] = {}
    wire_identities: set[tuple[str, str, int, str]] = set()
    relevant_gap_rows: SpilledSequence[tuple[str, Gap, bool]] = store.new_sequence()
    total_rows = 0
    selected_assets = frozenset(assets)

    def boundary_identity(
        venue: str,
        record_type: RecordType,
        asset: str,
        row: Mapping[str, object],
    ) -> tuple[object, ...] | None:
        connection = str(row.get("connection_id") or "")
        if record_type == RecordType.WIRE_MESSAGE:
            return (
                venue,
                record_type,
                connection,
                int(str(row.get("connection_epoch") or 0)),
                str(row.get("capture_epoch_id") or ""),
            )
        if record_type == RecordType.TRADE:
            return (
                venue,
                record_type,
                asset,
                connection,
                int(str(row.get("connection_epoch") or 0)),
            )
        if record_type == RecordType.CLOCK_SYNC:
            return (
                venue,
                record_type,
                str(row.get("capture_epoch_id") or ""),
                connection,
                int(str(row.get("connection_epoch") or 0)),
                str(row.get("observation_id") or ""),
                _timestamp(
                    row["received_time"],
                    label="clock boundary received_time",
                ),
            )
        if record_type == RecordType.CONNECTION_EVENT:
            return (
                venue,
                record_type,
                str(row.get("capture_epoch_id") or ""),
                asset,
                connection,
                str(row.get("event_kind") or ""),
            )
        return None

    def retain_boundary(
        candidates: dict[tuple[object, ...], _BoundaryCandidate],
        token: tuple[object, ...],
        map_key: tuple[str, RecordType, str],
        row: dict[str, object],
        manifest_order: int,
        row_order: int,
        columns: tuple[str, ...],
        *,
        keep_latest: bool,
    ) -> None:
        previous = candidates.get(token)
        if previous is None:
            candidates[token] = _BoundaryCandidate(
                map_key,
                row,
                manifest_order,
                row_order,
                columns,
            )
            return
        previous_time = _timestamp(
            previous.row["received_time"],
            label="boundary received_time",
        )
        current_time = _timestamp(row["received_time"], label="boundary received_time")
        if (keep_latest and current_time > previous_time) or (
            not keep_latest and current_time < previous_time
        ):
            candidates[token] = _BoundaryCandidate(
                map_key,
                row,
                manifest_order,
                row_order,
                columns,
            )

    validation_elapsed = 0.0
    scan_elapsed = 0.0
    try:
        paths = _discover_partition_paths_bounded(root, store)
        for manifest_order, path in enumerate(paths):
            validation_started = time.perf_counter()
            manifest = validate_partition(path)
            validation_elapsed += time.perf_counter() - validation_started
            expected_manifest_path = (canonical_root / manifest.relative_manifest_path).resolve()
            if path.resolve() != expected_manifest_path:
                raise PartitionValidationError(
                    f"manifest is outside canonical root layout: {path.relative_to(root).as_posix()}"
                )
            metrics.manifest_files_validated += 1
            metrics.max_file_rows = max(metrics.max_file_rows, manifest.row_count)
            metrics.max_file_size_bytes = max(
                metrics.max_file_size_bytes,
                manifest.size_bytes,
            )
            total_rows += manifest.row_count
            metrics.rows_validated += manifest.row_count
            key = manifest.partition
            record_type = RecordType(key.record_type)
            for gap in manifest.gaps:
                if _gap_is_relevant(key, gap, selected_assets, start, end):
                    relevant_gap_rows.append((key.relative_path.as_posix(), gap, False))

            spec = schema_for(record_type, manifest.schema_version)
            compatibility_family = _schema_compatibility_family(
                record_type,
                manifest.schema_version,
            )
            dataset_id = store.integrity_dataset_id(
                key.venue,
                key.asset,
                record_type.value,
                compatibility_family,
            )
            stream_id, cadence_consistent = store.integrity_stream_id(
                key.venue,
                key.asset,
                record_type.value,
                compatibility_family,
                manifest.stream_key,
                manifest.expected_interval_ns,
            )
            if not cadence_consistent:
                raise PartitionValidationError(
                    f"inconsistent gap cadence in stream: {key.venue}/{key.asset}/{record_type.value}"
                )

            semantic_selected = _manifest_needed_for_scan(
                manifest,
                selected_assets,
                start,
                end,
            )
            if semantic_selected:
                metrics.manifest_files_selected += 1
            else:
                metrics.manifest_files_pruned += 1

            metrics.parquet_file_scan_operations += 1
            metrics.unique_parquet_files_scanned += 1
            phase_schema_names = tuple(
                name for name in manifest.null_counts if name in _PROJECTED_ROW_COLUMNS
            )
            if semantic_selected and "received_time" not in phase_schema_names:
                raise ValueError("continuity projected scan requires received_time")

            integrity_columns = set(spec.primary_key)
            l2_definition = _L2_INTEGRITY_METADATA.get(record_type)
            if l2_definition is not None:
                integrity_columns.add(l2_definition[0])
                integrity_columns.update(l2_definition[1])
            cadence_selected = (
                manifest.expected_interval_ns is not None
                and key.venue in {BINANCE, HYPERLIQUID}
                and record_type in _REQUIRED_TYPES
                and key.asset in {*selected_assets, "GLOBAL", "CONNECTION"}
            )
            if cadence_selected:
                integrity_columns.update(spec.order_key)
                integrity_columns.add("funding_time" if record_type == RecordType.FUNDING else "event_time")
            selected_columns = integrity_columns | (set(phase_schema_names) if semantic_selected else set())
            scan_schema_names = tuple(name for name in manifest.null_counts if name in selected_columns)
            missing_columns = selected_columns - set(scan_schema_names)
            if missing_columns:
                raise ValueError(
                    "continuity integrity scan is missing columns: " + ",".join(sorted(missing_columns))
                )
            row_cursor = 0
            scan_started = time.perf_counter()
            for batch in iter_hashed_batches(
                root,
                manifest,
                columns=scan_schema_names,
            ):
                batch_rows = batch.to_pylist()
                metrics.observe_batch(
                    record_type,
                    len(batch_rows),
                    semantic_scan=semantic_selected,
                )
                primary_keys = [tuple(row.get(name) for name in spec.primary_key) for row in batch_rows]
                if not store.add_integrity_primary_keys(dataset_id, primary_keys):
                    raise PartitionValidationError("duplicate primary keys: 1")
                if l2_definition is not None:
                    identifier_name, metadata_names, label = l2_definition
                    metadata_by_identifier: dict[str, tuple[object, ...]] = {}
                    for row in batch_rows:
                        identifier = str(row[identifier_name])
                        metadata = tuple(row.get(name) for name in metadata_names)
                        previous_metadata = metadata_by_identifier.setdefault(
                            identifier,
                            metadata,
                        )
                        if previous_metadata != metadata:
                            raise PartitionValidationError(
                                f"inconsistent L2 {label} metadata for "
                                f"{identifier_name} {identifier!r} across partitions"
                            )
                    conflict = store.add_integrity_l2_metadata(
                        dataset_id,
                        metadata_by_identifier,
                    )
                    if conflict is not None:
                        raise PartitionValidationError(
                            f"inconsistent L2 {label} metadata for "
                            f"{identifier_name} {conflict!r} across partitions"
                        )
                if cadence_selected:
                    timestamp_name = "funding_time" if record_type == RecordType.FUNDING else "event_time"
                    cadence_values = _batch_timestamp_ns(batch, timestamp_name)
                    store.add_integrity_cadence_rows(
                        stream_id,
                        [
                            (
                                _normalized_integrity_order(row, spec.order_key),
                                manifest_order,
                                row_cursor + offset,
                                cadence_values[offset],
                                manifest.data_file,
                                key.relative_path.as_posix(),
                            )
                            for offset, row in enumerate(batch_rows)
                        ],
                    )
                if not semantic_selected:
                    row_cursor += len(batch_rows)
                    del batch_rows
                    continue
                in_window_rows: list[dict[str, object]] = []
                in_window_orders: list[int] = []
                for offset, raw_row in enumerate(batch_rows):
                    row_order = row_cursor + offset
                    row = {name: raw_row.get(name) for name in phase_schema_names}
                    received = _timestamp(row["received_time"], label="received_time")
                    venue = key.venue
                    asset = key.asset
                    map_key = (venue, record_type, asset)
                    if (
                        record_type == RecordType.WIRE_MESSAGE
                        and int(str(row.get("schema_version", 0))) >= 2
                        and received < end
                        and isinstance(row.get("connection_id"), str)
                        and row.get("connection_epoch") is not None
                        and isinstance(row.get("capture_epoch_id"), str)
                        and str(row["capture_epoch_id"])
                    ):
                        wire_identities.add(
                            (
                                venue,
                                str(row["connection_id"]),
                                int(str(row["connection_epoch"])),
                                str(row["capture_epoch_id"]),
                            )
                        )
                    include = _row_in_window(row, start, end)
                    if record_type == RecordType.CLOCK_SYNC and manifest.schema_version >= 2:
                        valid_from = row.get("causal_valid_from")
                        valid_until = row.get("causal_valid_until")
                        include = include or (
                            isinstance(valid_from, datetime)
                            and isinstance(valid_until, datetime)
                            and _utc(valid_from) < end
                            and _utc(valid_until) > start
                        )
                    if include:
                        in_window_rows.append(row)
                        in_window_orders.append(row_order)
                        continue
                    token = boundary_identity(venue, record_type, asset, row)
                    if token is None:
                        continue
                    if received < start:
                        if record_type == RecordType.CLOCK_SYNC:
                            if row.get("sample_status") == "valid":
                                continue
                            if received < start - timedelta(milliseconds=MAX_CLOCK_AGE_MS):
                                continue
                        retain_boundary(
                            predecessors,
                            ("predecessor", *token),
                            map_key,
                            row,
                            manifest_order,
                            row_order,
                            phase_schema_names,
                            keep_latest=True,
                        )
                    elif received >= end and record_type == RecordType.CLOCK_SYNC:
                        request_sent = _timestamp(
                            row["request_sent_time"],
                            label="clock boundary request_sent_time",
                        )
                        if request_sent < end:
                            retain_boundary(
                                clock_cadence_successors,
                                (
                                    "clock-cadence-successor",
                                    venue,
                                    record_type,
                                    str(row.get("capture_epoch_id") or ""),
                                    str(row.get("connection_id") or ""),
                                    int(str(row.get("connection_epoch") or 0)),
                                ),
                                map_key,
                                row,
                                manifest_order,
                                row_order,
                                phase_schema_names,
                                keep_latest=False,
                            )
                    elif record_type in {
                        RecordType.WIRE_MESSAGE,
                        RecordType.TRADE,
                    }:
                        retain_boundary(
                            successors,
                            ("successor", *token),
                            map_key,
                            row,
                            manifest_order,
                            row_order,
                            phase_schema_names,
                            keep_latest=False,
                        )
                if in_window_rows:
                    store.insert_batch(
                        venue=key.venue,
                        record_type=record_type,
                        asset=key.asset,
                        population="in",
                        manifest_order=manifest_order,
                        first_row_order=in_window_orders[0],
                        columns=phase_schema_names,
                        rows=in_window_rows,
                        row_orders=in_window_orders,
                    )
                row_cursor += len(batch_rows)
                del batch_rows
            scan_elapsed += time.perf_counter() - scan_started

        integrity_started = time.perf_counter()
        previous_stream_id: int | None = None
        previous_cadence: tuple[int, int, str] | None = None
        for (
            stream_id,
            cadence_record_type,
            _cadence_asset,
            expected_interval_ns,
            cadence_ns,
            manifest_id,
            partition_path,
        ) in store.iter_integrity_cadence_rows():
            if stream_id != previous_stream_id:
                previous_stream_id = stream_id
                previous_cadence = None
            is_funding = cadence_record_type == RecordType.FUNDING.value
            current_value = _funding_bucket(cadence_ns, expected_interval_ns) if is_funding else cadence_ns
            cadence_step = 1 if is_funding else expected_interval_ns
            if previous_cadence is not None:
                previous_value, previous_time_ns, previous_manifest = previous_cadence
                difference = current_value - previous_value
                if difference > cadence_step and previous_manifest != manifest_id:
                    gap = Gap(
                        kind="funding_bucket" if is_funding else "time",
                        start=_timestamp_iso_ns(previous_time_ns),
                        end=_timestamp_iso_ns(cadence_ns),
                        missing_count=(
                            difference - 1 if is_funding else max(difference // cadence_step - 1, 1)
                        ),
                    )
                    partition_key = PartitionKey.from_leaf(Path(partition_path))
                    if _gap_is_relevant(
                        partition_key,
                        gap,
                        selected_assets,
                        start,
                        end,
                    ):
                        relevant_gap_rows.append((partition_path, gap, True))
            previous_cadence = (current_value, cadence_ns, manifest_id)
        metrics.observe_phase(
            "cross_segment_integrity",
            time.perf_counter() - integrity_started,
        )
        metrics.max_boundary_candidates = (
            len(predecessors)
            + len(successors)
            + len(clock_cadence_successors)
        )
        metrics.wire_identity_keys = len(wire_identities)

        for candidate in (*predecessors.values(), *successors.values()):
            store.insert_batch(
                venue=candidate.map_key[0],
                record_type=candidate.map_key[1],
                asset=candidate.map_key[2],
                population="boundary",
                manifest_order=candidate.manifest_order,
                first_row_order=candidate.row_order,
                columns=candidate.columns,
                rows=(candidate.row,),
            )
        for candidate in clock_cadence_successors.values():
            store.insert_batch(
                venue=candidate.map_key[0],
                record_type=candidate.map_key[1],
                asset=candidate.map_key[2],
                population="cadence",
                manifest_order=candidate.manifest_order,
                first_row_order=candidate.row_order,
                columns=candidate.columns,
                rows=(candidate.row,),
            )
        index_started = time.perf_counter()
        store.finalize_indexes()
        metrics.observe_phase(
            "scratch_index_build",
            time.perf_counter() - index_started,
        )
    except BaseException:
        store.close()
        raise
    metrics.observe_phase("manifest_validation", validation_elapsed)
    metrics.observe_phase("projected_row_scan_and_spool", scan_elapsed)
    inventory = InventoryReport(
        partitions=(),
        total_rows=total_rows,
        venues=(),
        assets=(),
        record_types=(),
        dates=(),
        delisted_assets=(),
        cross_segment_gaps=(),
    )
    return _LoadedLake(
        inventory=inventory,
        inventory_partition_count=metrics.manifest_files_validated,
        store=store,
        wire_identities=frozenset(wire_identities),
        relevant_gaps=relevant_gap_rows,
        metrics=metrics,
    )


def _all_rows(
    loaded: _LoadedLake,
    venue: str,
    record_type: RecordType,
    asset: str | None = None,
    *,
    filters: Mapping[str, object] | None = None,
) -> Iterator[dict[str, object]]:
    return loaded.store.iter_rows(
        venue,
        record_type,
        asset,
        filters=filters,
    )


def _rows_with_boundaries(
    loaded: _LoadedLake,
    venue: str,
    record_type: RecordType,
    asset: str | None = None,
) -> Iterator[dict[str, object]]:
    return loaded.store.iter_rows(
        venue,
        record_type,
        asset,
        with_boundaries=True,
    )


def _connection_lineage(
    loaded: _LoadedLake,
    venue: str,
    required_roles: frozenset[str],
    start: datetime,
    end: datetime,
) -> _ConnectionLineage:
    candidates: dict[
        tuple[str, int, str],
        list[tuple[str, datetime]],
    ] = defaultdict(list)
    observed_captures: set[str] = set()
    unbound_connect_events = 0
    for row in _rows_with_boundaries(
        loaded,
        venue,
        RecordType.CONNECTION_EVENT,
    ):
        if int(str(row.get("schema_version", 0))) < 2 or row.get("event_kind") != "connect":
            continue
        received = _timestamp(row["received_time"], label="connect received_time")
        in_window = start <= received < end
        connection = row.get("connection_id")
        epoch = row.get("connection_epoch")
        capture = row.get("capture_epoch_id")
        role = row.get("socket_role")
        if in_window and isinstance(capture, str) and capture:
            observed_captures.add(capture)
        if (
            not isinstance(connection, str)
            or not connection
            or epoch is None
            or not isinstance(capture, str)
            or not capture
            or not isinstance(role, str)
            or not role
        ):
            if in_window:
                unbound_connect_events += 1
            continue
        candidates[(connection, int(str(epoch)), capture)].append(
            (
                role,
                received,
            )
        )

    identities: dict[tuple[str, int, str], tuple[str, datetime]] = {}
    rejected = 0
    rejected_captures: set[str] = set()
    for identity, events in candidates.items():
        if len(events) != 1 or events[0][0] not in required_roles:
            rejected += 1
            rejected_captures.add(identity[2])
            continue
        identities[identity] = events[0]

    by_capture_role: dict[str, dict[str, list[tuple[str, int, str]]]] = defaultdict(lambda: defaultdict(list))
    for identity, (role, _) in identities.items():
        by_capture_role[identity[2]][role].append(identity)
    eligible = frozenset(
        capture
        for capture, by_role in by_capture_role.items()
        if all(len(by_role.get(role, ())) == 1 for role in required_roles)
        and set(by_role) == set(required_roles)
        and (
            venue != BINANCE
            or len(
                {identity[1] for identities_for_role in by_role.values() for identity in identities_for_role}
            )
            == 1
        )
        and capture not in rejected_captures
    )
    return _ConnectionLineage(
        identities,
        frozenset(observed_captures),
        eligible,
        rejected,
        frozenset(rejected_captures),
        unbound_connect_events,
    )


def _wire_role_matches(
    row: Mapping[str, object],
    lineage: _ConnectionLineage,
    expected_role: str,
) -> bool:
    connection = row.get("connection_id")
    epoch = row.get("connection_epoch")
    capture = row.get("capture_epoch_id")
    if (
        not isinstance(connection, str)
        or not connection
        or epoch is None
        or not isinstance(capture, str)
        or not capture
        or capture not in lineage.eligible_captures
    ):
        return False
    match = lineage.identities.get((connection, int(str(epoch)), capture))
    if match is None or match[0] != expected_role:
        return False
    return match[1] <= _timestamp(row["received_time"], label="lineage received_time")


def _wire_kind(channel: object, raw_message: object) -> str | None:
    normalized = "" if channel is None else str(channel).lower()
    if normalized == "bbo":
        return "bbo"
    if normalized == "l2book":
        return "l2"
    if normalized == "trades":
        return "trade"
    if normalized.endswith("@aggtrade"):
        return "trade"
    if normalized.endswith("@bookticker"):
        return "bbo"
    if "@depth20" in normalized:
        return "l2"
    if isinstance(raw_message, str) and '"e":"aggTrade"' in raw_message:
        return "trade"
    return None


def _payload_wire_kind(venue: str, raw_message: object) -> str | None:
    if not isinstance(raw_message, str):
        return None
    try:
        root = json.loads(raw_message)
    except json.JSONDecodeError:
        return None
    if not isinstance(root, Mapping):
        return None
    if venue == BINANCE:
        data = root.get("data")
        if not isinstance(data, Mapping):
            return None
        return {
            "bookTicker": "bbo",
            "depthUpdate": "l2",
            "aggTrade": "trade",
        }.get(str(data.get("e") or ""))
    return {
        "bbo": "bbo",
        "l2Book": "l2",
        "trades": "trade",
    }.get(str(root.get("channel") or ""))


def _raw_required_kind(venue: str, raw: Mapping[str, object]) -> str | None:
    persisted_kind = _wire_kind(raw.get("channel"), raw.get("raw_message"))
    payload_kind = _payload_wire_kind(venue, raw.get("raw_message"))
    if payload_kind is not None:
        return payload_kind
    return persisted_kind


def _raw_payload_assets(
    venue: str,
    raw: Mapping[str, object],
) -> frozenset[str]:
    raw_message = raw.get("raw_message")
    if not isinstance(raw_message, str):
        return frozenset()
    try:
        root = json.loads(raw_message)
    except json.JSONDecodeError:
        return frozenset()
    if not isinstance(root, Mapping):
        return frozenset()
    if venue == BINANCE:
        data = root.get("data")
        if not isinstance(data, Mapping):
            return frozenset()
        symbol = str(data.get("s") or "").upper()
        return (
            frozenset({symbol.removesuffix("USDT")})
            if symbol.endswith("USDT") and len(symbol) > 4
            else frozenset()
        )
    data = root.get("data")
    if isinstance(data, Mapping):
        coin = data.get("coin")
        return frozenset({str(coin).upper()}) if isinstance(coin, str) and coin else frozenset()
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        return frozenset(
            str(item["coin"]).upper()
            for item in data
            if isinstance(item, Mapping) and isinstance(item.get("coin"), str) and str(item["coin"])
        )
    return frozenset()


def _raw_primary_payload_asset(
    venue: str,
    raw: Mapping[str, object],
) -> str | None:
    """Mirror producer message_asset selection while retaining multi-asset payloads."""

    raw_message = raw.get("raw_message")
    if not isinstance(raw_message, str):
        return None
    try:
        root = json.loads(raw_message)
    except json.JSONDecodeError:
        return None
    if not isinstance(root, Mapping):
        return None
    data = root.get("data")
    if venue == BINANCE and isinstance(data, Mapping):
        symbol = str(data.get("s") or "").upper()
        return symbol.removesuffix("USDT") if symbol.endswith("USDT") and len(symbol) > 4 else None
    if isinstance(data, Mapping):
        coin = data.get("coin")
        return str(coin).upper() if isinstance(coin, str) and coin else None
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        first = data[0] if data else None
        if isinstance(first, Mapping):
            coin = first.get("coin")
            return str(coin).upper() if isinstance(coin, str) and coin else None
    return None


def _binance_wire_by_sequence(
    loaded: _LoadedLake,
    connection_id: str,
    connection_epoch: int,
    arrival_sequence: int,
) -> Mapping[str, object] | None:
    rows = list(
        _all_rows(
            loaded,
            BINANCE,
            RecordType.WIRE_MESSAGE,
            filters={
                "connection_id": connection_id,
                "connection_epoch": connection_epoch,
                "arrival_sequence": arrival_sequence,
            },
        )
    )
    return rows[-1] if rows else None


def _binance_wire_frame_candidates(
    loaded: _LoadedLake,
    connection_id: str,
    received: datetime,
    asset: str,
    kind: str,
) -> tuple[dict[str, object], ...]:
    candidates = (
        row
        for row in _all_rows(
            loaded,
            BINANCE,
            RecordType.WIRE_MESSAGE,
            filters={
                "connection_id": connection_id,
                "received_time": received,
                "message_asset": asset,
            },
        )
        if kind
        in (
            ("l2", "bbo")
            if _wire_kind(row.get("channel"), row.get("raw_message")) == "l2"
            else (_wire_kind(row.get("channel"), row.get("raw_message")),)
        )
    )
    return tuple(
        sorted(
            candidates,
            key=lambda row: (
                int(str(row["connection_epoch"])),
                int(str(row["arrival_sequence"])),
                str(row["capture_epoch_id"]),
            ),
        )
    )


def _decoded_wire_data(raw: Mapping[str, object]) -> Mapping[str, object] | None:
    raw_message = raw.get("raw_message")
    if not isinstance(raw_message, str):
        return None
    try:
        root = json.loads(raw_message)
    except json.JSONDecodeError:
        return None
    if not isinstance(root, Mapping):
        return None
    persisted_channel = raw.get("channel")
    if (
        not isinstance(persisted_channel, str)
        or not persisted_channel
        or root.get("stream") != persisted_channel
    ):
        return None
    data = root.get("data")
    if not isinstance(data, Mapping):
        return None
    declared_versions = tuple(mapping["st"] for mapping in (root, data) if "st" in mapping)
    if any(
        not (
            (isinstance(declared, int) and not isinstance(declared, bool) and declared == 1)
            or (isinstance(declared, str) and declared == "1")
        )
        for declared in declared_versions
    ):
        return None
    return data


def _normalized_wire_identity_matches(
    normalized: Mapping[str, object],
    raw: Mapping[str, object],
) -> bool:
    """Require every persisted physical-lineage field to match one raw frame."""

    return all(normalized.get(field) == raw.get(field) for field in ("received_time", "connection_id"))


def _binance_market_matches_wire(
    normalized: Mapping[str, object],
    raw: Mapping[str, object],
    kind: str,
) -> bool:
    if not _normalized_wire_identity_matches(normalized, raw):
        return False
    data = _decoded_wire_data(raw)
    if data is None:
        return False
    try:
        symbol = str(data["s"]).upper()
        normalized_asset = normalized.get("asset")
        channel = str(raw["channel"])
        event = str(data.get("e") or "")
        if kind == "bbo":
            channel_matches = (event == "bookTicker" and channel.endswith("@bookTicker")) or (
                event == "depthUpdate" and "@depth20" in channel
            )
        elif kind == "l2":
            channel_matches = event == "depthUpdate" and "@depth20" in channel
        else:
            return False
        if (
            not isinstance(normalized_asset, str)
            or symbol != f"{normalized_asset.upper()}USDT"
            or not channel_matches
            or channel.partition("@")[0].upper() != symbol
        ):
            return False
        event_time = datetime.fromtimestamp(
            int(str(data.get("T", data["E"]))) / 1_000,
            tz=UTC,
        )
        exchange_time = datetime.fromtimestamp(int(str(data["E"])) / 1_000, tz=UTC)
        if (
            raw.get("message_asset") != normalized.get("asset")
            or normalized.get("event_time") != event_time
            or normalized.get("exchange_time") != exchange_time
        ):
            return False
        if kind == "bbo":
            update = int(str(data["u"]))
            if event == "bookTicker":
                bid_price = Decimal(str(data["b"]))
                bid_quantity = Decimal(str(data["B"]))
                ask_price = Decimal(str(data["a"]))
                ask_quantity = Decimal(str(data["A"]))
            else:
                bids = data["b"]
                asks = data["a"]
                if (
                    not isinstance(bids, Sequence)
                    or isinstance(bids, (str, bytes, bytearray))
                    or not bids
                    or not isinstance(asks, Sequence)
                    or isinstance(asks, (str, bytes, bytearray))
                    or not asks
                ):
                    return False
                bid = bids[0]
                ask = asks[0]
                if (
                    not isinstance(bid, Sequence)
                    or isinstance(bid, (str, bytes, bytearray))
                    or len(bid) != 2
                    or not isinstance(ask, Sequence)
                    or isinstance(ask, (str, bytes, bytearray))
                    or len(ask) != 2
                ):
                    return False
                bid_price = Decimal(str(bid[0]))
                bid_quantity = Decimal(str(bid[1]))
                ask_price = Decimal(str(ask[0]))
                ask_quantity = Decimal(str(ask[1]))
            return (
                normalized.get("source_sequence") == update
                and normalized.get("update_id") == f"{symbol}:{update}"
                and Decimal(str(normalized.get("bid_price"))) == bid_price
                and Decimal(str(normalized.get("bid_quantity"))) == bid_quantity
                and Decimal(str(normalized.get("ask_price"))) == ask_price
                and Decimal(str(normalized.get("ask_quantity"))) == ask_quantity
            )
        if kind == "l2":
            bids = data["b"]
            asks = data["a"]
            if (
                not isinstance(bids, Sequence)
                or isinstance(bids, (str, bytes, bytearray))
                or not isinstance(asks, Sequence)
                or isinstance(asks, (str, bytes, bytearray))
            ):
                return False
            connection = str(raw["connection_id"])
            epoch = int(str(raw["connection_epoch"]))
            arrival = int(str(raw["arrival_sequence"]))
            last_sequence = int(str(data["u"]))
            return (
                normalized.get("source_sequence") is None
                and normalized.get("snapshot_id")
                == f"ws:{connection}:{epoch}:{arrival}:{symbol}:{last_sequence}"
                and normalized.get("book_epoch_id") == f"{connection}:{epoch}"
                and normalized.get("bid_level_count") == len(bids)
                and normalized.get("ask_level_count") == len(asks)
            )
    except (KeyError, TypeError, ValueError):
        return False
    return False


def _persisted_l2_levels_match_raw(
    loaded: _LoadedLake,
    venue: str,
    asset: str,
    header: Mapping[str, object],
    raw: Mapping[str, object],
) -> bool:
    snapshot = header.get("snapshot_id")
    book_epoch = header.get("book_epoch_id")
    if not isinstance(snapshot, str) or not isinstance(book_epoch, str):
        return False
    candidates = [
        row
        for row in _all_rows(
            loaded,
            venue,
            RecordType.L2_SNAPSHOT,
            asset,
            filters={"snapshot_id": snapshot},
        )
    ]
    raw_message = raw.get("raw_message")
    if not isinstance(raw_message, str):
        return False
    try:
        root = json.loads(raw_message)
        if not isinstance(root, Mapping):
            return False
        data = root["data"]
        if venue == BINANCE:
            if not isinstance(data, Mapping):
                return False
            raw_sides: Sequence[object] = (data["b"], data["a"])
            last_sequence: int | None = int(str(data["u"]))
            order_count_required = False
        else:
            if not isinstance(data, Mapping):
                return False
            levels = data["levels"]
            if (
                not isinstance(levels, Sequence)
                or isinstance(levels, (str, bytes, bytearray))
                or len(levels) != 2
            ):
                return False
            raw_sides = levels
            last_sequence = None
            order_count_required = True
        expected: list[tuple[str, int, Decimal, Decimal, int | None]] = []
        for side, raw_levels in zip(("bid", "ask"), raw_sides, strict=True):
            if not isinstance(raw_levels, Sequence) or isinstance(raw_levels, (str, bytes, bytearray)):
                return False
            for level_number, raw_level in enumerate(raw_levels):
                if venue == BINANCE:
                    if (
                        not isinstance(raw_level, Sequence)
                        or isinstance(raw_level, (str, bytes, bytearray))
                        or len(raw_level) != 2
                    ):
                        return False
                    price = Decimal(str(raw_level[0]))
                    quantity = Decimal(str(raw_level[1]))
                    order_count = None
                else:
                    if not isinstance(raw_level, Mapping):
                        return False
                    price = Decimal(str(raw_level["px"]))
                    quantity = Decimal(str(raw_level["sz"]))
                    order_count = int(str(raw_level["n"]))
                expected.append((side, level_number, price, quantity, order_count))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False
    if len(candidates) != len(expected):
        return False
    normalized: list[tuple[str, int, Decimal, Decimal, int | None]] = []
    for row in candidates:
        if (
            row.get("book_epoch_id") != book_epoch
            or row.get("received_time") != header.get("received_time")
            or row.get("event_time") != header.get("event_time")
            or row.get("exchange_time") != header.get("exchange_time")
            or row.get("connection_id") != header.get("connection_id")
            or row.get("last_sequence") != last_sequence
        ):
            return False
        raw_order_count = row.get("order_count")
        if order_count_required and raw_order_count is None:
            return False
        if not order_count_required and raw_order_count is not None:
            return False
        try:
            normalized.append(
                (
                    str(row["side"]),
                    int(str(row["level"])),
                    Decimal(str(row["price"])),
                    Decimal(str(row["quantity"])),
                    None if raw_order_count is None else int(str(raw_order_count)),
                )
            )
        except (KeyError, TypeError, ValueError):
            return False
    expected.sort()
    normalized.sort()
    if normalized != expected:
        return False
    bid_count = sum(side == "bid" for side, *_ in expected)
    ask_count = sum(side == "ask" for side, *_ in expected)
    return header.get("bid_level_count") == bid_count and header.get("ask_level_count") == ask_count


def _normalized_bbo_is_executable(normalized: Mapping[str, object]) -> bool:
    try:
        values = tuple(
            Decimal(str(normalized.get(field)))
            for field in (
                "bid_price",
                "bid_quantity",
                "ask_price",
                "ask_quantity",
            )
        )
    except (TypeError, ValueError, ArithmeticError):
        return False
    return all(value.is_finite() and value > 0 for value in values)


def _normalized_l2_is_executable(
    loaded: _LoadedLake,
    venue: str,
    asset: str,
    header: Mapping[str, object],
) -> bool:
    snapshot = header.get("snapshot_id")
    if not isinstance(snapshot, str) or not snapshot:
        return False
    try:
        bid_count = int(str(header["bid_level_count"]))
        ask_count = int(str(header["ask_level_count"]))
    except (KeyError, TypeError, ValueError):
        return False
    if bid_count <= 0 or ask_count <= 0:
        return False
    levels = list(
        _all_rows(
            loaded,
            venue,
            RecordType.L2_SNAPSHOT,
            asset,
            filters={"snapshot_id": snapshot},
        )
    )
    if len(levels) != bid_count + ask_count:
        return False
    observed_counts = {"bid": 0, "ask": 0}
    for row in levels:
        side = row.get("side")
        if side not in observed_counts:
            return False
        try:
            price = Decimal(str(row["price"]))
            quantity = Decimal(str(row["quantity"]))
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return False
        if (
            not price.is_finite()
            or not quantity.is_finite()
            or price <= 0
            or quantity <= 0
        ):
            return False
        observed_counts[str(side)] += 1
    return observed_counts == {"bid": bid_count, "ask": ask_count}


def _binance_trade_matches_wire(
    normalized: Mapping[str, object],
    raw: Mapping[str, object],
) -> bool:
    if not _normalized_wire_identity_matches(normalized, raw):
        return False
    data = _decoded_wire_data(raw)
    if data is None:
        return False
    try:
        aggregate_id = int(str(data["a"]))
        event_time = datetime.fromtimestamp(int(str(data["T"])) / 1_000, tz=UTC)
        exchange_time = datetime.fromtimestamp(int(str(data["E"])) / 1_000, tz=UTC)
        symbol = str(data["s"]).upper()
        normalized_asset = normalized.get("asset")
        price = Decimal(str(data["p"]))
        quantity = Decimal(str(data["q"]))
        maker = data["m"]
        if not isinstance(maker, bool):
            return False
        aggressor = "sell" if maker else "buy"
        channel = str(raw["channel"])
        if (
            data.get("e") != "aggTrade"
            or not isinstance(normalized_asset, str)
            or symbol != f"{normalized_asset.upper()}USDT"
            or not channel.endswith("@aggTrade")
            or channel.partition("@")[0].upper() != symbol
        ):
            return False
    except (KeyError, TypeError, ValueError):
        return False
    return (
        normalized.get("source_sequence") == aggregate_id
        and normalized.get("event_time") == event_time
        and normalized.get("exchange_time") == exchange_time
        and normalized.get("trade_id") == f"{symbol}:agg:{aggregate_id}"
        and Decimal(str(normalized.get("price"))) == price
        and Decimal(str(normalized.get("quantity"))) == quantity
        and Decimal(str(normalized.get("quote_quantity"))) == price * quantity
        and normalized.get("is_liquidation") is None
        and normalized.get("aggressor_side") == aggressor
        and raw.get("message_asset") == normalized.get("asset")
    )


def _binance_normalized_observations(
    loaded: _LoadedLake,
    assets: Sequence[str],
    lineage: _ConnectionLineage,
) -> tuple[
    dict[str, dict[str, dict[str, SpilledTimestampSeries]]],
    dict[str, dict[str, int]],
    tuple[tuple[str, str], ...],
    int,
]:
    observations: dict[
        str,
        dict[str, dict[str, SpilledTimestampSeries]],
    ] = {}

    def append_observation(
        capture: str,
        asset: str,
        kind: str,
        received: datetime,
    ) -> None:
        kinds = observations.setdefault(capture, {}).setdefault(asset, {})
        series = kinds.get(kind)
        if series is None:
            series = loaded.store.new_timestamp_series()
            kinds[kind] = series
        series.append(received)

    counts = {
        asset: {
            "normalized_count": 0,
            "normalized_with_raw_lineage_count": 0,
            "raw_agg_trade_count": 0,
            "raw_agg_trade_with_role_lineage_count": 0,
        }
        for asset in assets
    }
    resync_starts: dict[
        tuple[str, str, str, int],
        list[tuple[datetime, str]],
    ] = defaultdict(list)
    resync_completions: dict[
        tuple[str, str, str, int],
        list[tuple[datetime, str, str]],
    ] = defaultdict(list)
    for event in _rows_with_boundaries(
        loaded,
        BINANCE,
        RecordType.CONNECTION_EVENT,
    ):
        if int(str(event.get("schema_version", 0))) < 2:
            continue
        capture = event.get("capture_epoch_id")
        event_asset = event.get("asset")
        connection = event.get("connection_id")
        connection_epoch = event.get("connection_epoch")
        book_epoch = event.get("book_epoch_id")
        received = _timestamp(
            event["received_time"],
            label="resync-event received_time",
        )
        if (
            isinstance(capture, str)
            and capture
            and isinstance(event_asset, str)
            and event_asset
            and isinstance(connection, str)
            and connection
            and connection_epoch is not None
            and isinstance(book_epoch, str)
            and book_epoch
            and event.get("socket_role") == "public"
            and isinstance(event.get("channel"), str)
            and "@depth20" in str(event["channel"])
            and str(event["channel"]).partition("@")[0].upper() == f"{event_asset}USDT"
            and lineage.identities.get((connection, int(str(connection_epoch)), capture)) is not None
            and lineage.identities[(connection, int(str(connection_epoch)), capture)][0] == "public"
            and lineage.identities[(connection, int(str(connection_epoch)), capture)][1] <= received
        ):
            key = (
                capture,
                event_asset,
                connection,
                int(str(connection_epoch)),
            )
            if event.get("event_kind") == "resync_start":
                resync_starts[key].append((received, book_epoch))
            elif (
                event.get("event_kind") == "resync_complete"
                and isinstance(event.get("resync_snapshot_id"), str)
                and str(event["resync_snapshot_id"])
            ):
                resync_completions[key].append(
                    (
                        received,
                        book_epoch,
                        str(event["resync_snapshot_id"]),
                    )
                )
    observed_l2: set[tuple[str, str]] = set()
    valid_l2: set[tuple[str, str]] = set()
    exact_l2_rows: SpilledSequence[tuple[str, str, str, int, str, str, datetime, int]] = (
        loaded.store.new_sequence()
    )
    lineage_rejections = 0
    for row in _all_rows(loaded, BINANCE, RecordType.WIRE_MESSAGE):
        asset = row.get("message_asset")
        if asset in counts and _wire_kind(row.get("channel"), row.get("raw_message")) == "trade":
            counts[str(asset)]["raw_agg_trade_count"] += 1
            if _wire_role_matches(row, lineage, "market"):
                counts[str(asset)]["raw_agg_trade_with_role_lineage_count"] += 1

    type_to_kind = {
        RecordType.BBO: "bbo",
        RecordType.L2_BOOK_STATE: "l2",
        RecordType.TRADE: "trade",
    }
    for asset in assets:
        for record_type, kind in type_to_kind.items():
            for row in _all_rows(loaded, BINANCE, record_type, asset):
                if record_type == RecordType.TRADE:
                    counts[asset]["normalized_count"] += 1
                connection_id = row.get("connection_id")
                if not isinstance(connection_id, str) or not connection_id:
                    lineage_rejections += 1
                    continue
                received = _timestamp(row["received_time"], label="market received_time")
                captures: set[str] = set()
                lineage_epoch: int | None = None
                lineage_arrival: int | None = None
                if record_type == RecordType.TRADE:
                    epoch = row.get("connection_epoch")
                    sequence = row.get("arrival_sequence")
                    if epoch is not None and sequence is not None:
                        raw = _binance_wire_by_sequence(
                            loaded,
                            connection_id,
                            int(str(epoch)),
                            int(str(sequence)),
                        )
                        if (
                            raw is not None
                            and raw.get("message_asset") == asset
                            and _wire_kind(raw.get("channel"), raw.get("raw_message")) == "trade"
                            and isinstance(raw.get("capture_epoch_id"), str)
                            and _wire_role_matches(raw, lineage, "market")
                            and _binance_trade_matches_wire(row, raw)
                        ):
                            captures.add(str(raw["capture_epoch_id"]))
                            lineage_epoch = int(str(raw["connection_epoch"]))
                else:
                    frame_candidates = _binance_wire_frame_candidates(
                        loaded,
                        connection_id,
                        received,
                        asset,
                        kind,
                    )
                    exact_candidates = [
                        raw
                        for raw in frame_candidates
                        if _wire_role_matches(raw, lineage, "public")
                        and _binance_market_matches_wire(row, raw, kind)
                        and (
                            kind != "l2"
                            or _persisted_l2_levels_match_raw(
                                loaded,
                                BINANCE,
                                asset,
                                row,
                                raw,
                            )
                        )
                    ]
                    if len(exact_candidates) == 1:
                        raw = exact_candidates[0]
                        captures.add(str(raw["capture_epoch_id"]))
                        lineage_epoch = int(str(raw["connection_epoch"]))
                        lineage_arrival = int(str(raw["arrival_sequence"]))
                    else:
                        lineage_rejections += 1
                if len(captures) != 1:
                    if record_type == RecordType.TRADE:
                        lineage_rejections += 1
                    continue
                capture = next(iter(captures))
                if (
                    record_type == RecordType.BBO
                    and not _normalized_bbo_is_executable(row)
                ):
                    continue
                if (
                    record_type == RecordType.L2_BOOK_STATE
                    and not _normalized_l2_is_executable(
                        loaded, BINANCE, asset, row
                    )
                ):
                    continue
                if record_type == RecordType.L2_BOOK_STATE:
                    pair = (capture, asset)
                    observed_l2.add(pair)
                    if lineage_epoch is None or lineage_arrival is None:
                        continue
                    snapshot_id = row.get("snapshot_id")
                    book_epoch_id = row.get("book_epoch_id")
                    if (
                        not isinstance(snapshot_id, str)
                        or not snapshot_id
                        or not isinstance(book_epoch_id, str)
                        or not book_epoch_id
                    ):
                        continue
                    exact_l2_rows.append(
                        (
                            capture,
                            asset,
                            connection_id,
                            lineage_epoch,
                            book_epoch_id,
                            snapshot_id,
                            received,
                            lineage_arrival,
                        )
                    )
                    continue
                append_observation(capture, asset, kind, received)
                if record_type == RecordType.TRADE:
                    counts[asset]["normalized_with_raw_lineage_count"] += 1
    resync_arm_points: dict[
        tuple[str, str, str, int, str],
        tuple[datetime, int],
    ] = {}
    for (
        capture,
        asset,
        connection_id,
        lineage_epoch,
        book_epoch_id,
        snapshot_id,
        received,
        arrival,
    ) in exact_l2_rows:
        resync_key = (capture, asset, connection_id, lineage_epoch)
        if any(
            start_at == completed_at == received
            and start_book_epoch == completed_book_epoch == book_epoch_id
            and completed_snapshot == snapshot_id
            for completed_at, completed_book_epoch, completed_snapshot in (
                resync_completions.get(resync_key, ())
            )
            for start_at, start_book_epoch in resync_starts.get(resync_key, ())
        ):
            book_key = (*resync_key, book_epoch_id)
            arm_point = (received, arrival)
            previous = resync_arm_points.get(book_key)
            if previous is None or arm_point < previous:
                resync_arm_points[book_key] = arm_point
    for (
        capture,
        asset,
        connection_id,
        lineage_epoch,
        book_epoch_id,
        _,
        received,
        arrival,
    ) in exact_l2_rows:
        arm = resync_arm_points.get((capture, asset, connection_id, lineage_epoch, book_epoch_id))
        if arm is None or not _at_or_after_resync_arm(received, arrival, arm):
            continue
        valid_l2.add((capture, asset))
        append_observation(capture, asset, "l2", received)
    return (
        observations,
        counts,
        tuple(sorted(observed_l2 - valid_l2)),
        lineage_rejections,
    )


def _hyperliquid_market_matches_wire(
    normalized: Mapping[str, object],
    raw: Mapping[str, object],
    kind: str,
) -> bool:
    if not _normalized_wire_identity_matches(normalized, raw):
        return False
    raw_message = raw.get("raw_message")
    if not isinstance(raw_message, str):
        return False
    try:
        root = json.loads(raw_message)
        if not isinstance(root, Mapping):
            return False
        expected_channel = {"bbo": "bbo", "l2": "l2Book", "trade": "trades"}[kind]
        if root.get("channel") != expected_channel or raw.get("channel") != expected_channel:
            return False
        data = root["data"]
        connection = str(raw["connection_id"])
        epoch = int(str(raw["connection_epoch"]))
        arrival = int(str(raw["arrival_sequence"]))
        asset = str(normalized["asset"])
        if kind in {"bbo", "l2"}:
            if not isinstance(data, Mapping) or str(data["coin"]) != asset:
                return False
            milliseconds = int(str(data["time"]))
            source_time = datetime.fromtimestamp(milliseconds / 1_000, tz=UTC)
            if normalized.get("event_time") != source_time or normalized.get("exchange_time") != source_time:
                return False
            if kind == "bbo":
                sides = data["bbo"]
                if (
                    not isinstance(sides, Sequence)
                    or isinstance(sides, (str, bytes, bytearray))
                    or len(sides) != 2
                ):
                    return False
                def side_matches(
                    raw_side: object,
                    price_field: str,
                    quantity_field: str,
                ) -> bool:
                    if raw_side is None:
                        return (
                            normalized.get(price_field) is None
                            and normalized.get(quantity_field) is None
                        )
                    if not isinstance(raw_side, Mapping):
                        return False
                    try:
                        return (
                            Decimal(str(normalized.get(price_field)))
                            == Decimal(str(raw_side["px"]))
                            and Decimal(str(normalized.get(quantity_field)))
                            == Decimal(str(raw_side["sz"]))
                        )
                    except (KeyError, TypeError, ValueError, ArithmeticError):
                        return False

                return (
                    normalized.get("source_sequence") is None
                    and normalized.get("update_id")
                    == f"{milliseconds}:{asset}:{connection}:{epoch}:{arrival}"
                    and side_matches(sides[0], "bid_price", "bid_quantity")
                    and side_matches(sides[1], "ask_price", "ask_quantity")
                )
            levels = data["levels"]
            if (
                not isinstance(levels, Sequence)
                or isinstance(levels, (str, bytes, bytearray))
                or len(levels) != 2
            ):
                return False
            bids = levels[0]
            asks = levels[1]
            if (
                not isinstance(bids, Sequence)
                or isinstance(bids, (str, bytes, bytearray))
                or not isinstance(asks, Sequence)
                or isinstance(asks, (str, bytes, bytearray))
            ):
                return False
            return (
                normalized.get("source_sequence") is None
                and normalized.get("snapshot_id") == f"ws:{connection}:{epoch}:{arrival}:{milliseconds}"
                and normalized.get("book_epoch_id") == f"{connection}:{epoch}"
                and normalized.get("bid_level_count") == len(bids)
                and normalized.get("ask_level_count") == len(asks)
            )
        if kind == "trade":
            if not isinstance(data, Sequence) or isinstance(data, (str, bytes, bytearray)):
                return False
            for raw_trade in data:
                if not isinstance(raw_trade, Mapping) or str(raw_trade.get("coin")) != asset:
                    continue
                milliseconds = int(str(raw_trade["time"]))
                source_time = datetime.fromtimestamp(milliseconds / 1_000, tz=UTC)
                trade_id = int(str(raw_trade["tid"]))
                aggressor = {"B": "buy", "A": "sell"}.get(
                    str(raw_trade.get("side")),
                    "unknown",
                )
                if (
                    normalized.get("event_time") == source_time
                    and normalized.get("exchange_time") == source_time
                    and normalized.get("trade_id") == f"{milliseconds}:{asset}:{trade_id}"
                    and Decimal(str(normalized.get("price"))) == Decimal(str(raw_trade["px"]))
                    and Decimal(str(normalized.get("quantity"))) == Decimal(str(raw_trade["sz"]))
                    and Decimal(str(normalized.get("quote_quantity")))
                    == Decimal(str(raw_trade["px"])) * Decimal(str(raw_trade["sz"]))
                    and normalized.get("is_liquidation") is None
                    and normalized.get("aggressor_side") == aggressor
                    and normalized.get("connection_epoch") == epoch
                    and normalized.get("arrival_sequence") == arrival
                ):
                    return True
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False
    return False


def _hyperliquid_rest_l2_identity(
    row: Mapping[str, object],
) -> tuple[str, str, str, datetime, datetime, datetime, str] | None:
    """Validate explicit REST snapshot provenance without treating it as wire data."""

    connection = row.get("connection_id")
    asset = row.get("asset")
    snapshot = row.get("snapshot_id")
    book_epoch = row.get("book_epoch_id")
    if not all(isinstance(value, str) and value for value in (connection, asset, snapshot, book_epoch)):
        return None
    assert isinstance(connection, str)
    assert isinstance(asset, str)
    assert isinstance(snapshot, str)
    assert isinstance(book_epoch, str)
    try:
        received = _timestamp(row.get("received_time"), label="REST L2 received_time")
        event_time = _timestamp(row.get("event_time"), label="REST L2 event_time")
        exchange_time = _timestamp(
            row.get("exchange_time"),
            label="REST L2 exchange_time",
        )
        prefix = f"rest:{connection}:"
        if not snapshot.startswith(prefix):
            return None
        identity = snapshot.removeprefix(prefix).split(":")
        if len(identity) != 3:
            return None
        epoch, arrival, milliseconds = (int(value) for value in identity)
    except (TypeError, ValueError):
        return None
    if (
        epoch <= 0
        or arrival <= 0
        or book_epoch != f"{connection}:{epoch}"
        or event_time != received
        or int(exchange_time.timestamp() * 1_000) != milliseconds
        or row.get("source_sequence") is not None
    ):
        return None
    return (
        asset,
        snapshot,
        book_epoch,
        received,
        event_time,
        exchange_time,
        connection,
    )


def _hyperliquid_rest_bbo_identity(
    row: Mapping[str, object],
) -> tuple[str, str, int, int, datetime, datetime] | None:
    update_id = row.get("update_id")
    connection = row.get("connection_id")
    asset = row.get("asset")
    if not (
        isinstance(update_id, str)
        and update_id
        and isinstance(connection, str)
        and connection
        and isinstance(asset, str)
        and asset
    ):
        return None
    try:
        received = _timestamp(row.get("received_time"), label="REST BBO received_time")
        event_time = _timestamp(row.get("event_time"), label="REST BBO event_time")
        exchange_time = _timestamp(
            row.get("exchange_time"),
            label="REST BBO exchange_time",
        )
        milliseconds = int(exchange_time.timestamp() * 1_000)
        prefix = f"rest:{milliseconds}:{asset}:{connection}:"
        if not update_id.startswith(prefix):
            return None
        identity = update_id.removeprefix(prefix).split(":")
        if len(identity) != 2:
            return None
        epoch, arrival = (int(value) for value in identity)
    except (TypeError, ValueError):
        return None
    if epoch <= 0 or arrival <= 0 or event_time != exchange_time or row.get("source_sequence") is not None:
        return None
    return asset, connection, epoch, arrival, received, exchange_time


def _persisted_hyperliquid_rest_l2_levels_match_header(
    loaded: _LoadedLake,
    asset: str,
    header: Mapping[str, object],
) -> bool:
    identity = _hyperliquid_rest_l2_identity(header)
    if identity is None or identity[0] != asset:
        return False
    snapshot = identity[1]
    candidates = [
        row
        for row in _all_rows(
            loaded,
            HYPERLIQUID,
            RecordType.L2_SNAPSHOT,
            asset,
            filters={"snapshot_id": snapshot},
        )
    ]
    try:
        bid_count = int(str(header["bid_level_count"]))
        ask_count = int(str(header["ask_level_count"]))
    except (KeyError, TypeError, ValueError):
        return False
    expected_levels = {
        *(("bid", level) for level in range(bid_count)),
        *(("ask", level) for level in range(ask_count)),
    }
    if bid_count < 0 or ask_count < 0 or len(candidates) != len(expected_levels):
        return False
    observed_levels: list[tuple[str, int]] = []
    for row in candidates:
        try:
            if (
                _hyperliquid_rest_l2_identity(row) != identity
                or row.get("last_sequence") is not None
                or row.get("order_count") is None
                or int(str(row["order_count"])) < 0
                or Decimal(str(row["price"])) <= 0
                or Decimal(str(row["quantity"])) <= 0
            ):
                return False
            observed_levels.append((str(row["side"]), int(str(row["level"]))))
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return False
    return len(observed_levels) == len(set(observed_levels)) and set(observed_levels) == expected_levels


def _persisted_hyperliquid_rest_bbo_matches_l2(
    loaded: _LoadedLake,
    asset: str,
    bbo: Mapping[str, object],
) -> bool:
    """Accept a REST BBO only when its exact persisted REST L2 source exists."""

    identity = _hyperliquid_rest_bbo_identity(bbo)
    if identity is None or identity[0] != asset:
        return False
    _, connection, epoch, arrival, received, exchange_time = identity
    milliseconds = int(exchange_time.timestamp() * 1_000)
    snapshot = f"rest:{connection}:{epoch}:{arrival}:{milliseconds}"
    headers = [
        row
        for row in _all_rows(
            loaded,
            HYPERLIQUID,
            RecordType.L2_BOOK_STATE,
            asset,
            filters={"snapshot_id": snapshot},
        )
    ]
    if len(headers) != 1:
        return False
    header = headers[0]
    header_identity = _hyperliquid_rest_l2_identity(header)
    if (
        header_identity is None
        or header_identity[0] != asset
        or header_identity[3] != received
        or header_identity[5] != exchange_time
        or header_identity[6] != connection
        or not _persisted_hyperliquid_rest_l2_levels_match_header(
            loaded,
            asset,
            header,
        )
    ):
        return False
    levels = [
        row
        for row in _all_rows(
            loaded,
            HYPERLIQUID,
            RecordType.L2_SNAPSHOT,
            asset,
            filters={"snapshot_id": snapshot},
        )
    ]

    def side_matches(side: str, price_field: str, quantity_field: str) -> bool:
        best = [row for row in levels if row.get("side") == side and row.get("level") == 0]
        if not best:
            return bbo.get(price_field) is None and bbo.get(quantity_field) is None
        if len(best) != 1:
            return False
        try:
            return Decimal(str(bbo.get(price_field))) == Decimal(str(best[0]["price"])) and Decimal(
                str(bbo.get(quantity_field))
            ) == Decimal(str(best[0]["quantity"]))
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return False

    return side_matches("bid", "bid_price", "bid_quantity") and side_matches(
        "ask",
        "ask_price",
        "ask_quantity",
    )


def _hyperliquid_observations(
    loaded: _LoadedLake,
    assets: Sequence[str],
    lineage: _ConnectionLineage,
) -> tuple[dict[str, dict[str, dict[str, SpilledTimestampSeries]]], int]:
    observations: dict[
        str,
        dict[str, dict[str, SpilledTimestampSeries]],
    ] = {}

    def append_observation(
        capture: str,
        asset: str,
        kind: str,
        received: datetime,
    ) -> None:
        kinds = observations.setdefault(capture, {}).setdefault(asset, {})
        series = kinds.get(kind)
        if series is None:
            series = loaded.store.new_timestamp_series()
            kinds[kind] = series
        series.append(received)

    type_to_kind = {
        RecordType.BBO: "bbo",
        RecordType.L2_BOOK_STATE: "l2",
        RecordType.TRADE: "trade",
    }
    lineage_rejections = 0
    for asset in assets:
        for record_type, kind in type_to_kind.items():
            for row in _all_rows(loaded, HYPERLIQUID, record_type, asset):
                if record_type == RecordType.BBO and _persisted_hyperliquid_rest_bbo_matches_l2(
                    loaded, asset, row
                ):
                    continue
                if (
                    record_type == RecordType.L2_BOOK_STATE
                    and _persisted_hyperliquid_rest_l2_levels_match_header(loaded, asset, row)
                ):
                    continue
                connection_id = row.get("connection_id")
                if not isinstance(connection_id, str) or not connection_id:
                    lineage_rejections += 1
                    continue
                received = _timestamp(row["received_time"], label="market received_time")
                candidates = (
                    raw
                    for raw in _all_rows(
                        loaded,
                        HYPERLIQUID,
                        RecordType.WIRE_MESSAGE,
                        filters={
                            "connection_id": connection_id,
                            "received_time": received,
                        },
                    )
                    if int(str(raw.get("schema_version", 0))) >= 2
                    and asset in _raw_payload_assets(HYPERLIQUID, raw)
                    and raw.get("message_asset") == _raw_primary_payload_asset(HYPERLIQUID, raw)
                    and {
                        "bbo": "bbo",
                        "l2Book": "l2",
                        "trades": "trade",
                    }.get(str(raw.get("channel") or ""))
                    == kind
                )
                exact = [
                    raw
                    for raw in candidates
                    if _wire_role_matches(raw, lineage, "public")
                    and _hyperliquid_market_matches_wire(row, raw, kind)
                    and (
                        kind != "l2"
                        or _persisted_l2_levels_match_raw(
                            loaded,
                            HYPERLIQUID,
                            asset,
                            row,
                            raw,
                        )
                    )
                ]
                if len(exact) != 1:
                    lineage_rejections += 1
                    continue
                if (
                    record_type == RecordType.BBO
                    and not _normalized_bbo_is_executable(row)
                ):
                    continue
                if (
                    record_type == RecordType.L2_BOOK_STATE
                    and not _normalized_l2_is_executable(
                        loaded, HYPERLIQUID, asset, row
                    )
                ):
                    continue
                append_observation(
                    str(exact[0]["capture_epoch_id"]),
                    asset,
                    kind,
                    received,
                )
    return observations, lineage_rejections


def _orphan_required_wire_counts(
    loaded: _LoadedLake,
    venue: str,
    assets: Sequence[str],
    lineage: _ConnectionLineage,
) -> _RequiredWireAudit:
    counts = {asset: 0 for asset in assets}
    by_kind_asset = {
        kind: {asset: 0 for asset in assets}
        for kind in ("bbo", "l2", "trade", "unknown")
    }
    sample_candidates: list[
        tuple[tuple[str, str, str, str, str, int, int], Mapping[str, object]]
    ] = []
    type_by_kind = {
        "bbo": RecordType.BBO,
        "l2": RecordType.L2_BOOK_STATE,
        "trade": RecordType.TRADE,
    }
    expected_role = (
        {"bbo": "public", "l2": "public", "trade": "market"}
        if venue == BINANCE
        else {"bbo": "public", "l2": "public", "trade": "public"}
    )

    def expected_normalized_count(
        raw: Mapping[str, object],
        kind: str,
        asset: str,
    ) -> int:
        if venue != HYPERLIQUID or kind != "trade":
            return 1
        try:
            root = json.loads(str(raw.get("raw_message")))
            data = root["data"]
            return sum(
                isinstance(item, Mapping) and item.get("coin") == asset
                for item in data
            )
        except (KeyError, TypeError, json.JSONDecodeError):
            return 0

    def record_orphan(
        raw: Mapping[str, object],
        *,
        asset: str,
        kind: str,
        reason: str,
        expected_count: int,
        exact_count: int | None,
        payload_kind: str | None,
    ) -> None:
        counts[asset] += 1
        by_kind_asset[kind][asset] += 1
        received = _timestamp(
            raw["received_time"],
            label="orphan raw received_time",
        )
        raw_message = raw.get("raw_message")
        persisted_sha256 = raw.get("payload_sha256")
        payload_sha256 = (
            str(persisted_sha256)
            if isinstance(persisted_sha256, str) and persisted_sha256
            else hashlib.sha256(str(raw_message).encode()).hexdigest()
        )
        connection_epoch_value = raw.get("connection_epoch")
        arrival_sequence_value = raw.get("arrival_sequence")
        connection_epoch = (
            None
            if connection_epoch_value is None
            else int(str(connection_epoch_value))
        )
        arrival_sequence = (
            None
            if arrival_sequence_value is None
            else int(str(arrival_sequence_value))
        )
        sample: Mapping[str, object] = {
            "venue": venue,
            "asset": asset,
            "kind": kind,
            "reason": reason,
            "capture_epoch_id": raw.get("capture_epoch_id"),
            "connection_id": raw.get("connection_id"),
            "connection_epoch": connection_epoch,
            "arrival_sequence": arrival_sequence,
            "received_time": _iso(received),
            "payload_sha256": payload_sha256,
            "persisted_message_asset": raw.get("message_asset"),
            "persisted_channel": raw.get("channel"),
            "payload_kind": payload_kind,
            "expected_normalized_count": expected_count,
            "exact_normalized_count": exact_count,
        }
        key = (
            _iso(received),
            venue,
            asset,
            str(raw.get("capture_epoch_id") or ""),
            str(raw.get("connection_id") or ""),
            -1 if connection_epoch is None else connection_epoch,
            -1 if arrival_sequence is None else arrival_sequence,
        )
        sample_candidates.append((key, sample))
        sample_candidates.sort(key=lambda candidate: candidate[0])
        if len(sample_candidates) > _REQUIRED_WIRE_FORENSIC_SAMPLE_LIMIT:
            sample_candidates.pop()

    for raw in _all_rows(loaded, venue, RecordType.WIRE_MESSAGE):
        persisted_asset = raw.get("message_asset")
        target_assets: tuple[str, ...]
        persisted_kind = _wire_kind(raw.get("channel"), raw.get("raw_message"))
        payload_kind = _payload_wire_kind(venue, raw.get("raw_message"))
        kind = payload_kind or persisted_kind
        if venue == BINANCE:
            payload_asset_candidates = _raw_payload_assets(BINANCE, raw) & set(assets)
            payload_asset = (
                next(iter(payload_asset_candidates)) if len(payload_asset_candidates) == 1 else None
            )
            if payload_asset is not None and persisted_asset != payload_asset:
                classified_kind = kind or "unknown"
                record_orphan(
                    raw,
                    asset=payload_asset,
                    kind=classified_kind,
                    reason="message_asset_mismatch",
                    expected_count=expected_normalized_count(
                        raw,
                        classified_kind,
                        payload_asset,
                    ),
                    exact_count=None,
                    payload_kind=payload_kind,
                )
                continue
            fallback_asset = persisted_asset if isinstance(persisted_asset, str) else None
            target = payload_asset or fallback_asset
            target_assets = (target,) if isinstance(target, str) and target in counts else ()
        else:
            payload_assets = _raw_payload_assets(venue, raw)
            requested_payload_assets = payload_assets & set(assets)
            primary_asset = _raw_primary_payload_asset(venue, raw)
            if requested_payload_assets and persisted_asset != primary_asset:
                for payload_asset in requested_payload_assets:
                    classified_kind = kind or "unknown"
                    record_orphan(
                        raw,
                        asset=payload_asset,
                        kind=classified_kind,
                        reason="message_asset_mismatch",
                        expected_count=expected_normalized_count(
                            raw,
                            classified_kind,
                            payload_asset,
                        ),
                        exact_count=None,
                        payload_kind=payload_kind,
                    )
                continue
            if requested_payload_assets:
                target_assets = tuple(sorted(requested_payload_assets))
            elif isinstance(persisted_asset, str) and persisted_asset in counts:
                target_assets = (persisted_asset,)
            else:
                target_assets = ()
        if not target_assets or kind not in type_by_kind:
            continue
        if payload_kind is not None and persisted_kind != payload_kind:
            for asset in target_assets:
                record_orphan(
                    raw,
                    asset=asset,
                    kind=kind,
                    reason="persisted_channel_payload_kind_mismatch",
                    expected_count=expected_normalized_count(raw, kind, asset),
                    exact_count=None,
                    payload_kind=payload_kind,
                )
            continue
        for asset in target_assets:
            candidates = [
                normalized
                for normalized in _all_rows(
                    loaded,
                    venue,
                    type_by_kind[kind],
                    asset,
                    filters={
                        "connection_id": str(raw.get("connection_id") or ""),
                        "received_time": _timestamp(
                            raw["received_time"],
                            label="orphan raw received_time",
                        ),
                    },
                )
            ]
            if not _wire_role_matches(raw, lineage, expected_role[kind]):
                record_orphan(
                    raw,
                    asset=asset,
                    kind=kind,
                    reason="connection_role_lineage_mismatch",
                    expected_count=expected_normalized_count(raw, kind, asset),
                    exact_count=None,
                    payload_kind=payload_kind,
                )
                continue
            if venue == BINANCE:
                exact = [
                    normalized
                    for normalized in candidates
                    if (
                        _binance_trade_matches_wire(normalized, raw)
                        if kind == "trade"
                        else _binance_market_matches_wire(normalized, raw, kind)
                    )
                    and (
                        kind != "l2"
                        or _persisted_l2_levels_match_raw(
                            loaded,
                            venue,
                            asset,
                            normalized,
                            raw,
                        )
                    )
                ]
                expected_count = 1
                if kind == "l2":
                    bbo_candidates = [
                        normalized
                        for normalized in _all_rows(
                            loaded,
                            BINANCE,
                            RecordType.BBO,
                            asset,
                            filters={
                                "connection_id": str(raw.get("connection_id") or ""),
                                "received_time": _timestamp(
                                    raw["received_time"],
                                    label="orphan raw received_time",
                                ),
                            },
                        )
                    ]
                    exact_bbo = [
                        normalized
                        for normalized in bbo_candidates
                        if _binance_market_matches_wire(normalized, raw, "bbo")
                    ]
                    if len(exact_bbo) != 1:
                        expected_count = 0
            else:
                exact = [
                    normalized
                    for normalized in candidates
                    if _hyperliquid_market_matches_wire(normalized, raw, kind)
                    and (
                        kind != "l2"
                        or _persisted_l2_levels_match_raw(
                            loaded,
                            venue,
                            asset,
                            normalized,
                            raw,
                        )
                    )
                ]
                expected_count = 1
                if kind == "trade":
                    raw_message = raw.get("raw_message")
                    try:
                        root = json.loads(str(raw_message))
                        data = root["data"]
                        expected_count = sum(
                            isinstance(item, Mapping) and item.get("coin") == asset for item in data
                        )
                    except (KeyError, TypeError, json.JSONDecodeError):
                        expected_count = 0
            if expected_count <= 0 or len(exact) != expected_count:
                record_orphan(
                    raw,
                    asset=asset,
                    kind=kind,
                    reason="exact_normalization_count_mismatch",
                    expected_count=expected_count,
                    exact_count=len(exact),
                    payload_kind=payload_kind,
                )
    return _RequiredWireAudit(
        counts=counts,
        by_kind_asset=by_kind_asset,
        total=sum(counts.values()),
        samples=tuple(sample for _, sample in sample_candidates),
    )


def _required_wire_forensics(
    audits: Mapping[str, _RequiredWireAudit],
) -> dict[str, object]:
    samples = sorted(
        (sample for audit in audits.values() for sample in audit.samples),
        key=lambda sample: (
            str(sample.get("received_time") or ""),
            str(sample.get("venue") or ""),
            str(sample.get("asset") or ""),
            str(sample.get("kind") or ""),
            str(sample.get("capture_epoch_id") or ""),
            str(sample.get("connection_id") or ""),
            int(str(sample.get("connection_epoch") or -1)),
            int(str(sample.get("arrival_sequence") or -1)),
        ),
    )[:_REQUIRED_WIRE_FORENSIC_SAMPLE_LIMIT]
    orphan_total = sum(audit.total for audit in audits.values())
    return {
        "classification": "evidence_only_no_economic_reuse",
        "sample_limit": _REQUIRED_WIRE_FORENSIC_SAMPLE_LIMIT,
        "orphan_total": orphan_total,
        "samples_retained": len(samples),
        "samples_truncated": max(orphan_total - len(samples), 0),
        "by_kind_asset": {
            venue: {
                kind: dict(by_asset)
                for kind, by_asset in audit.by_kind_asset.items()
            }
            for venue, audit in audits.items()
        },
        "samples": samples,
    }


def _orphan_normalized_l2_level_counts(
    loaded: _LoadedLake,
    venue: str,
    assets: Sequence[str],
    lineage: _ConnectionLineage,
) -> dict[str, int]:
    """Count persisted levels that do not belong to one exact raw/header frame."""

    counts = {asset: 0 for asset in assets}
    accepted_groups = loaded.store.new_set()
    for asset in assets:
        for header in _all_rows(loaded, venue, RecordType.L2_BOOK_STATE, asset):
            if venue == HYPERLIQUID:
                rest_identity = _hyperliquid_rest_l2_identity(header)
                if rest_identity is not None:
                    if _persisted_hyperliquid_rest_l2_levels_match_header(
                        loaded,
                        asset,
                        header,
                    ):
                        accepted_groups.add(rest_identity)
                    continue
            connection = header.get("connection_id")
            snapshot = header.get("snapshot_id")
            book_epoch = header.get("book_epoch_id")
            received = header.get("received_time")
            event_time = header.get("event_time")
            exchange_time = header.get("exchange_time")
            if not (
                isinstance(connection, str)
                and connection
                and isinstance(snapshot, str)
                and snapshot
                and isinstance(book_epoch, str)
                and book_epoch
                and isinstance(received, datetime)
                and isinstance(event_time, datetime)
                and isinstance(exchange_time, datetime)
            ):
                continue
            candidates = (
                raw
                for raw in _all_rows(
                    loaded,
                    venue,
                    RecordType.WIRE_MESSAGE,
                    filters={
                        "connection_id": connection,
                        "received_time": _utc(received),
                    },
                )
                if raw.get("message_asset") == asset
                and _wire_kind(raw.get("channel"), raw.get("raw_message")) == "l2"
            )
            exact = [
                raw
                for raw in candidates
                if _wire_role_matches(raw, lineage, "public")
                and (
                    _binance_market_matches_wire(header, raw, "l2")
                    if venue == BINANCE
                    else _hyperliquid_market_matches_wire(header, raw, "l2")
                )
                and _persisted_l2_levels_match_raw(
                    loaded,
                    venue,
                    asset,
                    header,
                    raw,
                )
            ]
            if len(exact) == 1:
                accepted_groups.add(
                    (
                        asset,
                        snapshot,
                        book_epoch,
                        _utc(received),
                        _utc(event_time),
                        _utc(exchange_time),
                        connection,
                    )
                )
    for asset in assets:
        current_key: tuple[object, ...] | None = None
        current_count = 0
        current_accepted = False

        for level in loaded.store.iter_rows(
            venue,
            RecordType.L2_SNAPSHOT,
            asset,
            order_by="snapshot_group",
        ):
            snapshot = level.get("snapshot_id")
            book_epoch = level.get("book_epoch_id")
            received = level.get("received_time")
            event_time = level.get("event_time")
            exchange_time = level.get("exchange_time")
            connection = level.get("connection_id")
            key = (
                asset,
                str(snapshot or ""),
                str(book_epoch or ""),
                _timestamp(received, label="l2 level received_time"),
                _timestamp(event_time, label="l2 level event_time"),
                _timestamp(exchange_time, label="l2 level exchange_time"),
                str(connection or ""),
            )
            if key != current_key:
                if current_key is not None and not current_accepted:
                    counts[asset] += current_count
                current_key = key
                current_accepted = key in accepted_groups
                current_count = 0
            current_count += 1
        if current_key is not None and not current_accepted:
            counts[asset] += current_count
    return counts


def _connection_capture_map(loaded: _LoadedLake, venue: str) -> dict[str, set[str]]:
    result: dict[str, set[str]] = defaultdict(set)
    for row_venue, connection, _, capture in loaded.wire_identities:
        if row_venue == venue:
            result[connection].add(capture)
    return result


def _is_clock_gap_event(row: Mapping[str, object]) -> bool:
    return str(row.get("event_kind") or "") in _FAIL_CLOSED_EVENTS and (
        row.get("channel") == "clock_sync" or row.get("socket_role") in {"clock", "clock_sync"}
    )


def _event_outages(
    loaded: _LoadedLake,
    venue: str,
    start: datetime,
    end: datetime,
    connection_captures: Mapping[str, set[str]],
    lineage: _ConnectionLineage,
) -> _EventOutageAudit:
    outages: list[Interval] = []
    unbound_fail_closed_events = 0
    unbound_resync_events = 0
    in_window_gap_events = 0
    unclean_in_window_disconnect_events = 0
    clean_terminal_roles: dict[str, set[str]] = defaultdict(set)
    active_event_captures: set[str] = set()
    failure_events_by_capture: dict[str, list[dict[str, object]]] = defaultdict(list)
    resync_events: dict[
        tuple[str, str, str, int, str],
        dict[str, list[tuple[datetime, str]]],
    ] = defaultdict(lambda: {"resync_start": [], "resync_complete": []})
    for row in _rows_with_boundaries(
        loaded,
        venue,
        RecordType.CONNECTION_EVENT,
    ):
        event_kind = str(row.get("event_kind") or "")
        at = _timestamp(row["received_time"], label="connection-event received_time")
        if _is_clock_gap_event(row):
            continue
        if event_kind == "gap" and start <= at < end:
            in_window_gap_events += 1
        if (
            event_kind == "disconnect"
            and start <= at < end
            and str(row.get("reason") or "").strip().lower()
            not in {
                "collector stop requested",
                "collector stop requested or bounded run completed",
            }
        ):
            unclean_in_window_disconnect_events += 1
        connection_id = row.get("connection_id")
        explicit_capture = row.get("capture_epoch_id")
        clean_stop = event_kind == "disconnect" and str(row.get("reason") or "").strip().lower() in {
            "collector stop requested",
            "collector stop requested or bounded run completed",
        }
        if clean_stop and start <= at < end:
            connection_epoch = row.get("connection_epoch")
            role = row.get("socket_role")
            identity = (
                None
                if not (
                    isinstance(connection_id, str)
                    and connection_id
                    and connection_epoch is not None
                    and isinstance(explicit_capture, str)
                    and explicit_capture
                    and isinstance(role, str)
                    and role
                )
                else lineage.identities.get(
                    (
                        connection_id,
                        int(str(connection_epoch)),
                        explicit_capture,
                    )
                )
            )
            if (
                identity is None
                or identity[0] != role
                or identity[1] > at
                or explicit_capture not in lineage.eligible_captures
            ):
                unclean_in_window_disconnect_events += 1
            else:
                assert isinstance(explicit_capture, str)
                clean_terminal_roles[explicit_capture].add(role)
        if event_kind in _FAIL_CLOSED_EVENTS:
            connection_epoch = row.get("connection_epoch")
            role = row.get("socket_role")
            identity = (
                None
                if not (
                    isinstance(connection_id, str)
                    and connection_id
                    and connection_epoch is not None
                    and isinstance(explicit_capture, str)
                    and explicit_capture
                    and isinstance(role, str)
                    and role
                )
                else lineage.identities.get(
                    (
                        connection_id,
                        int(str(connection_epoch)),
                        explicit_capture,
                    )
                )
            )
            bound_fail_closed = (
                identity is not None
                and identity[0] == role
                and identity[1] <= at
                and explicit_capture in lineage.eligible_captures
            )
            if not bound_fail_closed:
                physical_matches = [
                    identity_capture
                    for (
                        identity_connection,
                        identity_epoch,
                        identity_capture,
                    ), (identity_role, _) in lineage.identities.items()
                    if identity_connection == connection_id
                    and connection_epoch is not None
                    and identity_epoch == int(str(connection_epoch))
                    and identity_role == role
                    and identity_capture in lineage.eligible_captures
                ]
                capture_conflicts_with_active_physical_identity = (
                    len(physical_matches) == 1 and explicit_capture != physical_matches[0]
                )
                if (
                    capture_conflicts_with_active_physical_identity
                    or start - timedelta(milliseconds=MAX_CLOCK_AGE_MS) <= at < end
                ):
                    unbound_fail_closed_events += 1
                continue
        tags: set[str] = set()
        if event_kind in {"resync_start", "resync_complete"}:
            connection_epoch = row.get("connection_epoch")
            identity = (
                None
                if not (
                    isinstance(connection_id, str)
                    and connection_id
                    and connection_epoch is not None
                    and isinstance(explicit_capture, str)
                    and explicit_capture
                )
                else lineage.identities.get(
                    (
                        connection_id,
                        int(str(connection_epoch)),
                        explicit_capture,
                    )
                )
            )
            bound_resync = (
                identity is not None
                and identity[0] == "public"
                and explicit_capture in lineage.eligible_captures
                and row.get("socket_role") == "public"
                and isinstance(row.get("asset"), str)
                and str(row["asset"]) not in {"", "GLOBAL"}
                and row.get("book_epoch_id") == f"{connection_id}:{int(str(connection_epoch))}"
                and (
                    venue != BINANCE
                    or event_kind != "resync_complete"
                    or (
                        isinstance(row.get("resync_snapshot_id"), str)
                        and bool(str(row["resync_snapshot_id"]))
                    )
                )
                and (
                    venue != BINANCE
                    or (
                        isinstance(row.get("channel"), str)
                        and "@depth20" in str(row["channel"])
                        and str(row["channel"]).partition("@")[0].upper()
                        == f"{str(row['asset']).upper()}USDT"
                    )
                )
                and (venue != BINANCE or identity[1] <= at)
            )
            if not bound_resync:
                if start <= at < end:
                    unbound_resync_events += 1
                continue
            assert isinstance(explicit_capture, str)
            tags.add(explicit_capture)
        elif isinstance(explicit_capture, str) and explicit_capture:
            tags.add(explicit_capture)
        elif isinstance(connection_id, str):
            capture_candidates = connection_captures.get(connection_id, set())
            if len(capture_candidates) == 1:
                tags.update(capture_candidates)
        for tag in tags:
            if start <= at < end and event_kind in _FAIL_CLOSED_EVENTS:
                active_event_captures.add(tag)
                if not clean_stop:
                    raw_reason = row.get("reason")
                    failure_events_by_capture[tag].append(
                        {
                            "received_time": _iso(at),
                            "event_kind": event_kind,
                            "socket_role": (
                                str(row["socket_role"]) if row.get("socket_role") is not None else None
                            ),
                            "channel": (str(row["channel"]) if row.get("channel") is not None else None),
                            "connection_id": str(connection_id or "") or None,
                            "reason": (str(raw_reason) if raw_reason is not None else None),
                        }
                    )
            connection_epoch = row.get("connection_epoch")
            book_epoch = str(row.get("book_epoch_id") or "")
            resync_key = (
                tag,
                str(row.get("asset") or "GLOBAL"),
                str(connection_id or ""),
                int(str(connection_epoch or 0)),
                book_epoch,
            )
            if event_kind in _FAIL_CLOSED_EVENTS:
                if at < end:
                    outages.append(Interval(max(at, start), end, tag))
            elif event_kind == "resync_start":
                resync_events[resync_key][event_kind].append((max(at, start), ""))
            elif event_kind == "resync_complete":
                resync_snapshot = row.get("resync_snapshot_id")
                if venue != BINANCE or (isinstance(resync_snapshot, str) and resync_snapshot):
                    resync_events[resync_key][event_kind].append(
                        (
                            min(at, end),
                            str(resync_snapshot or ""),
                        )
                    )
    valid_snapshots: SpilledSequence[tuple[str, str, str, int, str, str, datetime]] = (
        loaded.store.new_sequence()
    )
    for header in _all_rows(loaded, venue, RecordType.L2_BOOK_STATE):
        connection = header.get("connection_id")
        header_snapshot = header.get("snapshot_id")
        header_book_epoch = header.get("book_epoch_id")
        asset = header.get("asset")
        if not all(
            isinstance(value, str) and value
            for value in (
                connection,
                header_snapshot,
                header_book_epoch,
                asset,
            )
        ):
            continue
        assert isinstance(connection, str)
        assert isinstance(header_snapshot, str)
        assert isinstance(header_book_epoch, str)
        assert isinstance(asset, str)
        l2_received = _timestamp(
            header["received_time"],
            label="l2 received_time",
        )
        l2_wire_rows = (
            wire_row
            for wire_row in _all_rows(
                loaded,
                venue,
                RecordType.WIRE_MESSAGE,
                filters={
                    "connection_id": connection,
                    "received_time": l2_received,
                },
            )
            if wire_row.get("message_asset") == asset and _raw_required_kind(venue, wire_row) == "l2"
        )
        exact = [
            candidate_wire
            for candidate_wire in l2_wire_rows
            if _wire_role_matches(candidate_wire, lineage, "public")
            and (
                _binance_market_matches_wire(header, candidate_wire, "l2")
                if venue == BINANCE
                else _hyperliquid_market_matches_wire(
                    header,
                    candidate_wire,
                    "l2",
                )
            )
            and _persisted_l2_levels_match_raw(
                loaded,
                venue,
                asset,
                header,
                candidate_wire,
            )
        ]
        if len(exact) == 1:
            exact_wire = exact[0]
            valid_snapshots.append(
                (
                    str(exact_wire["capture_epoch_id"]),
                    asset,
                    connection,
                    int(str(exact_wire["connection_epoch"])),
                    header_book_epoch,
                    header_snapshot,
                    l2_received,
                )
            )
    for (tag, asset, connection, epoch, book_epoch), events in resync_events.items():
        completions = sorted(events["resync_complete"])
        for outage_start, _ in sorted(events["resync_start"]):
            outage_end = end
            for completion_at, completion_snapshot in completions:
                if completion_at < outage_start:
                    continue
                for snapshot in valid_snapshots:
                    if snapshot[:5] != (
                        tag,
                        asset,
                        connection,
                        epoch,
                        book_epoch,
                    ):
                        continue
                    if venue == BINANCE:
                        if snapshot[5] == completion_snapshot and snapshot[6] == completion_at:
                            outage_end = min(outage_end, completion_at)
                    elif snapshot[6] >= completion_at and (
                        not completion_snapshot or snapshot[5] == completion_snapshot
                    ):
                        outage_end = min(outage_end, snapshot[6])
            if outage_start < outage_end:
                outages.append(Interval(outage_start, outage_end, tag))
    return _EventOutageAudit(
        intervals=_merge(outages),
        unbound_fail_closed_events=unbound_fail_closed_events,
        unbound_resync_events=unbound_resync_events,
        active_event_captures=frozenset(active_event_captures),
        in_window_gap_events=in_window_gap_events,
        unclean_in_window_disconnect_events=(unclean_in_window_disconnect_events),
        failure_events_by_capture={
            capture: tuple(
                sorted(
                    events,
                    key=lambda event: str(event["received_time"]),
                )
            )
            for capture, events in sorted(failure_events_by_capture.items())
        },
        clean_terminal_roles={
            capture: frozenset(roles) for capture, roles in sorted(clean_terminal_roles.items())
        },
    )


def _state_intervals(
    timestamps: Iterable[datetime],
    tag: str,
    ttl: timedelta,
    start: datetime,
    end: datetime,
    outages: Iterable[Interval],
) -> tuple[Interval, ...]:
    merged: list[Interval] = []
    current_start: datetime | None = None
    current_end: datetime | None = None
    for value in timestamps:
        if not start <= value < end:
            continue
        interval_end = min(value + ttl, end)
        if value >= interval_end:
            continue
        if current_start is None or current_end is None:
            current_start = value
            current_end = interval_end
            continue
        if value <= current_end:
            current_end = max(current_end, interval_end)
            continue
        merged.append(Interval(current_start, current_end, tag))
        current_start = value
        current_end = interval_end
    if current_start is not None and current_end is not None:
        merged.append(Interval(current_start, current_end, tag))
    return _subtract(_clip(merged, start, end), outages)


def _required_market_intervals(
    observations: Mapping[
        str,
        Mapping[str, Mapping[str, Iterable[datetime]]],
    ],
    assets: Sequence[str],
    ttl: timedelta,
    start: datetime,
    end: datetime,
    outages: Iterable[Interval],
) -> dict[str, dict[str, tuple[Interval, ...]]]:
    result: dict[str, dict[str, tuple[Interval, ...]]] = defaultdict(dict)
    for tag, by_asset in observations.items():
        for asset in assets:
            kinds = by_asset.get(asset, {})
            current: tuple[Interval, ...] | None = None
            for kind in ("bbo", "l2"):
                intervals = _state_intervals(kinds.get(kind, ()), tag, ttl, start, end, outages)
                current = (
                    intervals if current is None else _intersect(current, intervals, require_same_tag=True)
                )
            trade_freshness = _state_intervals(
                kinds.get("trade", ()),
                tag,
                ttl,
                start,
                end,
                outages,
            )
            result[tag][asset] = (
                ()
                if current is None
                else _intersect(
                    current,
                    trade_freshness,
                    require_same_tag=True,
                )
            )
    return result


def _clock_intervals(
    loaded: _LoadedLake,
    start: datetime,
    end: datetime,
    event_outages: Iterable[Interval],
    lineage: _ConnectionLineage,
    active_captures: frozenset[str],
) -> _ClockAudit:
    legacy = 0
    valid = 0
    invalid = 0
    rejected_probes = 0
    hard_invalid = 0
    failure_events = 0
    strict_policy_rejections = 0
    identity_rejections = 0
    unbound_invalid_events = 0
    in_window_invalid_events = 0
    in_window_rejected_probe_events = 0
    in_window_hard_invalid_events = 0
    in_window_failure_events = 0
    consecutive_rejection_violations = 0
    consecutive_rejection_violation_captures: set[str] = set()
    max_consecutive_rejected_probes = 0
    spacing_violations = 0
    spacing_violation_captures: set[str] = set()
    offset_discontinuities = 0
    offset_discontinuity_captures: set[str] = set()
    max_sample_gap_ms: float | None = None
    valid_by_capture: dict[str, list[_ClockSample]] = defaultdict(list)
    attempt_times_by_capture: dict[str, list[datetime]] = defaultdict(list)
    invalid_times: dict[str, list[datetime]] = defaultdict(list)
    consecutive_rejection_times: dict[str, list[datetime]] = defaultdict(list)
    rejection_streak_by_capture: dict[str, int] = defaultdict(int)
    clock_forensics = _ClockForensicAccumulator()

    def strict_policy_values(
        row: Mapping[str, object],
    ) -> tuple[int, int, Decimal, Decimal, Decimal] | None:
        try:
            sampling_interval_ms = int(str(row.get("sampling_interval_ms")))
            max_age_ms = int(str(row.get("max_age_ms")))
            declared_uncertainty = Decimal(str(row.get("max_uncertainty_ms")))
            measured_uncertainty = Decimal(str(row.get("drift_uncertainty_ms")))
            estimated_drift = Decimal(str(row.get("estimated_clock_drift_ms")))
        except (ArithmeticError, TypeError, ValueError):
            return None
        if (
            sampling_interval_ms <= 0
            or sampling_interval_ms > MAX_CLOCK_SAMPLING_INTERVAL_MS
            or max_age_ms < sampling_interval_ms
            or max_age_ms > MAX_CLOCK_AGE_MS
            or not declared_uncertainty.is_finite()
            or declared_uncertainty < 0
            or declared_uncertainty > MAX_CLOCK_UNCERTAINTY_MS
            or not measured_uncertainty.is_finite()
            or measured_uncertainty < 0
            or not estimated_drift.is_finite()
        ):
            return None
        return (
            sampling_interval_ms,
            max_age_ms,
            declared_uncertainty,
            measured_uncertainty,
            estimated_drift,
        )

    def is_expected_high_uncertainty_rejection(
        row: Mapping[str, object],
        received: datetime,
        policy: tuple[int, int, Decimal, Decimal, Decimal] | None,
    ) -> bool:
        if policy is None or row.get("sample_status") != "invalid":
            return False
        measured_uncertainty = policy[3]
        invalid_reason = row.get("invalid_reason")
        response_received = row.get("response_received_time")
        try:
            round_trip_ms = Decimal(str(row.get("round_trip_latency_ms")))
        except (ArithmeticError, TypeError, ValueError):
            return False
        return (
            measured_uncertainty > MAX_CLOCK_UNCERTAINTY_MS
            and round_trip_ms.is_finite()
            and round_trip_ms >= 0
            and measured_uncertainty == round_trip_ms / 2
            and isinstance(response_received, datetime)
            and _utc(response_received) == received
            and row.get("causal_valid_from") is None
            and row.get("causal_valid_until") is None
            and isinstance(invalid_reason, str)
            and invalid_reason.startswith("clock uncertainty exceeds threshold:")
        )

    for row in _rows_with_boundaries(
        loaded,
        BINANCE,
        RecordType.CLOCK_SYNC,
    ):
        version = int(str(row.get("schema_version", 0)))
        if version < 2:
            legacy += 1
            continue
        received = _timestamp(row["received_time"], label="clock received_time")
        capture = row.get("capture_epoch_id")
        if not isinstance(capture, str) or not capture:
            invalid += 1
            hard_invalid += 1
            clock_forensics.observe(row, "hard_invalid")
            if start <= received < end:
                in_window_invalid_events += 1
                in_window_hard_invalid_events += 1
            if start - timedelta(milliseconds=MAX_CLOCK_AGE_MS) <= received < end:
                unbound_invalid_events += 1
            continue
        connection = row.get("connection_id")
        connection_epoch = row.get("connection_epoch")
        request_sent = _timestamp(
            row["request_sent_time"],
            label="clock request_sent_time",
        )
        identity = (
            None
            if not (isinstance(connection, str) and connection and connection_epoch is not None)
            else lineage.identities.get(
                (
                    connection,
                    int(str(connection_epoch)),
                    capture,
                )
            )
        )
        identity_valid = (
            isinstance(connection, str)
            and connection
            and connection_epoch is not None
            and identity is not None
            and identity[0] == "public"
            and identity[1] <= request_sent
            and (
                BINANCE,
                connection,
                int(str(connection_epoch)),
                capture,
            )
            in loaded.wire_identities
            and _wire_role_matches(row, lineage, "public")
        )
        if not identity_valid:
            invalid += 1
            hard_invalid += 1
            clock_forensics.observe(row, "hard_invalid")
            identity_rejections += 1
            if start <= received < end:
                in_window_invalid_events += 1
                in_window_hard_invalid_events += 1
            invalid_times[capture].append(received)
            if start - timedelta(milliseconds=MAX_CLOCK_AGE_MS) <= received < end:
                unbound_invalid_events += 1
            continue
        attempt_times_by_capture[capture].append(request_sent)
        status = row.get("sample_status")
        policy = strict_policy_values(row)
        if status == "valid":
            strict_policy_valid = policy is not None and policy[3] <= MAX_CLOCK_UNCERTAINTY_MS
            if not strict_policy_valid:
                invalid += 1
                hard_invalid += 1
                clock_forensics.observe(row, "hard_invalid")
                strict_policy_rejections += 1
                if start <= received < end:
                    in_window_invalid_events += 1
                    in_window_hard_invalid_events += 1
                invalid_times[capture].append(received)
                continue
            assert policy is not None
            measured_uncertainty = policy[3]
            estimated_drift = policy[4]
            left = row.get("causal_valid_from")
            right = row.get("causal_valid_until")
            if not isinstance(left, datetime) or not isinstance(right, datetime):
                invalid += 1
                hard_invalid += 1
                clock_forensics.observe(row, "hard_invalid")
                if start <= received < end:
                    in_window_invalid_events += 1
                    in_window_hard_invalid_events += 1
                invalid_times[capture].append(received)
                continue
            left_utc = _utc(left)
            right_utc = _utc(right)
            if left_utc < right_utc:
                valid_by_capture[capture].append(
                    _ClockSample(
                        Interval(left_utc, right_utc, capture),
                        estimated_drift,
                        measured_uncertainty,
                    )
                )
                valid += 1
                rejection_streak_by_capture[capture] = 0
                clock_forensics.observe(row, "valid")
                clock_forensics.reset_rejection_streak(capture)
            else:
                invalid += 1
                hard_invalid += 1
                clock_forensics.observe(row, "hard_invalid")
                if start <= received < end:
                    in_window_invalid_events += 1
                    in_window_hard_invalid_events += 1
                invalid_times[capture].append(received)
        else:
            invalid += 1
            if start <= received < end:
                in_window_invalid_events += 1
            if is_expected_high_uncertainty_rejection(row, received, policy):
                rejected_probes += 1
                rejection_streak = rejection_streak_by_capture[capture] + 1
                rejection_streak_by_capture[capture] = rejection_streak
                rejected_sample = clock_forensics.observe(row, "rejected")
                clock_forensics.observe_rejection_streak(
                    capture,
                    rejection_streak,
                    rejected_sample,
                )
                max_consecutive_rejected_probes = max(
                    max_consecutive_rejected_probes,
                    rejection_streak,
                )
                if start <= received < end:
                    in_window_rejected_probe_events += 1
                if rejection_streak == MAX_CONSECUTIVE_REJECTED_CLOCK_PROBES + 1:
                    consecutive_rejection_violations += 1
                    consecutive_rejection_violation_captures.add(capture)
                    consecutive_rejection_times[capture].append(received)
                continue
            hard_invalid += 1
            clock_forensics.observe(row, "hard_invalid")
            if start <= received < end:
                in_window_hard_invalid_events += 1
            invalid_times[capture].append(received)

    for row in loaded.store.iter_rows(
        BINANCE,
        RecordType.CLOCK_SYNC,
        population="cadence",
    ):
        if int(str(row.get("schema_version", 0))) < 2:
            continue
        capture = row.get("capture_epoch_id")
        connection = row.get("connection_id")
        connection_epoch = row.get("connection_epoch")
        if (
            not isinstance(capture, str)
            or not capture
            or capture not in active_captures
            or not isinstance(connection, str)
            or not connection
            or connection_epoch is None
        ):
            continue
        request_sent = _timestamp(
            row["request_sent_time"],
            label="clock cadence successor request_sent_time",
        )
        identity = lineage.identities.get(
            (
                connection,
                int(str(connection_epoch)),
                capture,
            )
        )
        if (
            identity is None
            or identity[0] != "public"
            or identity[1] > request_sent
            or (
                BINANCE,
                connection,
                int(str(connection_epoch)),
                capture,
            )
            not in loaded.wire_identities
            or not _wire_role_matches(row, lineage, "public")
        ):
            continue
        attempt_times_by_capture[capture].append(request_sent)

    capture_start_by_capture = {
        capture: max(
            connected_at
            for (_, _, identity_capture), (_, connected_at) in (*lineage.identities.items(),)
            if identity_capture == capture
        )
        for capture in lineage.eligible_captures
    }
    capture_end_by_capture: dict[str, datetime] = {}
    for row in _rows_with_boundaries(
        loaded,
        BINANCE,
        RecordType.CONNECTION_EVENT,
    ):
        if str(row.get("event_kind") or "") not in _FAIL_CLOSED_EVENTS or _is_clock_gap_event(row):
            continue
        received = _timestamp(
            row["received_time"],
            label="capture terminal received_time",
        )
        capture = row.get("capture_epoch_id")
        connection = row.get("connection_id")
        connection_epoch = row.get("connection_epoch")
        role = row.get("socket_role")
        identity = (
            None
            if not (
                isinstance(capture, str)
                and capture
                and isinstance(connection, str)
                and connection
                and connection_epoch is not None
                and isinstance(role, str)
                and role
            )
            else lineage.identities.get(
                (
                    connection,
                    int(str(connection_epoch)),
                    capture,
                )
            )
        )
        if (
            identity is None
            or identity[0] != role
            or identity[1] > received
            or capture not in lineage.eligible_captures
            or not start < received <= end
        ):
            continue
        prior_end = capture_end_by_capture.get(capture)
        capture_end_by_capture[capture] = received if prior_end is None else min(prior_end, received)

    for row in _rows_with_boundaries(
        loaded,
        BINANCE,
        RecordType.CONNECTION_EVENT,
    ):
        if not _is_clock_gap_event(row):
            continue
        received = _timestamp(row["received_time"], label="clock gap received_time")
        capture = row.get("capture_epoch_id")
        failure_events += 1
        if start <= received < end:
            in_window_failure_events += 1
        event_connection = row.get("connection_id")
        event_epoch = row.get("connection_epoch")
        public_identities = [
            (connection, epoch)
            for (connection, epoch, identity_capture), (role, _) in (*lineage.identities.items(),)
            if identity_capture == capture and role == "public"
        ]
        event_bound = (
            isinstance(capture, str)
            and capture
            and capture in lineage.eligible_captures
            and isinstance(event_connection, str)
            and event_connection
            and event_epoch is not None
            and len(public_identities) == 1
            and int(str(event_epoch)) == public_identities[0][1]
            and event_connection
            in {
                public_identities[0][0],
                f"{public_identities[0][0]}:clock",
            }
        )
        if event_bound:
            assert isinstance(capture, str)
            invalid_times[capture].append(received)
        elif start - timedelta(milliseconds=MAX_CLOCK_AGE_MS) <= received < end:
            unbound_invalid_events += 1

    outages_by_tag: dict[str, list[Interval]] = defaultdict(list)
    for outage in event_outages:
        outages_by_tag[outage.tag].append(outage)
    result: dict[str, tuple[Interval, ...]] = {}
    consecutive_rejection_outages: dict[str, tuple[Interval, ...]] = {}

    def observe_spacing_gap(
        capture: str,
        previous: datetime,
        current: datetime,
    ) -> None:
        nonlocal max_sample_gap_ms, spacing_violations
        if current <= previous:
            return
        sample_gap_ms = (current - previous).total_seconds() * 1_000
        max_sample_gap_ms = (
            sample_gap_ms if max_sample_gap_ms is None else max(max_sample_gap_ms, sample_gap_ms)
        )
        if sample_gap_ms > MAX_CLOCK_SAMPLING_INTERVAL_MS:
            spacing_violations += 1
            spacing_violation_captures.add(capture)

    for capture in sorted(
        set(valid_by_capture)
        | set(attempt_times_by_capture)
        | set(invalid_times)
        | set(consecutive_rejection_times)
        | set(capture_start_by_capture)
    ):
        samples = sorted(
            valid_by_capture.get(capture, ()),
            key=lambda item: item.interval.start,
        )
        attempt_times = sorted(attempt_times_by_capture.get(capture, ()))
        invalid_outages: list[Interval] = []
        rejection_outages: list[Interval] = []
        offset_outages: list[Interval] = []
        if capture in active_captures:
            active_start = max(
                start,
                capture_start_by_capture.get(capture, start),
            )
            active_end = min(
                end,
                capture_end_by_capture.get(capture, end),
            )
            bounded_attempt_times = [attempt for attempt in attempt_times if attempt <= active_end]
            for previous, current in pairwise(bounded_attempt_times):
                if current <= active_start or previous >= active_end:
                    continue
                observe_spacing_gap(capture, previous, current)

            attempts_after_start = [attempt for attempt in bounded_attempt_times if attempt > active_start]
            if active_start < active_end and not any(
                attempt <= active_start for attempt in bounded_attempt_times
            ):
                observe_spacing_gap(
                    capture,
                    active_start,
                    (attempts_after_start[0] if attempts_after_start else active_end),
                )
            if active_start < active_end and bounded_attempt_times:
                observe_spacing_gap(
                    capture,
                    bounded_attempt_times[-1],
                    active_end,
                )

        def bands_overlap(first: _ClockSample, second: _ClockSample) -> bool:
            first_low = first.drift_ms - first.uncertainty_ms
            first_high = first.drift_ms + first.uncertainty_ms
            second_low = second.drift_ms - second.uncertainty_ms
            second_high = second.drift_ms + second.uncertainty_ms
            return first_high >= second_low and second_high >= first_low

        baseline_index = 0
        while baseline_index + 1 < len(samples):
            baseline = samples[baseline_index]
            discontinuous = samples[baseline_index + 1]
            if bands_overlap(baseline, discontinuous):
                baseline_index += 1
                continue
            offset_discontinuities += 1
            offset_discontinuity_captures.add(capture)
            recovery_index = next(
                (
                    candidate_index
                    for candidate_index in range(baseline_index + 2, len(samples))
                    if bands_overlap(baseline, samples[candidate_index])
                    or bands_overlap(discontinuous, samples[candidate_index])
                ),
                None,
            )
            recovery_at = end if recovery_index is None else samples[recovery_index].interval.start
            if discontinuous.interval.start < recovery_at:
                offset_outages.append(
                    Interval(
                        discontinuous.interval.start,
                        recovery_at,
                        capture,
                    )
                )
            if recovery_index is None:
                break
            baseline_index = recovery_index
        effective_samples = [sample.interval for sample in samples]
        for invalid_at in sorted(invalid_times.get(capture, ())):
            recovery = next(
                (sample.interval.start for sample in samples if sample.interval.start > invalid_at),
                end,
            )
            if invalid_at < recovery:
                invalid_outages.append(Interval(max(invalid_at, start), min(recovery, end), capture))
        for rejection_at in sorted(consecutive_rejection_times.get(capture, ())):
            recovery = next(
                (sample.interval.start for sample in samples if sample.interval.start > rejection_at),
                end,
            )
            outage_start = max(rejection_at, start)
            outage_end = min(recovery, end)
            if outage_start < outage_end:
                rejection_outages.append(Interval(outage_start, outage_end, capture))
        consecutive_rejection_outages[capture] = tuple(rejection_outages)
        result[capture] = _clip(
            _subtract(
                effective_samples,
                (
                    *outages_by_tag.get(capture, ()),
                    *invalid_outages,
                    *rejection_outages,
                    *offset_outages,
                ),
            ),
            start,
            end,
        )
    return _ClockAudit(
        intervals=result,
        legacy_samples=legacy,
        valid_samples=valid,
        invalid_samples=invalid,
        rejected_probe_samples=rejected_probes,
        hard_invalid_samples=hard_invalid,
        failure_events=failure_events,
        policy_rejections=strict_policy_rejections,
        identity_rejections=identity_rejections,
        unbound_invalid_events=unbound_invalid_events,
        in_window_invalid_events=in_window_invalid_events,
        in_window_rejected_probe_events=in_window_rejected_probe_events,
        in_window_hard_invalid_events=in_window_hard_invalid_events,
        in_window_failure_events=in_window_failure_events,
        consecutive_rejection_violations=consecutive_rejection_violations,
        consecutive_rejection_violation_captures=frozenset(consecutive_rejection_violation_captures),
        consecutive_rejection_outages=consecutive_rejection_outages,
        max_consecutive_rejected_probes=max_consecutive_rejected_probes,
        spacing_violations=spacing_violations,
        spacing_violation_captures=frozenset(spacing_violation_captures),
        offset_discontinuities=offset_discontinuities,
        offset_discontinuity_captures=frozenset(offset_discontinuity_captures),
        max_sample_gap_ms=(None if max_sample_gap_ms is None else round(max_sample_gap_ms, 3)),
        forensics=clock_forensics.as_dict(),
    )


def _gap_payload(partition: str, gap: Gap, *, boundary: bool) -> dict[str, object]:
    return {
        "partition": partition,
        "kind": gap.kind,
        "start": gap.start,
        "end": gap.end,
        "missing_count": gap.missing_count,
        "connection_id": gap.connection_id,
        "cross_segment": boundary,
    }


def _relevant_gaps(
    assets: Sequence[str],
    start: datetime,
    end: datetime,
    loaded: _LoadedLake,
) -> list[dict[str, object]]:
    """Return only gaps proven to intersect the bounded audit window.

    Manifest/cross-segment sequence gaps lack timestamps, so old same-day gaps
    cannot be attributed to a new bounded run. The filtered wire rows are
    checked independently below and are the authority for bounded sequencing.
    """

    result = [
        _gap_payload(partition, gap, boundary=boundary) for partition, gap, boundary in loaded.relevant_gaps
    ]

    # Recompute physical arrival continuity after filtering to the explicit run.
    epochs_beginning_in_window: set[tuple[str, str, int, str]] = set()
    for venue in (BINANCE, HYPERLIQUID):
        for row in _rows_with_boundaries(
            loaded,
            venue,
            RecordType.CONNECTION_EVENT,
        ):
            connection = row.get("connection_id")
            epoch = row.get("connection_epoch")
            capture = row.get("capture_epoch_id")
            if (
                int(str(row.get("schema_version", 0))) >= 2
                and row.get("event_kind") == "connect"
                and isinstance(connection, str)
                and connection
                and epoch is not None
                and isinstance(capture, str)
                and capture
            ):
                connected_at = _timestamp(
                    row["received_time"],
                    label="connect received_time",
                )
                if start <= connected_at < end:
                    epochs_beginning_in_window.add((venue, connection, int(str(epoch)), capture))
    for venue in (BINANCE, HYPERLIQUID):
        wire_previous_by_group: dict[
            tuple[str, int, str],
            tuple[int, datetime],
        ] = {}
        has_predecessor: set[tuple[str, int, str]] = set()
        seen_in_window: set[tuple[str, int, str]] = set()
        for row in loaded.store.iter_rows(
            venue,
            RecordType.WIRE_MESSAGE,
            with_boundaries=True,
            order_by="received_arrival",
        ):
            connection = row.get("connection_id")
            epoch = row.get("connection_epoch")
            capture = row.get("capture_epoch_id")
            arrival = row.get("arrival_sequence")
            if (
                isinstance(connection, str)
                and connection
                and epoch is not None
                and isinstance(capture, str)
                and capture
                and arrival is not None
            ):
                wire_group = (connection, int(str(epoch)), capture)
                wire_current = (
                    int(str(arrival)),
                    _timestamp(row["received_time"], label="wire received_time"),
                )
            else:
                continue
            if wire_current[1] < start:
                has_predecessor.add(wire_group)
            if start <= wire_current[1] < end and wire_group not in seen_in_window:
                seen_in_window.add(wire_group)
                epoch_begins_in_window = (
                    venue,
                    connection,
                    wire_group[1],
                    capture,
                ) in epochs_beginning_in_window
                if wire_group not in has_predecessor and epoch_begins_in_window and wire_current[0] != 1:
                    result.append(
                        {
                            "partition": f"venue={venue}/bounded-wire",
                            "kind": "arrival_sequence_initial",
                            "start": "0",
                            "end": str(wire_current[0]),
                            "missing_count": max(wire_current[0] - 1, 0),
                            "connection_id": connection,
                            "connection_epoch": wire_group[1],
                            "capture_epoch_id": capture,
                            "cross_segment": False,
                        }
                    )
            previous = wire_previous_by_group.get(wire_group)
            if (
                previous is not None
                and wire_group in seen_in_window
                and previous[1] < end
                and wire_current[1] >= start
                and wire_current[0] != previous[0] + 1
            ):
                result.append(
                    {
                        "partition": f"venue={venue}/bounded-wire",
                        "kind": (
                            "arrival_sequence"
                            if wire_current[0] > previous[0] + 1
                            else "arrival_sequence_regression"
                        ),
                        "start": str(previous[0]),
                        "end": str(wire_current[0]),
                        "missing_count": max(
                            wire_current[0] - previous[0] - 1,
                            0,
                        ),
                        "connection_id": connection,
                        "connection_epoch": wire_group[1],
                        "capture_epoch_id": capture,
                        "cross_segment": False,
                    }
                )
            wire_previous_by_group[wire_group] = wire_current

    for asset in assets:
        trade_previous_by_group: dict[
            tuple[str, int],
            tuple[int, datetime],
        ] = {}
        groups_with_in_window_rows: set[tuple[str, int]] = set()
        pending_gaps: dict[tuple[str, int], list[dict[str, object]]] = defaultdict(list)
        for row in loaded.store.iter_rows(
            BINANCE,
            RecordType.TRADE,
            asset,
            with_boundaries=True,
            order_by="received_source",
        ):
            connection = row.get("connection_id")
            epoch = row.get("connection_epoch")
            sequence = row.get("source_sequence")
            if isinstance(connection, str) and connection and epoch is not None and sequence is not None:
                trade_group = (connection, int(str(epoch)))
                trade_current = (
                    int(str(sequence)),
                    _timestamp(row["received_time"], label="trade received_time"),
                )
            else:
                continue
            if start <= trade_current[1] < end:
                groups_with_in_window_rows.add(trade_group)
            previous = trade_previous_by_group.get(trade_group)
            if (
                previous is not None
                and previous[1] < end
                and trade_current[1] >= start
                and trade_current[0] != previous[0] + 1
            ):
                pending_gaps[trade_group].append(
                    {
                        "partition": f"venue={BINANCE}/asset={asset}/bounded-trade",
                        "kind": (
                            "source_sequence"
                            if trade_current[0] > previous[0] + 1
                            else "source_sequence_regression"
                        ),
                        "start": str(previous[0]),
                        "end": str(trade_current[0]),
                        "missing_count": max(
                            trade_current[0] - previous[0] - 1,
                            0,
                        ),
                        "connection_id": connection,
                        "connection_epoch": trade_group[1],
                        "cross_segment": False,
                    }
                )
            trade_previous_by_group[trade_group] = trade_current
        for trade_group in sorted(groups_with_in_window_rows):
            result.extend(pending_gaps.get(trade_group, ()))
    return sorted(
        result,
        key=lambda item: (
            str(item["partition"]),
            str(item["kind"]),
            str(item["start"]),
            str(item["end"]),
            bool(item["cross_segment"]),
        ),
    )


def _interval_payload(intervals: Iterable[Interval]) -> list[dict[str, object]]:
    return [
        {
            "capture_epoch_id": item.tag,
            "start": _iso(item.start),
            "end": _iso(item.end),
            "duration_seconds": _seconds(item.end - item.start),
        }
        for item in sorted(intervals, key=lambda value: (value.start, value.end, value.tag))
    ]


def _terminal_role_diagnostic(
    loaded: _LoadedLake,
    *,
    venue: str,
    active_captures: Sequence[str],
    required_roles: frozenset[str],
    outage_audit: _EventOutageAudit,
    end: datetime,
) -> dict[str, object]:
    active = frozenset(active_captures)
    latest_connect: datetime | None = None
    latest_capture: str | None = None
    latest_ambiguous = False
    for row in _rows_with_boundaries(
        loaded,
        venue,
        RecordType.CONNECTION_EVENT,
    ):
        if row.get("event_kind") != "connect":
            continue
        capture = row.get("capture_epoch_id")
        if not isinstance(capture, str) or capture not in active:
            continue
        received = _timestamp(
            row["received_time"],
            label="terminal diagnostic connect received_time",
        )
        if received >= end:
            continue
        if latest_connect is None or received > latest_connect:
            latest_connect = received
            latest_capture = capture
            latest_ambiguous = False
        elif received == latest_connect and capture != latest_capture:
            latest_ambiguous = True

    if not active:
        selection_status = "no_active_capture_generation"
        final_capture = None
    elif latest_connect is None or latest_capture is None:
        selection_status = "no_connect_for_active_capture_generation"
        final_capture = None
    elif latest_ambiguous:
        selection_status = "ambiguous_latest_connect"
        final_capture = None
    else:
        selection_status = "selected_latest_connect"
        final_capture = latest_capture

    observed_roles = (
        frozenset()
        if final_capture is None
        else outage_audit.clean_terminal_roles.get(
            final_capture,
            frozenset(),
        )
    )
    missing_roles = required_roles - observed_roles
    complete = (
        selection_status == "selected_latest_connect"
        and observed_roles == required_roles
    )
    return {
        "selection_status": selection_status,
        "active_capture_generation_count": len(active),
        "final_capture_epoch_id": final_capture,
        "required_roles": sorted(required_roles),
        "observed_clean_terminal_roles": sorted(observed_roles),
        "missing_clean_terminal_roles": sorted(missing_roles),
        "complete": complete,
    }


def _intersect_many(interval_sets: Sequence[Iterable[Interval]]) -> tuple[Interval, ...]:
    if not interval_sets:
        return ()
    current = tuple(interval_sets[0])
    for intervals in interval_sets[1:]:
        current = _intersect(current, intervals)
    return current


def _interval_contains_event(
    interval: Interval,
    timestamps: Iterable[datetime],
) -> bool:
    return any(interval.start <= value < interval.end for value in timestamps)


def _at_or_after_resync_arm(
    received: datetime,
    arrival_sequence: int,
    arm: tuple[datetime, int],
) -> bool:
    """Order equal-time frames by physical arrival so future resync cannot arm past data."""

    return (_utc(received), arrival_sequence) >= (_utc(arm[0]), arm[1])


def _audit_phase10_continuity_impl(
    root: Path,
    *,
    assets: Sequence[str],
    start: datetime,
    end: datetime,
    state_ttl: timedelta = DEFAULT_STATE_TTL,
    loaded_holder: list[_LoadedLake],
) -> dict[str, object]:
    audit_started = time.perf_counter()
    start = _utc(start)
    end = _utc(end)
    original_assets = tuple(assets)
    normalized_assets = tuple(
        dict.fromkeys(asset.strip().upper() for asset in original_assets if asset.strip())
    )
    if not normalized_assets:
        raise ValueError("continuity audit requires at least one asset")
    if len(normalized_assets) != len(original_assets):
        raise ValueError("continuity audit assets must be non-empty and unique")
    if end <= start:
        raise ValueError("continuity audit end must be after start")
    if state_ttl <= timedelta():
        raise ValueError("continuity state TTL must be positive")
    if state_ttl > DEFAULT_STATE_TTL:
        raise ValueError("continuity state TTL cannot exceed the strict 30-second bound")

    loaded = _load_lake(root, start, end, normalized_assets)
    loaded_holder.append(loaded)
    semantic_started = time.perf_counter()
    phase_started = time.perf_counter()
    binance_lineage = _connection_lineage(
        loaded,
        BINANCE,
        frozenset({"public", "market"}),
        start,
        end,
    )
    hyperliquid_lineage = _connection_lineage(
        loaded,
        HYPERLIQUID,
        frozenset({"public"}),
        start,
        end,
    )
    loaded.metrics.observe_phase(
        "connection_lineage",
        time.perf_counter() - phase_started,
    )
    phase_started = time.perf_counter()
    binance_capture_map = _connection_capture_map(loaded, BINANCE)
    hyperliquid_capture_map = _connection_capture_map(loaded, HYPERLIQUID)
    binance_outage_audit = _event_outages(
        loaded,
        BINANCE,
        start,
        end,
        binance_capture_map,
        binance_lineage,
    )
    hyperliquid_outage_audit = _event_outages(
        loaded,
        HYPERLIQUID,
        start,
        end,
        hyperliquid_capture_map,
        hyperliquid_lineage,
    )
    loaded.metrics.observe_phase(
        "connection_events_and_outages",
        time.perf_counter() - phase_started,
    )
    market_active_captures = tuple(
        sorted(
            binance_lineage.observed_captures
            | binance_outage_audit.active_event_captures
            | {
                str(row["capture_epoch_id"])
                for row in _all_rows(
                    loaded,
                    BINANCE,
                    RecordType.WIRE_MESSAGE,
                )
                if int(str(row.get("schema_version", 0))) >= 2
                and row.get("message_asset") in normalized_assets
                and _wire_kind(row.get("channel"), row.get("raw_message")) in {"bbo", "l2", "trade"}
                and isinstance(row.get("capture_epoch_id"), str)
                and str(row["capture_epoch_id"])
            }
        )
    )
    hyperliquid_active_captures = tuple(
        sorted(
            hyperliquid_lineage.observed_captures
            | hyperliquid_outage_audit.active_event_captures
            | {
                str(row["capture_epoch_id"])
                for row in _all_rows(loaded, HYPERLIQUID, RecordType.WIRE_MESSAGE)
                if row.get("message_asset") in normalized_assets
                and _wire_kind(row.get("channel"), row.get("raw_message")) in {"bbo", "l2", "trade"}
                and isinstance(row.get("capture_epoch_id"), str)
                and str(row["capture_epoch_id"])
            }
        )
    )
    binance_role_invalid_captures = tuple(
        capture for capture in market_active_captures if capture not in binance_lineage.eligible_captures
    )
    hyperliquid_role_invalid_captures = tuple(
        capture
        for capture in hyperliquid_active_captures
        if capture not in hyperliquid_lineage.eligible_captures
    )
    binance_outages = binance_outage_audit.intervals
    hyperliquid_outages = hyperliquid_outage_audit.intervals
    phase_started = time.perf_counter()
    (
        binance_observations,
        trade_counts,
        missing_binance_resyncs,
        binance_lineage_rejections,
    ) = _binance_normalized_observations(
        loaded,
        normalized_assets,
        binance_lineage,
    )
    (
        hyperliquid_observations,
        hyperliquid_lineage_rejections,
    ) = _hyperliquid_observations(
        loaded,
        normalized_assets,
        hyperliquid_lineage,
    )
    loaded.metrics.observe_phase(
        "raw_normalized_lineage",
        time.perf_counter() - phase_started,
    )
    phase_started = time.perf_counter()
    binance_orphan_required_wire = _orphan_required_wire_counts(
        loaded,
        BINANCE,
        normalized_assets,
        binance_lineage,
    )
    hyperliquid_orphan_required_wire = _orphan_required_wire_counts(
        loaded,
        HYPERLIQUID,
        normalized_assets,
        hyperliquid_lineage,
    )
    orphan_required_wire_total = (
        binance_orphan_required_wire.total
        + hyperliquid_orphan_required_wire.total
    )
    binance_orphan_l2_levels = _orphan_normalized_l2_level_counts(
        loaded,
        BINANCE,
        normalized_assets,
        binance_lineage,
    )
    hyperliquid_orphan_l2_levels = _orphan_normalized_l2_level_counts(
        loaded,
        HYPERLIQUID,
        normalized_assets,
        hyperliquid_lineage,
    )
    orphan_normalized_l2_level_total = sum(binance_orphan_l2_levels.values()) + sum(
        hyperliquid_orphan_l2_levels.values()
    )
    loaded.metrics.observe_phase(
        "orphan_lineage",
        time.perf_counter() - phase_started,
    )
    phase_started = time.perf_counter()
    binance_market = _required_market_intervals(
        binance_observations,
        normalized_assets,
        state_ttl,
        start,
        end,
        binance_outages,
    )
    hyperliquid_market = _required_market_intervals(
        hyperliquid_observations,
        normalized_assets,
        state_ttl,
        start,
        end,
        hyperliquid_outages,
    )
    loaded.metrics.observe_phase(
        "market_coverage_intervals",
        time.perf_counter() - phase_started,
    )
    phase_started = time.perf_counter()
    clock_audit = _clock_intervals(
        loaded,
        start,
        end,
        binance_outages,
        binance_lineage,
        frozenset(market_active_captures),
    )
    loaded.metrics.observe_phase(
        "clock_causal_coverage",
        time.perf_counter() - phase_started,
    )
    clock_by_capture = clock_audit.intervals

    market_ready_by_capture = {
        capture: _intersect_many(
            [binance_market.get(capture, {}).get(asset, ()) for asset in normalized_assets]
        )
        for capture in sorted(binance_market)
    }
    market_ready_by_capture = {
        capture: intervals for capture, intervals in market_ready_by_capture.items() if intervals
    }
    captures_without_valid_clock = tuple(
        capture for capture in market_active_captures if not clock_by_capture.get(capture)
    )
    binance_market_incomplete_captures = tuple(
        capture
        for capture in market_active_captures
        if not all(binance_market.get(capture, {}).get(asset) for asset in normalized_assets)
    )
    hyperliquid_market_incomplete_captures = tuple(
        capture
        for capture in hyperliquid_active_captures
        if not all(hyperliquid_market.get(capture, {}).get(asset) for asset in normalized_assets)
    )
    multiple_hyperliquid_active_captures = len(hyperliquid_active_captures) > 1
    active_capture_set = set(market_active_captures)
    relevant_spacing_captures = tuple(
        sorted(active_capture_set & set(clock_audit.spacing_violation_captures))
    )
    relevant_offset_discontinuity_captures = tuple(
        sorted(active_capture_set & set(clock_audit.offset_discontinuity_captures))
    )

    phase_started = time.perf_counter()
    eligible_captures = tuple(
        capture
        for capture in sorted(clock_by_capture)
        if clock_by_capture[capture]
        and all(binance_market.get(capture, {}).get(asset) for asset in normalized_assets)
    )
    strict_by_asset: dict[str, list[Interval]] = defaultdict(list)
    strict_all: list[Interval] = []
    for binance_capture in eligible_captures:
        for hyperliquid_capture in sorted(hyperliquid_market):
            capture_assets: list[tuple[Interval, ...]] = []
            for asset in normalized_assets:
                binance_with_clock = _intersect(
                    binance_market[binance_capture][asset],
                    clock_by_capture[binance_capture],
                    require_same_tag=True,
                )
                strict_asset = _intersect(
                    binance_with_clock,
                    hyperliquid_market[hyperliquid_capture].get(asset, ()),
                )
                tag = f"{binance_capture}|{hyperliquid_capture}"
                retagged = tuple(Interval(item.start, item.end, tag) for item in strict_asset)
                trade_qualified = tuple(
                    item
                    for item in retagged
                    if _interval_contains_event(
                        item,
                        binance_observations[binance_capture][asset].get(
                            "trade",
                            (),
                        ),
                    )
                    and _interval_contains_event(
                        item,
                        hyperliquid_observations[hyperliquid_capture][asset].get(
                            "trade",
                            (),
                        ),
                    )
                )
                strict_by_asset[asset].extend(trade_qualified)
                capture_assets.append(trade_qualified)
            for candidate in _intersect_many(capture_assets):
                if all(
                    _interval_contains_event(
                        candidate,
                        observations[capture][asset].get("trade", ()),
                    )
                    for observations, capture in (
                        (binance_observations, binance_capture),
                        (hyperliquid_observations, hyperliquid_capture),
                    )
                    for asset in normalized_assets
                ):
                    strict_all.append(candidate)
    loaded.metrics.observe_phase(
        "strict_overlap",
        time.perf_counter() - phase_started,
    )

    phase_started = time.perf_counter()
    relevant_gaps = _relevant_gaps(normalized_assets, start, end, loaded)
    loaded.metrics.observe_phase(
        "bounded_gap_validation",
        time.perf_counter() - phase_started,
    )
    if (
        relevant_gaps
        or missing_binance_resyncs
        or binance_role_invalid_captures
        or hyperliquid_role_invalid_captures
        or binance_lineage_rejections
        or hyperliquid_lineage_rejections
        or clock_audit.unbound_invalid_events
        or captures_without_valid_clock
        or relevant_spacing_captures
        or relevant_offset_discontinuity_captures
        or binance_market_incomplete_captures
        or hyperliquid_market_incomplete_captures
        or multiple_hyperliquid_active_captures
        or binance_lineage.unbound_connect_events
        or hyperliquid_lineage.unbound_connect_events
        or orphan_required_wire_total
        or orphan_normalized_l2_level_total
        or binance_outage_audit.unbound_fail_closed_events
        or hyperliquid_outage_audit.unbound_fail_closed_events
        or binance_outage_audit.unbound_resync_events
        or hyperliquid_outage_audit.unbound_resync_events
        or binance_outage_audit.in_window_gap_events
        or hyperliquid_outage_audit.in_window_gap_events
        or binance_outage_audit.unclean_in_window_disconnect_events
        or hyperliquid_outage_audit.unclean_in_window_disconnect_events
        or clock_audit.in_window_hard_invalid_events
        or clock_audit.in_window_failure_events
    ):
        strict_all = []
        strict_by_asset = defaultdict(list)

    assessed_by_capture: dict[str, tuple[datetime, datetime]] = {}
    market_ready_at_by_capture: dict[str, datetime] = {}
    initial_clock_delay_ms_by_capture: dict[str, float | None] = {}
    initial_clock_delay_violations: list[str] = []
    for capture, intervals in market_ready_by_capture.items():
        market_ready_start = min(item.start for item in intervals)
        connect_times = [
            connected_at
            for (_, _, identity_capture), (_, connected_at) in (*binance_lineage.identities.items(),)
            if identity_capture == capture
        ]
        readiness = max((market_ready_start, *connect_times))
        market_ready_at_by_capture[capture] = readiness
        assessed_end = min(end, max(item.end for item in intervals))
        usable_clock = next(
            (
                interval
                for interval in sorted(
                    clock_by_capture.get(capture, ()),
                    key=lambda item: item.start,
                )
                if interval.end > readiness
            ),
            None,
        )
        if usable_clock is None:
            assessed_start = readiness
            initial_clock_delay_ms_by_capture[capture] = None
        else:
            assessed_start = max(readiness, usable_clock.start)
            acquisition_delay = assessed_start - readiness
            delay_ms = acquisition_delay.total_seconds() * 1_000
            initial_clock_delay_ms_by_capture[capture] = round(delay_ms, 3)
            if delay_ms > MAX_CLOCK_AGE_MS:
                initial_clock_delay_violations.append(capture)
        if assessed_start < assessed_end:
            assessed_by_capture[capture] = (
                assessed_start,
                assessed_end,
            )

    relevant_consecutive_rejection_captures = tuple(
        sorted(
            capture
            for capture, (assessed_start, assessed_end) in assessed_by_capture.items()
            if any(
                outage.start < assessed_end and outage.end > assessed_start
                for outage in clock_audit.consecutive_rejection_outages.get(
                    capture,
                    (),
                )
            )
        )
    )

    assessed_span: tuple[datetime, datetime] | None = None
    internal_gap_count = 0
    uncovered = timedelta()
    causal_coverage_continuous = False
    clock_continuous = False
    if assessed_by_capture:
        assessed_start = min(value[0] for value in assessed_by_capture.values())
        assessed_end = max(value[1] for value in assessed_by_capture.values())
        assessed_span = (assessed_start, assessed_end)
        coverage, internal_gap_count, uncovered = _covered_without_gaps(
            (interval for capture in assessed_by_capture for interval in clock_by_capture.get(capture, ())),
            assessed_start,
            assessed_end,
        )
        causal_coverage_continuous = (
            coverage and len(assessed_by_capture) == 1 and not captures_without_valid_clock
        )
        clock_continuous = (
            coverage
            and len(assessed_by_capture) == 1
            and not relevant_gaps
            and not captures_without_valid_clock
            and not binance_role_invalid_captures
            and not hyperliquid_role_invalid_captures
            and binance_lineage_rejections == 0
            and hyperliquid_lineage_rejections == 0
            and not binance_market_incomplete_captures
            and not hyperliquid_market_incomplete_captures
            and not multiple_hyperliquid_active_captures
            and not relevant_spacing_captures
            and not relevant_consecutive_rejection_captures
            and not relevant_offset_discontinuity_captures
            and clock_audit.unbound_invalid_events == 0
            and not initial_clock_delay_violations
            and binance_lineage.unbound_connect_events == 0
            and hyperliquid_lineage.unbound_connect_events == 0
            and orphan_required_wire_total == 0
            and orphan_normalized_l2_level_total == 0
            and binance_outage_audit.unbound_fail_closed_events == 0
            and hyperliquid_outage_audit.unbound_fail_closed_events == 0
            and binance_outage_audit.unbound_resync_events == 0
            and hyperliquid_outage_audit.unbound_resync_events == 0
            and binance_outage_audit.in_window_gap_events == 0
            and hyperliquid_outage_audit.in_window_gap_events == 0
            and binance_outage_audit.unclean_in_window_disconnect_events == 0
            and hyperliquid_outage_audit.unclean_in_window_disconnect_events == 0
            and clock_audit.in_window_hard_invalid_events == 0
            and clock_audit.in_window_failure_events == 0
        )

    generation_gap_count = max(len(assessed_by_capture) - 1, 0)
    leading_gap = timedelta() if assessed_span is None else max(assessed_span[0] - start, timedelta())
    trailing_gap = end - start if assessed_span is None else max(end - assessed_span[1], timedelta())
    requested_margin_limit = timedelta(milliseconds=MAX_CLOCK_AGE_MS)
    leading_margin_exceeded = leading_gap > requested_margin_limit
    trailing_margin_exceeded = trailing_gap > requested_margin_limit
    terminal_role_diagnostics = {
        BINANCE: _terminal_role_diagnostic(
            loaded,
            venue=BINANCE,
            active_captures=market_active_captures,
            required_roles=frozenset({"public", "market"}),
            outage_audit=binance_outage_audit,
            end=end,
        ),
        HYPERLIQUID: _terminal_role_diagnostic(
            loaded,
            venue=HYPERLIQUID,
            active_captures=hyperliquid_active_captures,
            required_roles=frozenset({"public"}),
            outage_audit=hyperliquid_outage_audit,
            end=end,
        ),
    }
    trailing_terminal_roles_complete = assessed_span is not None and (
        trailing_gap == timedelta()
        or (
            terminal_role_diagnostics[BINANCE]["complete"] is True
            and terminal_role_diagnostics[HYPERLIQUID]["complete"] is True
        )
    )
    trailing_terminal_incomplete = trailing_gap > timedelta() and not trailing_terminal_roles_complete
    if leading_margin_exceeded or trailing_margin_exceeded or trailing_terminal_incomplete:
        clock_continuous = False
        strict_all = []
        strict_by_asset = defaultdict(list)
    strict_intervals = _merge(strict_all)
    strict_duration = _duration(strict_intervals)

    reasons: list[str] = []
    if not PHASE_10_ASSETS.issubset(normalized_assets):
        reasons.append("required_phase10_assets_missing")
    for asset in normalized_assets:
        counts = trade_counts[asset]
        if counts["normalized_count"] == 0:
            reasons.append(f"binance_normalized_trades_missing:{asset}")
        if counts["raw_agg_trade_count"] == 0:
            reasons.append(f"binance_raw_agg_trade_missing:{asset}")
        if counts["raw_agg_trade_with_role_lineage_count"] == 0:
            reasons.append(f"binance_raw_agg_trade_role_lineage_missing:{asset}")
        if counts["normalized_with_raw_lineage_count"] == 0:
            reasons.append(f"binance_trade_lineage_missing:{asset}")
    if binance_lineage_rejections:
        reasons.append("binance_market_raw_lineage_rejected")
    if hyperliquid_lineage_rejections:
        reasons.append("hyperliquid_market_raw_lineage_rejected")
    if binance_lineage.unbound_connect_events:
        reasons.append("binance_connection_event_unbound")
    if hyperliquid_lineage.unbound_connect_events:
        reasons.append("hyperliquid_connection_event_unbound")
    if binance_outage_audit.unbound_fail_closed_events:
        reasons.append("binance_gap_or_disconnect_unbound")
    if hyperliquid_outage_audit.unbound_fail_closed_events:
        reasons.append("hyperliquid_gap_or_disconnect_unbound")
    if binance_outage_audit.unbound_resync_events or hyperliquid_outage_audit.unbound_resync_events:
        reasons.append("resync_event_unbound")
    if orphan_required_wire_total:
        reasons.append("required_raw_wire_without_exact_normalization")
    if orphan_normalized_l2_level_total:
        reasons.append("normalized_l2_level_without_exact_raw_header_lineage")
    if binance_outage_audit.in_window_gap_events or hyperliquid_outage_audit.in_window_gap_events:
        reasons.append("in_window_capture_gap_event")
    if (
        binance_outage_audit.unclean_in_window_disconnect_events
        or hyperliquid_outage_audit.unclean_in_window_disconnect_events
    ):
        reasons.append("in_window_disconnect_without_clean_stop_reason")
    reasons.extend(
        f"binance_l2_resync_missing:{asset}:{capture}" for capture, asset in missing_binance_resyncs
    )
    if multiple_hyperliquid_active_captures:
        reasons.append("hyperliquid_multiple_active_capture_generations")
    reasons.extend(
        f"binance_market_capture_incomplete:{capture}" for capture in binance_market_incomplete_captures
    )
    reasons.extend(
        f"hyperliquid_market_capture_incomplete:{capture}"
        for capture in hyperliquid_market_incomplete_captures
    )
    reasons.extend(
        f"clock_sync_missing_valid_for_market_capture:{capture}" for capture in captures_without_valid_clock
    )
    reasons.extend(
        f"binance_connection_role_lineage_invalid:{capture}" for capture in binance_role_invalid_captures
    )
    reasons.extend(
        f"hyperliquid_connection_role_lineage_invalid:{capture}"
        for capture in hyperliquid_role_invalid_captures
    )
    reasons.extend(
        f"clock_sync_initial_acquisition_delay_exceeded:{capture}"
        for capture in initial_clock_delay_violations
    )
    if relevant_spacing_captures:
        reasons.append("clock_sync_sample_spacing_exceeded")
    if relevant_consecutive_rejection_captures:
        reasons.append("clock_sync_consecutive_rejected_probes")
    if relevant_offset_discontinuity_captures:
        reasons.append("clock_sync_offset_discontinuity")
    if clock_audit.unbound_invalid_events:
        reasons.append("clock_sync_invalid_event_unbound")
    if clock_audit.in_window_hard_invalid_events:
        reasons.append("clock_sync_in_window_invalid_sample")
    if clock_audit.in_window_failure_events:
        reasons.append("clock_sync_in_window_failure_event")
    if not clock_continuous:
        reasons.append("clock_sync_not_continuous")
    if leading_margin_exceeded:
        reasons.append("requested_window_leading_margin_exceeded")
    if trailing_margin_exceeded:
        reasons.append("requested_window_trailing_margin_exceeded")
    if trailing_terminal_incomplete:
        reasons.append("requested_window_trailing_clean_stop_incomplete")
    if relevant_gaps:
        reasons.append("bounded_lake_gaps_present")
    if strict_duration <= timedelta():
        reasons.append("strict_phase10_overlap_zero")
    reasons = sorted(set(reasons))

    strict_by_asset_payload = {}
    for asset in normalized_assets:
        intervals = _merge(strict_by_asset.get(asset, ()))
        strict_by_asset_payload[asset] = {
            "interval_count": len(intervals),
            "duration_seconds": _seconds(_duration(intervals)),
        }
    clock_intervals = tuple(
        interval for capture in sorted(clock_by_capture) for interval in clock_by_capture[capture]
    )
    payload: dict[str, object] = {
        "audit_version": 1,
        "phase_10_status": PHASE_10_STATUS,
        "technical_capture_gate": "PASS" if not reasons else "FAIL",
        "assets": list(normalized_assets),
        "requested_window": {
            "start": _iso(start),
            "end": _iso(end),
            "duration_seconds": _seconds(end - start),
            "leading_unassessed_seconds": _seconds(leading_gap),
            "trailing_unassessed_seconds": _seconds(trailing_gap),
            "max_unassessed_margin_ms": MAX_CLOCK_AGE_MS,
            "leading_margin_within_limit": not leading_margin_exceeded,
            "trailing_margin_within_limit": not trailing_margin_exceeded,
            "trailing_terminal_roles_complete": (trailing_terminal_roles_complete),
            "terminal_role_diagnostics": terminal_role_diagnostics,
        },
        "policy": {
            "interval_semantics": "half_open_received_time_causal",
            "state_ttl_ms": int(state_ttl.total_seconds() * 1_000),
            "trade_semantics": "point_event_causal_freshness_no_interpolation",
            "trade_freshness_ms": int(state_ttl.total_seconds() * 1_000),
            "binance_l2_requires_v2_resync_complete": True,
            "clock_legacy_v1_usable": False,
            "clock_max_sampling_interval_ms": MAX_CLOCK_SAMPLING_INTERVAL_MS,
            "clock_max_age_ms": MAX_CLOCK_AGE_MS,
            "clock_max_uncertainty_ms": float(MAX_CLOCK_UNCERTAINTY_MS),
            "clock_actual_sample_spacing_enforced": True,
            "clock_sample_spacing_population": ("all_persisted_identity_bound_v2_clock_sync_attempts"),
            "clock_sample_spacing_timestamp": "request_sent_time",
            "clock_sample_spacing_bounds": ("active_generation_clipped_to_requested_window"),
            "clock_identity_requires_v2_wire_lineage": True,
            "clock_offset_uncertainty_bands_must_overlap": True,
            "physical_connection_roles_required": {
                BINANCE: ["market", "public"],
                HYPERLIQUID: ["public"],
            },
            "market_lineage_requires_exact_raw_payload": True,
            "assessed_span_starts_at_market_readiness_or_initial_clock": True,
            "initial_clock_acquisition_max_delay_ms": MAX_CLOCK_AGE_MS,
            "interpolate_across_capture_generations": False,
            "phase_10_may_be_unblocked_by_this_audit": False,
        },
        "binance_trades": {
            "normalized_total": sum(value["normalized_count"] for value in trade_counts.values()),
            "normalized_with_raw_lineage_total": sum(
                value["normalized_with_raw_lineage_count"] for value in trade_counts.values()
            ),
            "raw_agg_trade_total": sum(value["raw_agg_trade_count"] for value in trade_counts.values()),
            "raw_agg_trade_with_role_lineage_total": sum(
                value["raw_agg_trade_with_role_lineage_count"] for value in trade_counts.values()
            ),
            "by_asset": trade_counts,
        },
        "connection_lineage": {
            BINANCE: {
                "eligible_capture_generations": sorted(binance_lineage.eligible_captures),
                "market_active_invalid_capture_generations": list(binance_role_invalid_captures),
                "incomplete_capture_generations": list(binance_market_incomplete_captures),
                "ambiguous_or_wrong_role_connect_identities": (binance_lineage.rejected_identity_count),
                "unbound_connect_events": binance_lineage.unbound_connect_events,
                "normalized_market_lineage_rejections": (binance_lineage_rejections),
            },
            HYPERLIQUID: {
                "eligible_capture_generations": sorted(hyperliquid_lineage.eligible_captures),
                "market_active_invalid_capture_generations": list(hyperliquid_role_invalid_captures),
                "incomplete_capture_generations": list(hyperliquid_market_incomplete_captures),
                "multiple_active_capture_generations": (multiple_hyperliquid_active_captures),
                "ambiguous_or_wrong_role_connect_identities": (hyperliquid_lineage.rejected_identity_count),
                "unbound_connect_events": hyperliquid_lineage.unbound_connect_events,
                "normalized_market_lineage_rejections": (hyperliquid_lineage_rejections),
            },
        },
        "connection_events": {
            BINANCE: {
                "unbound_gap_or_disconnect_events": (binance_outage_audit.unbound_fail_closed_events),
                "unbound_resync_events": (binance_outage_audit.unbound_resync_events),
                "in_window_gap_events": (binance_outage_audit.in_window_gap_events),
                "unclean_in_window_disconnect_events": (
                    binance_outage_audit.unclean_in_window_disconnect_events
                ),
                "event_active_capture_generations": sorted(binance_outage_audit.active_event_captures),
                "failure_events_by_capture_generation": (binance_outage_audit.failure_events_by_capture),
            },
            HYPERLIQUID: {
                "unbound_gap_or_disconnect_events": (hyperliquid_outage_audit.unbound_fail_closed_events),
                "unbound_resync_events": (hyperliquid_outage_audit.unbound_resync_events),
                "in_window_gap_events": (hyperliquid_outage_audit.in_window_gap_events),
                "unclean_in_window_disconnect_events": (
                    hyperliquid_outage_audit.unclean_in_window_disconnect_events
                ),
                "event_active_capture_generations": sorted(hyperliquid_outage_audit.active_event_captures),
                "failure_events_by_capture_generation": (hyperliquid_outage_audit.failure_events_by_capture),
            },
        },
        "required_wire_lineage": {
            "orphan_required_wire_total": orphan_required_wire_total,
            "by_venue_asset": {
                BINANCE: dict(binance_orphan_required_wire.counts),
                HYPERLIQUID: dict(hyperliquid_orphan_required_wire.counts),
            },
            "forensics": _required_wire_forensics(
                {
                    BINANCE: binance_orphan_required_wire,
                    HYPERLIQUID: hyperliquid_orphan_required_wire,
                }
            ),
        },
        "normalized_l2_level_lineage": {
            "orphan_level_total": orphan_normalized_l2_level_total,
            "by_venue_asset": {
                BINANCE: binance_orphan_l2_levels,
                HYPERLIQUID: hyperliquid_orphan_l2_levels,
            },
        },
        "binance_l2_resync": {
            "missing_count": len(missing_binance_resyncs),
            "missing": [
                {"capture_epoch_id": capture, "asset": asset} for capture, asset in missing_binance_resyncs
            ],
        },
        "clock_sync": {
            "forensics": clock_audit.forensics,
            "legacy_v1_ignored": clock_audit.legacy_samples,
            "valid_v2_samples": clock_audit.valid_samples,
            "invalid_v2_samples": clock_audit.invalid_samples,
            "rejected_probe_samples": clock_audit.rejected_probe_samples,
            "hard_invalid_v2_samples": clock_audit.hard_invalid_samples,
            "failure_events": clock_audit.failure_events,
            "strict_policy_rejections": clock_audit.policy_rejections,
            "wire_identity_rejections": clock_audit.identity_rejections,
            "unbound_invalid_events": clock_audit.unbound_invalid_events,
            "in_window_invalid_events": clock_audit.in_window_invalid_events,
            "in_window_rejected_probe_events": (clock_audit.in_window_rejected_probe_events),
            "in_window_hard_invalid_events": (clock_audit.in_window_hard_invalid_events),
            "in_window_failure_events": clock_audit.in_window_failure_events,
            "consecutive_rejection_violations": (clock_audit.consecutive_rejection_violations),
            "consecutive_rejection_violation_capture_generations": list(
                relevant_consecutive_rejection_captures
            ),
            "consecutive_rejection_outages": _interval_payload(
                tuple(
                    outage
                    for capture in sorted(clock_audit.consecutive_rejection_outages)
                    for outage in clock_audit.consecutive_rejection_outages[capture]
                )
            ),
            "max_consecutive_rejected_probes": (clock_audit.max_consecutive_rejected_probes),
            "strict_max_consecutive_rejected_probes": (MAX_CONSECUTIVE_REJECTED_CLOCK_PROBES),
            "sample_spacing_violations": clock_audit.spacing_violations,
            "sample_spacing_violation_capture_generations": list(relevant_spacing_captures),
            "sample_spacing_population": ("all_persisted_identity_bound_v2_clock_sync_attempts"),
            "sample_spacing_timestamp": "request_sent_time",
            "sample_spacing_bounds": ("active_generation_clipped_to_requested_window"),
            "offset_discontinuities": clock_audit.offset_discontinuities,
            "offset_discontinuity_capture_generations": list(relevant_offset_discontinuity_captures),
            "actual_max_sample_gap_ms": clock_audit.max_sample_gap_ms,
            "actual_max_cadence_gap_ms": clock_audit.max_sample_gap_ms,
            "strict_max_sampling_interval_ms": MAX_CLOCK_SAMPLING_INTERVAL_MS,
            "strict_max_age_ms": MAX_CLOCK_AGE_MS,
            "strict_max_uncertainty_ms": float(MAX_CLOCK_UNCERTAINTY_MS),
            "eligible_capture_generations": list(eligible_captures),
            "market_active_capture_generations": list(market_active_captures),
            "market_active_without_valid_clock": list(captures_without_valid_clock),
            "assessed_capture_generations": sorted(assessed_by_capture),
            "market_ready_at_by_capture": {
                capture: _iso(value) for capture, value in sorted(market_ready_at_by_capture.items())
            },
            "initial_acquisition_delay_ms_by_capture": {
                capture: value for capture, value in sorted(initial_clock_delay_ms_by_capture.items())
            },
            "initial_acquisition_delay_violations": sorted(initial_clock_delay_violations),
            "assessed_span": (
                None
                if assessed_span is None
                else {"start": _iso(assessed_span[0]), "end": _iso(assessed_span[1])}
            ),
            "causal_coverage_continuous": causal_coverage_continuous,
            "coverage_continuous": clock_continuous,
            "valid_duration_seconds": _seconds(_duration(clock_intervals)),
            "uncovered_seconds": _seconds(uncovered),
            "internal_gap_count": internal_gap_count,
            "generation_gap_count": generation_gap_count,
            "requested_window_leading_gap_seconds": _seconds(leading_gap),
            "requested_window_trailing_gap_seconds": _seconds(trailing_gap),
            "intervals": _interval_payload(clock_intervals),
        },
        "strict_phase_10_overlap": {
            "duration_seconds": _seconds(strict_duration),
            "interval_count": len(strict_intervals),
            "by_asset": strict_by_asset_payload,
            "intervals": _interval_payload(strict_intervals),
        },
        "validation": {
            "inventory_partition_count": loaded.inventory_partition_count,
            "inventory_row_count": loaded.inventory.total_rows,
            "relevant_gap_count": len(relevant_gaps),
            "relevant_gaps": relevant_gaps,
        },
        "failure_reasons": reasons,
    }
    loaded.metrics.observe_phase(
        "semantic_validation",
        time.perf_counter() - semantic_started,
    )
    loaded.store.observe_scratch_size()
    loaded.metrics.observe_phase(
        "total",
        time.perf_counter() - audit_started,
    )
    payload["observability"] = loaded.metrics.as_dict()
    loaded.store.close()
    return payload


def audit_phase10_continuity(
    root: Path,
    *,
    assets: Sequence[str],
    start: datetime,
    end: datetime,
    state_ttl: timedelta = DEFAULT_STATE_TTL,
) -> dict[str, object]:
    """Audit technical capture readiness while Phase 10 remains blocked."""

    loaded_holder: list[_LoadedLake] = []
    try:
        return _audit_phase10_continuity_impl(
            root,
            assets=assets,
            start=start,
            end=end,
            state_ttl=state_ttl,
            loaded_holder=loaded_holder,
        )
    finally:
        for loaded in loaded_holder:
            loaded.store.close()
