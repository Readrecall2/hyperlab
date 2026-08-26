"""Fact-derived Golden shape for synthetic Storage V4 capacity workloads.

The result is a workload *shape*, not a copy of market data.  It reads only an
already verified Golden export, records exact source counts and canonical JSON
payload-size bounds, and emits a visibly synthetic capacity configuration.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Callable, Iterable, Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter_ns
from typing import cast

from hyperlab.backtest.protocol import canonical_json
from hyperlab.paper.golden_v3 import GOLDEN_STREAM_NAMES, GoldenVerification, iter_golden_stream

from .canonical import canonical_json_bytes
from .capacity import (
    CAPACITY_MARKERS,
    MAX_SYNTHETIC_PAYLOAD_BYTES,
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
)

GOLDEN_SHAPE_FORMAT = "hyperlab-storage-v4-golden-capacity-shape-v1"
GOLDEN_PAYLOAD_SIZE_BASIS = "GOLDEN_V3_INBOX_PAYLOAD_CANONICAL_JSON_UTF8_BYTES"
GOLDEN_ACTIVITY_SIZE_BASIS = "GOLDEN_V3_LOGICAL_ROW_CANONICAL_JSONL_UTF8_BYTES"

GoldenShapeStreamFactory = Callable[
    [GoldenVerification, str], Iterable[Mapping[str, object]]
]
GoldenShapeProgressCallback = Callable[[Mapping[str, object]], None]

_CARDINALITY_COMMIT_INTERVAL = 4_096
_CARDINALITY_CACHE_KIB = 2_048
_PROGRESS_ROW_INTERVAL = 4_096
_GOLDEN_SHAPE_PROGRESS_WORKLOAD = "GOLDEN_V3_CAPACITY_SHAPE_SCAN"
_GOLDEN_SHAPE_PROGRESS_SCOPE = (
    "AUTHENTICATED_GOLDEN_DESCRIPTOR_ROWS_FOR_SCANNED_STREAMS_ONLY"
)
_GOLDEN_SHAPE_SCANNED_STREAMS = (
    "inbox",
    "alerts",
    "incidents",
    "ledger_transactions",
)
_GOLDEN_SHAPE_SEGMENT_CHECKPOINT_STATUS = (
    "EXACT_ZERO_NOT_APPLICABLE_READ_ONLY_GOLDEN_CENSUS"
)


class GoldenCapacityShapeError(ValueError):
    """The verified Golden metadata cannot define an exact synthetic shape."""


def _golden_canonical_json_bytes(value: object, *, label: str) -> bytes:
    """Encode with the Golden V3 JSON contract, including finite JSON floats."""

    try:
        return canonical_json(value).encode("utf-8", errors="strict")
    except (RecursionError, TypeError, UnicodeEncodeError, ValueError) as error:
        raise GoldenCapacityShapeError(
            f"{label} is not valid Golden V3 canonical JSON"
        ) from error


def _require_sha256(value: object, *, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise GoldenCapacityShapeError(f"{label} must be a lowercase SHA-256")
    return value


def _require_non_negative(value: object, *, label: str) -> int:
    if type(value) is not int or value < 0:
        raise GoldenCapacityShapeError(f"{label} must be a non-negative integer")
    return value


def _epoch_ns(value: object) -> int:
    if type(value) is not str or not value:
        raise GoldenCapacityShapeError("Golden inbox created_at is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise GoldenCapacityShapeError("Golden inbox created_at is not ISO-8601") from error
    if parsed.tzinfo is None:
        raise GoldenCapacityShapeError("Golden inbox created_at is timezone-naive")
    delta = parsed.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _average_size(total: int, count: int) -> int:
    if count == 0:
        return 0
    return max(1, (total + (count // 2)) // count)


def _interval(commit_count: int, activity_count: int) -> int | None:
    if activity_count == 0:
        return None
    return max(1, (commit_count + (activity_count // 2)) // activity_count)


def _scaled_count(source_count: int, source_commits: int, target_commits: int) -> int:
    if source_count == 0:
        return 0
    scaled = (target_commits * source_count + (source_commits // 2)) // source_commits
    return min(target_commits, max(1, scaled))


@dataclass(slots=True)
class _SizeAccumulator:
    count: int = 0
    total: int = 0

    def add(self, value: int) -> None:
        self.count += 1
        self.total += value

    @property
    def average(self) -> int:
        return _average_size(self.total, self.count)


@dataclass(slots=True)
class _GoldenShapeProgress:
    callback: GoldenShapeProgressCallback | None
    golden_root: str
    commits_total: int
    stream_row_counts: Mapping[str, int]
    census_started_ns: int
    prior_elapsed_ns: int = 0

    @property
    def logical_rows_total(self) -> int:
        return sum(
            self.stream_row_counts[stream]
            for stream in _GOLDEN_SHAPE_SCANNED_STREAMS
        )

    def emit(
        self,
        *,
        stream: str,
        rows_completed: int,
        status: str,
    ) -> None:
        if stream not in _GOLDEN_SHAPE_SCANNED_STREAMS:
            raise GoldenCapacityShapeError(
                f"Golden shape progress stream {stream!r} is not scanned"
            )
        rows_total = self.stream_row_counts[stream]
        if not 0 <= rows_completed <= rows_total:
            raise GoldenCapacityShapeError(
                f"Golden {stream} rows exceed authenticated descriptor count"
            )
        if self.callback is None:
            return
        stream_index = _GOLDEN_SHAPE_SCANNED_STREAMS.index(stream)
        logical_row_offset = sum(
            self.stream_row_counts[name]
            for name in _GOLDEN_SHAPE_SCANNED_STREAMS[:stream_index]
        )
        elapsed_ns = perf_counter_ns() - self.census_started_ns
        if elapsed_ns < self.prior_elapsed_ns:
            raise GoldenCapacityShapeError("Golden shape progress clock regressed")
        self.prior_elapsed_ns = elapsed_ns
        self.callback(
            {
                "phase": "golden_capacity_shape_scan",
                "status": status,
                "workload": _GOLDEN_SHAPE_PROGRESS_WORKLOAD,
                "workload_profile": CapacityProfile.GOLDEN_SHAPED.value,
                "workload_id": f"golden-shape:{self.golden_root}",
                "golden_root_hash": self.golden_root,
                "commits_completed": (
                    rows_completed if stream == "inbox" else self.commits_total
                ),
                "commits_total": self.commits_total,
                "logical_rows_completed": logical_row_offset + rows_completed,
                "logical_rows_total": self.logical_rows_total,
                "elapsed_ns": elapsed_ns,
                "workload_elapsed_ns": elapsed_ns,
                "raw_segment_count": 0,
                "paper_segment_count": 0,
                "segment_count": 0,
                "checkpoint_count": 0,
                "segment_checkpoint_status": (
                    _GOLDEN_SHAPE_SEGMENT_CHECKPOINT_STATUS
                ),
                "progress_metrics_scope": _GOLDEN_SHAPE_PROGRESS_SCOPE,
                "stream": stream,
                "rows_completed": rows_completed,
                "rows_total": rows_total,
                "rows_completed_scope": "CURRENT_STREAM_ONLY",
                "logical_row_offset": logical_row_offset,
            }
        )


@dataclass(frozen=True, slots=True)
class GoldenCapacityTypeObservation:
    record_type: str
    count: int
    payload_min_bytes: int
    payload_max_bytes: int
    distinct_payload_hashes: int

    def __post_init__(self) -> None:
        if type(self.record_type) is not str or not self.record_type:
            raise GoldenCapacityShapeError("Golden capacity record_type is invalid")
        if type(self.count) is not int or self.count < 1:
            raise GoldenCapacityShapeError("Golden capacity type count must be positive")
        if (
            type(self.payload_min_bytes) is not int
            or type(self.payload_max_bytes) is not int
            or not 0 <= self.payload_min_bytes <= self.payload_max_bytes
            or self.payload_max_bytes > MAX_SYNTHETIC_PAYLOAD_BYTES
        ):
            raise GoldenCapacityShapeError("Golden capacity payload bounds are invalid")
        if (
            type(self.distinct_payload_hashes) is not int
            or not 1 <= self.distinct_payload_hashes <= self.count
        ):
            raise GoldenCapacityShapeError("Golden capacity cardinality is invalid")

    def payload(self) -> dict[str, object]:
        return {
            "count": self.count,
            "distinct_payload_hashes": self.distinct_payload_hashes,
            "payload_max_bytes": self.payload_max_bytes,
            "payload_min_bytes": self.payload_min_bytes,
            "record_type": self.record_type,
        }


@dataclass(frozen=True, slots=True)
class GoldenCapacityShape:
    golden_root: str
    source_sha256: str
    commit_count: int
    logical_row_count: int
    start_time_ns: int
    end_time_ns: int
    cadence_ns: int
    type_observations: tuple[GoldenCapacityTypeObservation, ...]
    strategies: tuple[str, ...]
    alert_count: int
    incident_count: int
    ledger_transaction_count: int
    market_gap_count: int
    alert_payload_bytes: int
    incident_payload_bytes: int
    ledger_payload_bytes: int
    market_gap_payload_bytes: int

    def __post_init__(self) -> None:
        _require_sha256(self.golden_root, label="golden_root")
        _require_sha256(self.source_sha256, label="source_sha256")
        if type(self.commit_count) is not int or self.commit_count < 2:
            raise GoldenCapacityShapeError("Golden shape requires at least two commits")
        if type(self.logical_row_count) is not int or self.logical_row_count < self.commit_count:
            raise GoldenCapacityShapeError("Golden logical row count is invalid")
        if not 0 <= self.start_time_ns < self.end_time_ns or self.cadence_ns < 1:
            raise GoldenCapacityShapeError("Golden temporal shape is invalid")
        if not self.type_observations or sum(item.count for item in self.type_observations) != self.commit_count:
            raise GoldenCapacityShapeError("Golden type observations do not cover every commit")
        if tuple(sorted(item.record_type for item in self.type_observations)) != tuple(
            item.record_type for item in self.type_observations
        ):
            raise GoldenCapacityShapeError("Golden type observations must be sorted")
        if not self.strategies or len(set(self.strategies)) != len(self.strategies):
            raise GoldenCapacityShapeError("Golden strategy IDs are invalid")
        for label, value in (
            ("alert_count", self.alert_count),
            ("incident_count", self.incident_count),
            ("ledger_transaction_count", self.ledger_transaction_count),
            ("market_gap_count", self.market_gap_count),
            ("alert_payload_bytes", self.alert_payload_bytes),
            ("incident_payload_bytes", self.incident_payload_bytes),
            ("ledger_payload_bytes", self.ledger_payload_bytes),
            ("market_gap_payload_bytes", self.market_gap_payload_bytes),
        ):
            _require_non_negative(value, label=label)
        if self.market_gap_count > self.alert_count:
            raise GoldenCapacityShapeError("Golden MARKET_GAP count exceeds alerts")

    def payload(self) -> dict[str, object]:
        return {
            "activity": {
                "alert_count": self.alert_count,
                "alert_payload_bytes": self.alert_payload_bytes,
                "incident_count": self.incident_count,
                "incident_payload_bytes": self.incident_payload_bytes,
                "ledger_payload_bytes": self.ledger_payload_bytes,
                "ledger_transaction_count": self.ledger_transaction_count,
                "market_gap_count": self.market_gap_count,
                "market_gap_payload_bytes": self.market_gap_payload_bytes,
                "size_basis": GOLDEN_ACTIVITY_SIZE_BASIS,
            },
            "cadence": {
                "average_ns": self.cadence_ns,
                "end_time_ns": self.end_time_ns,
                "start_time_ns": self.start_time_ns,
            },
            "format": GOLDEN_SHAPE_FORMAT,
            "golden": {
                "commit_count": self.commit_count,
                "logical_row_count": self.logical_row_count,
                "root": self.golden_root,
                "source_sha256": self.source_sha256,
            },
            "markers": list(CAPACITY_MARKERS),
            "payload_size_basis": GOLDEN_PAYLOAD_SIZE_BASIS,
            "strategies": list(self.strategies),
            "types": [item.payload() for item in self.type_observations],
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.payload())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    def workload_config(self, *, commit_count: int, seed: int) -> CapacityWorkloadConfig:
        """Scale source rates to a deterministic, visibly synthetic workload."""

        if type(commit_count) is not int or commit_count < 1:
            raise ValueError("capacity commit_count must be a positive exact integer")
        regular_alerts = self.alert_count - self.market_gap_count
        return CapacityWorkloadConfig(
            profile=CapacityProfile.GOLDEN_SHAPED,
            seed=seed,
            commit_count=commit_count,
            start_time_ns=self.start_time_ns,
            cadence_ns=self.cadence_ns,
            type_distribution=tuple(
                CapacityTypeSpec(
                    record_type=item.record_type,
                    stream="inbox",
                    weight=item.count,
                    payload_min_bytes=item.payload_min_bytes,
                    payload_max_bytes=item.payload_max_bytes,
                    payload_cardinality=item.distinct_payload_hashes,
                )
                for item in self.type_observations
            ),
            strategies=self.strategies,
            alert_every_commits=_interval(self.commit_count, regular_alerts),
            incident_every_commits=_interval(self.commit_count, self.incident_count),
            ledger_every_commits=_interval(
                self.commit_count,
                self.ledger_transaction_count,
            ),
            market_gap_count=_scaled_count(
                self.market_gap_count,
                self.commit_count,
                commit_count,
            ),
            alert_payload_bytes=self.alert_payload_bytes,
            incident_payload_bytes=self.incident_payload_bytes,
            ledger_payload_bytes=self.ledger_payload_bytes,
            market_gap_payload_bytes=self.market_gap_payload_bytes,
            golden_census_sha256=self.sha256,
        )


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


def _stream_descriptors(verification: GoldenVerification) -> Mapping[str, object]:
    raw = verification.manifest.get("streams")
    if not isinstance(raw, Mapping) or set(raw) != set(GOLDEN_STREAM_NAMES):
        raise GoldenCapacityShapeError("Golden shape requires the fixed 13 streams")
    return raw


def _descriptor_row_count(
    descriptors: Mapping[str, object],
    name: str,
) -> int:
    descriptor = descriptors.get(name)
    if not isinstance(descriptor, Mapping):
        raise GoldenCapacityShapeError(f"Golden stream {name!r} descriptor is missing")
    return _require_non_negative(
        descriptor.get("row_count"),
        label=f"Golden stream {name} row_count",
    )


def _logical_row_count(descriptors: Mapping[str, object]) -> int:
    return sum(_descriptor_row_count(descriptors, name) for name in GOLDEN_STREAM_NAMES)


def _require_descriptor_count(
    *,
    stream: str,
    observed: int,
    expected: int,
) -> None:
    if observed != expected:
        raise GoldenCapacityShapeError(
            f"Golden {stream} rows differ from authenticated descriptor count"
        )


def _activity_sizes(
    verification: GoldenVerification,
    stream_factory: GoldenShapeStreamFactory,
    progress: _GoldenShapeProgress,
) -> tuple[int, int, int, int, int, int, int, int]:
    regular_alert_sizes = _SizeAccumulator()
    gap_sizes = _SizeAccumulator()
    progress.emit(
        stream="alerts",
        rows_completed=0,
        status="started",
    )
    for row in stream_factory(verification, "alerts"):
        if not isinstance(row, Mapping):
            raise GoldenCapacityShapeError("Golden alerts emitted a non-mapping row")
        encoded_size = len(
            _golden_canonical_json_bytes(row, label="Golden alert row")
        ) + 1
        (gap_sizes if row.get("code") == "MARKET_GAP" else regular_alert_sizes).add(
            encoded_size
        )
        rows_completed = regular_alert_sizes.count + gap_sizes.count
        if rows_completed > progress.stream_row_counts["alerts"]:
            raise GoldenCapacityShapeError(
                "Golden alerts rows exceed authenticated descriptor count"
            )
        if rows_completed % _PROGRESS_ROW_INTERVAL == 0:
            progress.emit(
                stream="alerts",
                rows_completed=rows_completed,
                status="running",
            )
    alert_count = regular_alert_sizes.count + gap_sizes.count
    _require_descriptor_count(
        stream="alerts",
        observed=alert_count,
        expected=progress.stream_row_counts["alerts"],
    )
    progress.emit(
        stream="alerts",
        rows_completed=alert_count,
        status="complete",
    )
    incident_sizes = _SizeAccumulator()
    progress.emit(
        stream="incidents",
        rows_completed=0,
        status="started",
    )
    for row in stream_factory(verification, "incidents"):
        if not isinstance(row, Mapping):
            raise GoldenCapacityShapeError("Golden incidents emitted a non-mapping row")
        incident_sizes.add(
            len(_golden_canonical_json_bytes(row, label="Golden incident row")) + 1
        )
        if incident_sizes.count > progress.stream_row_counts["incidents"]:
            raise GoldenCapacityShapeError(
                "Golden incidents rows exceed authenticated descriptor count"
            )
        if incident_sizes.count % _PROGRESS_ROW_INTERVAL == 0:
            progress.emit(
                stream="incidents",
                rows_completed=incident_sizes.count,
                status="running",
            )
    _require_descriptor_count(
        stream="incidents",
        observed=incident_sizes.count,
        expected=progress.stream_row_counts["incidents"],
    )
    progress.emit(
        stream="incidents",
        rows_completed=incident_sizes.count,
        status="complete",
    )
    ledger_sizes = _SizeAccumulator()
    progress.emit(
        stream="ledger_transactions",
        rows_completed=0,
        status="started",
    )
    for row in stream_factory(verification, "ledger_transactions"):
        if not isinstance(row, Mapping):
            raise GoldenCapacityShapeError(
                "Golden ledger transactions emitted a non-mapping row"
            )
        ledger_sizes.add(
            len(
                _golden_canonical_json_bytes(
                    row,
                    label="Golden ledger transaction row",
                )
            )
            + 1
        )
        if ledger_sizes.count > progress.stream_row_counts["ledger_transactions"]:
            raise GoldenCapacityShapeError(
                "Golden ledger_transactions rows exceed authenticated descriptor count"
            )
        if ledger_sizes.count % _PROGRESS_ROW_INTERVAL == 0:
            progress.emit(
                stream="ledger_transactions",
                rows_completed=ledger_sizes.count,
                status="running",
            )
    _require_descriptor_count(
        stream="ledger_transactions",
        observed=ledger_sizes.count,
        expected=progress.stream_row_counts["ledger_transactions"],
    )
    progress.emit(
        stream="ledger_transactions",
        rows_completed=ledger_sizes.count,
        status="complete",
    )
    return (
        alert_count,
        incident_sizes.count,
        ledger_sizes.count,
        gap_sizes.count,
        regular_alert_sizes.average,
        incident_sizes.average,
        ledger_sizes.average,
        gap_sizes.average,
    )


def _prepare_cardinality_store(connection: sqlite3.Connection) -> None:
    connection.execute(f"PRAGMA cache_size = -{_CARDINALITY_CACHE_KIB}")
    connection.execute("PRAGMA temp_store = FILE")
    connection.execute("PRAGMA journal_mode = DELETE")
    connection.execute("PRAGMA synchronous = FULL")
    connection.execute(
        """
        CREATE TABLE payload_hashes (
            record_type TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            PRIMARY KEY (record_type, payload_hash)
        ) WITHOUT ROWID
        """
    )


def _distinct_payload_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        cast(str, record_type): cast(int, count)
        for record_type, count in connection.execute(
            """
            SELECT record_type, COUNT(*)
            FROM payload_hashes
            GROUP BY record_type
            """
        )
    }


def derive_golden_capacity_shape(
    verification: GoldenVerification,
    *,
    stream_factory: GoldenShapeStreamFactory = _default_stream_factory,
    scratch_parent: Path | None = None,
    progress: GoldenShapeProgressCallback | None = None,
) -> GoldenCapacityShape:
    """Exhaustively census Golden inputs with bounded process memory.

    Exact distinct-payload cardinalities are held in a disposable, disk-backed
    SQLite index.  ``scratch_parent`` selects only the parent of a newly created
    private temporary directory; the directory is removed on success or error.
    """

    if type(verification) is not GoldenVerification or not callable(stream_factory):
        raise TypeError("Golden shape requires GoldenVerification and a stream factory")
    if scratch_parent is not None and not isinstance(scratch_parent, Path):
        raise TypeError("Golden shape scratch_parent must be pathlib.Path or None")
    if progress is not None and not callable(progress):
        raise TypeError("Golden shape progress must be callable or None")
    descriptors = _stream_descriptors(verification)
    golden_root = _require_sha256(verification.root_hash, label="Golden root")
    census = verification.manifest.get("census")
    source = verification.manifest.get("source")
    if not isinstance(census, Mapping) or not isinstance(source, Mapping):
        raise GoldenCapacityShapeError("Golden census or source binding is missing")
    expected_counts = census.get("input_type_counts")
    strategies = census.get("strategy_ids")
    if (
        not isinstance(expected_counts, Mapping)
        or any(type(key) is not str or type(value) is not int for key, value in expected_counts.items())
        or type(strategies) is not list
        or any(type(value) is not str or not value for value in strategies)
    ):
        raise GoldenCapacityShapeError("Golden input type or strategy census is malformed")
    expected_commit_count = _require_non_negative(
        census.get("commit_count"),
        label="Golden census commit_count",
    )
    scanned_stream_row_counts = {
        name: _descriptor_row_count(descriptors, name)
        for name in _GOLDEN_SHAPE_SCANNED_STREAMS
    }
    if scanned_stream_row_counts["inbox"] != expected_commit_count:
        raise GoldenCapacityShapeError(
            "Golden inbox descriptor count differs from manifest census"
        )
    progress_state = _GoldenShapeProgress(
        callback=progress,
        golden_root=golden_root,
        commits_total=expected_commit_count,
        stream_row_counts=scanned_stream_row_counts,
        census_started_ns=perf_counter_ns(),
    )

    observations: dict[str, list[int]] = {}
    first_time: int | None = None
    last_time: int | None = None
    prior_time: int | None = None
    observed_commits = 0
    with TemporaryDirectory(
        prefix="hyperlab-phase1c-golden-shape-",
        dir=scratch_parent,
    ) as temporary_directory:
        cardinality_path = Path(temporary_directory) / "payload-cardinality.sqlite3"
        with closing(sqlite3.connect(cardinality_path)) as connection:
            _prepare_cardinality_store(connection)
            progress_state.emit(
                stream="inbox",
                rows_completed=0,
                status="started",
            )
            for row in stream_factory(verification, "inbox"):
                if not isinstance(row, Mapping):
                    raise GoldenCapacityShapeError(
                        "Golden inbox emitted a non-mapping row"
                    )
                payload = row.get("payload")
                if not isinstance(payload, Mapping):
                    raise GoldenCapacityShapeError("Golden inbox payload is not an object")
                record_type = payload.get("input_type")
                if type(record_type) is not str or not record_type:
                    raise GoldenCapacityShapeError("Golden inbox input_type is missing")
                expected_type_count = expected_counts.get(record_type)
                if type(expected_type_count) is not int:
                    raise GoldenCapacityShapeError(
                        "Golden inbox input_type is absent from manifest census"
                    )
                encoded_payload = _golden_canonical_json_bytes(
                    payload,
                    label="Golden inbox payload",
                )
                encoded_size = len(encoded_payload)
                values = observations.setdefault(
                    record_type,
                    [0, encoded_size, encoded_size],
                )
                values[0] += 1
                if values[0] > expected_type_count:
                    raise GoldenCapacityShapeError(
                        "Golden inbox input_type count exceeds manifest census"
                    )
                values[1] = min(values[1], encoded_size)
                values[2] = max(values[2], encoded_size)
                payload_hash = _require_sha256(
                    row.get("payload_hash"),
                    label="Golden payload_hash",
                )
                if hashlib.sha256(encoded_payload).hexdigest() != payload_hash:
                    raise GoldenCapacityShapeError(
                        "Golden inbox payload hash differs from canonical payload"
                    )
                connection.execute(
                    "INSERT OR IGNORE INTO payload_hashes VALUES (?, ?)",
                    (record_type, payload_hash),
                )
                timestamp = _epoch_ns(row.get("created_at"))
                if prior_time is not None and timestamp < prior_time:
                    raise GoldenCapacityShapeError("Golden inbox timestamps regress")
                if first_time is None:
                    first_time = timestamp
                prior_time = timestamp
                last_time = timestamp
                observed_commits += 1
                if observed_commits > scanned_stream_row_counts["inbox"]:
                    raise GoldenCapacityShapeError(
                        "Golden inbox rows exceed authenticated descriptor count"
                    )
                if observed_commits % _CARDINALITY_COMMIT_INTERVAL == 0:
                    connection.commit()
                if observed_commits % _PROGRESS_ROW_INTERVAL == 0:
                    progress_state.emit(
                        stream="inbox",
                        rows_completed=observed_commits,
                        status="running",
                    )
            connection.commit()
            distinct_payload_counts = _distinct_payload_counts(connection)

    _require_descriptor_count(
        stream="inbox",
        observed=observed_commits,
        expected=scanned_stream_row_counts["inbox"],
    )
    observed_counts = {name: values[0] for name, values in observations.items()}
    if observed_commits != expected_commit_count or observed_counts != dict(expected_counts):
        raise GoldenCapacityShapeError("Golden inbox census differs from manifest counts")
    progress_state.emit(
        stream="inbox",
        rows_completed=observed_commits,
        status="complete",
    )
    if first_time is None or last_time is None or last_time <= first_time:
        raise GoldenCapacityShapeError("Golden inbox does not define a positive time span")
    cadence = (last_time - first_time) // (observed_commits - 1)
    if cadence < 1:
        raise GoldenCapacityShapeError("Golden average cadence is below one nanosecond")

    activity = _activity_sizes(verification, stream_factory, progress_state)
    observations_payload = tuple(
        GoldenCapacityTypeObservation(
            record_type=name,
            count=values[0],
            payload_min_bytes=values[1],
            payload_max_bytes=values[2],
            distinct_payload_hashes=distinct_payload_counts[name],
        )
        for name, values in sorted(observations.items())
    )
    return GoldenCapacityShape(
        golden_root=golden_root,
        source_sha256=_require_sha256(source.get("sha256"), label="Golden source SHA-256"),
        commit_count=observed_commits,
        logical_row_count=_logical_row_count(descriptors),
        start_time_ns=first_time,
        end_time_ns=last_time,
        cadence_ns=cadence,
        type_observations=observations_payload,
        strategies=tuple(cast(list[str], strategies)),
        alert_count=activity[0],
        incident_count=activity[1],
        ledger_transaction_count=activity[2],
        market_gap_count=activity[3],
        alert_payload_bytes=activity[4],
        incident_payload_bytes=activity[5],
        ledger_payload_bytes=activity[6],
        market_gap_payload_bytes=activity[7],
    )


__all__ = [
    "GOLDEN_ACTIVITY_SIZE_BASIS",
    "GOLDEN_PAYLOAD_SIZE_BASIS",
    "GOLDEN_SHAPE_FORMAT",
    "GoldenCapacityShape",
    "GoldenCapacityShapeError",
    "GoldenCapacityTypeObservation",
    "GoldenShapeProgressCallback",
    "GoldenShapeStreamFactory",
    "derive_golden_capacity_shape",
]
