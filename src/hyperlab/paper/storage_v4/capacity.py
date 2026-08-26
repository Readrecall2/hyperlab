"""Bounded synthetic capacity workloads and measurement contracts for Storage v4.

This module deliberately has no dependency on a concrete Storage v4 writer.  It
defines deterministic, streaming workload descriptors plus a narrow runner
protocol so a native coordinator can be connected without making this technical
capacity fixture an economic or trading surface.
"""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from functools import partial
from itertools import pairwise
from pathlib import Path
from typing import Protocol, runtime_checkable

from hyperlab.paper.storage_v4.canonical import canonical_json_bytes

SYNTHETIC_CAPACITY_WORKLOAD = "SYNTHETIC_CAPACITY_WORKLOAD"
NOT_ECONOMIC_EVIDENCE = "NOT_ECONOMIC_EVIDENCE"
NOT_ALPHA_EVIDENCE = "NOT_ALPHA_EVIDENCE"
PAPER_ONLY = "PAPER_ONLY"
CAPACITY_MARKERS = (
    SYNTHETIC_CAPACITY_WORKLOAD,
    NOT_ECONOMIC_EVIDENCE,
    NOT_ALPHA_EVIDENCE,
    PAPER_ONLY,
)

GENERATOR_VERSION = "storage-v4-synthetic-capacity-v3"
WORKLOAD_HASH_DOMAIN = b"HL4-SYNTHETIC-CAPACITY-WORKLOAD-V4\x00"
WORKLOAD_HASH_STEP_DOMAIN = b"HL4-SYNTHETIC-CAPACITY-WORKLOAD-STEP-V1\x00"
PAYLOAD_ALGORITHM = "SHAKE256_WINDOW_64K_V1"
MAX_SYNTHETIC_PAYLOAD_BYTES = 16 * 1024 * 1024
SYNTHETIC_PAYLOAD_WINDOW_BYTES = 64 * 1024
ADVERSARIAL_FUNDING_BURST_PERIOD = 97
ADVERSARIAL_FUNDING_BURST_WIDTH = 8
ADVERSARIAL_HIGH_CARDINALITY = 10_000
MIN_PERCENTILE_OBSERVATIONS = 100
GIB_BYTES = 1 << 30
_NANOSECONDS_PER_HOUR = 3_600_000_000_000
_SHA256_LENGTH = 64


class CapacityProfile(StrEnum):
    GOLDEN_SHAPED = "GOLDEN_SHAPED"
    ADVERSARIAL_STORAGE = "ADVERSARIAL_STORAGE"
    BOUNDED_TAIL_RESTART = "BOUNDED_TAIL_RESTART"


def _require_text(value: str, *, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be non-empty text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise ValueError(f"{label} must be strict UTF-8 text") from error
    return value


def _require_non_negative(value: int, *, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _require_positive(value: int, *, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _require_sha256(value: str, *, label: str) -> str:
    if type(value) is not str or len(value) != _SHA256_LENGTH:
        raise ValueError(f"{label} must be a 64-character lowercase SHA-256")
    if value != value.lower() or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a 64-character lowercase SHA-256")
    return value


def _optional_interval(value: int | None, *, label: str) -> None:
    if value is not None:
        _require_positive(value, label=label)


@dataclass(frozen=True, slots=True)
class CapacityTypeSpec:
    """One explicitly weighted logical input type and its bounded payload shape."""

    record_type: str
    stream: str
    weight: int
    payload_min_bytes: int
    payload_max_bytes: int
    payload_cardinality: int

    def __post_init__(self) -> None:
        _require_text(self.record_type, label="record_type")
        _require_text(self.stream, label="stream")
        _require_positive(self.weight, label="weight")
        _require_non_negative(self.payload_min_bytes, label="payload_min_bytes")
        _require_non_negative(self.payload_max_bytes, label="payload_max_bytes")
        if self.payload_max_bytes < self.payload_min_bytes:
            raise ValueError("payload_max_bytes must be >= payload_min_bytes")
        if self.payload_max_bytes > MAX_SYNTHETIC_PAYLOAD_BYTES:
            raise ValueError(
                f"payload_max_bytes must be <= {MAX_SYNTHETIC_PAYLOAD_BYTES}"
            )
        _require_positive(self.payload_cardinality, label="payload_cardinality")

    def payload(self) -> dict[str, object]:
        return {
            "payload_cardinality": self.payload_cardinality,
            "payload_max_bytes": self.payload_max_bytes,
            "payload_min_bytes": self.payload_min_bytes,
            "record_type": self.record_type,
            "stream": self.stream,
            "weight": self.weight,
        }


@dataclass(frozen=True, slots=True)
class CapacityWorkloadConfig:
    """All inputs required to reproduce one bounded synthetic workload."""

    profile: CapacityProfile
    seed: int
    commit_count: int
    start_time_ns: int
    cadence_ns: int | None
    type_distribution: tuple[CapacityTypeSpec, ...]
    strategies: tuple[str, ...]
    alert_every_commits: int | None
    incident_every_commits: int | None
    ledger_every_commits: int | None
    market_gap_count: int
    alert_payload_bytes: int
    incident_payload_bytes: int
    ledger_payload_bytes: int
    market_gap_payload_bytes: int
    golden_census_sha256: str | None = None
    bounded_tail_max: int | None = None
    projection_every_commits: int | None = None
    projection_payload_bytes: int = 0
    adversarial_boundary_intervals: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.profile, CapacityProfile):
            raise TypeError("profile must be CapacityProfile")
        _require_non_negative(self.seed, label="seed")
        _require_positive(self.commit_count, label="commit_count")
        _require_non_negative(self.start_time_ns, label="start_time_ns")
        if self.cadence_ns is not None:
            _require_positive(self.cadence_ns, label="cadence_ns")
        if type(self.type_distribution) is not tuple or not self.type_distribution:
            raise ValueError("type_distribution must be a non-empty tuple")
        if any(not isinstance(item, CapacityTypeSpec) for item in self.type_distribution):
            raise TypeError("type_distribution must contain CapacityTypeSpec values")
        record_types = tuple(item.record_type for item in self.type_distribution)
        if len(record_types) != len(set(record_types)):
            raise ValueError("type_distribution record_type values must be unique")
        if type(self.strategies) is not tuple or not self.strategies:
            raise ValueError("strategies must be a non-empty tuple")
        for strategy in self.strategies:
            _require_text(strategy, label="strategy")
        if len(self.strategies) != len(set(self.strategies)):
            raise ValueError("strategies must be unique")
        _optional_interval(self.alert_every_commits, label="alert_every_commits")
        _optional_interval(self.incident_every_commits, label="incident_every_commits")
        _optional_interval(self.ledger_every_commits, label="ledger_every_commits")
        _optional_interval(self.projection_every_commits, label="projection_every_commits")
        _require_non_negative(self.market_gap_count, label="market_gap_count")
        if self.market_gap_count > self.commit_count:
            raise ValueError("market_gap_count must not exceed commit_count")
        for label, value in (
            ("alert_payload_bytes", self.alert_payload_bytes),
            ("incident_payload_bytes", self.incident_payload_bytes),
            ("ledger_payload_bytes", self.ledger_payload_bytes),
            ("market_gap_payload_bytes", self.market_gap_payload_bytes),
            ("projection_payload_bytes", self.projection_payload_bytes),
        ):
            _require_non_negative(value, label=label)
            if value > MAX_SYNTHETIC_PAYLOAD_BYTES:
                raise ValueError(f"{label} must be <= {MAX_SYNTHETIC_PAYLOAD_BYTES}")
        if type(self.adversarial_boundary_intervals) is not tuple or any(
            type(interval) is not int or interval < 2
            for interval in self.adversarial_boundary_intervals
        ):
            raise ValueError("adversarial_boundary_intervals must contain integers >= 2")
        if len(set(self.adversarial_boundary_intervals)) != len(
            self.adversarial_boundary_intervals
        ):
            raise ValueError("adversarial_boundary_intervals must be unique")

        if self.profile is CapacityProfile.GOLDEN_SHAPED:
            if self.golden_census_sha256 is None:
                raise ValueError(
                    "golden_census_sha256 is required for a fact-derived GOLDEN_SHAPED profile"
                )
            _require_sha256(self.golden_census_sha256, label="golden_census_sha256")
        elif self.golden_census_sha256 is not None:
            _require_sha256(self.golden_census_sha256, label="golden_census_sha256")

        if self.profile is CapacityProfile.ADVERSARIAL_STORAGE:
            if self.commit_count < ADVERSARIAL_FUNDING_BURST_WIDTH:
                raise ValueError(
                    "ADVERSARIAL_STORAGE requires enough commits for one funding burst"
                )
            if any(
                item.payload_min_bytes >= item.payload_max_bytes
                for item in self.type_distribution
            ):
                raise ValueError(
                    "ADVERSARIAL_STORAGE requires variable configured bounds for every type"
                )
            if not any(item.payload_cardinality == 1 for item in self.type_distribution):
                raise ValueError("ADVERSARIAL_STORAGE requires a repeated payload identity")
            required_cardinality = min(self.commit_count, ADVERSARIAL_HIGH_CARDINALITY)
            if not any(
                item.payload_cardinality >= required_cardinality
                for item in self.type_distribution
            ):
                raise ValueError("ADVERSARIAL_STORAGE requires high-cardinality payloads")
            if not any("FUNDING" in item.record_type for item in self.type_distribution):
                raise ValueError("ADVERSARIAL_STORAGE requires a funding input type")
            for label, interval in (
                ("alert_every_commits", self.alert_every_commits),
                ("ledger_every_commits", self.ledger_every_commits),
                ("projection_every_commits", self.projection_every_commits),
            ):
                if interval is None or interval > self.commit_count:
                    raise ValueError(
                        f"ADVERSARIAL_STORAGE requires observable {label} activity"
                    )
            if self.market_gap_count == 0:
                raise ValueError("ADVERSARIAL_STORAGE requires valid MARKET_GAP coverage")
            if not self.adversarial_boundary_intervals or any(
                interval > self.commit_count
                for interval in self.adversarial_boundary_intervals
            ):
                raise ValueError(
                    "ADVERSARIAL_STORAGE requires observable segment/checkpoint boundaries"
                )
        elif self.adversarial_boundary_intervals:
            raise ValueError(
                "adversarial_boundary_intervals are exclusive to ADVERSARIAL_STORAGE"
            )

        if self.profile is CapacityProfile.BOUNDED_TAIL_RESTART:
            if self.bounded_tail_max is None:
                raise ValueError("bounded_tail_max is required for BOUNDED_TAIL_RESTART")
            _require_positive(self.bounded_tail_max, label="bounded_tail_max")
            if self.bounded_tail_max < 10_000:
                raise ValueError("bounded_tail_max must be at least 10000")
            if self.bounded_tail_max >= self.commit_count:
                raise ValueError("bounded_tail_max must be strictly less than commit_count")
        elif self.bounded_tail_max is not None:
            _require_positive(self.bounded_tail_max, label="bounded_tail_max")

    @property
    def tail_restart_sizes(self) -> tuple[int, ...]:
        if self.profile is not CapacityProfile.BOUNDED_TAIL_RESTART:
            return ()
        assert self.bounded_tail_max is not None
        ordered = (0, 1, 100, 10_000, self.bounded_tail_max)
        return tuple(dict.fromkeys(ordered))

    def configuration_payload(self) -> dict[str, object]:
        return {
            "activity_payload_bytes": {
                "alert": self.alert_payload_bytes,
                "incident": self.incident_payload_bytes,
                "ledger": self.ledger_payload_bytes,
                "market_gap": self.market_gap_payload_bytes,
                "projection": self.projection_payload_bytes,
            },
            "adversarial_schedule": {
                "boundary_intervals": list(self.adversarial_boundary_intervals),
                "funding_burst_period": (
                    ADVERSARIAL_FUNDING_BURST_PERIOD
                    if self.profile is CapacityProfile.ADVERSARIAL_STORAGE
                    else None
                ),
                "funding_burst_width": (
                    ADVERSARIAL_FUNDING_BURST_WIDTH
                    if self.profile is CapacityProfile.ADVERSARIAL_STORAGE
                    else None
                ),
            },
            "bounded_tail_max": self.bounded_tail_max,
            "commit_count": self.commit_count,
            "start_time_ns": self.start_time_ns,
        }


def _deterministic_u64(config: CapacityWorkloadConfig, sequence: int, lane: str) -> int:
    material = (
        f"{GENERATOR_VERSION}\x00{config.profile.value}\x00{config.seed}\x00"
        f"{sequence}\x00{lane}"
    ).encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big")


def _payload_key(
    config: CapacityWorkloadConfig,
    *,
    record_type: str,
    identity: int,
) -> str:
    material = (
        f"{PAYLOAD_ALGORITHM}\x00{GENERATOR_VERSION}\x00{config.profile.value}\x00"
        f"{config.seed}\x00{record_type}\x00{identity}"
    ).encode()
    return hashlib.sha256(material).hexdigest()


@dataclass(frozen=True, slots=True)
class SyntheticPayload:
    """A deterministic payload recipe that can be consumed in bounded chunks."""

    size_bytes: int
    key_sha256: str
    algorithm: str = PAYLOAD_ALGORITHM

    def __post_init__(self) -> None:
        _require_non_negative(self.size_bytes, label="size_bytes")
        if self.size_bytes > MAX_SYNTHETIC_PAYLOAD_BYTES:
            raise ValueError(f"size_bytes must be <= {MAX_SYNTHETIC_PAYLOAD_BYTES}")
        _require_sha256(self.key_sha256, label="key_sha256")
        if self.algorithm != PAYLOAD_ALGORITHM:
            raise ValueError(f"algorithm must be {PAYLOAD_ALGORITHM}")

    def iter_chunks(self, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        """Yield exact payload bytes without materializing the record as a whole."""

        _require_positive(chunk_size, label="chunk_size")
        pattern_size = min(self.size_bytes, SYNTHETIC_PAYLOAD_WINDOW_BYTES)
        if pattern_size == 0:
            return
        pattern = hashlib.shake_256(bytes.fromhex(self.key_sha256)).digest(pattern_size)
        remaining = self.size_bytes
        offset = 0
        while remaining:
            take = min(remaining, chunk_size)
            pattern_offset = offset % len(pattern)
            repetitions = (pattern_offset + take + len(pattern) - 1) // len(pattern)
            expanded = pattern * repetitions
            yield expanded[pattern_offset : pattern_offset + take]
            offset += take
            remaining -= take

    def to_bytes(self) -> bytes:
        """Materialize one bounded record, never the complete workload."""

        return b"".join(self.iter_chunks())

    def descriptor(self) -> dict[str, object]:
        return {
            "algorithm": self.algorithm,
            "key_sha256": self.key_sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class SyntheticCapacityRow:
    commit_sequence: int
    row_ordinal: int
    source_sequence: int
    stream: str
    record_type: str
    strategy: str
    logical_time_ns: int | None
    payload: SyntheticPayload
    code: str | None = None

    def __post_init__(self) -> None:
        _require_positive(self.commit_sequence, label="commit_sequence")
        _require_non_negative(self.row_ordinal, label="row_ordinal")
        _require_positive(self.source_sequence, label="source_sequence")
        _require_text(self.stream, label="stream")
        _require_text(self.record_type, label="record_type")
        _require_text(self.strategy, label="strategy")
        if self.logical_time_ns is not None:
            _require_non_negative(self.logical_time_ns, label="logical_time_ns")
        if not isinstance(self.payload, SyntheticPayload):
            raise TypeError("payload must be SyntheticPayload")
        if self.code is not None:
            _require_text(self.code, label="code")

    def descriptor(self) -> dict[str, object]:
        return {
            "code": self.code,
            "commit_sequence": self.commit_sequence,
            "logical_time_ns": self.logical_time_ns,
            "markers": list(CAPACITY_MARKERS),
            "payload": self.payload.descriptor(),
            "record_type": self.record_type,
            "row_ordinal": self.row_ordinal,
            "source_sequence": self.source_sequence,
            "strategy": self.strategy,
            "stream": self.stream,
        }


@dataclass(frozen=True, slots=True)
class SyntheticCapacityCommit:
    sequence: int
    logical_time_ns: int | None
    strategy: str
    rows: tuple[SyntheticCapacityRow, ...]

    def __post_init__(self) -> None:
        _require_positive(self.sequence, label="sequence")
        if self.logical_time_ns is not None:
            _require_non_negative(self.logical_time_ns, label="logical_time_ns")
        _require_text(self.strategy, label="strategy")
        if type(self.rows) is not tuple or not self.rows:
            raise ValueError("rows must be a non-empty tuple")
        for ordinal, row in enumerate(self.rows):
            if not isinstance(row, SyntheticCapacityRow):
                raise TypeError("rows must contain SyntheticCapacityRow values")
            if row.commit_sequence != self.sequence or row.row_ordinal != ordinal:
                raise ValueError("row ownership or ordinal differs from its commit")

    def descriptor(self) -> dict[str, object]:
        return {
            "logical_time_ns": self.logical_time_ns,
            "markers": list(CAPACITY_MARKERS),
            "rows": [row.descriptor() for row in self.rows],
            "sequence": self.sequence,
            "strategy": self.strategy,
        }


def _selected_type(config: CapacityWorkloadConfig, sequence: int) -> CapacityTypeSpec:
    if config.profile is CapacityProfile.ADVERSARIAL_STORAGE and any(
        sequence % interval == 0 for interval in config.adversarial_boundary_intervals
    ):
        # Boundary commits are sparse, declared in the canonical manifest, and
        # must exercise the largest configured payload bound deterministically.
        return max(
            config.type_distribution,
            key=lambda item: (item.payload_max_bytes, item.record_type),
        )
    if (
        config.profile is CapacityProfile.ADVERSARIAL_STORAGE
        and (sequence - 1) % ADVERSARIAL_FUNDING_BURST_PERIOD
        < ADVERSARIAL_FUNDING_BURST_WIDTH
    ):
        funding = tuple(
            item for item in config.type_distribution if "FUNDING" in item.record_type
        )
        if not funding:
            raise AssertionError("validated adversarial profile has no funding type")
        return funding[(sequence - 1) % len(funding)]
    total_weight = sum(item.weight for item in config.type_distribution)
    selected = _deterministic_u64(config, sequence, "record-type") % total_weight
    cumulative = 0
    for item in config.type_distribution:
        cumulative += item.weight
        if selected < cumulative:
            return item
    raise AssertionError("weighted type selection exhausted")


def _base_payload(config: CapacityWorkloadConfig, sequence: int, spec: CapacityTypeSpec) -> SyntheticPayload:
    if config.profile is CapacityProfile.ADVERSARIAL_STORAGE:
        boundary_side: int | None = None
        for interval in config.adversarial_boundary_intervals:
            remainder = sequence % interval
            if remainder == 0:
                boundary_side = 1
                break
            if remainder in {1, interval - 1}:
                boundary_side = 0
                break
        use_maximum = sequence % 2 == 0 if boundary_side is None else boundary_side == 1
        size = spec.payload_max_bytes if use_maximum else spec.payload_min_bytes
    else:
        width = spec.payload_max_bytes - spec.payload_min_bytes + 1
        size = spec.payload_min_bytes + (
            _deterministic_u64(config, sequence, "payload-size") % width
        )
    identity = (
        (sequence - 1) % spec.payload_cardinality
        if config.profile is CapacityProfile.ADVERSARIAL_STORAGE
        else _deterministic_u64(config, sequence, f"payload-identity:{spec.record_type}")
        % spec.payload_cardinality
    )
    return SyntheticPayload(
        size_bytes=size,
        key_sha256=_payload_key(
            config,
            record_type=spec.record_type,
            identity=identity,
        ),
    )


def _activity_payload(
    config: CapacityWorkloadConfig,
    *,
    sequence: int,
    record_type: str,
    size_bytes: int,
) -> SyntheticPayload:
    return SyntheticPayload(
        size_bytes=size_bytes,
        key_sha256=_payload_key(
            config,
            record_type=record_type,
            identity=sequence,
        ),
    )


def _market_gap_sequences(commit_count: int, count: int) -> Iterator[int]:
    for ordinal in range(count):
        yield ((ordinal * commit_count) // count) + 1


def _append_capacity_row(
    rows: list[SyntheticCapacityRow],
    stream_sequences: dict[str, int],
    sequence: int,
    strategy: str,
    logical_time_ns: int | None,
    *,
    stream: str,
    record_type: str,
    payload: SyntheticPayload,
    code: str | None = None,
) -> None:
    source_sequence = stream_sequences.get(stream, 0) + 1
    stream_sequences[stream] = source_sequence
    rows.append(
        SyntheticCapacityRow(
            commit_sequence=sequence,
            row_ordinal=len(rows),
            source_sequence=source_sequence,
            stream=stream,
            record_type=record_type,
            strategy=strategy,
            logical_time_ns=logical_time_ns,
            payload=payload,
            code=code,
        )
    )


def iter_capacity_commits(
    config: CapacityWorkloadConfig,
    *,
    start_sequence: int = 1,
    initial_stream_sequences: Mapping[str, int] | None = None,
) -> Iterator[SyntheticCapacityCommit]:
    """Generate a deterministic workload while retaining only one commit in memory."""

    if not isinstance(config, CapacityWorkloadConfig):
        raise TypeError("config must be CapacityWorkloadConfig")
    if type(start_sequence) is not int or not 1 <= start_sequence <= config.commit_count + 1:
        raise ValueError("start_sequence must be within the configured workload")
    if initial_stream_sequences is None:
        if start_sequence != 1:
            raise ValueError("resumed generation requires authenticated stream sequences")
        stream_sequences: dict[str, int] = {}
    else:
        if not isinstance(initial_stream_sequences, Mapping):
            raise TypeError("initial_stream_sequences must be a mapping")
        stream_sequences = {}
        for stream, sequence in initial_stream_sequences.items():
            _require_text(stream, label="initial stream")
            _require_non_negative(sequence, label="initial stream sequence")
            if sequence:
                stream_sequences[stream] = sequence
        if start_sequence == 1 and stream_sequences:
            raise ValueError("fresh generation cannot carry prior stream sequences")
        if start_sequence > 1 and not stream_sequences:
            raise ValueError("resumed generation requires non-empty stream sequences")
    gap_positions = iter(_market_gap_sequences(config.commit_count, config.market_gap_count))
    next_gap = next(gap_positions, None)
    while next_gap is not None and next_gap < start_sequence:
        next_gap = next(gap_positions, None)

    for sequence in range(start_sequence, config.commit_count + 1):
        strategy = config.strategies[
            _deterministic_u64(config, sequence, "strategy") % len(config.strategies)
        ]
        logical_time_ns = (
            None
            if config.cadence_ns is None
            else config.start_time_ns + ((sequence - 1) * config.cadence_ns)
        )
        rows: list[SyntheticCapacityRow] = []

        append_row = partial(
            _append_capacity_row,
            rows,
            stream_sequences,
            sequence,
            strategy,
            logical_time_ns,
        )

        spec = _selected_type(config, sequence)
        append_row(
            stream=spec.stream,
            record_type=spec.record_type,
            payload=_base_payload(config, sequence, spec),
        )
        if config.ledger_every_commits is not None and sequence % config.ledger_every_commits == 0:
            append_row(
                stream="ledger_entries",
                record_type="SYNTHETIC_LEDGER_ACTIVITY",
                payload=_activity_payload(
                    config,
                    sequence=sequence,
                    record_type="SYNTHETIC_LEDGER_ACTIVITY",
                    size_bytes=config.ledger_payload_bytes,
                ),
            )
        if (
            config.projection_every_commits is not None
            and sequence % config.projection_every_commits == 0
        ):
            append_row(
                stream="projection_history",
                record_type="SYNTHETIC_PROJECTION_ACTIVITY",
                payload=_activity_payload(
                    config,
                    sequence=sequence,
                    record_type="SYNTHETIC_PROJECTION_ACTIVITY",
                    size_bytes=config.projection_payload_bytes,
                ),
            )
        if config.alert_every_commits is not None and sequence % config.alert_every_commits == 0:
            append_row(
                stream="alerts",
                record_type="SYNTHETIC_ALERT",
                code="SYNTHETIC_ALERT",
                payload=_activity_payload(
                    config,
                    sequence=sequence,
                    record_type="SYNTHETIC_ALERT",
                    size_bytes=config.alert_payload_bytes,
                ),
            )
        if config.incident_every_commits is not None and sequence % config.incident_every_commits == 0:
            append_row(
                stream="incidents",
                record_type="SYNTHETIC_INCIDENT",
                code="SYNTHETIC_INCIDENT",
                payload=_activity_payload(
                    config,
                    sequence=sequence,
                    record_type="SYNTHETIC_INCIDENT",
                    size_bytes=config.incident_payload_bytes,
                ),
            )
        if next_gap == sequence:
            append_row(
                stream="alerts",
                record_type="MARKET_GAP",
                code="MARKET_GAP",
                payload=_activity_payload(
                    config,
                    sequence=sequence,
                    record_type="MARKET_GAP",
                    size_bytes=config.market_gap_payload_bytes,
                ),
            )
            next_gap = next(gap_positions, None)

        yield SyntheticCapacityCommit(
            sequence=sequence,
            logical_time_ns=logical_time_ns,
            strategy=strategy,
            rows=tuple(rows),
        )


@dataclass(frozen=True, slots=True)
class CapacityWorkloadDigest:
    commit_count: int
    logical_row_count: int
    sha256: str

    def __post_init__(self) -> None:
        _require_non_negative(self.commit_count, label="commit_count")
        _require_non_negative(self.logical_row_count, label="logical_row_count")
        _require_sha256(self.sha256, label="sha256")


class CapacityWorkloadHasher:
    """Incrementally hash the exact ordered workload descriptors."""

    def __init__(self) -> None:
        self._state = hashlib.sha256(WORKLOAD_HASH_DOMAIN).digest()
        self._commit_count = 0
        self._row_count = 0
        self._final: CapacityWorkloadDigest | None = None

    @classmethod
    def resume_from_prefix(cls, prefix: CapacityWorkloadDigest) -> CapacityWorkloadHasher:
        """Restore the authenticated rolling state without replaying prefix descriptors."""

        if not isinstance(prefix, CapacityWorkloadDigest):
            raise TypeError("prefix must be CapacityWorkloadDigest")
        restored = cls()
        restored._state = bytes.fromhex(prefix.sha256)
        restored._commit_count = prefix.commit_count
        restored._row_count = prefix.logical_row_count
        return restored

    def update(self, commit: SyntheticCapacityCommit) -> None:
        if self._final is not None:
            raise RuntimeError("workload hasher is already finalized")
        if not isinstance(commit, SyntheticCapacityCommit):
            raise TypeError("commit must be SyntheticCapacityCommit")
        expected_sequence = self._commit_count + 1
        if commit.sequence != expected_sequence:
            raise ValueError("capacity commits must be contiguous and start at one")
        encoded = canonical_json_bytes(commit.descriptor())
        digest = hashlib.sha256()
        digest.update(WORKLOAD_HASH_STEP_DOMAIN)
        digest.update(self._state)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        self._state = digest.digest()
        self._commit_count += 1
        self._row_count += len(commit.rows)

    def snapshot(self) -> CapacityWorkloadDigest:
        """Return the current prefix digest without freezing future updates."""

        if self._final is not None:
            return self._final
        return CapacityWorkloadDigest(
            commit_count=self._commit_count,
            logical_row_count=self._row_count,
            sha256=self._state.hex(),
        )

    def finalize(self) -> CapacityWorkloadDigest:
        if self._final is None:
            self._final = self.snapshot()
        return self._final


@dataclass(frozen=True, slots=True)
class CapacityWorkloadManifest:
    config: CapacityWorkloadConfig
    digest: CapacityWorkloadDigest

    def __post_init__(self) -> None:
        if not isinstance(self.config, CapacityWorkloadConfig):
            raise TypeError("config must be CapacityWorkloadConfig")
        if not isinstance(self.digest, CapacityWorkloadDigest):
            raise TypeError("digest must be CapacityWorkloadDigest")
        if self.digest.commit_count != self.config.commit_count:
            raise ValueError("manifest commit count differs from configuration")

    @property
    def commit_count(self) -> int:
        return self.digest.commit_count

    @property
    def logical_row_count(self) -> int:
        return self.digest.logical_row_count

    @property
    def workload_sha256(self) -> str:
        return self.digest.sha256

    def payload(self) -> dict[str, object]:
        type_distribution = [item.payload() for item in self.config.type_distribution]
        payload_sizes = [
            {
                "payload_cardinality": item.payload_cardinality,
                "payload_max_bytes": item.payload_max_bytes,
                "payload_min_bytes": item.payload_min_bytes,
                "record_type": item.record_type,
            }
            for item in self.config.type_distribution
        ]
        return {
            "activity_rates": {
                "alert_every_commits": self.config.alert_every_commits,
                "incident_every_commits": self.config.incident_every_commits,
                "ledger_every_commits": self.config.ledger_every_commits,
                "market_gap_count": self.config.market_gap_count,
                "projection_every_commits": self.config.projection_every_commits,
            },
            "artifact": "STORAGE_V4_SYNTHETIC_CAPACITY_WORKLOAD_MANIFEST_V1",
            "configuration": self.config.configuration_payload(),
            "expected": {
                "commit_count": self.commit_count,
                "logical_row_count": self.logical_row_count,
                "workload_sha256": self.workload_sha256,
            },
            "generator_version": GENERATOR_VERSION,
            "golden_census_sha256": self.config.golden_census_sha256,
            "markers": list(CAPACITY_MARKERS),
            "payload_sizes": payload_sizes,
            "profile": self.config.profile.value,
            "seed": self.config.seed,
            "strategies": list(self.config.strategies),
            "tail_restart_sizes": list(self.config.tail_restart_sizes),
            "temporal_cadence": {
                "cadence_ns": self.config.cadence_ns,
                "start_time_ns": self.config.start_time_ns,
                "status": "DEFINED" if self.config.cadence_ns is not None else "UNDEFINED",
            },
            "type_distribution": type_distribution,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.payload())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


def build_capacity_workload_manifest(config: CapacityWorkloadConfig) -> CapacityWorkloadManifest:
    """Compute the expected digest with one bounded streaming pass."""

    hasher = CapacityWorkloadHasher()
    for commit in iter_capacity_commits(config):
        hasher.update(commit)
    return CapacityWorkloadManifest(config=config, digest=hasher.finalize())


@dataclass(frozen=True, slots=True)
class CapacityBytePaths:
    """Explicit paths plus authenticated in-file raw index byte assignments."""

    raw_segments: tuple[Path, ...] = ()
    raw_manifests: tuple[Path, ...] = ()
    raw_index: tuple[Path, ...] = ()
    raw_embedded_index_bytes: tuple[tuple[Path, int], ...] = ()
    paper_segments: tuple[Path, ...] = ()
    paper_overlay: tuple[Path, ...] = ()
    paper_checkpoints: tuple[Path, ...] = ()
    paper_manifests: tuple[Path, ...] = ()
    raw_anchors_witnesses: tuple[Path, ...] = ()
    paper_anchors_witnesses: tuple[Path, ...] = ()
    raw_current_cache: tuple[Path, ...] = ()
    paper_current_cache: tuple[Path, ...] = ()
    scratch: tuple[Path, ...] = ()

    def __post_init__(self) -> None:
        for label, paths in self.categories():
            if type(paths) is not tuple:
                raise TypeError(f"{label} paths must be a tuple")
            for path in paths:
                if not isinstance(path, Path):
                    raise TypeError(f"{label} paths must be pathlib.Path values")
                if not path.is_absolute():
                    raise ValueError(f"{label} paths must be absolute")
        if type(self.raw_embedded_index_bytes) is not tuple:
            raise TypeError("raw_embedded_index_bytes must be a tuple")
        witnessed_embedded_paths: set[str] = set()
        for entry in self.raw_embedded_index_bytes:
            if type(entry) is not tuple or len(entry) != 2:
                raise TypeError(
                    "raw_embedded_index_bytes entries must be (Path, int) tuples"
                )
            path, byte_count = entry
            if not isinstance(path, Path):
                raise TypeError("embedded raw index paths must be pathlib.Path values")
            if not path.is_absolute():
                raise ValueError("embedded raw index paths must be absolute")
            if type(byte_count) is not int or byte_count < 1:
                raise ValueError(
                    "embedded raw index bytes must be positive exact integers"
                )
            normalized = os.path.normcase(os.path.abspath(os.fspath(path)))
            if normalized in witnessed_embedded_paths:
                raise ValueError("embedded raw index paths must be unique")
            witnessed_embedded_paths.add(normalized)

    def categories(self) -> tuple[tuple[str, tuple[Path, ...]], ...]:
        return (
            ("raw_segments_bytes", self.raw_segments),
            ("raw_manifests_bytes", self.raw_manifests),
            ("raw_index_bytes", self.raw_index),
            ("paper_segments_bytes", self.paper_segments),
            ("paper_overlay_bytes", self.paper_overlay),
            ("paper_checkpoints_bytes", self.paper_checkpoints),
            ("paper_manifests_bytes", self.paper_manifests),
            ("raw_anchors_witnesses_bytes", self.raw_anchors_witnesses),
            ("paper_anchors_witnesses_bytes", self.paper_anchors_witnesses),
            ("raw_current_cache_bytes", self.raw_current_cache),
            ("paper_current_cache_bytes", self.paper_current_cache),
            ("scratch_current_bytes", self.scratch),
        )


@dataclass(frozen=True, slots=True)
class ByteCategoryCensus:
    raw_segments_bytes: int
    raw_manifests_bytes: int
    raw_index_bytes: int
    paper_segments_bytes: int
    paper_overlay_bytes: int
    paper_checkpoints_bytes: int
    paper_manifests_bytes: int
    raw_anchors_witnesses_bytes: int
    paper_anchors_witnesses_bytes: int
    raw_current_cache_bytes: int
    paper_current_cache_bytes: int
    scratch_current_bytes: int
    scratch_peak_bytes: int

    def __post_init__(self) -> None:
        for label, value in (
            ("raw_segments_bytes", self.raw_segments_bytes),
            ("raw_manifests_bytes", self.raw_manifests_bytes),
            ("raw_index_bytes", self.raw_index_bytes),
            ("paper_segments_bytes", self.paper_segments_bytes),
            ("paper_overlay_bytes", self.paper_overlay_bytes),
            ("paper_checkpoints_bytes", self.paper_checkpoints_bytes),
            ("paper_manifests_bytes", self.paper_manifests_bytes),
            ("raw_anchors_witnesses_bytes", self.raw_anchors_witnesses_bytes),
            ("paper_anchors_witnesses_bytes", self.paper_anchors_witnesses_bytes),
            ("raw_current_cache_bytes", self.raw_current_cache_bytes),
            ("paper_current_cache_bytes", self.paper_current_cache_bytes),
            ("scratch_current_bytes", self.scratch_current_bytes),
            ("scratch_peak_bytes", self.scratch_peak_bytes),
        ):
            _require_non_negative(value, label=label)
        if self.scratch_peak_bytes < self.scratch_current_bytes:
            raise ValueError("scratch_peak_bytes must be >= scratch_current_bytes")

    @property
    def raw_bytes(self) -> int:
        return (
            self.raw_segments_bytes
            + self.raw_manifests_bytes
            + self.raw_index_bytes
            + self.raw_anchors_witnesses_bytes
            + self.raw_current_cache_bytes
        )

    @property
    def paper_incremental_bytes(self) -> int:
        return (
            self.paper_segments_bytes
            + self.paper_overlay_bytes
            + self.paper_checkpoints_bytes
            + self.paper_manifests_bytes
            + self.paper_anchors_witnesses_bytes
            + self.paper_current_cache_bytes
        )

    @property
    def anchors_witnesses_bytes(self) -> int:
        return self.raw_anchors_witnesses_bytes + self.paper_anchors_witnesses_bytes

    @property
    def current_cache_bytes(self) -> int:
        return self.raw_current_cache_bytes + self.paper_current_cache_bytes

    @property
    def total_bytes(self) -> int:
        """Final authoritative raw plus incremental Paper bytes, excluding scratch."""

        return self.raw_bytes + self.paper_incremental_bytes

    @property
    def total_with_current_scratch_bytes(self) -> int:
        return self.total_bytes + self.scratch_current_bytes

    def payload(self) -> dict[str, object]:
        category_bytes = {
            "paper_checkpoints_bytes": self.paper_checkpoints_bytes,
            "paper_anchors_witnesses_bytes": self.paper_anchors_witnesses_bytes,
            "paper_current_cache_bytes": self.paper_current_cache_bytes,
            "paper_manifests_bytes": self.paper_manifests_bytes,
            "paper_overlay_bytes": self.paper_overlay_bytes,
            "paper_segments_bytes": self.paper_segments_bytes,
            "raw_index_bytes": self.raw_index_bytes,
            "raw_anchors_witnesses_bytes": self.raw_anchors_witnesses_bytes,
            "raw_current_cache_bytes": self.raw_current_cache_bytes,
            "raw_manifests_bytes": self.raw_manifests_bytes,
            "raw_segments_bytes": self.raw_segments_bytes,
        }
        shares: dict[str, str | None]
        if self.total_bytes == 0:
            shares = {label: None for label in category_bytes}
        else:
            shares = {
                label: _decimal_fraction(byte_count, self.total_bytes)
                for label, byte_count in category_bytes.items()
            }
        return {
            "anchors_witnesses": {
                "paper_bytes": self.paper_anchors_witnesses_bytes,
                "raw_bytes": self.raw_anchors_witnesses_bytes,
                "total_bytes": self.anchors_witnesses_bytes,
            },
            "anchors_witnesses_bytes": self.anchors_witnesses_bytes,
            "current_cache": {
                "paper_bytes": self.paper_current_cache_bytes,
                "raw_bytes": self.raw_current_cache_bytes,
                "total_bytes": self.current_cache_bytes,
            },
            "current_cache_bytes": self.current_cache_bytes,
            "paper": {
                "anchors_witnesses_bytes": self.paper_anchors_witnesses_bytes,
                "checkpoints_bytes": self.paper_checkpoints_bytes,
                "current_cache_bytes": self.paper_current_cache_bytes,
                "manifests_bytes": self.paper_manifests_bytes,
                "overlay_bytes": self.paper_overlay_bytes,
                "segments_bytes": self.paper_segments_bytes,
            },
            "paper_incremental_bytes": self.paper_incremental_bytes,
            "raw": {
                "anchors_witnesses_bytes": self.raw_anchors_witnesses_bytes,
                "current_cache_bytes": self.raw_current_cache_bytes,
                "index_bytes": self.raw_index_bytes,
                "manifests_bytes": self.raw_manifests_bytes,
                "segments_bytes": self.raw_segments_bytes,
            },
            "raw_bytes": self.raw_bytes,
            "category_shares_of_total": shares,
            "category_shares_status": (
                "AVAILABLE" if self.total_bytes else "UNAVAILABLE_ZERO_TOTAL"
            ),
            "scratch_current_bytes": self.scratch_current_bytes,
            "scratch_peak_bytes": self.scratch_peak_bytes,
            "total_bytes": self.total_bytes,
            "total_excludes_scratch": True,
            "total_with_current_scratch_bytes": self.total_with_current_scratch_bytes,
        }


def _is_reparse(stat_result: os.stat_result) -> bool:
    attributes = getattr(stat_result, "st_file_attributes", 0)
    marker = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(marker and attributes & marker)


def _regular_files(root: Path) -> Iterator[tuple[Path, os.stat_result]]:
    try:
        root_stat = root.stat(follow_symlinks=False)
    except OSError as error:
        raise ValueError(f"byte census path is unavailable: {root}") from error
    if root.is_symlink() or _is_reparse(root_stat):
        raise ValueError(f"byte census path must not be a symlink or reparse point: {root}")
    if stat.S_ISREG(root_stat.st_mode):
        yield root, root_stat
        return
    if not stat.S_ISDIR(root_stat.st_mode):
        raise ValueError(f"byte census path must be a regular file or directory: {root}")

    stack = [root]
    while stack:
        directory = stack.pop()
        try:
            with os.scandir(directory) as entries:
                ordered = sorted(entries, key=lambda entry: entry.name)
        except OSError as error:
            raise ValueError(f"byte census directory could not be read: {directory}") from error
        for entry in reversed(ordered):
            path = Path(entry.path)
            try:
                entry_stat = entry.stat(follow_symlinks=False)
            except OSError as error:
                raise ValueError(f"byte census entry could not be witnessed: {path}") from error
            if entry.is_symlink() or _is_reparse(entry_stat):
                raise ValueError(f"byte census entry must not be a link or reparse point: {path}")
            if stat.S_ISDIR(entry_stat.st_mode):
                stack.append(path)
            elif stat.S_ISREG(entry_stat.st_mode):
                yield path, entry_stat
            else:
                raise ValueError(f"byte census entry must be regular: {path}")


def _census_file_identity(
    file_path: Path,
    file_stat: os.stat_result,
) -> tuple[str, str]:
    """Return one stable identity after rejecting hard-link ambiguity.

    Windows may expose different ``st_ino`` values for the same file through
    ``Path.stat`` and ``DirEntry.stat``.  Reparse points are already rejected,
    so a strict resolved path is stable; rejecting multi-link files prevents a
    second pathname from making the same physical bytes count twice.
    """

    try:
        stable_stat = file_path.stat(follow_symlinks=False)
    except OSError as error:
        raise ValueError(f"byte census file could not be restated: {file_path}") from error
    if (
        stable_stat.st_size != file_stat.st_size
        or stable_stat.st_mtime_ns != file_stat.st_mtime_ns
    ):
        raise ValueError(f"byte census file changed while being witnessed: {file_path}")
    if stable_stat.st_nlink != 1:
        raise ValueError(
            f"byte census file must have exactly one hard link: {file_path}"
        )
    if stable_stat.st_ino:
        return ("inode", f"{stable_stat.st_dev}:{stable_stat.st_ino}")
    try:
        resolved = file_path.resolve(strict=True)
    except OSError as error:
        raise ValueError(f"byte census file could not be resolved: {file_path}") from error
    return ("path", os.path.normcase(str(resolved)))


def census_byte_categories(
    paths: CapacityBytePaths,
    *,
    scratch_peak_bytes: int,
    candidate_root: Path | None = None,
) -> ByteCategoryCensus:
    """Census exact files once, rejecting links, missing paths, and overlap."""

    if not isinstance(paths, CapacityBytePaths):
        raise TypeError("paths must be CapacityBytePaths")
    _require_non_negative(scratch_peak_bytes, label="scratch_peak_bytes")
    if candidate_root is not None and (
        not isinstance(candidate_root, Path) or not candidate_root.is_absolute()
    ):
        raise ValueError("candidate_root must be an absolute pathlib.Path or None")
    totals = {label: 0 for label, _ in paths.categories()}
    embedded_index_bytes = {
        os.path.normcase(os.path.abspath(os.fspath(path))): byte_count
        for path, byte_count in paths.raw_embedded_index_bytes
    }
    require_complete_embedded_index = bool(embedded_index_bytes)
    witnessed_physical_bytes = 0
    witnessed: dict[tuple[str, str], tuple[str, Path]] = {}
    for label, roots in paths.categories():
        for root in roots:
            for file_path, file_stat in _regular_files(root):
                identity = _census_file_identity(file_path, file_stat)
                previous = witnessed.get(identity)
                if previous is not None:
                    previous_label, previous_path = previous
                    raise ValueError(
                        "file belongs to more than one byte category: "
                        f"{previous_label}:{previous_path} and {label}:{file_path}"
                    )
                witnessed[identity] = (label, file_path)
                physical_bytes = int(file_stat.st_size)
                witnessed_physical_bytes += physical_bytes
                if label == "raw_segments_bytes":
                    normalized = os.path.normcase(
                        os.path.abspath(os.fspath(file_path))
                    )
                    embedded_bytes = embedded_index_bytes.pop(normalized, None)
                    if require_complete_embedded_index and embedded_bytes is None:
                        raise ValueError(
                            "raw segment file lacks authenticated embedded index bytes: "
                            f"{file_path}"
                        )
                    if embedded_bytes is not None:
                        if embedded_bytes >= physical_bytes:
                            raise ValueError(
                                "embedded raw index bytes must be smaller than the "
                                f"physical raw segment: {file_path}"
                            )
                        totals[label] += physical_bytes - embedded_bytes
                        totals["raw_index_bytes"] += embedded_bytes
                        continue
                totals[label] += physical_bytes
    if embedded_index_bytes:
        raise ValueError(
            "embedded raw index paths were not witnessed as raw segment files: "
            f"{sorted(embedded_index_bytes)!r}"
        )
    if sum(totals.values()) != witnessed_physical_bytes:
        raise ValueError("embedded raw index reclassification changed physical total")
    if candidate_root is not None:
        candidate_identities: dict[tuple[str, str], Path] = {}
        for file_path, file_stat in _regular_files(candidate_root):
            identity = _census_file_identity(file_path, file_stat)
            candidate_identities[identity] = file_path
        unassigned = [
            path
            for identity, path in candidate_identities.items()
            if identity not in witnessed
        ]
        outside = [
            path
            for identity, (_, path) in witnessed.items()
            if identity not in candidate_identities
        ]
        if unassigned or outside:
            raise ValueError(
                "byte census does not exactly cover candidate_root; "
                f"unassigned={[str(path) for path in unassigned]!r}, "
                f"outside={[str(path) for path in outside]!r}"
            )
    return ByteCategoryCensus(
        raw_segments_bytes=totals["raw_segments_bytes"],
        raw_manifests_bytes=totals["raw_manifests_bytes"],
        raw_index_bytes=totals["raw_index_bytes"],
        paper_segments_bytes=totals["paper_segments_bytes"],
        paper_overlay_bytes=totals["paper_overlay_bytes"],
        paper_checkpoints_bytes=totals["paper_checkpoints_bytes"],
        paper_manifests_bytes=totals["paper_manifests_bytes"],
        raw_anchors_witnesses_bytes=totals["raw_anchors_witnesses_bytes"],
        paper_anchors_witnesses_bytes=totals["paper_anchors_witnesses_bytes"],
        raw_current_cache_bytes=totals["raw_current_cache_bytes"],
        paper_current_cache_bytes=totals["paper_current_cache_bytes"],
        scratch_current_bytes=totals["scratch_current_bytes"],
        scratch_peak_bytes=scratch_peak_bytes,
    )


@dataclass(frozen=True, slots=True)
class DurationObservations:
    observations_ns: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if type(self.observations_ns) is not tuple:
            raise TypeError("observations_ns must be a tuple")
        for observation in self.observations_ns:
            _require_non_negative(observation, label="duration observation")

    def payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "count": len(self.observations_ns),
            "observations_ns": list(self.observations_ns),
            "total_ns": sum(self.observations_ns),
        }
        if len(self.observations_ns) < MIN_PERCENTILE_OBSERVATIONS:
            payload["percentiles_status"] = "UNAVAILABLE_INSUFFICIENT_OBSERVATIONS"
            payload["minimum_observations_required"] = MIN_PERCENTILE_OBSERVATIONS
            return payload
        ordered = sorted(self.observations_ns)
        payload.update(
            {
                "p50_ns": ordered[((50 * len(ordered)) + 99) // 100 - 1],
                "p95_ns": ordered[((95 * len(ordered)) + 99) // 100 - 1],
                "p99_ns": ordered[((99 * len(ordered)) + 99) // 100 - 1],
                "percentile_method": "NEAREST_RANK",
                "percentiles_status": "AVAILABLE_NEAREST_RANK",
            }
        )
        return payload


def _decimal_fraction(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        raise ValueError("decimal fraction denominator must be positive")
    with localcontext() as context:
        context.prec = 50
        value = Decimal(numerator) / Decimal(denominator)
        rendered = format(value, ".12f").rstrip("0").rstrip(".")
    return rendered or "0"


@dataclass(frozen=True, slots=True)
class StorageGrowthAssessment:
    status: str
    basis: str
    gib_per_hour: str | None
    bytes_per_hour: str | None
    passed: bool | None

    def payload(self) -> dict[str, object]:
        return {
            "basis": self.basis,
            "bytes_per_hour": self.bytes_per_hour,
            "gib_per_hour": self.gib_per_hour,
            "passed": self.passed,
            "relation": "<",
            "status": self.status,
            "target_gib_per_hour": "0.20",
        }


def assess_storage_growth(
    *,
    total_bytes: int,
    commit_count: int,
    logical_span_ns: int | None = None,
    commits_per_hour: int | None = None,
) -> StorageGrowthAssessment:
    """Evaluate the strict 0.20 GiB/h target only from a declared time basis."""

    _require_non_negative(total_bytes, label="total_bytes")
    _require_positive(commit_count, label="commit_count")
    if logical_span_ns is not None and commits_per_hour is not None:
        raise ValueError("logical_span_ns and commits_per_hour are mutually exclusive")
    if logical_span_ns is None and commits_per_hour is None:
        return StorageGrowthAssessment(
            status="UNAVAILABLE_SPAN_AND_RATE_UNDEFINED",
            basis="UNAVAILABLE",
            gib_per_hour=None,
            bytes_per_hour=None,
            passed=None,
        )
    if logical_span_ns is not None:
        _require_positive(logical_span_ns, label="logical_span_ns")
        numerator = total_bytes * _NANOSECONDS_PER_HOUR
        denominator = logical_span_ns
        basis = "LOGICAL_SPAN"
    else:
        assert commits_per_hour is not None
        _require_positive(commits_per_hour, label="commits_per_hour")
        numerator = total_bytes * commits_per_hour
        denominator = commit_count
        basis = "COMMITS_PER_HOUR"
    passed = numerator * 5 < denominator * GIB_BYTES
    return StorageGrowthAssessment(
        status="AVAILABLE",
        basis=basis,
        gib_per_hour=_decimal_fraction(numerator, denominator * GIB_BYTES),
        bytes_per_hour=_decimal_fraction(numerator, denominator),
        passed=passed,
    )


@dataclass(frozen=True, slots=True)
class CapacityMeasurement:
    workload_manifest_sha256: str
    observed_workload_sha256: str
    commit_count: int
    logical_row_count: int
    wall_ns: int
    cpu_ns: int
    peak_rss_bytes: int | None
    byte_census: ByteCategoryCensus
    segment_count: int
    checkpoint_count: int
    manifest_count: int
    startup_ns: int
    startup_historical_segments_read: int
    startup_historical_commits_replayed: int
    startup_tail_entries_replayed: int
    metadata_authentication_ns: int
    full_history_audit_ns: int
    seal_durations: DurationObservations = field(default_factory=DurationObservations)
    checkpoint_durations: DurationObservations = field(default_factory=DurationObservations)
    manifest_publish_durations: DurationObservations = field(default_factory=DurationObservations)
    logical_span_ns: int | None = None
    commits_per_hour: int | None = None
    raw_input_bytes: int | None = None
    cumulative_bytes_written: int | None = None

    def __post_init__(self) -> None:
        _require_sha256(self.workload_manifest_sha256, label="workload_manifest_sha256")
        _require_sha256(self.observed_workload_sha256, label="observed_workload_sha256")
        _require_positive(self.commit_count, label="commit_count")
        _require_positive(self.logical_row_count, label="logical_row_count")
        _require_positive(self.wall_ns, label="wall_ns")
        _require_non_negative(self.cpu_ns, label="cpu_ns")
        if self.peak_rss_bytes is not None:
            _require_non_negative(self.peak_rss_bytes, label="peak_rss_bytes")
        if not isinstance(self.byte_census, ByteCategoryCensus):
            raise TypeError("byte_census must be ByteCategoryCensus")
        for label, value in (
            ("segment_count", self.segment_count),
            ("checkpoint_count", self.checkpoint_count),
            ("manifest_count", self.manifest_count),
            ("startup_ns", self.startup_ns),
            ("startup_historical_segments_read", self.startup_historical_segments_read),
            ("startup_historical_commits_replayed", self.startup_historical_commits_replayed),
            ("startup_tail_entries_replayed", self.startup_tail_entries_replayed),
            ("metadata_authentication_ns", self.metadata_authentication_ns),
            ("full_history_audit_ns", self.full_history_audit_ns),
        ):
            _require_non_negative(value, label=label)
        for label, observations in (
            ("seal_durations", self.seal_durations),
            ("checkpoint_durations", self.checkpoint_durations),
            ("manifest_publish_durations", self.manifest_publish_durations),
        ):
            if not isinstance(observations, DurationObservations):
                raise TypeError(f"{label} must be DurationObservations")
        if self.logical_span_ns is not None:
            _require_positive(self.logical_span_ns, label="logical_span_ns")
        if self.commits_per_hour is not None:
            _require_positive(self.commits_per_hour, label="commits_per_hour")
        if self.logical_span_ns is not None and self.commits_per_hour is not None:
            raise ValueError("logical_span_ns and commits_per_hour are mutually exclusive")
        if self.raw_input_bytes is not None:
            _require_non_negative(self.raw_input_bytes, label="raw_input_bytes")
        if self.cumulative_bytes_written is not None:
            _require_non_negative(
                self.cumulative_bytes_written,
                label="cumulative_bytes_written",
            )

    @property
    def storage_growth(self) -> StorageGrowthAssessment:
        return assess_storage_growth(
            total_bytes=self.byte_census.total_bytes,
            commit_count=self.commit_count,
            logical_span_ns=self.logical_span_ns,
            commits_per_hour=self.commits_per_hour,
        )

    def payload(self) -> dict[str, object]:
        rss: dict[str, object]
        if self.peak_rss_bytes is None:
            rss = {"status": "UNAVAILABLE", "peak_bytes": None}
        else:
            rss = {"status": "AVAILABLE", "peak_bytes": self.peak_rss_bytes}
        if self.raw_input_bytes is None or self.raw_input_bytes == 0:
            retained_ratio: dict[str, object] = {
                "status": "UNAVAILABLE_INPUT_BYTES_UNDEFINED_OR_ZERO",
                "ratio": None,
            }
        else:
            retained_ratio = {
                "status": "AVAILABLE",
                "ratio": _decimal_fraction(
                    self.byte_census.total_bytes,
                    self.raw_input_bytes,
                ),
            }
        if (
            self.raw_input_bytes is None
            or self.raw_input_bytes == 0
            or self.cumulative_bytes_written is None
        ):
            write_amplification: dict[str, object] = {
                "status": "UNAVAILABLE_CUMULATIVE_BYTES_WRITTEN_NOT_MEASURED",
                "ratio": None,
            }
        else:
            write_amplification = {
                "status": "AVAILABLE",
                "ratio": _decimal_fraction(
                    self.cumulative_bytes_written,
                    self.raw_input_bytes,
                ),
            }
        return {
            "byte_census": self.byte_census.payload(),
            "counts": {
                "checkpoints": self.checkpoint_count,
                "commits": self.commit_count,
                "logical_rows": self.logical_row_count,
                "manifests": self.manifest_count,
                "segments": self.segment_count,
            },
            "cpu_ns": self.cpu_ns,
            "durations": {
                "checkpoint": self.checkpoint_durations.payload(),
                "manifest_publication": self.manifest_publish_durations.payload(),
                "seal": self.seal_durations.payload(),
            },
            "full_history_audit_ns": self.full_history_audit_ns,
            "markers": list(CAPACITY_MARKERS),
            "metadata_authentication_ns": self.metadata_authentication_ns,
            "observed_workload_sha256": self.observed_workload_sha256,
            "rss": rss,
            "startup": {
                "duration_ns": self.startup_ns,
                "historical_commits_replayed": self.startup_historical_commits_replayed,
                "historical_segments_read": self.startup_historical_segments_read,
                "tail_entries_replayed": self.startup_tail_entries_replayed,
            },
            "storage_growth_target": self.storage_growth.payload(),
            "throughput": {
                "bytes_per_commit": _decimal_fraction(
                    self.byte_census.total_bytes,
                    self.commit_count,
                ),
                "bytes_per_logical_row": _decimal_fraction(
                    self.byte_census.total_bytes,
                    self.logical_row_count,
                ),
                "gib_per_million_commits": _decimal_fraction(
                    self.byte_census.total_bytes * 1_000_000,
                    self.commit_count * GIB_BYTES,
                ),
                "commits_per_second": _decimal_fraction(
                    self.commit_count * 1_000_000_000,
                    self.wall_ns,
                ),
                "logical_rows_per_second": _decimal_fraction(
                    self.logical_row_count * 1_000_000_000,
                    self.wall_ns,
                ),
            },
            "wall_ns": self.wall_ns,
            "workload_manifest_sha256": self.workload_manifest_sha256,
            "retained_bytes_per_raw_input_byte": retained_ratio,
            "write_amplification": write_amplification,
        }


def _ratio_or_unavailable(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "UNAVAILABLE_ZERO_BASELINE"
    return _decimal_fraction(numerator, denominator)


def compute_capacity_scaling(
    measurements: Iterable[CapacityMeasurement],
) -> dict[str, object]:
    """Compare ordered capacity levels without converting failed targets into gates."""

    levels = tuple(measurements)
    for level in levels:
        if not isinstance(level, CapacityMeasurement):
            raise TypeError("measurements must contain CapacityMeasurement values")
    for previous, current in pairwise(levels):
        if current.commit_count <= previous.commit_count:
            raise ValueError("capacity scaling levels must have strictly increasing commit counts")
    points: list[dict[str, object]] = []
    for level in levels:
        points.append(
            {
                "audit_ns": level.full_history_audit_ns,
                "bytes_per_commit": _decimal_fraction(
                    level.byte_census.total_bytes,
                    level.commit_count,
                ),
                "checkpoint_total_ns": sum(level.checkpoint_durations.observations_ns),
                "commit_count": level.commit_count,
                "cpu_ns": level.cpu_ns,
                "file_count": level.segment_count + level.checkpoint_count + level.manifest_count,
                "peak_rss_bytes": level.peak_rss_bytes,
                "startup_ns": level.startup_ns,
                "storage_growth_target": level.storage_growth.payload(),
                "tail_entries_replayed": level.startup_tail_entries_replayed,
                "total_bytes": level.byte_census.total_bytes,
                "wall_ns": level.wall_ns,
            }
        )
    transitions: list[dict[str, object]] = []
    for previous, current in pairwise(levels):
        rss_multiplier = (
            "UNAVAILABLE_RSS"
            if previous.peak_rss_bytes is None or current.peak_rss_bytes is None
            else _ratio_or_unavailable(current.peak_rss_bytes, previous.peak_rss_bytes)
        )
        transitions.append(
            {
                "audit_multiplier": _ratio_or_unavailable(
                    current.full_history_audit_ns,
                    previous.full_history_audit_ns,
                ),
                "bytes_per_commit_multiplier": _ratio_or_unavailable(
                    current.byte_census.total_bytes * previous.commit_count,
                    previous.byte_census.total_bytes * current.commit_count,
                ),
                "checkpoint_cost_multiplier": _ratio_or_unavailable(
                    sum(current.checkpoint_durations.observations_ns),
                    sum(previous.checkpoint_durations.observations_ns),
                ),
                "cpu_multiplier": _ratio_or_unavailable(current.cpu_ns, previous.cpu_ns),
                "file_count_delta": (
                    current.segment_count
                    + current.checkpoint_count
                    + current.manifest_count
                    - previous.segment_count
                    - previous.checkpoint_count
                    - previous.manifest_count
                ),
                "from_commit_count": previous.commit_count,
                "peak_rss_multiplier": rss_multiplier,
                "startup_multiplier": _ratio_or_unavailable(
                    current.startup_ns,
                    previous.startup_ns,
                ),
                "tail_entries_delta": (
                    current.startup_tail_entries_replayed
                    - previous.startup_tail_entries_replayed
                ),
                "to_commit_count": current.commit_count,
                "wall_multiplier": _ratio_or_unavailable(current.wall_ns, previous.wall_ns),
            }
        )
    return {
        "markers": list(CAPACITY_MARKERS),
        "points": points,
        "status": "AVAILABLE" if len(levels) >= 2 else "UNAVAILABLE_INSUFFICIENT_LEVELS",
        "transitions": transitions,
    }


@dataclass(frozen=True, slots=True)
class CanonicalArtifact:
    canonical_bytes: bytes

    def __post_init__(self) -> None:
        if type(self.canonical_bytes) is not bytes:
            raise TypeError("canonical_bytes must be exact bytes")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @property
    def canonical_jsonl_bytes(self) -> bytes:
        return self.canonical_bytes + b"\n"


def build_capacity_report_artifact(
    *,
    status: str,
    manifest: CapacityWorkloadManifest,
    measurement: CapacityMeasurement,
    scaling: Mapping[str, object],
    limitations: tuple[str, ...] = (),
) -> CanonicalArtifact:
    """Build canonical report bytes linked to one exact workload manifest."""

    _require_text(status, label="status")
    if not isinstance(manifest, CapacityWorkloadManifest):
        raise TypeError("manifest must be CapacityWorkloadManifest")
    if not isinstance(measurement, CapacityMeasurement):
        raise TypeError("measurement must be CapacityMeasurement")
    if type(limitations) is not tuple:
        raise TypeError("limitations must be a tuple")
    for limitation in limitations:
        _require_text(limitation, label="limitation")
    if measurement.workload_manifest_sha256 != manifest.sha256:
        # A caller may construct a report for a separately measured level only
        # after it explicitly binds the measurement to this manifest.
        raise ValueError("measurement is not linked to the supplied manifest")
    if measurement.observed_workload_sha256 != manifest.workload_sha256:
        raise ValueError("measurement workload digest differs from the manifest")
    payload = {
        "artifact": "STORAGE_V4_PHASE1C_CAPACITY_REPORT_V1",
        "limitations": list(limitations),
        "manifest": {
            "commit_count": manifest.commit_count,
            "logical_row_count": manifest.logical_row_count,
            "sha256": manifest.sha256,
            "workload_sha256": manifest.workload_sha256,
        },
        "markers": list(CAPACITY_MARKERS),
        "measurement": measurement.payload(),
        "scaling": dict(scaling),
        "status": status,
    }
    return CanonicalArtifact(canonical_json_bytes(payload))


def build_capacity_complete_artifact(
    *,
    status: str,
    manifest: CapacityWorkloadManifest,
    report: CanonicalArtifact,
) -> CanonicalArtifact:
    """Build terminal link bytes; publication ordering remains the runner's duty."""

    _require_text(status, label="status")
    if not isinstance(manifest, CapacityWorkloadManifest):
        raise TypeError("manifest must be CapacityWorkloadManifest")
    if not isinstance(report, CanonicalArtifact):
        raise TypeError("report must be CanonicalArtifact")
    return CanonicalArtifact(
        canonical_json_bytes(
            {
                "artifact": "STORAGE_V4_PHASE1C_CAPACITY_COMPLETE_V1",
                "manifest_sha256": manifest.sha256,
                "markers": list(CAPACITY_MARKERS),
                "report_sha256": report.sha256,
                "status": status,
                "workload_sha256": manifest.workload_sha256,
            }
        )
    )


@runtime_checkable
class CapacityWorkloadRunner(Protocol):
    """Native-coordinator adapter boundary; it contains no concrete storage policy."""

    def run_capacity_workload(
        self,
        *,
        manifest: CapacityWorkloadManifest,
        commits: Iterable[SyntheticCapacityCommit],
    ) -> CapacityMeasurement: ...


@dataclass(frozen=True, slots=True)
class CapacityRunResult:
    manifest: CapacityWorkloadManifest
    measurement: CapacityMeasurement


def run_capacity_workload(
    *,
    config: CapacityWorkloadConfig,
    runner: CapacityWorkloadRunner,
) -> CapacityRunResult:
    """Bind a precomputed manifest to a fresh streaming generator and validate output."""

    manifest = build_capacity_workload_manifest(config)
    measurement = runner.run_capacity_workload(
        manifest=manifest,
        commits=iter_capacity_commits(config),
    )
    if not isinstance(measurement, CapacityMeasurement):
        raise TypeError("capacity runner must return CapacityMeasurement")
    if measurement.workload_manifest_sha256 != manifest.sha256:
        raise ValueError("capacity runner returned a different manifest SHA-256")
    if measurement.observed_workload_sha256 != manifest.workload_sha256:
        raise ValueError("capacity runner returned a divergent workload SHA-256")
    if measurement.commit_count != manifest.commit_count:
        raise ValueError("capacity runner returned a divergent commit count")
    if measurement.logical_row_count != manifest.logical_row_count:
        raise ValueError("capacity runner returned a divergent logical row count")
    return CapacityRunResult(manifest=manifest, measurement=measurement)


__all__ = [
    "CAPACITY_MARKERS",
    "GENERATOR_VERSION",
    "GIB_BYTES",
    "MIN_PERCENTILE_OBSERVATIONS",
    "NOT_ALPHA_EVIDENCE",
    "NOT_ECONOMIC_EVIDENCE",
    "PAPER_ONLY",
    "SYNTHETIC_CAPACITY_WORKLOAD",
    "ByteCategoryCensus",
    "CanonicalArtifact",
    "CapacityBytePaths",
    "CapacityMeasurement",
    "CapacityProfile",
    "CapacityRunResult",
    "CapacityTypeSpec",
    "CapacityWorkloadConfig",
    "CapacityWorkloadDigest",
    "CapacityWorkloadHasher",
    "CapacityWorkloadManifest",
    "CapacityWorkloadRunner",
    "DurationObservations",
    "StorageGrowthAssessment",
    "SyntheticCapacityCommit",
    "SyntheticCapacityRow",
    "SyntheticPayload",
    "assess_storage_growth",
    "build_capacity_complete_artifact",
    "build_capacity_report_artifact",
    "build_capacity_workload_manifest",
    "census_byte_categories",
    "compute_capacity_scaling",
    "iter_capacity_commits",
    "run_capacity_workload",
]
