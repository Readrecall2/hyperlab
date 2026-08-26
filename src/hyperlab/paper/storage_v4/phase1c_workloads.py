"""Pure production workload and assessment contracts for Storage v4 Phase 1C.

The workload fixtures are deterministic, streaming, visibly synthetic and
PAPER-only.  This module performs no I/O and publishes no evidence.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation, localcontext
from enum import StrEnum
from typing import Protocol

from .capacity import (
    CAPACITY_MARKERS,
    MAX_SYNTHETIC_PAYLOAD_BYTES,
    ByteCategoryCensus,
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
    CapacityWorkloadHasher,
    CapacityWorkloadManifest,
    StorageGrowthAssessment,
    build_capacity_workload_manifest,
    iter_capacity_commits,
)
from .capacity_shape import GoldenCapacityShape

PHASE1C_WORKLOAD_SEED = 20_260_825
PHASE1C_CAPACITY_LEVELS = (100_000, 500_000, 1_000_000)
PHASE1C_TERMINAL_LEVEL = 1_000_000
PHASE1C_TAIL_COMMIT_COUNT = 20_001
PHASE1C_BOUNDED_TAIL_MAX = 20_000
PHASE1C_TAIL_RESTART_SIZES = (0, 1, 100, 10_000, 20_000)
PHASE1C_ADVERSARIAL_COMMIT_COUNT = 20_000
PHASE1C_PRODUCTION_BATCH_COMMITS = 10_000
PHASE1C_PRODUCTION_CHECKPOINT_EVERY_BATCHES = 1
PHASE1C_MANIFEST_PROGRESS_EVERY_COMMITS = 10_000

CANONICAL_TARGET_GIB_PER_HOUR = "0.20"
TARGET_RELATION = "<"
TARGET_MET = "TARGET_MET"
TARGET_NOT_MET = "TARGET_NOT_MET"

PHASE1C_PROVEN_VERDICT = "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_PROVEN"
PHASE1C_TARGET_NOT_MET_VERDICT = "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_CHARACTERIZED_TARGET_NOT_MET"
PHASE1C_NO_CANONICAL_TARGET_VERDICT = "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_CHARACTERIZED_NO_CANONICAL_TARGET"
PHASE1C_TERMINAL_VERDICTS = frozenset(
    {
        PHASE1C_PROVEN_VERDICT,
        PHASE1C_TARGET_NOT_MET_VERDICT,
        PHASE1C_NO_CANONICAL_TARGET_VERDICT,
    }
)
PHASE1C_TARGET_STATUSES = frozenset({TARGET_MET, TARGET_NOT_MET})

ORIGINAL_V3_SOURCE_BYTES = 2_014_072_832
GOLDEN_EXPORT_PHYSICAL_BYTES = 2_456_283_751
PHASE1B_STORAGE_V4_STORE_BYTES = 528_250_030
PHASE1B_ANCHOR_BYTES = 12_288
PHASE1B_STORE_PLUS_ANCHOR_BYTES = PHASE1B_STORAGE_V4_STORE_BYTES + PHASE1B_ANCHOR_BYTES
PHASE1B_COMPATIBILITY_SEGMENT_BYTES = 317_492_777

_SHA256_LENGTH = 64


def _require_sha256(value: str, *, label: str) -> None:
    if (
        type(value) is not str
        or len(value) != _SHA256_LENGTH
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256")


def _require_census(value: ByteCategoryCensus, *, label: str) -> None:
    if not isinstance(value, ByteCategoryCensus):
        raise TypeError(f"{label} must be ByteCategoryCensus")


def _manifest_summary(manifest: CapacityWorkloadManifest) -> dict[str, object]:
    return {
        "commit_count": manifest.commit_count,
        "manifest_sha256": manifest.sha256,
        "profile": manifest.config.profile.value,
        "workload_sha256": manifest.workload_sha256,
    }


def _market_gap_sequence(*, commit_count: int, count: int, ordinal: int) -> int:
    if not 0 <= ordinal < count:
        raise ValueError("MARKET_GAP ordinal is outside the configured schedule")
    return ((ordinal * commit_count) // count) + 1


def _require_exact_cumulative_prefix_configs(
    manifests: tuple[CapacityWorkloadManifest, ...],
) -> None:
    """Prove the boundary configs generate exact prefixes of one terminal stream."""

    if tuple(manifest.commit_count for manifest in manifests) != PHASE1C_CAPACITY_LEVELS:
        raise ValueError("cumulative manifests must cover the exact Phase 1C boundaries")
    terminal = manifests[-1].config
    if terminal.commit_count != PHASE1C_TERMINAL_LEVEL:
        raise ValueError("cumulative terminal manifest must contain one million commits")

    for manifest in manifests:
        config = manifest.config
        expected = replace(
            terminal,
            commit_count=config.commit_count,
            market_gap_count=config.market_gap_count,
        )
        if config != expected:
            raise ValueError(
                "capacity boundary configs differ outside commit_count and MARKET_GAP count"
            )
        terminal_prefix_gap_count = sum(
            1
            for ordinal in range(terminal.market_gap_count)
            if _market_gap_sequence(
                commit_count=terminal.commit_count,
                count=terminal.market_gap_count,
                ordinal=ordinal,
            )
            <= config.commit_count
        )
        if terminal_prefix_gap_count != config.market_gap_count:
            raise ValueError("capacity MARKET_GAP count is not the exact terminal prefix count")
        for ordinal in range(config.market_gap_count):
            boundary_sequence = _market_gap_sequence(
                commit_count=config.commit_count,
                count=config.market_gap_count,
                ordinal=ordinal,
            )
            terminal_sequence = _market_gap_sequence(
                commit_count=terminal.commit_count,
                count=terminal.market_gap_count,
                ordinal=ordinal,
            )
            if boundary_sequence != terminal_sequence:
                raise ValueError("capacity MARKET_GAP schedule is not an exact terminal prefix")


def _cumulative_prefix_proof_payload(
    manifests: tuple[CapacityWorkloadManifest, ...],
) -> dict[str, object]:
    _require_exact_cumulative_prefix_configs(manifests)
    terminal = manifests[-1]
    return {
        "artifact": "STORAGE_V4_PHASE1C_CUMULATIVE_PREFIX_PLAN_V1",
        "boundaries": [
            {
                "commit_count": manifest.commit_count,
                "logical_row_count": manifest.logical_row_count,
                "manifest_sha256": manifest.sha256,
                "market_gap_count": manifest.config.market_gap_count,
                "workload_prefix_sha256": manifest.workload_sha256,
            }
            for manifest in manifests
        ],
        "configuration_compatibility": {
            "allowed_boundary_fields": ["commit_count", "market_gap_count"],
            "market_gap_schedule": "EXACT_TERMINAL_PREFIX",
            "profile": terminal.config.profile.value,
            "same_configuration_outside_boundary_fields": True,
            "seed": terminal.config.seed,
        },
        "execution_contract": {
            "commits_generated": PHASE1C_TERMINAL_LEVEL,
            "commits_ingested": PHASE1C_TERMINAL_LEVEL,
            "prefix_commits_reingested": 0,
            "store_count": 1,
            "stream_count": 1,
            "worker_count": 1,
        },
        "terminal_manifest_sha256": terminal.sha256,
        "terminal_workload_sha256": terminal.workload_sha256,
    }


def _adversarial_config(shape: GoldenCapacityShape) -> CapacityWorkloadConfig:
    """Build a bounded fixture aligned to 10k production batch/checkpoints."""

    return CapacityWorkloadConfig(
        profile=CapacityProfile.ADVERSARIAL_STORAGE,
        seed=PHASE1C_WORKLOAD_SEED,
        commit_count=PHASE1C_ADVERSARIAL_COMMIT_COUNT,
        start_time_ns=shape.start_time_ns,
        cadence_ns=shape.cadence_ns,
        type_distribution=(
            CapacityTypeSpec(
                record_type="PUBLIC_MARKET_EVENT",
                stream="inbox",
                weight=9_998,
                payload_min_bytes=64,
                payload_max_bytes=4_096,
                payload_cardinality=10_000,
            ),
            CapacityTypeSpec(
                record_type="PUBLIC_SOURCE_FAILURE",
                stream="inbox",
                weight=1,
                payload_min_bytes=1,
                payload_max_bytes=MAX_SYNTHETIC_PAYLOAD_BYTES,
                payload_cardinality=10_000,
            ),
            CapacityTypeSpec(
                record_type="PUBLIC_FUNDING_SETTLEMENT",
                stream="inbox",
                weight=1,
                payload_min_bytes=32,
                payload_max_bytes=512,
                payload_cardinality=1,
            ),
        ),
        strategies=shape.strategies,
        alert_every_commits=PHASE1C_PRODUCTION_BATCH_COMMITS,
        incident_every_commits=PHASE1C_ADVERSARIAL_COMMIT_COUNT,
        ledger_every_commits=PHASE1C_PRODUCTION_BATCH_COMMITS,
        market_gap_count=2,
        alert_payload_bytes=1_024,
        incident_payload_bytes=4_096,
        ledger_payload_bytes=2_048,
        market_gap_payload_bytes=8_192,
        projection_every_commits=PHASE1C_PRODUCTION_BATCH_COMMITS,
        projection_payload_bytes=16_384,
        adversarial_boundary_intervals=(
            PHASE1C_PRODUCTION_BATCH_COMMITS,
            PHASE1C_ADVERSARIAL_COMMIT_COUNT,
        ),
    )


@dataclass(frozen=True, slots=True)
class Phase1CWorkloadSuite:
    """The exact production manifests frozen for one Golden-derived shape."""

    golden_shape_sha256: str
    seed: int
    golden_shaped_manifests: tuple[CapacityWorkloadManifest, ...]
    bounded_tail_restart_manifest: CapacityWorkloadManifest
    adversarial_storage_manifest: CapacityWorkloadManifest

    def __post_init__(self) -> None:
        _require_sha256(self.golden_shape_sha256, label="golden_shape_sha256")
        if type(self.seed) is not int or self.seed < 0:
            raise ValueError("seed must be a non-negative exact integer")
        if type(self.golden_shaped_manifests) is not tuple:
            raise TypeError("golden_shaped_manifests must be a tuple")
        if (
            tuple(manifest.commit_count for manifest in self.golden_shaped_manifests)
            != PHASE1C_CAPACITY_LEVELS
        ):
            raise ValueError("Golden-shaped manifests must be the exact production levels")
        for manifest in self.golden_shaped_manifests:
            self._validate_manifest(
                manifest,
                profile=CapacityProfile.GOLDEN_SHAPED,
                label="golden_shaped_manifest",
            )
            if manifest.config.golden_census_sha256 != self.golden_shape_sha256:
                raise ValueError("Golden-shaped manifest is not bound to this Golden shape")
        _require_exact_cumulative_prefix_configs(self.golden_shaped_manifests)

        self._validate_manifest(
            self.bounded_tail_restart_manifest,
            profile=CapacityProfile.BOUNDED_TAIL_RESTART,
            label="bounded_tail_restart_manifest",
        )
        tail = self.bounded_tail_restart_manifest
        if tail.commit_count != PHASE1C_TAIL_COMMIT_COUNT:
            raise ValueError("bounded-tail manifest has the wrong commit count")
        if tail.config.golden_census_sha256 != self.golden_shape_sha256:
            raise ValueError("bounded-tail manifest is not fact-derived from this Golden shape")
        if tail.config.tail_restart_sizes != PHASE1C_TAIL_RESTART_SIZES:
            raise ValueError("bounded-tail manifest does not expose the exact restart matrix")

        self._validate_manifest(
            self.adversarial_storage_manifest,
            profile=CapacityProfile.ADVERSARIAL_STORAGE,
            label="adversarial_storage_manifest",
        )
        adversarial = self.adversarial_storage_manifest
        if adversarial.commit_count != PHASE1C_ADVERSARIAL_COMMIT_COUNT:
            raise ValueError("adversarial manifest has the wrong commit count")
        expected_boundaries = (
            PHASE1C_PRODUCTION_BATCH_COMMITS,
            PHASE1C_ADVERSARIAL_COMMIT_COUNT,
        )
        if adversarial.config.adversarial_boundary_intervals != expected_boundaries:
            raise ValueError("adversarial boundaries differ from production boundaries")

    def _validate_manifest(
        self,
        manifest: CapacityWorkloadManifest,
        *,
        profile: CapacityProfile,
        label: str,
    ) -> None:
        if not isinstance(manifest, CapacityWorkloadManifest):
            raise TypeError(f"{label} must be CapacityWorkloadManifest")
        if manifest.config.profile is not profile:
            raise ValueError(f"{label} has the wrong profile")
        if manifest.config.seed != self.seed:
            raise ValueError(f"{label} has the wrong fixed seed")
        if manifest.payload().get("markers") != list(CAPACITY_MARKERS):
            raise ValueError(f"{label} is missing visible synthetic markers")

    @property
    def capacity_level_manifests(self) -> dict[int, CapacityWorkloadManifest]:
        return {manifest.commit_count: manifest for manifest in self.golden_shaped_manifests}

    @property
    def all_manifests(self) -> tuple[CapacityWorkloadManifest, ...]:
        return (
            *self.golden_shaped_manifests,
            self.bounded_tail_restart_manifest,
            self.adversarial_storage_manifest,
        )

    def payload(self) -> dict[str, object]:
        return {
            "artifact": "STORAGE_V4_PHASE1C_PRODUCTION_WORKLOAD_SUITE_V1",
            "adversarial_storage": _manifest_summary(self.adversarial_storage_manifest),
            "bounded_tail_restart": _manifest_summary(self.bounded_tail_restart_manifest),
            "golden_shape_sha256": self.golden_shape_sha256,
            "golden_shaped_levels": [
                _manifest_summary(manifest) for manifest in self.golden_shaped_manifests
            ],
            "golden_shaped_cumulative_prefix_proof": _cumulative_prefix_proof_payload(
                self.golden_shaped_manifests
            ),
            "markers": list(CAPACITY_MARKERS),
            "production_boundaries": {
                "batch_commits": PHASE1C_PRODUCTION_BATCH_COMMITS,
                "checkpoint_every_batches": (PHASE1C_PRODUCTION_CHECKPOINT_EVERY_BATCHES),
            },
            "seed": self.seed,
        }


class Phase1CWorkloadProgressStatus(StrEnum):
    STARTED = "STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True, slots=True)
class Phase1CWorkloadProgress:
    manifest_label: str
    profile: CapacityProfile
    processed_commits: int
    total_commits: int
    processed_logical_rows: int
    total_logical_rows: int
    workload_elapsed_ns: int
    status: Phase1CWorkloadProgressStatus

    def __post_init__(self) -> None:
        if type(self.manifest_label) is not str or not self.manifest_label:
            raise ValueError("manifest_label must be non-empty text")
        if not isinstance(self.profile, CapacityProfile):
            raise TypeError("profile must be CapacityProfile")
        if (
            type(self.processed_commits) is not int
            or type(self.total_commits) is not int
            or not 0 <= self.processed_commits <= self.total_commits
            or self.total_commits < 1
        ):
            raise ValueError("progress commit counts are invalid")
        if (
            type(self.processed_logical_rows) is not int
            or type(self.total_logical_rows) is not int
            or not 0 <= self.processed_logical_rows <= self.total_logical_rows
            or self.total_logical_rows < self.total_commits
            or self.processed_logical_rows < self.processed_commits
            or self.total_logical_rows - self.processed_logical_rows
            < self.total_commits - self.processed_commits
        ):
            raise ValueError("progress logical row counts are invalid")
        if type(self.workload_elapsed_ns) is not int or self.workload_elapsed_ns < 0:
            raise ValueError("workload_elapsed_ns must be a non-negative exact integer")
        if not isinstance(self.status, Phase1CWorkloadProgressStatus):
            raise TypeError("status must be Phase1CWorkloadProgressStatus")
        if self.status is Phase1CWorkloadProgressStatus.STARTED:
            if (
                self.processed_commits != 0
                or self.processed_logical_rows != 0
                or self.workload_elapsed_ns != 0
            ):
                raise ValueError("STARTED progress must precede all workload activity")
        elif self.status is Phase1CWorkloadProgressStatus.COMPLETE:
            if (
                self.processed_commits != self.total_commits
                or self.processed_logical_rows != self.total_logical_rows
            ):
                raise ValueError("COMPLETE progress must cover the full workload")
        elif (
            self.processed_commits in {0, self.total_commits}
            or self.processed_logical_rows in {0, self.total_logical_rows}
        ):
            raise ValueError("IN_PROGRESS must be strictly inside the workload")


class Phase1CWorkloadProgressCallback(Protocol):
    def __call__(self, progress: Phase1CWorkloadProgress, /) -> None: ...


def _exact_logical_row_count(config: CapacityWorkloadConfig) -> int:
    intervals = (
        config.alert_every_commits,
        config.incident_every_commits,
        config.ledger_every_commits,
        config.projection_every_commits,
    )
    return (
        config.commit_count
        + sum(
            config.commit_count // interval
            for interval in intervals
            if interval is not None
        )
        + config.market_gap_count
    )


def _build_manifest_with_progress(
    config: CapacityWorkloadConfig,
    *,
    manifest_label: str,
    progress_callback: Phase1CWorkloadProgressCallback | None,
) -> CapacityWorkloadManifest:
    if progress_callback is None:
        return build_capacity_workload_manifest(config)
    total_logical_rows = _exact_logical_row_count(config)
    started_ns = time.monotonic_ns()
    previous_elapsed_ns = 0
    processed_logical_rows = 0
    progress_callback(
        Phase1CWorkloadProgress(
            manifest_label=manifest_label,
            profile=config.profile,
            processed_commits=0,
            total_commits=config.commit_count,
            processed_logical_rows=0,
            total_logical_rows=total_logical_rows,
            workload_elapsed_ns=0,
            status=Phase1CWorkloadProgressStatus.STARTED,
        )
    )
    hasher = CapacityWorkloadHasher()
    for commit in iter_capacity_commits(config):
        hasher.update(commit)
        processed_logical_rows += len(commit.rows)
        if (
            commit.sequence % PHASE1C_MANIFEST_PROGRESS_EVERY_COMMITS == 0
            and commit.sequence != config.commit_count
        ):
            elapsed_ns = time.monotonic_ns() - started_ns
            if elapsed_ns < previous_elapsed_ns:
                raise RuntimeError("manifest workload elapsed time regressed")
            previous_elapsed_ns = elapsed_ns
            progress_callback(
                Phase1CWorkloadProgress(
                    manifest_label=manifest_label,
                    profile=config.profile,
                    processed_commits=commit.sequence,
                    total_commits=config.commit_count,
                    processed_logical_rows=processed_logical_rows,
                    total_logical_rows=total_logical_rows,
                    workload_elapsed_ns=elapsed_ns,
                    status=Phase1CWorkloadProgressStatus.IN_PROGRESS,
                )
            )
    manifest = CapacityWorkloadManifest(config=config, digest=hasher.finalize())
    if (
        processed_logical_rows != total_logical_rows
        or manifest.logical_row_count != total_logical_rows
    ):
        raise RuntimeError("manifest logical row count differs from the exact workload total")
    elapsed_ns = time.monotonic_ns() - started_ns
    if elapsed_ns < previous_elapsed_ns:
        raise RuntimeError("manifest workload elapsed time regressed")
    progress_callback(
        Phase1CWorkloadProgress(
            manifest_label=manifest_label,
            profile=config.profile,
            processed_commits=config.commit_count,
            total_commits=config.commit_count,
            processed_logical_rows=processed_logical_rows,
            total_logical_rows=total_logical_rows,
            workload_elapsed_ns=elapsed_ns,
            status=Phase1CWorkloadProgressStatus.COMPLETE,
        )
    )
    return manifest


def _build_cumulative_golden_manifests(
    shape: GoldenCapacityShape,
    *,
    progress_callback: Phase1CWorkloadProgressCallback | None,
) -> tuple[CapacityWorkloadManifest, ...]:
    """Hash 100k, 500k and 1M as snapshots of one terminal generator pass."""

    configs = tuple(
        shape.workload_config(commit_count=level, seed=PHASE1C_WORKLOAD_SEED)
        for level in PHASE1C_CAPACITY_LEVELS
    )
    config_by_level = {config.commit_count: config for config in configs}
    totals = {config.commit_count: _exact_logical_row_count(config) for config in configs}
    started_ns = time.monotonic_ns()
    previous_elapsed = {level: 0 for level in PHASE1C_CAPACITY_LEVELS}
    if progress_callback is not None:
        for config in configs:
            progress_callback(
                Phase1CWorkloadProgress(
                    manifest_label=f"GOLDEN_SHAPED_{config.commit_count}",
                    profile=config.profile,
                    processed_commits=0,
                    total_commits=config.commit_count,
                    processed_logical_rows=0,
                    total_logical_rows=totals[config.commit_count],
                    workload_elapsed_ns=0,
                    status=Phase1CWorkloadProgressStatus.STARTED,
                )
            )

    terminal_config = configs[-1]
    hasher = CapacityWorkloadHasher()
    manifests: list[CapacityWorkloadManifest] = []
    processed_logical_rows = 0
    for commit in iter_capacity_commits(terminal_config):
        hasher.update(commit)
        processed_logical_rows += len(commit.rows)
        at_progress_interval = (
            commit.sequence % PHASE1C_MANIFEST_PROGRESS_EVERY_COMMITS == 0
        )
        for level in PHASE1C_CAPACITY_LEVELS:
            if commit.sequence > level:
                continue
            at_boundary = commit.sequence == level
            if progress_callback is not None and (at_progress_interval or at_boundary):
                elapsed_ns = time.monotonic_ns() - started_ns
                if elapsed_ns < previous_elapsed[level]:
                    raise RuntimeError("manifest workload elapsed time regressed")
                previous_elapsed[level] = elapsed_ns
                progress_callback(
                    Phase1CWorkloadProgress(
                        manifest_label=f"GOLDEN_SHAPED_{level}",
                        profile=terminal_config.profile,
                        processed_commits=commit.sequence,
                        total_commits=level,
                        processed_logical_rows=processed_logical_rows,
                        total_logical_rows=totals[level],
                        workload_elapsed_ns=elapsed_ns,
                        status=(
                            Phase1CWorkloadProgressStatus.COMPLETE
                            if at_boundary
                            else Phase1CWorkloadProgressStatus.IN_PROGRESS
                        ),
                    )
                )
            if at_boundary:
                digest = hasher.snapshot()
                if digest.logical_row_count != totals[level]:
                    raise RuntimeError(
                        "terminal workload prefix differs from boundary logical row count"
                    )
                manifests.append(
                    CapacityWorkloadManifest(
                        config=config_by_level[level],
                        digest=digest,
                    )
                )

    if hasher.finalize() != manifests[-1].digest:
        raise RuntimeError("terminal workload snapshot differs from the finalized digest")
    result = tuple(manifests)
    _require_exact_cumulative_prefix_configs(result)
    return result


def build_phase1c_workload_suite(
    shape: GoldenCapacityShape,
    *,
    progress_callback: Phase1CWorkloadProgressCallback | None = None,
) -> Phase1CWorkloadSuite:
    """Materialize deterministic manifests with bounded-memory generators."""

    if not isinstance(shape, GoldenCapacityShape):
        raise TypeError("shape must be GoldenCapacityShape")
    golden_manifests = _build_cumulative_golden_manifests(
        shape,
        progress_callback=progress_callback,
    )
    tail_config = replace(
        shape.workload_config(
            commit_count=PHASE1C_TAIL_COMMIT_COUNT,
            seed=PHASE1C_WORKLOAD_SEED,
        ),
        profile=CapacityProfile.BOUNDED_TAIL_RESTART,
        bounded_tail_max=PHASE1C_BOUNDED_TAIL_MAX,
    )
    return Phase1CWorkloadSuite(
        golden_shape_sha256=shape.sha256,
        seed=PHASE1C_WORKLOAD_SEED,
        golden_shaped_manifests=golden_manifests,
        bounded_tail_restart_manifest=_build_manifest_with_progress(
            tail_config,
            manifest_label="BOUNDED_TAIL_RESTART",
            progress_callback=progress_callback,
        ),
        adversarial_storage_manifest=_build_manifest_with_progress(
            _adversarial_config(shape),
            manifest_label="ADVERSARIAL_STORAGE",
            progress_callback=progress_callback,
        ),
    )


class Phase1CRatioComparability(StrEnum):
    NON_LIKE_FOR_LIKE_DIAGNOSTIC = "NON_LIKE_FOR_LIKE_DIAGNOSTIC"


@dataclass(frozen=True, slots=True)
class Phase1CAuthoritativeBaseline:
    label: str
    byte_count: int
    semantic_basis: str

    def __post_init__(self) -> None:
        if type(self.label) is not str or not self.label:
            raise ValueError("baseline label must be non-empty text")
        if type(self.byte_count) is not int or self.byte_count <= 0:
            raise ValueError("baseline byte_count must be a positive exact integer")
        if type(self.semantic_basis) is not str or not self.semantic_basis:
            raise ValueError("baseline semantic_basis must be non-empty text")

    def payload(self) -> dict[str, object]:
        return {
            "byte_count": self.byte_count,
            "label": self.label,
            "semantic_basis": self.semantic_basis,
        }


AUTHORITATIVE_BASELINES = (
    Phase1CAuthoritativeBaseline(
        label="ORIGINAL_V3_SOURCE_BYTES",
        byte_count=ORIGINAL_V3_SOURCE_BYTES,
        semantic_basis="ORIGINAL_V3_SOURCE_PHYSICAL_TREE",
    ),
    Phase1CAuthoritativeBaseline(
        label="GOLDEN_EXPORT_PHYSICAL_BYTES",
        byte_count=GOLDEN_EXPORT_PHYSICAL_BYTES,
        semantic_basis=(
            "GOLDEN_V3_EXPORT_PAYLOAD_SHARDS_PHYSICAL_BYTES_EXCLUDING_CONTROLS"
        ),
    ),
    Phase1CAuthoritativeBaseline(
        label="PHASE1B_STORE_PLUS_ANCHOR_BYTES",
        byte_count=PHASE1B_STORE_PLUS_ANCHOR_BYTES,
        semantic_basis="V3_COMPATIBILITY_IMPORT_STORE_PLUS_ANCHOR",
    ),
    Phase1CAuthoritativeBaseline(
        label="PHASE1B_COMPATIBILITY_SEGMENT_BYTES",
        byte_count=PHASE1B_COMPATIBILITY_SEGMENT_BYTES,
        semantic_basis="V3_COMPATIBILITY_IMPORT_SEGMENTS",
    ),
)


def _decimal_ratio(numerator: int, denominator: int) -> str:
    with localcontext() as context:
        context.prec = 50
        rendered = format(Decimal(numerator) / Decimal(denominator), ".12f")
    return rendered.rstrip("0").rstrip(".") or "0"


@dataclass(frozen=True, slots=True)
class Phase1CBaselineRatio:
    numerator_label: str
    numerator_bytes: int
    denominator: Phase1CAuthoritativeBaseline
    ratio: str
    comparability: Phase1CRatioComparability
    limitation: str

    def __post_init__(self) -> None:
        if type(self.numerator_label) is not str or not self.numerator_label:
            raise ValueError("ratio numerator_label must be non-empty text")
        if type(self.numerator_bytes) is not int or self.numerator_bytes < 0:
            raise ValueError("ratio numerator_bytes must be non-negative")
        if not isinstance(self.denominator, Phase1CAuthoritativeBaseline):
            raise TypeError("ratio denominator must be Phase1CAuthoritativeBaseline")
        expected = _decimal_ratio(self.numerator_bytes, self.denominator.byte_count)
        if self.ratio != expected:
            raise ValueError("ratio does not match its exact byte counts")
        if self.comparability is not (Phase1CRatioComparability.NON_LIKE_FOR_LIKE_DIAGNOSTIC):
            raise ValueError("cross-layout ratios must remain diagnostic")
        if type(self.limitation) is not str or not self.limitation:
            raise ValueError("ratio limitation must be non-empty text")

    def payload(self) -> dict[str, object]:
        return {
            "comparability": self.comparability.value,
            "denominator_bytes": self.denominator.byte_count,
            "denominator_label": self.denominator.label,
            "limitation": self.limitation,
            "numerator_bytes": self.numerator_bytes,
            "numerator_label": self.numerator_label,
            "ratio": self.ratio,
        }


@dataclass(frozen=True, slots=True)
class Phase1CCensusRatioSet:
    label: str
    commit_count: int
    census: ByteCategoryCensus
    ratios: tuple[Phase1CBaselineRatio, ...]

    def __post_init__(self) -> None:
        if type(self.label) is not str or not self.label:
            raise ValueError("census ratio label must be non-empty text")
        if type(self.commit_count) is not int or self.commit_count <= 0:
            raise ValueError("commit_count must be a positive exact integer")
        _require_census(self.census, label="census")
        if type(self.ratios) is not tuple or len(self.ratios) != 4:
            raise ValueError("each census must expose all four authoritative ratios")

    def payload(self) -> dict[str, object]:
        return {
            "byte_census": self.census.payload(),
            "commit_count": self.commit_count,
            "label": self.label,
            "ratios": [ratio.payload() for ratio in self.ratios],
        }


@dataclass(frozen=True, slots=True)
class Phase1CBaselineRatioReport:
    baselines: tuple[Phase1CAuthoritativeBaseline, ...]
    observations: tuple[Phase1CCensusRatioSet, ...]

    def __post_init__(self) -> None:
        if self.baselines != AUTHORITATIVE_BASELINES:
            raise ValueError("ratio report must use the authoritative baselines")
        expected = (
            "GOLDEN_NATIVE",
            *(f"GOLDEN_SHAPED_{count}" for count in PHASE1C_CAPACITY_LEVELS),
        )
        if tuple(observation.label for observation in self.observations) != expected:
            raise ValueError("ratio report observations are missing or out of order")

    def payload(self) -> dict[str, object]:
        return {
            "artifact": "STORAGE_V4_PHASE1C_BASELINE_RATIO_REPORT_V1",
            "baselines": [baseline.payload() for baseline in self.baselines],
            "comparison_policy": ("ALL_RATIOS_ARE_NON_LIKE_FOR_LIKE_DIAGNOSTICS_NO_CAPACITY_GATE"),
            "markers": list(CAPACITY_MARKERS),
            "observations": [observation.payload() for observation in self.observations],
        }


def _ratios_for_census(census: ByteCategoryCensus) -> tuple[Phase1CBaselineRatio, ...]:
    v3_source, golden_export, phase1b_store, phase1b_segments = AUTHORITATIVE_BASELINES
    persistent_segments = census.raw_segments_bytes + census.paper_segments_bytes
    total_limitation = (
        "Native raw-plus-Paper bytes and the denominator use different layouts, "
        "representations, or workload populations; ratio is diagnostic only."
    )
    segment_limitation = (
        "Native raw-plus-Paper segments are not the Phase 1B V3-compatibility "
        "import segment population; ratio is diagnostic only."
    )

    def ratio(
        numerator_label: str,
        numerator_bytes: int,
        denominator: Phase1CAuthoritativeBaseline,
        limitation: str,
    ) -> Phase1CBaselineRatio:
        return Phase1CBaselineRatio(
            numerator_label=numerator_label,
            numerator_bytes=numerator_bytes,
            denominator=denominator,
            ratio=_decimal_ratio(numerator_bytes, denominator.byte_count),
            comparability=(Phase1CRatioComparability.NON_LIKE_FOR_LIKE_DIAGNOSTIC),
            limitation=limitation,
        )

    return (
        ratio(
            "NATIVE_RAW_PLUS_PAPER_TOTAL_BYTES",
            census.total_bytes,
            v3_source,
            total_limitation,
        ),
        ratio(
            "NATIVE_RAW_PLUS_PAPER_TOTAL_BYTES",
            census.total_bytes,
            golden_export,
            total_limitation,
        ),
        ratio(
            "NATIVE_RAW_PLUS_PAPER_TOTAL_BYTES",
            census.total_bytes,
            phase1b_store,
            total_limitation,
        ),
        ratio(
            "NATIVE_RAW_PLUS_PAPER_SEGMENT_BYTES",
            persistent_segments,
            phase1b_segments,
            segment_limitation,
        ),
    )


def build_phase1c_baseline_ratio_report(
    *,
    golden_census: ByteCategoryCensus,
    level_censuses: Mapping[int, ByteCategoryCensus],
) -> Phase1CBaselineRatioReport:
    """Compare measured bytes without implying cross-layout equivalence."""

    _require_census(golden_census, label="golden_census")
    if not isinstance(level_censuses, Mapping):
        raise TypeError("level_censuses must be a mapping")
    if set(level_censuses) != set(PHASE1C_CAPACITY_LEVELS):
        raise ValueError("level_censuses must contain exactly 100k, 500k, and 1m")
    for commit_count, census in level_censuses.items():
        if type(commit_count) is not int:
            raise TypeError("level_censuses keys must be exact integers")
        _require_census(census, label=f"level_censuses[{commit_count}]")

    observations = (
        Phase1CCensusRatioSet(
            label="GOLDEN_NATIVE",
            commit_count=252_262,
            census=golden_census,
            ratios=_ratios_for_census(golden_census),
        ),
        *(
            Phase1CCensusRatioSet(
                label=f"GOLDEN_SHAPED_{commit_count}",
                commit_count=commit_count,
                census=level_censuses[commit_count],
                ratios=_ratios_for_census(level_censuses[commit_count]),
            )
            for commit_count in PHASE1C_CAPACITY_LEVELS
        ),
    )
    return Phase1CBaselineRatioReport(
        baselines=AUTHORITATIVE_BASELINES,
        observations=observations,
    )


class Phase1CTargetDecisionRole(StrEnum):
    DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"
    TERMINAL_DECISION = "TERMINAL_DECISION"


def _normalized_target(value: str | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, Decimal)):
        raise TypeError("canonical_target_gib_per_hour must be text, Decimal, or None")
    try:
        target = Decimal(value)
    except InvalidOperation as error:
        raise ValueError("canonical target must be a finite positive decimal") from error
    if not target.is_finite() or target <= 0:
        raise ValueError("canonical target must be a finite positive decimal")
    if target != Decimal(CANONICAL_TARGET_GIB_PER_HOUR):
        raise ValueError("StorageGrowthAssessment is bound to the strict canonical 0.20 GiB/h target")
    return target


def _assessment_target_status(
    assessment: StorageGrowthAssessment,
) -> str | None:
    if not isinstance(assessment, StorageGrowthAssessment):
        raise TypeError("growth assessment must be StorageGrowthAssessment")
    if assessment.status != "AVAILABLE":
        if assessment.passed is not None:
            raise ValueError("unavailable growth assessment cannot carry a target result")
        return None
    if assessment.passed is None or type(assessment.passed) is not bool:
        raise ValueError("available growth assessment must carry an exact boolean result")
    if assessment.gib_per_hour is None:
        raise ValueError("available growth assessment must carry GiB/h")
    try:
        observed = Decimal(assessment.gib_per_hour)
    except InvalidOperation as error:
        raise ValueError("growth assessment GiB/h must be a decimal") from error
    if not observed.is_finite() or observed < 0:
        raise ValueError("growth assessment GiB/h must be finite and non-negative")
    # `passed` is computed from exact integers before `gib_per_hour` is rounded
    # for rendering.  Recomputing the decision from the rendered decimal could
    # invert a result arbitrarily close to the strict boundary.
    return TARGET_MET if assessment.passed else TARGET_NOT_MET


@dataclass(frozen=True, slots=True)
class Phase1CTargetDiagnostic:
    label: str
    role: Phase1CTargetDecisionRole
    assessment: StorageGrowthAssessment
    target_status: str | None

    def __post_init__(self) -> None:
        if type(self.label) is not str or not self.label:
            raise ValueError("target diagnostic label must be non-empty text")
        if not isinstance(self.role, Phase1CTargetDecisionRole):
            raise TypeError("target diagnostic role must be Phase1CTargetDecisionRole")
        if not isinstance(self.assessment, StorageGrowthAssessment):
            raise TypeError("target diagnostic assessment must be StorageGrowthAssessment")
        if self.target_status is not None and self.target_status not in (PHASE1C_TARGET_STATUSES):
            raise ValueError("target diagnostic status is not authorized")

    def payload(self) -> dict[str, object]:
        return {
            "assessment": self.assessment.payload(),
            "label": self.label,
            "role": self.role.value,
            "target_status": self.target_status,
        }


@dataclass(frozen=True, slots=True)
class Phase1CTargetVerdict:
    canonical_target_gib_per_hour: str | None
    terminal_verdict: str
    terminal_target_status: str | None
    diagnostics: tuple[Phase1CTargetDiagnostic, ...]

    def __post_init__(self) -> None:
        if self.terminal_verdict not in PHASE1C_TERMINAL_VERDICTS:
            raise ValueError("terminal verdict is not authorized")
        if (
            self.terminal_target_status is not None
            and self.terminal_target_status not in PHASE1C_TARGET_STATUSES
        ):
            raise ValueError("terminal target status is not authorized")
        if self.canonical_target_gib_per_hour is None:
            if self.terminal_verdict != PHASE1C_NO_CANONICAL_TARGET_VERDICT:
                raise ValueError("missing target requires the no-canonical-target verdict")
            if self.terminal_target_status is not None:
                raise ValueError("missing target cannot have a target status")
        elif self.canonical_target_gib_per_hour != CANONICAL_TARGET_GIB_PER_HOUR:
            raise ValueError("target verdict must use the strict canonical target")
        expected_labels = (
            "GOLDEN_NATIVE",
            *(f"GOLDEN_SHAPED_{count}" for count in PHASE1C_CAPACITY_LEVELS),
        )
        if tuple(item.label for item in self.diagnostics) != expected_labels:
            raise ValueError("target diagnostics are missing or out of order")
        if self.diagnostics[-1].role is not (Phase1CTargetDecisionRole.TERMINAL_DECISION):
            raise ValueError("only the 1m level may be the terminal decision")
        if any(item.role is not Phase1CTargetDecisionRole.DIAGNOSTIC_ONLY for item in self.diagnostics[:-1]):
            raise ValueError("Golden, 100k, and 500k assessments are diagnostic only")

    def payload(self) -> dict[str, object]:
        return {
            "artifact": "STORAGE_V4_PHASE1C_TARGET_VERDICT_V1",
            "canonical_target": (
                None
                if self.canonical_target_gib_per_hour is None
                else {
                    "gib_per_hour": self.canonical_target_gib_per_hour,
                    "relation": TARGET_RELATION,
                }
            ),
            "diagnostics": [item.payload() for item in self.diagnostics],
            "markers": list(CAPACITY_MARKERS),
            "terminal_decision_basis": "GOLDEN_SHAPED_1000000_TOTAL_RAW_PLUS_PAPER",
            "terminal_target_status": self.terminal_target_status,
            "terminal_verdict": self.terminal_verdict,
        }


def decide_phase1c_target_verdict(
    *,
    golden_assessment: StorageGrowthAssessment,
    level_assessments: Mapping[int, StorageGrowthAssessment],
    canonical_target_gib_per_hour: str | Decimal | None = CANONICAL_TARGET_GIB_PER_HOUR,
) -> Phase1CTargetVerdict:
    """Select the terminal verdict solely from the 1m Golden-shaped level."""

    if not isinstance(golden_assessment, StorageGrowthAssessment):
        raise TypeError("golden_assessment must be StorageGrowthAssessment")
    if not isinstance(level_assessments, Mapping):
        raise TypeError("level_assessments must be a mapping")
    if set(level_assessments) != set(PHASE1C_CAPACITY_LEVELS):
        raise ValueError("level_assessments must contain exactly 100k, 500k, and 1m")
    for commit_count, assessment in level_assessments.items():
        if type(commit_count) is not int:
            raise TypeError("level_assessments keys must be exact integers")
        if not isinstance(assessment, StorageGrowthAssessment):
            raise TypeError(f"level_assessments[{commit_count}] must be StorageGrowthAssessment")

    target = _normalized_target(canonical_target_gib_per_hour)
    ordered = (
        ("GOLDEN_NATIVE", golden_assessment),
        *(
            (f"GOLDEN_SHAPED_{commit_count}", level_assessments[commit_count])
            for commit_count in PHASE1C_CAPACITY_LEVELS
        ),
    )
    if target is None:
        diagnostics = tuple(
            Phase1CTargetDiagnostic(
                label=label,
                role=(
                    Phase1CTargetDecisionRole.TERMINAL_DECISION
                    if label == "GOLDEN_SHAPED_1000000"
                    else Phase1CTargetDecisionRole.DIAGNOSTIC_ONLY
                ),
                assessment=assessment,
                target_status=None,
            )
            for label, assessment in ordered
        )
        return Phase1CTargetVerdict(
            canonical_target_gib_per_hour=None,
            terminal_verdict=PHASE1C_NO_CANONICAL_TARGET_VERDICT,
            terminal_target_status=None,
            diagnostics=diagnostics,
        )

    diagnostics = tuple(
        Phase1CTargetDiagnostic(
            label=label,
            role=(
                Phase1CTargetDecisionRole.TERMINAL_DECISION
                if label == "GOLDEN_SHAPED_1000000"
                else Phase1CTargetDecisionRole.DIAGNOSTIC_ONLY
            ),
            assessment=assessment,
            target_status=_assessment_target_status(assessment),
        )
        for label, assessment in ordered
    )
    terminal_status = diagnostics[-1].target_status
    if terminal_status is None:
        raise ValueError("1m Golden-shaped assessment cannot be unavailable")
    verdict = PHASE1C_PROVEN_VERDICT if terminal_status == TARGET_MET else PHASE1C_TARGET_NOT_MET_VERDICT
    return Phase1CTargetVerdict(
        canonical_target_gib_per_hour=CANONICAL_TARGET_GIB_PER_HOUR,
        terminal_verdict=verdict,
        terminal_target_status=terminal_status,
        diagnostics=diagnostics,
    )


__all__ = [
    "AUTHORITATIVE_BASELINES",
    "CANONICAL_TARGET_GIB_PER_HOUR",
    "GOLDEN_EXPORT_PHYSICAL_BYTES",
    "ORIGINAL_V3_SOURCE_BYTES",
    "PHASE1B_ANCHOR_BYTES",
    "PHASE1B_COMPATIBILITY_SEGMENT_BYTES",
    "PHASE1B_STORAGE_V4_STORE_BYTES",
    "PHASE1B_STORE_PLUS_ANCHOR_BYTES",
    "PHASE1C_ADVERSARIAL_COMMIT_COUNT",
    "PHASE1C_BOUNDED_TAIL_MAX",
    "PHASE1C_CAPACITY_LEVELS",
    "PHASE1C_MANIFEST_PROGRESS_EVERY_COMMITS",
    "PHASE1C_NO_CANONICAL_TARGET_VERDICT",
    "PHASE1C_PRODUCTION_BATCH_COMMITS",
    "PHASE1C_PRODUCTION_CHECKPOINT_EVERY_BATCHES",
    "PHASE1C_PROVEN_VERDICT",
    "PHASE1C_TAIL_COMMIT_COUNT",
    "PHASE1C_TAIL_RESTART_SIZES",
    "PHASE1C_TARGET_NOT_MET_VERDICT",
    "PHASE1C_TARGET_STATUSES",
    "PHASE1C_TERMINAL_LEVEL",
    "PHASE1C_TERMINAL_VERDICTS",
    "PHASE1C_WORKLOAD_SEED",
    "TARGET_MET",
    "TARGET_NOT_MET",
    "TARGET_RELATION",
    "Phase1CAuthoritativeBaseline",
    "Phase1CBaselineRatio",
    "Phase1CBaselineRatioReport",
    "Phase1CCensusRatioSet",
    "Phase1CRatioComparability",
    "Phase1CTargetDecisionRole",
    "Phase1CTargetDiagnostic",
    "Phase1CTargetVerdict",
    "Phase1CWorkloadProgress",
    "Phase1CWorkloadProgressCallback",
    "Phase1CWorkloadProgressStatus",
    "Phase1CWorkloadSuite",
    "build_phase1c_baseline_ratio_report",
    "build_phase1c_workload_suite",
    "decide_phase1c_target_verdict",
]
