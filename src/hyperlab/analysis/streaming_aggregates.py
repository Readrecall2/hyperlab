from __future__ import annotations

import math
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Protocol

import numpy as np
import pandas as pd

from hyperlab.analysis.lead_lag import (
    SIGNAL_FAMILIES,
    SOURCE_TIME_STATUS,
    ExecutionAssumptions,
    LeadLagConfig,
    StrictInterval,
)


class DiskBackedEventSource(Protocol):
    """Bounded access required by the exact Phase 10-2 aggregate pass.

    Implementations must stream rows in deterministic logical order, apply the
    supplied filter before yielding, project only ``columns``, calculate
    quantiles exactly with pandas' linear interpolation over non-null float64
    values, and return the exact set of blocks used by evaluable primary
    information rows. Approximate sketches do not satisfy this protocol.
    """

    def iter_rows(
        self,
        *,
        filters: Mapping[str, object],
        columns: Sequence[str],
    ) -> Iterator[tuple[object, ...]]: ...

    def exact_quantile(
        self,
        *,
        metric: str,
        filters: Mapping[str, object],
        quantile: float,
    ) -> float: ...

    def distinct_randomization_blocks(self) -> Iterable[str]: ...


@dataclass(frozen=True, slots=True)
class StreamingAggregateResult:
    metrics: pd.DataFrame
    bucket_metrics: pd.DataFrame
    controls: pd.DataFrame


@dataclass(slots=True)
class RandomizationDiagnostics:
    """Deterministic work/state counters for the bounded null-control pass."""

    evaluable_response_rows_scanned: int = 0
    block_accumulator_cells: int = 0
    eligible_hypotheses: int = 0
    globally_used_blocks: int = 0
    sign_matrix_values: int = 0
    randomized_output_values: int = 0
    block_matrix_applications: int = 0
    per_event_resample_vector_updates: int = 0
    scalar_reduction_rows_scanned: int = 0


@dataclass(slots=True)
class _CompensatedSum:
    total: float = 0.0
    correction: float = 0.0

    def add(self, value: float) -> None:
        combined = self.total + value
        if abs(self.total) >= abs(value):
            self.correction += (self.total - combined) + value
        else:
            self.correction += (value - combined) + self.total
        self.total = combined

    @property
    def value(self) -> float:
        return self.total + self.correction


@dataclass(slots=True)
class _Moments:
    count: int = 0
    total: _CompensatedSum = field(default_factory=_CompensatedSum)
    squares: _CompensatedSum = field(default_factory=_CompensatedSum)
    running_mean: float = 0.0
    m2: float = 0.0

    def add(self, value: float) -> None:
        if not math.isfinite(value):
            raise ValueError("streaming aggregate values must be finite")
        self.count += 1
        self.total.add(value)
        self.squares.add(value * value)
        delta = value - self.running_mean
        self.running_mean += delta / self.count
        self.m2 += delta * (value - self.running_mean)

    @property
    def mean(self) -> float:
        return self.total.value / self.count if self.count else math.nan

    @property
    def sample_std(self) -> float:
        if self.count <= 1:
            return math.nan
        variance = self.m2 / (self.count - 1)
        return math.sqrt(max(variance, 0.0))


@dataclass(slots=True)
class _RandomizationBlockMoments:
    """Oracle-order scalar moments for one hypothesis/block cell."""

    count: int = 0
    total: float = 0.0
    squares: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.squares += value * value


@dataclass(slots=True)
class _InformationAccumulator:
    signal_count: int = 0
    evaluable_rows: int = 0
    same_direction_count: int = 0
    neutral_count: int = 0
    adverse_count: int = 0
    first_same_count: int = 0
    first_opposite_count: int = 0
    response: _Moments = field(default_factory=_Moments)
    negative_lag: _Moments = field(default_factory=_Moments)
    first_delay: _Moments = field(default_factory=_Moments)

    def add(
        self, row: Mapping[str, object], used_blocks: set[str] | None
    ) -> None:
        self.signal_count += 1
        if not _is_true(row.get("evaluable")):
            return
        self.evaluable_rows += 1
        classification = str(row.get("classification") or "")
        if classification == "same_direction":
            self.same_direction_count += 1
        elif classification == "neutral":
            self.neutral_count += 1
        elif classification == "adverse":
            self.adverse_count += 1

        block = row.get("randomization_block")
        if not isinstance(block, str) or not block:
            raise ValueError("evaluable information rows require randomization_block")
        if used_blocks is not None:
            used_blocks.add(block)

        response = _optional_float(row.get("response_bps"))
        if response is not None:
            self.response.add(response)
        negative = _optional_float(row.get("negative_lag_response_bps"))
        if negative is not None:
            self.negative_lag.add(negative)

        first_direction = str(row.get("first_move_direction") or "")
        if first_direction == "same":
            self.first_same_count += 1
        elif first_direction == "opposite":
            self.first_opposite_count += 1
        else:
            return
        first_delay = _optional_float(row.get("first_move_delay_ms"))
        if first_delay is not None:
            self.first_delay.add(first_delay)


@dataclass(slots=True)
class _ReverseAccumulator:
    response: _Moments = field(default_factory=_Moments)

    def add(self, row: Mapping[str, object]) -> None:
        if not _is_true(row.get("evaluable")):
            return
        response = _optional_float(row.get("response_bps"))
        if response is not None:
            self.response.add(response)


_EXECUTION_MEAN_COLUMNS = {
    "net": "net_execution_bps",
    "gross": "gross_execution_bps",
    "break_even": "break_even_move_bps",
    "adjusted_net": "fill_adjusted_net_bps",
    "adjusted_gross": "fill_adjusted_gross_bps",
    "before_funding": "before_funding_execution_bps",
    "entry_fees": "entry_fee_bps_applied",
    "exit_fees": "exit_fee_bps_applied",
    "entry_spread": "entry_spread_cost_bps",
    "exit_spread": "exit_spread_cost_bps",
    "entry_slippage": "entry_slippage_cost_bps",
    "exit_slippage": "exit_slippage_cost_bps",
    "adverse_exit": "adverse_exit_cost_bps",
}


@dataclass(slots=True)
class _ExecutionAccumulator:
    attempt_count: int = 0
    completed_count: int = 0
    filled_count: int = 0
    partial_count: int = 0
    unresolved_count: int = 0
    economically_unresolved_count: int = 0
    residual_exposure_count: int = 0
    positive_net_count: int = 0
    zero_net_count: int = 0
    negative_net_count: int = 0
    matched_fill: _Moments = field(default_factory=_Moments)
    values: dict[str, _Moments] = field(
        default_factory=lambda: {
            name: _Moments() for name in _EXECUTION_MEAN_COLUMNS
        }
    )

    def add(self, row: Mapping[str, object]) -> None:
        self.attempt_count += 1
        status = str(row.get("execution_status") or "")
        completed = status in {"FILLED", "PARTIAL"}
        unresolved = status.startswith("UNRESOLVED_")
        if status == "FILLED":
            self.filled_count += 1
        elif status == "PARTIAL":
            self.partial_count += 1
        if completed:
            self.completed_count += 1
        if unresolved:
            self.unresolved_count += 1

        residual = _float_or_zero(row.get("unclosed_exposure_fraction"))
        matched = _float_or_zero(row.get("matched_fill_fraction"))
        self.matched_fill.add(matched)
        has_residual = residual > 1e-15
        if has_residual:
            self.residual_exposure_count += 1
        if unresolved or has_residual:
            self.economically_unresolved_count += 1

        if not completed:
            return
        for name, column in _EXECUTION_MEAN_COLUMNS.items():
            value = _optional_float(row.get(column))
            if value is not None:
                self.values[name].add(value)
        net = _optional_float(row.get("net_execution_bps"))
        if net is None:
            return
        if net > 0.0:
            self.positive_net_count += 1
        elif net < 0.0:
            self.negative_net_count += 1
        else:
            self.zero_net_count += 1


@dataclass(frozen=True, slots=True)
class _Inference:
    null_means: np.ndarray
    empirical_p_value: float
    fwer_p_value: float
    fdr_q_value: float


_NUMPY_PAIRWISE_BLOCK_ROWS = 128


def _numpy_pairwise_plan(count: int) -> Iterator[int | None]:
    """Yield NumPy's float-reduction leaves and post-order combines."""

    if count <= _NUMPY_PAIRWISE_BLOCK_ROWS:
        yield count
        return
    left_count = (count // 2) // 8 * 8
    yield from _numpy_pairwise_plan(left_count)
    yield from _numpy_pairwise_plan(count - left_count)
    yield None


@dataclass(slots=True)
class _PairwiseVectorSum:
    """Bounded emulation of NumPy's contiguous pairwise float64 reduction."""

    count: int
    width: int
    _plan: Iterator[int | None] = field(init=False, repr=False)
    _leaf_size: int = field(init=False, default=0)
    _seen: int = field(init=False, default=0)
    _leaf: list[np.ndarray] = field(init=False, default_factory=list)
    _stack: list[np.ndarray] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        if self.count <= 0 or self.width <= 0:
            raise ValueError("pairwise reduction dimensions must be positive")
        self._plan = _numpy_pairwise_plan(self.count)
        self._advance_plan()

    def _advance_plan(self) -> None:
        for token in self._plan:
            if token is not None:
                self._leaf_size = token
                return
            if len(self._stack) < 2:
                raise AssertionError("invalid pairwise reduction plan")
            right = self._stack.pop()
            left = self._stack.pop()
            self._stack.append(left + right)
        self._leaf_size = 0

    def add(self, values: np.ndarray) -> None:
        if values.shape != (self.width,):
            raise ValueError("pairwise reduction row has the wrong width")
        if self._seen >= self.count or self._leaf_size == 0:
            raise ValueError("pairwise reduction received too many rows")
        self._leaf.append(values)
        self._seen += 1
        if len(self._leaf) != self._leaf_size:
            return
        matrix = np.stack(self._leaf, axis=1)
        self._stack.append(np.sum(matrix, axis=1))
        self._leaf.clear()
        self._advance_plan()

    def total(self) -> np.ndarray:
        if self._seen != self.count or self._leaf or self._leaf_size != 0:
            raise ValueError("pairwise reduction did not receive its declared rows")
        if len(self._stack) != 1:
            raise AssertionError("pairwise reduction did not converge")
        return self._stack[0]


_HypothesisKey = tuple[str, str, int]
_BlockHypothesisKey = tuple[str, str, int, str]
_BucketHypothesisKey = tuple[pd.Timestamp, str, str, int]
_ExecutionKey = tuple[str, str, str, str, int]
_BucketExecutionKey = tuple[pd.Timestamp, str, str, str, str, int]


def _is_true(value: object) -> bool:
    if value is True or (isinstance(value, np.bool_) and bool(value)):
        return True
    return (
        isinstance(value, (int, np.integer))
        and not isinstance(value, bool)
        and int(value) == 1
    )


def _optional_float(value: object) -> float | None:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, bool):
        return None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed):
        return None
    if not math.isfinite(parsed):
        raise ValueError("streaming aggregate values must be finite")
    return parsed


def _float_or_zero(value: object) -> float:
    parsed = _optional_float(value)
    return 0.0 if parsed is None else parsed


def _integer(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be an integer")
    try:
        return int(str(value))
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be an integer") from None


def _validated_intervals(intervals: Sequence[StrictInterval]) -> tuple[StrictInterval, ...]:
    normalized = tuple(sorted(intervals, key=lambda item: (item.start, item.end, item.tag)))
    if not normalized:
        raise ValueError("streaming aggregates require strict gate intervals")
    for previous, current in pairwise(normalized):
        if current.start < previous.end:
            raise ValueError("strict gate intervals cannot overlap")
    return normalized


def _bucket_starts(
    intervals: Sequence[StrictInterval], bucket_minutes: int
) -> tuple[pd.Timestamp, ...]:
    frequency = pd.Timedelta(minutes=bucket_minutes)
    starts: set[pd.Timestamp] = set()
    for interval in intervals:
        cursor = pd.Timestamp(interval.start).floor(f"{bucket_minutes}min")
        stop = pd.Timestamp(interval.end)
        while cursor < stop:
            if cursor + frequency > pd.Timestamp(interval.start):
                starts.add(cursor)
            cursor += frequency
    return tuple(sorted(starts))


_INFORMATION_COLUMNS = (
    "asset",
    "signal_family",
    "horizon_ms",
    "time_bucket_ns",
    "evaluable",
    "classification",
    "response_bps",
    "negative_lag_response_bps",
    "first_move_direction",
    "first_move_delay_ms",
    "randomization_block",
)
_REVERSE_COLUMNS = (
    "asset",
    "signal_family",
    "horizon_ms",
    "evaluable",
    "response_bps",
)
_RANDOMIZATION_COLUMNS = (
    "asset",
    "signal_family",
    "horizon_ms",
    "evaluable",
    "response_bps",
    "randomization_block",
)
_EXECUTION_COLUMNS = (
    "execution_scenario",
    "execution_model",
    "asset",
    "signal_family",
    "horizon_ms",
    "time_bucket_ns",
    "execution_status",
    "net_execution_bps",
    "gross_execution_bps",
    "fill_adjusted_net_bps",
    "fill_adjusted_gross_bps",
    "break_even_move_bps",
    "before_funding_execution_bps",
    "entry_fee_bps_applied",
    "exit_fee_bps_applied",
    "entry_spread_cost_bps",
    "exit_spread_cost_bps",
    "entry_slippage_cost_bps",
    "exit_slippage_cost_bps",
    "adverse_exit_cost_bps",
    "unclosed_exposure_fraction",
    "matched_fill_fraction",
)


@dataclass(slots=True)
class _ScannedAggregates:
    information: dict[_HypothesisKey, _InformationAccumulator] = field(default_factory=dict)
    bucket_information: dict[_BucketHypothesisKey, _InformationAccumulator] = field(
        default_factory=dict
    )
    reverse: dict[_HypothesisKey, _ReverseAccumulator] = field(default_factory=dict)
    execution: dict[_ExecutionKey, _ExecutionAccumulator] = field(default_factory=dict)
    bucket_execution: dict[_BucketExecutionKey, _ExecutionAccumulator] = field(
        default_factory=dict
    )
    randomization_blocks: dict[
        _BlockHypothesisKey, _RandomizationBlockMoments
    ] = field(default_factory=dict)
    used_blocks: set[str] = field(default_factory=set)


def _configured_hypothesis(
    row: Mapping[str, object], config: LeadLagConfig
) -> _HypothesisKey | None:
    asset = str(row.get("asset") or "")
    family = str(row.get("signal_family") or "")
    horizon = _integer(row.get("horizon_ms"), label="horizon_ms")
    if (
        asset not in config.assets
        or family not in SIGNAL_FAMILIES
        or horizon not in config.horizons_ms
    ):
        return None
    return asset, family, horizon


def _configured_execution_key(
    row: Mapping[str, object], config: LeadLagConfig
) -> _ExecutionKey | None:
    scenario = str(row.get("execution_scenario") or "")
    model = str(row.get("execution_model") or "")
    hypothesis = _configured_hypothesis(row, config)
    if (
        hypothesis is None
        or scenario not in {item.name for item in config.execution_scenarios}
        or model not in {"taker", "maker"}
    ):
        return None
    return scenario, model, *hypothesis


def _event_bucket(
    row: Mapping[str, object], bucket_set: frozenset[pd.Timestamp]
) -> pd.Timestamp | None:
    value = row.get("time_bucket_ns")
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    bucket = pd.Timestamp(
        _integer(value, label="time_bucket_ns"), unit="ns", tz="UTC"
    )
    return bucket if bucket in bucket_set else None


def _iter_projected_rows(
    source: DiskBackedEventSource,
    *,
    filters: Mapping[str, object],
    columns: Sequence[str],
) -> Iterator[dict[str, object]]:
    """Name a projected tuple without retaining a fetched batch or event group."""

    projected = tuple(columns)
    for values in source.iter_rows(filters=filters, columns=projected):
        if len(values) != len(projected):
            raise ValueError("event source returned a row with the wrong projection width")
        yield dict(zip(projected, values, strict=True))


def _scan_events(
    source: DiskBackedEventSource,
    intervals: Sequence[StrictInterval],
    config: LeadLagConfig,
    diagnostics: RandomizationDiagnostics | None = None,
) -> _ScannedAggregates:
    result = _ScannedAggregates()
    bucket_set = frozenset(_bucket_starts(intervals, config.bucket_minutes))

    information_filter = {"row_kind": "information", "signal_role": "primary"}
    for row in _iter_projected_rows(
        source,
        filters=information_filter,
        columns=_INFORMATION_COLUMNS,
    ):
        hypothesis = _configured_hypothesis(row, config)
        if hypothesis is None:
            if _is_true(row.get("evaluable")):
                block = row.get("randomization_block")
                if not isinstance(block, str) or not block:
                    raise ValueError(
                        "evaluable information rows require randomization_block"
                    )
                result.used_blocks.add(block)
            continue
        result.information.setdefault(hypothesis, _InformationAccumulator()).add(
            row, result.used_blocks
        )
        if _is_true(row.get("evaluable")):
            response = _optional_float(row.get("response_bps"))
            if response is not None:
                block = row.get("randomization_block")
                if not isinstance(block, str) or not block:
                    raise ValueError(
                        "evaluable information rows require randomization_block"
                    )
                block_key: _BlockHypothesisKey = (*hypothesis, block)
                result.randomization_blocks.setdefault(
                    block_key, _RandomizationBlockMoments()
                ).add(response)
                if diagnostics is not None:
                    diagnostics.evaluable_response_rows_scanned += 1
        bucket = _event_bucket(row, bucket_set)
        if bucket is not None:
            information_bucket_key: _BucketHypothesisKey = (bucket, *hypothesis)
            result.bucket_information.setdefault(
                information_bucket_key, _InformationAccumulator()
            ).add(row, None)

    reverse_filter = {"row_kind": "control", "signal_role": "reverse"}
    for row in _iter_projected_rows(
        source, filters=reverse_filter, columns=_REVERSE_COLUMNS
    ):
        hypothesis = _configured_hypothesis(row, config)
        if hypothesis is not None:
            result.reverse.setdefault(hypothesis, _ReverseAccumulator()).add(row)

    execution_filter = {"row_kind": "execution"}
    for row in _iter_projected_rows(
        source, filters=execution_filter, columns=_EXECUTION_COLUMNS
    ):
        execution_key = _configured_execution_key(row, config)
        if execution_key is None:
            continue
        result.execution.setdefault(execution_key, _ExecutionAccumulator()).add(row)
        bucket = _event_bucket(row, bucket_set)
        if bucket is not None:
            execution_bucket_key: _BucketExecutionKey = (bucket, *execution_key)
            result.bucket_execution.setdefault(
                execution_bucket_key, _ExecutionAccumulator()
            ).add(row)
    if diagnostics is not None:
        diagnostics.block_accumulator_cells = len(result.randomization_blocks)
    return result


def _hypothesis_order(config: LeadLagConfig) -> Iterator[_HypothesisKey]:
    for asset in config.assets:
        for family in SIGNAL_FAMILIES:
            for horizon_ms in config.horizons_ms:
                yield asset, family, horizon_ms


def _benjamini_hochberg(values: Sequence[float]) -> list[float]:
    if not values:
        return []
    raw = np.asarray(values, dtype=float)
    order = np.argsort(raw, kind="mergesort")
    adjusted = np.empty(len(raw), dtype=float)
    running = 1.0
    for reverse_rank in range(len(raw) - 1, -1, -1):
        position = int(order[reverse_rank])
        rank = reverse_rank + 1
        candidate = min(1.0, float(raw[position]) * len(raw) / rank)
        running = min(running, candidate)
        adjusted[position] = running
    return [float(value) for value in adjusted]


def _randomized_inference(
    source: DiskBackedEventSource,
    scanned: _ScannedAggregates,
    config: LeadLagConfig,
    diagnostics: RandomizationDiagnostics | None = None,
) -> dict[_HypothesisKey, _Inference]:
    source_blocks: set[str] = set()
    for block in source.distinct_randomization_blocks():
        if not isinstance(block, str) or not block:
            raise ValueError("randomization blocks must be non-empty strings")
        source_blocks.add(block)
    if source_blocks != scanned.used_blocks:
        raise ValueError(
            "event source randomization blocks do not match evaluable information rows"
        )
    blocks = tuple(sorted(source_blocks))
    if diagnostics is not None:
        diagnostics.globally_used_blocks = len(blocks)
    if not blocks:
        return {}

    eligible_keys: list[_HypothesisKey] = []
    for key in _hypothesis_order(config):
        accumulator = scanned.information.get(key, _InformationAccumulator())
        if accumulator.evaluable_rows < config.minimum_events:
            continue
        if accumulator.response.count != accumulator.evaluable_rows:
            raise ValueError(
                "evaluable information rows must have finite response_bps for randomization"
            )
        eligible_keys.append(key)
    if diagnostics is not None:
        diagnostics.eligible_hypotheses = len(eligible_keys)
        diagnostics.randomized_output_values = (
            len(eligible_keys) * config.randomization_resamples
        )
    if not eligible_keys:
        return {}

    reducers = {
        key: _PairwiseVectorSum(
            count=scanned.information[key].evaluable_rows,
            width=2,
        )
        for key in eligible_keys
    }
    sequential_totals = dict.fromkeys(eligible_keys, 0.0)
    eligible_set = frozenset(eligible_keys)
    for row in _iter_projected_rows(
        source,
        filters={"row_kind": "information", "signal_role": "primary"},
        columns=_RANDOMIZATION_COLUMNS,
    ):
        if not _is_true(row.get("evaluable")):
            continue
        row_key = _configured_hypothesis(row, config)
        if row_key is None or row_key not in eligible_set:
            continue
        response = _optional_float(row.get("response_bps"))
        if response is None:
            raise ValueError(
                "evaluable information rows must have finite response_bps for randomization"
            )
        response64 = np.float64(response)
        reducers[row_key].add(
            np.asarray([response64, np.square(response64)], dtype=np.float64)
        )
        sequential_totals[row_key] += response
        if diagnostics is not None:
            diagnostics.scalar_reduction_rows_scanned += 1

    block_totals = np.zeros(
        (len(blocks), len(eligible_keys)), dtype=np.float64
    )
    for hypothesis_position, key in enumerate(eligible_keys):
        accumulator = scanned.information[key]
        block_count = 0
        for block_position, block in enumerate(blocks):
            moments = scanned.randomization_blocks.get((*key, block))
            if moments is None:
                continue
            block_count += moments.count
            block_totals[block_position, hypothesis_position] = moments.total
        if block_count != accumulator.evaluable_rows:
            raise ValueError(
                "per-block response counts do not match evaluable information rows"
            )

    rng = np.random.default_rng(config.randomization_seed)
    signs = rng.choice(
        np.array([-1.0, 1.0]),
        size=(config.randomization_resamples, len(blocks)),
        replace=True,
    )
    randomized_totals = np.zeros(
        (config.randomization_resamples, len(eligible_keys)),
        dtype=np.float64,
    )
    for block_position in range(len(blocks)):
        randomized_totals += (
            signs[:, block_position, np.newaxis]
            * block_totals[block_position, np.newaxis, :]
        )
    positive_rows = np.all(signs == 1.0, axis=1)
    negative_rows = np.all(signs == -1.0, axis=1)
    sequential = np.asarray(
        [sequential_totals[key] for key in eligible_keys], dtype=np.float64
    )
    randomized_totals[positive_rows, :] = sequential
    randomized_totals[negative_rows, :] = -sequential
    if diagnostics is not None:
        diagnostics.sign_matrix_values = signs.size
        diagnostics.block_matrix_applications = 1

    eligible: list[tuple[_HypothesisKey, np.ndarray, float, float]] = []
    maximum_null = np.zeros(config.randomization_resamples, dtype=float)
    for hypothesis_position, key in enumerate(eligible_keys):
        accumulator = scanned.information[key]
        totals = reducers[key].total()
        randomized_means = (
            randomized_totals[:, hypothesis_position]
            / accumulator.evaluable_rows
        )
        denominator = max(
            math.sqrt(float(totals[1])) / accumulator.evaluable_rows,
            1e-12,
        )
        observed_mean = float(totals[0]) / accumulator.evaluable_rows
        observed_statistic = observed_mean / denominator
        null_statistics = randomized_means / denominator
        maximum_null = np.maximum(maximum_null, np.abs(null_statistics))
        eligible.append((key, randomized_means, observed_mean, observed_statistic))

    raw_p_values: list[float] = []
    fwer_values: list[float] = []
    for _key, null_means, observed_mean, observed_statistic in eligible:
        raw_p_values.append(
            (
                1.0
                + float(np.count_nonzero(np.abs(null_means) >= abs(observed_mean)))
            )
            / (config.randomization_resamples + 1.0)
        )
        fwer_values.append(
            (
                1.0
                + float(np.count_nonzero(maximum_null >= abs(observed_statistic)))
            )
            / (config.randomization_resamples + 1.0)
        )
    q_values = _benjamini_hochberg(raw_p_values)
    return {
        key: _Inference(
            null_means=null_means,
            empirical_p_value=raw_p_values[position],
            fwer_p_value=fwer_values[position],
            fdr_q_value=q_values[position],
        )
        for position, (key, null_means, _observed_mean, _observed_statistic) in enumerate(
            eligible
        )
    }


def _information_quantile_filter(
    key: _HypothesisKey,
    *,
    bucket: pd.Timestamp | None = None,
) -> dict[str, object]:
    asset, family, horizon_ms = key
    filters: dict[str, object] = {
        "asset": asset,
        "signal_family": family,
        "horizon_ms": horizon_ms,
    }
    if bucket is not None:
        filters["time_bucket_ns"] = int(bucket.value)
    return filters


def _execution_quantile_filter(
    key: _ExecutionKey,
    *,
    bucket: pd.Timestamp | None = None,
) -> dict[str, object]:
    scenario, model, asset, family, horizon_ms = key
    filters: dict[str, object] = {
        "execution_scenario": scenario,
        "execution_model": model,
        "asset": asset,
        "signal_family": family,
        "horizon_ms": horizon_ms,
    }
    if bucket is not None:
        filters["time_bucket_ns"] = int(bucket.value)
    return filters


def _exact_quantile(
    source: DiskBackedEventSource,
    *,
    metric: str,
    filters: Mapping[str, object],
    quantile: float,
    count: int,
) -> float:
    if count == 0:
        return math.nan
    value = float(
        source.exact_quantile(
            metric=metric,
            filters=filters,
            quantile=quantile,
        )
    )
    if not math.isfinite(value):
        raise ValueError(
            f"exact quantile source returned a non-finite {metric} value"
        )
    return value


def _information_metric_row(
    source: DiskBackedEventSource,
    accumulator: _InformationAccumulator,
    *,
    key: _HypothesisKey,
    minimum_events: int,
    inference: _Inference | None,
    bucket: pd.Timestamp | None = None,
) -> dict[str, object]:
    asset, family, horizon_ms = key
    count = accumulator.response.count
    first_count = accumulator.first_same_count + accumulator.first_opposite_count
    query = _information_quantile_filter(key, bucket=bucket)
    minimum_met = count >= minimum_events
    if bucket is not None:
        inference_status = (
            "DESCRIPTIVE_BUCKET_NO_INFERENCE"
            if minimum_met
            else "NOT_ADMISSIBLE_MINIMUM_EVENTS"
        )
    elif inference is not None:
        inference_status = "ADMISSIBLE_RANDOMIZATION"
    else:
        inference_status = (
            "PENDING_RANDOMIZATION"
            if minimum_met
            else "NOT_ADMISSIBLE_MINIMUM_EVENTS"
        )
    return {
        "analysis_kind": "information",
        "asset": asset,
        "signal_family": family,
        "horizon_ms": horizon_ms,
        "execution_scenario": None,
        "execution_model": None,
        "signal_count": accumulator.signal_count,
        "evaluable_count": count,
        "excluded_count": accumulator.signal_count - count,
        "same_direction_count": accumulator.same_direction_count,
        "neutral_count": accumulator.neutral_count,
        "adverse_count": accumulator.adverse_count,
        "same_direction_rate": accumulator.same_direction_count / count
        if count
        else math.nan,
        "false_positive_rate": (
            accumulator.neutral_count + accumulator.adverse_count
        )
        / count
        if count
        else math.nan,
        "adverse_rate": accumulator.adverse_count / count if count else math.nan,
        "expected_move_bps": accumulator.response.mean,
        "median_move_bps": _exact_quantile(
            source,
            filters=query,
            metric="information_response",
            quantile=0.5,
            count=count,
        ),
        "q10_move_bps": _exact_quantile(
            source,
            filters=query,
            metric="information_response",
            quantile=0.1,
            count=count,
        ),
        "q90_move_bps": _exact_quantile(
            source,
            filters=query,
            metric="information_response",
            quantile=0.9,
            count=count,
        ),
        "negative_lag_expected_move_bps": accumulator.negative_lag.mean,
        "first_move_observed_count": first_count,
        "first_move_same_direction_count": accumulator.first_same_count,
        "first_move_opposite_count": accumulator.first_opposite_count,
        "first_move_same_direction_rate": accumulator.first_same_count / first_count
        if first_count
        else math.nan,
        "first_move_opposite_rate": accumulator.first_opposite_count / first_count
        if first_count
        else math.nan,
        "median_first_move_delay_ms": _exact_quantile(
            source,
            filters=query,
            metric="information_first_move_delay",
            quantile=0.5,
            count=accumulator.first_delay.count,
        ),
        "minimum_events_met": minimum_met,
        "inference_status": inference_status,
        "empirical_p_value": math.nan
        if inference is None
        else inference.empirical_p_value,
        "fwer_p_value": math.nan if inference is None else inference.fwer_p_value,
        "fdr_q_value": math.nan if inference is None else inference.fdr_q_value,
        "source_time_status": SOURCE_TIME_STATUS,
    }


def _execution_metric_row(
    source: DiskBackedEventSource,
    accumulator: _ExecutionAccumulator,
    *,
    key: _ExecutionKey,
    scenario: ExecutionAssumptions,
    minimum_events: int,
    bucket: pd.Timestamp | None = None,
) -> dict[str, object]:
    scenario_name, model, asset, family, horizon_ms = key
    attempts = accumulator.attempt_count
    completed = accumulator.completed_count
    net = accumulator.values["net"]
    query = _execution_quantile_filter(key, bucket=bucket)
    attempt_weighted_net = (
        math.nan
        if accumulator.unresolved_count > 0
        or accumulator.residual_exposure_count > 0
        else (
            accumulator.values["adjusted_net"].total.value / attempts
            if attempts
            else math.nan
        )
    )
    return {
        "analysis_kind": "execution",
        "asset": asset,
        "signal_family": family,
        "horizon_ms": horizon_ms,
        "execution_scenario": scenario_name,
        "execution_model": model,
        "signal_count": attempts,
        "evaluable_count": completed,
        "excluded_count": attempts - completed,
        "same_direction_count": accumulator.positive_net_count,
        "neutral_count": accumulator.zero_net_count,
        "adverse_count": accumulator.negative_net_count,
        "same_direction_rate": accumulator.positive_net_count / net.count
        if net.count
        else math.nan,
        "false_positive_rate": (
            accumulator.zero_net_count + accumulator.negative_net_count
        )
        / net.count
        if net.count
        else math.nan,
        "adverse_rate": accumulator.negative_net_count / net.count
        if net.count
        else math.nan,
        "expected_move_bps": net.mean,
        "median_move_bps": _exact_quantile(
            source,
            filters=query,
            metric="execution_net",
            quantile=0.5,
            count=net.count,
        ),
        "q10_move_bps": _exact_quantile(
            source,
            filters=query,
            metric="execution_net",
            quantile=0.1,
            count=net.count,
        ),
        "q90_move_bps": _exact_quantile(
            source,
            filters=query,
            metric="execution_net",
            quantile=0.9,
            count=net.count,
        ),
        "negative_lag_expected_move_bps": math.nan,
        "minimum_events_met": completed >= minimum_events,
        "inference_status": "NOT_APPLICABLE_EXECUTION_SCENARIO",
        "empirical_p_value": math.nan,
        "fwer_p_value": math.nan,
        "fdr_q_value": math.nan,
        "source_time_status": SOURCE_TIME_STATUS,
        "attempt_count": attempts,
        "filled_count": accumulator.filled_count,
        "partial_count": accumulator.partial_count,
        "missed_or_unresolved_count": attempts - completed,
        "unresolved_count": accumulator.unresolved_count,
        "economically_unresolved_count": accumulator.economically_unresolved_count,
        "residual_exposure_count": accumulator.residual_exposure_count,
        "residual_exposure_rate": accumulator.residual_exposure_count / attempts
        if attempts
        else math.nan,
        "any_fill_rate": completed / attempts if attempts else math.nan,
        "fill_rate": accumulator.matched_fill.mean,
        "mean_matched_fill_fraction": accumulator.matched_fill.mean,
        "gross_expected_move_bps": accumulator.values["gross"].mean,
        "fill_adjusted_expected_gross_bps": accumulator.values[
            "adjusted_gross"
        ].mean,
        "fill_adjusted_expected_net_bps": accumulator.values["adjusted_net"].mean,
        "attempt_weighted_expected_net_bps": attempt_weighted_net,
        "minimum_profitable_move_bps": accumulator.values["break_even"].mean,
        "minimum_profitable_move_scope": "BEFORE_FUNDING",
        "expected_before_funding_execution_bps": accumulator.values[
            "before_funding"
        ].mean,
        "expected_entry_fee_bps": accumulator.values["entry_fees"].mean,
        "expected_exit_fee_bps": accumulator.values["exit_fees"].mean,
        "expected_entry_spread_cost_bps": accumulator.values["entry_spread"].mean,
        "expected_exit_spread_cost_bps": accumulator.values["exit_spread"].mean,
        "expected_entry_slippage_cost_bps": accumulator.values[
            "entry_slippage"
        ].mean,
        "expected_exit_slippage_cost_bps": accumulator.values[
            "exit_slippage"
        ].mean,
        "expected_adverse_exit_cost_bps": accumulator.values["adverse_exit"].mean,
        "calibration_status": scenario.calibration_status,
        "economic_claim": "NOT_CLAIMED",
        "economic_scope": "BEFORE_FUNDING",
        "funding_status": "NOT_EVALUATED",
        "economic_admissibility": "NOT_ADMISSIBLE_FUNDING_NOT_EVALUATED",
    }


def _control_rows(
    scanned: _ScannedAggregates,
    inference: Mapping[_HypothesisKey, _Inference],
    config: LeadLagConfig,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for key in _hypothesis_order(config):
        asset, family, horizon_ms = key
        information = scanned.information.get(key, _InformationAccumulator())
        reverse = scanned.reverse.get(key, _ReverseAccumulator())
        result = inference.get(key)
        rows.append(
            {
                "control_type": "block_sign_randomization",
                "asset": asset,
                "signal_family": family,
                "horizon_ms": horizon_ms,
                "sample_count": information.evaluable_rows,
                "observed_expected_move_bps": information.response.mean,
                "control_expected_move_bps": float(result.null_means.mean())
                if result is not None
                else math.nan,
                "control_std_bps": float(result.null_means.std(ddof=1))
                if result is not None
                else math.nan,
                "empirical_p_value": result.empirical_p_value
                if result is not None
                else math.nan,
                "fwer_p_value": result.fwer_p_value
                if result is not None
                else math.nan,
                "fdr_q_value": result.fdr_q_value
                if result is not None
                else math.nan,
                "resamples": config.randomization_resamples
                if result is not None
                else 0,
                "block_ms": config.randomization_block_ms,
                "seed": config.randomization_seed,
                "inference_status": "ADMISSIBLE_RANDOMIZATION"
                if result is not None
                else "NOT_ADMISSIBLE_MINIMUM_EVENTS",
            }
        )
        rows.append(
            {
                "control_type": "negative_lag",
                "asset": asset,
                "signal_family": family,
                "horizon_ms": horizon_ms,
                "sample_count": information.negative_lag.count,
                "observed_expected_move_bps": information.response.mean,
                "control_expected_move_bps": information.negative_lag.mean,
                "control_std_bps": information.negative_lag.sample_std,
                "empirical_p_value": math.nan,
                "fwer_p_value": math.nan,
                "fdr_q_value": math.nan,
                "resamples": 0,
                "block_ms": None,
                "seed": None,
                "inference_status": "DESCRIPTIVE_CONTROL_ONLY",
            }
        )
        rows.append(
            {
                "control_type": "reverse_hyperliquid_to_binance",
                "asset": asset,
                "signal_family": family,
                "horizon_ms": horizon_ms,
                "sample_count": reverse.response.count,
                "observed_expected_move_bps": information.response.mean,
                "control_expected_move_bps": reverse.response.mean,
                "control_std_bps": reverse.response.sample_std,
                "empirical_p_value": math.nan,
                "fwer_p_value": math.nan,
                "fdr_q_value": math.nan,
                "resamples": 0,
                "block_ms": None,
                "seed": None,
                "inference_status": "DESCRIPTIVE_CONTROL_ONLY",
            }
        )
    return rows


def _sort_frame(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    if frame.empty:
        return frame.reset_index(drop=True)
    available = [column for column in columns if column in frame.columns]
    return frame.sort_values(available, kind="mergesort").reset_index(drop=True)


def _add_decay_fields(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for name in (
        "earliest_admissible_horizon_ms",
        "expected_move_at_earliest_horizon_bps",
        "expected_move_decay_from_earliest_bps",
        "expected_move_retention_vs_earliest",
        "peak_absolute_expected_move_bps",
        "absolute_retention_vs_peak",
    ):
        result[name] = math.nan
    if result.empty:
        return result
    group_columns = [
        name
        for name in (
            "time_bucket",
            "analysis_kind",
            "asset",
            "signal_family",
            "execution_scenario",
            "execution_model",
        )
        if name in result.columns
    ]
    for positions in result.groupby(
        group_columns, dropna=False, sort=False
    ).groups.values():
        ordered = result.loc[list(positions)].sort_values(
            "horizon_ms", kind="mergesort"
        )
        expected = pd.to_numeric(ordered["expected_move_bps"], errors="coerce")
        counts = pd.to_numeric(ordered["evaluable_count"], errors="coerce").fillna(0)
        admissible = expected.notna() & counts.gt(0)
        if not bool(admissible.any()):
            continue
        earliest_index = admissible[admissible].index[0]
        earliest_horizon = _integer(
            result.at[earliest_index, "horizon_ms"], label="horizon_ms"
        )
        earliest_value = _optional_float(
            result.at[earliest_index, "expected_move_bps"]
        )
        if earliest_value is None:
            raise AssertionError("admissible expected move unexpectedly missing")
        peak = float(expected.loc[admissible].abs().max())
        for index in ordered.index:
            value = _optional_float(result.at[index, "expected_move_bps"])
            if value is None:
                continue
            result.at[index, "earliest_admissible_horizon_ms"] = earliest_horizon
            result.at[index, "expected_move_at_earliest_horizon_bps"] = earliest_value
            result.at[index, "expected_move_decay_from_earliest_bps"] = (
                value - earliest_value
            )
            if abs(earliest_value) > 1e-15:
                result.at[index, "expected_move_retention_vs_earliest"] = (
                    value / earliest_value
                )
            result.at[index, "peak_absolute_expected_move_bps"] = peak
            if peak > 1e-15:
                result.at[index, "absolute_retention_vs_peak"] = abs(value) / peak
    return result


def aggregate_streaming_events(
    source: DiskBackedEventSource,
    intervals: Sequence[StrictInterval],
    config: LeadLagConfig,
    *,
    randomization_diagnostics: RandomizationDiagnostics | None = None,
) -> StreamingAggregateResult:
    """Produce oracle-equivalent exact aggregates from a bounded event source."""

    if not isinstance(config, LeadLagConfig):
        raise TypeError("config must be a LeadLagConfig")
    strict_intervals = _validated_intervals(intervals)
    scanned = _scan_events(
        source,
        strict_intervals,
        config,
        randomization_diagnostics,
    )
    inference = _randomized_inference(
        source,
        scanned,
        config,
        randomization_diagnostics,
    )

    information_rows: list[dict[str, object]] = []
    for hypothesis_key in _hypothesis_order(config):
        information_rows.append(
            _information_metric_row(
                source,
                scanned.information.get(hypothesis_key, _InformationAccumulator()),
                key=hypothesis_key,
                minimum_events=config.minimum_events,
                inference=inference.get(hypothesis_key),
            )
        )

    scenario_by_name = {
        scenario.name: scenario for scenario in config.execution_scenarios
    }
    execution_rows: list[dict[str, object]] = []
    for scenario in config.execution_scenarios:
        for model in ("taker", "maker"):
            for hypothesis in _hypothesis_order(config):
                execution_key: _ExecutionKey = (scenario.name, model, *hypothesis)
                execution_rows.append(
                    _execution_metric_row(
                        source,
                        scanned.execution.get(
                            execution_key, _ExecutionAccumulator()
                        ),
                        key=execution_key,
                        scenario=scenario,
                        minimum_events=config.minimum_events,
                    )
                )

    bucket_information_rows: list[dict[str, object]] = []
    bucket_execution_rows: list[dict[str, object]] = []
    for bucket in _bucket_starts(strict_intervals, config.bucket_minutes):
        for hypothesis_key in _hypothesis_order(config):
            information_bucket_key = (bucket, *hypothesis_key)
            row = _information_metric_row(
                source,
                scanned.bucket_information.get(
                    information_bucket_key, _InformationAccumulator()
                ),
                key=hypothesis_key,
                minimum_events=config.minimum_events,
                inference=None,
                bucket=bucket,
            )
            row["time_bucket"] = bucket
            bucket_information_rows.append(row)
        for scenario in config.execution_scenarios:
            for model in ("taker", "maker"):
                for hypothesis in _hypothesis_order(config):
                    execution_key = (scenario.name, model, *hypothesis)
                    execution_bucket_key = (bucket, *execution_key)
                    row = _execution_metric_row(
                        source,
                        scanned.bucket_execution.get(
                            execution_bucket_key, _ExecutionAccumulator()
                        ),
                        key=execution_key,
                        scenario=scenario_by_name[scenario.name],
                        minimum_events=config.minimum_events,
                        bucket=bucket,
                    )
                    row["time_bucket"] = bucket
                    bucket_execution_rows.append(row)

    metrics = _add_decay_fields(
        _sort_frame(
            pd.concat(
                [
                    pd.DataFrame(information_rows),
                    pd.DataFrame(execution_rows),
                ],
                ignore_index=True,
                sort=False,
            ),
            (
                "analysis_kind",
                "asset",
                "signal_family",
                "horizon_ms",
                "execution_scenario",
                "execution_model",
            ),
        )
    )
    bucket_metrics = _add_decay_fields(
        _sort_frame(
            pd.concat(
                [
                    pd.DataFrame(bucket_information_rows),
                    pd.DataFrame(bucket_execution_rows),
                ],
                ignore_index=True,
                sort=False,
            ),
            (
                "time_bucket",
                "analysis_kind",
                "asset",
                "signal_family",
                "horizon_ms",
                "execution_scenario",
                "execution_model",
            ),
        )
    )
    controls = _sort_frame(
        pd.DataFrame(_control_rows(scanned, inference, config)),
        ("control_type", "asset", "signal_family", "horizon_ms"),
    )
    return StreamingAggregateResult(
        metrics=metrics,
        bucket_metrics=bucket_metrics,
        controls=controls,
    )


__all__ = [
    "DiskBackedEventSource",
    "RandomizationDiagnostics",
    "StreamingAggregateResult",
    "aggregate_streaming_events",
]
