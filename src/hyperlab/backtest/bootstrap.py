from __future__ import annotations

import math
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, cast

import numpy as np
import pandas as pd


class InsufficientBootstrapSampleWarning(UserWarning):
    """Warn that too few independent blocks are available for a reliable interval."""


@dataclass(frozen=True, slots=True)
class BlockBootstrapInterval:
    """Percentile interval produced from contiguous moving-block resamples."""

    estimate: float
    lower: float
    upper: float
    standard_error: float
    confidence_level: float
    block_size: int
    n_resamples: int
    sample_size: int
    effective_blocks: float
    seed: int
    insufficient_sample: bool
    time_index_verified: bool
    cadence: str | None
    bootstrap_statistics: tuple[float, ...]


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        raise TypeError(f"{name} must be an integer")
    integer = int(value)
    if integer <= 0:
        raise ValueError(f"{name} must be positive")
    return integer


def _seed(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        raise TypeError("seed must be an integer")
    seed = int(value)
    if seed < 0:
        raise ValueError("seed must be non-negative")
    return seed


def moving_block_indices(
    sample_size: int,
    *,
    block_size: int,
    n_resamples: int,
    seed: int,
) -> np.ndarray:
    """Return resample indices made exclusively from contiguous, non-wrapping blocks.

    Blocks are sampled with replacement from every valid start in the original sample.
    The final block of a resample is truncated to ``sample_size`` when necessary.
    """

    n = _positive_integer(sample_size, "sample_size")
    block = _positive_integer(block_size, "block_size")
    draws = _positive_integer(n_resamples, "n_resamples")
    rng_seed = _seed(seed)
    if block > n:
        raise ValueError("block_size cannot exceed sample_size")

    blocks_per_resample = math.ceil(n / block)
    rng = np.random.default_rng(rng_seed)
    starts = rng.integers(
        0,
        n - block + 1,
        size=(draws, blocks_per_resample),
    )
    offsets = np.arange(block, dtype=np.int64)
    indices = starts[..., np.newaxis] + offsets
    return indices.reshape(draws, blocks_per_resample * block)[:, :n]


def _numeric_sample(values: Sequence[float] | np.ndarray | pd.Series) -> np.ndarray:
    try:
        sample = np.asarray(values, dtype=float)
    except (TypeError, ValueError) as error:
        raise TypeError("values must be a one-dimensional numeric sample") from error
    if sample.ndim != 1:
        raise ValueError("values must be one-dimensional")
    if sample.size == 0:
        raise ValueError("values cannot be empty")
    if not bool(np.isfinite(sample).all()):
        raise ValueError("values must contain only finite observations")
    return sample


def _validated_time_index(
    values: Sequence[float] | np.ndarray | pd.Series,
    timestamps: pd.DatetimeIndex | Sequence[object] | None,
    *,
    sample_size: int,
) -> tuple[bool, str | None]:
    if timestamps is not None and isinstance(values, pd.Series):
        raise ValueError("timestamps cannot be supplied when values is already a Series")
    raw_index: object | None = values.index if isinstance(values, pd.Series) else timestamps
    if raw_index is None:
        raise ValueError("block bootstrap requires an explicit UTC time index")
    if isinstance(raw_index, pd.DatetimeIndex):
        index = raw_index
    else:
        try:
            index = pd.DatetimeIndex(cast(Any, raw_index))
        except (TypeError, ValueError) as error:
            raise TypeError("bootstrap timestamps must form a DatetimeIndex") from error
    if len(index) != sample_size:
        raise ValueError("bootstrap timestamps must match the number of observations")
    if index.hasnans:
        raise ValueError("bootstrap time index cannot contain missing timestamps")
    if index.tz is None or str(index.tz).upper() not in {"UTC", "UTC+00:00"}:
        raise ValueError("bootstrap time index must use UTC")
    if not index.is_monotonic_increasing or index.has_duplicates:
        raise ValueError("bootstrap time index must be strictly increasing")
    if len(index) < 2:
        return True, None
    deltas = index[1:] - index[:-1]
    cadence = deltas[0]
    if cadence <= pd.Timedelta(0) or not bool((deltas == cadence).all()):
        raise ValueError("bootstrap time index contains gaps or an irregular cadence")
    return True, str(cadence)


def block_bootstrap_ci(
    values: Sequence[float] | np.ndarray | pd.Series,
    *,
    block_size: int,
    n_resamples: int = 2_000,
    confidence_level: float = 0.95,
    seed: int = 0,
    statistic: Callable[[np.ndarray], float] | None = None,
    minimum_effective_blocks: int = 5,
    timestamps: pd.DatetimeIndex | Sequence[object] | None = None,
) -> BlockBootstrapInterval:
    """Estimate a percentile confidence interval with a moving-block bootstrap.

    The default statistic is the arithmetic mean. ``values`` must be a Series with a
    regular, strictly increasing UTC index, or callers must supply equivalent
    ``timestamps`` explicitly; blocks are never allowed to bridge an unobserved gap.
    A sample with fewer than ``minimum_effective_blocks`` non-overlapping block
    equivalents is still processed, but emits
    :class:`InsufficientBootstrapSampleWarning` and marks the result.
    """

    sample = _numeric_sample(values)
    block = _positive_integer(block_size, "block_size")
    draws = _positive_integer(n_resamples, "n_resamples")
    rng_seed = _seed(seed)
    minimum_blocks = _positive_integer(minimum_effective_blocks, "minimum_effective_blocks")
    if block > sample.size:
        raise ValueError("block_size cannot exceed the number of observations")
    if draws < 2:
        raise ValueError("n_resamples must be at least 2")
    if not math.isfinite(confidence_level) or not 0.0 < confidence_level < 1.0:
        raise ValueError("confidence_level must be finite and strictly between 0 and 1")
    if statistic is not None and not callable(statistic):
        raise TypeError("statistic must be callable")
    time_index_verified, cadence = _validated_time_index(
        values,
        timestamps,
        sample_size=int(sample.size),
    )

    selected_statistic = np.mean if statistic is None else statistic
    estimate = float(selected_statistic(sample.copy()))
    if not math.isfinite(estimate):
        raise ValueError("statistic must return a finite scalar")

    effective_blocks = float(sample.size / block)
    insufficient = effective_blocks < minimum_blocks
    if insufficient:
        warnings.warn(
            (
                "block bootstrap sample is insufficient: "
                f"{effective_blocks:.2f} effective blocks, at least {minimum_blocks} recommended"
            ),
            InsufficientBootstrapSampleWarning,
            stacklevel=2,
        )

    indices = moving_block_indices(
        int(sample.size),
        block_size=block,
        n_resamples=draws,
        seed=rng_seed,
    )
    statistics = np.empty(draws, dtype=float)
    for position, row in enumerate(indices):
        statistics[position] = float(selected_statistic(sample[row].copy()))
    if not bool(np.isfinite(statistics).all()):
        raise ValueError("statistic must return a finite scalar for every resample")

    alpha = 1.0 - confidence_level
    lower, upper = np.quantile(statistics, [alpha / 2.0, 1.0 - alpha / 2.0])
    return BlockBootstrapInterval(
        estimate=estimate,
        lower=float(lower),
        upper=float(upper),
        standard_error=float(statistics.std(ddof=1)),
        confidence_level=float(confidence_level),
        block_size=block,
        n_resamples=draws,
        sample_size=int(sample.size),
        effective_blocks=effective_blocks,
        seed=rng_seed,
        insufficient_sample=insufficient,
        time_index_verified=time_index_verified,
        cadence=cadence,
        bootstrap_statistics=tuple(float(value) for value in statistics),
    )
