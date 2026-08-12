from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import pairwise
from typing import SupportsFloat, cast

import numpy as np
import pandas as pd

LEDGER_COLUMNS = ("timestamp", "asset", "regime", "size_usd", "component", "pnl")
DEFAULT_SIZE_BUCKET_EDGES = (0.0, 10_000.0, 100_000.0, math.inf)
DEFAULT_SIZE_BUCKET_LABELS = ("small", "medium", "large")


class AttributionReconciliationError(ValueError):
    """Raised when an attribution view no longer reconciles with its ledger total."""


@dataclass(slots=True)
class PnlAttribution:
    """Reconciled PnL component pivots for the required research dimensions."""

    by_asset: pd.DataFrame
    by_month: pd.DataFrame
    by_regime: pd.DataFrame
    by_size_bucket: pd.DataFrame
    component_totals: pd.Series
    total_pnl: float
    row_count: int
    reconciliation_tolerance: float

    def assert_reconciled(self) -> None:
        """Raise if any component, row total, or view total differs from the ledger total."""

        components = tuple(str(value) for value in self.component_totals.index)
        expected_columns = (*components, "total_pnl")
        _assert_close(
            math.fsum(float(value) for value in self.component_totals),
            self.total_pnl,
            "component total",
            self.reconciliation_tolerance,
        )
        for view_name, frame in (
            ("asset", self.by_asset),
            ("month", self.by_month),
            ("regime", self.by_regime),
            ("size_bucket", self.by_size_bucket),
        ):
            if tuple(str(column) for column in frame.columns) != expected_columns:
                raise AttributionReconciliationError(
                    f"{view_name} attribution has unexpected component columns"
                )
            if frame.index.has_duplicates:
                raise AttributionReconciliationError(f"{view_name} attribution has duplicate groups")
            calculated_rows = frame.loc[:, list(components)].sum(axis=1)
            for group, calculated in calculated_rows.items():
                _assert_close(
                    float(calculated),
                    _scalar_float(frame.at[group, "total_pnl"]),
                    f"{view_name} row {group!r}",
                    self.reconciliation_tolerance,
                )
            for component in components:
                _assert_close(
                    float(frame[component].sum()),
                    _scalar_float(self.component_totals.at[component]),
                    f"{view_name} component {component!r}",
                    self.reconciliation_tolerance,
                )
            _assert_close(
                float(frame["total_pnl"].sum()),
                self.total_pnl,
                f"{view_name} total",
                self.reconciliation_tolerance,
            )


def _assert_close(actual: float, expected: float, label: str, tolerance: float) -> None:
    if not math.isfinite(actual) or not math.isfinite(expected):
        raise AttributionReconciliationError(f"{label} is not finite")
    if not math.isclose(actual, expected, rel_tol=1e-12, abs_tol=tolerance):
        raise AttributionReconciliationError(
            f"{label} does not reconcile: actual={actual!r}, expected={expected!r}"
        )


def _scalar_float(value: object) -> float:
    return float(cast(SupportsFloat, value))


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        raise TypeError(f"{name} must be an integer")
    integer = int(value)
    if integer <= 0:
        raise ValueError(f"{name} must be positive")
    return integer


def _utc_timestamps(values: pd.Series) -> pd.DatetimeIndex:
    timestamps: list[pd.Timestamp] = []
    for position, value in enumerate(values):
        try:
            timestamp = pd.Timestamp(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"timestamp at row {position} is invalid") from error
        if pd.isna(timestamp):
            raise ValueError(f"timestamp at row {position} is missing")
        if timestamp.tzinfo is None:
            raise ValueError(f"timestamp at row {position} must be timezone-aware")
        timestamps.append(timestamp.tz_convert("UTC"))
    return pd.DatetimeIndex(timestamps)


def _validated_labels(frame: pd.DataFrame, column: str) -> None:
    for position, value in enumerate(frame[column]):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{column} at row {position} must be a non-empty string")
        if column == "component" and value == "total_pnl":
            raise ValueError("component name 'total_pnl' is reserved")


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    try:
        result = pd.to_numeric(frame[column], errors="raise").astype(float)
    except (TypeError, ValueError) as error:
        raise TypeError(f"{column} must be numeric") from error
    if not bool(np.isfinite(result.to_numpy()).all()):
        raise ValueError(f"{column} must contain only finite values")
    return result


def _size_buckets(
    sizes: pd.Series,
    edges: tuple[float, ...],
    labels: tuple[str, ...],
) -> pd.Series:
    if len(edges) < 2:
        raise ValueError("size_bucket_edges must contain at least two edges")
    if edges[0] != 0.0 or not math.isinf(edges[-1]) or edges[-1] < 0.0:
        raise ValueError("size_bucket_edges must start at zero and end at positive infinity")
    if any(math.isnan(edge) for edge in edges):
        raise ValueError("size_bucket_edges cannot contain NaN")
    if any(left >= right for left, right in pairwise(edges)):
        raise ValueError("size_bucket_edges must be strictly increasing")
    if len(labels) != len(edges) - 1:
        raise ValueError("size_bucket_labels must have one label per interval")
    if len(set(labels)) != len(labels) or any(not label.strip() for label in labels):
        raise ValueError("size_bucket_labels must be non-empty and unique")
    buckets = pd.cut(
        sizes,
        bins=list(edges),
        labels=list(labels),
        include_lowest=True,
        right=False,
        ordered=True,
    )
    if bool(buckets.isna().any()):
        raise ValueError("every size_usd value must belong to a configured size bucket")
    return buckets


def _pivot(frame: pd.DataFrame, dimension: str, components: tuple[str, ...]) -> pd.DataFrame:
    grouped = frame.groupby(
        [dimension, "component"],
        observed=True,
        sort=True,
        dropna=False,
    )["pnl"].sum()
    result = grouped.unstack("component", fill_value=0)
    result = result.reindex(columns=list(components), fill_value=0.0).astype(float)
    result["total_pnl"] = result.loc[:, list(components)].sum(axis=1)
    result.index.name = dimension
    return result


def aggregate_pnl(
    ledger: pd.DataFrame,
    *,
    size_bucket_edges: tuple[float, ...] = DEFAULT_SIZE_BUCKET_EDGES,
    size_bucket_labels: tuple[str, ...] = DEFAULT_SIZE_BUCKET_LABELS,
    reconciliation_tolerance: float = 1e-9,
) -> PnlAttribution:
    """Aggregate a long PnL ledger and strictly reconcile every resulting view.

    Each ledger row describes one PnL component and must contain ``timestamp``,
    ``asset``, ``regime``, ``size_usd``, ``component`` and ``pnl``. Timestamps are
    converted to UTC before assigning calendar months.
    """

    if not isinstance(ledger, pd.DataFrame):
        raise TypeError("ledger must be a pandas DataFrame")
    missing = [column for column in LEDGER_COLUMNS if column not in ledger.columns]
    if missing:
        raise ValueError(f"ledger is missing required columns: {missing}")
    if ledger.empty:
        raise ValueError("ledger cannot be empty")
    if not math.isfinite(reconciliation_tolerance) or reconciliation_tolerance < 0.0:
        raise ValueError("reconciliation_tolerance must be finite and non-negative")

    frame = ledger.loc[:, list(LEDGER_COLUMNS)].copy(deep=True)
    frame["timestamp"] = _utc_timestamps(frame["timestamp"])
    for column in ("asset", "regime", "component"):
        _validated_labels(frame, column)
    frame["size_usd"] = _numeric_column(frame, "size_usd")
    frame["pnl"] = _numeric_column(frame, "pnl")
    if bool((frame["size_usd"] < 0.0).any()):
        raise ValueError("size_usd must be non-negative")

    try:
        edges = tuple(float(edge) for edge in size_bucket_edges)
    except (TypeError, ValueError) as error:
        raise TypeError("size_bucket_edges must be numeric") from error
    if not all(isinstance(label, str) for label in size_bucket_labels):
        raise TypeError("size_bucket_labels must contain strings")
    labels = tuple(size_bucket_labels)
    frame["month_utc"] = frame["timestamp"].dt.strftime("%Y-%m")
    frame["size_bucket"] = _size_buckets(frame["size_usd"], edges, labels)

    components = tuple(sorted(str(value) for value in frame["component"].unique()))
    component_totals = (
        frame.groupby("component", observed=True, sort=True)["pnl"]
        .sum()
        .reindex(list(components))
        .astype(float)
    )
    component_totals.index.name = "component"
    total_pnl = math.fsum(float(value) for value in frame["pnl"])
    result = PnlAttribution(
        by_asset=_pivot(frame, "asset", components),
        by_month=_pivot(frame, "month_utc", components),
        by_regime=_pivot(frame, "regime", components),
        by_size_bucket=_pivot(frame, "size_bucket", components),
        component_totals=component_totals,
        total_pnl=total_pnl,
        row_count=len(frame),
        reconciliation_tolerance=float(reconciliation_tolerance),
    )
    result.assert_reconciled()
    return result


def causal_regimes(
    returns: pd.Series,
    *,
    lookback: int = 24,
    calm_volatility: float = 0.002,
    chaos_volatility: float = 0.02,
    trend_threshold: float = 0.001,
) -> pd.Series:
    """Classify each timestamp using a fixed window ending strictly before it."""

    if not isinstance(returns, pd.Series):
        raise TypeError("returns must be a pandas Series")
    if returns.empty:
        raise ValueError("returns cannot be empty")
    window = _positive_integer(lookback, "lookback")
    if window < 2:
        raise ValueError("lookback must be at least 2")
    index = returns.index
    if not isinstance(index, pd.DatetimeIndex):
        raise TypeError("returns index must be a DatetimeIndex")
    if index.tz is None:
        raise ValueError("returns index must be timezone-aware")
    if not index.is_monotonic_increasing or index.has_duplicates:
        raise ValueError("returns index must be strictly increasing")
    for name, value in (
        ("calm_volatility", calm_volatility),
        ("chaos_volatility", chaos_volatility),
        ("trend_threshold", trend_threshold),
    ):
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(f"{name} must be finite and non-negative")
    if calm_volatility >= chaos_volatility:
        raise ValueError("calm_volatility must be lower than chaos_volatility")
    if trend_threshold == 0.0:
        raise ValueError("trend_threshold must be positive")

    try:
        numeric = pd.to_numeric(returns, errors="raise").astype(float)
    except (TypeError, ValueError) as error:
        raise TypeError("returns must be numeric") from error
    if not bool(np.isfinite(numeric.to_numpy()).all()):
        raise ValueError("returns must contain only finite observations")

    past = numeric.shift(1)
    trailing_mean = past.rolling(window, min_periods=window).mean()
    trailing_volatility = past.rolling(window, min_periods=window).std(ddof=0)
    ready = trailing_mean.notna() & trailing_volatility.notna()
    regimes = pd.Series("warmup", index=index, dtype="string", name="regime")
    regimes.loc[ready] = "neutral"

    chaos = ready & (trailing_volatility >= chaos_volatility)
    trend_up = ready & ~chaos & (trailing_mean >= trend_threshold)
    trend_down = ready & ~chaos & (trailing_mean <= -trend_threshold)
    calm = ready & ~chaos & ~trend_up & ~trend_down & (trailing_volatility <= calm_volatility)
    regimes.loc[chaos] = "chaos"
    regimes.loc[trend_up] = "trend_up"
    regimes.loc[trend_down] = "trend_down"
    regimes.loc[calm] = "calm"
    return regimes
