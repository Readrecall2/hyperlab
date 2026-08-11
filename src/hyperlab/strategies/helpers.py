from __future__ import annotations

from typing import SupportsFloat, cast

import pandas as pd

from hyperlab.models import MarketPanel


def empty_weights(panel: MarketPanel) -> pd.DataFrame:
    return pd.DataFrame(0.0, index=panel.prices.index, columns=panel.prices.columns)


def columns_by(panel: MarketPanel, *, exchange: str | None = None, kind: str | None = None) -> list[str]:
    result: list[str] = []
    for column in panel.prices.columns:
        parts = column.split(":")
        if len(parts) != 3:
            continue
        col_exchange, _, col_kind = parts
        if exchange is not None and col_exchange != exchange.upper():
            continue
        if kind is not None and col_kind != kind:
            continue
        result.append(column)
    return result


def asset_from(column: str) -> str:
    return column.split(":")[1]


def scalar_float(value: object) -> float:
    """Normalize a pandas scalar at the boundary of numeric strategy logic."""
    return float(cast(SupportsFloat, value))


def rebalance_mask(index: pd.Index, every_hours: int) -> pd.Series:
    if every_hours <= 0:
        raise ValueError("every_hours must be positive")
    datetime_index = pd.DatetimeIndex(index)
    elapsed = (datetime_index - datetime_index[0]).total_seconds() / 3600.0
    return pd.Series((elapsed.astype(int) % every_hours) == 0, index=datetime_index)
