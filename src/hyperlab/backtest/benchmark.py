from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

from hyperlab.models import MarketPanel


@dataclass(frozen=True, slots=True)
class PassiveBenchmarkSpec:
    """A passive comparison on the exact simulated calendar.

    With ``instrument=None`` this is a continuously accrued cash/yield alternative.
    With an instrument, it is a buy-and-hold price benchmark normalized to one.
    Neither form is an optimization target.
    """

    annual_rate: float = 0.045
    instrument: str | None = None
    label: str = "passive_cash_yield"
    source: str = "research-assumption"

    def __post_init__(self) -> None:
        if not math.isfinite(self.annual_rate) or self.annual_rate <= -1.0:
            raise ValueError("annual_rate must be finite and greater than -1")
        if not self.label.strip() or not self.source.strip():
            raise ValueError("benchmark label and source cannot be empty")


def build_passive_benchmark(panel: MarketPanel, spec: PassiveBenchmarkSpec) -> pd.Series:
    panel.validate()
    index = panel.prices.index
    if spec.instrument is not None:
        if spec.instrument not in panel.prices:
            raise ValueError(f"benchmark instrument is absent: {spec.instrument}")
        prices = panel.prices[spec.instrument]
        if prices.isna().any() or bool((prices <= 0.0).any()):
            raise ValueError("benchmark instrument requires complete positive prices")
        benchmark = prices / float(prices.iloc[0])
    else:
        elapsed_years = (index - index[0]).total_seconds() / (365.25 * 24.0 * 3600.0)
        benchmark = pd.Series(
            (1.0 + spec.annual_rate) ** elapsed_years,
            index=index,
            dtype=float,
        )
    benchmark.name = spec.label
    benchmark.attrs.update(
        {
            "annual_rate": spec.annual_rate,
            "instrument": spec.instrument,
            "source": spec.source,
        }
    )
    return benchmark
