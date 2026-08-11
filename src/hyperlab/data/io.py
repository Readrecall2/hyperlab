from __future__ import annotations

from pathlib import Path

import pandas as pd

from hyperlab.models import MarketPanel


def save_panel_csv(panel: MarketPanel, directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    panel.prices.to_csv(directory / "prices.csv")
    panel.funding.to_csv(directory / "funding.csv")
    panel.spreads_bps.to_csv(directory / "spreads_bps.csv")
    panel.volume_usd.to_csv(directory / "volume_usd.csv")


def load_panel_csv(directory: Path) -> MarketPanel:
    def read(name: str) -> pd.DataFrame:
        frame = pd.read_csv(directory / name, index_col=0, parse_dates=True)
        frame.index = pd.DatetimeIndex(frame.index)
        return frame

    prices = read("prices.csv")
    funding = read("funding.csv").reindex(columns=prices.columns)
    spreads = read("spreads_bps.csv").reindex(columns=prices.columns)
    volume = read("volume_usd.csv").reindex(columns=prices.columns)
    return MarketPanel(prices=prices, funding=funding, spreads_bps=spreads, volume_usd=volume)


def save_panel_parquet(panel: MarketPanel, directory: Path) -> None:
    """Save compact research files; requires the optional ``research`` dependencies."""
    directory.mkdir(parents=True, exist_ok=True)
    panel.prices.to_parquet(directory / "prices.parquet")
    panel.funding.to_parquet(directory / "funding.parquet")
    panel.spreads_bps.to_parquet(directory / "spreads_bps.parquet")
    panel.volume_usd.to_parquet(directory / "volume_usd.parquet")
