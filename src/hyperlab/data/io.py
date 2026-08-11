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
    """Save one immutable legacy panel snapshot.

    The event lake is the canonical Phase 01 storage layer. This compact matrix export is
    retained for compatibility and deliberately refuses to replace an existing snapshot.
    """
    try:
        # Owning the leaf directory atomically prevents two writers from passing a
        # separate exists() check and racing on the fixed compatibility filenames.
        directory.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        raise FileExistsError(f"immutable panel export already exists: {directory}") from None
    frames = {
        "prices.parquet": panel.prices,
        "funding.parquet": panel.funding,
        "spreads_bps.parquet": panel.spreads_bps,
        "volume_usd.parquet": panel.volume_usd,
    }
    temporary_paths: list[Path] = []
    try:
        for name, frame in frames.items():
            temporary = directory / f".{name}.tmp"
            frame.to_parquet(temporary)
            temporary_paths.append(temporary)
        for name in frames:
            (directory / f".{name}.tmp").replace(directory / name)
    finally:
        for temporary in temporary_paths:
            temporary.unlink(missing_ok=True)
