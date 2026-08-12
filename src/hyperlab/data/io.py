from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hyperlab.models import MarketPanel


def save_panel_csv(panel: MarketPanel, directory: Path) -> None:
    panel.validate()
    directory.mkdir(parents=True, exist_ok=True)
    panel.prices.to_csv(directory / "prices.csv")
    panel.funding.to_csv(directory / "funding.csv")
    panel.spreads_bps.to_csv(directory / "spreads_bps.csv")
    panel.volume_usd.to_csv(directory / "volume_usd.csv")
    optional_frames = {
        "depth_usd.csv": panel.depth_usd,
        "open_interest_usd.csv": panel.open_interest_usd,
        "available_at.csv": panel.available_at,
        "finality.csv": panel.finality,
        "tradable.csv": panel.tradable,
    }
    for name, frame in optional_frames.items():
        if frame is not None:
            frame.to_csv(directory / name)
    if panel.regimes is not None:
        panel.regimes.to_frame("regime").to_csv(directory / "regimes.csv")
    (directory / "metadata.json").write_text(
        json.dumps(panel.metadata, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def load_panel_csv(directory: Path) -> MarketPanel:
    def read(name: str) -> pd.DataFrame:
        frame = pd.read_csv(directory / name, index_col=0, parse_dates=True)
        frame.index = pd.DatetimeIndex(frame.index)
        return frame

    prices = read("prices.csv")
    funding = read("funding.csv").reindex(columns=prices.columns)
    spreads = read("spreads_bps.csv").reindex(columns=prices.columns)
    volume = read("volume_usd.csv").reindex(columns=prices.columns)

    def read_optional(name: str) -> pd.DataFrame | None:
        path = directory / name
        return read(name).reindex(columns=prices.columns) if path.exists() else None

    available_at = read_optional("available_at.csv")
    if available_at is not None:
        available_at = available_at.map(lambda value: pd.Timestamp(value) if not pd.isna(value) else pd.NaT)
    finality = read_optional("finality.csv")
    tradable = read_optional("tradable.csv")

    def strict_boolean(value: object, *, field: str) -> object:
        if isinstance(value, bool) or value is None or value is pd.NA or value is pd.NaT:
            return value
        if isinstance(value, float) and pd.isna(value):
            return value
        normalized = str(value).strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
        raise ValueError(f"{field} CSV contains a non-boolean value: {value!r}")

    for frame in (finality, tradable):
        if frame is not None:
            field = "finality" if frame is finality else "tradable"
            frame[:] = frame.map(lambda value, field=field: strict_boolean(value, field=field))
    regimes_path = directory / "regimes.csv"
    regimes = None
    if regimes_path.exists():
        regimes_frame = pd.read_csv(regimes_path, index_col=0, parse_dates=True)
        regimes_frame.index = pd.DatetimeIndex(regimes_frame.index)
        regimes = regimes_frame["regime"].reindex(prices.index)
    metadata_path = directory / "metadata.json"
    metadata: dict[str, object] = {}
    if metadata_path.exists():
        loaded = json.loads(metadata_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("panel metadata.json must contain an object")
        metadata = loaded
    panel = MarketPanel(
        prices=prices,
        funding=funding,
        spreads_bps=spreads,
        volume_usd=volume,
        metadata=metadata,
        depth_usd=read_optional("depth_usd.csv"),
        open_interest_usd=read_optional("open_interest_usd.csv"),
        available_at=available_at,
        finality=finality,
        tradable=tradable,
        regimes=regimes,
    )
    panel.validate()
    return panel


def save_panel_parquet(panel: MarketPanel, directory: Path) -> None:
    """Save one immutable legacy panel snapshot.

    The event lake is the canonical Phase 01 storage layer. This compact matrix export is
    retained for compatibility and deliberately refuses to replace an existing snapshot.
    """
    panel.validate()
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
    for name, frame in {
        "depth_usd.parquet": panel.depth_usd,
        "open_interest_usd.parquet": panel.open_interest_usd,
        "available_at.parquet": panel.available_at,
        "finality.parquet": panel.finality,
        "tradable.parquet": panel.tradable,
    }.items():
        if frame is not None:
            frames[name] = frame
    if panel.regimes is not None:
        frames["regimes.parquet"] = panel.regimes.to_frame("regime")
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
