from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hyperlab.backtest.cross_exchange import CrossVenueMarketData, FundingConvention
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


def save_cross_venue_csv(
    data: CrossVenueMarketData,
    directory: Path,
    *,
    conventions: dict[str, FundingConvention],
) -> None:
    """Write a reproducible Phase-07 matrix export with explicit venue semantics."""

    data.validate(conventions)
    directory.mkdir(parents=True, exist_ok=True)
    data.mark_prices.to_csv(directory / "mark_prices.csv")
    data.oracle_prices.to_csv(directory / "oracle_prices.csv")
    data.funding_rates.to_csv(directory / "funding_rates.csv")
    if data.venue_available is not None:
        data.venue_available.to_csv(directory / "venue_available.csv")
    if data.transfers_available is not None:
        data.transfers_available.to_frame("available").to_csv(
            directory / "transfers_available.csv"
        )
    (directory / "cross_venue_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "asset": data.asset,
                "venues": list(data.venues),
                "conventions": {
                    venue: {
                        "interval_hours": convention.calendar.interval_hours,
                        "anchor_hour_utc": convention.calendar.anchor_hour_utc,
                        "explicit_settlements": [
                            timestamp.isoformat()
                            for timestamp in convention.calendar.explicit_settlements
                        ],
                        "notional_price_source": convention.notional_price_source,
                        "formula_name": convention.formula_name,
                        "documentation_url": convention.documentation_url,
                    }
                    for venue, convention in conventions.items()
                },
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (directory / "metadata.json").write_text(
        json.dumps(data.metadata, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )


def load_cross_venue_csv(
    directory: Path,
    *,
    conventions: dict[str, FundingConvention],
) -> CrossVenueMarketData:
    """Load the explicit Phase-07 format; never infer a venue funding calendar."""

    def read(name: str) -> pd.DataFrame:
        frame = pd.read_csv(directory / name, index_col=0, parse_dates=True)
        frame.index = pd.DatetimeIndex(frame.index)
        return frame

    def strict_boolean(value: object, *, field: str) -> bool:
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().casefold()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
        raise ValueError(f"{field} contains a non-boolean value: {value!r}")

    manifest_value = json.loads(
        (directory / "cross_venue_manifest.json").read_text(encoding="utf-8")
    )
    if not isinstance(manifest_value, dict):
        raise ValueError("cross_venue_manifest.json must contain an object")
    if manifest_value.get("schema_version") != 1:
        raise ValueError("unsupported cross-venue manifest schema_version")
    asset = manifest_value.get("asset")
    if not isinstance(asset, str) or not asset.strip():
        raise ValueError("cross-venue manifest needs an asset")
    declared_conventions = manifest_value.get("conventions")
    if not isinstance(declared_conventions, dict):
        raise ValueError("cross-venue manifest needs explicit funding conventions")
    expected = {
        venue: {
            "interval_hours": convention.calendar.interval_hours,
            "anchor_hour_utc": convention.calendar.anchor_hour_utc,
            "explicit_settlements": [
                timestamp.isoformat() for timestamp in convention.calendar.explicit_settlements
            ],
            "notional_price_source": convention.notional_price_source,
            "formula_name": convention.formula_name,
            "documentation_url": convention.documentation_url,
        }
        for venue, convention in conventions.items()
    }
    if declared_conventions != expected:
        raise ValueError("cross-venue funding conventions differ from the requested model")

    marks = read("mark_prices.csv")
    if manifest_value.get("venues") != list(marks.columns):
        raise ValueError("cross-venue manifest venues differ from mark_prices.csv")
    oracles = read("oracle_prices.csv").reindex(columns=marks.columns)
    funding = read("funding_rates.csv").reindex(columns=marks.columns)
    availability_path = directory / "venue_available.csv"
    availability = read("venue_available.csv").reindex(columns=marks.columns) if availability_path.exists() else None
    if availability is not None:
        availability = availability.map(
            lambda value: strict_boolean(value, field="venue_available")
        )
    transfers_path = directory / "transfers_available.csv"
    transfers = None
    if transfers_path.exists():
        transfers_frame = read("transfers_available.csv")
        transfers = transfers_frame["available"].map(
            lambda value: strict_boolean(value, field="transfers_available")
        )
    metadata_value = json.loads((directory / "metadata.json").read_text(encoding="utf-8"))
    if not isinstance(metadata_value, dict):
        raise ValueError("cross-venue metadata.json must contain an object")
    data = CrossVenueMarketData(
        asset=asset,
        mark_prices=marks,
        oracle_prices=oracles,
        funding_rates=funding,
        metadata=metadata_value,
        venue_available=availability,
        transfers_available=transfers,
    )
    data.validate(conventions)
    return data


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
