from __future__ import annotations

from dataclasses import dataclass

from hyperlab.models import MarketPanel


@dataclass(frozen=True, slots=True)
class PanelSplit:
    train: MarketPanel
    validation: MarketPanel
    test: MarketPanel


def _slice(panel: MarketPanel, start: int, stop: int) -> MarketPanel:
    index = panel.prices.index[start:stop]
    return MarketPanel(
        prices=panel.prices.loc[index].copy(),
        funding=panel.funding.loc[index].copy(),
        spreads_bps=panel.spreads_bps.loc[index].copy(),
        volume_usd=panel.volume_usd.loc[index].copy(),
        metadata={**panel.metadata, "slice": (start, stop)},
    )


def chronological_split(
    panel: MarketPanel,
    train_fraction: float = 0.60,
    validation_fraction: float = 0.20,
) -> PanelSplit:
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between 0 and 1")
    if not 0 < validation_fraction < 1:
        raise ValueError("validation_fraction must be between 0 and 1")
    if train_fraction + validation_fraction >= 1:
        raise ValueError("train + validation must leave a test segment")

    n = len(panel.prices)
    train_end = int(n * train_fraction)
    validation_end = int(n * (train_fraction + validation_fraction))
    return PanelSplit(
        train=_slice(panel, 0, train_end),
        validation=_slice(panel, train_end, validation_end),
        test=_slice(panel, validation_end, n),
    )
