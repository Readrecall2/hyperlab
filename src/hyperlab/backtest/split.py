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
        metadata={
            **panel.metadata,
            "slice": (start, stop),
            "start": index[0].isoformat() if len(index) else None,
            "end_exclusive": panel.prices.index[stop].isoformat() if stop < len(panel.prices) else None,
        },
        depth_usd=panel.depth_usd.loc[index].copy() if panel.depth_usd is not None else None,
        open_interest_usd=(
            panel.open_interest_usd.loc[index].copy()
            if panel.open_interest_usd is not None
            else None
        ),
        liquidation_usd=(
            panel.liquidation_usd.loc[index].copy()
            if panel.liquidation_usd is not None
            else None
        ),
        available_at=panel.available_at.loc[index].copy() if panel.available_at is not None else None,
        finality=panel.finality.loc[index].copy() if panel.finality is not None else None,
        tradable=panel.tradable.loc[index].copy() if panel.tradable is not None else None,
        regimes=panel.regimes.loc[index].copy() if panel.regimes is not None else None,
    )


def chronological_split(
    panel: MarketPanel,
    train_fraction: float = 0.60,
    validation_fraction: float = 0.20,
) -> PanelSplit:
    panel.validate()
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between 0 and 1")
    if not 0 < validation_fraction < 1:
        raise ValueError("validation_fraction must be between 0 and 1")
    if train_fraction + validation_fraction >= 1:
        raise ValueError("train + validation must leave a test segment")

    n = len(panel.prices)
    train_end = int(n * train_fraction)
    validation_end = int(n * (train_fraction + validation_fraction))
    if train_end <= 0 or validation_end <= train_end or validation_end >= n:
        raise ValueError("split fractions produce an empty train, validation, or test segment")
    return PanelSplit(
        train=_slice(panel, 0, train_end),
        validation=_slice(panel, train_end, validation_end),
        test=_slice(panel, validation_end, n),
    )
