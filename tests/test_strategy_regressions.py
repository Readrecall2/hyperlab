from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from hyperlab.models import MarketPanel
from hyperlab.strategies.momentum import MomentumRegimeStrategy
from hyperlab.strategies.pairs import PairsMeanReversionStrategy


def _market_panel(prices: pd.DataFrame, *, funding: float = 0.0) -> MarketPanel:
    return MarketPanel(
        prices=prices,
        funding=pd.DataFrame(funding, index=prices.index, columns=prices.columns),
        spreads_bps=pd.DataFrame(0.0, index=prices.index, columns=prices.columns),
        volume_usd=pd.DataFrame(1_000_000.0, index=prices.index, columns=prices.columns),
    )


def _pairs_outlier_panel(outlier: float) -> MarketPanel:
    index = pd.date_range("2026-01-01", periods=61, freq="1h", tz="UTC")
    return_pattern = np.array([0.01, -0.01] * 9 + [0.0, 0.0])
    log_b = np.concatenate(([0.0], np.cumsum(np.tile(return_pattern, 3))))
    residual = np.zeros(len(index))
    residual[-1] = outlier
    prices = pd.DataFrame(
        {
            "HL:ETH:perp": 100.0 * np.exp(log_b + residual),
            "HL:BTC:perp": 100.0 * np.exp(log_b),
        },
        index=index,
    )
    return _market_panel(prices)


@pytest.mark.parametrize("outlier", [0.5, -0.5])
def test_pairs_does_not_enter_beyond_stop(outlier: float) -> None:
    panel = _pairs_outlier_panel(outlier)
    blocked = PairsMeanReversionStrategy(
        lookback_hours=20,
        enter_z=2.0,
        exit_z=0.5,
        stop_z=4.0,
    ).generate(panel)
    permitted = PairsMeanReversionStrategy(
        lookback_hours=20,
        enter_z=2.0,
        exit_z=0.5,
        stop_z=5.0,
    ).generate(panel)

    assert float(blocked.weights.iloc[-1].abs().sum()) == 0.0
    assert float(permitted.weights.iloc[-1].abs().sum()) > 0.0


def test_positive_funding_rewards_negative_momentum_without_reversing_it() -> None:
    index = pd.date_range("2026-01-01", periods=32, freq="1h", tz="UTC")
    hourly_returns = np.array([-0.01, -0.02] * 16)
    prices = pd.DataFrame(
        {"HL:BTC:perp": 100.0 * np.cumprod(1.0 + hourly_returns)},
        index=index,
    )
    panel = _market_panel(prices, funding=0.001)

    result = MomentumRegimeStrategy(
        lookback_hours=4,
        volatility_hours=4,
        assets_to_trade=1,
        minimum_signal=0.25,
        funding_penalty=10_000.0,
        rebalance_hours=1,
    ).generate(panel)

    assert result.weights.at[index[-1], "HL:BTC:perp"] == pytest.approx(-1.0)
