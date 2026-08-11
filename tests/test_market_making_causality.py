from __future__ import annotations

import pandas as pd
import pytest

from hyperlab.strategies.market_making import InventoryAwareMarketMaker


def test_flow_fills_only_quotes_from_the_previous_event() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1s", tz="UTC")
    events = pd.DataFrame(
        {
            "mid": [100.0, 100.0],
            "bid": [99.0, 99.0],
            "ask": [101.0, 101.0],
            "spread_bps": [200.0, 200.0],
            "buy_trade_qty": [100.0, 100.0],
            "sell_trade_qty": [100.0, 100.0],
            "toxicity": [0.0, 0.0],
        },
        index=index,
    )

    result = InventoryAwareMarketMaker(
        queue_ahead_units=0.0,
        maker_fee_bps=0.0,
        taker_fee_bps=0.0,
        seed=0,
    ).run(events)

    assert result.returns["net_return"].iloc[0] == pytest.approx(0.0)
    assert result.diagnostics["maker_fills"] == 2
    assert result.metrics.total_return == pytest.approx(0.0005)
    assert result.metrics.turnover == pytest.approx(0.05)
    assert result.returns["price_return"].iloc[1] == pytest.approx(0.0005)
    assert result.returns["net_return"].equals(
        result.returns[["price_return", "funding_return", "cost_return"]].sum(axis=1)
    )


def test_final_inventory_close_reconciles_pnl_components() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1s", tz="UTC")
    events = pd.DataFrame(
        {
            "mid": [100.0, 100.0],
            "bid": [99.0, 99.0],
            "ask": [101.0, 101.0],
            "spread_bps": [200.0, 200.0],
            "buy_trade_qty": [0.0, 0.0],
            "sell_trade_qty": [100.0, 100.0],
            "toxicity": [0.0, 0.0],
        },
        index=index,
    )

    result = InventoryAwareMarketMaker(
        queue_ahead_units=0.0,
        max_inventory_fraction=1.0,
        maker_fee_bps=0.0,
        taker_fee_bps=100.0,
        seed=0,
    ).run(events)

    components = result.returns[["price_return", "funding_return", "cost_return"]].sum(axis=1)
    assert result.diagnostics["maker_fills"] == 1
    assert result.returns["net_return"].to_numpy() == pytest.approx(components.to_numpy())
    assert result.diagnostics["ending_cash"] == pytest.approx(19_995.05)


def test_partial_fill_uses_only_available_trade_flow() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1s", tz="UTC")
    events = pd.DataFrame(
        {
            "mid": [100.0, 100.0],
            "bid": [99.0, 99.0],
            "ask": [101.0, 101.0],
            "spread_bps": [200.0, 200.0],
            "buy_trade_qty": [0.0, 0.0],
            "sell_trade_qty": [0.0, 2.0],
            "toxicity": [0.0, 0.0],
        },
        index=index,
    )

    result = InventoryAwareMarketMaker(
        queue_ahead_units=0.0,
        max_inventory_fraction=1.0,
        maker_fee_bps=0.0,
        taker_fee_bps=0.0,
        seed=0,
    ).run(events)

    assert result.diagnostics["maker_fills"] == 1
    assert result.diagnostics["partial_fills"] == 1
    assert result.diagnostics["filled_units"] == pytest.approx(2.0)


def test_stale_quote_is_not_filled_without_reaching_its_price() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1s", tz="UTC")
    events = pd.DataFrame(
        {
            "mid": [100.0, 200.0],
            "bid": [99.0, 199.0],
            "ask": [101.0, 201.0],
            "spread_bps": [200.0, 100.0],
            "buy_trade_qty": [0.0, 0.0],
            "sell_trade_qty": [0.0, 100.0],
            "toxicity": [0.0, 0.0],
        },
        index=index,
    )

    result = InventoryAwareMarketMaker(
        queue_ahead_units=0.0,
        maker_fee_bps=0.0,
        taker_fee_bps=0.0,
        seed=0,
    ).run(events)

    assert result.diagnostics["maker_fills"] == 0
    assert result.metrics.total_return == pytest.approx(0.0)


def test_emergency_taker_spread_is_reported_as_a_cost() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1s", tz="UTC")
    events = pd.DataFrame(
        {
            "mid": [100.0, 100.0],
            "bid": [99.0, 99.0],
            "ask": [101.0, 101.0],
            "spread_bps": [200.0, 200.0],
            "buy_trade_qty": [0.0, 0.0],
            "sell_trade_qty": [0.0, 100.0],
            "toxicity": [0.0, 0.0],
        },
        index=index,
    )

    result = InventoryAwareMarketMaker(
        queue_ahead_units=0.0,
        max_inventory_fraction=0.0,
        maker_fee_bps=0.0,
        taker_fee_bps=0.0,
        seed=0,
    ).run(events)

    assert result.diagnostics["emergency_flattens"] == 1
    assert result.returns["price_return"].iloc[1] == pytest.approx(0.00025)
    assert result.returns["cost_return"].iloc[1] == pytest.approx(-0.00025)
    assert result.returns["net_return"].iloc[1] == pytest.approx(0.0)


@pytest.mark.parametrize(
    "index",
    [
        pd.DatetimeIndex(["2026-01-01T00:00:01Z", "2026-01-01T00:00:00Z"]),
        pd.DatetimeIndex(["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]),
    ],
)
def test_market_making_requires_strictly_increasing_events(index: pd.DatetimeIndex) -> None:
    events = pd.DataFrame(
        {
            "mid": [100.0, 100.0],
            "bid": [99.0, 99.0],
            "ask": [101.0, 101.0],
            "spread_bps": [200.0, 200.0],
            "buy_trade_qty": [0.0, 0.0],
            "sell_trade_qty": [0.0, 0.0],
            "toxicity": [0.0, 0.0],
        },
        index=index,
    )

    with pytest.raises(ValueError, match="strictly increasing"):
        InventoryAwareMarketMaker().run(events)


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), -float("inf")])
def test_market_making_rejects_non_finite_event_values(bad_value: float) -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1s", tz="UTC")
    events = pd.DataFrame(
        {
            "mid": [100.0, 100.0],
            "bid": [99.0, 99.0],
            "ask": [101.0, 101.0],
            "spread_bps": [200.0, 200.0],
            "buy_trade_qty": [0.0, 0.0],
            "sell_trade_qty": [0.0, bad_value],
            "toxicity": [0.0, 0.0],
        },
        index=index,
    )

    with pytest.raises(ValueError, match="must be finite"):
        InventoryAwareMarketMaker().run(events)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("initial_capital", 0.0),
        ("order_notional_fraction", 0.0),
        ("max_inventory_fraction", -0.01),
        ("maker_fee_bps", -0.01),
        ("taker_fee_bps", -0.01),
        ("inventory_skew_bps", -0.01),
        ("minimum_half_spread_bps", -0.01),
        ("toxicity_limit", -0.01),
        ("queue_ahead_units", -0.01),
        ("initial_capital", float("nan")),
        ("order_notional_fraction", float("inf")),
        ("taker_fee_bps", -float("inf")),
    ],
)
def test_market_making_rejects_invalid_model_parameters(field: str, value: float) -> None:
    values = {field: value}

    with pytest.raises(ValueError, match=field):
        InventoryAwareMarketMaker(**values)
