from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from hyperlab.backtest.metrics import compute_metrics
from hyperlab.models import BacktestResult
from hyperlab.strategies.helpers import scalar_float


@dataclass(slots=True)
class InventoryAwareMarketMaker:
    """Simplified queue/fill simulator for microstructure research.

    It is intentionally not a production validator. Real validation requires full L2 event
    replay, queue position estimation, synchronized timestamps, rejects, cancels, and latency.
    """

    name: str = "inventory_market_making"
    risk_tier: str = "4 — agressif"
    initial_capital: float = 20_000.0
    order_notional_fraction: float = 0.025
    max_inventory_fraction: float = 0.20
    maker_fee_bps: float = 1.5
    taker_fee_bps: float = 4.5
    inventory_skew_bps: float = 2.0
    minimum_half_spread_bps: float = 0.55
    toxicity_limit: float = 1.35
    queue_ahead_units: float = 1.5
    seed: int = 123
    simulation_label: str = "TOY"

    def __post_init__(self) -> None:
        for name, value in (
            ("initial_capital", self.initial_capital),
            ("order_notional_fraction", self.order_notional_fraction),
        ):
            if not np.isfinite(value) or value <= 0.0:
                raise ValueError(f"{name} must be finite and positive")
        for name, value in (
            ("max_inventory_fraction", self.max_inventory_fraction),
            ("maker_fee_bps", self.maker_fee_bps),
            ("taker_fee_bps", self.taker_fee_bps),
            ("inventory_skew_bps", self.inventory_skew_bps),
            ("minimum_half_spread_bps", self.minimum_half_spread_bps),
            ("toxicity_limit", self.toxicity_limit),
            ("queue_ahead_units", self.queue_ahead_units),
        ):
            if not np.isfinite(value) or value < 0.0:
                raise ValueError(f"{name} must be finite and non-negative")

    def run(self, events: pd.DataFrame) -> BacktestResult:
        required = {
            "mid",
            "bid",
            "ask",
            "spread_bps",
            "buy_trade_qty",
            "sell_trade_qty",
            "toxicity",
        }
        missing = required.difference(events.columns)
        if missing:
            raise ValueError(f"missing market-making columns: {sorted(missing)}")
        if events.empty:
            raise ValueError("market-making events cannot be empty")
        if not isinstance(events.index, pd.DatetimeIndex):
            raise TypeError("market-making index must be a DatetimeIndex")
        if not events.index.is_monotonic_increasing or events.index.has_duplicates:
            raise ValueError("market-making index must be strictly increasing")
        try:
            numeric_events = events.loc[:, sorted(required)].astype(float)
        except (TypeError, ValueError) as error:
            raise ValueError("market-making event values must be numeric") from error
        if not bool(np.isfinite(numeric_events.to_numpy()).all()):
            raise ValueError("market-making event values must be finite")
        if bool((numeric_events[["mid", "bid", "ask"]] <= 0.0).any(axis=None)):
            raise ValueError("market-making prices must be positive")
        if bool(
            (
                numeric_events[["spread_bps", "buy_trade_qty", "sell_trade_qty", "toxicity"]]
                < 0.0
            ).any(axis=None)
        ):
            raise ValueError("market-making spreads, flows, and toxicity must be non-negative")
        if bool(
            (
                (numeric_events["bid"] > numeric_events["mid"])
                | (numeric_events["mid"] > numeric_events["ask"])
            ).any()
        ):
            raise ValueError("market-making quotes must satisfy bid <= mid <= ask")

        rng = np.random.default_rng(self.seed)
        cash = self.initial_capital
        inventory = 0.0
        previous_equity = self.initial_capital
        rows: list[dict[str, float]] = []
        inventory_weights: list[float] = []
        maker_fills = 0
        partial_fills = 0
        filled_units = 0.0
        gross_traded_notional = 0.0
        emergency_flattens = 0
        fees_paid = 0.0
        pending_bid: float | None = None
        pending_ask: float | None = None
        pending_order_units = 0.0

        for _, row in numeric_events.iterrows():
            mid = scalar_float(row["mid"])
            event_fees = 0.0
            event_spread_cost = 0.0

            if pending_bid is not None and pending_ask is not None:
                sell_flow = scalar_float(row["sell_trade_qty"])
                buy_flow = scalar_float(row["buy_trade_qty"])
                bid_probability = (
                    min(0.90, sell_flow / (self.queue_ahead_units + sell_flow))
                    if sell_flow > 0.0 and scalar_float(row["bid"]) <= pending_bid
                    else 0.0
                )
                ask_probability = (
                    min(0.90, buy_flow / (self.queue_ahead_units + buy_flow))
                    if buy_flow > 0.0 and scalar_float(row["ask"]) >= pending_ask
                    else 0.0
                )

                if rng.random() < bid_probability:
                    bid_fill_units = min(pending_order_units, sell_flow)
                    inventory += bid_fill_units
                    fill_notional = bid_fill_units * pending_bid
                    cash -= fill_notional
                    fee = fill_notional * self.maker_fee_bps / 10_000.0
                    cash -= fee
                    event_fees += fee
                    maker_fills += 1
                    partial_fills += int(bid_fill_units < pending_order_units)
                    filled_units += bid_fill_units
                    gross_traded_notional += fill_notional
                if rng.random() < ask_probability:
                    ask_fill_units = min(pending_order_units, buy_flow)
                    inventory -= ask_fill_units
                    fill_notional = ask_fill_units * pending_ask
                    cash += fill_notional
                    fee = fill_notional * self.maker_fee_bps / 10_000.0
                    cash -= fee
                    event_fees += fee
                    maker_fills += 1
                    partial_fills += int(ask_fill_units < pending_order_units)
                    filled_units += ask_fill_units
                    gross_traded_notional += fill_notional

            equity = cash + inventory * mid
            inventory_fraction = (inventory * mid / equity) if equity else 0.0
            if abs(inventory_fraction) > self.max_inventory_fraction and inventory != 0.0:
                exit_price = scalar_float(row["bid"]) if inventory > 0 else scalar_float(row["ask"])
                notional = abs(inventory) * exit_price
                event_spread_cost += abs(inventory) * abs(mid - exit_price)
                fee = notional * self.taker_fee_bps / 10_000.0
                cash += inventory * exit_price
                cash -= fee
                event_fees += fee
                inventory = 0.0
                emergency_flattens += 1
                gross_traded_notional += notional
                equity = cash

            pending_bid = None
            pending_ask = None
            pending_order_units = 0.0
            if scalar_float(row["toxicity"]) <= self.toxicity_limit and equity > 0:
                inventory_fraction = inventory * mid / equity
                skew_price = mid * self.inventory_skew_bps / 10_000.0 * inventory_fraction
                reservation = mid - skew_price
                half_spread_bps = max(
                    self.minimum_half_spread_bps,
                    scalar_float(row["spread_bps"]) / 2.0,
                )
                half_spread = mid * half_spread_bps / 10_000.0
                next_bid = min(reservation - half_spread, scalar_float(row["bid"]))
                next_ask = max(reservation + half_spread, scalar_float(row["ask"]))
                if next_bid <= 0.0 or next_bid >= next_ask:
                    raise ValueError("market-making quote model produced invalid prices")
                pending_bid = next_bid
                pending_ask = next_ask
                pending_order_units = self.initial_capital * self.order_notional_fraction / mid

            if previous_equity == 0.0:
                raise ValueError("cannot compute a return from zero equity")
            fees_paid += event_fees
            net_return = equity / previous_equity - 1.0
            cost_return = -(event_fees + event_spread_cost) / previous_equity
            price_return = net_return - cost_return
            rows.append(
                {
                    "price_return": price_return,
                    "funding_return": 0.0,
                    "cost_return": cost_return,
                    "net_return": net_return,
                }
            )
            inventory_weights.append((inventory * mid / equity) if equity else 0.0)
            previous_equity = equity

        components = pd.DataFrame(rows, index=events.index)
        if inventory != 0.0:
            final_mid = scalar_float(numeric_events["mid"].iloc[-1])
            equity_before_close = cash + inventory * final_mid
            if equity_before_close == 0.0:
                raise ValueError("cannot compute a closing return from zero equity")
            exit_price = (
                scalar_float(numeric_events["bid"].iloc[-1])
                if inventory > 0.0
                else scalar_float(numeric_events["ask"].iloc[-1])
            )
            notional = abs(inventory) * exit_price
            fee = notional * self.taker_fee_bps / 10_000.0
            cash += inventory * exit_price
            cash -= fee
            fees_paid += fee
            gross_traded_notional += notional
            inventory = 0.0
            close_return = cash / equity_before_close - 1.0
            last = components.index[-1]
            existing = scalar_float(components.at[last, "net_return"])
            components.at[last, "net_return"] = (1.0 + existing) * (1.0 + close_return) - 1.0
            existing_cost = scalar_float(components.at[last, "cost_return"])
            components.at[last, "cost_return"] = existing_cost + (1.0 + existing) * close_return
            inventory_weights[-1] = 0.0

        equity_curve = (1.0 + components["net_return"]).cumprod()
        weights = pd.DataFrame({"HL:BTC:perp": inventory_weights}, index=events.index)
        metrics = compute_metrics(components, equity_curve, weights)
        metrics.turnover = gross_traded_notional / self.initial_capital
        return BacktestResult(
            strategy_name=self.name,
            risk_tier=self.risk_tier,
            returns=components,
            equity=equity_curve,
            weights=weights,
            metrics=metrics,
            diagnostics={
                "simulation_label": self.simulation_label,
                "warning": "TOY synthetic queue model only; not sufficient for live deployment",
                "fill_timing": "event t flow applies only to quotes created at t-1",
                "maker_fills": maker_fills,
                "partial_fills": partial_fills,
                "filled_units": filled_units,
                "gross_traded_notional": gross_traded_notional,
                "emergency_flattens": emergency_flattens,
                "fees_paid": fees_paid,
                "ending_cash": cash,
            },
        )
