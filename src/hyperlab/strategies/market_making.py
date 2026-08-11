from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from hyperlab.backtest.metrics import compute_metrics
from hyperlab.models import BacktestResult


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

        rng = np.random.default_rng(self.seed)
        cash = self.initial_capital
        inventory = 0.0
        previous_mid = float(events["mid"].iloc[0])
        previous_equity = self.initial_capital
        rows: list[dict[str, float]] = []
        inventory_weights: list[float] = []
        maker_fills = 0
        emergency_flattens = 0
        fees_paid = 0.0

        for timestamp, row in events.iterrows():
            mid = float(row["mid"])
            equity_before_quotes = cash + inventory * mid
            inventory_fraction = (inventory * mid / equity_before_quotes) if equity_before_quotes else 0.0
            mark_pnl = inventory * (mid - previous_mid)
            event_fees = 0.0

            if float(row["toxicity"]) <= self.toxicity_limit and equity_before_quotes > 0:
                skew_price = mid * self.inventory_skew_bps / 10_000.0 * inventory_fraction
                reservation = mid - skew_price
                half_spread_bps = max(
                    self.minimum_half_spread_bps,
                    float(row["spread_bps"]) / 2.0,
                )
                half_spread = mid * half_spread_bps / 10_000.0
                quote_bid = min(reservation - half_spread, float(row["bid"]))
                quote_ask = max(reservation + half_spread, float(row["ask"]))
                order_units = self.initial_capital * self.order_notional_fraction / mid

                sell_flow = float(row["sell_trade_qty"])
                buy_flow = float(row["buy_trade_qty"])
                bid_probability = min(0.90, sell_flow / (self.queue_ahead_units + sell_flow))
                ask_probability = min(0.90, buy_flow / (self.queue_ahead_units + buy_flow))

                if rng.random() < bid_probability:
                    inventory += order_units
                    cash -= order_units * quote_bid
                    fee = order_units * quote_bid * self.maker_fee_bps / 10_000.0
                    cash -= fee
                    event_fees += fee
                    maker_fills += 1
                if rng.random() < ask_probability:
                    sell_units = min(order_units, max(order_units, abs(inventory)))
                    inventory -= sell_units
                    cash += sell_units * quote_ask
                    fee = sell_units * quote_ask * self.maker_fee_bps / 10_000.0
                    cash -= fee
                    event_fees += fee
                    maker_fills += 1

            equity = cash + inventory * mid
            inventory_fraction = (inventory * mid / equity) if equity else 0.0
            if abs(inventory_fraction) > self.max_inventory_fraction and inventory != 0.0:
                exit_price = float(row["bid"]) if inventory > 0 else float(row["ask"])
                notional = abs(inventory) * exit_price
                fee = notional * self.taker_fee_bps / 10_000.0
                cash += inventory * exit_price
                cash -= fee
                event_fees += fee
                inventory = 0.0
                emergency_flattens += 1
                equity = cash

            fees_paid += event_fees
            net_return = equity / previous_equity - 1.0 if previous_equity > 0 else -0.99
            price_return = mark_pnl / previous_equity if previous_equity > 0 else 0.0
            cost_return = -event_fees / previous_equity if previous_equity > 0 else 0.0
            rows.append(
                {
                    "price_return": price_return,
                    "funding_return": 0.0,
                    "cost_return": cost_return,
                    "net_return": max(net_return, -0.99),
                }
            )
            inventory_weights.append((inventory * mid / equity) if equity else 0.0)
            previous_equity = equity
            previous_mid = mid

        components = pd.DataFrame(rows, index=events.index)
        if inventory != 0.0:
            final_mid = float(events["mid"].iloc[-1])
            equity_before_close = cash + inventory * final_mid
            notional = abs(inventory) * final_mid
            fee = notional * self.taker_fee_bps / 10_000.0
            cash += inventory * final_mid
            cash -= fee
            fees_paid += fee
            inventory = 0.0
            close_return = cash / equity_before_close - 1.0 if equity_before_close > 0 else -0.99
            last = components.index[-1]
            existing = float(components.at[last, "net_return"])
            components.at[last, "net_return"] = (1.0 + existing) * (1.0 + close_return) - 1.0
            components.at[last, "cost_return"] += close_return
            inventory_weights[-1] = 0.0

        equity_curve = (1.0 + components["net_return"]).cumprod()
        weights = pd.DataFrame({"HL:BTC:perp": inventory_weights}, index=events.index)
        metrics = compute_metrics(components, equity_curve, weights)
        return BacktestResult(
            strategy_name=self.name,
            risk_tier=self.risk_tier,
            returns=components,
            equity=equity_curve,
            weights=weights,
            metrics=metrics,
            diagnostics={
                "warning": "synthetic queue model only; not sufficient for live deployment",
                "maker_fills": maker_fills,
                "emergency_flattens": emergency_flattens,
                "fees_paid": fees_paid,
                "ending_cash": cash,
            },
        )
