from __future__ import annotations

import hashlib
from dataclasses import dataclass

import numpy as np
import pandas as pd

from hyperlab.data.schema import instrument
from hyperlab.models import MarketPanel


@dataclass(frozen=True, slots=True)
class MicrostructureData:
    events: pd.DataFrame
    seed: int


def _ar1(rng: np.random.Generator, n: int, phi: float, sigma: float) -> np.ndarray:
    values = np.zeros(n, dtype=float)
    noise = rng.normal(0.0, sigma, n)
    for idx in range(1, n):
        values[idx] = phi * values[idx - 1] + noise[idx]
    return values


def generate_demo_panel(
    *,
    hours: int = 24 * 180,
    seed: int = 42,
    assets: tuple[str, ...] = ("BTC", "ETH", "SOL", "HYPE"),
) -> MarketPanel:
    """Generate deterministic synthetic data for installation checks only.

    The series deliberately contain mild carry, cross-sectional, momentum, and lead-lag
    structure so every research module can be exercised. They are not evidence that any
    strategy is profitable on real markets.
    """
    if hours < 600:
        raise ValueError("hours must be at least 600 for rolling strategies")

    rng = np.random.default_rng(seed)
    index = pd.date_range("2025-01-01", periods=hours, freq="1h", tz="UTC")
    prices: dict[str, np.ndarray] = {}
    funding: dict[str, np.ndarray] = {}
    spreads: dict[str, np.ndarray] = {}
    volume: dict[str, np.ndarray] = {}
    open_interest: dict[str, np.ndarray] = {}

    common_shock = rng.normal(0.0, 0.006, hours)
    regime = np.repeat(rng.choice([-1.0, 0.0, 1.0], size=(hours // 360) + 2), 360)[:hours]
    common_drift = regime * 0.00018

    starts = {"BTC": 45_000.0, "ETH": 2_400.0, "SOL": 100.0, "HYPE": 25.0}
    funding_bias = {"BTC": 0.000006, "ETH": 0.000009, "SOL": 0.000014, "HYPE": 0.000020}

    for asset_idx, asset in enumerate(assets):
        idio = rng.normal(0.0, 0.003 + asset_idx * 0.0008, hours)
        latent = (0.75 + asset_idx * 0.05) * common_shock + idio + common_drift
        reference_return = latent + rng.normal(0.0, 0.0008, hours)
        hl_return = 0.78 * latent + 0.17 * np.roll(latent, 1) + rng.normal(0.0, 0.0010, hours)
        hl_return[0] = reference_return[0]

        # Add temporary relative-value deviations for ETH and other alts.
        relative = _ar1(rng, hours, phi=0.96, sigma=0.0015 + asset_idx * 0.0002)
        if asset != "BTC":
            hl_return += -0.035 * np.roll(relative, 1)

        spot_px = starts.get(asset, 100.0) * np.exp(np.cumsum(hl_return))
        reference_px = starts.get(asset, 100.0) * np.exp(np.cumsum(reference_return))

        basis = _ar1(rng, hours, phi=0.92, sigma=0.00015)
        funding_regime = _ar1(rng, hours, phi=0.985, sigma=0.000003)
        hl_funding = (
            funding_bias.get(asset, 0.000008) + funding_regime + np.clip(basis, -0.003, 0.003) * 0.006
        )
        ref_funding = 0.45 * hl_funding + rng.normal(0.0, 0.0000035, hours)
        if asset_idx % 2:
            ref_funding -= 0.000004

        hl_perp_px = spot_px * (1.0 + basis)
        ref_perp_px = reference_px * (1.0 + 0.55 * basis + rng.normal(0.0, 0.00008, hours))

        hl_spot = instrument("HL", asset, "spot")
        hl_perp = instrument("HL", asset, "perp")
        ref_perp = instrument("REF", asset, "perp")

        prices[hl_spot] = spot_px
        prices[hl_perp] = hl_perp_px
        prices[ref_perp] = ref_perp_px
        funding[hl_spot] = np.zeros(hours)
        funding[hl_perp] = hl_funding
        funding[ref_perp] = ref_funding

        liquidity_scale = 1.0 + asset_idx * 0.45
        spreads[hl_spot] = np.clip(rng.lognormal(np.log(2.0 * liquidity_scale), 0.25, hours), 0.5, 30.0)
        spreads[hl_perp] = np.clip(rng.lognormal(np.log(1.1 * liquidity_scale), 0.20, hours), 0.25, 20.0)
        spreads[ref_perp] = np.clip(rng.lognormal(np.log(0.9 * liquidity_scale), 0.20, hours), 0.20, 18.0)

        base_volume = 2_000_000_000 / (1 + asset_idx * 1.5)
        volume[hl_spot] = rng.lognormal(np.log(base_volume * 0.08), 0.35, hours)
        volume[hl_perp] = rng.lognormal(np.log(base_volume), 0.30, hours)
        volume[ref_perp] = rng.lognormal(np.log(base_volume * 1.8), 0.25, hours)
        open_interest[hl_spot] = np.full(hours, np.nan)
        open_interest[hl_perp] = rng.lognormal(np.log(base_volume * 0.7), 0.18, hours)
        open_interest[ref_perp] = rng.lognormal(np.log(base_volume), 0.18, hours)

    price_frame = pd.DataFrame(prices, index=index)
    funding_frame = pd.DataFrame(funding, index=index)[price_frame.columns]
    spread_frame = pd.DataFrame(spreads, index=index)[price_frame.columns]
    volume_frame = pd.DataFrame(volume, index=index)[price_frame.columns]
    depth_frame = (volume_frame * 0.002).clip(lower=25_000.0)
    open_interest_frame = pd.DataFrame(open_interest, index=index)[price_frame.columns]
    available_at = pd.DataFrame(
        {column: index for column in price_frame.columns},
        index=index,
    )
    synthetic_lifecycle_hash = hashlib.sha256(
        ("synthetic-lifecycle:" + ",".join(str(column) for column in price_frame.columns)).encode("utf-8")
    ).hexdigest()
    return MarketPanel(
        prices=price_frame,
        funding=funding_frame,
        spreads_bps=spread_frame,
        volume_usd=volume_frame,
        depth_usd=depth_frame,
        open_interest_usd=open_interest_frame,
        available_at=available_at,
        finality=pd.DataFrame(True, index=index, columns=price_frame.columns),
        tradable=pd.DataFrame(True, index=index, columns=price_frame.columns),
        metadata={
            "source": "synthetic-demo-only",
            "seed": seed,
            "point_in_time": True,
            "historical_universe_source": "synthetic-lifecycle-fixture",
            "lifecycle_hash": synthetic_lifecycle_hash,
            "calibration_status": "SYNTHETIC",
            "warning": "Never use synthetic results as an investment decision.",
        },
    )


def generate_microstructure_demo(*, events: int = 25_000, seed: int = 7) -> MicrostructureData:
    if events < 1_000:
        raise ValueError("events must be at least 1000")
    rng = np.random.default_rng(seed)
    index = pd.date_range("2025-01-01", periods=events, freq="250ms", tz="UTC")
    imbalance = _ar1(rng, events, phi=0.82, sigma=0.22)
    imbalance = np.tanh(imbalance)
    informed_move = 0.000018 * imbalance + rng.normal(0.0, 0.000025, events)
    mid = 50_000.0 * np.exp(np.cumsum(informed_move))
    spread_bps = np.clip(rng.lognormal(np.log(0.9), 0.25, events), 0.25, 4.0)
    half = mid * spread_bps / 20_000.0
    bid = mid - half
    ask = mid + half
    buy_qty = rng.gamma(1.2, 0.6, events) * np.clip(1.0 + imbalance, 0.05, None)
    sell_qty = rng.gamma(1.2, 0.6, events) * np.clip(1.0 - imbalance, 0.05, None)
    toxicity = np.abs(imbalance) * (1.0 + np.abs(informed_move) * 5_000)
    frame = pd.DataFrame(
        {
            "mid": mid,
            "bid": bid,
            "ask": ask,
            "spread_bps": spread_bps,
            "imbalance": imbalance,
            "buy_trade_qty": buy_qty,
            "sell_trade_qty": sell_qty,
            "toxicity": toxicity,
        },
        index=index,
    )
    return MicrostructureData(events=frame, seed=seed)
