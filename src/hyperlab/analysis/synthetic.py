from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np
import pandas as pd

from hyperlab.analysis.lead_lag import LeadLagDataset, StrictInterval

SYNTHETIC_WARNING = "SYNTHETIC_DETECTOR_VALIDATION_ONLY"


@dataclass(frozen=True, slots=True)
class SyntheticLeadLagFixture:
    dataset: LeadLagDataset
    intervals: tuple[StrictInterval, ...]
    signal_times: tuple[datetime, ...]
    injected_lag_ms: int
    null: bool
    warning: str = SYNTHETIC_WARNING


def _balanced_signs(rng: np.random.Generator, count: int) -> np.ndarray:
    signs = np.ones(count, dtype=np.int8)
    signs[: count // 2] = -1
    rng.shuffle(signs)
    return signs


def _low_correlation_signs(
    rng: np.random.Generator, reference: np.ndarray
) -> np.ndarray:
    for _attempt in range(1_000):
        candidate = _balanced_signs(rng, len(reference))
        if abs(float(np.dot(reference, candidate))) <= max(2.0, len(reference) * 0.05):
            return candidate
    raise RuntimeError("could not construct deterministic synthetic null signs")


def _mid_price(base_price: float, cumulative_bps: float) -> float:
    return base_price * math.exp(cumulative_bps / 10_000.0)


def _bbo_row(
    *,
    venue: str,
    asset: str,
    timestamp: pd.Timestamp,
    mid: float,
    direction: int,
) -> dict[str, object]:
    half_spread = 0.5 / 10_000.0
    bid_quantity = 20.0 + 4.0 * direction
    ask_quantity = 20.0 - 4.0 * direction
    return {
        "venue": venue,
        "asset": asset,
        "received_time": timestamp,
        "bid_price": mid * (1.0 - half_spread),
        "ask_price": mid * (1.0 + half_spread),
        "bid_quantity": bid_quantity,
        "ask_quantity": ask_quantity,
    }


def _atomic_book(
    *,
    venue: str,
    asset: str,
    timestamp: pd.Timestamp,
    mid: float,
    direction: int,
    snapshot_id: str,
) -> dict[str, object]:
    half_spread = 0.5 / 10_000.0
    bid = mid * (1.0 - half_spread)
    ask = mid * (1.0 + half_spread)
    bid_quantity = 20.0 + 4.0 * direction
    ask_quantity = 20.0 - 4.0 * direction
    return {
        "venue": venue,
        "asset": asset,
        "received_time": timestamp,
        "snapshot_id": snapshot_id,
        "bids": (
            (0, bid, bid_quantity, 1),
            (1, bid * (1.0 - 0.0001), 30.0, 1),
            (2, bid * (1.0 - 0.0002), 40.0, 1),
        ),
        "asks": (
            (0, ask, ask_quantity, 1),
            (1, ask * (1.0 + 0.0001), 30.0, 1),
            (2, ask * (1.0 + 0.0002), 40.0, 1),
        ),
    }


def generate_synthetic_lead_lag_dataset(
    *,
    event_count: int = 96,
    injected_lag_ms: int = 250,
    seed: int = 20_260_815,
    signal_spacing_ms: int = 7_000,
    move_bps: float = 5.0,
    assets: tuple[str, ...] = ("BTC", "ETH"),
    start: datetime = datetime(2025, 1, 1, tzinfo=UTC),
    null: bool = False,
) -> SyntheticLeadLagFixture:
    """Build a deterministic detector-validation fixture, never economic evidence.

    Binance innovations occur at known receive times.  Hyperliquid innovations
    either copy each sign after ``injected_lag_ms`` or use a balanced,
    low-correlation sign sequence for the null control.  No parameter is inferred
    from analysis output.
    """

    if event_count < 32:
        raise ValueError("event_count must be at least 32")
    if not 0 <= injected_lag_ms <= 5_000:
        raise ValueError("injected_lag_ms must be within the observable 0ms to 5s horizon")
    if signal_spacing_ms <= 6_000:
        raise ValueError("signal_spacing_ms must exceed the 5s study horizon and exits")
    if not math.isfinite(move_bps) or move_bps <= 0.0:
        raise ValueError("move_bps must be finite and positive")
    normalized_assets = tuple(asset.strip().upper() for asset in assets)
    if not normalized_assets or any(not asset for asset in normalized_assets):
        raise ValueError("synthetic assets must be non-empty")
    if len(set(normalized_assets)) != len(normalized_assets):
        raise ValueError("synthetic assets must be unique")
    if not {"BTC", "ETH"}.issubset(normalized_assets):
        raise ValueError("synthetic Phase 10-2 fixture must include BTC and ETH")
    start_timestamp = pd.Timestamp(start)
    if start_timestamp.tz is None:
        raise ValueError("start must be timezone-aware")
    start_timestamp = start_timestamp.tz_convert("UTC")

    rng = np.random.default_rng(seed)
    signal_times = tuple(
        start_timestamp + pd.Timedelta(milliseconds=6_000 + index * signal_spacing_ms)
        for index in range(event_count)
    )
    offsets_ms = {
        -1_000,
        -50,
        0,
        50,
        100,
        250,
        350,
        500,
        600,
        750,
        1_000,
        1_100,
        1_250,
        2_000,
        2_100,
        2_250,
        5_000,
        5_100,
        5_250,
        injected_lag_ms,
    }
    observation_times = tuple(
        sorted(
            {
                signal_time + pd.Timedelta(milliseconds=offset)
                for signal_time in signal_times
                for offset in offsets_ms
            }
            | {start_timestamp}
        )
    )
    base_prices = {"BTC": 60_000.0, "ETH": 3_000.0}
    reference_signs: dict[str, np.ndarray] = {}
    execution_signs: dict[str, np.ndarray] = {}
    for asset in normalized_assets:
        reference = _balanced_signs(rng, event_count)
        reference_signs[asset] = reference
        execution_signs[asset] = (
            _low_correlation_signs(rng, reference) if null else reference.copy()
        )

    bbo_rows: list[dict[str, object]] = []
    l2_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    signal_ns = np.asarray([int(value.value) for value in signal_times], dtype=np.int64)
    for asset in normalized_assets:
        reference = reference_signs[asset]
        execution = execution_signs[asset]
        delayed_ns = signal_ns + injected_lag_ms * 1_000_000
        for ordinal, timestamp in enumerate(observation_times):
            timestamp_ns = int(timestamp.value)
            reference_count = int(np.searchsorted(signal_ns, timestamp_ns, side="right"))
            execution_count = int(np.searchsorted(delayed_ns, timestamp_ns, side="right"))
            reference_direction = int(reference[reference_count - 1]) if reference_count else 0
            execution_direction = int(execution[execution_count - 1]) if execution_count else 0
            reference_mid = _mid_price(
                base_prices[asset], float(reference[:reference_count].sum()) * move_bps
            )
            execution_mid = _mid_price(
                base_prices[asset], float(execution[:execution_count].sum()) * move_bps
            )
            bbo_rows.append(
                _bbo_row(
                    venue="binance_usdm",
                    asset=asset,
                    timestamp=timestamp,
                    mid=reference_mid,
                    direction=reference_direction,
                )
            )
            bbo_rows.append(
                _bbo_row(
                    venue="hyperliquid",
                    asset=asset,
                    timestamp=timestamp,
                    mid=execution_mid,
                    direction=execution_direction,
                )
            )
            l2_rows.append(
                _atomic_book(
                    venue="hyperliquid",
                    asset=asset,
                    timestamp=timestamp,
                    mid=execution_mid,
                    direction=execution_direction,
                    snapshot_id=f"hl-{asset}-{ordinal:08d}",
                )
            )
        for index, signal_time in enumerate(signal_times):
            sign = int(reference[index])
            reference_count = index + 1
            reference_mid = _mid_price(
                base_prices[asset], float(reference[:reference_count].sum()) * move_bps
            )
            trade_rows.append(
                {
                    "venue": "binance_usdm",
                    "asset": asset,
                    "received_time": signal_time,
                    "price": reference_mid,
                    "quantity": 1.0,
                    "quote_quantity": reference_mid,
                    "aggressor_side": "buy" if sign > 0 else "sell",
                }
            )
            l2_rows.append(
                _atomic_book(
                    venue="binance_usdm",
                    asset=asset,
                    timestamp=signal_time,
                    mid=reference_mid,
                    direction=sign,
                    snapshot_id=f"bn-{asset}-{index:08d}",
                )
            )
            execution_sign = int(execution[index])
            execution_time = signal_time + pd.Timedelta(milliseconds=injected_lag_ms)
            execution_mid = _mid_price(
                base_prices[asset], float(execution[: index + 1].sum()) * move_bps
            )
            trade_rows.append(
                {
                    "venue": "hyperliquid",
                    "asset": asset,
                    "received_time": execution_time,
                    "price": execution_mid,
                    "quantity": 1.0,
                    "quote_quantity": execution_mid,
                    "aggressor_side": "buy" if execution_sign > 0 else "sell",
                }
            )

    end_timestamp = signal_times[-1] + pd.Timedelta(milliseconds=6_000)
    interval = StrictInterval(
        start=start_timestamp.to_pydatetime(),
        end=end_timestamp.to_pydatetime(),
        tag="synthetic-detector-validation",
    )
    fingerprint_payload = {
        "warning": SYNTHETIC_WARNING,
        "event_count": event_count,
        "injected_lag_ms": injected_lag_ms,
        "seed": seed,
        "signal_spacing_ms": signal_spacing_ms,
        "move_bps": move_bps,
        "assets": list(normalized_assets),
        "start": start_timestamp.isoformat(),
        "null": null,
    }
    fingerprint = hashlib.sha256(
        json.dumps(
            fingerprint_payload, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    clock_times = pd.date_range(
        start=start_timestamp,
        end=end_timestamp,
        freq="60s",
        inclusive="left",
    )
    dataset = LeadLagDataset(
        bbo=pd.DataFrame(bbo_rows),
        trades=pd.DataFrame(trade_rows),
        l2=pd.DataFrame(l2_rows),
        clock_sync=pd.DataFrame(
            {
                "received_time": clock_times,
                "venue": "binance_usdm",
                "valid": True,
                "uncertainty_ms": 0.1,
            }
        ),
        provenance={
            "kind": "SYNTHETIC",
            "warning": SYNTHETIC_WARNING,
            "purpose": "DETECTOR_REGRESSION_ONLY_NOT_ECONOMIC_EVIDENCE",
            "parameters": fingerprint_payload,
        },
        source_fingerprint=fingerprint,
    )
    return SyntheticLeadLagFixture(
        dataset=dataset,
        intervals=(interval,),
        signal_times=tuple(value.to_pydatetime() for value in signal_times),
        injected_lag_ms=injected_lag_ms,
        null=null,
    )
