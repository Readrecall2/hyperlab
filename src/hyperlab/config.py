from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hyperlab.models import CostModel, RiskLimits


@dataclass(frozen=True, slots=True)
class AppSettings:
    network: str
    data_dir: Path
    request_timeout_seconds: float
    mode: str


@dataclass(frozen=True, slots=True)
class Settings:
    app: AppSettings
    costs: CostModel
    risk_profiles: dict[str, RiskLimits]


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"TOML section [{name}] must be a table")
    return value


def load_settings(path: Path = Path("config/research.toml")) -> Settings:
    with path.open("rb") as handle:
        data = tomllib.load(handle)

    app_data = _section(data, "app")
    cost_data = _section(data, "costs")
    profiles_data = _section(data, "risk_profiles")
    data_dir = Path(os.getenv("HYPERLAB_DATA_DIR", str(app_data.get("data_dir", "data"))))
    mode = os.getenv("HYPERLAB_MODE", str(app_data.get("mode", "readonly"))).lower()
    if mode not in {"readonly", "research", "paper", "testnet", "mainnet"}:
        raise ValueError(f"unsupported HYPERLAB_MODE: {mode}")

    settings = AppSettings(
        network=str(app_data.get("network", "mainnet")),
        data_dir=data_dir,
        request_timeout_seconds=float(app_data.get("request_timeout_seconds", 15.0)),
        mode=mode,
    )
    costs = CostModel(
        spot_fee_bps=float(cost_data.get("spot_fee_bps", 4.0)),
        perp_fee_bps=float(cost_data.get("perp_fee_bps", 1.5)),
        external_perp_fee_bps=float(cost_data.get("external_perp_fee_bps", 2.0)),
        base_slippage_bps=float(cost_data.get("base_slippage_bps", 1.0)),
        stress_multiplier=float(cost_data.get("stress_multiplier", 1.0)),
    )

    profiles: dict[str, RiskLimits] = {}
    for name, raw in profiles_data.items():
        if not isinstance(raw, dict):
            continue
        profiles[name] = RiskLimits(
            max_gross_leverage=float(raw.get("max_gross_leverage", 1.0)),
            max_net_exposure=float(raw.get("max_net_exposure", 1.0)),
            max_instrument_weight=float(raw.get("max_instrument_weight", 0.5)),
        )
    if not profiles:
        profiles["balanced"] = RiskLimits()

    return Settings(app=settings, costs=costs, risk_profiles=profiles)
