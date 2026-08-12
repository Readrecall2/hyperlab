from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hyperlab.backtest.benchmark import PassiveBenchmarkSpec
from hyperlab.backtest.costs import CostRule, CostSchedule, SlippageModel
from hyperlab.backtest.execution import ExecutionConfig, MakerFillModel
from hyperlab.models import CostModel, RiskLimits


@dataclass(frozen=True, slots=True)
class AppSettings:
    network: str
    data_dir: Path
    request_timeout_seconds: float
    mode: str


@dataclass(frozen=True, slots=True)
class ResearchSettings:
    train_fraction: float
    validation_fraction: float
    walk_forward_train_bars: int
    walk_forward_validation_bars: int
    walk_forward_step_bars: int
    embargo_bars: int
    expanding: bool
    registry_path: Path
    bootstrap_block_size: int
    bootstrap_resamples: int
    bootstrap_confidence_level: float
    bootstrap_seed: int
    benchmark: PassiveBenchmarkSpec


@dataclass(frozen=True, slots=True)
class Settings:
    app: AppSettings
    costs: CostModel
    cost_schedule: CostSchedule
    execution: ExecutionConfig
    research: ResearchSettings
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
    execution_data = _section(data, "execution")
    protocol_data = _section(data, "research_protocol")
    bootstrap_data = _section(data, "bootstrap")
    benchmark_data = _section(data, "benchmark")
    profiles_data = _section(data, "risk_profiles")
    data_dir = Path(os.getenv("HYPERLAB_DATA_DIR", str(app_data.get("data_dir", "data"))))
    mode = os.getenv("HYPERLAB_MODE", str(app_data.get("mode", "readonly"))).strip().lower()
    if mode not in {"readonly", "research"}:
        raise ValueError(f"unsupported HYPERLAB_MODE: {mode}; HyperLab 0.2.x only allows readonly/research")

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
    raw_instruments = cost_data.get("instruments", {})
    if not isinstance(raw_instruments, dict):
        raise ValueError("TOML [costs.instruments] must be a table")
    if raw_instruments:
        rules: list[CostRule] = []
        for instrument, raw_rule in raw_instruments.items():
            if not isinstance(raw_rule, dict):
                raise ValueError(f"cost rule for {instrument} must be a table")
            fallback_fee = costs.spot_fee_bps if str(instrument).endswith(":spot") else costs.perp_fee_bps
            rules.append(
                CostRule(
                    instrument=str(instrument),
                    maker_fee_bps=float(raw_rule.get("maker_fee_bps", fallback_fee)),
                    taker_fee_bps=float(raw_rule.get("taker_fee_bps", fallback_fee)),
                    slippage=SlippageModel(
                        base_bps=float(raw_rule.get("base_slippage_bps", costs.base_slippage_bps)),
                        impact_coefficient_bps=float(raw_rule.get("impact_coefficient_bps", 0.0)),
                        exponent=float(raw_rule.get("impact_exponent", 0.5)),
                        max_participation=float(raw_rule.get("max_participation", 0.10)),
                    ),
                    effective_from=raw_rule.get("effective_from"),
                    effective_to=raw_rule.get("effective_to"),
                    source=str(raw_rule.get("source", "research-placeholder")),
                )
            )
    else:
        rules = [
            CostRule(
                "HL:*:spot",
                costs.spot_fee_bps,
                costs.spot_fee_bps,
                SlippageModel(base_bps=costs.base_slippage_bps, max_participation=1.0),
            ),
            CostRule(
                "HL:*:perp",
                costs.perp_fee_bps,
                costs.perp_fee_bps,
                SlippageModel(base_bps=costs.base_slippage_bps, max_participation=1.0),
            ),
            CostRule(
                "*:*:perp",
                costs.external_perp_fee_bps,
                costs.external_perp_fee_bps,
                SlippageModel(base_bps=costs.base_slippage_bps, max_participation=1.0),
            ),
        ]
    cost_schedule = CostSchedule(
        rules=tuple(rules),
        calibration_status=str(cost_data.get("calibration_status", "UNCALIBRATED")),
        calibration_evidence_hash=(
            str(cost_data["calibration_evidence_hash"])
            if "calibration_evidence_hash" in cost_data
            else None
        ),
    )
    maker_data = execution_data.get("maker_fill", {})
    if not isinstance(maker_data, dict):
        raise ValueError("TOML [execution.maker_fill] must be a table")
    execution = ExecutionConfig(
        initial_capital=float(execution_data.get("initial_capital", 100_000.0)),
        default_order_type=str(execution_data.get("default_order_type", "taker")),  # type: ignore[arg-type]
        maker_fill=MakerFillModel(
            base_probability=float(maker_data.get("base_probability", 0.65)),
            participation_decay=float(maker_data.get("participation_decay", 2.0)),
            calibration_id=str(maker_data.get("calibration_id", "uncalibrated-default")),
            calibration_status=str(maker_data.get("calibration_status", "UNCALIBRATED")),
            calibration_evidence_hash=(
                str(maker_data["calibration_evidence_hash"])
                if "calibration_evidence_hash" in maker_data
                else None
            ),
        ),
        maker_fill_multiplier=float(execution_data.get("maker_fill_multiplier", 1.0)),
        maker_timeout_bars=int(execution_data.get("maker_timeout_bars", 1)),
        emergency_ioc=bool(execution_data.get("emergency_ioc", True)),
        ioc_fill_probability=float(execution_data.get("ioc_fill_probability", 1.0)),
        ioc_extra_slippage_bps=float(execution_data.get("ioc_extra_slippage_bps", 2.0)),
        base_latency_bars=int(execution_data.get("base_latency_bars", 0)),
        leg_delay_bars=int(execution_data.get("leg_delay_bars", 0)),
        cost_multiplier=float(execution_data.get("cost_multiplier", 1.0)),
        seed=int(execution_data.get("seed", 0)),
        require_depth=bool(execution_data.get("require_depth", False)),
        require_point_in_time=bool(execution_data.get("require_point_in_time", False)),
    )
    train_fraction = float(protocol_data.get("train_fraction", 0.60))
    validation_fraction = float(protocol_data.get("validation_fraction", 0.20))
    if not 0.0 < train_fraction < 1.0:
        raise ValueError("research train_fraction must be in (0, 1)")
    if not 0.0 < validation_fraction < 1.0:
        raise ValueError("research validation_fraction must be in (0, 1)")
    if train_fraction + validation_fraction >= 1.0:
        raise ValueError("research split must leave a non-empty final test fraction")
    research = ResearchSettings(
        train_fraction=train_fraction,
        validation_fraction=validation_fraction,
        walk_forward_train_bars=int(protocol_data.get("walk_forward_train_bars", 720)),
        walk_forward_validation_bars=int(protocol_data.get("walk_forward_validation_bars", 168)),
        walk_forward_step_bars=int(protocol_data.get("walk_forward_step_bars", 168)),
        embargo_bars=int(protocol_data.get("embargo_bars", 1)),
        expanding=bool(protocol_data.get("expanding", True)),
        registry_path=Path(str(protocol_data.get("registry_path", "reports/research/variants.jsonl"))),
        bootstrap_block_size=int(bootstrap_data.get("block_size", 24)),
        bootstrap_resamples=int(bootstrap_data.get("resamples", 2_000)),
        bootstrap_confidence_level=float(bootstrap_data.get("confidence_level", 0.95)),
        bootstrap_seed=int(bootstrap_data.get("seed", 42)),
        benchmark=PassiveBenchmarkSpec(
            annual_rate=float(benchmark_data.get("annual_rate", 0.045)),
            instrument=str(benchmark_data["instrument"]) if "instrument" in benchmark_data else None,
            label=str(benchmark_data.get("label", "passive_cash_yield")),
            source=str(benchmark_data.get("source", "research-assumption")),
        ),
    )
    if research.walk_forward_train_bars <= 0:
        raise ValueError("walk_forward_train_bars must be positive")
    if research.walk_forward_validation_bars <= 0:
        raise ValueError("walk_forward_validation_bars must be positive")
    if research.walk_forward_step_bars != research.walk_forward_validation_bars:
        raise ValueError(
            "walk_forward_step_bars must equal walk_forward_validation_bars "
            "for contiguous OOS bootstrap"
        )
    if research.embargo_bars < 0:
        raise ValueError("embargo_bars must be non-negative")
    if research.bootstrap_block_size <= 0 or research.bootstrap_resamples < 2:
        raise ValueError("bootstrap block_size must be positive and resamples at least 2")
    if not 0.0 < research.bootstrap_confidence_level < 1.0:
        raise ValueError("bootstrap confidence_level must be in (0, 1)")
    if research.bootstrap_seed < 0:
        raise ValueError("bootstrap seed must be non-negative")

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

    return Settings(
        app=settings,
        costs=costs,
        cost_schedule=cost_schedule,
        execution=execution,
        research=research,
        risk_profiles=profiles,
    )
