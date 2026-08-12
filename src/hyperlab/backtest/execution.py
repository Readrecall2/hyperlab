from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Literal

OrderType = Literal["maker", "taker"]
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class MakerFillModel:
    """Bernoulli eligibility model; a maker order may always remain unfilled."""

    base_probability: float = 0.65
    participation_decay: float = 2.0
    calibration_id: str = "uncalibrated-default"
    calibration_status: str = "UNCALIBRATED"
    calibration_evidence_hash: str | None = None

    def __post_init__(self) -> None:
        if not math.isfinite(self.base_probability) or not 0.0 <= self.base_probability <= 1.0:
            raise ValueError("base_probability must be in [0, 1]")
        if not math.isfinite(self.participation_decay) or self.participation_decay < 0.0:
            raise ValueError("participation_decay must be finite and non-negative")
        if not self.calibration_id.strip():
            raise ValueError("calibration_id cannot be empty")
        status = self.calibration_status.upper()
        if status not in {"CALIBRATED", "UNCALIBRATED", "SYNTHETIC"}:
            raise ValueError("unknown maker calibration_status")
        object.__setattr__(self, "calibration_status", status)
        evidence = self.calibration_evidence_hash
        if evidence is not None and not _SHA256_RE.fullmatch(evidence):
            raise ValueError("maker calibration_evidence_hash must be a lowercase SHA-256 digest")
        if status == "CALIBRATED" and evidence is None:
            raise ValueError("CALIBRATED maker fills require a calibration_evidence_hash")
        if status == "CALIBRATED" and any(
            marker in self.calibration_id.casefold()
            for marker in ("uncalibrated", "placeholder", "synthetic", "default")
        ):
            raise ValueError("CALIBRATED maker fills require a non-placeholder calibration_id")

    def probability(self, *, participation: float, multiplier: float = 1.0) -> float:
        if not math.isfinite(participation) or participation < 0.0:
            raise ValueError("participation must be finite and non-negative")
        if not math.isfinite(multiplier) or multiplier < 0.0:
            raise ValueError("fill probability multiplier must be finite and non-negative")
        if self.base_probability in {0.0, 1.0}:
            probability = self.base_probability
        else:
            probability = self.base_probability * math.exp(-self.participation_decay * participation)
        return min(1.0, max(0.0, probability * multiplier))


@dataclass(frozen=True, slots=True)
class ExecutionConfig:
    initial_capital: float = 100_000.0
    default_order_type: OrderType = "taker"
    maker_fill: MakerFillModel = field(default_factory=MakerFillModel)
    maker_fill_multiplier: float = 1.0
    maker_timeout_bars: int = 1
    emergency_ioc: bool = True
    ioc_fill_probability: float = 1.0
    ioc_extra_slippage_bps: float = 2.0
    base_latency_bars: int = 0
    leg_delay_bars: int = 0
    cost_multiplier: float = 1.0
    seed: int = 0
    require_depth: bool = False
    require_point_in_time: bool = False

    def __post_init__(self) -> None:
        if not math.isfinite(self.initial_capital) or self.initial_capital <= 0.0:
            raise ValueError("initial_capital must be finite and positive")
        if self.default_order_type not in {"maker", "taker"}:
            raise ValueError("default_order_type must be maker or taker")
        for name, integer_value in (
            ("maker_timeout_bars", self.maker_timeout_bars),
            ("base_latency_bars", self.base_latency_bars),
            ("leg_delay_bars", self.leg_delay_bars),
        ):
            if not isinstance(integer_value, int) or isinstance(integer_value, bool) or integer_value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name, float_value in (
            ("maker_fill_multiplier", self.maker_fill_multiplier),
            ("cost_multiplier", self.cost_multiplier),
        ):
            if not math.isfinite(float_value) or float_value <= 0.0:
                raise ValueError(f"{name} must be finite and positive")
        if not math.isfinite(self.ioc_fill_probability) or not 0.0 <= self.ioc_fill_probability <= 1.0:
            raise ValueError("ioc_fill_probability must be in [0, 1]")
        if not math.isfinite(self.ioc_extra_slippage_bps) or self.ioc_extra_slippage_bps < 0.0:
            raise ValueError("ioc_extra_slippage_bps must be finite and non-negative")
