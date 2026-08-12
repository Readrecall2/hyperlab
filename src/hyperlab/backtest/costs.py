from __future__ import annotations

import fnmatch
import math
import re
from dataclasses import dataclass
from typing import cast

import pandas as pd

CALIBRATION_STATUSES = frozenset({"CALIBRATED", "UNCALIBRATED", "SYNTHETIC"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def parse_instrument(instrument: str) -> tuple[str, str, str]:
    """Return ``venue, asset, kind`` from the canonical research identifier."""

    parts = instrument.split(":")
    if len(parts) != 3 or not all(parts):
        raise ValueError(f"instrument must use the canonical VENUE:ASSET:kind form: {instrument!r}")
    venue, asset, kind = parts
    if kind not in {"spot", "perp"}:
        raise ValueError(f"unsupported instrument kind {kind!r} in {instrument!r}")
    return venue, asset, kind


def _utc_timestamp(value: pd.Timestamp | str | None, *, label: str) -> pd.Timestamp | None:
    if value is None:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tz is None:
        raise ValueError(f"{label} must be timezone-aware UTC")
    timestamp = timestamp.tz_convert("UTC")
    return timestamp


@dataclass(frozen=True, slots=True)
class SlippageEstimate:
    slippage_bps: float
    participation: float
    fill_fraction: float
    capacity_usd: float


@dataclass(frozen=True, slots=True)
class SlippageModel:
    """Calibratable square-root/power impact model with an explicit capacity cap.

    ``depth_usd`` is the executable depth for the simulated horizon, not daily volume.
    If the requested notional exceeds ``max_participation * depth_usd``, only the
    capacity is filled. The unfilled residual remains visible to the execution model.
    """

    base_bps: float = 0.0
    impact_coefficient_bps: float = 0.0
    exponent: float = 0.5
    max_participation: float = 0.10

    def __post_init__(self) -> None:
        for name, value in (
            ("base_bps", self.base_bps),
            ("impact_coefficient_bps", self.impact_coefficient_bps),
        ):
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{name} must be finite and non-negative")
        if not math.isfinite(self.exponent) or self.exponent <= 0.0:
            raise ValueError("exponent must be finite and positive")
        if not math.isfinite(self.max_participation) or not 0.0 < self.max_participation <= 1.0:
            raise ValueError("max_participation must be in (0, 1]")

    def estimate(self, *, notional_usd: float, depth_usd: float) -> SlippageEstimate:
        if not math.isfinite(notional_usd) or notional_usd < 0.0:
            raise ValueError("notional_usd must be finite and non-negative")
        if not math.isfinite(depth_usd) or depth_usd <= 0.0:
            raise ValueError("depth_usd must be finite and positive")
        participation = notional_usd / depth_usd
        capacity_usd = self.max_participation * depth_usd
        fill_fraction = 1.0 if notional_usd == 0.0 else min(1.0, capacity_usd / notional_usd)
        impact = self.impact_coefficient_bps * participation**self.exponent
        return SlippageEstimate(
            slippage_bps=self.base_bps + impact,
            participation=participation,
            fill_fraction=fill_fraction,
            capacity_usd=capacity_usd,
        )


@dataclass(frozen=True, slots=True)
class CostRule:
    """Point-in-time venue/instrument fee and impact rule.

    ``instrument`` may be exact or contain shell-style wildcards. Exact rules take
    precedence over venue/kind patterns. Effective intervals are ``[from, to)``.
    Signed maker fees are supported so rebates remain explicit.
    """

    instrument: str
    maker_fee_bps: float
    taker_fee_bps: float
    slippage: SlippageModel
    effective_from: pd.Timestamp | str | None = None
    effective_to: pd.Timestamp | str | None = None
    source: str = "research-placeholder"

    def __post_init__(self) -> None:
        if not self.instrument or self.instrument.count(":") != 2:
            raise ValueError("instrument rule must use VENUE:ASSET:kind or a matching wildcard")
        for name, value in (
            ("maker_fee_bps", self.maker_fee_bps),
            ("taker_fee_bps", self.taker_fee_bps),
        ):
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        start = _utc_timestamp(self.effective_from, label="effective_from")
        stop = _utc_timestamp(self.effective_to, label="effective_to")
        object.__setattr__(self, "effective_from", start)
        object.__setattr__(self, "effective_to", stop)
        if start is not None and stop is not None and start >= stop:
            raise ValueError("effective_from must precede effective_to")
        if not self.source.strip():
            raise ValueError("source cannot be empty")

    @property
    def specificity(self) -> int:
        return sum(character not in "*?[]" for character in self.instrument)

    def matches(self, timestamp: pd.Timestamp, instrument: str) -> bool:
        when = _utc_timestamp(timestamp, label="timestamp")
        assert when is not None
        start = cast(pd.Timestamp | None, self.effective_from)
        stop = cast(pd.Timestamp | None, self.effective_to)
        if not fnmatch.fnmatchcase(instrument, self.instrument):
            return False
        if start is not None and when < start:
            return False
        return stop is None or when < stop


@dataclass(frozen=True, slots=True)
class CostSchedule:
    rules: tuple[CostRule, ...]
    calibration_status: str = "UNCALIBRATED"
    calibration_evidence_hash: str | None = None

    def __post_init__(self) -> None:
        if not self.rules:
            raise ValueError("cost schedule requires at least one rule")
        status = self.calibration_status.upper()
        if status not in CALIBRATION_STATUSES:
            raise ValueError(f"unknown calibration_status: {self.calibration_status}")
        object.__setattr__(self, "calibration_status", status)
        evidence = self.calibration_evidence_hash
        if evidence is not None and not _SHA256_RE.fullmatch(evidence):
            raise ValueError("cost calibration_evidence_hash must be a lowercase SHA-256 digest")
        if status == "CALIBRATED" and evidence is None:
            raise ValueError("CALIBRATED costs require a calibration_evidence_hash")
        if status == "CALIBRATED" and any(
            marker in rule.source.casefold()
            for rule in self.rules
            for marker in ("uncalibrated", "placeholder", "synthetic")
        ):
            raise ValueError("CALIBRATED cost rules require non-placeholder evidence sources")

    def lookup(self, timestamp: pd.Timestamp, instrument: str) -> CostRule:
        parse_instrument(instrument)
        candidates = [rule for rule in self.rules if rule.matches(timestamp, instrument)]
        if not candidates:
            raise ValueError(
                f"no point-in-time cost rule for {instrument} at {pd.Timestamp(timestamp).isoformat()}"
            )
        candidates.sort(
            key=lambda rule: (
                rule.specificity,
                cast(pd.Timestamp | None, rule.effective_from) or pd.Timestamp.min.tz_localize("UTC"),
            ),
            reverse=True,
        )
        winner = candidates[0]
        if len(candidates) > 1:
            runner_up = candidates[1]
            if (
                winner.specificity == runner_up.specificity
                and winner.effective_from == runner_up.effective_from
            ):
                raise ValueError(
                    f"ambiguous cost rules for {instrument} at {pd.Timestamp(timestamp).isoformat()}"
                )
        return winner


def adverse_fee_bps(fee_bps: float, multiplier: float) -> float:
    """Stress costs without making a signed maker rebate more favourable."""

    if not math.isfinite(multiplier) or multiplier <= 0.0:
        raise ValueError("cost multiplier must be finite and positive")
    if fee_bps >= 0.0:
        return fee_bps * multiplier
    return fee_bps / multiplier
