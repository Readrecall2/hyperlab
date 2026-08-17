from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from types import MappingProxyType
from typing import cast

from hyperlab.backtest.costs import CostRule, CostSchedule, SlippageModel, parse_instrument
from hyperlab.backtest.execution import MakerFillModel
from hyperlab.backtest.protocol import JsonValue, canonical_json, canonical_sha256

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CALIBRATION_STATUSES = frozenset({"CALIBRATED", "UNCALIBRATED", "SYNTHETIC", "TOY"})
_ACTIVE_ORDER_STATUSES = frozenset(
    {"RISK_ACCEPTED", "ACKED", "CANCEL_PENDING", "PARTIALLY_FILLED"}
)
PAPER_ENGINE_BUILD_HASH = canonical_sha256(
    {
        "component": "hyperlab.paper",
        "execution_semantics": "phase12-event-sourced-v2",
        "schema_version": 1,
    }
)


def _utc(value: datetime, *, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{label} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must use UTC")
    return value.astimezone(UTC)


def utc_text(value: datetime) -> str:
    return _utc(value, label="timestamp").isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: str, *, label: str = "timestamp") -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} must be an ISO-8601 UTC timestamp") from error
    return _utc(parsed, label=label)


def decimal_value(
    value: Decimal | str | int,
    *,
    label: str,
    positive: bool = False,
    non_negative: bool = False,
) -> Decimal:
    if isinstance(value, (bool, float)):
        raise TypeError(f"{label} must be an exact Decimal, integer, or decimal string")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{label} must be a valid decimal") from error
    if not result.is_finite():
        raise ValueError(f"{label} must be finite")
    if positive and result <= 0:
        raise ValueError(f"{label} must be positive")
    if non_negative and result < 0:
        raise ValueError(f"{label} must be non-negative")
    return Decimal(0) if result == 0 else result


def decimal_text(value: Decimal) -> str:
    exact = decimal_value(value, label="decimal")
    text = format(exact, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _identifier(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} cannot be empty")
    if any(character.isspace() for character in normalized):
        raise ValueError(f"{label} cannot contain whitespace")
    return normalized


def _digest(value: str, *, label: str) -> str:
    normalized = _identifier(value, label=label)
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return normalized


def _json_value(value: object, *, path: str = "$") -> JsonValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} cannot contain NaN or infinity")
        return value
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, datetime):
        return utc_text(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        normalized: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} contains a non-string key")
            normalized[key] = _json_value(item, path=f"{path}.{key}")
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    raise TypeError(f"{path} contains unsupported type {type(value).__name__}")


def _deep_freeze(value: JsonValue) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_deep_freeze(item) for item in value)
    return value


def frozen_json_mapping(value: Mapping[str, object], *, label: str) -> Mapping[str, object]:
    normalized = _json_value(value, path=label)
    if not isinstance(normalized, dict):
        raise TypeError(f"{label} must be a mapping")
    decoded = cast(dict[str, JsonValue], json.loads(canonical_json(normalized)))
    return cast(Mapping[str, object], _deep_freeze(decoded))


def deterministic_id(kind: str, *components: object) -> str:
    """Return a process-, clock-, and restart-independent logical identifier."""

    return canonical_sha256(
        {
            "components": [_json_value(component) for component in components],
            "kind": _identifier(kind, label="identifier kind"),
            "schema_version": 1,
        }
    )


def event_genesis_hash(run_id: str, config_hash: str) -> str:
    return canonical_sha256(
        {
            "config_hash": _digest(config_hash, label="config_hash"),
            "domain": "hyperlab-paper-event-genesis-v1",
            "run_id": _digest(run_id, label="run_id"),
        }
    )


def keyed_uniform(seed: int, *, purpose: str, identity: str, attempt: int = 0) -> float:
    """Produce a deterministic draw without a mutable process-wide RNG stream."""

    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("seed must be a non-negative integer")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0:
        raise ValueError("attempt must be a non-negative integer")
    digest = deterministic_id("paper_draw", seed, purpose, identity, attempt)
    return int(digest[:16], 16) / float(1 << 64)


class PaperState(StrEnum):
    FLAT = "FLAT"
    ENTRY_PLANNED = "ENTRY_PLANNED"
    LEG_1_PENDING = "LEG_1_PENDING"
    HEDGE_PENDING = "HEDGE_PENDING"
    HEDGED = "HEDGED"
    EXIT_PLANNED = "EXIT_PLANNED"
    EXIT_PENDING = "EXIT_PENDING"
    PAUSED = "PAUSED"
    REDUCE_ONLY = "REDUCE_ONLY"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    EMERGENCY_FLATTEN = "EMERGENCY_FLATTEN"


_TRANSITIONS: Mapping[PaperState, frozenset[PaperState]] = {
    PaperState.FLAT: frozenset(
        {PaperState.ENTRY_PLANNED, PaperState.PAUSED, PaperState.MANUAL_REVIEW}
    ),
    PaperState.ENTRY_PLANNED: frozenset(
        {
            PaperState.LEG_1_PENDING,
            PaperState.FLAT,
            PaperState.PAUSED,
            PaperState.MANUAL_REVIEW,
        }
    ),
    PaperState.LEG_1_PENDING: frozenset(
        {
            PaperState.HEDGE_PENDING,
            PaperState.HEDGED,
            PaperState.FLAT,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        }
    ),
    PaperState.HEDGE_PENDING: frozenset(
        {
            PaperState.HEDGED,
            PaperState.FLAT,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        }
    ),
    PaperState.HEDGED: frozenset(
        {
            PaperState.EXIT_PLANNED,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        }
    ),
    PaperState.EXIT_PLANNED: frozenset(
        {
            PaperState.EXIT_PENDING,
            PaperState.HEDGED,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        }
    ),
    PaperState.EXIT_PENDING: frozenset(
        {
            PaperState.FLAT,
            PaperState.HEDGED,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        }
    ),
    PaperState.PAUSED: frozenset(
        {
            PaperState.FLAT,
            PaperState.LEG_1_PENDING,
            PaperState.HEDGE_PENDING,
            PaperState.HEDGED,
            PaperState.EXIT_PENDING,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        }
    ),
    PaperState.REDUCE_ONLY: frozenset(
        {
            PaperState.EXIT_PLANNED,
            PaperState.EXIT_PENDING,
            PaperState.FLAT,
            PaperState.PAUSED,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        }
    ),
    PaperState.MANUAL_REVIEW: frozenset(),
    PaperState.EMERGENCY_FLATTEN: frozenset(
        {PaperState.FLAT, PaperState.PAUSED, PaperState.MANUAL_REVIEW}
    ),
}


def legal_transition(source: PaperState, target: PaperState) -> bool:
    return source == target or target in _TRANSITIONS[source]


def require_transition(source: PaperState, target: PaperState) -> None:
    if not legal_transition(source, target):
        raise ValueError(f"illegal paper state transition: {source.value} -> {target.value}")


class DecisionAction(StrEnum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    HOLD = "HOLD"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def sign(self) -> Decimal:
        return Decimal(1) if self is OrderSide.BUY else Decimal(-1)


class PaperOrderType(StrEnum):
    MAKER = "MAKER"
    TAKER = "TAKER"


class TimeInForce(StrEnum):
    GTC = "GTC"
    IOC = "IOC"


class OrderStatus(StrEnum):
    PLANNED = "PLANNED"
    RISK_ACCEPTED = "RISK_ACCEPTED"
    ACKED = "ACKED"
    CANCEL_PENDING = "CANCEL_PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    NO_FILL = "NO_FILL"

    @property
    def active(self) -> bool:
        return self.value in _ACTIVE_ORDER_STATUSES


class PaperEventType(StrEnum):
    RUN_STARTED = "RUN_STARTED"
    DECISION_RECORDED = "DECISION_RECORDED"
    ORDER_PLANNED = "ORDER_PLANNED"
    RISK_ACCEPTED = "RISK_ACCEPTED"
    RISK_REJECTED = "RISK_REJECTED"
    ORDER_ACKED = "ORDER_ACKED"
    ORDER_REJECTED = "ORDER_REJECTED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    ORDER_PARTIALLY_FILLED = "ORDER_PARTIALLY_FILLED"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    ORDER_EXPIRED = "ORDER_EXPIRED"
    ORDER_NO_FILL = "ORDER_NO_FILL"
    MARK_RECORDED = "MARK_RECORDED"
    FUNDING_POSTED = "FUNDING_POSTED"
    TIMER_TICKED = "TIMER_TICKED"
    CYCLE_COMPLETED = "CYCLE_COMPLETED"
    STRESS_RESULT_RECORDED = "STRESS_RESULT_RECORDED"
    RESILIENCE_EXERCISE_RECORDED = "RESILIENCE_EXERCISE_RECORDED"
    OBSERVATION_COVERAGE_RECORDED = "OBSERVATION_COVERAGE_RECORDED"
    STATE_TRANSITIONED = "STATE_TRANSITIONED"
    ALERT_RAISED = "ALERT_RAISED"
    RECONCILIATION_SUCCEEDED = "RECONCILIATION_SUCCEEDED"
    RECONCILIATION_FAILED = "RECONCILIATION_FAILED"


class AlertSeverity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True, slots=True)
class PaperRiskLimits:
    max_gross_notional: Decimal = Decimal("100000")
    max_net_notional: Decimal = Decimal("100000")
    max_instrument_notional: Decimal = Decimal("50000")
    max_order_notional: Decimal = Decimal("25000")
    max_daily_loss: Decimal = Decimal("5000")
    max_drawdown: Decimal = Decimal("10000")
    stale_after_seconds: int = 30
    unhedged_timeout_seconds: int = 60

    def __post_init__(self) -> None:
        for name in (
            "max_gross_notional",
            "max_net_notional",
            "max_instrument_notional",
            "max_order_notional",
            "max_daily_loss",
            "max_drawdown",
        ):
            object.__setattr__(
                self,
                name,
                decimal_value(getattr(self, name), label=name, positive=True),
            )
        for name in ("stale_after_seconds", "unhedged_timeout_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "max_daily_loss": decimal_text(self.max_daily_loss),
            "max_drawdown": decimal_text(self.max_drawdown),
            "max_gross_notional": decimal_text(self.max_gross_notional),
            "max_instrument_notional": decimal_text(self.max_instrument_notional),
            "max_net_notional": decimal_text(self.max_net_notional),
            "max_order_notional": decimal_text(self.max_order_notional),
            "stale_after_seconds": self.stale_after_seconds,
            "unhedged_timeout_seconds": self.unhedged_timeout_seconds,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> PaperRiskLimits:
        return cls(
            max_gross_notional=decimal_value(
                str(value.get("max_gross_notional", "100000")), label="max_gross_notional"
            ),
            max_net_notional=decimal_value(
                str(value.get("max_net_notional", "100000")), label="max_net_notional"
            ),
            max_instrument_notional=decimal_value(
                str(value.get("max_instrument_notional", "50000")),
                label="max_instrument_notional",
            ),
            max_order_notional=decimal_value(
                str(value.get("max_order_notional", "25000")), label="max_order_notional"
            ),
            max_daily_loss=decimal_value(
                str(value.get("max_daily_loss", "5000")), label="max_daily_loss"
            ),
            max_drawdown=decimal_value(
                str(value.get("max_drawdown", "10000")), label="max_drawdown"
            ),
            stale_after_seconds=int(str(value.get("stale_after_seconds", 30))),
            unhedged_timeout_seconds=int(str(value.get("unhedged_timeout_seconds", 60))),
        )


@dataclass(frozen=True, slots=True)
class PaperExecutionConfig:
    maker_fill: MakerFillModel = field(default_factory=MakerFillModel)
    slippage: SlippageModel = field(default_factory=SlippageModel)
    maker_fee_bps: Decimal = Decimal("0")
    taker_fee_bps: Decimal = Decimal("0")
    cost_multiplier: Decimal = Decimal("1")
    ioc_fill_probability: Decimal = Decimal("1")
    ioc_extra_slippage_bps: Decimal = Decimal("0")
    ack_latency_ms: int = 0
    fill_latency_ms: int = 0
    leg_delay_ms: int = 0
    cancel_latency_ms: int = 0
    maker_timeout_ms: int = 1000
    calibration_status: str = "UNCALIBRATED"
    calibration_evidence_hash: str | None = None
    source: str = "research-placeholder"
    cost_schedule: CostSchedule | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.maker_fill, MakerFillModel):
            raise TypeError("maker_fill must be a MakerFillModel")
        if not isinstance(self.slippage, SlippageModel):
            raise TypeError("slippage must be a SlippageModel")
        for name in ("maker_fee_bps", "taker_fee_bps"):
            object.__setattr__(self, name, decimal_value(getattr(self, name), label=name))
        for name in ("cost_multiplier", "ioc_fill_probability", "ioc_extra_slippage_bps"):
            value = decimal_value(getattr(self, name), label=name, non_negative=True)
            object.__setattr__(self, name, value)
        if self.cost_multiplier <= 0:
            raise ValueError("cost_multiplier must be positive")
        if self.ioc_fill_probability > 1:
            raise ValueError("ioc_fill_probability must be in [0, 1]")
        for name in (
            "ack_latency_ms",
            "fill_latency_ms",
            "leg_delay_ms",
            "cancel_latency_ms",
            "maker_timeout_ms",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.maker_timeout_ms <= 0:
            raise ValueError("maker_timeout_ms must be positive")
        status = self.calibration_status.strip().upper()
        if status not in _CALIBRATION_STATUSES:
            raise ValueError(f"unknown execution calibration status: {self.calibration_status}")
        object.__setattr__(self, "calibration_status", status)
        if self.calibration_evidence_hash is not None:
            object.__setattr__(
                self,
                "calibration_evidence_hash",
                _digest(self.calibration_evidence_hash, label="execution calibration_evidence_hash"),
            )
        source = self.source.strip()
        if not source:
            raise ValueError("execution calibration source cannot be empty")
        object.__setattr__(self, "source", source)
        if status == "CALIBRATED":
            if self.calibration_evidence_hash is None:
                raise ValueError("CALIBRATED paper execution requires a calibration evidence hash")
            markers = ("placeholder", "synthetic", "uncalibrated", "default", "toy")
            if any(marker in source.casefold() for marker in markers):
                raise ValueError("CALIBRATED paper execution requires a non-placeholder source")
            if self.maker_fill.calibration_status != "CALIBRATED":
                raise ValueError("CALIBRATED paper execution requires calibrated maker fills")
            if self.cost_schedule is None:
                raise ValueError("CALIBRATED paper execution requires a point-in-time cost schedule")
            if self.cost_schedule.calibration_status != "CALIBRATED":
                raise ValueError("CALIBRATED paper execution requires calibrated costs")
            cost_markers = ("placeholder", "synthetic", "uncalibrated", "default", "toy")
            if any(
                marker in rule.source.casefold()
                for rule in self.cost_schedule.rules
                for marker in cost_markers
            ):
                raise ValueError("CALIBRATED paper costs require non-placeholder sources")

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "ack_latency_ms": self.ack_latency_ms,
            "calibration_evidence_hash": self.calibration_evidence_hash,
            "calibration_status": self.calibration_status,
            "cancel_latency_ms": self.cancel_latency_ms,
            "cost_multiplier": decimal_text(self.cost_multiplier),
            "cost_schedule": (
                {
                    "calibration_evidence_hash": self.cost_schedule.calibration_evidence_hash,
                    "calibration_status": self.cost_schedule.calibration_status,
                    "rules": [
                        {
                            "effective_from": (
                                str(rule.effective_from)
                                if rule.effective_from is not None
                                else None
                            ),
                            "effective_to": (
                                str(rule.effective_to)
                                if rule.effective_to is not None
                                else None
                            ),
                            "instrument": rule.instrument,
                            "maker_fee_bps": rule.maker_fee_bps,
                            "slippage": {
                                "base_bps": rule.slippage.base_bps,
                                "exponent": rule.slippage.exponent,
                                "impact_coefficient_bps": rule.slippage.impact_coefficient_bps,
                                "max_participation": rule.slippage.max_participation,
                            },
                            "source": rule.source,
                            "taker_fee_bps": rule.taker_fee_bps,
                        }
                        for rule in self.cost_schedule.rules
                    ],
                }
                if self.cost_schedule is not None
                else None
            ),
            "fill_latency_ms": self.fill_latency_ms,
            "ioc_extra_slippage_bps": decimal_text(self.ioc_extra_slippage_bps),
            "ioc_fill_probability": decimal_text(self.ioc_fill_probability),
            "leg_delay_ms": self.leg_delay_ms,
            "maker_fee_bps": decimal_text(self.maker_fee_bps),
            "maker_fill": {
                "base_probability": self.maker_fill.base_probability,
                "calibration_evidence_hash": self.maker_fill.calibration_evidence_hash,
                "calibration_id": self.maker_fill.calibration_id,
                "calibration_status": self.maker_fill.calibration_status,
                "participation_decay": self.maker_fill.participation_decay,
            },
            "maker_timeout_ms": self.maker_timeout_ms,
            "slippage": {
                "base_bps": self.slippage.base_bps,
                "exponent": self.slippage.exponent,
                "impact_coefficient_bps": self.slippage.impact_coefficient_bps,
                "max_participation": self.slippage.max_participation,
            },
            "source": self.source,
            "taker_fee_bps": decimal_text(self.taker_fee_bps),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> PaperExecutionConfig:
        maker_raw = value.get("maker_fill")
        slippage_raw = value.get("slippage")
        if not isinstance(maker_raw, Mapping) or not isinstance(slippage_raw, Mapping):
            raise ValueError("paper execution config lacks maker_fill or slippage")
        maker = MakerFillModel(
            base_probability=float(maker_raw["base_probability"]),
            participation_decay=float(maker_raw["participation_decay"]),
            calibration_id=str(maker_raw["calibration_id"]),
            calibration_status=str(maker_raw["calibration_status"]),
            calibration_evidence_hash=(
                str(maker_raw["calibration_evidence_hash"])
                if maker_raw.get("calibration_evidence_hash") is not None
                else None
            ),
        )
        slippage = SlippageModel(
            base_bps=float(slippage_raw["base_bps"]),
            impact_coefficient_bps=float(slippage_raw["impact_coefficient_bps"]),
            exponent=float(slippage_raw["exponent"]),
            max_participation=float(slippage_raw["max_participation"]),
        )
        raw_cost_schedule = value.get("cost_schedule")
        cost_schedule: CostSchedule | None = None
        if raw_cost_schedule is not None:
            if not isinstance(raw_cost_schedule, Mapping):
                raise ValueError("paper cost_schedule must be an object")
            raw_rules = raw_cost_schedule.get("rules")
            if not isinstance(raw_rules, Sequence) or isinstance(raw_rules, (str, bytes)):
                raise ValueError("paper cost_schedule rules must be an array")
            cost_rules: list[CostRule] = []
            for raw_rule in raw_rules:
                if not isinstance(raw_rule, Mapping):
                    raise ValueError("paper cost rule must be an object")
                raw_rule_slippage = raw_rule.get("slippage")
                if not isinstance(raw_rule_slippage, Mapping):
                    raise ValueError("paper cost rule lacks slippage")
                cost_rules.append(
                    CostRule(
                        instrument=str(raw_rule["instrument"]),
                        maker_fee_bps=float(raw_rule["maker_fee_bps"]),
                        taker_fee_bps=float(raw_rule["taker_fee_bps"]),
                        slippage=SlippageModel(
                            base_bps=float(raw_rule_slippage["base_bps"]),
                            impact_coefficient_bps=float(
                                raw_rule_slippage["impact_coefficient_bps"]
                            ),
                            exponent=float(raw_rule_slippage["exponent"]),
                            max_participation=float(
                                raw_rule_slippage["max_participation"]
                            ),
                        ),
                        effective_from=(
                            str(raw_rule["effective_from"])
                            if raw_rule.get("effective_from") is not None
                            else None
                        ),
                        effective_to=(
                            str(raw_rule["effective_to"])
                            if raw_rule.get("effective_to") is not None
                            else None
                        ),
                        source=str(raw_rule["source"]),
                    )
                )
            cost_schedule = CostSchedule(
                rules=tuple(cost_rules),
                calibration_status=str(
                    raw_cost_schedule.get("calibration_status", "UNCALIBRATED")
                ),
                calibration_evidence_hash=(
                    str(raw_cost_schedule["calibration_evidence_hash"])
                    if raw_cost_schedule.get("calibration_evidence_hash") is not None
                    else None
                ),
            )
        return cls(
            maker_fill=maker,
            slippage=slippage,
            maker_fee_bps=decimal_value(
                str(value.get("maker_fee_bps", "0")), label="maker_fee_bps"
            ),
            taker_fee_bps=decimal_value(
                str(value.get("taker_fee_bps", "0")), label="taker_fee_bps"
            ),
            cost_multiplier=decimal_value(
                str(value.get("cost_multiplier", "1")), label="cost_multiplier"
            ),
            ioc_fill_probability=decimal_value(
                str(value.get("ioc_fill_probability", "1")), label="ioc_fill_probability"
            ),
            ioc_extra_slippage_bps=decimal_value(
                str(value.get("ioc_extra_slippage_bps", "0")),
                label="ioc_extra_slippage_bps",
            ),
            ack_latency_ms=int(str(value.get("ack_latency_ms", 0))),
            fill_latency_ms=int(str(value.get("fill_latency_ms", 0))),
            leg_delay_ms=int(str(value.get("leg_delay_ms", 0))),
            cancel_latency_ms=int(str(value.get("cancel_latency_ms", 0))),
            maker_timeout_ms=int(str(value.get("maker_timeout_ms", 1000))),
            calibration_status=str(value.get("calibration_status", "UNCALIBRATED")),
            calibration_evidence_hash=(
                str(value["calibration_evidence_hash"])
                if value.get("calibration_evidence_hash") is not None
                else None
            ),
            source=str(value.get("source", "research-placeholder")),
            cost_schedule=cost_schedule,
        )


@dataclass(frozen=True, slots=True)
class PaperRunConfig:
    strategy_name: str
    strategy_hash: str
    parameters: Mapping[str, object]
    data_hash: str
    execution: PaperExecutionConfig
    risk: PaperRiskLimits
    seed: int
    initial_cash: Decimal
    validation_started_at: datetime
    run_kind: str = "DEMO"
    data_calibration_status: str = "UNCALIBRATED"
    data_calibration_evidence_hash: str | None = None
    data_source: str = "research-placeholder"
    economic_prerequisites_satisfied: bool = False
    economic_prerequisites_evidence_hash: str | None = None
    required_instruments: tuple[str, ...] = ()
    engine_build_hash: str = PAPER_ENGINE_BUILD_HASH
    minimum_validation_cycles: int = 30
    schema_version: int = 2
    environment: str = "PAPER"

    def __post_init__(self) -> None:
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version not in {1, 2}
        ):
            raise ValueError("paper configuration schema_version must be 1 or 2")
        if self.environment != "PAPER":
            raise ValueError("paper configuration environment must be PAPER")
        object.__setattr__(self, "strategy_name", _identifier(self.strategy_name, label="strategy_name"))
        object.__setattr__(self, "strategy_hash", _digest(self.strategy_hash, label="strategy_hash"))
        object.__setattr__(self, "data_hash", _digest(self.data_hash, label="data_hash"))
        if not isinstance(self.parameters, Mapping):
            raise TypeError("parameters must be a mapping")
        object.__setattr__(
            self,
            "parameters",
            frozen_json_mapping(self.parameters, label="parameters"),
        )
        if not isinstance(self.execution, PaperExecutionConfig):
            raise TypeError("execution must be a PaperExecutionConfig")
        if not isinstance(self.risk, PaperRiskLimits):
            raise TypeError("risk must be a PaperRiskLimits")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int) or self.seed < 0:
            raise ValueError("seed must be a non-negative integer")
        object.__setattr__(
            self,
            "initial_cash",
            decimal_value(self.initial_cash, label="initial_cash", positive=True),
        )
        object.__setattr__(
            self,
            "validation_started_at",
            _utc(self.validation_started_at, label="validation_started_at"),
        )
        kind = self.run_kind.strip().upper()
        if kind not in {"DEMO", "TECHNICAL", "VALIDATION"}:
            raise ValueError("run_kind must be DEMO, TECHNICAL, or VALIDATION")
        object.__setattr__(self, "run_kind", kind)
        status = self.data_calibration_status.strip().upper()
        if status not in _CALIBRATION_STATUSES:
            raise ValueError(f"unknown data calibration status: {self.data_calibration_status}")
        object.__setattr__(self, "data_calibration_status", status)
        if self.data_calibration_evidence_hash is not None:
            object.__setattr__(
                self,
                "data_calibration_evidence_hash",
                _digest(self.data_calibration_evidence_hash, label="data calibration evidence hash"),
            )
        source = self.data_source.strip()
        if not source:
            raise ValueError("data_source cannot be empty")
        object.__setattr__(self, "data_source", source)
        if not isinstance(self.required_instruments, tuple):
            raise TypeError("required_instruments must be a frozen tuple")
        for instrument in self.required_instruments:
            parse_instrument(instrument)
        if len(set(self.required_instruments)) != len(self.required_instruments):
            raise ValueError("required_instruments must not contain duplicates")
        object.__setattr__(
            self,
            "required_instruments",
            tuple(sorted(self.required_instruments)),
        )
        if self.economic_prerequisites_evidence_hash is not None:
            object.__setattr__(
                self,
                "economic_prerequisites_evidence_hash",
                _digest(
                    self.economic_prerequisites_evidence_hash,
                    label="economic prerequisites evidence hash",
                ),
            )
        if (
            self.economic_prerequisites_satisfied
            and self.economic_prerequisites_evidence_hash is None
        ):
            raise ValueError(
                "satisfied economic prerequisites require a Gate B/C evidence hash"
            )
        if status == "CALIBRATED":
            if self.data_calibration_evidence_hash is None:
                raise ValueError("CALIBRATED paper data requires a calibration evidence hash")
            markers = ("placeholder", "synthetic", "uncalibrated", "default", "toy")
            if any(marker in source.casefold() for marker in markers):
                raise ValueError("CALIBRATED paper data requires a non-placeholder source")
        object.__setattr__(
            self,
            "engine_build_hash",
            _digest(self.engine_build_hash, label="engine_build_hash"),
        )
        if self.engine_build_hash != PAPER_ENGINE_BUILD_HASH:
            raise ValueError("paper configuration targets a different execution-engine build")
        if (
            isinstance(self.minimum_validation_cycles, bool)
            or not isinstance(self.minimum_validation_cycles, int)
            or self.minimum_validation_cycles < 1
        ):
            raise ValueError("minimum_validation_cycles must be at least 1")
        if kind == "VALIDATION" and self.minimum_validation_cycles < 30:
            raise ValueError("VALIDATION minimum_validation_cycles must be at least 30")
        if kind == "VALIDATION" and not self.economically_eligible:
            raise ValueError(
                "VALIDATION paper runs require satisfied economic prerequisites and calibrated data/execution"
            )

    @property
    def economically_eligible(self) -> bool:
        return (
            self.economic_prerequisites_satisfied
            and bool(self.required_instruments)
            and self.data_calibration_status == "CALIBRATED"
            and self.execution.calibration_status == "CALIBRATED"
            and self.execution.maker_fill.calibration_status == "CALIBRATED"
            and self.execution.cost_schedule is not None
            and self.execution.cost_schedule.calibration_status == "CALIBRATED"
        )

    def to_dict(self) -> dict[str, JsonValue]:
        payload: dict[str, JsonValue] = {
            "data_calibration_evidence_hash": self.data_calibration_evidence_hash,
            "data_calibration_status": self.data_calibration_status,
            "data_hash": self.data_hash,
            "data_source": self.data_source,
            "economic_prerequisites_satisfied": self.economic_prerequisites_satisfied,
            "economic_prerequisites_evidence_hash": (
                self.economic_prerequisites_evidence_hash
            ),
            "engine_build_hash": self.engine_build_hash,
            "execution": self.execution.to_dict(),
            "initial_cash": decimal_text(self.initial_cash),
            "minimum_validation_cycles": self.minimum_validation_cycles,
            "parameters": cast(dict[str, JsonValue], json.loads(canonical_json(self.parameters))),
            "required_instruments": list(self.required_instruments),
            "risk": self.risk.to_dict(),
            "run_kind": self.run_kind,
            "schema_version": self.schema_version,
            "seed": self.seed,
            "strategy_hash": self.strategy_hash,
            "strategy_name": self.strategy_name,
            "validation_started_at": utc_text(self.validation_started_at),
        }
        if self.schema_version >= 2:
            payload["environment"] = self.environment
        return payload

    @property
    def config_hash(self) -> str:
        return canonical_sha256(self.to_dict())

    @property
    def run_id(self) -> str:
        return deterministic_id("paper_run", self.config_hash)

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> PaperRunConfig:
        execution = value.get("execution")
        risk = value.get("risk")
        parameters = value.get("parameters")
        if not isinstance(execution, Mapping) or not isinstance(risk, Mapping):
            raise ValueError("paper run config lacks execution or risk")
        if not isinstance(parameters, Mapping):
            raise ValueError("paper run config parameters must be an object")
        raw_schema_version = value.get("schema_version", 1)
        if isinstance(raw_schema_version, bool):
            raise ValueError("paper configuration schema_version must be 1 or 2")
        schema_version = int(str(raw_schema_version))
        if schema_version == 2 and "environment" not in value:
            raise ValueError("schema v2 paper configuration requires explicit environment PAPER")
        return cls(
            strategy_name=str(value["strategy_name"]),
            strategy_hash=str(value["strategy_hash"]),
            parameters=cast(Mapping[str, object], parameters),
            data_hash=str(value["data_hash"]),
            execution=PaperExecutionConfig.from_dict(execution),
            risk=PaperRiskLimits.from_dict(risk),
            seed=int(str(value["seed"])),
            initial_cash=decimal_value(str(value["initial_cash"]), label="initial_cash"),
            validation_started_at=parse_utc(str(value["validation_started_at"])),
            schema_version=schema_version,
            environment=str(value.get("environment", "PAPER")),
            run_kind=str(value.get("run_kind", "DEMO")),
            data_calibration_status=str(value.get("data_calibration_status", "UNCALIBRATED")),
            data_calibration_evidence_hash=(
                str(value["data_calibration_evidence_hash"])
                if value.get("data_calibration_evidence_hash") is not None
                else None
            ),
            data_source=str(value.get("data_source", "research-placeholder")),
            economic_prerequisites_satisfied=bool(
                value.get("economic_prerequisites_satisfied", False)
            ),
            economic_prerequisites_evidence_hash=(
                str(value["economic_prerequisites_evidence_hash"])
                if value.get("economic_prerequisites_evidence_hash") is not None
                else None
            ),
            required_instruments=tuple(
                str(instrument)
                for instrument in cast(
                    Sequence[object], value.get("required_instruments", ())
                )
            ),
            engine_build_hash=str(value.get("engine_build_hash", PAPER_ENGINE_BUILD_HASH)),
            minimum_validation_cycles=int(str(value.get("minimum_validation_cycles", 30))),
        )


@dataclass(frozen=True, slots=True)
class OrderIntent:
    order_id: str
    decision_id: str
    run_id: str
    instrument: str
    side: OrderSide | str
    quantity: Decimal
    order_type: PaperOrderType | str
    time_in_force: TimeInForce | str
    created_at: datetime
    ordinal: int = 0
    limit_price: Decimal | None = None
    reduce_only: bool = False
    hedge_group_id: str | None = None
    leg_number: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "order_id", _digest(self.order_id, label="order_id"))
        object.__setattr__(self, "decision_id", _digest(self.decision_id, label="decision_id"))
        object.__setattr__(self, "run_id", _digest(self.run_id, label="run_id"))
        parse_instrument(self.instrument)
        object.__setattr__(self, "side", OrderSide(self.side))
        object.__setattr__(
            self,
            "quantity",
            decimal_value(self.quantity, label="quantity", positive=True),
        )
        object.__setattr__(self, "order_type", PaperOrderType(self.order_type))
        object.__setattr__(self, "time_in_force", TimeInForce(self.time_in_force))
        object.__setattr__(self, "created_at", _utc(self.created_at, label="created_at"))
        if self.limit_price is not None:
            object.__setattr__(
                self,
                "limit_price",
                decimal_value(self.limit_price, label="limit_price", positive=True),
            )
        if self.order_type is PaperOrderType.MAKER and self.limit_price is None:
            raise ValueError("maker orders require an explicit limit_price")
        if isinstance(self.leg_number, bool) or not isinstance(self.leg_number, int) or self.leg_number <= 0:
            raise ValueError("leg_number must be a positive integer")
        if isinstance(self.ordinal, bool) or not isinstance(self.ordinal, int) or self.ordinal < 0:
            raise ValueError("order ordinal must be a non-negative integer")
        if self.hedge_group_id is not None:
            object.__setattr__(
                self,
                "hedge_group_id",
                _identifier(self.hedge_group_id, label="hedge_group_id"),
            )
        expected_id = deterministic_id(
            "paper_order",
            self.run_id,
            self.decision_id,
            self.ordinal,
            self.instrument,
            cast(OrderSide, self.side).value,
            self.quantity,
            cast(PaperOrderType, self.order_type).value,
            cast(TimeInForce, self.time_in_force).value,
            self.limit_price,
            self.reduce_only,
            self.hedge_group_id,
            self.leg_number,
        )
        if self.order_id != expected_id:
            raise ValueError("order_id does not match the deterministic order payload")

    @classmethod
    def create(
        cls,
        *,
        decision_id: str,
        run_id: str,
        instrument: str,
        side: OrderSide | str,
        quantity: Decimal | str | int,
        order_type: PaperOrderType | str,
        time_in_force: TimeInForce | str,
        created_at: datetime,
        ordinal: int,
        limit_price: Decimal | str | int | None = None,
        reduce_only: bool = False,
        hedge_group_id: str | None = None,
        leg_number: int = 1,
    ) -> OrderIntent:
        order_id = deterministic_id(
            "paper_order",
            run_id,
            decision_id,
            ordinal,
            instrument,
            str(side),
            quantity,
            str(order_type),
            str(time_in_force),
            limit_price,
            reduce_only,
            hedge_group_id,
            leg_number,
        )
        return cls(
            order_id=order_id,
            decision_id=decision_id,
            run_id=run_id,
            instrument=instrument,
            side=side,
            quantity=cast(Decimal, quantity),
            order_type=order_type,
            time_in_force=time_in_force,
            created_at=created_at,
            ordinal=ordinal,
            limit_price=cast(Decimal | None, limit_price),
            reduce_only=reduce_only,
            hedge_group_id=hedge_group_id,
            leg_number=leg_number,
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "created_at": utc_text(self.created_at),
            "decision_id": self.decision_id,
            "hedge_group_id": self.hedge_group_id,
            "instrument": self.instrument,
            "leg_number": self.leg_number,
            "limit_price": decimal_text(self.limit_price) if self.limit_price is not None else None,
            "order_id": self.order_id,
            "ordinal": self.ordinal,
            "order_type": cast(PaperOrderType, self.order_type).value,
            "quantity": decimal_text(self.quantity),
            "reduce_only": self.reduce_only,
            "run_id": self.run_id,
            "side": cast(OrderSide, self.side).value,
            "time_in_force": cast(TimeInForce, self.time_in_force).value,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> OrderIntent:
        return cls(
            order_id=str(value["order_id"]),
            decision_id=str(value["decision_id"]),
            run_id=str(value["run_id"]),
            instrument=str(value["instrument"]),
            side=str(value["side"]),
            quantity=decimal_value(str(value["quantity"]), label="quantity"),
            order_type=str(value["order_type"]),
            time_in_force=str(value["time_in_force"]),
            created_at=parse_utc(str(value["created_at"])),
            ordinal=int(str(value.get("ordinal", 0))),
            limit_price=(
                decimal_value(str(value["limit_price"]), label="limit_price")
                if value.get("limit_price") is not None
                else None
            ),
            reduce_only=bool(value.get("reduce_only", False)),
            hedge_group_id=(
                str(value["hedge_group_id"]) if value.get("hedge_group_id") is not None else None
            ),
            leg_number=int(str(value.get("leg_number", 1))),
        )


@dataclass(frozen=True, slots=True)
class DecisionIntent:
    decision_id: str
    run_id: str
    strategy_name: str
    action: DecisionAction | str
    decided_at: datetime
    received_at: datetime
    market_event_id: str
    observed_event_ids: tuple[str, ...]
    orders: tuple[OrderIntent, ...]
    ordinal: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "decision_id", _digest(self.decision_id, label="decision_id"))
        object.__setattr__(self, "run_id", _digest(self.run_id, label="run_id"))
        object.__setattr__(self, "strategy_name", _identifier(self.strategy_name, label="strategy_name"))
        object.__setattr__(self, "action", DecisionAction(self.action))
        object.__setattr__(self, "decided_at", _utc(self.decided_at, label="decided_at"))
        object.__setattr__(self, "received_at", _utc(self.received_at, label="received_at"))
        if self.decided_at < self.received_at:
            raise ValueError("decided_at cannot precede the last received observation")
        object.__setattr__(self, "market_event_id", _digest(self.market_event_id, label="market_event_id"))
        observed = tuple(_digest(value, label="observed_event_id") for value in self.observed_event_ids)
        if not observed or self.market_event_id not in observed:
            raise ValueError("observed_event_ids must include market_event_id")
        if len(set(observed)) != len(observed):
            raise ValueError("observed_event_ids cannot contain duplicates")
        object.__setattr__(self, "observed_event_ids", observed)
        if self.action is DecisionAction.HOLD and self.orders:
            raise ValueError("HOLD decisions cannot contain orders")
        if self.action is not DecisionAction.HOLD and not self.orders:
            raise ValueError("ENTRY and EXIT decisions require at least one order")
        for order in self.orders:
            if order.run_id != self.run_id or order.decision_id != self.decision_id:
                raise ValueError("every order must be bound to the same run and decision")
            if order.created_at != self.decided_at:
                raise ValueError("order created_at must equal decision decided_at")
        order_ids = [order.order_id for order in self.orders]
        if len(set(order_ids)) != len(order_ids):
            raise ValueError("a decision cannot contain duplicate orders")
        if self.action is DecisionAction.ENTRY and any(order.reduce_only for order in self.orders):
            raise ValueError("ENTRY decisions cannot contain reduce_only orders")
        if self.action is DecisionAction.EXIT and any(not order.reduce_only for order in self.orders):
            raise ValueError("EXIT decisions require reduce_only orders")
        if isinstance(self.ordinal, bool) or not isinstance(self.ordinal, int) or self.ordinal < 0:
            raise ValueError("decision ordinal must be a non-negative integer")
        expected_id = self.identifier(
            run_id=self.run_id,
            market_event_id=self.market_event_id,
            action=cast(DecisionAction, self.action),
            ordinal=self.ordinal,
        )
        if self.decision_id != expected_id:
            raise ValueError("decision_id does not match the deterministic decision payload")

    @classmethod
    def identifier(
        cls,
        *,
        run_id: str,
        market_event_id: str,
        action: DecisionAction | str,
        ordinal: int,
    ) -> str:
        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
            raise ValueError("decision ordinal must be a non-negative integer")
        return deterministic_id("paper_decision", run_id, market_event_id, str(action), ordinal)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "action": cast(DecisionAction, self.action).value,
            "decided_at": utc_text(self.decided_at),
            "decision_id": self.decision_id,
            "market_event_id": self.market_event_id,
            "observed_event_ids": list(self.observed_event_ids),
            "orders": [order.to_dict() for order in self.orders],
            "ordinal": self.ordinal,
            "received_at": utc_text(self.received_at),
            "run_id": self.run_id,
            "strategy_name": self.strategy_name,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> DecisionIntent:
        raw_orders = value.get("orders")
        raw_observed = value.get("observed_event_ids")
        if not isinstance(raw_orders, Sequence) or isinstance(raw_orders, (str, bytes)):
            raise ValueError("decision orders must be an array")
        if not isinstance(raw_observed, Sequence) or isinstance(
            raw_observed, (str, bytes)
        ):
            raise ValueError("decision observed_event_ids must be an array")
        return cls(
            decision_id=str(value["decision_id"]),
            run_id=str(value["run_id"]),
            strategy_name=str(value["strategy_name"]),
            action=str(value["action"]),
            decided_at=parse_utc(str(value["decided_at"]), label="decided_at"),
            received_at=parse_utc(str(value["received_at"]), label="received_at"),
            market_event_id=str(value["market_event_id"]),
            observed_event_ids=tuple(str(item) for item in raw_observed),
            orders=tuple(
                OrderIntent.from_dict(cast(Mapping[str, object], item))
                for item in raw_orders
                if isinstance(item, Mapping)
            ),
            ordinal=int(str(value.get("ordinal", 0))),
        )


@dataclass(frozen=True, slots=True)
class MarketEvent:
    event_id: str
    received_at: datetime
    instrument: str
    bid_price: Decimal
    ask_price: Decimal
    bid_depth: Decimal
    ask_depth: Decimal
    source_sequence: int | None = None
    capture_ordinal: int = 0
    trade_price: Decimal | None = None
    trade_quantity: Decimal | None = None
    aggressor_side: OrderSide | str | None = None
    stale: bool = False
    gap: bool = False
    tradable: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _digest(self.event_id, label="market event_id"))
        object.__setattr__(self, "received_at", _utc(self.received_at, label="received_at"))
        parse_instrument(self.instrument)
        for name in ("bid_price", "ask_price", "bid_depth", "ask_depth"):
            object.__setattr__(
                self,
                name,
                decimal_value(getattr(self, name), label=name, positive=True),
            )
        if self.bid_price > self.ask_price:
            raise ValueError("bid_price cannot exceed ask_price")
        if self.source_sequence is not None and (
            isinstance(self.source_sequence, bool)
            or not isinstance(self.source_sequence, int)
            or self.source_sequence < 0
        ):
            raise ValueError("source_sequence must be a non-negative integer")
        if (
            isinstance(self.capture_ordinal, bool)
            or not isinstance(self.capture_ordinal, int)
            or self.capture_ordinal < 0
        ):
            raise ValueError("capture_ordinal must be a non-negative integer")
        if (self.trade_price is None) != (self.trade_quantity is None):
            raise ValueError("trade_price and trade_quantity must be supplied together")
        if self.trade_price is not None:
            object.__setattr__(
                self,
                "trade_price",
                decimal_value(self.trade_price, label="trade_price", positive=True),
            )
            object.__setattr__(
                self,
                "trade_quantity",
                decimal_value(cast(Decimal, self.trade_quantity), label="trade_quantity", positive=True),
            )
            if self.aggressor_side is None:
                raise ValueError("a trade requires aggressor_side")
            object.__setattr__(self, "aggressor_side", OrderSide(self.aggressor_side))
        elif self.aggressor_side is not None:
            raise ValueError("aggressor_side requires a trade")
        expected_id = self.identifier(
            received_at=self.received_at,
            instrument=self.instrument,
            source_sequence=self.source_sequence,
            capture_ordinal=self.capture_ordinal,
        )
        if self.event_id != expected_id:
            raise ValueError("market event_id does not match its deterministic source identity")

    @classmethod
    def identifier(
        cls,
        *,
        received_at: datetime,
        instrument: str,
        source_sequence: int | None,
        capture_ordinal: int,
    ) -> str:
        if source_sequence is None and capture_ordinal == 0:
            raise ValueError(
                "market identity requires source_sequence or a positive capture_ordinal"
            )
        return deterministic_id(
            "paper_market_event",
            instrument,
            utc_text(received_at),
            source_sequence,
            capture_ordinal,
        )

    @classmethod
    def create(
        cls,
        *,
        received_at: datetime,
        instrument: str,
        bid_price: Decimal,
        ask_price: Decimal,
        bid_depth: Decimal,
        ask_depth: Decimal,
        source_sequence: int | None = None,
        capture_ordinal: int = 0,
        trade_price: Decimal | None = None,
        trade_quantity: Decimal | None = None,
        aggressor_side: OrderSide | str | None = None,
        stale: bool = False,
        gap: bool = False,
        tradable: bool = True,
    ) -> MarketEvent:
        return cls(
            event_id=cls.identifier(
                received_at=received_at,
                instrument=instrument,
                source_sequence=source_sequence,
                capture_ordinal=capture_ordinal,
            ),
            received_at=received_at,
            instrument=instrument,
            bid_price=bid_price,
            ask_price=ask_price,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            source_sequence=source_sequence,
            capture_ordinal=capture_ordinal,
            trade_price=trade_price,
            trade_quantity=trade_quantity,
            aggressor_side=aggressor_side,
            stale=stale,
            gap=gap,
            tradable=tradable,
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "aggressor_side": (
                cast(OrderSide, self.aggressor_side).value if self.aggressor_side else None
            ),
            "ask_depth": decimal_text(self.ask_depth),
            "ask_price": decimal_text(self.ask_price),
            "bid_depth": decimal_text(self.bid_depth),
            "bid_price": decimal_text(self.bid_price),
            "capture_ordinal": self.capture_ordinal,
            "event_id": self.event_id,
            "gap": self.gap,
            "instrument": self.instrument,
            "received_at": utc_text(self.received_at),
            "source_sequence": self.source_sequence,
            "stale": self.stale,
            "tradable": self.tradable,
            "trade_price": decimal_text(self.trade_price) if self.trade_price is not None else None,
            "trade_quantity": (
                decimal_text(self.trade_quantity)
                if self.trade_quantity is not None
                else None
            ),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> MarketEvent:
        return cls(
            event_id=str(value["event_id"]),
            received_at=parse_utc(str(value["received_at"]), label="received_at"),
            instrument=str(value["instrument"]),
            bid_price=decimal_value(str(value["bid_price"]), label="bid_price"),
            ask_price=decimal_value(str(value["ask_price"]), label="ask_price"),
            bid_depth=decimal_value(str(value["bid_depth"]), label="bid_depth"),
            ask_depth=decimal_value(str(value["ask_depth"]), label="ask_depth"),
            source_sequence=(
                int(str(value["source_sequence"]))
                if value.get("source_sequence") is not None
                else None
            ),
            capture_ordinal=int(str(value.get("capture_ordinal", 0))),
            trade_price=(
                decimal_value(str(value["trade_price"]), label="trade_price")
                if value.get("trade_price") is not None
                else None
            ),
            trade_quantity=(
                decimal_value(str(value["trade_quantity"]), label="trade_quantity")
                if value.get("trade_quantity") is not None
                else None
            ),
            aggressor_side=(
                str(value["aggressor_side"])
                if value.get("aggressor_side") is not None
                else None
            ),
            stale=bool(value.get("stale", False)),
            gap=bool(value.get("gap", False)),
            tradable=bool(value.get("tradable", True)),
        )


@dataclass(frozen=True, slots=True)
class PaperEvent:
    event_id: str
    run_id: str
    event_type: PaperEventType | str
    occurred_at: datetime
    received_at: datetime
    causation_id: str | None
    correlation_id: str
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _digest(self.event_id, label="event_id"))
        object.__setattr__(self, "run_id", _digest(self.run_id, label="run_id"))
        object.__setattr__(self, "event_type", PaperEventType(self.event_type))
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, label="occurred_at"))
        object.__setattr__(self, "received_at", _utc(self.received_at, label="received_at"))
        if self.occurred_at < self.received_at:
            raise ValueError("occurred_at cannot precede received_at")
        if self.causation_id is not None:
            object.__setattr__(self, "causation_id", _digest(self.causation_id, label="causation_id"))
        object.__setattr__(
            self,
            "correlation_id",
            _digest(self.correlation_id, label="correlation_id"),
        )
        if not isinstance(self.payload, Mapping):
            raise TypeError("event payload must be a mapping")
        object.__setattr__(self, "payload", frozen_json_mapping(self.payload, label="event.payload"))

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        event_type: PaperEventType | str,
        occurred_at: datetime,
        received_at: datetime,
        causation_id: str | None,
        correlation_id: str,
        payload: Mapping[str, object],
        ordinal: int = 0,
    ) -> PaperEvent:
        normalized_payload = frozen_json_mapping(payload, label="event.payload")
        event_id = deterministic_id(
            "paper_event",
            run_id,
            str(event_type),
            causation_id,
            correlation_id,
            normalized_payload,
            ordinal,
        )
        return cls(
            event_id=event_id,
            run_id=run_id,
            event_type=event_type,
            occurred_at=occurred_at,
            received_at=received_at,
            causation_id=causation_id,
            correlation_id=correlation_id,
            payload=normalized_payload,
        )

    def unsigned_dict(self) -> dict[str, JsonValue]:
        return {
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
            "event_id": self.event_id,
            "event_type": cast(PaperEventType, self.event_type).value,
            "occurred_at": utc_text(self.occurred_at),
            "payload": cast(dict[str, JsonValue], json.loads(canonical_json(self.payload))),
            "received_at": utc_text(self.received_at),
            "run_id": self.run_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> PaperEvent:
        payload = value.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("paper event payload must be an object")
        return cls(
            event_id=str(value["event_id"]),
            run_id=str(value["run_id"]),
            event_type=str(value["event_type"]),
            occurred_at=parse_utc(str(value["occurred_at"]), label="occurred_at"),
            received_at=parse_utc(str(value["received_at"]), label="received_at"),
            causation_id=(
                str(value["causation_id"]) if value.get("causation_id") is not None else None
            ),
            correlation_id=str(value["correlation_id"]),
            payload=cast(Mapping[str, object], payload),
        )

    @property
    def payload_hash(self) -> str:
        return canonical_sha256(self.unsigned_dict())


@dataclass(frozen=True, slots=True)
class StoredPaperEvent:
    event: PaperEvent
    sequence: int
    previous_event_hash: str | None
    event_hash: str

    def __post_init__(self) -> None:
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence <= 0:
            raise ValueError("event sequence must be a positive integer")
        if self.previous_event_hash is not None:
            object.__setattr__(
                self,
                "previous_event_hash",
                _digest(self.previous_event_hash, label="previous_event_hash"),
            )
        object.__setattr__(self, "event_hash", _digest(self.event_hash, label="event_hash"))

    def hash_payload(self) -> dict[str, JsonValue]:
        return {
            **self.event.unsigned_dict(),
            "previous_event_hash": self.previous_event_hash,
            "sequence": self.sequence,
        }

    def verify_hash(self) -> bool:
        return self.event_hash == canonical_sha256(self.hash_payload())


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    entry_id: str
    run_id: str
    event_id: str
    transaction_id: str
    account: str
    amount: Decimal
    currency: str = "USD"

    def __post_init__(self) -> None:
        object.__setattr__(self, "entry_id", _digest(self.entry_id, label="entry_id"))
        object.__setattr__(self, "run_id", _digest(self.run_id, label="run_id"))
        object.__setattr__(self, "event_id", _digest(self.event_id, label="event_id"))
        object.__setattr__(
            self,
            "transaction_id",
            _digest(self.transaction_id, label="transaction_id"),
        )
        object.__setattr__(self, "account", _identifier(self.account, label="ledger account"))
        object.__setattr__(self, "amount", decimal_value(self.amount, label="ledger amount"))
        object.__setattr__(self, "currency", _identifier(self.currency, label="currency"))

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        event_id: str,
        transaction_id: str,
        account: str,
        amount: Decimal,
        ordinal: int,
    ) -> LedgerEntry:
        return cls(
            entry_id=deterministic_id("paper_ledger_entry", transaction_id, account, ordinal),
            run_id=run_id,
            event_id=event_id,
            transaction_id=transaction_id,
            account=account,
            amount=amount,
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "account": self.account,
            "amount": decimal_text(self.amount),
            "currency": self.currency,
            "entry_id": self.entry_id,
            "event_id": self.event_id,
            "run_id": self.run_id,
            "transaction_id": self.transaction_id,
        }


@dataclass(slots=True)
class PaperOrder:
    intent: OrderIntent
    action: DecisionAction
    status: OrderStatus = OrderStatus.PLANNED
    filled_quantity: Decimal = Decimal(0)
    average_fill_price: Decimal | None = None
    fees: Decimal = Decimal(0)
    active_at: datetime | None = None
    expires_at: datetime | None = None
    cancel_effective_at: datetime | None = None
    fill_attempts: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.intent, OrderIntent):
            raise TypeError("paper order intent must be an OrderIntent")
        self.action = DecisionAction(self.action)
        self.status = OrderStatus(self.status)
        self.filled_quantity = decimal_value(
            self.filled_quantity, label="filled_quantity", non_negative=True
        )
        if self.filled_quantity > self.intent.quantity:
            raise ValueError("filled_quantity cannot exceed requested quantity")
        if self.average_fill_price is not None:
            self.average_fill_price = decimal_value(
                self.average_fill_price, label="average_fill_price", positive=True
            )
        self.fees = decimal_value(self.fees, label="fees")
        for name in ("active_at", "expires_at", "cancel_effective_at"):
            timestamp = getattr(self, name)
            if timestamp is not None:
                setattr(self, name, _utc(timestamp, label=name))
        if (
            isinstance(self.fill_attempts, bool)
            or not isinstance(self.fill_attempts, int)
            or self.fill_attempts < 0
        ):
            raise ValueError("fill_attempts must be a non-negative integer")

    @property
    def remaining_quantity(self) -> Decimal:
        return max(Decimal(0), self.intent.quantity - self.filled_quantity)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "action": self.action.value,
            "active_at": utc_text(self.active_at) if self.active_at is not None else None,
            "average_fill_price": (
                decimal_text(self.average_fill_price) if self.average_fill_price is not None else None
            ),
            "cancel_effective_at": (
                utc_text(self.cancel_effective_at) if self.cancel_effective_at is not None else None
            ),
            "expires_at": utc_text(self.expires_at) if self.expires_at is not None else None,
            "fees": decimal_text(self.fees),
            "fill_attempts": self.fill_attempts,
            "filled_quantity": decimal_text(self.filled_quantity),
            "intent": self.intent.to_dict(),
            "status": self.status.value,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> PaperOrder:
        intent = value.get("intent")
        if not isinstance(intent, Mapping):
            raise ValueError("paper order lacks its intent")
        return cls(
            intent=OrderIntent.from_dict(intent),
            action=DecisionAction(str(value["action"])),
            status=OrderStatus(str(value["status"])),
            filled_quantity=decimal_value(
                str(value.get("filled_quantity", "0")), label="filled_quantity", non_negative=True
            ),
            average_fill_price=(
                decimal_value(str(value["average_fill_price"]), label="average_fill_price", positive=True)
                if value.get("average_fill_price") is not None
                else None
            ),
            fees=decimal_value(str(value.get("fees", "0")), label="fees"),
            active_at=parse_utc(str(value["active_at"])) if value.get("active_at") else None,
            expires_at=parse_utc(str(value["expires_at"])) if value.get("expires_at") else None,
            cancel_effective_at=(
                parse_utc(str(value["cancel_effective_at"]))
                if value.get("cancel_effective_at")
                else None
            ),
            fill_attempts=int(str(value.get("fill_attempts", 0))),
        )


@dataclass(slots=True)
class PaperProjection:
    run_id: str
    config_hash: str
    initial_cash: Decimal
    state: PaperState = PaperState.FLAT
    state_since: datetime | None = None
    suspended_from: PaperState | None = None
    # NaN is an internal constructor sentinel only.  Zero is a valid durable
    # balance and must survive serialization/replay without being mistaken for
    # an omitted initial value.
    cash: Decimal = Decimal("NaN")
    fees: Decimal = Decimal(0)
    realized_pnl: Decimal = Decimal(0)
    positions: dict[str, Decimal] = field(default_factory=dict)
    cost_basis: dict[str, Decimal] = field(default_factory=dict)
    inventory_value: dict[str, Decimal] = field(default_factory=dict)
    marks: dict[str, Decimal] = field(default_factory=dict)
    orders: dict[str, PaperOrder] = field(default_factory=dict)
    decisions: int = 0
    completed_cycles: int = 0
    current_entry_decision_id: str | None = None
    current_exit_decision_id: str | None = None
    peak_equity: Decimal = Decimal("NaN")
    session_start_equity: Decimal = Decimal("NaN")
    session_date: str | None = None
    critical_incidents: list[datetime] = field(default_factory=list)
    last_received_at: datetime | None = None
    last_market_received_at: datetime | None = None
    last_market_received_at_by_instrument: dict[str, datetime] = field(
        default_factory=dict
    )
    last_sequence: int = 0
    last_event_hash: str | None = None
    reconciled: bool = True

    def __post_init__(self) -> None:
        self.run_id = _digest(self.run_id, label="run_id")
        self.config_hash = _digest(self.config_hash, label="config_hash")
        self.state = PaperState(self.state)
        if self.suspended_from is not None:
            self.suspended_from = PaperState(self.suspended_from)
        if self.state_since is not None:
            self.state_since = _utc(self.state_since, label="state_since")
        self.initial_cash = decimal_value(self.initial_cash, label="initial_cash", positive=True)
        if isinstance(self.cash, Decimal) and self.cash.is_nan():
            self.cash = self.initial_cash
        self.cash = decimal_value(self.cash, label="cash")
        self.fees = decimal_value(self.fees, label="fees")
        self.realized_pnl = decimal_value(self.realized_pnl, label="realized_pnl")
        if isinstance(self.peak_equity, Decimal) and self.peak_equity.is_nan():
            self.peak_equity = self.initial_cash
        if (
            isinstance(self.session_start_equity, Decimal)
            and self.session_start_equity.is_nan()
        ):
            self.session_start_equity = self.initial_cash
        self.peak_equity = decimal_value(self.peak_equity, label="peak_equity")
        self.session_start_equity = decimal_value(
            self.session_start_equity, label="session_start_equity"
        )
        if self.session_date is not None:
            try:
                datetime.fromisoformat(f"{self.session_date}T00:00:00+00:00")
            except ValueError as error:
                raise ValueError("session_date must use YYYY-MM-DD") from error
        self.positions = {
            instrument: decimal_value(quantity, label=f"positions.{instrument}")
            for instrument, quantity in self.positions.items()
            if decimal_value(quantity, label=f"positions.{instrument}") != 0
        }
        self.cost_basis = {
            instrument: decimal_value(value, label=f"cost_basis.{instrument}", positive=True)
            for instrument, value in self.cost_basis.items()
        }
        if set(self.cost_basis) != set(self.positions):
            raise ValueError("cost_basis keys must exactly match open position keys")
        if not self.inventory_value and self.positions:
            self.inventory_value = {
                instrument: quantity * self.cost_basis[instrument]
                for instrument, quantity in self.positions.items()
            }
        else:
            self.inventory_value = {
                instrument: decimal_value(value, label=f"inventory_value.{instrument}")
                for instrument, value in self.inventory_value.items()
            }
        if set(self.inventory_value) != set(self.positions):
            raise ValueError("inventory_value keys must exactly match open position keys")
        for instrument, quantity in self.positions.items():
            if (self.inventory_value[instrument] > 0) != (quantity > 0):
                raise ValueError(f"inventory_value sign differs from position for {instrument}")
        self.marks = {
            instrument: decimal_value(value, label=f"marks.{instrument}", positive=True)
            for instrument, value in self.marks.items()
        }
        if any(key != order.intent.order_id for key, order in self.orders.items()):
            raise ValueError("paper order projection keys must equal their deterministic order IDs")
        if self.current_entry_decision_id is not None:
            self.current_entry_decision_id = _digest(
                self.current_entry_decision_id,
                label="current_entry_decision_id",
            )
        if self.current_exit_decision_id is not None:
            self.current_exit_decision_id = _digest(
                self.current_exit_decision_id,
                label="current_exit_decision_id",
            )
        self.critical_incidents = [
            _utc(value, label="critical incident") for value in self.critical_incidents
        ]
        if self.last_received_at is not None:
            self.last_received_at = _utc(self.last_received_at, label="last_received_at")
        if self.last_market_received_at is not None:
            self.last_market_received_at = _utc(
                self.last_market_received_at,
                label="last_market_received_at",
            )
        self.last_market_received_at_by_instrument = {
            instrument: _utc(received_at, label=f"last market {instrument}")
            for instrument, received_at in self.last_market_received_at_by_instrument.items()
        }
        if self.last_market_received_at_by_instrument:
            latest_market = max(self.last_market_received_at_by_instrument.values())
            if self.last_market_received_at is None:
                self.last_market_received_at = latest_market
            elif self.last_market_received_at != latest_market:
                raise ValueError(
                    "last_market_received_at must equal the latest per-instrument market time"
                )
        if (
            isinstance(self.last_sequence, bool)
            or not isinstance(self.last_sequence, int)
            or self.last_sequence < 0
        ):
            raise ValueError("last_sequence must be a non-negative integer")
        if self.last_event_hash is not None:
            self.last_event_hash = _digest(self.last_event_hash, label="last_event_hash")
        if self.last_event_hash is None and self.last_sequence == 0:
            self.last_event_hash = event_genesis_hash(self.run_id, self.config_hash)

    @property
    def equity(self) -> Decimal:
        marked = sum(
            (quantity * self.marks.get(instrument, Decimal(0)))
            for instrument, quantity in self.positions.items()
        )
        return self.cash + marked

    @property
    def net_pnl(self) -> Decimal:
        return self.equity - self.initial_cash

    @property
    def active_orders(self) -> tuple[PaperOrder, ...]:
        return tuple(order for order in self.orders.values() if order.status.active)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "cash": decimal_text(self.cash),
            "completed_cycles": self.completed_cycles,
            "config_hash": self.config_hash,
            "cost_basis": {
                key: decimal_text(value) for key, value in sorted(self.cost_basis.items())
            },
            "critical_incidents": [utc_text(value) for value in self.critical_incidents],
            "current_entry_decision_id": self.current_entry_decision_id,
            "current_exit_decision_id": self.current_exit_decision_id,
            "decisions": self.decisions,
            "fees": decimal_text(self.fees),
            "initial_cash": decimal_text(self.initial_cash),
            "inventory_value": {
                key: decimal_text(value) for key, value in sorted(self.inventory_value.items())
            },
            "last_event_hash": self.last_event_hash,
            "last_market_received_at": (
                utc_text(self.last_market_received_at)
                if self.last_market_received_at is not None
                else None
            ),
            "last_market_received_at_by_instrument": {
                instrument: utc_text(received_at)
                for instrument, received_at in sorted(
                    self.last_market_received_at_by_instrument.items()
                )
            },
            "last_received_at": (
                utc_text(self.last_received_at) if self.last_received_at is not None else None
            ),
            "last_sequence": self.last_sequence,
            "marks": {key: decimal_text(value) for key, value in sorted(self.marks.items())},
            "orders": {key: value.to_dict() for key, value in sorted(self.orders.items())},
            "peak_equity": decimal_text(self.peak_equity),
            "positions": {
                key: decimal_text(value) for key, value in sorted(self.positions.items())
            },
            "realized_pnl": decimal_text(self.realized_pnl),
            "reconciled": self.reconciled,
            "run_id": self.run_id,
            "schema_version": 1,
            "session_start_equity": decimal_text(self.session_start_equity),
            "session_date": self.session_date,
            "state": self.state.value,
            "state_since": utc_text(self.state_since) if self.state_since is not None else None,
            "suspended_from": self.suspended_from.value if self.suspended_from else None,
        }

    @property
    def canonical_hash(self) -> str:
        return canonical_sha256(self.to_dict())

    def clone(self) -> PaperProjection:
        return PaperProjection.from_dict(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> PaperProjection:
        def decimal_map(name: str) -> dict[str, Decimal]:
            raw = value.get(name, {})
            if not isinstance(raw, Mapping):
                raise ValueError(f"projection {name} must be an object")
            return {str(key): decimal_value(str(item), label=f"{name}.{key}") for key, item in raw.items()}

        raw_orders = value.get("orders", {})
        if not isinstance(raw_orders, Mapping):
            raise ValueError("projection orders must be an object")
        return cls(
            run_id=str(value["run_id"]),
            config_hash=str(value["config_hash"]),
            initial_cash=decimal_value(str(value["initial_cash"]), label="initial_cash"),
            state=PaperState(str(value.get("state", "FLAT"))),
            state_since=(
                parse_utc(str(value["state_since"]))
                if value.get("state_since") is not None
                else None
            ),
            suspended_from=(
                PaperState(str(value["suspended_from"]))
                if value.get("suspended_from") is not None
                else None
            ),
            cash=decimal_value(str(value.get("cash", value["initial_cash"])), label="cash"),
            fees=decimal_value(str(value.get("fees", "0")), label="fees"),
            realized_pnl=decimal_value(
                str(value.get("realized_pnl", "0")), label="realized_pnl"
            ),
            positions=decimal_map("positions"),
            cost_basis=decimal_map("cost_basis"),
            inventory_value=decimal_map("inventory_value"),
            marks=decimal_map("marks"),
            orders={
                str(key): PaperOrder.from_dict(cast(Mapping[str, object], item))
                for key, item in raw_orders.items()
                if isinstance(item, Mapping)
            },
            decisions=int(str(value.get("decisions", 0))),
            completed_cycles=int(str(value.get("completed_cycles", 0))),
            current_entry_decision_id=(
                str(value["current_entry_decision_id"])
                if value.get("current_entry_decision_id") is not None
                else None
            ),
            current_exit_decision_id=(
                str(value["current_exit_decision_id"])
                if value.get("current_exit_decision_id") is not None
                else None
            ),
            peak_equity=decimal_value(
                str(value.get("peak_equity", value["initial_cash"])), label="peak_equity"
            ),
            session_start_equity=decimal_value(
                str(value.get("session_start_equity", value["initial_cash"])),
                label="session_start_equity",
            ),
            session_date=(str(value["session_date"]) if value.get("session_date") else None),
            critical_incidents=[
                parse_utc(str(item)) for item in cast(Sequence[object], value.get("critical_incidents", []))
            ],
            last_received_at=(
                parse_utc(str(value["last_received_at"]))
                if value.get("last_received_at") is not None
                else None
            ),
            last_market_received_at=(
                parse_utc(str(value["last_market_received_at"]))
                if value.get("last_market_received_at") is not None
                else None
            ),
            last_market_received_at_by_instrument={
                str(instrument): parse_utc(str(received_at))
                for instrument, received_at in cast(
                    Mapping[str, object],
                    value.get("last_market_received_at_by_instrument", {}),
                ).items()
            },
            last_sequence=int(str(value.get("last_sequence", 0))),
            last_event_hash=(
                str(value["last_event_hash"]) if value.get("last_event_hash") is not None else None
            ),
            reconciled=bool(value.get("reconciled", True)),
        )


def input_payload_hash(payload: Mapping[str, object]) -> str:
    return canonical_sha256(_json_value(payload, path="input"))


def ensure_json_object(value: Mapping[str, object]) -> dict[str, JsonValue]:
    normalized = _json_value(value)
    if not isinstance(normalized, dict):
        raise TypeError("value must normalize to a JSON object")
    return normalized


__all__ = [
    "AlertSeverity",
    "DecisionAction",
    "DecisionIntent",
    "LedgerEntry",
    "MarketEvent",
    "OrderIntent",
    "OrderSide",
    "OrderStatus",
    "PaperEvent",
    "PaperEventType",
    "PaperExecutionConfig",
    "PaperOrder",
    "PaperOrderType",
    "PaperProjection",
    "PaperRiskLimits",
    "PaperRunConfig",
    "PaperState",
    "StoredPaperEvent",
    "TimeInForce",
    "decimal_text",
    "decimal_value",
    "deterministic_id",
    "ensure_json_object",
    "event_genesis_hash",
    "input_payload_hash",
    "keyed_uniform",
    "legal_transition",
    "parse_utc",
    "require_transition",
    "utc_text",
]
