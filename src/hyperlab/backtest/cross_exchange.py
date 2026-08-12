from __future__ import annotations

import html
import json
import math
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

PriceSource = Literal["mark", "oracle"]


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value.casefold())
    )


def _finite(value: float, *, name: str, minimum: float | None = None) -> None:
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")


@dataclass(frozen=True, slots=True)
class FundingCalendar:
    """UTC settlement calendar, periodic or explicitly observed.

    Explicit timestamps are useful when a venue changes an instrument's cadence.
    The simulator never silently assumes an eight-hour schedule.
    """

    interval_hours: int | None
    anchor_hour_utc: int = 0
    explicit_settlements: tuple[pd.Timestamp, ...] = ()

    def __post_init__(self) -> None:
        if self.interval_hours is None and not self.explicit_settlements:
            raise ValueError("a funding calendar needs an interval or explicit settlements")
        if self.interval_hours is not None and (
            self.interval_hours <= 0 or 24 % self.interval_hours != 0
        ):
            raise ValueError("interval_hours must be a positive divisor of 24")
        if not 0 <= self.anchor_hour_utc <= 23:
            raise ValueError("anchor_hour_utc must be between 0 and 23")
        normalized: list[pd.Timestamp] = []
        for raw in self.explicit_settlements:
            timestamp = pd.Timestamp(raw)
            if timestamp.tz is None or timestamp.utcoffset() != pd.Timedelta(0):
                raise ValueError("explicit funding settlements must use UTC")
            normalized.append(timestamp.tz_convert("UTC"))
        if len(normalized) != len(set(normalized)) or normalized != sorted(normalized):
            raise ValueError("explicit funding settlements must be unique and sorted")
        object.__setattr__(self, "explicit_settlements", tuple(normalized))

    def settles_at(self, timestamp: pd.Timestamp) -> bool:
        resolved = pd.Timestamp(timestamp)
        if resolved.tz is None or resolved.utcoffset() != pd.Timedelta(0):
            raise ValueError("funding timestamps must use UTC")
        resolved = resolved.tz_convert("UTC")
        if self.explicit_settlements:
            return resolved in self.explicit_settlements
        assert self.interval_hours is not None
        return bool(
            resolved.minute == 0
            and resolved.second == 0
            and resolved.microsecond == 0
            and (resolved.hour - self.anchor_hour_utc) % self.interval_hours == 0
        )

    def period_hours_at(self, timestamp: pd.Timestamp) -> float:
        if self.interval_hours is not None:
            return float(self.interval_hours)
        resolved = pd.Timestamp(timestamp).tz_convert("UTC")
        position = self.explicit_settlements.index(resolved)
        if position == 0:
            if len(self.explicit_settlements) < 2:
                raise ValueError("two explicit settlements are required to infer the first period")
            previous = self.explicit_settlements[0]
            following = self.explicit_settlements[1]
        else:
            previous = self.explicit_settlements[position - 1]
            following = self.explicit_settlements[position]
        hours = (following - previous).total_seconds() / 3_600.0
        if hours <= 0.0:
            raise ValueError("explicit funding settlement period must be positive")
        return hours


@dataclass(frozen=True, slots=True)
class FundingConvention:
    venue: str
    calendar: FundingCalendar
    notional_price_source: PriceSource
    formula_name: str
    documentation_url: str

    def __post_init__(self) -> None:
        if not self.venue.strip():
            raise ValueError("funding convention venue cannot be empty")
        if self.notional_price_source not in {"mark", "oracle"}:
            raise ValueError("notional_price_source must be mark or oracle")
        if not self.formula_name.strip() or not self.documentation_url.startswith("https://"):
            raise ValueError("funding convention needs a formula name and public documentation URL")

    def settlement_rate(self, observed_rate: float) -> float:
        """Return the realized per-settlement rate without fabricating missing inputs."""

        if not math.isfinite(observed_rate):
            raise ValueError(f"{self.venue} funding settlement rate must be finite")
        return observed_rate

    def payment(
        self,
        *,
        quantity: float,
        mark_price: float,
        oracle_price: float,
        observed_rate: float,
    ) -> float:
        price = mark_price if self.notional_price_source == "mark" else oracle_price
        return -quantity * price * self.settlement_rate(observed_rate)


def hyperliquid_hourly_rate_from_premium(
    premium: float,
    *,
    interest_rate_per_8h: float = 0.0001,
) -> float:
    """Official vanilla-perp formula, converted from its 8 h form to one hour.

    Real simulations still consume realized settlements. This helper documents and
    tests the venue formula without reconstructing premium samples unavailable in a
    bar-level dataset.
    """

    _finite(premium, name="premium")
    _finite(interest_rate_per_8h, name="interest_rate_per_8h")
    clamped = min(max(interest_rate_per_8h - premium, -0.0005), 0.0005)
    return float(np.clip((premium + clamped) / 8.0, -0.04, 0.04))


def default_funding_conventions() -> dict[str, FundingConvention]:
    return {
        "HL": FundingConvention(
            venue="HL",
            calendar=FundingCalendar(interval_hours=1),
            notional_price_source="oracle",
            formula_name="hyperliquid_realized_hourly_clamped_premium",
            documentation_url="https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding",
        ),
        "BINANCE_USDM": FundingConvention(
            venue="BINANCE_USDM",
            calendar=FundingCalendar(interval_hours=8),
            notional_price_source="mark",
            formula_name="binance_published_realized_per_settlement_rate",
            documentation_url=(
                "https://developers.binance.com/docs/derivatives/usds-margined-futures/"
                "market-data/rest-api/Get-Funding-Rate-History"
            ),
        ),
    }


def default_cross_venue_risk_rules() -> dict[str, VenueRiskRule]:
    """Conservative, visibly uncalibrated demo assumptions for both venues."""

    return {
        "HL": VenueRiskRule(
            venue="HL",
            initial_margin_fraction=0.20,
            maintenance_margin_fraction=0.10,
            fee_bps=1.5,
            slippage_bps=1.0,
            liquidation_penalty_bps=20.0,
        ),
        "BINANCE_USDM": VenueRiskRule(
            venue="BINANCE_USDM",
            initial_margin_fraction=0.20,
            maintenance_margin_fraction=0.10,
            fee_bps=2.0,
            slippage_bps=1.0,
            liquidation_penalty_bps=25.0,
        ),
    }


def default_cross_venue_config() -> CrossVenueConfig:
    return CrossVenueConfig(
        initial_capital_by_venue={"HL": 50_000.0, "BINANCE_USDM": 50_000.0},
        target_notional_usd=40_000.0,
    )


def venue_risk_rules_from_metadata(
    metadata: dict[str, object],
) -> dict[str, VenueRiskRule]:
    """Load explicit risk inputs, falling back only to uncalibrated demo rules."""

    raw_rules = metadata.get("venue_risk_rules")
    if raw_rules is None:
        return default_cross_venue_risk_rules()
    if not isinstance(raw_rules, dict) or len(raw_rules) != 2:
        raise ValueError("venue_risk_rules metadata must contain exactly two venues")
    rules: dict[str, VenueRiskRule] = {}
    required = (
        "initial_margin_fraction",
        "maintenance_margin_fraction",
        "fee_bps",
        "slippage_bps",
        "liquidation_penalty_bps",
        "calibration_evidence_hash",
    )
    for venue, raw_rule in raw_rules.items():
        if not isinstance(venue, str) or not isinstance(raw_rule, dict):
            raise ValueError("venue_risk_rules entries must be named objects")
        missing = [name for name in required if name not in raw_rule]
        if missing:
            raise ValueError(f"venue_risk_rules[{venue}] missing fields: {missing}")
        rules[venue] = VenueRiskRule(
            venue=venue,
            initial_margin_fraction=float(raw_rule["initial_margin_fraction"]),
            maintenance_margin_fraction=float(raw_rule["maintenance_margin_fraction"]),
            fee_bps=float(raw_rule["fee_bps"]),
            slippage_bps=float(raw_rule["slippage_bps"]),
            liquidation_penalty_bps=float(raw_rule["liquidation_penalty_bps"]),
            calibration_evidence_hash=str(raw_rule["calibration_evidence_hash"]),
        )
    return rules


@dataclass(frozen=True, slots=True)
class VenueRiskRule:
    venue: str
    initial_margin_fraction: float
    maintenance_margin_fraction: float
    fee_bps: float
    slippage_bps: float
    liquidation_penalty_bps: float
    calibration_evidence_hash: str = ""

    def __post_init__(self) -> None:
        if not self.venue.strip():
            raise ValueError("venue risk rule venue cannot be empty")
        for name, value in (
            ("initial_margin_fraction", self.initial_margin_fraction),
            ("maintenance_margin_fraction", self.maintenance_margin_fraction),
            ("fee_bps", self.fee_bps),
            ("slippage_bps", self.slippage_bps),
            ("liquidation_penalty_bps", self.liquidation_penalty_bps),
        ):
            _finite(value, name=name, minimum=0.0)
        if self.initial_margin_fraction <= 0.0:
            raise ValueError("initial_margin_fraction must be positive")
        if self.maintenance_margin_fraction >= self.initial_margin_fraction:
            raise ValueError("maintenance margin must be below initial margin")


@dataclass(frozen=True, slots=True)
class CrossVenueConfig:
    initial_capital_by_venue: dict[str, float]
    target_notional_usd: float = 20_000.0
    lookback_hours: int = 24
    min_funding_edge_hourly: float = 0.000005
    position_rebalance_hours: int = 8
    position_rebalance_tolerance_fraction: float = 0.05
    collateral_rebalance_trigger_fraction: float = 0.20
    collateral_rebalance_target_fraction: float = 0.40
    transfer_delay_hours: int = 1
    transfer_fee_bps: float = 5.0
    transfer_fixed_fee_usd: float = 1.0
    enable_collateral_rebalancing: bool = True
    halt_after_liquidation: bool = True
    uncovered_tolerance_quantity: float = 1e-10

    def __post_init__(self) -> None:
        if len(self.initial_capital_by_venue) != 2:
            raise ValueError("initial capital must be allocated to exactly two venues")
        for venue, capital in self.initial_capital_by_venue.items():
            if not venue.strip():
                raise ValueError("initial capital venue cannot be empty")
            _finite(capital, name=f"initial capital for {venue}", minimum=0.0)
            if capital <= 0.0:
                raise ValueError("initial capital by venue must be positive")
        _finite(self.target_notional_usd, name="target_notional_usd", minimum=0.0)
        if self.target_notional_usd <= 0.0:
            raise ValueError("target_notional_usd must be positive")
        if self.lookback_hours <= 0 or self.position_rebalance_hours <= 0:
            raise ValueError("lookback and position rebalance hours must be positive")
        if self.transfer_delay_hours < 0:
            raise ValueError("transfer_delay_hours cannot be negative")
        for name, value in (
            ("min_funding_edge_hourly", self.min_funding_edge_hourly),
            ("position_rebalance_tolerance_fraction", self.position_rebalance_tolerance_fraction),
            (
                "collateral_rebalance_trigger_fraction",
                self.collateral_rebalance_trigger_fraction,
            ),
            (
                "collateral_rebalance_target_fraction",
                self.collateral_rebalance_target_fraction,
            ),
            ("transfer_fee_bps", self.transfer_fee_bps),
            ("transfer_fixed_fee_usd", self.transfer_fixed_fee_usd),
            ("uncovered_tolerance_quantity", self.uncovered_tolerance_quantity),
        ):
            _finite(value, name=name, minimum=0.0)
        if (
            self.collateral_rebalance_target_fraction
            < self.collateral_rebalance_trigger_fraction
        ):
            raise ValueError("collateral rebalance target must be at least its trigger")


@dataclass(frozen=True, slots=True)
class CrossVenueMarketData:
    asset: str
    mark_prices: pd.DataFrame
    oracle_prices: pd.DataFrame
    funding_rates: pd.DataFrame
    metadata: dict[str, object] = field(default_factory=dict)
    venue_available: pd.DataFrame | None = None
    transfers_available: pd.Series | None = None

    @property
    def venues(self) -> tuple[str, ...]:
        return tuple(str(column) for column in self.mark_prices.columns)

    def resolved_venue_available(self) -> pd.DataFrame:
        if self.venue_available is not None:
            return self.venue_available.astype(bool).copy()
        return pd.DataFrame(True, index=self.mark_prices.index, columns=self.mark_prices.columns)

    def resolved_transfers_available(self) -> pd.Series:
        if self.transfers_available is not None:
            return self.transfers_available.astype(bool).copy()
        return pd.Series(True, index=self.mark_prices.index, dtype=bool)

    def validate(self, conventions: dict[str, FundingConvention]) -> None:
        if not self.asset.strip():
            raise ValueError("cross-venue asset cannot be empty")
        if self.mark_prices.empty or len(self.mark_prices.columns) != 2:
            raise ValueError("cross-venue data needs exactly two venues")
        index = self.mark_prices.index
        if not isinstance(index, pd.DatetimeIndex):
            raise TypeError("cross-venue index must be a DatetimeIndex")
        if index.tz is None or str(index.tz).upper() not in {"UTC", "UTC+00:00"}:
            raise ValueError("cross-venue timestamps must use UTC")
        if not index.is_monotonic_increasing or index.has_duplicates:
            raise ValueError("cross-venue timestamps must be sorted and unique")
        if len(index) > 1 and not bool(
            index.to_series().diff().dropna().eq(pd.Timedelta(hours=1)).all()
        ):
            raise ValueError("cross-venue simulation requires a regular hourly grid")
        columns = list(self.mark_prices.columns)
        if set(columns) != set(conventions):
            raise ValueError("funding conventions must exactly match market venues")
        for name, frame in (
            ("oracle_prices", self.oracle_prices),
            ("funding_rates", self.funding_rates),
        ):
            if not frame.index.equals(index) or list(frame.columns) != columns:
                raise ValueError(f"{name} must align exactly with mark_prices")
        for name, frame in (
            ("mark_prices", self.mark_prices),
            ("oracle_prices", self.oracle_prices),
        ):
            numeric = frame.apply(pd.to_numeric, errors="coerce")
            if bool((~numeric.map(math.isfinite) | numeric.le(0.0)).any(axis=None)):
                raise ValueError(f"{name} values must be finite and positive")
        supplied_funding = self.funding_rates.notna()
        numeric_funding = self.funding_rates.apply(pd.to_numeric, errors="coerce")
        if bool((supplied_funding & ~numeric_funding.map(math.isfinite)).any(axis=None)):
            raise ValueError("supplied funding rates must be finite")
        if self.venue_available is not None and (
            not self.venue_available.index.equals(index)
            or list(self.venue_available.columns) != columns
        ):
            raise ValueError("venue_available must align exactly with mark_prices")
        if self.transfers_available is not None and not self.transfers_available.index.equals(index):
            raise ValueError("transfers_available must align exactly with mark_prices")


@dataclass(frozen=True, slots=True)
class CrossVenueMetrics:
    gross_return_on_total_capital: float
    return_on_total_capital: float
    return_by_venue: dict[str, float]
    max_drawdown: float
    worst_hour: float
    turnover: float
    time_in_market: float
    max_gross_exposure: float
    max_net_exposure: float
    worst_local_margin_deficit_usd: dict[str, float]
    minimum_free_margin_usd: dict[str, float]
    max_capital_immobilized_usd: dict[str, float]
    uncovered_hours: float
    rebalancing_cost_usd: float
    blocked_transfer_hours: float
    liquidation_count_by_venue: dict[str, int]
    outage_hours_by_venue: dict[str, float]
    funding_pnl_by_venue: dict[str, float]
    price_pnl_by_venue: dict[str, float]
    fee_by_venue: dict[str, float]
    slippage_by_venue: dict[str, float]
    liquidation_penalty_by_venue: dict[str, float]
    execution_cost_by_venue: dict[str, float]


@dataclass(slots=True)
class CrossVenueResult:
    asset: str
    timeline: pd.DataFrame
    trades: pd.DataFrame
    transfers: pd.DataFrame
    liquidations: pd.DataFrame
    metrics: CrossVenueMetrics
    diagnostics: dict[str, object]


@dataclass(frozen=True, slots=True)
class CrossVenueDataAudit:
    observed_hours: int
    minimum_history_hours: int
    asset: str
    venues: tuple[str, ...]
    checks: dict[str, bool]
    reasons: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return all(self.checks.values())


@dataclass(slots=True)
class CrossExchangeValidation:
    audit: CrossVenueDataAudit
    scenarios: dict[str, CrossVenueResult]
    status: str
    failed_venue: str
    outage_start: pd.Timestamp
    conventions: dict[str, FundingConvention]
    risk_rules: dict[str, VenueRiskRule]
    config: CrossVenueConfig


@dataclass(slots=True)
class _PendingTransfer:
    initiated_at: pd.Timestamp
    due_index: int
    source: str
    destination: str
    amount_usd: float
    fee_usd: float


def _empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _position_metrics(
    venue: str,
    *,
    cash: dict[str, float],
    quantity: dict[str, float],
    mark: dict[str, float],
    risk_rules: dict[str, VenueRiskRule],
) -> tuple[float, float, float, float]:
    notional = abs(quantity[venue]) * mark[venue]
    initial_margin = notional * risk_rules[venue].initial_margin_fraction
    maintenance_margin = notional * risk_rules[venue].maintenance_margin_fraction
    free_margin = cash[venue] - initial_margin
    return notional, initial_margin, maintenance_margin, free_margin if quantity[venue] else cash[venue]


def simulate_cross_exchange_funding(
    data: CrossVenueMarketData,
    *,
    conventions: dict[str, FundingConvention],
    risk_rules: dict[str, VenueRiskRule],
    config: CrossVenueConfig,
) -> CrossVenueResult:
    """Simulate two linear-perp legs with isolated venue capital and no order route."""

    data.validate(conventions)
    venues = data.venues
    if set(risk_rules) != set(venues):
        raise ValueError("venue risk rules must exactly match market venues")
    if set(config.initial_capital_by_venue) != set(venues):
        raise ValueError("initial capital must exactly match market venues")
    for venue, rule in risk_rules.items():
        if rule.venue != venue:
            raise ValueError("venue risk rule key and venue must match")

    index = data.mark_prices.index
    availability = data.resolved_venue_available()
    transfer_availability = data.resolved_transfers_available()
    hourly_rates = data.funding_rates.copy().astype(float)
    for venue in venues:
        convention = conventions[venue]
        settlement_hours = pd.Series(np.nan, index=index, dtype=float)
        for timestamp in index:
            if convention.calendar.settles_at(timestamp):
                rate = hourly_rates.at[timestamp, venue]
                if not pd.isna(rate):
                    settlement_hours.at[timestamp] = float(
                        cast(Any, rate)
                    ) / convention.calendar.period_hours_at(timestamp)
        hourly_rates[venue] = settlement_hours
    signal_rates = hourly_rates.ffill().rolling(
        config.lookback_hours,
        min_periods=1,
    ).mean()

    cash = {venue: float(config.initial_capital_by_venue[venue]) for venue in venues}
    quantity = dict.fromkeys(venues, 0.0)
    price_pnl = dict.fromkeys(venues, 0.0)
    funding_pnl = dict.fromkeys(venues, 0.0)
    execution_cost = dict.fromkeys(venues, 0.0)
    fee_cost = dict.fromkeys(venues, 0.0)
    slippage_cost = dict.fromkeys(venues, 0.0)
    liquidation_cost = dict.fromkeys(venues, 0.0)
    venue_economic_pnl = dict.fromkeys(venues, 0.0)
    worst_deficit = dict.fromkeys(venues, 0.0)
    minimum_free_margin = {venue: math.inf for venue in venues}
    max_initial_margin = dict.fromkeys(venues, 0.0)
    liquidation_counts = dict.fromkeys(venues, 0)
    pending: list[_PendingTransfer] = []
    timeline_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    transfer_rows: list[dict[str, object]] = []
    liquidation_rows: list[dict[str, object]] = []
    incident_halt = False
    uncovered_hours = 0.0
    blocked_transfer_hours = 0.0
    rebalancing_cost = 0.0
    trade_notional_total = 0.0
    previous_mark = {venue: float(data.mark_prices.iloc[0][venue]) for venue in venues}

    def execute_trade(
        *,
        timestamp: pd.Timestamp,
        venue: str,
        target_quantity: float,
        reason: str,
        liquidation: bool = False,
    ) -> None:
        nonlocal trade_notional_total
        delta = target_quantity - quantity[venue]
        if abs(delta) <= config.uncovered_tolerance_quantity:
            quantity[venue] = target_quantity
            return
        current_mark = float(cast(Any, data.mark_prices.at[timestamp, venue]))
        notional = abs(delta) * current_mark
        rule = risk_rules[venue]
        fee = notional * rule.fee_bps / 10_000.0
        slippage = notional * rule.slippage_bps / 10_000.0
        penalty = notional * rule.liquidation_penalty_bps / 10_000.0 if liquidation else 0.0
        total_cost = fee + slippage + penalty
        cash[venue] -= total_cost
        execution_cost[venue] += total_cost
        fee_cost[venue] += fee
        slippage_cost[venue] += slippage
        liquidation_cost[venue] += penalty
        trade_notional_total += notional
        venue_economic_pnl[venue] -= total_cost
        quantity[venue] = target_quantity
        trade_rows.append(
            {
                "timestamp": timestamp,
                "venue": venue,
                "delta_quantity": delta,
                "target_quantity": target_quantity,
                "mark_price": current_mark,
                "notional_usd": notional,
                "fee_usd": fee,
                "slippage_usd": slippage,
                "liquidation_penalty_usd": penalty,
                "reason": reason,
            }
        )

    def can_hold_targets(
        timestamp: pd.Timestamp,
        targets: dict[str, float],
    ) -> bool:
        for venue in venues:
            mark_price = float(cast(Any, data.mark_prices.at[timestamp, venue]))
            delta_notional = abs(targets[venue] - quantity[venue]) * mark_price
            rule = risk_rules[venue]
            projected_cost = delta_notional * (rule.fee_bps + rule.slippage_bps) / 10_000.0
            projected_cash = cash[venue] - projected_cost
            projected_margin = (
                abs(targets[venue]) * mark_price * rule.initial_margin_fraction
            )
            if projected_cash + 1e-12 < projected_margin:
                return False
        return True

    for row_number, timestamp in enumerate(index):
        mark = {
            venue: float(cast(Any, data.mark_prices.at[timestamp, venue])) for venue in venues
        }
        oracle = {
            venue: float(cast(Any, data.oracle_prices.at[timestamp, venue])) for venue in venues
        }
        row_price_pnl = dict.fromkeys(venues, 0.0)
        row_funding_pnl = dict.fromkeys(venues, 0.0)

        remaining_pending: list[_PendingTransfer] = []
        for transfer in pending:
            can_arrive = bool(
                row_number >= transfer.due_index
                and availability.at[timestamp, transfer.destination]
                and transfer_availability.at[timestamp]
            )
            if can_arrive:
                cash[transfer.destination] += transfer.amount_usd
                for record in reversed(transfer_rows):
                    if (
                        record["initiated_at"] == transfer.initiated_at
                        and record["source"] == transfer.source
                        and record["destination"] == transfer.destination
                    ):
                        record["arrived_at"] = timestamp
                        record["status"] = "arrived"
                        break
            else:
                remaining_pending.append(transfer)
        pending = remaining_pending

        if row_number > 0:
            for venue in venues:
                pnl = quantity[venue] * (mark[venue] - previous_mark[venue])
                cash[venue] += pnl
                price_pnl[venue] += pnl
                venue_economic_pnl[venue] += pnl
                row_price_pnl[venue] = pnl

        for venue in venues:
            convention = conventions[venue]
            observed_rate = data.funding_rates.at[timestamp, venue]
            if (
                quantity[venue] != 0.0
                and convention.calendar.settles_at(timestamp)
                and not pd.isna(observed_rate)
            ):
                pnl = convention.payment(
                    quantity=quantity[venue],
                    mark_price=mark[venue],
                    oracle_price=oracle[venue],
                    observed_rate=float(cast(Any, observed_rate)),
                )
                cash[venue] += pnl
                funding_pnl[venue] += pnl
                venue_economic_pnl[venue] += pnl
                row_funding_pnl[venue] = pnl

        liquidated_this_bar = False
        for venue in venues:
            notional, _initial, maintenance, _free = _position_metrics(
                venue,
                cash=cash,
                quantity=quantity,
                mark=mark,
                risk_rules=risk_rules,
            )
            maintenance_buffer = cash[venue] - maintenance
            worst_deficit[venue] = max(worst_deficit[venue], -maintenance_buffer)
            if quantity[venue] != 0.0 and maintenance_buffer <= 0.0:
                equity_before = cash[venue]
                quantity_before = quantity[venue]
                execute_trade(
                    timestamp=timestamp,
                    venue=venue,
                    target_quantity=0.0,
                    reason="local_liquidation",
                    liquidation=True,
                )
                liquidation_counts[venue] += 1
                liquidated_this_bar = True
                liquidation_rows.append(
                    {
                        "timestamp": timestamp,
                        "venue": venue,
                        "quantity": quantity_before,
                        "notional_usd": notional,
                        "equity_before_usd": equity_before,
                        "maintenance_margin_usd": maintenance,
                        "margin_deficit_usd": max(0.0, -maintenance_buffer),
                    }
                )
        if liquidated_this_bar and config.halt_after_liquidation:
            incident_halt = True

        gross_quantity = sum(abs(quantity[venue]) for venue in venues)
        unmatched_quantity = abs(sum(quantity.values()))
        if incident_halt or (
            gross_quantity > config.uncovered_tolerance_quantity
            and unmatched_quantity > config.uncovered_tolerance_quantity
        ):
            for venue in venues:
                if quantity[venue] != 0.0 and bool(availability.at[timestamp, venue]):
                    execute_trade(
                        timestamp=timestamp,
                        venue=venue,
                        target_quantity=0.0,
                        reason="emergency_unwind_after_local_incident",
                    )

        if not incident_halt and row_number < len(index) - 1:
            if row_number % config.position_rebalance_hours == 0:
                observed_signal = {
                    venue: float(cast(Any, signal_rates.at[timestamp, venue]))
                    for venue in venues
                    if not pd.isna(signal_rates.at[timestamp, venue])
                }
                if len(observed_signal) == 2:
                    low = min(venues, key=lambda venue: observed_signal[venue])
                    high = max(venues, key=lambda venue: observed_signal[venue])
                    edge = observed_signal[high] - observed_signal[low]
                    if edge >= config.min_funding_edge_hourly and all(
                        bool(availability.at[timestamp, venue]) for venue in venues
                    ):
                        shared_quantity = config.target_notional_usd / float(
                            np.mean([mark[venue] for venue in venues])
                        )
                        targets = {
                            venue: shared_quantity if venue == low else -shared_quantity
                            for venue in venues
                        }
                        current_signs_match = all(
                            quantity[venue] == 0.0
                            or math.copysign(1.0, quantity[venue])
                            == math.copysign(1.0, targets[venue])
                            for venue in venues
                        )
                        relative_gap = max(
                            abs(quantity[venue] - targets[venue])
                            / max(abs(targets[venue]), config.uncovered_tolerance_quantity)
                            for venue in venues
                        )
                        should_trade = not current_signs_match or (
                            relative_gap >= config.position_rebalance_tolerance_fraction
                        )
                        if should_trade and can_hold_targets(timestamp, targets):
                            for venue in venues:
                                execute_trade(
                                    timestamp=timestamp,
                                    venue=venue,
                                    target_quantity=targets[venue],
                                    reason="funding_orientation_rebalance",
                                )

            if config.enable_collateral_rebalancing:
                transfer_blocked_this_bar = False
                destinations = sorted(
                    venues,
                    key=lambda venue: cash[venue]
                    - abs(quantity[venue])
                    * mark[venue]
                    * risk_rules[venue].initial_margin_fraction,
                )
                for destination in destinations:
                    destination_notional = abs(quantity[destination]) * mark[destination]
                    if destination_notional == 0.0:
                        continue
                    destination_initial = (
                        destination_notional * risk_rules[destination].initial_margin_fraction
                    )
                    destination_free = cash[destination] - destination_initial
                    trigger = (
                        destination_notional
                        * config.collateral_rebalance_trigger_fraction
                    )
                    if destination_free >= trigger:
                        continue
                    if any(item.destination == destination for item in pending):
                        continue
                    source = next(venue for venue in venues if venue != destination)
                    source_notional = abs(quantity[source]) * mark[source]
                    source_initial = source_notional * risk_rules[source].initial_margin_fraction
                    source_free = cash[source] - source_initial
                    source_reserve = (
                        source_notional * config.collateral_rebalance_target_fraction
                    )
                    desired = (
                        destination_notional
                        * config.collateral_rebalance_target_fraction
                        - destination_free
                    )
                    amount = max(0.0, min(desired, source_free - source_reserve))
                    operationally_available = bool(
                        availability.at[timestamp, source]
                        and availability.at[timestamp, destination]
                        and transfer_availability.at[timestamp]
                    )
                    if not operationally_available:
                        transfer_blocked_this_bar = True
                        continue
                    if amount <= 0.0:
                        continue
                    fee = amount * config.transfer_fee_bps / 10_000.0 + config.transfer_fixed_fee_usd
                    if cash[source] - amount - fee < source_initial:
                        amount = max(0.0, cash[source] - source_initial - fee)
                    if amount <= 0.0:
                        continue
                    cash[source] -= amount + fee
                    venue_economic_pnl[source] -= fee
                    rebalancing_cost += fee
                    due_index = row_number + config.transfer_delay_hours
                    transfer_record: dict[str, object] = {
                        "initiated_at": timestamp,
                        "arrived_at": pd.NaT,
                        "source": source,
                        "destination": destination,
                        "amount_usd": amount,
                        "fee_usd": fee,
                        "delay_hours": config.transfer_delay_hours,
                        "status": "in_transit",
                    }
                    transfer_rows.append(transfer_record)
                    if config.transfer_delay_hours == 0:
                        cash[destination] += amount
                        transfer_record["arrived_at"] = timestamp
                        transfer_record["status"] = "arrived"
                    else:
                        pending.append(
                            _PendingTransfer(
                                initiated_at=timestamp,
                                due_index=due_index,
                                source=source,
                                destination=destination,
                                amount_usd=amount,
                                fee_usd=fee,
                            )
                        )
                if transfer_blocked_this_bar:
                    blocked_transfer_hours += 1.0

        if row_number == len(index) - 1:
            for venue in venues:
                if quantity[venue] != 0.0 and bool(availability.at[timestamp, venue]):
                    execute_trade(
                        timestamp=timestamp,
                        venue=venue,
                        target_quantity=0.0,
                        reason="terminal_close",
                    )

        gross_quantity = sum(abs(quantity[venue]) for venue in venues)
        unmatched_quantity = abs(sum(quantity.values()))
        if (
            gross_quantity > config.uncovered_tolerance_quantity
            and unmatched_quantity > config.uncovered_tolerance_quantity
        ):
            uncovered_hours += 1.0

        timeline_row: dict[str, object] = {"timestamp": timestamp}
        for venue in venues:
            notional, initial_margin, maintenance_margin, free_margin = _position_metrics(
                venue,
                cash=cash,
                quantity=quantity,
                mark=mark,
                risk_rules=risk_rules,
            )
            minimum_free_margin[venue] = min(minimum_free_margin[venue], free_margin)
            max_initial_margin[venue] = max(max_initial_margin[venue], initial_margin)
            maintenance_buffer = cash[venue] - maintenance_margin
            worst_deficit[venue] = max(worst_deficit[venue], -maintenance_buffer)
            timeline_row.update(
                {
                    f"{venue}_equity": cash[venue],
                    f"{venue}_quantity": quantity[venue],
                    f"{venue}_notional": notional,
                    f"{venue}_initial_margin": initial_margin,
                    f"{venue}_maintenance_margin": maintenance_margin,
                    f"{venue}_free_margin": free_margin,
                    f"{venue}_maintenance_buffer": maintenance_buffer,
                    f"{venue}_price_pnl": row_price_pnl[venue],
                    f"{venue}_funding_pnl": row_funding_pnl[venue],
                    f"{venue}_available": bool(availability.at[timestamp, venue]),
                }
            )
        capital_in_transit = sum(item.amount_usd for item in pending)
        timeline_row.update(
            {
                "capital_in_transit": capital_in_transit,
                "total_equity": sum(cash.values()) + capital_in_transit,
                "uncovered_quantity": unmatched_quantity,
                "transfers_available": bool(transfer_availability.at[timestamp]),
                "incident_halt": incident_halt,
            }
        )
        timeline_rows.append(timeline_row)
        previous_mark = mark

    timeline = pd.DataFrame(timeline_rows).set_index("timestamp")
    initial_total = sum(config.initial_capital_by_venue.values())
    final_total = float(timeline["total_equity"].iloc[-1])
    equity_curve = timeline["total_equity"] / initial_total
    drawdown = equity_curve / equity_curve.cummax() - 1.0
    hourly_return = timeline["total_equity"].pct_change(fill_method=None).fillna(0.0)
    gross_notional = pd.Series(0.0, index=timeline.index)
    signed_notional = pd.Series(0.0, index=timeline.index)
    for venue in venues:
        gross_notional += timeline[f"{venue}_notional"]
        signed_notional += timeline[f"{venue}_quantity"] * pd.Series(
            data.mark_prices[venue].to_numpy(),
            index=timeline.index,
        )
    outage_hours = {
        venue: float((~availability[venue].astype(bool)).sum()) for venue in venues
    }
    metrics = CrossVenueMetrics(
        gross_return_on_total_capital=(
            sum(price_pnl.values()) + sum(funding_pnl.values())
        )
        / initial_total,
        return_on_total_capital=final_total / initial_total - 1.0,
        return_by_venue={
            venue: venue_economic_pnl[venue] / config.initial_capital_by_venue[venue]
            for venue in venues
        },
        max_drawdown=float(drawdown.min()),
        worst_hour=float(hourly_return.min()),
        turnover=trade_notional_total / initial_total,
        time_in_market=float(gross_notional.gt(0.0).mean()),
        max_gross_exposure=float(gross_notional.max() / initial_total),
        max_net_exposure=float(signed_notional.abs().max() / initial_total),
        worst_local_margin_deficit_usd={
            venue: max(0.0, worst_deficit[venue]) for venue in venues
        },
        minimum_free_margin_usd=minimum_free_margin,
        max_capital_immobilized_usd=max_initial_margin,
        uncovered_hours=uncovered_hours,
        rebalancing_cost_usd=rebalancing_cost,
        blocked_transfer_hours=blocked_transfer_hours,
        liquidation_count_by_venue=liquidation_counts,
        outage_hours_by_venue=outage_hours,
        funding_pnl_by_venue=funding_pnl,
        price_pnl_by_venue=price_pnl,
        fee_by_venue=fee_cost,
        slippage_by_venue=slippage_cost,
        liquidation_penalty_by_venue=liquidation_cost,
        execution_cost_by_venue=execution_cost,
    )
    return CrossVenueResult(
        asset=data.asset,
        timeline=timeline,
        trades=pd.DataFrame(trade_rows)
        if trade_rows
        else _empty_frame(
            (
                "timestamp",
                "venue",
                "delta_quantity",
                "target_quantity",
                "mark_price",
                "notional_usd",
                "fee_usd",
                "slippage_usd",
                "liquidation_penalty_usd",
                "reason",
            )
        ),
        transfers=pd.DataFrame(transfer_rows)
        if transfer_rows
        else _empty_frame(
            (
                "initiated_at",
                "arrived_at",
                "source",
                "destination",
                "amount_usd",
                "fee_usd",
                "delay_hours",
                "status",
            )
        ),
        liquidations=pd.DataFrame(liquidation_rows)
        if liquidation_rows
        else _empty_frame(
            (
                "timestamp",
                "venue",
                "quantity",
                "notional_usd",
                "equity_before_usd",
                "maintenance_margin_usd",
                "margin_deficit_usd",
            )
        ),
        metrics=metrics,
        diagnostics={
            "model": "separate_variation_margin_accounts",
            "initial_capital_by_venue": dict(config.initial_capital_by_venue),
            "target_notional_usd": config.target_notional_usd,
            "halt_after_liquidation": config.halt_after_liquidation,
            "network_enabled": False,
            "data_status": str(data.metadata.get("calibration_status", "UNCALIBRATED")),
            "data_source": str(data.metadata.get("source", "UNDECLARED")),
            "calibration_evidence_hash": data.metadata.get("calibration_evidence_hash"),
            "counterfactual_stress": data.metadata.get("counterfactual_stress"),
        },
    )


def audit_cross_venue_data(
    data: CrossVenueMarketData,
    *,
    conventions: dict[str, FundingConvention],
    risk_rules: dict[str, VenueRiskRule],
    minimum_history_hours: int = 30 * 24,
) -> CrossVenueDataAudit:
    data.validate(conventions)
    if minimum_history_hours < 24:
        raise ValueError("minimum_history_hours must be at least 24")
    venues = data.venues
    observed_hours = len(data.mark_prices)
    source = str(data.metadata.get("source", "")).casefold()
    funding_evidence = data.metadata.get("funding_convention_evidence_hashes")
    identity = data.metadata.get("venue_identity")
    settlements_complete = True
    no_unscheduled_funding = True
    for venue in venues:
        convention = conventions[venue]
        scheduled = pd.Series(
            [convention.calendar.settles_at(timestamp) for timestamp in data.mark_prices.index],
            index=data.mark_prices.index,
        )
        settlements_complete &= bool(data.funding_rates.loc[scheduled, venue].notna().all())
        no_unscheduled_funding &= bool(data.funding_rates.loc[~scheduled, venue].isna().all())
    checks = {
        "hourly_regular": observed_hours > 1,
        "minimum_history": observed_hours >= minimum_history_hours,
        "two_distinct_venues": len(set(venues)) == 2,
        "real_not_synthetic": bool(source) and "synthetic" not in source,
        "point_in_time": data.metadata.get("point_in_time") is True,
        "calibrated_data": (
            str(data.metadata.get("calibration_status", "")).upper() == "CALIBRATED"
            and _is_sha256(data.metadata.get("calibration_evidence_hash"))
        ),
        "parameters_frozen_before_period": (
            data.metadata.get("parameters_frozen_before_period") is True
        ),
        "chronological_evaluation_split": str(
            data.metadata.get("evaluation_split", "")
        ).casefold()
        in {"validation", "final_test", "forward"},
        "marks_complete": not bool(data.mark_prices.isna().any(axis=None)),
        "oracles_complete": not bool(data.oracle_prices.isna().any(axis=None)),
        "funding_settlements_complete": settlements_complete,
        "no_unscheduled_funding": no_unscheduled_funding,
        "funding_conventions_evidenced": (
            isinstance(funding_evidence, dict)
            and all(_is_sha256(funding_evidence.get(venue)) for venue in venues)
        ),
        "venue_risk_rules_calibrated": all(
            _is_sha256(risk_rules[venue].calibration_evidence_hash) for venue in venues
        ),
        "transfer_policy_evidenced": _is_sha256(
            data.metadata.get("transfer_policy_evidence_hash")
        ),
        "venue_identity_verified": (
            isinstance(identity, dict)
            and all(isinstance(identity.get(venue), str) and identity[venue] for venue in venues)
        ),
    }
    reason_by_check = {
        "hourly_regular": "La grille multi-venue n'est pas horaire et régulière.",
        "minimum_history": (
            f"Couverture insuffisante : {observed_hours} h observées, "
            f"{minimum_history_hours} h requises."
        ),
        "two_distinct_venues": "Deux venues distinctes sont obligatoires.",
        "real_not_synthetic": "La provenance réelle versionnée n'est pas établie.",
        "point_in_time": "Les observations ne sont pas déclarées point-in-time.",
        "calibrated_data": "La calibration des données n'est pas vérifiable.",
        "parameters_frozen_before_period": (
            "Les paramètres ne sont pas déclarés figés avant la période évaluée."
        ),
        "chronological_evaluation_split": (
            "La période n'est pas identifiée comme validation, test final ou forward."
        ),
        "marks_complete": "Un mark manque sur au moins une venue.",
        "oracles_complete": "Un oracle/index de référence manque sur au moins une venue.",
        "funding_settlements_complete": "Un règlement de funding attendu manque.",
        "no_unscheduled_funding": "Un funding est placé hors du calendrier déclaré.",
        "funding_conventions_evidenced": "La preuve des conventions de funding manque.",
        "venue_risk_rules_calibrated": "Les marges/frais par venue ne sont pas calibrés.",
        "transfer_policy_evidenced": "La politique et les coûts de transfert ne sont pas sourcés.",
        "venue_identity_verified": "L'identité économique du contrat manque pour une venue.",
    }
    return CrossVenueDataAudit(
        observed_hours=observed_hours,
        minimum_history_hours=minimum_history_hours,
        asset=data.asset,
        venues=venues,
        checks=checks,
        reasons=tuple(reason_by_check[name] for name, passed in checks.items() if not passed),
    )


def run_cross_exchange_validation(
    data: CrossVenueMarketData,
    *,
    conventions: dict[str, FundingConvention],
    risk_rules: dict[str, VenueRiskRule],
    config: CrossVenueConfig,
    failed_venue: str,
    outage_start: pd.Timestamp,
    audit: CrossVenueDataAudit | None = None,
    outage_hours: tuple[int, ...] = (1, 6, 24),
) -> CrossExchangeValidation:
    if failed_venue not in data.venues:
        raise ValueError("failed venue must be present in cross-venue data")
    if outage_hours != (1, 6, 24):
        raise ValueError("Phase 07 requires the preregistered 1 h, 6 h and 24 h outages")
    resolved_start = pd.Timestamp(outage_start)
    if resolved_start not in data.mark_prices.index:
        raise ValueError("outage_start must be present in the market grid")
    start_position = data.mark_prices.index.get_loc(resolved_start)
    if not isinstance(start_position, int):
        raise ValueError("outage_start must resolve to one row")
    if start_position + max(outage_hours) >= len(data.mark_prices):
        raise ValueError("market data needs a recovery bar after the full 24 h outage")

    scenarios = {
        "base": simulate_cross_exchange_funding(
            data,
            conventions=conventions,
            risk_rules=risk_rules,
            config=config,
        )
    }
    base_availability = data.resolved_venue_available()
    base_transfers = data.resolved_transfers_available()
    for duration in outage_hours:
        availability = base_availability.copy()
        transfers = base_transfers.copy()
        affected = data.mark_prices.index[start_position : start_position + duration]
        availability.loc[affected, failed_venue] = False
        transfers.loc[affected] = False
        stressed = replace(
            data,
            venue_available=availability,
            transfers_available=transfers,
            metadata={
                **data.metadata,
                "counterfactual_stress": f"{failed_venue}_outage_{duration}h",
            },
        )
        scenarios[f"outage_{duration}h"] = simulate_cross_exchange_funding(
            stressed,
            conventions=conventions,
            risk_rules=risk_rules,
            config=config,
        )

    resolved_audit = audit or audit_cross_venue_data(
        data,
        conventions=conventions,
        risk_rules=risk_rules,
    )
    if not resolved_audit.checks.get("minimum_history", False):
        status = "BLOCKED_INSUFFICIENT_REAL_DATA"
    elif not resolved_audit.passed:
        status = "BLOCKED_UNCALIBRATED_OR_INCOMPLETE_MODEL"
    elif any(
        sum(result.metrics.liquidation_count_by_venue.values()) > 0
        or result.metrics.uncovered_hours > 0.0
        for result in scenarios.values()
    ):
        status = "BLOCKED_LOCAL_MARGIN_OR_OUTAGE_RISK"
    else:
        status = "VALIDATED_RESEARCH_ONLY"
    return CrossExchangeValidation(
        audit=resolved_audit,
        scenarios=scenarios,
        status=status,
        failed_venue=failed_venue,
        outage_start=resolved_start,
        conventions=conventions,
        risk_rules=risk_rules,
        config=config,
    )


def _result_summary(result: CrossVenueResult) -> dict[str, object]:
    return asdict(result.metrics)


def _percent(value: float) -> str:
    return f"{value * 100:.2f} %"


def _money(value: float) -> str:
    return f"{value:,.2f} USD".replace(",", " ")


def write_cross_exchange_report(
    validation: CrossExchangeValidation,
    *,
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries = {
        name: _result_summary(result) for name, result in validation.scenarios.items()
    }
    payload = {
        "schema_version": 1,
        "status": validation.status,
        "data_status": validation.scenarios["base"].diagnostics.get("data_status"),
        "data_audit": asdict(validation.audit),
        "failed_venue": validation.failed_venue,
        "outage_start": validation.outage_start.isoformat(),
        "model": {
            "config": asdict(validation.config),
            "risk_rules": {
                venue: asdict(rule) for venue, rule in validation.risk_rules.items()
            },
            "funding_conventions": {
                venue: {
                    "interval_hours": convention.calendar.interval_hours,
                    "anchor_hour_utc": convention.calendar.anchor_hour_utc,
                    "explicit_settlements": [
                        timestamp.isoformat()
                        for timestamp in convention.calendar.explicit_settlements
                    ],
                    "notional_price_source": convention.notional_price_source,
                    "formula_name": convention.formula_name,
                    "documentation_url": convention.documentation_url,
                }
                for venue, convention in validation.conventions.items()
            },
            "outage_durations_hours": [1, 6, 24],
        },
        "data_provenance": {
            "source": validation.scenarios["base"].diagnostics.get("data_source"),
            "calibration_evidence_hash": validation.scenarios["base"].diagnostics.get(
                "calibration_evidence_hash"
            ),
        },
        "scenarios": summaries,
        "pnl_identity": (
            "net = mark variation + venue funding - fees - slippage - liquidation penalties "
            "- collateral transfer fees"
        ),
        "capital_identity": "total = venue A equity + venue B equity + capital in transit",
    }
    summary_path = output_dir / "cross_exchange_funding_summary.json"
    temporary = summary_path.with_name(f".{summary_path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(summary_path)

    base = validation.scenarios["base"]
    base.timeline.to_csv(output_dir / "cross_exchange_funding_timeline.csv")
    base.trades.to_csv(output_dir / "cross_exchange_funding_trades.csv", index=False)
    base.transfers.to_csv(output_dir / "cross_exchange_funding_transfers.csv", index=False)
    base.liquidations.to_csv(
        output_dir / "cross_exchange_funding_liquidations.csv",
        index=False,
    )

    rows: list[str] = []
    labels = {
        "base": "Base",
        "outage_1h": "Indisponibilité 1 h",
        "outage_6h": "Indisponibilité 6 h",
        "outage_24h": "Indisponibilité 24 h",
    }
    for name, summary in summaries.items():
        venue_returns = cast(dict[str, float], summary["return_by_venue"])
        deficits = cast(dict[str, float], summary["worst_local_margin_deficit_usd"])
        rows.append(
            "<tr>"
            f"<td>{html.escape(labels[name])}</td>"
            f"<td>{_percent(cast(float, summary['return_on_total_capital']))}</td>"
            f"<td>{html.escape(' / '.join(f'{venue}: {_percent(value)}' for venue, value in venue_returns.items()))}</td>"
            f"<td>{html.escape(' / '.join(f'{venue}: {_money(value)}' for venue, value in deficits.items()))}</td>"
            f"<td>{cast(float, summary['uncovered_hours']):.1f} h</td>"
            f"<td>{_money(cast(float, summary['rebalancing_cost_usd']))}</td>"
            "</tr>"
        )
    reasons = "".join(f"<li>{html.escape(reason)}</li>" for reason in validation.audit.reasons)
    data_status = str(base.diagnostics.get("data_status", "UNCALIBRATED"))
    base_summary = summaries["base"]
    base_price = cast(dict[str, float], base_summary["price_pnl_by_venue"])
    base_funding = cast(dict[str, float], base_summary["funding_pnl_by_venue"])
    base_execution = cast(dict[str, float], base_summary["execution_cost_by_venue"])
    document = f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><title>HyperLab Phase 07 — funding inter-exchanges</title>
<style>body{{font-family:Segoe UI,sans-serif;max-width:1250px;margin:auto;padding:32px;background:#091321;color:#edf5ff}}table{{width:100%;border-collapse:collapse;margin:16px 0}}th,td{{padding:9px;border:1px solid #29405d;text-align:right}}th:first-child,td:first-child{{text-align:left}}section{{padding:16px;border:1px solid #496786;border-radius:10px;margin:16px 0}}.warning{{border-color:#f2b84b}}</style></head>
<body><h1>Phase 07 — arbitrage de funding inter-exchanges</h1>
<section class="warning"><strong>{html.escape(validation.status)} — {html.escape(data_status)}</strong><ul>{reasons}</ul></section>
<p>Deux comptes de marge sont simulés séparément. Le mark pilote le PnL et la liquidation locale ; chaque convention choisit explicitement le mark ou l'oracle pour le funding.</p>
<table><thead><tr><th>Scénario</th><th>Rendement sur capital total</th><th>Rendement par venue</th><th>Pire déficit de marge local</th><th>Temps non couvert</th><th>Frais de rééquilibrage</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
<h2>Attribution du scénario de base</h2><p>Rendement brut {_percent(cast(float, base_summary['gross_return_on_total_capital']))} ; rendement net {_percent(cast(float, base_summary['return_on_total_capital']))} ; drawdown {_percent(cast(float, base_summary['max_drawdown']))} ; turnover {cast(float, base_summary['turnover']):.3f}x.</p>
<p>PnL mark par venue : {html.escape(' / '.join(f'{venue}: {_money(value)}' for venue, value in base_price.items()))}. Funding : {html.escape(' / '.join(f'{venue}: {_money(value)}' for venue, value in base_funding.items()))}. Coûts d'exécution : {html.escape(' / '.join(f'{venue}: {_money(value)}' for venue, value in base_execution.items()))}.</p>
<p>Exposition brute maximale {_percent(cast(float, base_summary['max_gross_exposure']))} du capital total ; exposition nette maximale {_percent(cast(float, base_summary['max_net_exposure']))}.</p>
<h2>Capital immobilisé séparément</h2><p>Le fichier timeline expose equity, marge initiale, marge de maintenance et marge libre heure par heure pour chaque venue.</p>
<h2>Hypothèses et limites</h2><ul><li>Funding Hyperliquid : règlement horaire sur notional oracle.</li><li>Funding Binance USD-M : taux réalisé publié au calendrier observé, notional mark associé au règlement.</li><li>Une panne bloque les actions utilisateur et les transferts ; le moteur de liquidation local reste actif.</li><li>Les résultats synthétiques ou non calibrés ne constituent aucune preuve de rentabilité.</li><li>Recherche uniquement — aucune route d'ordre, clé ou signature.</li></ul>
</body></html>"""
    report_path = output_dir / "cross_exchange_funding_report.html"
    report_path.write_text(document, encoding="utf-8")
    return report_path


def generate_cross_exchange_demo_data(
    *,
    hours: int = 240,
    seed: int = 707,
) -> CrossVenueMarketData:
    """Deterministic synthetic input used only to exercise Phase-07 plumbing."""

    if hours < 72:
        raise ValueError("Phase-07 demo needs at least 72 hours")
    rng = np.random.default_rng(seed)
    index = pd.date_range("2025-01-01", periods=hours, freq="1h", tz="UTC")
    common = rng.normal(0.0, 0.006, hours)
    reference = 50_000.0 * np.exp(np.cumsum(common))
    hl_basis = np.zeros(hours)
    binance_basis = np.zeros(hours)
    for position in range(1, hours):
        hl_basis[position] = 0.94 * hl_basis[position - 1] + rng.normal(0.0, 0.00035)
        binance_basis[position] = 0.92 * binance_basis[position - 1] + rng.normal(
            0.0, 0.00025
        )
    marks = pd.DataFrame(
        {
            "HL": reference * (1.0 + hl_basis),
            "BINANCE_USDM": reference * (1.0 + binance_basis),
        },
        index=index,
    )
    oracles = pd.DataFrame(
        {
            "HL": reference * (1.0 + rng.normal(0.0, 0.00008, hours)),
            "BINANCE_USDM": reference * (1.0 + rng.normal(0.0, 0.00005, hours)),
        },
        index=index,
    )
    hl_rate = 0.000006 + 0.012 * hl_basis + rng.normal(0.0, 0.000002, hours)
    binance_rate = 0.00012 + 0.09 * binance_basis + rng.normal(0.0, 0.00002, hours)
    funding = pd.DataFrame(np.nan, index=index, columns=list(marks.columns))
    funding["HL"] = hl_rate
    settlement = index.hour.isin([0, 8, 16])
    funding.loc[settlement, "BINANCE_USDM"] = binance_rate[settlement]
    return CrossVenueMarketData(
        asset="BTC",
        mark_prices=marks,
        oracle_prices=oracles,
        funding_rates=funding,
        metadata={
            "source": "synthetic-phase07-demo-only",
            "seed": seed,
            "point_in_time": True,
            "calibration_status": "SYNTHETIC",
            "warning": "Never use synthetic Phase-07 results as an investment decision.",
        },
    )


__all__ = [
    "CrossExchangeValidation",
    "CrossVenueConfig",
    "CrossVenueDataAudit",
    "CrossVenueMarketData",
    "CrossVenueMetrics",
    "CrossVenueResult",
    "FundingCalendar",
    "FundingConvention",
    "VenueRiskRule",
    "audit_cross_venue_data",
    "default_cross_venue_config",
    "default_cross_venue_risk_rules",
    "default_funding_conventions",
    "generate_cross_exchange_demo_data",
    "hyperliquid_hourly_rate_from_premium",
    "run_cross_exchange_validation",
    "simulate_cross_exchange_funding",
    "venue_risk_rules_from_metadata",
    "write_cross_exchange_report",
]
