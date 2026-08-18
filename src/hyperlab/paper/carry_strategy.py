from __future__ import annotations

import math
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from types import MappingProxyType
from typing import cast

import pandas as pd

from hyperlab.backtest.costs import parse_instrument
from hyperlab.backtest.protocol import canonical_sha256
from hyperlab.models import MarketPanel
from hyperlab.paper.models import (
    DecisionAction,
    DecisionIntent,
    MarketEvent,
    OrderIntent,
    OrderSide,
    PaperOrderType,
    PaperRiskLimits,
    PaperState,
    PaperStrategyConfig,
    TimeInForce,
    decimal_text,
    decimal_value,
    utc_text,
)
from hyperlab.paper.public_source import PublicFundingSettlement
from hyperlab.paper.runner import PaperStrategyView
from hyperlab.strategies.carry import CashAndCarryStrategy

PHASE05_CARRY_STRATEGY_ID = "phase05_cash_and_carry"
_STRATEGY_NAME = "cash_and_carry"
_IMPLEMENTATION_ID = "phase05-reviewed-cash-and-carry-paper-adapter-v1"
_ECONOMIC_STATUS = "TECHNICAL_ONLY_UNCALIBRATED"
_FUNDING_WINDOWS = (8, 24, 72)
_HOUR_SECONDS = 3_600
_ECONOMICS: dict[str, object] = {
    "basis_speed_lookback_hours": 8,
    "benchmark_annual_rate": "0.045",
    "capital_fraction": "0.50",
    "edge_horizons_hours": list(_FUNDING_WINDOWS),
    "estimated_round_trip_slippage_bps": "4.0",
    "funding_trend": "mean_8h_minus_mean_24h",
    "lookback_hours": 72,
    "maker_attempt_then_engine_emergency_ioc": True,
    "max_abs_basis_bps": "150.0",
    "max_annualized_volatility": "1.50",
    "max_positions": 1,
    "min_depth_usd": "100000.0",
    "min_mean_funding_hourly": "0.000005",
    "min_open_interest_usd": "5000000.0",
    "min_positive_share": "0.70",
    "min_volume_usd": "1000000.0",
    "minimum_net_edge_bps": "0.0",
    "perp_margin_fraction": "1.0",
    "rebalance_hours": 8,
    "round_trip_fees_bps": "11.0",
}
_STRATEGY_HASH = canonical_sha256(
    {
        "component": _IMPLEMENTATION_ID,
        "economics": _ECONOMICS,
        "reviewed_source": "hyperlab.strategies.carry.CashAndCarryStrategy",
        "schema_version": 1,
    }
)


def _hour(value: datetime) -> datetime:
    epoch = int(value.timestamp())
    return datetime.fromtimestamp(epoch - epoch % _HOUR_SECONDS, tz=UTC)


def _context_decimal(
    market: MarketEvent,
    name: str,
    *,
    non_negative: bool = False,
) -> Decimal:
    raw = market.context.get(name)
    if raw is None or isinstance(raw, (bool, float)):
        raise ValueError(f"{market.instrument} context lacks exact {name}")
    return decimal_value(
        cast(Decimal | str | int, raw),
        label=f"{market.instrument}.{name}",
        non_negative=non_negative,
    )


def _finite(value: object) -> float | None:
    converted = float(cast(float, value))
    return converted if math.isfinite(converted) else None


@dataclass(frozen=True, slots=True)
class FrozenCashAndCarryPaperConfig:
    """Operational identity around the reviewed, unchanged Phase-05 economics."""

    spot_instrument: str
    perp_instrument: str
    spot_product_identity_sha256: str
    perp_product_identity_sha256: str
    retained_hours: int
    maximum_gross_notional: Decimal
    spot_quantity_step: Decimal
    perp_quantity_step: Decimal
    spot_max_quantity: Decimal
    perp_max_quantity: Decimal
    maximum_execution_skew_seconds: int = 5

    def __post_init__(self) -> None:
        spot_exchange, spot_asset, spot_kind = parse_instrument(self.spot_instrument)
        perp_exchange, perp_asset, perp_kind = parse_instrument(self.perp_instrument)
        if spot_kind != "spot" or perp_kind != "perp":
            raise ValueError("Phase 05 requires one canonical spot and one canonical perp")
        if (spot_exchange, spot_asset) != (perp_exchange, perp_asset):
            raise ValueError("Phase 05 spot and perp must share one canonical exchange/asset")
        for name in ("spot_product_identity_sha256", "perp_product_identity_sha256"):
            digest = getattr(self, name)
            if (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")
        if (
            isinstance(self.retained_hours, bool)
            or not isinstance(self.retained_hours, int)
            or self.retained_hours < 73
        ):
            raise ValueError("retained_hours must cover at least 72 completed funding hours")
        if isinstance(self.maximum_execution_skew_seconds, bool) or self.maximum_execution_skew_seconds < 0:
            raise ValueError("maximum_execution_skew_seconds must be non-negative")
        for name in (
            "maximum_gross_notional",
            "spot_quantity_step",
            "perp_quantity_step",
            "spot_max_quantity",
            "perp_max_quantity",
        ):
            object.__setattr__(
                self,
                name,
                decimal_value(getattr(self, name), label=name, positive=True),
            )

    @property
    def asset(self) -> str:
        return self.spot_instrument.split(":", 2)[1]

    def to_parameters(self) -> dict[str, object]:
        return {
            "economic_status": _ECONOMIC_STATUS,
            "implementation": _IMPLEMENTATION_ID,
            "operational_binding": {
                "maximum_execution_skew_seconds": self.maximum_execution_skew_seconds,
                "maximum_gross_notional": decimal_text(self.maximum_gross_notional),
                "perp_instrument": self.perp_instrument,
                "perp_max_quantity": decimal_text(self.perp_max_quantity),
                "perp_product_identity_sha256": self.perp_product_identity_sha256,
                "perp_quantity_step": decimal_text(self.perp_quantity_step),
                "retained_hours": self.retained_hours,
                "spot_instrument": self.spot_instrument,
                "spot_max_quantity": decimal_text(self.spot_max_quantity),
                "spot_product_identity_sha256": self.spot_product_identity_sha256,
                "spot_quantity_step": decimal_text(self.spot_quantity_step),
            },
            "phase": "05",
            "reviewed_economics": _ECONOMICS,
            "source_contract": "ONE_SHARED_PUBLIC_NORMALIZED_FEED_NO_PRIVATE_API",
        }


def make_phase05_paper_strategy_config(
    *, config: FrozenCashAndCarryPaperConfig, risk: PaperRiskLimits
) -> PaperStrategyConfig:
    return PaperStrategyConfig(
        strategy_id=PHASE05_CARRY_STRATEGY_ID,
        strategy_name=_STRATEGY_NAME,
        strategy_hash=_STRATEGY_HASH,
        parameters=config.to_parameters(),
        risk=risk,
        required_instruments=(config.spot_instrument, config.perp_instrument),
    )


@dataclass(frozen=True, slots=True)
class _CompletedCarryBar:
    observed_at: datetime
    spot_mid: Decimal
    perp_mid: Decimal
    spot_depth_usd: Decimal
    perp_depth_usd: Decimal
    spot_volume_usd: Decimal
    perp_volume_usd: Decimal
    perp_open_interest_usd: Decimal
    spot_spread_bps: Decimal
    perp_spread_bps: Decimal
    event_ids: tuple[str, str]


class FrozenCashAndCarryPaperStrategy:
    """Causal hourly Phase-05 adapter for the deterministic Paper gateway."""

    strategy_id = PHASE05_CARRY_STRATEGY_ID
    strategy_name = _STRATEGY_NAME
    strategy_hash = _STRATEGY_HASH

    def __init__(
        self,
        *,
        config: FrozenCashAndCarryPaperConfig,
        strategy_config: PaperStrategyConfig | None = None,
    ) -> None:
        self.config = config
        if strategy_config is None:
            strategy_config = make_phase05_paper_strategy_config(
                config=config,
                risk=PaperRiskLimits(
                    max_gross_notional=config.maximum_gross_notional,
                    max_net_notional=config.maximum_gross_notional,
                    max_instrument_notional=config.maximum_gross_notional / Decimal(2),
                    max_order_notional=config.maximum_gross_notional / Decimal(2),
                    max_position_quantity=max(config.spot_max_quantity, config.perp_max_quantity),
                    max_order_quantity=max(config.spot_max_quantity, config.perp_max_quantity),
                    max_concurrent_orders=2,
                ),
            )
        expected = make_phase05_paper_strategy_config(config=config, risk=strategy_config.risk)
        if strategy_config != expected:
            raise ValueError("Phase 05 adapter differs from its frozen strategy configuration")
        leg_notional = config.maximum_gross_notional / Decimal(2)
        if (
            strategy_config.risk.max_gross_notional < config.maximum_gross_notional
            or strategy_config.risk.max_instrument_notional < leg_notional
            or strategy_config.risk.max_order_notional < leg_notional
            or strategy_config.risk.max_concurrent_orders < 2
        ):
            raise ValueError("Phase 05 local risk budget is below its frozen two-leg allocation")
        self.strategy_config_hash = strategy_config.strategy_config_hash
        self._reviewed = CashAndCarryStrategy()
        self._bars: deque[_CompletedCarryBar] = deque(maxlen=config.retained_hours)
        self._funding: dict[datetime, PublicFundingSettlement] = {}
        self._funding_order: deque[datetime] = deque()
        self._pending_bucket: datetime | None = None
        self._pending_markets: dict[str, MarketEvent] = {}
        self._seen_funding_ids: set[str] = set()
        self._last_market_received_at: datetime | None = None
        self._last_funding_time: datetime | None = None
        self._last_evaluated_at: datetime | None = None
        self._pending_signal_at: datetime | None = None
        self._eligible = False
        self._diagnostic: dict[str, object] = {
            "adapter": _IMPLEMENTATION_ID,
            "economic_status": _ECONOMIC_STATUS,
            "status": "WARMING_UP",
        }

    @property
    def diagnostic_snapshot(self) -> Mapping[str, object]:
        return MappingProxyType(dict(self._diagnostic))

    @property
    def _instruments(self) -> tuple[str, str]:
        return (self.config.spot_instrument, self.config.perp_instrument)

    def observe_funding(self, settlement: PublicFundingSettlement) -> None:
        if not isinstance(settlement, PublicFundingSettlement):
            raise TypeError("funding input must be PublicFundingSettlement")
        if settlement.instrument != self.config.perp_instrument:
            return
        if settlement.event_id in self._seen_funding_ids:
            return
        if (
            settlement.rate_kind != "hyperliquid-hourly-settlement"
            or settlement.funding_interval_seconds != _HOUR_SECONDS
        ):
            raise ValueError("Phase 05 admits only finalized hourly Hyperliquid funding")
        bucket = _hour(settlement.funding_time)
        if settlement.funding_time != bucket:
            raise ValueError("Phase 05 funding must lie on an exact UTC hour")
        previous = self._funding.get(bucket)
        if previous is not None and previous != settlement:
            raise ValueError("conflicting funding settlement for an admitted UTC hour")
        if self._last_funding_time is not None and bucket < self._last_funding_time:
            raise ValueError("funding settlements must be supplied in causal time order")
        self._seen_funding_ids.add(settlement.event_id)
        self._funding[bucket] = settlement
        self._funding_order.append(bucket)
        self._last_funding_time = bucket
        while len(self._funding_order) > self.config.retained_hours:
            expired = self._funding.pop(self._funding_order.popleft(), None)
            if expired is not None:
                self._seen_funding_ids.discard(expired.event_id)

    def restore_public_inputs(
        self,
        inputs: Iterable[MarketEvent | PublicFundingSettlement],
        view: PaperStrategyView | None = None,
    ) -> None:
        del view
        self._reset()
        self._restore(inputs)
        self._diagnostic = {**self._diagnostic, "status": "RESTORED"}

    def restore_incremental_public_inputs(
        self,
        inputs: Iterable[MarketEvent | PublicFundingSettlement],
        view: PaperStrategyView | None = None,
    ) -> None:
        del view
        self._restore(inputs)
        self._diagnostic = {**self._diagnostic, "status": "RESTORED_INCREMENTAL"}

    def _restore(self, inputs: Iterable[MarketEvent | PublicFundingSettlement]) -> None:
        for item in inputs:
            if isinstance(item, PublicFundingSettlement):
                self.observe_funding(item)
            elif isinstance(item, MarketEvent):
                self._ingest_market(item)
            else:
                raise TypeError("unsupported durable public input for Phase 05 restoration")
        if self._bars and self._scheduled(self._bars[-1].observed_at):
            self._evaluate_reviewed()
        self._last_evaluated_at = self._bars[-1].observed_at if self._bars else None
        self._pending_signal_at = None

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        if not markets or any(key != event.instrument for key, event in markets.items()):
            raise ValueError("Phase 05 frame must be non-empty and canonically keyed")
        completed = False
        own_events = (event for event in markets.values() if event.instrument in self._instruments)
        for event in sorted(
            own_events,
            key=lambda item: (item.received_at, item.capture_ordinal, item.event_id),
        ):
            completed = self._ingest_market(event) or completed
        if completed and self._bars:
            observed_at = self._bars[-1].observed_at
            if self._last_evaluated_at != observed_at and self._scheduled(observed_at):
                self._last_evaluated_at = observed_at
                self._evaluate_reviewed()
                self._pending_signal_at = observed_at
        signal_at = self._pending_signal_at
        if signal_at is None:
            return None
        if set(self._instruments).issubset(markets) and any(
            markets[instrument].received_at < signal_at for instrument in self._instruments
        ):
            self._diagnostic = {
                **self._diagnostic,
                "status": "WAITING_FOR_COMPLETE_POST_BAR_EXECUTION_FRAME",
            }
            return None
        execution = self._execution_snapshot(markets)
        if execution is None:
            return None
        self._pending_signal_at = None
        self._diagnostic = {
            **self._diagnostic,
            "execution_event_ids": self._observed_event_ids(execution)[-2:],
            "signal_bar_observed_at": utc_text(signal_at),
            "status": "SIGNAL_EXECUTION_READY",
        }
        positions = {
            instrument: view.positions.get(instrument, Decimal(0)) for instrument in self._instruments
        }
        has_position = any(quantity != 0 for quantity in positions.values())
        if not has_position:
            if view.state is not PaperState.FLAT or not self._eligible:
                return None
            return self._entry_intent(execution, view)
        if view.state not in {PaperState.HEDGED, PaperState.REDUCE_ONLY} or self._eligible:
            return None
        return self._exit_intent(execution, view, positions)

    def _reset(self) -> None:
        self._bars.clear()
        self._funding.clear()
        self._funding_order.clear()
        self._pending_bucket = None
        self._pending_markets.clear()
        self._seen_funding_ids.clear()
        self._last_market_received_at = None
        self._last_funding_time = None
        self._last_evaluated_at = None
        self._pending_signal_at = None
        self._eligible = False
        self._diagnostic = {
            "adapter": _IMPLEMENTATION_ID,
            "economic_status": _ECONOMIC_STATUS,
            "status": "WARMING_UP",
        }

    def _ingest_market(self, market: MarketEvent) -> bool:
        if market.instrument not in self._instruments:
            return False
        if self._pending_markets.get(market.instrument) == market:
            return False
        if self._last_market_received_at is not None and market.received_at < self._last_market_received_at:
            raise ValueError("durable Phase 05 markets must be supplied in commit-time order")
        self._last_market_received_at = market.received_at
        bucket = _hour(market.received_at)
        completed = False
        if self._pending_bucket is None:
            self._pending_bucket = bucket
        elif bucket < self._pending_bucket:
            raise ValueError("Phase 05 public market moved behind the open UTC hour")
        elif bucket > self._pending_bucket:
            completed = self._finalize_open_hour()
            if bucket != self._pending_bucket + timedelta(hours=1):
                self._diagnostic = {**self._diagnostic, "status": "MISSING_UTC_HOUR"}
                self._bars.clear()
                completed = False
            self._pending_bucket = bucket
            self._pending_markets = {}
        self._pending_markets[market.instrument] = market
        return completed

    def _finalize_open_hour(self) -> bool:
        if self._pending_bucket is None or set(self._pending_markets) != set(self._instruments):
            self._diagnostic = {**self._diagnostic, "status": "INCOMPLETE_UTC_HOUR"}
            return False
        ordered = [self._pending_markets[instrument] for instrument in self._instruments]
        skew = abs((ordered[0].received_at - ordered[1].received_at).total_seconds())
        if skew > self.config.maximum_execution_skew_seconds:
            self._diagnostic = {**self._diagnostic, "status": "HOURLY_CONTEXT_SKEW"}
            return False
        observed_at = self._pending_bucket + timedelta(hours=1)
        bar = self._bar_from_markets(ordered[0], ordered[1], observed_at)
        if self._bars and bar.observed_at <= self._bars[-1].observed_at:
            raise ValueError("Phase 05 completed bars must advance by UTC hour")
        self._bars.append(bar)
        self._diagnostic = {
            **self._diagnostic,
            "bars_retained": len(self._bars),
            "bar_observed_at": utc_text(bar.observed_at),
            "status": "BAR_COMPLETED",
        }
        return True

    def _bar_from_markets(
        self,
        spot: MarketEvent,
        perp: MarketEvent,
        bucket: datetime,
    ) -> _CompletedCarryBar:
        if spot.instrument != self.config.spot_instrument:
            spot, perp = perp, spot
        self._validate_context(
            spot,
            expected_kind="spot",
            expected_hash=self.config.spot_product_identity_sha256,
        )
        self._validate_context(
            perp,
            expected_kind="perp",
            expected_hash=self.config.perp_product_identity_sha256,
        )
        if any(event.stale or event.gap or not event.tradable for event in (spot, perp)):
            raise ValueError("Phase 05 refuses stale, gapped, or non-tradable public books")
        spot_mid = (spot.bid_price + spot.ask_price) / Decimal(2)
        perp_mid = (perp.bid_price + perp.ask_price) / Decimal(2)
        return _CompletedCarryBar(
            observed_at=bucket,
            spot_mid=spot_mid,
            perp_mid=perp_mid,
            spot_depth_usd=min(spot.bid_depth, spot.ask_depth) * spot_mid,
            perp_depth_usd=min(perp.bid_depth, perp.ask_depth) * perp_mid,
            spot_volume_usd=_context_decimal(spot, "notional_volume_24h", non_negative=True),
            perp_volume_usd=_context_decimal(perp, "notional_volume_24h", non_negative=True),
            perp_open_interest_usd=_context_decimal(perp, "open_interest_notional", non_negative=True),
            spot_spread_bps=(spot.ask_price - spot.bid_price) / spot_mid * Decimal(10_000),
            perp_spread_bps=(perp.ask_price - perp.bid_price) / perp_mid * Decimal(10_000),
            event_ids=(spot.event_id, perp.event_id),
        )

    @staticmethod
    def _validate_context(
        market: MarketEvent,
        *,
        expected_kind: str,
        expected_hash: str,
    ) -> None:
        if market.context.get("instrument_kind") != expected_kind:
            raise ValueError("public context kind differs from the frozen Phase 05 leg")
        if market.context.get("product_identity_sha256") != expected_hash:
            raise ValueError("public product identity differs from the frozen Phase 05 leg")
        observation_id = market.context.get("observation_id")
        if not isinstance(observation_id, str) or not observation_id:
            raise ValueError("public context lacks a stable observation_id")

    @staticmethod
    def _scheduled(observed_at: datetime) -> bool:
        return int(observed_at.timestamp()) // _HOUR_SECONDS % 8 == 0

    def _panel(self) -> MarketPanel:
        bars = tuple(self._bars)
        columns = list(self._instruments)
        index = pd.DatetimeIndex([bar.observed_at for bar in bars])
        prices = pd.DataFrame(
            [[float(bar.spot_mid), float(bar.perp_mid)] for bar in bars],
            index=index,
            columns=columns,
        )
        funding = pd.DataFrame(0.0, index=index, columns=columns)
        funding[self.config.perp_instrument] = [
            float(self._funding[bar.observed_at].funding_rate)
            if bar.observed_at in self._funding
            else math.nan
            for bar in bars
        ]
        spreads = pd.DataFrame(
            [[float(bar.spot_spread_bps), float(bar.perp_spread_bps)] for bar in bars],
            index=index,
            columns=columns,
        )
        volume = pd.DataFrame(
            [[float(bar.spot_volume_usd), float(bar.perp_volume_usd)] for bar in bars],
            index=index,
            columns=columns,
        )
        depth = pd.DataFrame(
            [[float(bar.spot_depth_usd), float(bar.perp_depth_usd)] for bar in bars],
            index=index,
            columns=columns,
        )
        open_interest = pd.DataFrame(
            [[0.0, float(bar.perp_open_interest_usd)] for bar in bars],
            index=index,
            columns=columns,
        )
        return MarketPanel(
            prices=prices,
            funding=funding,
            spreads_bps=spreads,
            volume_usd=volume,
            depth_usd=depth,
            open_interest_usd=open_interest,
            tradable=pd.DataFrame(True, index=index, columns=columns),
            metadata={"paper_adapter": _IMPLEMENTATION_ID, "synthetic": False},
        )

    def _evaluate_reviewed(self) -> None:
        row = self._reviewed.features(self._panel())[self.config.asset].iloc[-1]
        required = [
            "funding_72h",
            "positive_funding_share_72h",
            "basis_bps",
            "spot_depth_usd",
            "perp_depth_usd",
            "spot_volume_usd",
            "perp_volume_usd",
            "annualized_volatility",
            "open_interest_usd",
            "edge_net_8h_bps",
            "edge_net_24h_bps",
            "edge_net_72h_bps",
        ]
        complete = not bool(row[required].isna().any())
        edges = [float(row[f"edge_net_{hours}h_bps"]) for hours in _FUNDING_WINDOWS]
        gates = {
            "funding": complete
            and float(row["funding_72h"]) / 72.0 >= self._reviewed.min_mean_funding_hourly,
            "positive_share": complete
            and float(row["positive_funding_share_72h"]) >= self._reviewed.min_positive_share,
            "basis": complete and abs(float(row["basis_bps"])) <= self._reviewed.max_abs_basis_bps,
            "depth": complete
            and min(float(row["spot_depth_usd"]), float(row["perp_depth_usd"]))
            >= self._reviewed.min_depth_usd,
            "volume": complete
            and min(float(row["spot_volume_usd"]), float(row["perp_volume_usd"]))
            >= self._reviewed.min_volume_usd,
            "open_interest": complete
            and float(row["open_interest_usd"]) >= self._reviewed.min_open_interest_usd,
            "volatility": complete
            and float(row["annualized_volatility"]) <= self._reviewed.max_annualized_volatility,
            "edge": complete and min(edges) >= self._reviewed.minimum_net_edge_bps,
        }
        self._eligible = all(gates.values())
        observed_ids = self._observed_event_ids()
        self._diagnostic = {
            "adapter": _IMPLEMENTATION_ID,
            "annualized_volatility": _finite(row["annualized_volatility"]),
            "bar_observed_at": utc_text(self._bars[-1].observed_at),
            "bars_retained": len(self._bars),
            "basis_bps": _finite(row["basis_bps"]),
            "basis_convergence_bps_per_hour": _finite(row["basis_convergence_bps_per_hour"]),
            "economic_status": _ECONOMIC_STATUS,
            "edge_net_bps": {
                str(hours): _finite(row[f"edge_net_{hours}h_bps"]) for hours in _FUNDING_WINDOWS
            },
            "eligible": self._eligible,
            "funding_72h": _finite(row["funding_72h"]),
            "funding_trend_hourly": _finite(row["funding_trend_hourly"]),
            "gate_checks": gates,
            "positive_funding_share_72h": _finite(row["positive_funding_share_72h"]),
            "signal_event_count": len(observed_ids),
            "signal_input_hash": canonical_sha256(list(observed_ids)),
            "status": "SIGNAL_EVALUATED",
        }

    def _execution_snapshot(
        self,
        supplied: Mapping[str, MarketEvent],
    ) -> Mapping[str, MarketEvent] | None:
        if not set(self._instruments).issubset(supplied):
            self._diagnostic = {**self._diagnostic, "status": "INCOMPLETE_EXECUTION_FRAME"}
            return None
        selected = {instrument: supplied[instrument] for instrument in self._instruments}
        if any(event.stale or event.gap or not event.tradable for event in selected.values()):
            self._diagnostic = {**self._diagnostic, "status": "UNSAFE_EXECUTION_FRAME"}
            return None
        skew = abs(
            (
                selected[self.config.spot_instrument].received_at
                - selected[self.config.perp_instrument].received_at
            ).total_seconds()
        )
        if skew > self.config.maximum_execution_skew_seconds:
            self._diagnostic = {**self._diagnostic, "status": "EXECUTION_FRAME_SKEW"}
            return None
        return MappingProxyType(selected)

    def _entry_intent(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        anchor = self._anchor(markets)
        signal = {**self._diagnostic, "status": "ENTRY_READY"}
        decision_id = DecisionIntent.identifier(
            run_id=view.run_id,
            strategy_id=self.strategy_id,
            market_event_id=anchor.event_id,
            action=DecisionAction.ENTRY,
            ordinal=0,
            signal=signal,
        )
        target_per_leg = self.config.maximum_gross_notional / Decimal(2)
        specifications = (
            (self.config.spot_instrument, OrderSide.BUY, 1),
            (self.config.perp_instrument, OrderSide.SELL, 2),
        )
        hedge_group_id = f"carry:{self.config.asset}:{decision_id[:16]}"
        orders: list[OrderIntent] = []
        for ordinal, (instrument, side, leg_number) in enumerate(specifications):
            market = markets[instrument]
            adverse_price = market.ask_price if side is OrderSide.BUY else market.bid_price
            limit_price = market.bid_price if side is OrderSide.BUY else market.ask_price
            step, maximum = self._quantity_bounds(instrument)
            raw_quantity = target_per_leg / adverse_price
            quantity = (raw_quantity / step).to_integral_value(rounding=ROUND_DOWN) * step
            if quantity <= 0 or quantity > maximum:
                self._diagnostic = {**self._diagnostic, "status": "QUANTITY_BOUND_BLOCKED"}
                return None
            orders.append(
                OrderIntent.create(
                    decision_id=decision_id,
                    run_id=view.run_id,
                    strategy_id=self.strategy_id,
                    instrument=instrument,
                    side=side,
                    quantity=quantity,
                    order_type=PaperOrderType.MAKER,
                    time_in_force=TimeInForce.GTC,
                    created_at=anchor.received_at,
                    ordinal=ordinal,
                    limit_price=limit_price,
                    reduce_only=False,
                    hedge_group_id=hedge_group_id,
                    leg_number=leg_number,
                )
            )
        self._diagnostic = signal
        return self._decision(
            view=view,
            markets=markets,
            anchor=anchor,
            action=DecisionAction.ENTRY,
            decision_id=decision_id,
            orders=tuple(orders),
            signal=signal,
        )

    def _exit_intent(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
        positions: Mapping[str, Decimal],
    ) -> DecisionIntent | None:
        anchor = self._anchor(markets)
        signal = {**self._diagnostic, "status": "EXIT_READY"}
        decision_id = DecisionIntent.identifier(
            run_id=view.run_id,
            strategy_id=self.strategy_id,
            market_event_id=anchor.event_id,
            action=DecisionAction.EXIT,
            ordinal=0,
            signal=signal,
        )
        hedge_group_id = f"carry:{self.config.asset}:{decision_id[:16]}"
        orders = tuple(
            OrderIntent.create(
                decision_id=decision_id,
                run_id=view.run_id,
                strategy_id=self.strategy_id,
                instrument=instrument,
                side=OrderSide.SELL if quantity > 0 else OrderSide.BUY,
                quantity=abs(quantity),
                order_type=PaperOrderType.MAKER,
                time_in_force=TimeInForce.GTC,
                created_at=anchor.received_at,
                ordinal=ordinal,
                limit_price=(
                    markets[instrument].ask_price if quantity > 0 else markets[instrument].bid_price
                ),
                reduce_only=True,
                hedge_group_id=hedge_group_id,
                leg_number=ordinal + 1,
            )
            for ordinal, (instrument, quantity) in enumerate(positions.items())
            if quantity != 0
        )
        if not orders:
            return None
        self._diagnostic = signal
        return self._decision(
            view=view,
            markets=markets,
            anchor=anchor,
            action=DecisionAction.EXIT,
            decision_id=decision_id,
            orders=orders,
            signal=signal,
        )

    def _decision(
        self,
        *,
        view: PaperStrategyView,
        markets: Mapping[str, MarketEvent],
        anchor: MarketEvent,
        action: DecisionAction,
        decision_id: str,
        orders: tuple[OrderIntent, ...],
        signal: Mapping[str, object],
    ) -> DecisionIntent:
        return DecisionIntent(
            decision_id=decision_id,
            run_id=view.run_id,
            strategy_id=self.strategy_id,
            strategy_name=self.strategy_name,
            strategy_hash=self.strategy_hash,
            strategy_config_hash=self.strategy_config_hash,
            action=action,
            decided_at=anchor.received_at,
            received_at=anchor.received_at,
            market_event_id=anchor.event_id,
            observed_event_ids=self._observed_event_ids(markets),
            orders=orders,
            ordinal=0,
            signal=signal,
        )

    def _observed_event_ids(
        self,
        markets: Mapping[str, MarketEvent] | None = None,
    ) -> tuple[str, ...]:
        candidates = [event_id for bar in self._bars for event_id in bar.event_ids]
        funding_cutoff = self._bars[-1].observed_at if self._bars else None
        candidates.extend(
            self._funding[bucket].event_id
            for bucket in self._funding_order
            if bucket in self._funding and funding_cutoff is not None and bucket <= funding_cutoff
        )
        if markets is not None:
            candidates.extend(
                markets[instrument].event_id for instrument in self._instruments if instrument in markets
            )
        return tuple(dict.fromkeys(candidates))

    @staticmethod
    def _anchor(markets: Mapping[str, MarketEvent]) -> MarketEvent:
        return max(
            markets.values(),
            key=lambda item: (item.received_at, item.capture_ordinal, item.event_id),
        )

    def _quantity_bounds(self, instrument: str) -> tuple[Decimal, Decimal]:
        if instrument == self.config.spot_instrument:
            return self.config.spot_quantity_step, self.config.spot_max_quantity
        return self.config.perp_quantity_step, self.config.perp_max_quantity


__all__ = [
    "PHASE05_CARRY_STRATEGY_ID",
    "FrozenCashAndCarryPaperConfig",
    "FrozenCashAndCarryPaperStrategy",
    "make_phase05_paper_strategy_config",
]
