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
    utc_text,
)
from hyperlab.paper.runner import PaperStrategyView
from hyperlab.strategies.pairs import HedgeMethod, PairModel, RobustPairsStrategy

_IMPLEMENTATION_ID = "phase08-robust-pairs-paper-adapter-v2"
_PAIR_ID = "phase08-robust-eth-btc"
PHASE08_PAIRS_STRATEGY_ID = "phase08_robust_pairs"


@dataclass(frozen=True, slots=True)
class FrozenRobustPairsPaperConfig:
    """Frozen, technical-only parameters for the first Phase 08 Paper adapter.

    The selection scores are deliberately zero: this adapter binds executable
    semantics, not an economic validation result.  A later validation run must
    supply independently admitted model parameters and a different strategy hash.
    """

    asset_a: str = "HL:ETH:perp"
    asset_b: str = "HL:BTC:perp"
    model_method: str = "rolling"
    hedge_ratio: float = 1.0
    intercept: float = 0.0
    lookback_bars: int = 12
    bar_seconds: int = 30
    enter_z: float = 2.0
    exit_z: float = 0.5
    stop_z: float = 4.0
    max_holding_bars: int = 120
    cooldown_bars: int = 12
    volatility_lookback_bars: int = 12
    target_spread_volatility: float = 0.01
    maximum_pair_gross: float = 0.20
    maximum_gross_notional: Decimal = Decimal("250")
    asset_a_quantity_step: Decimal = Decimal("0.001")
    asset_b_quantity_step: Decimal = Decimal("0.00001")
    asset_a_max_quantity: Decimal = Decimal("0.25")
    asset_b_max_quantity: Decimal = Decimal("0.01")
    maximum_close_skew_seconds: int = 2
    maximum_execution_skew_seconds: int = 2
    retained_bars: int = 256

    def __post_init__(self) -> None:
        model = self.pair_model()
        RobustPairsStrategy(
            models=(model,),
            enter_z=self.enter_z,
            exit_z=self.exit_z,
            stop_z=self.stop_z,
            max_holding_bars=self.max_holding_bars,
            cooldown_bars=self.cooldown_bars,
            volatility_lookback_bars=self.volatility_lookback_bars,
            target_spread_volatility=self.target_spread_volatility,
            maximum_pair_gross=self.maximum_pair_gross,
        )
        if self.asset_a != "HL:ETH:perp" or self.asset_b != "HL:BTC:perp":
            raise ValueError("the first Paper adapter is frozen to the ETH/BTC Hyperliquid pair")
        if self.bar_seconds < 1:
            raise ValueError("bar_seconds must be positive")
        for name in ("maximum_close_skew_seconds", "maximum_execution_skew_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        minimum_retained = (
            max(self.lookback_bars * 2 + 2, self.volatility_lookback_bars + 2)
            + self.max_holding_bars
            + self.cooldown_bars
        )
        if self.retained_bars < minimum_retained:
            raise ValueError("retained_bars must cover feature warm-up, maximum holding, and cooldown")
        for name in (
            "maximum_gross_notional",
            "asset_a_quantity_step",
            "asset_b_quantity_step",
            "asset_a_max_quantity",
            "asset_b_max_quantity",
        ):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be a positive finite Decimal")

    def pair_model(self) -> PairModel:
        return PairModel(
            asset_a=self.asset_a,
            asset_b=self.asset_b,
            method=cast(HedgeMethod, self.model_method),
            hedge_ratio=self.hedge_ratio,
            intercept=self.intercept,
            lookback_bars=self.lookback_bars,
            train_score=0.0,
            validation_score=0.0,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "asset_a": self.asset_a,
            "asset_a_max_quantity": str(self.asset_a_max_quantity),
            "asset_a_quantity_step": str(self.asset_a_quantity_step),
            "asset_b": self.asset_b,
            "asset_b_max_quantity": str(self.asset_b_max_quantity),
            "asset_b_quantity_step": str(self.asset_b_quantity_step),
            "bar_seconds": self.bar_seconds,
            "cooldown_bars": self.cooldown_bars,
            "enter_z": self.enter_z,
            "exit_z": self.exit_z,
            "hedge_ratio": self.hedge_ratio,
            "intercept": self.intercept,
            "lookback_bars": self.lookback_bars,
            "max_holding_bars": self.max_holding_bars,
            "maximum_close_skew_seconds": self.maximum_close_skew_seconds,
            "maximum_execution_skew_seconds": self.maximum_execution_skew_seconds,
            "maximum_gross_notional": str(self.maximum_gross_notional),
            "maximum_pair_gross": self.maximum_pair_gross,
            "model_method": self.model_method,
            "retained_bars": self.retained_bars,
            "stop_z": self.stop_z,
            "target_spread_volatility": self.target_spread_volatility,
            "volatility_lookback_bars": self.volatility_lookback_bars,
        }


def _pairs_strategy_hash(config: FrozenRobustPairsPaperConfig) -> str:
    return canonical_sha256(
        {
            "adapter": _IMPLEMENTATION_ID,
            "configuration": config.to_dict(),
            "reviewed_strategy": "pairs_mean_reversion_phase08",
            "semantics": "completed_bar_pending_until_complete_post_bar_pair_frame_ioc_v2",
        }
    )


def make_phase08_paper_strategy_config(
    *,
    config: FrozenRobustPairsPaperConfig,
    risk: PaperRiskLimits,
) -> PaperStrategyConfig:
    return PaperStrategyConfig(
        strategy_id=PHASE08_PAIRS_STRATEGY_ID,
        strategy_name="pairs_mean_reversion_phase08",
        strategy_hash=_pairs_strategy_hash(config),
        parameters={
            "economic_status": "TECHNICAL_ONLY_NOT_VALIDATED",
            "phase": "08",
            "reviewed_config": config.to_dict(),
        },
        risk=risk,
        required_instruments=(config.asset_a, config.asset_b),
    )


@dataclass(frozen=True, slots=True)
class _CompletedBar:
    ended_at: datetime
    prices: Mapping[str, Decimal]
    close_event_ids: tuple[str, str]


class FrozenRobustPairsPaperStrategy:
    """Paper-only gateway from public BBO bars to simulated IOC intents.

    Every target is produced by :class:`RobustPairsStrategy`.  The reviewed
    strategy is evaluated over one frozen, bounded rolling window.  This window
    definition is part of ``strategy_hash`` and is therefore also the restart
    contract: ``restore`` may stream the complete durable inbox while this class
    retains no more than ``retained_bars`` completed observations.
    """

    strategy_name = "pairs_mean_reversion_phase08"

    def __init__(
        self,
        config: FrozenRobustPairsPaperConfig | None = None,
        *,
        strategy_config: PaperStrategyConfig | None = None,
    ) -> None:
        self.config = config or FrozenRobustPairsPaperConfig()
        self.strategy_id = PHASE08_PAIRS_STRATEGY_ID
        self._bound_strategy_id: str | None = None
        self._model = self.config.pair_model()
        self._reviewed = RobustPairsStrategy(
            models=(self._model,),
            enter_z=self.config.enter_z,
            exit_z=self.config.exit_z,
            stop_z=self.config.stop_z,
            max_holding_bars=self.config.max_holding_bars,
            cooldown_bars=self.config.cooldown_bars,
            volatility_lookback_bars=self.config.volatility_lookback_bars,
            target_spread_volatility=self.config.target_spread_volatility,
            maximum_pair_gross=self.config.maximum_pair_gross,
        )
        self.strategy_hash = _pairs_strategy_hash(self.config)
        self.strategy_config_hash: str | None = None
        if strategy_config is not None:
            expected = make_phase08_paper_strategy_config(
                config=self.config,
                risk=strategy_config.risk,
            )
            if strategy_config != expected:
                raise ValueError("Phase 08 adapter differs from its frozen strategy configuration")
            self._bound_strategy_id = strategy_config.strategy_id
            self.strategy_config_hash = strategy_config.strategy_config_hash
        self._bars: deque[_CompletedBar] = deque(maxlen=self.config.retained_bars)
        self._latest: dict[str, MarketEvent] = {}
        self._open_bucket_start: datetime | None = None
        self._open_closes: dict[str, MarketEvent] = {}
        self._open_invalid = False
        self._last_received_at: datetime | None = None
        self._last_target: dict[str, float] = {
            self.config.asset_a: 0.0,
            self.config.asset_b: 0.0,
        }
        self._pending_signal_bar_ended_at: datetime | None = None
        self._diagnostic: dict[str, object] = {
            "adapter": _IMPLEMENTATION_ID,
            "bars_retained": 0,
            "economic_status": "TECHNICAL_ONLY_NOT_VALIDATED",
            "status": "WARMING_UP",
        }

    @property
    def diagnostic_snapshot(self) -> Mapping[str, object]:
        return MappingProxyType(dict(self._diagnostic))

    def restore(
        self,
        markets: Iterable[MarketEvent],
        view: PaperStrategyView | None = None,
    ) -> None:
        """Reconstruct state from durable events without emitting old decisions.

        ``markets`` is intentionally an ``Iterable`` so a SQLite cursor can feed
        it without materialising the journal.  ``view`` is accepted for runtime
        protocol symmetry; positions remain authoritative in each later
        ``decide`` call and are never inferred from public prices.
        """

        self._reset_rolling_state()
        self.restore_incremental(markets, view)
        self._diagnostic = {
            **self._diagnostic,
            "bars_retained": len(self._bars),
            "status": "RESTORED",
        }

    def restore_incremental(
        self,
        markets: Iterable[MarketEvent],
        view: PaperStrategyView | None = None,
    ) -> None:
        """Apply a durable suffix and evaluate only its final bounded window."""

        del view
        completed = False
        for market in markets:
            completed = self._ingest(market) or completed
        if completed and self._bars:
            self._evaluate_reviewed_strategy()
        self._pending_signal_bar_ended_at = None
        self._diagnostic = {
            **self._diagnostic,
            "bars_retained": len(self._bars),
            "status": "RESTORED_INCREMENTAL",
        }

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        if not markets:
            raise ValueError("the robust-pairs Paper adapter requires a non-empty frame")
        if any(key != market.instrument for key, market in markets.items()):
            raise ValueError("the robust-pairs frame must use canonical instrument keys")
        pair_markets = {
            instrument: market
            for instrument, market in markets.items()
            if instrument in self._instruments
        }
        if not pair_markets:
            return None

        completed = False
        for market in sorted(
            pair_markets.values(),
            key=lambda item: (item.received_at, item.capture_ordinal, item.event_id),
        ):
            completed = self._ingest(market) or completed
        if completed:
            self._evaluate_reviewed_strategy()
            self._pending_signal_bar_ended_at = self._bars[-1].ended_at
        signal_bar_ended_at = self._pending_signal_bar_ended_at
        if signal_bar_ended_at is None:
            return None

        execution = self._execution_snapshot(pair_markets)
        if execution is None:
            return None
        if any(
            market.received_at < signal_bar_ended_at
            for market in execution.values()
        ):
            self._diagnostic = {
                **self._diagnostic,
                "status": "WAITING_FOR_COMPLETE_POST_BAR_EXECUTION_FRAME",
            }
            return None
        if view.run_id.strip() == "" or view.config_hash.strip() == "":
            raise ValueError("Paper strategy view lacks its frozen identity")

        self._pending_signal_bar_ended_at = None
        self._diagnostic = {
            **self._diagnostic,
            "execution_event_ids": self._observed_event_ids(execution)[-2:],
            "signal_bar_ended_at": utc_text(signal_bar_ended_at),
            "status": "SIGNAL_EXECUTION_READY",
        }
        positions = {
            instrument: view.positions.get(instrument, Decimal(0)) for instrument in self._instruments
        }
        has_position = any(quantity != 0 for quantity in positions.values())
        target_active = all(abs(self._last_target[instrument]) > 1e-12 for instrument in self._instruments)

        if not has_position:
            if view.state is not PaperState.FLAT or not target_active:
                return None
            return self._entry_intent(execution, view)

        if view.state not in {PaperState.HEDGED, PaperState.REDUCE_ONLY}:
            return None
        same_direction = target_active and all(
            positions[instrument] == 0 or (positions[instrument] > 0) == (self._last_target[instrument] > 0)
            for instrument in self._instruments
        )
        if same_direction:
            return None
        return self._exit_intent(execution, view, positions)

    @property
    def _instruments(self) -> tuple[str, str]:
        return (self.config.asset_a, self.config.asset_b)

    def _reset_rolling_state(self) -> None:
        self._bars.clear()
        self._latest.clear()
        self._open_bucket_start = None
        self._open_closes.clear()
        self._open_invalid = False
        self._last_received_at = None
        self._last_target = {instrument: 0.0 for instrument in self._instruments}
        self._pending_signal_bar_ended_at = None
        self._diagnostic = {
            "adapter": _IMPLEMENTATION_ID,
            "bars_retained": 0,
            "economic_status": "TECHNICAL_ONLY_NOT_VALIDATED",
            "status": "WARMING_UP",
        }

    def _bucket_start(self, received_at: datetime) -> datetime:
        epoch = int(received_at.timestamp())
        return datetime.fromtimestamp(
            epoch - (epoch % self.config.bar_seconds),
            tz=UTC,
        )

    def _ingest(self, market: MarketEvent) -> bool:
        if market.instrument not in self._instruments:
            raise ValueError("durable reconstruction contains an instrument outside the frozen pair")
        if self._latest.get(market.instrument) == market:
            return False
        if self._last_received_at is not None and market.received_at < self._last_received_at:
            raise ValueError("durable Paper market events must be supplied in commit-time order")
        self._last_received_at = market.received_at

        bucket_start = self._bucket_start(market.received_at)
        completed = False
        if self._open_bucket_start is None:
            self._open_bucket_start = bucket_start
        elif bucket_start < self._open_bucket_start:
            raise ValueError("market event moved behind the open UTC bar")
        elif bucket_start > self._open_bucket_start:
            expected = self._open_bucket_start + timedelta(seconds=self.config.bar_seconds)
            completed = self._finalize_open_bucket()
            if bucket_start != expected:
                self._clear_after_discontinuity("MISSING_UTC_BAR")
                completed = False
            self._open_bucket_start = bucket_start
            self._open_closes = {}
            self._open_invalid = False

        self._latest[market.instrument] = market
        if market.stale or market.gap or not market.tradable:
            self._open_invalid = True
            self._pending_signal_bar_ended_at = None
            self._diagnostic = {
                **self._diagnostic,
                "status": "UNSAFE_PUBLIC_MARKET_EVENT",
                "unsafe_event_id": market.event_id,
            }
        else:
            self._open_closes[market.instrument] = market
        return completed

    def _finalize_open_bucket(self) -> bool:
        if self._open_bucket_start is None:
            return False
        if self._open_invalid or set(self._open_closes) != set(self._instruments):
            self._clear_after_discontinuity("INVALID_OR_INCOMPLETE_UTC_BAR")
            return False
        close_a = self._open_closes[self.config.asset_a]
        close_b = self._open_closes[self.config.asset_b]
        skew = abs((close_a.received_at - close_b.received_at).total_seconds())
        if skew > self.config.maximum_close_skew_seconds:
            self._clear_after_discontinuity("BAR_CLOSE_SKEW")
            return False

        ended_at = self._open_bucket_start + timedelta(seconds=self.config.bar_seconds)
        if self._bars and self._bars[-1].ended_at != self._open_bucket_start:
            self._bars.clear()
        bar = _CompletedBar(
            ended_at=ended_at,
            prices=MappingProxyType(
                {
                    self.config.asset_a: (close_a.bid_price + close_a.ask_price) / Decimal(2),
                    self.config.asset_b: (close_b.bid_price + close_b.ask_price) / Decimal(2),
                }
            ),
            close_event_ids=(close_a.event_id, close_b.event_id),
        )
        self._bars.append(bar)
        self._diagnostic = {
            **self._diagnostic,
            "bar_ended_at": utc_text(ended_at),
            "bars_retained": len(self._bars),
            "status": "BAR_COMPLETED",
        }
        return True

    def _clear_after_discontinuity(self, reason: str) -> None:
        self._bars.clear()
        self._last_target = {instrument: 0.0 for instrument in self._instruments}
        self._pending_signal_bar_ended_at = None
        self._diagnostic = {
            **self._diagnostic,
            "bars_retained": 0,
            "status": reason,
        }

    def _panel(self) -> MarketPanel:
        index = pd.DatetimeIndex([bar.ended_at for bar in self._bars], tz="UTC")
        prices = pd.DataFrame(
            [
                {instrument: float(bar.prices[instrument]) for instrument in self._instruments}
                for bar in self._bars
            ],
            index=index,
            columns=list(self._instruments),
            dtype=float,
        )
        zeroes = pd.DataFrame(0.0, index=index, columns=list(self._instruments))
        tradable = pd.DataFrame(True, index=index, columns=list(self._instruments))
        return MarketPanel(
            prices=prices,
            funding=zeroes.copy(),
            spreads_bps=zeroes.copy(),
            volume_usd=zeroes.copy(),
            tradable=tradable,
            metadata={
                "paper_adapter": _IMPLEMENTATION_ID,
                "synthetic": False,
            },
        )

    def _evaluate_reviewed_strategy(self) -> None:
        panel = self._panel()
        output = self._reviewed.generate(panel)
        weights = output.weights.iloc[-1]
        self._last_target = {instrument: float(weights[instrument]) for instrument in self._instruments}
        features = self._reviewed.features(panel)[self._model.pair_id].iloc[-1]

        def finite_or_none(value: object) -> float | None:
            converted = float(cast(float, value))
            return converted if math.isfinite(converted) else None

        self._diagnostic = {
            "adapter": _IMPLEMENTATION_ID,
            "bar_ended_at": utc_text(self._bars[-1].ended_at),
            "bars_retained": len(self._bars),
            "economic_status": "TECHNICAL_ONLY_NOT_VALIDATED",
            "hedge_ratio": finite_or_none(features["hedge_ratio"]),
            "intercept": finite_or_none(features["intercept"]),
            "spread": finite_or_none(features["spread"]),
            "spread_volatility": finite_or_none(features["spread_volatility"]),
            "signal_event_count": len(self._bars) * 2,
            "signal_input_hash": canonical_sha256(
                [event_id for bar in self._bars for event_id in bar.close_event_ids]
            ),
            "status": "SIGNAL_EVALUATED",
            "target_weights": dict(sorted(self._last_target.items())),
            "zscore": finite_or_none(features["zscore"]),
        }

    def _execution_snapshot(
        self,
        supplied: Mapping[str, MarketEvent],
    ) -> Mapping[str, MarketEvent] | None:
        if set(supplied) != set(self._instruments):
            self._diagnostic = {**self._diagnostic, "status": "INCOMPLETE_EXECUTION_FRAME"}
            return None
        ordered = [supplied[instrument] for instrument in self._instruments]
        if any(market.stale or market.gap or not market.tradable for market in ordered):
            self._diagnostic = {**self._diagnostic, "status": "UNSAFE_EXECUTION_FRAME"}
            return None
        skew = abs((ordered[0].received_at - ordered[1].received_at).total_seconds())
        if skew > self.config.maximum_execution_skew_seconds:
            self._diagnostic = {**self._diagnostic, "status": "EXECUTION_FRAME_SKEW"}
            return None
        return MappingProxyType(dict(supplied))

    def _entry_intent(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        anchor = self._anchor(markets)
        decision_id = DecisionIntent.identifier(
            run_id=view.run_id,
            market_event_id=anchor.event_id,
            action=DecisionAction.ENTRY,
            ordinal=0,
            signal=self.diagnostic_snapshot,
            strategy_id=self._bound_strategy_id,
        )
        capital = self.config.maximum_gross_notional / Decimal(str(self.config.maximum_pair_gross))
        orders: list[OrderIntent] = []
        for ordinal, instrument in enumerate(self._instruments):
            weight = Decimal(str(abs(self._last_target[instrument])))
            side = OrderSide.BUY if self._last_target[instrument] > 0 else OrderSide.SELL
            market = markets[instrument]
            executable_price = market.ask_price if side is OrderSide.BUY else market.bid_price
            step, maximum = self._quantity_bounds(instrument)
            raw_quantity = weight * capital / executable_price
            if raw_quantity > maximum:
                self._diagnostic = {
                    **self._diagnostic,
                    "status": "ENTRY_EXCEEDS_FROZEN_QUANTITY_CAP",
                }
                return None
            quantity = (raw_quantity / step).to_integral_value(rounding=ROUND_DOWN) * step
            if quantity <= 0:
                self._diagnostic = {
                    **self._diagnostic,
                    "status": "ENTRY_BELOW_FROZEN_QUANTITY_STEP",
                }
                return None
            orders.append(
                OrderIntent.create(
                    decision_id=decision_id,
                    run_id=view.run_id,
                    strategy_id=self._bound_strategy_id,
                    instrument=instrument,
                    side=side,
                    quantity=quantity,
                    order_type=PaperOrderType.TAKER,
                    time_in_force=TimeInForce.IOC,
                    created_at=anchor.received_at,
                    ordinal=ordinal,
                    reduce_only=False,
                    hedge_group_id=_PAIR_ID,
                    leg_number=ordinal + 1,
                )
            )
        return DecisionIntent(
            decision_id=decision_id,
            run_id=view.run_id,
            strategy_id=self._bound_strategy_id,
            strategy_name=self.strategy_name,
            strategy_hash=(self.strategy_hash if self._bound_strategy_id is not None else None),
            strategy_config_hash=(
                self.strategy_config_hash if self._bound_strategy_id is not None else None
            ),
            action=DecisionAction.ENTRY,
            decided_at=anchor.received_at,
            received_at=anchor.received_at,
            market_event_id=anchor.event_id,
            observed_event_ids=self._observed_event_ids(markets),
            orders=tuple(orders),
            ordinal=0,
            signal=self.diagnostic_snapshot,
        )

    def _exit_intent(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
        positions: Mapping[str, Decimal],
    ) -> DecisionIntent | None:
        anchor = self._anchor(markets)
        decision_id = DecisionIntent.identifier(
            run_id=view.run_id,
            market_event_id=anchor.event_id,
            action=DecisionAction.EXIT,
            ordinal=0,
            signal=self.diagnostic_snapshot,
            strategy_id=self._bound_strategy_id,
        )
        orders = tuple(
            OrderIntent.create(
                decision_id=decision_id,
                run_id=view.run_id,
                strategy_id=self._bound_strategy_id,
                instrument=instrument,
                side=OrderSide.SELL if positions[instrument] > 0 else OrderSide.BUY,
                quantity=abs(positions[instrument]),
                order_type=PaperOrderType.TAKER,
                time_in_force=TimeInForce.IOC,
                created_at=anchor.received_at,
                ordinal=ordinal,
                reduce_only=True,
                hedge_group_id=_PAIR_ID,
                leg_number=ordinal + 1,
            )
            for ordinal, instrument in enumerate(self._instruments)
            if positions[instrument] != 0
        )
        if not orders:
            return None
        return DecisionIntent(
            decision_id=decision_id,
            run_id=view.run_id,
            strategy_id=self._bound_strategy_id,
            strategy_name=self.strategy_name,
            strategy_hash=(self.strategy_hash if self._bound_strategy_id is not None else None),
            strategy_config_hash=(
                self.strategy_config_hash if self._bound_strategy_id is not None else None
            ),
            action=DecisionAction.EXIT,
            decided_at=anchor.received_at,
            received_at=anchor.received_at,
            market_event_id=anchor.event_id,
            observed_event_ids=self._observed_event_ids(markets),
            orders=orders,
            ordinal=0,
            signal=self.diagnostic_snapshot,
        )

    def _anchor(self, markets: Mapping[str, MarketEvent]) -> MarketEvent:
        return max(
            markets.values(),
            key=lambda item: (item.received_at, item.capture_ordinal, item.event_id),
        )

    def _observed_event_ids(self, markets: Mapping[str, MarketEvent]) -> tuple[str, ...]:
        candidates = [
            *(event_id for bar in self._bars for event_id in bar.close_event_ids),
            *(
                markets[instrument].event_id
                for instrument in self._instruments
                if instrument in markets
            ),
        ]
        return tuple(dict.fromkeys(candidates))

    def _quantity_bounds(self, instrument: str) -> tuple[Decimal, Decimal]:
        if instrument == self.config.asset_a:
            return self.config.asset_a_quantity_step, self.config.asset_a_max_quantity
        return self.config.asset_b_quantity_step, self.config.asset_b_max_quantity


__all__ = [
    "PHASE08_PAIRS_STRATEGY_ID",
    "FrozenRobustPairsPaperConfig",
    "FrozenRobustPairsPaperStrategy",
    "make_phase08_paper_strategy_config",
]
