from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Protocol

from hyperlab.paper.engine import PaperCommandResult, PaperEngine
from hyperlab.paper.models import (
    DecisionIntent,
    MarketEvent,
    MarketExecutionPolicy,
    PaperProjection,
    PaperState,
)


@dataclass(frozen=True, slots=True)
class PaperStrategyView:
    """Read-only strategy view; it deliberately exposes no store or fill mutator."""

    run_id: str
    config_hash: str
    state: PaperState
    positions: Mapping[str, Decimal]
    marks: Mapping[str, Decimal]
    completed_cycles: int

    @classmethod
    def from_projection(cls, projection: PaperProjection) -> PaperStrategyView:
        return cls(
            run_id=projection.run_id,
            config_hash=projection.config_hash,
            state=projection.state,
            positions=MappingProxyType(dict(projection.positions)),
            marks=MappingProxyType(dict(projection.marks)),
            completed_cycles=projection.completed_cycles,
        )


class FrozenPaperStrategy(Protocol):
    strategy_name: str
    strategy_hash: str

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None: ...


@dataclass(frozen=True, slots=True)
class PaperRunnerResult:
    market_results: tuple[PaperCommandResult, ...]
    decision_result: PaperCommandResult | None

    @property
    def projection(self) -> PaperProjection:
        if self.decision_result is not None:
            return self.decision_result.projection
        if not self.market_results:
            raise ValueError("paper runner result contains no command")
        return self.market_results[-1].projection


class PaperRunner:
    """Route a frozen strategy through the single simulated execution gateway."""

    def __init__(self, engine: PaperEngine, strategy: FrozenPaperStrategy) -> None:
        if strategy.strategy_name != engine.config.strategy_name:
            raise ValueError("strategy name differs from the frozen paper configuration")
        if strategy.strategy_hash != engine.config.strategy_hash:
            raise ValueError("strategy implementation hash differs from the frozen configuration")
        self.engine = engine
        self._strategy = strategy

    def process_frame(
        self,
        markets: Mapping[str, MarketEvent],
        *,
        decision_markets: Mapping[str, MarketEvent] | None = None,
        processed_at: datetime | None = None,
        execution_policies: Mapping[
            str, MarketExecutionPolicy | str
        ] | None = None,
    ) -> PaperRunnerResult:
        if not markets or any(key != value.instrument for key, value in markets.items()):
            raise ValueError("paper runner markets must be keyed by canonical instrument")
        decision_frame = dict(markets if decision_markets is None else decision_markets)
        if not decision_frame or any(
            key != value.instrument for key, value in decision_frame.items()
        ):
            raise ValueError("paper decision markets must be keyed by canonical instrument")
        for instrument, event in markets.items():
            if decision_frame.get(instrument) != event:
                raise ValueError("paper decision frame must contain every fresh market event")
        processed = processed_at or max(event.received_at for event in markets.values())
        if any(processed < event.received_at for event in markets.values()):
            raise ValueError("paper frame processing time precedes a source market event")
        if execution_policies is None:
            policies = {
                instrument: MarketExecutionPolicy.EXECUTE for instrument in markets
            }
        else:
            if set(execution_policies) != set(markets):
                raise ValueError("paper execution policies must exactly cover fresh markets")
            policies = {
                instrument: MarketExecutionPolicy(value)
                for instrument, value in execution_policies.items()
            }
        ordered = sorted(
            markets.values(),
            key=lambda item: (item.received_at, item.capture_ordinal, item.event_id),
        )
        if len({(item.received_at, item.capture_ordinal) for item in ordered}) != len(ordered):
            raise ValueError("equal-time paper frames require unique capture ordinals")
        market_results = tuple(
            self.engine.process_market(
                market,
                processed_at=processed,
                execution_policy=policies[market.instrument],
            )
            for market in ordered
        )
        projection = market_results[-1].projection
        if any(policy is not MarketExecutionPolicy.EXECUTE for policy in policies.values()):
            return PaperRunnerResult(market_results, None)
        decision = self._strategy.decide(
            MappingProxyType(decision_frame),
            PaperStrategyView.from_projection(projection),
        )
        if decision is None:
            return PaperRunnerResult(market_results, None)
        if decision.decided_at > processed:
            raise ValueError("strategy decision time is ahead of frame processing time")
        decision = replace(
            decision,
            decided_at=processed,
            orders=tuple(
                replace(order, created_at=processed) for order in decision.orders
            ),
        )
        result = self.engine.submit_decision(
            decision,
            decision_frame,
            processed_at=processed,
        )
        return PaperRunnerResult(market_results, result)


__all__ = [
    "FrozenPaperStrategy",
    "PaperRunner",
    "PaperRunnerResult",
    "PaperStrategyView",
]
