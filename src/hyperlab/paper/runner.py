from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Protocol

from hyperlab.paper.engine import PaperCommandResult, PaperEngine
from hyperlab.paper.models import DecisionIntent, MarketEvent, PaperProjection, PaperState


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

    def process_frame(self, markets: Mapping[str, MarketEvent]) -> PaperRunnerResult:
        if not markets or any(key != value.instrument for key, value in markets.items()):
            raise ValueError("paper runner markets must be keyed by canonical instrument")
        ordered = sorted(
            markets.values(),
            key=lambda item: (item.received_at, item.capture_ordinal, item.event_id),
        )
        if len({(item.received_at, item.capture_ordinal) for item in ordered}) != len(ordered):
            raise ValueError("equal-time paper frames require unique capture ordinals")
        market_results = tuple(self.engine.process_market(market) for market in ordered)
        projection = market_results[-1].projection
        decision = self._strategy.decide(
            MappingProxyType(dict(markets)),
            PaperStrategyView.from_projection(projection),
        )
        if decision is None:
            return PaperRunnerResult(market_results, None)
        result = self.engine.submit_decision(decision, markets)
        return PaperRunnerResult(market_results, result)


__all__ = [
    "FrozenPaperStrategy",
    "PaperRunner",
    "PaperRunnerResult",
    "PaperStrategyView",
]
