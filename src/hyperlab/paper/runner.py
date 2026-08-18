from __future__ import annotations

from collections.abc import Iterable, Mapping
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
    PaperStrategyConfig,
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
    strategy_id: str | None = None
    strategy_name: str | None = None
    strategy_hash: str | None = None
    strategy_config_hash: str | None = None

    @classmethod
    def from_projection(
        cls,
        projection: PaperProjection,
        strategy: PaperStrategyConfig | None = None,
    ) -> PaperStrategyView:
        if strategy is not None:
            owned = projection.strategy_projection(strategy.strategy_id)
            return cls(
                run_id=projection.run_id,
                config_hash=projection.config_hash,
                state=owned.state,
                positions=MappingProxyType(dict(owned.positions)),
                marks=MappingProxyType(dict(projection.marks)),
                completed_cycles=owned.completed_cycles,
                strategy_id=strategy.strategy_id,
                strategy_name=strategy.strategy_name,
                strategy_hash=strategy.strategy_hash,
                strategy_config_hash=strategy.strategy_config_hash,
            )
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
    strategy_results: tuple[PaperStrategyResult, ...] = ()

    @property
    def projection(self) -> PaperProjection:
        if self.decision_result is not None:
            return self.decision_result.projection
        if not self.market_results:
            raise ValueError("paper runner result contains no command")
        return self.market_results[-1].projection


@dataclass(frozen=True, slots=True)
class PaperStrategyResult:
    strategy_id: str
    status: str
    decision_result: PaperCommandResult | None = None
    failure_result: PaperCommandResult | None = None


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
        execution_policies: Mapping[str, MarketExecutionPolicy | str] | None = None,
    ) -> PaperRunnerResult:
        if not markets or any(key != value.instrument for key, value in markets.items()):
            raise ValueError("paper runner markets must be keyed by canonical instrument")
        decision_frame = dict(markets if decision_markets is None else decision_markets)
        if not decision_frame or any(key != value.instrument for key, value in decision_frame.items()):
            raise ValueError("paper decision markets must be keyed by canonical instrument")
        for instrument, event in markets.items():
            if decision_frame.get(instrument) != event:
                raise ValueError("paper decision frame must contain every fresh market event")
        processed = processed_at or max(event.received_at for event in markets.values())
        if any(processed < event.received_at for event in markets.values()):
            raise ValueError("paper frame processing time precedes a source market event")
        if execution_policies is None:
            policies = {instrument: MarketExecutionPolicy.EXECUTE for instrument in markets}
        else:
            if set(execution_policies) != set(markets):
                raise ValueError("paper execution policies must exactly cover fresh markets")
            policies = {
                instrument: MarketExecutionPolicy(value) for instrument, value in execution_policies.items()
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
            orders=tuple(replace(order, created_at=processed) for order in decision.orders),
        )
        result = self.engine.submit_decision(
            decision,
            decision_frame,
            processed_at=processed,
        )
        return PaperRunnerResult(market_results, result)


class PortfolioRunner:
    """Deterministic sequential scheduler over one shared PaperEngine and frame."""

    def __init__(
        self,
        engine: PaperEngine,
        strategies: Iterable[FrozenPaperStrategy],
    ) -> None:
        if not engine.config.strategies:
            raise ValueError("PortfolioRunner requires a schema-v3 paper config")
        registered: dict[str, FrozenPaperStrategy] = {}
        for strategy in strategies:
            raw_id = getattr(strategy, "strategy_id", None)
            if not isinstance(raw_id, str) or not raw_id:
                raise ValueError("portfolio strategy adapters require an explicit strategy_id")
            if raw_id in registered:
                raise ValueError("portfolio strategy adapters contain duplicate strategy_id")
            registered[raw_id] = strategy
        expected_ids = {strategy.strategy_id for strategy in engine.config.strategy_configs}
        if set(registered) != expected_ids:
            raise ValueError("registered strategy adapters differ from the frozen portfolio")
        bindings: list[tuple[PaperStrategyConfig, FrozenPaperStrategy]] = []
        for config in engine.config.strategy_configs:
            adapter = registered[config.strategy_id]
            adapter_config_hash = getattr(adapter, "strategy_config_hash", None)
            if (
                adapter.strategy_name != config.strategy_name
                or adapter.strategy_hash != config.strategy_hash
                or adapter_config_hash != config.strategy_config_hash
            ):
                raise ValueError(f"strategy adapter identity differs for {config.strategy_id}")
            bindings.append((config, adapter))
        self.engine = engine
        self._bindings = tuple(bindings)

    @property
    def strategies(
        self,
    ) -> tuple[tuple[PaperStrategyConfig, FrozenPaperStrategy], ...]:
        return self._bindings

    def process_frame(
        self,
        markets: Mapping[str, MarketEvent],
        *,
        decision_markets: Mapping[str, MarketEvent] | None = None,
        processed_at: datetime | None = None,
        execution_policies: Mapping[str, MarketExecutionPolicy | str] | None = None,
    ) -> PaperRunnerResult:
        if not markets or any(key != value.instrument for key, value in markets.items()):
            raise ValueError("paper runner markets must be keyed by canonical instrument")
        decision_frame = dict(markets if decision_markets is None else decision_markets)
        if not decision_frame or any(key != value.instrument for key, value in decision_frame.items()):
            raise ValueError("paper decision markets must be keyed by canonical instrument")
        for instrument, event in markets.items():
            if decision_frame.get(instrument) != event:
                raise ValueError("paper decision frame must contain every fresh market event")
        processed = processed_at or max(event.received_at for event in markets.values())
        if any(processed < event.received_at for event in markets.values()):
            raise ValueError("paper frame processing time precedes a source market event")
        if execution_policies is None:
            policies = {instrument: MarketExecutionPolicy.EXECUTE for instrument in markets}
        else:
            if set(execution_policies) != set(markets):
                raise ValueError("paper execution policies must exactly cover fresh markets")
            policies = {
                instrument: MarketExecutionPolicy(value) for instrument, value in execution_policies.items()
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
        if any(policy is not MarketExecutionPolicy.EXECUTE for policy in policies.values()):
            return PaperRunnerResult(market_results, None)

        strategy_results: list[PaperStrategyResult] = []
        last_decision: PaperCommandResult | None = None
        for config, adapter in self._bindings:
            projection = self.engine.projection()
            owned = projection.strategy_projection(config.strategy_id)
            if owned.state in {PaperState.PAUSED, PaperState.MANUAL_REVIEW}:
                strategy_results.append(PaperStrategyResult(config.strategy_id, "PAUSED"))
                continue
            try:
                decision = adapter.decide(
                    MappingProxyType(decision_frame),
                    PaperStrategyView.from_projection(projection, config),
                )
            except Exception as error:
                failure = self.engine.record_strategy_failure(
                    strategy_id=config.strategy_id,
                    as_of=processed,
                    phase="EVALUATION",
                    error_type=type(error).__name__,
                    market_event_ids=tuple(sorted(event.event_id for event in markets.values())),
                )
                strategy_results.append(
                    PaperStrategyResult(
                        config.strategy_id,
                        "FAILED_LOCAL",
                        failure_result=failure,
                    )
                )
                continue
            if decision is None:
                strategy_results.append(PaperStrategyResult(config.strategy_id, "HOLD"))
                continue
            if decision.decided_at > processed:
                failure = self.engine.record_strategy_failure(
                    strategy_id=config.strategy_id,
                    as_of=processed,
                    phase="DECISION_VALIDATION",
                    error_type="ValueError",
                    market_event_ids=tuple(sorted(event.event_id for event in markets.values())),
                )
                strategy_results.append(
                    PaperStrategyResult(
                        config.strategy_id,
                        "FAILED_LOCAL",
                        failure_result=failure,
                    )
                )
                continue
            decision = replace(
                decision,
                decided_at=processed,
                orders=tuple(replace(order, created_at=processed) for order in decision.orders),
            )
            try:
                primary = next(
                    (
                        event
                        for event in decision_frame.values()
                        if event.event_id == decision.market_event_id
                    ),
                    None,
                )
                if primary is None:
                    raise ValueError(
                        "strategy decision primary market is absent from the shared frame"
                    )
                admission_instruments = {
                    primary.instrument,
                    *(order.instrument for order in decision.orders),
                }
                admission_markets = {
                    instrument: decision_frame[instrument]
                    for instrument in sorted(admission_instruments)
                }
                last_decision = self.engine.submit_decision(
                    decision,
                    admission_markets,
                    processed_at=processed,
                )
            except (KeyError, TypeError, ValueError) as error:
                failure = self.engine.record_strategy_failure(
                    strategy_id=config.strategy_id,
                    as_of=processed,
                    phase="DECISION_ADMISSION",
                    error_type=type(error).__name__,
                    market_event_ids=tuple(sorted(event.event_id for event in markets.values())),
                )
                strategy_results.append(
                    PaperStrategyResult(
                        config.strategy_id,
                        "FAILED_LOCAL",
                        failure_result=failure,
                    )
                )
                continue
            strategy_results.append(
                PaperStrategyResult(
                    config.strategy_id,
                    "DECISION",
                    decision_result=last_decision,
                )
            )
        return PaperRunnerResult(
            market_results,
            last_decision,
            tuple(strategy_results),
        )


__all__ = [
    "FrozenPaperStrategy",
    "PaperRunner",
    "PaperRunnerResult",
    "PaperStrategyResult",
    "PaperStrategyView",
    "PortfolioRunner",
]
