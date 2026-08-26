"""Persistent, deterministic, offline-only paper execution for Phase 12.

Public exports are loaded on first use so importing an independent Paper
submodule does not require runtime dependencies that it never uses.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from hyperlab.paper.carry_strategy import (
        PHASE05_CARRY_STRATEGY_ID,
        FrozenCashAndCarryPaperConfig,
        FrozenCashAndCarryPaperStrategy,
        make_phase05_paper_strategy_config,
    )
    from hyperlab.paper.engine import PaperCommandResult, PaperEngine
    from hyperlab.paper.gate import (
        PaperGateEvidence,
        PaperGateResult,
        PaperGateStatus,
        evaluate_paper_gate,
    )
    from hyperlab.paper.models import (
        AlertSeverity,
        DecisionAction,
        DecisionIntent,
        LedgerEntry,
        MarketEvent,
        OrderIntent,
        OrderSide,
        OrderStatus,
        PaperEvent,
        PaperEventType,
        PaperExecutionConfig,
        PaperOrderType,
        PaperProjection,
        PaperRiskLimits,
        PaperRunConfig,
        PaperState,
        PaperStrategyConfig,
        PaperStrategyProjection,
        StoredPaperEvent,
        TimeInForce,
        deterministic_id,
        keyed_uniform,
    )
    from hyperlab.paper.phase05_portfolio import (
        Phase05Phase08PaperFoundation,
        Phase05Phase08RiskAllocation,
        build_phase05_phase08_paper_foundation,
        default_phase05_phase08_risk_allocation,
    )
    from hyperlab.paper.risk import RiskDecision, evaluate_order_risk, is_risk_reducing
    from hyperlab.paper.runner import (
        FrozenPaperStrategy,
        PaperRunner,
        PaperRunnerResult,
        PaperStrategyView,
        PortfolioRunner,
    )
    from hyperlab.paper.store import (
        AppendResult,
        ConcurrentWriteError,
        IdempotencyConflictError,
        InputRecord,
        IntegrityError,
        LedgerImbalanceError,
        PaperStore,
        RunConflictError,
        RunNotFoundError,
        SchemaVersionError,
    )

_MODULE_EXPORTS = {
    "hyperlab.paper.carry_strategy": (
        "PHASE05_CARRY_STRATEGY_ID",
        "FrozenCashAndCarryPaperConfig",
        "FrozenCashAndCarryPaperStrategy",
        "make_phase05_paper_strategy_config",
    ),
    "hyperlab.paper.engine": ("PaperCommandResult", "PaperEngine"),
    "hyperlab.paper.gate": (
        "PaperGateEvidence",
        "PaperGateResult",
        "PaperGateStatus",
        "evaluate_paper_gate",
    ),
    "hyperlab.paper.models": (
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
        "PaperOrderType",
        "PaperProjection",
        "PaperRiskLimits",
        "PaperRunConfig",
        "PaperState",
        "PaperStrategyConfig",
        "PaperStrategyProjection",
        "StoredPaperEvent",
        "TimeInForce",
        "deterministic_id",
        "keyed_uniform",
    ),
    "hyperlab.paper.phase05_portfolio": (
        "Phase05Phase08PaperFoundation",
        "Phase05Phase08RiskAllocation",
        "build_phase05_phase08_paper_foundation",
        "default_phase05_phase08_risk_allocation",
    ),
    "hyperlab.paper.risk": (
        "RiskDecision",
        "evaluate_order_risk",
        "is_risk_reducing",
    ),
    "hyperlab.paper.runner": (
        "FrozenPaperStrategy",
        "PaperRunner",
        "PaperRunnerResult",
        "PaperStrategyView",
        "PortfolioRunner",
    ),
    "hyperlab.paper.store": (
        "AppendResult",
        "ConcurrentWriteError",
        "IdempotencyConflictError",
        "InputRecord",
        "IntegrityError",
        "LedgerImbalanceError",
        "PaperStore",
        "RunConflictError",
        "RunNotFoundError",
        "SchemaVersionError",
    ),
}
_EXPORT_MODULE = {
    name: module_name
    for module_name, names in _MODULE_EXPORTS.items()
    for name in names
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))

__all__ = [
    "PHASE05_CARRY_STRATEGY_ID",
    "AlertSeverity",
    "AppendResult",
    "ConcurrentWriteError",
    "DecisionAction",
    "DecisionIntent",
    "FrozenCashAndCarryPaperConfig",
    "FrozenCashAndCarryPaperStrategy",
    "FrozenPaperStrategy",
    "IdempotencyConflictError",
    "InputRecord",
    "IntegrityError",
    "LedgerEntry",
    "LedgerImbalanceError",
    "MarketEvent",
    "OrderIntent",
    "OrderSide",
    "OrderStatus",
    "PaperCommandResult",
    "PaperEngine",
    "PaperEvent",
    "PaperEventType",
    "PaperExecutionConfig",
    "PaperGateEvidence",
    "PaperGateResult",
    "PaperGateStatus",
    "PaperOrderType",
    "PaperProjection",
    "PaperRiskLimits",
    "PaperRunConfig",
    "PaperRunner",
    "PaperRunnerResult",
    "PaperState",
    "PaperStore",
    "PaperStrategyConfig",
    "PaperStrategyProjection",
    "PaperStrategyView",
    "Phase05Phase08PaperFoundation",
    "Phase05Phase08RiskAllocation",
    "PortfolioRunner",
    "RiskDecision",
    "RunConflictError",
    "RunNotFoundError",
    "SchemaVersionError",
    "StoredPaperEvent",
    "TimeInForce",
    "build_phase05_phase08_paper_foundation",
    "default_phase05_phase08_risk_allocation",
    "deterministic_id",
    "evaluate_order_risk",
    "evaluate_paper_gate",
    "is_risk_reducing",
    "keyed_uniform",
    "make_phase05_paper_strategy_config",
]
