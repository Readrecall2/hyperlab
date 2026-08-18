from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from hyperlab.paper.carry_strategy import (
    FrozenCashAndCarryPaperConfig,
    FrozenCashAndCarryPaperStrategy,
    make_phase05_paper_strategy_config,
)
from hyperlab.paper.collector_source import (
    PHASE12_PHASE05_PRODUCT_IDENTITY_HASHES,
    HyperliquidPaperPublicSource,
)
from hyperlab.paper.models import (
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
)
from hyperlab.paper.pairs_strategy import (
    FrozenRobustPairsPaperConfig,
    FrozenRobustPairsPaperStrategy,
    make_phase08_paper_strategy_config,
)
from hyperlab.paper.runner import FrozenPaperStrategy


@dataclass(frozen=True, slots=True)
class Phase05Phase08PaperFoundation:
    """Frozen technical-only portfolio composition; it owns no execution route."""

    config: PaperRunConfig
    strategies: tuple[FrozenPaperStrategy, ...]
    source: HyperliquidPaperPublicSource


@dataclass(frozen=True, slots=True)
class Phase05Phase08RiskAllocation:
    phase05: PaperRiskLimits
    phase08: PaperRiskLimits
    portfolio: PaperRiskLimits


def default_phase05_phase08_risk_allocation() -> Phase05Phase08RiskAllocation:
    return Phase05Phase08RiskAllocation(
        phase05=PaperRiskLimits(
            max_gross_notional=Decimal("500"),
            max_net_notional=Decimal("300"),
            max_instrument_notional=Decimal("250"),
            max_order_notional=Decimal("250"),
            max_position_quantity=Decimal("10"),
            max_order_quantity=Decimal("10"),
            max_concurrent_orders=2,
            max_daily_loss=Decimal("100"),
            max_drawdown=Decimal("200"),
            stale_after_seconds=15,
            unhedged_timeout_seconds=20,
        ),
        phase08=PaperRiskLimits(
            max_gross_notional=Decimal("250"),
            max_net_notional=Decimal("250"),
            max_instrument_notional=Decimal("250"),
            max_order_notional=Decimal("250"),
            max_position_quantity=Decimal("1"),
            max_order_quantity=Decimal("1"),
            max_concurrent_orders=2,
            max_daily_loss=Decimal("75"),
            max_drawdown=Decimal("150"),
            stale_after_seconds=15,
            unhedged_timeout_seconds=20,
        ),
        portfolio=PaperRiskLimits(
            max_gross_notional=Decimal("750"),
            max_net_notional=Decimal("500"),
            max_instrument_notional=Decimal("500"),
            max_order_notional=Decimal("250"),
            max_position_quantity=Decimal("11"),
            max_order_quantity=Decimal("10"),
            max_concurrent_orders=4,
            max_daily_loss=Decimal("175"),
            max_drawdown=Decimal("350"),
            stale_after_seconds=15,
            unhedged_timeout_seconds=20,
        ),
    )


def build_phase05_phase08_paper_foundation(
    *,
    runtime_status_path: Path,
    validation_started_at: datetime,
    initial_cash: Decimal = Decimal("2000"),
    seed: int = 12_508,
    execution: PaperExecutionConfig | None = None,
    risk_allocation: Phase05Phase08RiskAllocation | None = None,
) -> Phase05Phase08PaperFoundation:
    """Build the first real two-strategy candidate without starting its collector."""

    allocation = risk_allocation or default_phase05_phase08_risk_allocation()
    source = HyperliquidPaperPublicSource.create_mainnet_portfolio(
        runtime_status_path=runtime_status_path,
    )
    carry_config = FrozenCashAndCarryPaperConfig(
        spot_instrument="HL:HYPE:spot",
        perp_instrument="HL:HYPE:perp",
        spot_product_identity_sha256=PHASE12_PHASE05_PRODUCT_IDENTITY_HASHES[
            "HL:HYPE:spot"
        ],
        perp_product_identity_sha256=PHASE12_PHASE05_PRODUCT_IDENTITY_HASHES[
            "HL:HYPE:perp"
        ],
        retained_hours=96,
        maximum_gross_notional=allocation.phase05.max_gross_notional,
        spot_quantity_step=Decimal("0.01"),
        perp_quantity_step=Decimal("0.01"),
        spot_max_quantity=Decimal("10"),
        perp_max_quantity=Decimal("10"),
    )
    pairs_config = FrozenRobustPairsPaperConfig()
    carry_identity = make_phase05_paper_strategy_config(
        config=carry_config,
        risk=allocation.phase05,
    )
    pairs_identity = make_phase08_paper_strategy_config(
        config=pairs_config,
        risk=allocation.phase08,
    )
    identities = tuple(sorted((carry_identity, pairs_identity), key=lambda item: item.strategy_id))
    primary = identities[0]
    config = PaperRunConfig(
        strategy_name=primary.strategy_name,
        strategy_hash=primary.strategy_hash,
        parameters=primary.parameters,
        data_hash=source.descriptor.data_hash,
        execution=execution
        or PaperExecutionConfig(
            calibration_status="UNCALIBRATED",
            source="phase05-phase08-technical-placeholder",
        ),
        risk=allocation.portfolio,
        seed=seed,
        initial_cash=initial_cash,
        validation_started_at=validation_started_at,
        run_kind="TECHNICAL",
        data_calibration_status="UNCALIBRATED",
        data_source=source.descriptor.source,
        economic_prerequisites_satisfied=False,
        economic_prerequisites_evidence_hash=None,
        required_instruments=tuple(
            sorted(
                {
                    instrument
                    for identity in identities
                    for instrument in identity.required_instruments
                }
            )
        ),
        schema_version=3,
        strategies=identities,
    )
    strategies: tuple[FrozenPaperStrategy, ...] = (
        FrozenCashAndCarryPaperStrategy(
            config=carry_config,
            strategy_config=carry_identity,
        ),
        FrozenRobustPairsPaperStrategy(
            pairs_config,
            strategy_config=pairs_identity,
        ),
    )
    return Phase05Phase08PaperFoundation(
        config=config,
        strategies=strategies,
        source=source,
    )


__all__ = [
    "Phase05Phase08PaperFoundation",
    "Phase05Phase08RiskAllocation",
    "build_phase05_phase08_paper_foundation",
    "default_phase05_phase08_risk_allocation",
]
