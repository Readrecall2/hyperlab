from hyperlab.backtest.attribution import aggregate_pnl, causal_regimes
from hyperlab.backtest.bootstrap import block_bootstrap_ci
from hyperlab.backtest.costs import CostRule, CostSchedule, SlippageModel
from hyperlab.backtest.cross_exchange import (
    CrossVenueConfig,
    CrossVenueMarketData,
    FundingCalendar,
    FundingConvention,
    VenueRiskRule,
    simulate_cross_exchange_funding,
)
from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.execution import ExecutionConfig, MakerFillModel
from hyperlab.backtest.pairs import (
    PairSelectionConfig,
    PairsGateConfig,
    audit_pairs_panel,
    run_pairs_validation,
)
from hyperlab.backtest.point_in_time import (
    CandleFinalityPolicy,
    join_venues_as_of,
    select_candle_revisions_as_of,
    universe_mask_as_of,
)
from hyperlab.backtest.protocol import (
    FinalTestLock,
    SplitPlan,
    TimeRange,
    WalkForwardSpec,
)
from hyperlab.backtest.registry import (
    ResearchRegistry,
    SelectionObjective,
    ValidationResult,
    VariantSpec,
    select_best_variant,
)
from hyperlab.backtest.report import write_comparison_report
from hyperlab.backtest.research import (
    WalkForwardResult,
    causal_evaluation_with_terminal_mark,
    generate_causal_evaluation,
    run_walk_forward,
    slice_panel,
)
from hyperlab.backtest.split import chronological_split
from hyperlab.backtest.stress import StressScenario, run_stress_matrix
from hyperlab.backtest.workflow import (
    ResearchWorkflowArtifacts,
    ResearchWorkflowSpec,
    run_research_workflow,
)

__all__ = [
    "CandleFinalityPolicy",
    "CostRule",
    "CostSchedule",
    "CrossVenueConfig",
    "CrossVenueMarketData",
    "ExecutionConfig",
    "FinalTestLock",
    "FundingCalendar",
    "FundingConvention",
    "MakerFillModel",
    "PairSelectionConfig",
    "PairsGateConfig",
    "PanelBacktester",
    "ResearchRegistry",
    "ResearchWorkflowArtifacts",
    "ResearchWorkflowSpec",
    "SelectionObjective",
    "SlippageModel",
    "SplitPlan",
    "StressScenario",
    "TimeRange",
    "ValidationResult",
    "VariantSpec",
    "VenueRiskRule",
    "WalkForwardResult",
    "WalkForwardSpec",
    "aggregate_pnl",
    "audit_pairs_panel",
    "block_bootstrap_ci",
    "causal_evaluation_with_terminal_mark",
    "causal_regimes",
    "chronological_split",
    "generate_causal_evaluation",
    "join_venues_as_of",
    "run_pairs_validation",
    "run_research_workflow",
    "run_stress_matrix",
    "run_walk_forward",
    "select_best_variant",
    "select_candle_revisions_as_of",
    "simulate_cross_exchange_funding",
    "slice_panel",
    "universe_mask_as_of",
    "write_comparison_report",
]
