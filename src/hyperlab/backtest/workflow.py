from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from hyperlab.backtest.benchmark import PassiveBenchmarkSpec
from hyperlab.backtest.bootstrap import block_bootstrap_ci
from hyperlab.backtest.costs import CostSchedule
from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.execution import ExecutionConfig
from hyperlab.backtest.metrics import compute_metrics
from hyperlab.backtest.protocol import FinalTestLock, SplitPlan, TimeRange, WalkForwardSpec
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
    run_walk_forward,
    slice_panel,
)
from hyperlab.backtest.stress import StressScenario, default_stress_scenarios, run_stress_matrix
from hyperlab.models import BacktestMetrics, BacktestResult, MarketPanel, RiskLimits
from hyperlab.strategies.base import Strategy


@dataclass(frozen=True, slots=True)
class ResearchWorkflowSpec:
    train_fraction: float = 0.60
    validation_fraction: float = 0.20
    walk_forward_train_bars: int = 720
    walk_forward_validation_bars: int = 168
    walk_forward_step_bars: int = 168
    embargo_bars: int = 1
    expanding: bool = True
    bootstrap_block_size: int = 24
    bootstrap_resamples: int = 2_000
    bootstrap_confidence_level: float = 0.95
    bootstrap_seed: int = 42
    reveal_final: bool = False
    final_liquidation_bars: int = 0

    def __post_init__(self) -> None:
        if not 0.0 < self.train_fraction < 1.0:
            raise ValueError("train_fraction must be in (0, 1)")
        if not 0.0 < self.validation_fraction < 1.0:
            raise ValueError("validation_fraction must be in (0, 1)")
        if self.train_fraction + self.validation_fraction >= 1.0:
            raise ValueError("train and validation must leave a non-empty final test")
        for name, value in (
            ("walk_forward_train_bars", self.walk_forward_train_bars),
            ("walk_forward_validation_bars", self.walk_forward_validation_bars),
            ("walk_forward_step_bars", self.walk_forward_step_bars),
            ("bootstrap_block_size", self.bootstrap_block_size),
            ("bootstrap_resamples", self.bootstrap_resamples),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.walk_forward_step_bars != self.walk_forward_validation_bars:
            raise ValueError(
                "research workflow requires contiguous, non-overlapping OOS windows "
                "for block bootstrap"
            )
        if self.embargo_bars < 0:
            raise ValueError("embargo_bars must be non-negative")
        if self.bootstrap_resamples < 2:
            raise ValueError("bootstrap_resamples must be at least two")
        if not 0.0 < self.bootstrap_confidence_level < 1.0:
            raise ValueError("bootstrap_confidence_level must be in (0, 1)")
        if self.bootstrap_seed < 0:
            raise ValueError("bootstrap_seed must be non-negative")
        if (
            isinstance(self.final_liquidation_bars, bool)
            or not isinstance(self.final_liquidation_bars, int)
            or self.final_liquidation_bars < 0
        ):
            raise ValueError("final_liquidation_bars must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class ResearchWorkflowArtifacts:
    output_dir: Path
    split_plan_path: Path
    registry_path: Path
    validation_path: Path
    report_path: Path | None
    manifest_path: Path
    selected_variant_hash: str
    final_revealed: bool
    supplemental_report_path: Path | None = None


def _json_value(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_value(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, float) and not math.isfinite(value):
            return str(value)
        return value
    return str(value)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(_json_value(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _update_frame_hash(hasher: Any, name: str, frame: pd.DataFrame | pd.Series | None) -> None:
    hasher.update(name.encode("utf-8"))
    if frame is None:
        hasher.update(b"<none>")
        return
    normalized = frame.to_frame() if isinstance(frame, pd.Series) else frame
    hasher.update(
        json.dumps([str(column) for column in normalized.columns], separators=(",", ":")).encode("utf-8")
    )
    hasher.update(
        json.dumps([str(dtype) for dtype in normalized.dtypes], separators=(",", ":")).encode("utf-8")
    )
    hashes = pd.util.hash_pandas_object(normalized, index=True, categorize=False).to_numpy(dtype="uint64")
    hasher.update(hashes.astype("<u8", copy=False).tobytes())


def hash_market_panel(panel: MarketPanel) -> str:
    """Hash the exact aligned matrices and research metadata used by a run."""

    panel.validate()
    hasher = hashlib.sha256()
    for name in (
        "prices",
        "funding",
        "spreads_bps",
        "volume_usd",
        "depth_usd",
        "open_interest_usd",
        "available_at",
        "finality",
        "tradable",
        "regimes",
    ):
        _update_frame_hash(hasher, name, cast(pd.DataFrame | pd.Series | None, getattr(panel, name)))
    metadata = json.dumps(_json_value(panel.metadata), sort_keys=True, separators=(",", ":"))
    hasher.update(metadata.encode("utf-8"))
    return hasher.hexdigest()


def hash_source_tree(root: Path | None = None) -> str:
    """Hash normalized Python sources so dirty research code remains identifiable."""

    source_root = (root or Path(__file__).resolve().parents[1]).resolve()
    files = sorted(source_root.rglob("*.py"), key=lambda item: item.relative_to(source_root).as_posix())
    if not files:
        raise ValueError(f"no Python source found under {source_root}")
    hasher = hashlib.sha256()
    for path in files:
        relative = path.relative_to(source_root).as_posix()
        content = path.read_bytes().replace(b"\r\n", b"\n")
        hasher.update(relative.encode("utf-8") + b"\0" + content + b"\0")
    return hasher.hexdigest()


def _regular_cadence(index: pd.DatetimeIndex) -> pd.Timedelta:
    if len(index) < 3:
        raise ValueError("a research workflow needs at least three timestamped observations")
    deltas = index.to_series().diff().dropna()
    if bool(deltas.le(pd.Timedelta(0)).any()) or deltas.nunique() != 1:
        raise ValueError("research workflow requires a regular UTC panel without timestamp gaps")
    return pd.Timedelta(deltas.iloc[0])


def build_split_plan(
    panel: MarketPanel,
    *,
    dataset_hash: str,
    train_fraction: float,
    validation_fraction: float,
) -> SplitPlan:
    panel.validate()
    index = panel.prices.index
    _regular_cadence(pd.DatetimeIndex(index))
    usable_rows = len(index) - 1
    train_end = int(usable_rows * train_fraction)
    validation_end = int(usable_rows * (train_fraction + validation_fraction))
    if train_end <= 0 or validation_end <= train_end or validation_end >= len(index):
        raise ValueError("split fractions produce an empty train, validation, or final test")
    return SplitPlan(
        train=TimeRange(index[0].to_pydatetime(), index[train_end].to_pydatetime()),
        validation=TimeRange(index[train_end].to_pydatetime(), index[validation_end].to_pydatetime()),
        # The terminal price is reserved as the close/mark for the final decision.
        # It is never exposed to the strategy as a decision row.
        test=TimeRange(index[validation_end].to_pydatetime(), index[-1].to_pydatetime()),
        dataset_hash=dataset_hash,
    )


def _walk_forward_metrics(result: WalkForwardResult) -> tuple[BacktestMetrics, pd.DataFrame]:
    components = result.out_of_sample_returns
    equity = (1.0 + components["net_return"]).cumprod()
    equity.name = "walk_forward_oos"
    weights = pd.concat([fold.result.weights for fold in result.folds]).sort_index()
    fills = (
        pd.concat(
            [fold.result.fills for fold in result.folds if not fold.result.fills.empty],
            ignore_index=True,
        )
        if any(not fold.result.fills.empty for fold in result.folds)
        else pd.DataFrame()
    )
    benchmark_returns: list[pd.Series] = []
    for fold in result.folds:
        if fold.result.benchmark is None:
            continue
        fold_returns = fold.result.benchmark.pct_change().fillna(0.0)
        benchmark_returns.append(fold_returns)
    benchmark = None
    if len(benchmark_returns) == len(result.folds):
        joined = pd.concat(benchmark_returns).sort_index()
        benchmark = (1.0 + joined).cumprod()
        benchmark.name = "walk_forward_passive"
    return compute_metrics(
        components,
        equity,
        weights,
        benchmark=benchmark,
        fills=fills,
    ), weights


def _result_exports(result: BacktestResult, directory: Path, name: str) -> None:
    safe_name = name.replace(":", "_").replace("/", "_")
    target = directory / "ledgers" / safe_name
    target.mkdir(parents=True, exist_ok=True)
    result.returns.to_csv(target / "returns.csv")
    result.weights.to_csv(target / "filled_weights.csv")
    if result.target_weights is not None:
        result.target_weights.to_csv(target / "target_weights.csv")
    result.fills.to_csv(target / "fills.csv", index=False)
    result.attribution.to_csv(target / "pnl_attribution.csv", index=False)
    if result.benchmark is not None:
        result.benchmark.to_frame().to_csv(target / "benchmark.csv")
    for dimension, frame in result.breakdowns.items():
        frame.to_csv(target / f"pnl_by_{dimension}.csv")


def _manifest_payload(
    directory: Path,
    *,
    registry_path: Path,
    dataset_hash: str,
    code_hash: str,
    split_hash: str,
    selected_variant_hash: str,
    final_revealed: bool,
) -> dict[str, object]:
    manifest_path = directory / "run_manifest.json"
    files: dict[str, str] = {}
    for path in sorted(directory.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file() or path == manifest_path:
            continue
        files[path.relative_to(directory).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    registry_head = registry_path.with_name(f"{registry_path.name}.head.json")
    registry_artifact = {
        "path": registry_path.resolve().as_posix(),
        "sha256": hashlib.sha256(registry_path.read_bytes()).hexdigest(),
        "head_path": registry_head.resolve().as_posix(),
        "head_sha256": hashlib.sha256(registry_head.read_bytes()).hexdigest(),
    }
    return {
        "schema_version": 1,
        "dataset_hash": dataset_hash,
        "code_hash": code_hash,
        "split_hash": split_hash,
        "selected_variant_hash": selected_variant_hash,
        "final_revealed": final_revealed,
        "registry": registry_artifact,
        "files": files,
    }


def run_research_workflow(
    panel: MarketPanel,
    *,
    strategy_name: str,
    fit_strategy: Callable[[MarketPanel], Strategy],
    strategy_parameters: Mapping[str, object],
    costs: CostSchedule,
    risk_limits: RiskLimits,
    execution: ExecutionConfig,
    benchmark: PassiveBenchmarkSpec,
    spec: ResearchWorkflowSpec,
    output_dir: Path,
    registry_path: Path | None = None,
    stress_scenarios: tuple[StressScenario, ...] | None = None,
    final_reporter: Callable[[BacktestResult, dict[str, BacktestResult], Path], Path] | None = None,
) -> ResearchWorkflowArtifacts:
    """Run the only auditable Phase-04 path from locked split to artifacts.

    The final range remains absent unless ``spec.reveal_final`` is explicitly true.
    No method in this workflow has a venue transport or a real-order capability.
    """

    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError("research output directory must be new and empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    engine_execution = replace(execution, require_depth=True, require_point_in_time=True)
    engine_template = PanelBacktester(
        costs=costs,
        risk_limits=risk_limits,
        execution=engine_execution,
        benchmark=benchmark,
    )
    engine_template.validate_research_panel(panel)
    if not isinstance(panel.metadata.get("historical_universe_source"), str):
        raise ValueError("research panel must identify its historical lifecycle universe source")
    lifecycle_hash = panel.metadata.get("lifecycle_hash")
    if not isinstance(lifecycle_hash, str) or len(lifecycle_hash) != 64:
        raise ValueError("research panel must carry a SHA-256 lifecycle_hash")

    dataset_hash = hash_market_panel(panel)
    code_hash = hash_source_tree()
    plan = build_split_plan(
        panel,
        dataset_hash=dataset_hash,
        train_fraction=spec.train_fraction,
        validation_fraction=spec.validation_fraction,
    )
    split_plan_path = output_dir / "split_plan.json"
    _write_json(
        split_plan_path,
        {"plan": plan.to_dict(), "plan_hash": plan.canonical_hash, "created_before_trials": True},
    )

    resolved_registry = registry_path or (output_dir / "variants.jsonl")
    registry = ResearchRegistry(resolved_registry)
    registry.register_plan(plan)
    objective = SelectionObjective("sharpe", "maximize")
    variant_parameters = {
        "strategy": _json_value(strategy_parameters),
        "risk_limits": _json_value(risk_limits),
        "execution": _json_value(engine_execution),
        "cost_schedule": _json_value(costs),
        "benchmark": _json_value(benchmark),
        "protocol": _json_value(spec),
    }
    base_variant = VariantSpec(
        strategy_name=strategy_name,
        parameters=cast(Mapping[str, object], variant_parameters),
        split_hash=plan.canonical_hash,
        data_hash=dataset_hash,
        code_hash=code_hash,
        objective=objective,
        seed=engine_execution.seed,
        scenario="base",
    )
    registry.register_variant(base_variant)

    cadence = _regular_cadence(pd.DatetimeIndex(panel.prices.index))
    walk_forward = WalkForwardSpec(
        bounds=TimeRange(plan.train.start, plan.validation.end),
        train_window=timedelta(seconds=cadence.total_seconds() * spec.walk_forward_train_bars),
        validation_window=timedelta(seconds=cadence.total_seconds() * spec.walk_forward_validation_bars),
        step=timedelta(seconds=cadence.total_seconds() * spec.walk_forward_step_bars),
        embargo=timedelta(seconds=cadence.total_seconds() * spec.embargo_bars),
        expanding=spec.expanding,
    )
    try:
        walk_forward_result = run_walk_forward(
            panel,
            plan.selection_view.walk_forward(walk_forward),
            selection_view=plan.selection_view,
            fit_strategy=fit_strategy,
            backtester=lambda _window: PanelBacktester(
                costs=costs,
                risk_limits=risk_limits,
                execution=engine_execution,
                benchmark=benchmark,
            ),
        )
        validation_metrics, _validation_weights = _walk_forward_metrics(walk_forward_result)
        validation_event = registry.record_success(
            base_variant.variant_hash,
            validation_metrics.as_dict(),
            split="walk_forward",
        )
    except (KeyboardInterrupt, SystemExit) as error:
        registry.record_interrupted(base_variant.variant_hash, error, split="walk_forward")
        raise
    except Exception as error:
        registry.record_failure(base_variant.variant_hash, error, split="walk_forward")
        raise

    validation_result = ValidationResult.from_event(validation_event)
    selected = select_best_variant([validation_result], objective, selection_view=plan.selection_view)
    registry.record_selection(selected, objective=objective, selection_view=plan.selection_view)

    oos_returns = walk_forward_result.out_of_sample_returns["net_return"]
    uncertainty = block_bootstrap_ci(
        oos_returns,
        block_size=min(spec.bootstrap_block_size, len(oos_returns)),
        n_resamples=spec.bootstrap_resamples,
        confidence_level=spec.bootstrap_confidence_level,
        seed=spec.bootstrap_seed,
    )
    validation_path = output_dir / "validation_oos.json"
    for fold in walk_forward_result.folds:
        _result_exports(
            fold.result,
            output_dir,
            f"validation_fold_{fold.window.ordinal:04d}",
        )
    _write_json(
        validation_path,
        {
            "evaluation_split": "walk_forward_oos",
            "split_hash": plan.canonical_hash,
            "data_hash": dataset_hash,
            "code_hash": code_hash,
            "variant_hash": selected.variant_hash,
            "execution_seed": engine_execution.seed,
            "bootstrap_seed": spec.bootstrap_seed,
            "calibration_statuses": {
                "data": str(panel.metadata.get("calibration_status", "UNCALIBRATED")),
                "costs": costs.calibration_status,
                "maker_fills": engine_execution.maker_fill.calibration_status,
            },
            "calibration_evidence": {
                "data_hash": panel.metadata.get("calibration_evidence_hash"),
                "data_source": panel.metadata.get("calibration_source"),
                "cost_hash": costs.calibration_evidence_hash,
                "cost_sources": sorted({rule.source for rule in costs.rules}),
                "maker_hash": engine_execution.maker_fill.calibration_evidence_hash,
                "maker_id": engine_execution.maker_fill.calibration_id,
            },
            "planned_stress_scenarios": [
                _json_value(scenario) for scenario in (stress_scenarios or default_stress_scenarios())
            ],
            "metrics": validation_metrics.as_dict(),
            "folds": [fold.window.to_dict() for fold in walk_forward_result.folds],
            "bootstrap": {
                "available": True,
                "status": "AVAILABLE_OOS",
                "statistic": "mean_oos_bar_return",
                "estimate": uncertainty.estimate,
                "lower": uncertainty.lower,
                "upper": uncertainty.upper,
                "standard_error": uncertainty.standard_error,
                "confidence_level": uncertainty.confidence_level,
                "block_size": uncertainty.block_size,
                "n_resamples": uncertainty.n_resamples,
                "effective_blocks": uncertainty.effective_blocks,
                "insufficient_sample": uncertainty.insufficient_sample,
                "seed": uncertainty.seed,
                "time_index_verified": uncertainty.time_index_verified,
                "cadence": uncertainty.cadence,
            },
        },
    )

    planned_stress_variants: list[tuple[StressScenario, VariantSpec]] = []
    for scenario in stress_scenarios or default_stress_scenarios():
        if scenario.name == "base":
            continue
        stress_variant = VariantSpec(
            strategy_name=strategy_name,
            parameters={
                **variant_parameters,
                "parent_variant_hash": selected.variant_hash,
                "stress": _json_value(scenario),
            },
            split_hash=plan.canonical_hash,
            data_hash=dataset_hash,
            code_hash=code_hash,
            objective=objective,
            seed=engine_execution.seed,
            scenario=scenario.name,
        )
        registry.register_variant(stress_variant)
        planned_stress_variants.append((scenario, stress_variant))

    lock = FinalTestLock(plan)
    token = registry.freeze_final_test(selected.variant_hash, lock)
    report_path: Path | None = None
    supplemental_report_path: Path | None = None
    final_revealed = False
    if spec.reveal_final:
        try:
            selection_panel = slice_panel(panel, TimeRange(plan.train.start, plan.validation.end))
            fitted = fit_strategy(selection_panel)
        except (KeyboardInterrupt, SystemExit) as error:
            registry.record_interrupted(
                selected.variant_hash,
                error,
                split="train",
                provenance={"stage": "selection_refit"},
            )
            raise
        except Exception as error:
            registry.record_failure(
                selected.variant_hash,
                error,
                split="train",
                provenance={"stage": "selection_refit"},
            )
            raise
        final_range = registry.reveal_final_test(selected.variant_hash, lock, token)
        try:
            final_decisions = slice_panel(panel, final_range)
            full_history = slice_panel(
                panel,
                TimeRange(
                    plan.train.start,
                    (panel.prices.index[-1] + cadence).to_pydatetime(),
                ),
            )
            engine_template.validate_research_panel(full_history)
            execution_index = pd.DatetimeIndex([*final_decisions.prices.index, panel.prices.index[-1]])
            final_panel = MarketPanel(
                prices=panel.prices.loc[execution_index].copy(),
                funding=panel.funding.loc[execution_index].copy(),
                spreads_bps=panel.spreads_bps.loc[execution_index].copy(),
                volume_usd=panel.volume_usd.loc[execution_index].copy(),
                metadata={**panel.metadata, "evaluation_split": "final_test"},
                depth_usd=panel.depth_usd.loc[execution_index].copy()
                if panel.depth_usd is not None
                else None,
                open_interest_usd=panel.open_interest_usd.loc[execution_index].copy()
                if panel.open_interest_usd is not None
                else None,
                liquidation_usd=panel.liquidation_usd.loc[execution_index].copy()
                if panel.liquidation_usd is not None
                else None,
                available_at=panel.available_at.loc[execution_index].copy()
                if panel.available_at is not None
                else None,
                finality=panel.finality.loc[execution_index].copy() if panel.finality is not None else None,
                tradable=panel.tradable.loc[execution_index].copy() if panel.tradable is not None else None,
                regimes=panel.regimes.loc[execution_index].copy() if panel.regimes is not None else None,
            )
            final_output = causal_evaluation_with_terminal_mark(
                fitted,
                full_history,
                pd.DatetimeIndex(final_decisions.prices.index),
                panel.prices.index[-1],
            )
            if spec.final_liquidation_bars > 0:
                liquidation_count = min(spec.final_liquidation_bars, len(final_decisions.prices.index))
                liquidation_index = final_decisions.prices.index[-liquidation_count:]
                final_output.weights.loc[liquidation_index] = 0.0
                final_output.weights.loc[panel.prices.index[-1]] = 0.0
                final_output.diagnostics.update(
                    {
                        "predeclared_final_liquidation": True,
                        "final_liquidation_bars": liquidation_count,
                        "final_liquidation_start": liquidation_index[0].isoformat(),
                    }
                )
            final_result = engine_template.run(final_panel, final_output)
            final_result.diagnostics.update(
                {
                    "evaluation_split": "final_test",
                    "split_hash": plan.canonical_hash,
                    "data_hash": dataset_hash,
                    "code_hash": code_hash,
                    "variant_hash": selected.variant_hash,
                }
            )
            registry.record_final_success(
                selected.variant_hash,
                final_result.metrics.as_dict(),
                lock=lock,
            )
        except (KeyboardInterrupt, SystemExit) as error:
            registry.record_final_interrupted(selected.variant_hash, error, lock=lock)
            raise
        except Exception as error:
            registry.record_final_failure(selected.variant_hash, error, lock=lock)
            raise

        stress_results: dict[str, BacktestResult] = {}
        for scenario, stress_variant in planned_stress_variants:
            try:
                stressed = run_stress_matrix(
                    panel=final_panel,
                    output=final_output,
                    costs=costs,
                    risk_limits=risk_limits,
                    base_execution=engine_execution,
                    scenarios=(scenario,),
                    benchmark=benchmark,
                )[scenario.name]
                stressed.diagnostics.update(
                    {
                        "evaluation_split": "stress_final_test",
                        "split_hash": plan.canonical_hash,
                        "data_hash": dataset_hash,
                        "code_hash": code_hash,
                        "variant_hash": stress_variant.variant_hash,
                        "parent_variant_hash": selected.variant_hash,
                    }
                )
                registry.record_success(
                    stress_variant.variant_hash,
                    stressed.metrics.as_dict(),
                    split="stress",
                )
                stress_results[scenario.name] = stressed
            except (KeyboardInterrupt, SystemExit) as error:
                registry.record_interrupted(
                    stress_variant.variant_hash,
                    error,
                    split="stress",
                )
                raise
            except Exception as error:
                registry.record_failure(stress_variant.variant_hash, error, split="stress")
                raise

        _result_exports(final_result, output_dir, "final_test")
        for scenario_name, result in stress_results.items():
            _result_exports(result, output_dir, f"stress_{scenario_name}")
        report_path = write_comparison_report(
            [final_result, *stress_results.values()],
            output_dir,
            title=f"HyperLab Phase 04 — {strategy_name}",
            data_label=(
                "Test final verrouillé et stress; statut de calibration visible dans chaque résultat"
            ),
            benchmark_annual_return=benchmark.annual_rate,
            bootstrap_block_size=spec.bootstrap_block_size,
            bootstrap_resamples=spec.bootstrap_resamples,
            bootstrap_seed=spec.bootstrap_seed,
            bootstrap_confidence_level=spec.bootstrap_confidence_level,
        )
        _write_json(
            output_dir / "stress_summary.json",
            {
                "base": final_result.metrics.as_dict(),
                "scenarios": {name: result.metrics.as_dict() for name, result in stress_results.items()},
            },
        )
        if final_reporter is not None:
            supplemental_report_path = final_reporter(final_result, stress_results, output_dir)
            if not supplemental_report_path.is_file():
                raise ValueError("final_reporter must return an existing report file")
            try:
                supplemental_report_path.resolve().relative_to(output_dir.resolve())
            except ValueError:
                raise ValueError("final_reporter output must stay inside the research directory") from None
        final_revealed = True

    manifest_path = output_dir / "run_manifest.json"
    _write_json(
        manifest_path,
        _manifest_payload(
            output_dir,
            registry_path=resolved_registry,
            dataset_hash=dataset_hash,
            code_hash=code_hash,
            split_hash=plan.canonical_hash,
            selected_variant_hash=selected.variant_hash,
            final_revealed=final_revealed,
        ),
    )
    return ResearchWorkflowArtifacts(
        output_dir=output_dir,
        split_plan_path=split_plan_path,
        registry_path=resolved_registry,
        validation_path=validation_path,
        report_path=report_path,
        manifest_path=manifest_path,
        selected_variant_hash=selected.variant_hash,
        final_revealed=final_revealed,
        supplemental_report_path=supplemental_report_path,
    )


__all__ = [
    "ResearchWorkflowArtifacts",
    "ResearchWorkflowSpec",
    "build_split_plan",
    "hash_market_panel",
    "hash_source_tree",
    "run_research_workflow",
]
