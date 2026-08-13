from __future__ import annotations

import json
import os
import platform
import signal
import sys
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Annotated, Any

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from hyperlab.backtest.carry import (
    audit_carry_panel,
    carry_stress_scenarios,
    evaluate_carry_gate,
    write_carry_report,
)
from hyperlab.backtest.cross_exchange import (
    CrossVenueConfig,
    audit_cross_venue_data,
    default_cross_venue_config,
    default_cross_venue_risk_rules,
    default_funding_conventions,
    generate_cross_exchange_demo_data,
    run_cross_exchange_validation,
    venue_risk_rules_from_metadata,
    write_cross_exchange_report,
)
from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.funding_basket import (
    FundingBasketValidation,
    audit_funding_basket_panel,
    funding_basket_stress_scenarios,
    write_funding_basket_report,
)
from hyperlab.backtest.momentum import (
    MomentumGateConfig,
    MomentumSelectionConfig,
    audit_momentum_panel,
    run_momentum_validation,
    write_momentum_report,
)
from hyperlab.backtest.pairs import (
    PairSelectionConfig,
    PairsGateConfig,
    audit_pairs_panel,
    run_pairs_validation,
    write_pairs_report,
)
from hyperlab.backtest.report import write_comparison_report
from hyperlab.backtest.workflow import ResearchWorkflowSpec, run_research_workflow
from hyperlab.config import Settings, load_settings
from hyperlab.data.cli import data_app
from hyperlab.data.io import load_cross_venue_csv, load_panel_csv, save_panel_csv
from hyperlab.data.synthetic import generate_demo_panel, generate_microstructure_demo
from hyperlab.models import BacktestResult, MarketPanel, StrategyOutput
from hyperlab.storage.sqlite import database_status, save_carry_snapshots
from hyperlab.strategies.funding_basket import FundingBasketStrategy
from hyperlab.strategies.market_making import InventoryAwareMarketMaker
from hyperlab.strategies.market_making_l2 import (
    AdaptiveMarketMakerConfig,
    L2MarketMakingReplay,
    audit_market_making_records,
    load_market_making_records,
    write_market_making_report,
)
from hyperlab.strategies.registry import STRATEGY_CATALOG, STRATEGY_FACTORIES, create_strategy

app = typer.Typer(
    name="hyperlab",
    help="Laboratoire multi-stratégies Hyperliquid, en lecture seule dans cette version.",
    no_args_is_help=True,
)
console = Console()
CONFIG = Path("config/research.toml")
SECRET_ENV_MARKERS = (
    "PRIVATE_KEY",
    "SEED_PHRASE",
    "MNEMONIC",
    "WALLET_KEY",
    "API_KEY",
)

app.add_typer(data_app, name="data")


def _settings() -> Settings:
    if not CONFIG.exists():
        raise typer.BadParameter(f"Configuration introuvable: {CONFIG.resolve()}")
    return load_settings(CONFIG)


def _csv_values(value: str, *, label: str) -> tuple[str, ...]:
    values = tuple(item.strip() for item in value.split(",") if item.strip())
    if not values:
        raise typer.BadParameter(f"{label} ne peut pas être vide")
    if len(values) != len(set(values)):
        raise typer.BadParameter(f"{label} contient des doublons")
    return values


@contextmanager
def _cooperative_signal_handlers(stop: Callable[[], None]) -> Iterator[None]:
    """Ask the collector to stop, then restore the process signal handlers."""

    managed_signals = tuple(
        dict.fromkeys(
            int(signum)
            for name in ("SIGTERM", "SIGINT")
            if isinstance(signum := getattr(signal, name, None), int)
        )
    )
    previous: dict[int, Any] = {}

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop()

    try:
        for signum in managed_signals:
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, request_stop)
        yield
    finally:
        for signum, handler in reversed(previous.items()):
            signal.signal(signum, handler)


def _close_preserving_active_exception(close: Callable[[], None]) -> None:
    """Close resources without replacing an exception already in flight."""

    primary_error = sys.exception()
    try:
        close()
    except BaseException as cleanup_error:
        if primary_error is None:
            raise
        primary_error.add_note(f"cleanup also failed: {type(cleanup_error).__name__}: {cleanup_error}")


def _profile_for(name: str) -> str:
    return {
        "cash_and_carry": "defensive",
        "funding_basket": "balanced",
        "cross_exchange_funding": "balanced",
        "pairs_mean_reversion": "offensive",
        "momentum_regime": "offensive",
        "lead_lag": "aggressive_research_only",
    }[name]


def _secret_like_environment_variables(environment: Mapping[str, str]) -> list[str]:
    return sorted(key for key in environment if any(marker in key.upper() for marker in SECRET_ENV_MARKERS))


@app.command()
def doctor() -> None:
    """Vérifie l'installation et confirme l'absence d'exécution réelle."""
    settings = _settings()
    secret_like = _secret_like_environment_variables(os.environ)
    table = Table(title="Diagnostic HyperLab")
    table.add_column("Contrôle")
    table.add_column("Résultat")
    table.add_row("Python", sys.version.split()[0])
    table.add_row("Système", f"{platform.system()} {platform.release()}")
    table.add_row("Mode demandé", settings.app.mode)
    table.add_row("Exécuteur d'ordres", "ABSENT — BLOQUÉ")
    table.add_row("Répertoire données", str(settings.app.data_dir.resolve()))
    table.add_row("Variables secrètes détectées", ", ".join(secret_like) if secret_like else "aucune")
    console.print(table)
    if settings.app.mode not in {"readonly", "research"}:
        console.print("[bold red]Refus : HyperLab 0.2.0 n'autorise que readonly/research.[/bold red]")
        raise typer.Exit(2)
    console.print("[bold green]Installation saine : aucun chemin d'ordre réel n'est inclus.[/bold green]")


@app.command("strategies")
def list_strategies() -> None:
    """Affiche les stratégies et leur niveau de maturité."""
    table = Table(title="Catalogue HyperLab")
    table.add_column("Identifiant")
    table.add_column("Niveau")
    table.add_column("Type")
    table.add_column("État")
    table.add_column("Données requises")
    for key, entry in STRATEGY_CATALOG.items():
        table.add_row(key, entry["tier"], entry["label"], entry["status"], entry["data"])
    console.print(table)


def _run_panel_strategies(strategy_names: list[str], hours: int, seed: int) -> list[BacktestResult]:
    settings = _settings()
    panel = generate_demo_panel(hours=hours, seed=seed)
    results: list[BacktestResult] = []
    for name in strategy_names:
        strategy = create_strategy(name)
        profile = settings.risk_profiles[_profile_for(name)]
        engine = PanelBacktester(
            costs=settings.cost_schedule,
            risk_limits=profile,
            execution=replace(
                settings.execution,
                seed=seed,
                require_depth=True,
                require_point_in_time=True,
            ),
            benchmark=settings.research.benchmark,
        )
        results.append(engine.run(panel, strategy.generate(panel)))
    return results


def _write_cross_exchange_demo(*, hours: int, seed: int, output: Path) -> Path:
    data = generate_cross_exchange_demo_data(hours=max(hours, 72), seed=seed + 707)
    conventions = default_funding_conventions()
    risk_rules = default_cross_venue_risk_rules()
    config = default_cross_venue_config()
    audit = audit_cross_venue_data(
        data,
        conventions=conventions,
        risk_rules=risk_rules,
    )
    outage_position = min(len(data.mark_prices) // 2, len(data.mark_prices) - 25)
    validation = run_cross_exchange_validation(
        data,
        conventions=conventions,
        risk_rules=risk_rules,
        config=config,
        failed_venue="HL",
        outage_start=data.mark_prices.index[outage_position],
        audit=audit,
    )
    return write_cross_exchange_report(validation, output_dir=output)


@app.command()
def demo(
    strategy: Annotated[str, typer.Option(help="Nom de stratégie ou 'all'")] = "all",
    hours: Annotated[int, typer.Option(min=600, help="Nombre d'heures synthétiques")] = 1_200,
    seed: Annotated[int, typer.Option(help="Graine déterministe")] = 42,
    output: Annotated[Path, typer.Option(help="Dossier de rapport")] = Path("reports/demo"),
) -> None:
    """Exécute des démonstrations synthétiques non prédictives."""
    if strategy == "all":
        names = list(STRATEGY_FACTORIES)
    elif strategy in STRATEGY_FACTORIES:
        names = [strategy]
    elif strategy == "inventory_market_making":
        names = []
    else:
        raise typer.BadParameter(f"Stratégie inconnue: {strategy}")

    results = _run_panel_strategies(names, hours, seed)
    if strategy in {"all", "inventory_market_making"}:
        micro = generate_microstructure_demo(events=25_000, seed=seed + 1)
        market_making_result = InventoryAwareMarketMaker(seed=seed + 2).run(micro.events)
        market_making_result.diagnostics.update(
            {
                "audit_status": "SYNTHETIC",
                "data_status": "SYNTHETIC",
                "warnings": ["Synthetic toy market-making replay; no L2 queue calibration or live evidence."],
            }
        )
        results.append(market_making_result)

    research_settings = _settings().research
    report = write_comparison_report(
        results,
        output,
        data_label="Données synthétiques conçues uniquement pour vérifier le moteur",
        benchmark_annual_return=research_settings.benchmark.annual_rate,
        bootstrap_block_size=research_settings.bootstrap_block_size,
        bootstrap_resamples=research_settings.bootstrap_resamples,
        bootstrap_seed=research_settings.bootstrap_seed,
        bootstrap_confidence_level=research_settings.bootstrap_confidence_level,
    )
    detailed_cross_exchange_report = None
    if "cross_exchange_funding" in names:
        detailed_cross_exchange_report = _write_cross_exchange_demo(
            hours=hours,
            seed=seed,
            output=output / "cross_exchange_funding",
        )
    table = Table(title="Résultats synthétiques — aucune valeur prédictive")
    table.add_column("Stratégie")
    table.add_column("Retour total", justify="right")
    table.add_column("Drawdown", justify="right")
    table.add_column("Sharpe", justify="right")
    table.add_column("Signaux entrée", justify="right")
    table.add_column("Positions", justify="right")
    table.add_column("Ordres", justify="right")
    for result in results:
        table.add_row(
            result.strategy_name,
            f"{result.metrics.total_return * 100:.2f}%",
            f"{result.metrics.max_drawdown * 100:.2f}%",
            f"{result.metrics.sharpe:.2f}",
            str(result.diagnostics.get("target_entry_signals", "n/a")),
            str(result.diagnostics.get("position_entries", "n/a")),
            str(result.diagnostics.get("orders", "n/a")),
        )
    console.print(table)
    console.print(f"[bold green]Rapport : {report.resolve()}[/bold green]")
    if detailed_cross_exchange_report is not None:
        console.print(
            "[bold green]Rapport Phase 07 à marges séparées : "
            f"{detailed_cross_exchange_report.resolve()}[/bold green]"
        )
    console.print(
        "[yellow]Ces résultats synthétiques valident l'installation, jamais une rentabilité.[/yellow]"
    )


@app.command("cross-exchange-demo")
def cross_exchange_demo(
    hours: Annotated[int, typer.Option(min=72, help="Nombre d'heures synthétiques")] = 240,
    seed: Annotated[int, typer.Option(help="Graine déterministe")] = 707,
    output: Annotated[Path, typer.Option(help="Dossier du rapport Phase 07")] = Path(
        "reports/cross-exchange-demo"
    ),
) -> None:
    """Exerce marges séparées et pannes 1/6/24 h sur données synthétiques visibles."""

    report = _write_cross_exchange_demo(hours=hours, seed=seed, output=output)
    console.print("[yellow]SYNTHETIC — aucune valeur prédictive ni route d'ordre.[/yellow]")
    console.print(f"Rapport Phase 07 : {report.resolve()}")


@app.command("demo-data")
def demo_data(
    directory: Annotated[Path, typer.Option(help="Dossier CSV de sortie")] = Path("data/demo_panel"),
    hours: Annotated[int, typer.Option(min=600)] = 1_200,
    seed: int = 42,
) -> None:
    panel = generate_demo_panel(hours=hours, seed=seed)
    save_panel_csv(panel, directory)
    console.print(f"Données synthétiques écrites dans {directory.resolve()}")


@app.command()
def backtest(
    data: Annotated[
        Path,
        typer.Option(
            help=(
                "Export point-in-time: prices/funding/spreads/volume/depth, "
                "available_at/finality/tradable et metadata"
            )
        ),
    ],
    strategy: Annotated[str, typer.Option(help="Nom de stratégie")],
    output: Annotated[Path, typer.Option(help="Dossier de rapport")] = Path("reports/custom"),
    stress_multiplier: Annotated[float, typer.Option(min=0.1, max=20.0)] = 1.0,
    reveal_final: Annotated[
        bool,
        typer.Option(
            "--reveal-final/--keep-final-locked",
            help="Révèle une seule fois le test final après sélection; verrouillé par défaut.",
        ),
    ] = False,
) -> None:
    """Exécute le protocole Phase 04 auditable. N'autorise aucun ordre réel."""
    if strategy not in STRATEGY_FACTORIES:
        raise typer.BadParameter(f"Stratégie inconnue: {strategy}")
    settings = _settings()
    panel = load_panel_csv(data)
    selected = create_strategy(strategy)
    if strategy == "cash_and_carry" and is_dataclass(selected):
        selected = replace(
            selected,
            round_trip_fees_bps=(
                2.0 * (settings.costs.spot_fee_bps + settings.costs.perp_fee_bps)
            ),
            estimated_round_trip_slippage_bps=4.0 * settings.costs.base_slippage_bps,
            benchmark_annual_rate=settings.research.benchmark.annual_rate,
        )
    parameters = asdict(selected) if is_dataclass(selected) else {"factory": strategy}
    research = settings.research
    if strategy == "cash_and_carry":
        stress_scenarios = carry_stress_scenarios()
    elif strategy == "funding_basket":
        stress_scenarios = funding_basket_stress_scenarios()
    else:
        stress_scenarios = None

    def phase05_reporter(
        base_result: BacktestResult,
        stress_results: dict[str, BacktestResult],
        directory: Path,
    ) -> Path:
        results = {"base": base_result, **stress_results}
        audit = audit_carry_panel(panel, minimum_history_hours=30 * 24)
        gate = evaluate_carry_gate(results, audit=audit)
        return write_carry_report(
            results,
            gate=gate,
            audit=audit,
            output_dir=directory,
            perp_margin_fraction=float(parameters.get("perp_margin_fraction", 1.0)),
        )

    def phase06_reporter(
        base_result: BacktestResult,
        stress_results: dict[str, BacktestResult],
        directory: Path,
    ) -> Path:
        if not isinstance(selected, FundingBasketStrategy):
            raise TypeError("Phase-06 reporter requires FundingBasketStrategy")
        final_index = base_result.weights.index
        final_panel = MarketPanel(
            prices=panel.prices.loc[final_index].copy(),
            funding=panel.funding.loc[final_index].copy(),
            spreads_bps=panel.spreads_bps.loc[final_index].copy(),
            volume_usd=panel.volume_usd.loc[final_index].copy(),
            metadata={**panel.metadata, "evaluation_split": "final_test"},
            depth_usd=panel.depth_usd.loc[final_index].copy() if panel.depth_usd is not None else None,
            open_interest_usd=(
                panel.open_interest_usd.loc[final_index].copy()
                if panel.open_interest_usd is not None
                else None
            ),
            liquidation_usd=(
                panel.liquidation_usd.loc[final_index].copy()
                if panel.liquidation_usd is not None
                else None
            ),
            available_at=(
                panel.available_at.loc[final_index].copy()
                if panel.available_at is not None
                else None
            ),
            finality=panel.finality.loc[final_index].copy() if panel.finality is not None else None,
            tradable=panel.tradable.loc[final_index].copy() if panel.tradable is not None else None,
            regimes=panel.regimes.loc[final_index].copy() if panel.regimes is not None else None,
        )
        sensitivity_engine = PanelBacktester(
            costs=settings.cost_schedule,
            risk_limits=settings.risk_profiles[_profile_for(strategy)],
            execution=replace(
                settings.execution,
                cost_multiplier=settings.execution.cost_multiplier * stress_multiplier,
                require_depth=True,
                require_point_in_time=True,
            ),
            benchmark=research.benchmark,
        )

        def final_output(candidate: FundingBasketStrategy) -> StrategyOutput:
            generated = candidate.generate(panel)
            candidate_weights = generated.weights.loc[final_index].copy()
            candidate_weights.iloc[-min(3, len(candidate_weights)) :] = 0.0
            return StrategyOutput(
                name=generated.name,
                risk_tier=generated.risk_tier,
                weights=candidate_weights,
                diagnostics=generated.diagnostics,
            )

        ranking = sensitivity_engine.run(final_panel, final_output(replace(selected, mode="ranking")))
        audit = audit_funding_basket_panel(panel)
        leave_one_out: dict[str, BacktestResult] = {}
        for asset in audit.assets:
            excluded = tuple(sorted({*selected.excluded_assets, asset}))
            candidate = replace(selected, mode="optimized", excluded_assets=excluded)
            leave_one_out[asset] = sensitivity_engine.run(final_panel, final_output(candidate))
        if not audit.checks.get("minimum_history", False):
            status = "BLOCKED_INSUFFICIENT_REAL_DATA"
        elif not audit.passed:
            status = "BLOCKED_UNCALIBRATED_OR_SURVIVORSHIP_BIAS"
        elif any(
            result.diagnostics.get("audit_status") != "CALIBRATED"
            for result in (base_result, ranking)
        ):
            status = "BLOCKED_UNCALIBRATED_EXECUTION_MODEL"
        else:
            status = "VALIDATED_RESEARCH_ONLY"
        validation = FundingBasketValidation(
            audit=audit,
            comparison={"ranking": ranking, "optimized": base_result},
            stresses={"base": base_result, **stress_results},
            leave_one_out=leave_one_out,
            status=status,
        )
        return write_funding_basket_report(validation, output_dir=directory)

    final_reporter = None
    if strategy == "cash_and_carry":
        final_reporter = phase05_reporter
    elif strategy == "funding_basket":
        final_reporter = phase06_reporter

    artifacts = run_research_workflow(
        panel,
        strategy_name=strategy,
        fit_strategy=lambda _train: replace(selected) if is_dataclass(selected) else selected,
        strategy_parameters=parameters,
        costs=settings.cost_schedule,
        risk_limits=settings.risk_profiles[_profile_for(strategy)],
        execution=replace(
            settings.execution,
            cost_multiplier=settings.execution.cost_multiplier * stress_multiplier,
        ),
        benchmark=research.benchmark,
        spec=ResearchWorkflowSpec(
            train_fraction=research.train_fraction,
            validation_fraction=research.validation_fraction,
            walk_forward_train_bars=research.walk_forward_train_bars,
            walk_forward_validation_bars=research.walk_forward_validation_bars,
            walk_forward_step_bars=research.walk_forward_step_bars,
            embargo_bars=research.embargo_bars,
            expanding=research.expanding,
            bootstrap_block_size=research.bootstrap_block_size,
            bootstrap_resamples=research.bootstrap_resamples,
            bootstrap_confidence_level=research.bootstrap_confidence_level,
            bootstrap_seed=research.bootstrap_seed,
            reveal_final=reveal_final,
            final_liquidation_bars=2 if strategy in {"cash_and_carry", "funding_basket"} else 0,
        ),
        output_dir=output,
        registry_path=research.registry_path,
        stress_scenarios=stress_scenarios,
        final_reporter=final_reporter,
    )
    console.print(f"Plan verrouillé : {artifacts.split_plan_path.resolve()}")
    console.print(f"Registre vérifiable : {artifacts.registry_path.resolve()}")
    console.print(f"Validation OOS : {artifacts.validation_path.resolve()}")
    if artifacts.report_path is None:
        console.print(
            "[yellow]Test final toujours verrouillé. Relancez dans un nouveau run seulement "
            "avec une procédure explicite de révélation.[/yellow]"
        )
    else:
        console.print(f"Rapport final et stress : {artifacts.report_path.resolve()}")
    if artifacts.supplemental_report_path is not None:
        console.print(f"Rapport dédié de stratégie : {artifacts.supplemental_report_path.resolve()}")


@app.command("carry-audit")
def carry_audit(
    data: Annotated[
        Path,
        typer.Option(help="Export panel point-in-time avec spot/perp, OI et profondeur"),
    ],
    output: Annotated[
        Path,
        typer.Option(help="Rapport JSON de préparation Gate B"),
    ] = Path("reports/carry-readiness.json"),
    minimum_history_hours: Annotated[int, typer.Option(min=72)] = 30 * 24,
) -> None:
    """Audite les données Phase 05 sans simuler ni envoyer aucun ordre."""
    panel = load_panel_csv(data)
    audit = audit_carry_panel(panel, minimum_history_hours=minimum_history_hours)
    payload = asdict(audit)
    payload["passed"] = audit.passed
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    console.print_json(json.dumps(payload, ensure_ascii=False))
    console.print(f"Rapport Gate B : {output.resolve()}")
    if not audit.passed:
        raise typer.Exit(2)


@app.command("funding-basket-audit")
def funding_basket_audit(
    data: Annotated[
        Path,
        typer.Option(help="Export panel point-in-time avec perps, profondeur et lifecycle"),
    ],
    output: Annotated[
        Path,
        typer.Option(help="Rapport JSON de préparation Phase 06"),
    ] = Path("reports/funding-basket-readiness.json"),
    minimum_history_hours: Annotated[int, typer.Option(min=24)] = 90 * 24,
    minimum_assets: Annotated[int, typer.Option(min=4)] = 6,
) -> None:
    """Audite l'univers Phase 06, marchés délistés inclus, sans simuler d'ordre."""

    panel = load_panel_csv(data)
    audit = audit_funding_basket_panel(
        panel,
        minimum_history_hours=minimum_history_hours,
        minimum_assets=minimum_assets,
    )
    payload = asdict(audit)
    payload["passed"] = audit.passed
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    console.print_json(json.dumps(payload, ensure_ascii=False))
    console.print(f"Rapport Phase 06 : {output.resolve()}")
    if not audit.passed:
        raise typer.Exit(2)


@app.command("cross-exchange-audit")
def cross_exchange_audit(
    data: Annotated[
        Path,
        typer.Option(help="Export Phase 07 avec marks, oracles et funding par venue"),
    ],
    output: Annotated[
        Path,
        typer.Option(help="Rapport JSON de préparation Phase 07"),
    ] = Path("reports/cross-exchange-readiness.json"),
    minimum_history_hours: Annotated[int, typer.Option(min=24)] = 30 * 24,
) -> None:
    """Audite deux venues et leurs marges sans simuler ni envoyer aucun ordre."""

    conventions = default_funding_conventions()
    market = load_cross_venue_csv(data, conventions=conventions)
    risk_rules = venue_risk_rules_from_metadata(market.metadata)
    audit = audit_cross_venue_data(
        market,
        conventions=conventions,
        risk_rules=risk_rules,
        minimum_history_hours=minimum_history_hours,
    )
    payload = asdict(audit)
    payload["passed"] = audit.passed
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    console.print_json(json.dumps(payload, ensure_ascii=False))
    console.print(f"Rapport Phase 07 : {output.resolve()}")
    if not audit.passed:
        raise typer.Exit(2)


@app.command("pairs-audit")
def pairs_audit(
    data: Annotated[
        Path,
        typer.Option(help="Export panel Phase 08 point-in-time avec lifecycle, funding et profondeur"),
    ],
    output: Annotated[
        Path,
        typer.Option(help="Rapport JSON de préparation Phase 08"),
    ] = Path("reports/pairs-readiness.json"),
    minimum_history_hours: Annotated[int, typer.Option(min=24)] = 180 * 24,
    minimum_assets: Annotated[int, typer.Option(min=4)] = 6,
) -> None:
    """Audite l'univers historique Phase 08 sans simuler ni envoyer aucun ordre."""

    panel = load_panel_csv(data)
    audit = audit_pairs_panel(
        panel,
        minimum_history_hours=minimum_history_hours,
        minimum_assets=minimum_assets,
    )
    payload = asdict(audit)
    payload["passed"] = audit.passed
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    console.print_json(json.dumps(payload, ensure_ascii=False))
    console.print(f"Rapport Phase 08 : {output.resolve()}")
    if not audit.passed:
        raise typer.Exit(2)


@app.command("pairs-backtest")
def pairs_backtest(
    data: Annotated[Path, typer.Option(help="Export CSV Phase 08 point-in-time")],
    output: Annotated[Path, typer.Option(help="Dossier du rapport Phase 08")] = Path(
        "reports/pairs"
    ),
    minimum_history_hours: Annotated[int, typer.Option(min=24)] = 180 * 24,
    minimum_assets: Annotated[int, typer.Option(min=4)] = 6,
    maximum_pairs: Annotated[int, typer.Option(min=2)] = 3,
    minimum_stressed_return: Annotated[float, typer.Option(min=-1.0)] = 0.0,
) -> None:
    """Exécute la Phase 08 avec sélection gelée et gates de rupture/retrait."""

    panel = load_panel_csv(data)
    settings = _settings()
    audit = audit_pairs_panel(
        panel,
        minimum_history_hours=minimum_history_hours,
        minimum_assets=minimum_assets,
    )
    train_bars = max(24, int(len(panel.prices) * settings.research.train_fraction))
    validation_bars = max(12, int(len(panel.prices) * settings.research.validation_fraction))
    selection = PairSelectionConfig(
        maximum_pairs=maximum_pairs,
        minimum_train_bars=train_bars,
        minimum_validation_bars=validation_bars,
        lookback_bars=min(30 * 24, max(12, train_bars // 3)),
    )
    gate = PairsGateConfig(
        train_fraction=settings.research.train_fraction,
        validation_fraction=settings.research.validation_fraction,
        minimum_stressed_return=minimum_stressed_return,
    )
    engine = PanelBacktester(
        costs=settings.cost_schedule,
        risk_limits=settings.risk_profiles["offensive"],
        execution=replace(
            settings.execution,
            require_depth=True,
            require_point_in_time=True,
        ),
        benchmark=settings.research.benchmark,
    )
    validation = run_pairs_validation(
        panel,
        engine=engine,
        selection_config=selection,
        gate_config=gate,
        audit=audit,
    )
    report = write_pairs_report(validation, output_dir=output)
    console.print(f"Statut Phase 08 : {validation.status}")
    console.print(f"Rapport Phase 08 : {report.resolve()}")


@app.command("momentum-audit")
def momentum_audit(
    data: Annotated[
        Path,
        typer.Option(
            help="Export Phase 09 point-in-time avec volume, OI, funding et liquidations"
        ),
    ],
    output: Annotated[
        Path,
        typer.Option(help="Rapport JSON de preparation Phase 09"),
    ] = Path("reports/momentum-readiness.json"),
    minimum_history_hours: Annotated[int, typer.Option(min=24)] = 365 * 24,
    minimum_assets: Annotated[int, typer.Option(min=2)] = 6,
) -> None:
    """Audite les donnees directionnelles Phase 09 sans route d'ordre."""

    panel = load_panel_csv(data)
    audit = audit_momentum_panel(
        panel,
        minimum_history_hours=minimum_history_hours,
        minimum_assets=minimum_assets,
    )
    payload = asdict(audit)
    payload["passed"] = audit.passed
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    console.print_json(json.dumps(payload, ensure_ascii=False))
    console.print(f"Rapport Phase 09 : {output.resolve()}")
    if not audit.passed:
        raise typer.Exit(2)


@app.command("momentum-backtest")
def momentum_backtest(
    data: Annotated[Path, typer.Option(help="Export CSV Phase 09 point-in-time")],
    output: Annotated[Path, typer.Option(help="Dossier du rapport Phase 09")] = Path(
        "reports/momentum"
    ),
    minimum_history_hours: Annotated[int, typer.Option(min=24)] = 365 * 24,
    minimum_assets: Annotated[int, typer.Option(min=2)] = 6,
    minimum_non_bull_pnl: Annotated[float, typer.Option()] = 0.0,
    maximum_bull_profit_fraction: Annotated[float, typer.Option(min=0.0, max=1.0)] = 0.80,
) -> None:
    """Compare momentum/breakout puis evalue le test final par regime."""

    panel = load_panel_csv(data)
    settings = _settings()
    audit = audit_momentum_panel(
        panel,
        minimum_history_hours=minimum_history_hours,
        minimum_assets=minimum_assets,
    )
    train_bars = max(24, int(len(panel.prices) * settings.research.train_fraction))
    validation_bars = max(12, int(len(panel.prices) * settings.research.validation_fraction))
    selection = MomentumSelectionConfig(
        minimum_train_bars=train_bars,
        minimum_validation_bars=validation_bars,
    )
    gate = MomentumGateConfig(
        train_fraction=settings.research.train_fraction,
        validation_fraction=settings.research.validation_fraction,
        minimum_non_bull_pnl=minimum_non_bull_pnl,
        maximum_bull_profit_fraction=maximum_bull_profit_fraction,
    )
    engine = PanelBacktester(
        costs=settings.cost_schedule,
        risk_limits=settings.risk_profiles["offensive"],
        execution=replace(
            settings.execution,
            require_depth=True,
            require_point_in_time=True,
        ),
        benchmark=settings.research.benchmark,
    )
    validation = run_momentum_validation(
        panel,
        engine=engine,
        selection_config=selection,
        gate_config=gate,
        audit=audit,
    )
    report = write_momentum_report(validation, output_dir=output)
    console.print(f"Statut Phase 09 : {validation.status}")
    console.print(f"Rapport Phase 09 : {report.resolve()}")


@app.command("market-making-audit")
def market_making_audit(
    data: Annotated[Path, typer.Option(help="Racine du lake Parquet immuable")],
    asset: Annotated[str, typer.Option(help="Actif public a auditer")] = "BTC",
    target_venue: Annotated[str, typer.Option(help="Venue simulee")] = "hyperliquid",
    reference_venues: Annotated[
        str,
        typer.Option(help="Venues publiques de fair value, separees par des virgules"),
    ] = "binance_usdm",
    output: Annotated[
        Path,
        typer.Option(help="Rapport JSON de preparation Phase 11"),
    ] = Path("reports/market-making-readiness.json"),
    minimum_events: Annotated[int, typer.Option(min=1)] = 10_000,
    calibration_evidence_hash: Annotated[
        str | None,
        typer.Option(help="SHA-256 optionnel de la preuve de calibration"),
    ] = None,
) -> None:
    """Audite les flux L2 Phase 11 sans reseau, secret ni route d'ordre."""

    references = _csv_values(reference_venues, label="reference_venues")
    venues = (target_venue, *references)
    records, manifests = load_market_making_records(data, asset=asset, venues=venues)
    audit = audit_market_making_records(
        records,
        asset=asset,
        target_venue=target_venue,
        minimum_events=minimum_events,
        calibration_evidence_hash=calibration_evidence_hash,
        manifest_hashes=(manifest.sha256 for manifest in manifests),
    )
    payload = audit.as_dict()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    console.print_json(json.dumps(payload, ensure_ascii=False))
    console.print(f"Rapport Phase 11 : {output.resolve()}")
    if not audit.passed:
        raise typer.Exit(2)


@app.command("market-making-replay")
def market_making_replay(
    data: Annotated[Path, typer.Option(help="Racine du lake Parquet immuable")],
    output: Annotated[Path, typer.Option(help="Dossier du rapport Phase 11")] = Path(
        "reports/market-making"
    ),
    asset: Annotated[str, typer.Option(help="Actif public a rejouer")] = "BTC",
    target_venue: Annotated[str, typer.Option(help="Venue simulee")] = "hyperliquid",
    reference_venues: Annotated[
        str,
        typer.Option(help="Venues publiques de fair value, separees par des virgules"),
    ] = "binance_usdm",
    minimum_events: Annotated[int, typer.Option(min=1)] = 10_000,
    calibration_evidence_hash: Annotated[
        str | None,
        typer.Option(help="SHA-256 optionnel de la preuve de calibration"),
    ] = None,
    maker_fee_bps: Annotated[float, typer.Option(min=0.0)] = 1.5,
    taker_fee_bps: Annotated[float, typer.Option(min=0.0)] = 4.5,
    quote_latency_ms: Annotated[int, typer.Option(min=0)] = 25,
    cancel_latency_ms: Annotated[int, typer.Option(min=0)] = 25,
) -> None:
    """Rejoue le carnet event-by-event; ne construit et n'envoie aucun ordre."""

    references = _csv_values(reference_venues, label="reference_venues")
    venues = (target_venue, *references)
    records, manifests = load_market_making_records(data, asset=asset, venues=venues)
    audit = audit_market_making_records(
        records,
        asset=asset,
        target_venue=target_venue,
        minimum_events=minimum_events,
        calibration_evidence_hash=calibration_evidence_hash,
        manifest_hashes=(manifest.sha256 for manifest in manifests),
    )
    if not audit.passed:
        output.mkdir(parents=True, exist_ok=True)
        readiness = output / "market_making_readiness.json"
        temporary = readiness.with_name(f".{readiness.name}.tmp")
        temporary.write_text(
            json.dumps(audit.as_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(readiness)
        console.print("Statut Phase 11 : BLOCKED_DATA_READINESS")
        console.print(f"Audit Phase 11 : {readiness.resolve()}")
        raise typer.Exit(2)
    weights = {target_venue: 0.75}
    reference_weight = 0.25 / len(references)
    weights.update({venue: reference_weight for venue in references})
    config = AdaptiveMarketMakerConfig(
        target_venue=target_venue,
        asset=asset,
        maker_fee_bps=maker_fee_bps,
        taker_fee_bps=taker_fee_bps,
        quote_latency_ms=quote_latency_ms,
        cancel_latency_ms=cancel_latency_ms,
        venue_weights=weights,
        calibration_status=(
            "CALIBRATED" if calibration_evidence_hash is not None else "UNCALIBRATED"
        ),
        calibration_evidence_hash=calibration_evidence_hash,
        data_label="IMMUTABLE_LAKE_REPLAY",
    )
    result = L2MarketMakingReplay(config).run(records)
    report = write_market_making_report(result, output_dir=output, audit=audit)
    console.print(f"Statut Phase 11 : {result.status}")
    console.print(f"Audit Phase 11 : {'PASS' if audit.passed else 'BLOCKED'}")
    console.print(f"Rapport Phase 11 : {report.resolve()}")
    if result.status != "RESEARCH_REPLAY_COMPLETE":
        raise typer.Exit(2)


@app.command("cross-exchange-backtest")
def cross_exchange_backtest(
    data: Annotated[Path, typer.Option(help="Export CSV Phase 07 point-in-time")],
    output: Annotated[Path, typer.Option(help="Dossier du rapport Phase 07")] = Path(
        "reports/cross-exchange"
    ),
    capital_hl: Annotated[float, typer.Option(min=1.0)] = 50_000.0,
    capital_binance: Annotated[float, typer.Option(min=1.0)] = 50_000.0,
    target_notional: Annotated[float, typer.Option(min=1.0)] = 40_000.0,
    failed_venue: Annotated[str, typer.Option(help="Venue indisponible dans les stress")] = "HL",
    outage_start: Annotated[
        str | None,
        typer.Option(help="Début UTC ISO-8601; milieu de période par défaut"),
    ] = None,
    minimum_history_hours: Annotated[int, typer.Option(min=24)] = 30 * 24,
) -> None:
    """Simule la Phase 07 en recherche seule avec pannes préenregistrées 1/6/24 h."""

    conventions = default_funding_conventions()
    market = load_cross_venue_csv(data, conventions=conventions)
    if len(market.mark_prices) < 25:
        raise typer.BadParameter(
            "la matrice de panne exige 24 heures d'incident puis une barre de reprise"
        )
    risk_rules = venue_risk_rules_from_metadata(market.metadata)
    audit = audit_cross_venue_data(
        market,
        conventions=conventions,
        risk_rules=risk_rules,
        minimum_history_hours=minimum_history_hours,
    )
    if outage_start is None:
        position = min(len(market.mark_prices) // 2, len(market.mark_prices) - 25)
        resolved_outage_start = market.mark_prices.index[position]
    else:
        resolved_outage_start = pd.Timestamp(outage_start)
        if (
            resolved_outage_start.tz is None
            or resolved_outage_start.utcoffset() != pd.Timedelta(0)
        ):
            raise typer.BadParameter("outage-start doit être un horodatage UTC explicite")
        resolved_outage_start = resolved_outage_start.tz_convert("UTC")
    config = CrossVenueConfig(
        initial_capital_by_venue={"HL": capital_hl, "BINANCE_USDM": capital_binance},
        target_notional_usd=target_notional,
    )
    validation = run_cross_exchange_validation(
        market,
        conventions=conventions,
        risk_rules=risk_rules,
        config=config,
        failed_venue=failed_venue,
        outage_start=resolved_outage_start,
        audit=audit,
    )
    report = write_cross_exchange_report(validation, output_dir=output)
    console.print(f"Statut Phase 07 : {validation.status}")
    console.print(f"Rapport Phase 07 : {report.resolve()}")


@app.command()
def snapshot(
    network: Annotated[str, typer.Option(help="mainnet ou testnet")] = "mainnet",
    save: Annotated[bool, typer.Option("--save/--no-save")] = True,
) -> None:
    """Lit un snapshot public spot/perp Hyperliquid."""
    from hyperlab.api.public import HyperliquidPublicClient

    settings = _settings()
    with HyperliquidPublicClient(
        network=network, timeout_seconds=settings.app.request_timeout_seconds
    ) as client:
        snapshots = client.carry_snapshot()
    table = Table(title=f"Snapshot public Hyperliquid — {network}")
    for heading in ("Actif", "Spot", "Perp", "Funding/h", "Basis bps", "Volume perp 24h"):
        table.add_column(heading, justify="right" if heading != "Actif" else "left")
    for item in snapshots:
        table.add_row(
            item.asset,
            f"{item.spot_mid}",
            f"{item.perp_mid}",
            f"{float(item.funding_hourly) * 100:.5f}%",
            f"{float(item.basis_bps):.2f}",
            f"{float(item.perp_volume_usd):,.0f}",
        )
    console.print(table)
    if save:
        database = settings.app.data_dir / "hyperlab.sqlite3"
        count = save_carry_snapshots(database, snapshots)
        console.print(f"{count} lignes enregistrées dans {database.resolve()}")


@app.command()
def collect(
    network: Annotated[str, typer.Option(help="mainnet ou testnet")] = "mainnet",
    assets: Annotated[str, typer.Option(help="Coins API séparés par des virgules")] = "BTC,ETH",
    candle_intervals: Annotated[str, typer.Option(help="Intervalles candle séparés par des virgules")] = "1m",
    duration_seconds: Annotated[float, typer.Option(min=0.0, help="0 = boucle continue")] = 0.0,
    max_messages: Annotated[int, typer.Option(min=0, help="0 = aucune limite de messages")] = 0,
    batch_size: Annotated[int, typer.Option(min=1, max=10_000)] = 500,
    history_lookback_hours: Annotated[int, typer.Option(min=1, max=24)] = 24,
) -> None:
    """Collecte REST+WebSocket publique, sans adresse ni secret."""
    from hyperlab.collector.models import CollectorConfig
    from hyperlab.collector.runtime import PublicCollector
    from hyperlab.collector.websocket import WebsocketClientFactory

    settings = _settings()
    if settings.app.mode not in {"readonly", "research"}:
        raise typer.BadParameter("Le collecteur 0.2.0 refuse tout mode autre que readonly/research")
    try:
        config = CollectorConfig(
            network=network,
            assets=_csv_values(assets, label="assets"),
            candle_intervals=_csv_values(candle_intervals, label="candle-intervals"),
            batch_size=batch_size,
            history_lookback_hours=history_lookback_hours,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None
    socket_factory = WebsocketClientFactory(queue_capacity=config.queue_capacity)
    collector = PublicCollector.create_default(
        config,
        data_dir=settings.app.data_dir,
        request_timeout_seconds=settings.app.request_timeout_seconds,
        socket_factory=socket_factory,
    )
    try:
        with _cooperative_signal_handlers(collector.stop):
            collector.run(
                max_messages=max_messages,
                duration_seconds=None if duration_seconds == 0 else duration_seconds,
            )
    except KeyboardInterrupt:
        collector.stop()
        console.print("Arrêt demandé; fermeture et publication du dernier batch.")
    finally:
        _close_preserving_active_exception(collector.close)
    console.print_json(json.dumps(collector.metrics.as_dict(datetime.now(tz=UTC))))


@app.command("collect-reference")
def collect_reference(
    assets: Annotated[str, typer.Option(help="Actifs de base séparés par des virgules")] = "BTC,ETH",
    candle_intervals: Annotated[str, typer.Option(help="Intervalles candle séparés par des virgules")] = "1m",
    duration_seconds: Annotated[float, typer.Option(min=0.0, help="0 = boucle continue")] = 0.0,
    max_messages: Annotated[int, typer.Option(min=0, help="0 = aucune limite")] = 0,
    batch_size: Annotated[int, typer.Option(min=1, max=10_000)] = 500,
    history_lookback_hours: Annotated[int, typer.Option(min=1, max=168)] = 24,
) -> None:
    """Collecte Binance USD-M publique et sans clé; aucune API de trading."""
    from hyperlab.venues.runtime import BinanceReferenceCollector, ReferenceCollectorConfig

    settings = _settings()
    if settings.app.mode not in {"readonly", "research"}:
        raise typer.BadParameter("Le collecteur de référence refuse tout mode non readonly/research")
    try:
        config = ReferenceCollectorConfig(
            assets=tuple(value.upper() for value in _csv_values(assets, label="assets")),
            candle_intervals=_csv_values(candle_intervals, label="candle-intervals"),
            batch_size=batch_size,
            history_lookback_hours=history_lookback_hours,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None
    collector = BinanceReferenceCollector.create_default(
        config,
        data_dir=settings.app.data_dir,
        request_timeout_seconds=settings.app.request_timeout_seconds,
    )
    try:
        with _cooperative_signal_handlers(collector.stop):
            collector.run(
                duration_seconds=None if duration_seconds == 0 else duration_seconds,
                max_messages=None if max_messages == 0 else max_messages,
            )
    except KeyboardInterrupt:
        collector.stop()
        console.print("Arrêt demandé; fermeture du collecteur de référence.")
    finally:
        _close_preserving_active_exception(collector.close)
    console.print_json(json.dumps(collector.metrics))

@app.command("collect-multi-venue")
def collect_multi_venue(
    assets: Annotated[str, typer.Option(help="Actifs communs séparés par des virgules")] = "BTC,ETH",
    candle_intervals: Annotated[
        str,
        typer.Option(help="Intervalles candle communs séparés par des virgules"),
    ] = "1m",
    duration_seconds: Annotated[
        float,
        typer.Option(min=0.0, help="Durée simultanée; 0 = boucle continue"),
    ] = 86_400.0,
    batch_size: Annotated[int, typer.Option(min=1, max=10_000)] = 500,
    history_lookback_hours: Annotated[int, typer.Option(min=1, max=24)] = 24,
) -> None:
    """Collecte Hyperliquid + Binance ensemble via un unique writer coordonné."""
    from hyperlab.collector.models import CollectorConfig
    from hyperlab.collector.multivenue import MultiVenueCollector
    from hyperlab.collector.runtime import PublicCollector
    from hyperlab.collector.storage import CoordinatedLakeWriter
    from hyperlab.collector.websocket import WebsocketClientFactory
    from hyperlab.venues.runtime import BinanceReferenceCollector, ReferenceCollectorConfig

    settings = _settings()
    if settings.app.mode not in {"readonly", "research"}:
        raise typer.BadParameter("La collecte multi-venue refuse tout mode non readonly/research")
    try:
        shared_assets = tuple(value.upper() for value in _csv_values(assets, label="assets"))
        shared_intervals = _csv_values(candle_intervals, label="candle-intervals")
        hyperliquid_config = CollectorConfig(
            assets=shared_assets,
            candle_intervals=shared_intervals,
            batch_size=batch_size,
            history_lookback_hours=history_lookback_hours,
        )
        binance_config = ReferenceCollectorConfig(
            assets=shared_assets,
            candle_intervals=shared_intervals,
            batch_size=batch_size,
            history_lookback_hours=history_lookback_hours,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None

    writer = CoordinatedLakeWriter(
        settings.app.data_dir / "lake",
        venues=("hyperliquid", "binance_usdm"),
        batch_size=batch_size,
        queue_capacity=(
            hyperliquid_config.queue_capacity + binance_config.queue_capacity
        ),
    )
    hyperliquid = None
    binance = None
    hyperliquid_sink = writer.client("hyperliquid")
    binance_sink = writer.client("binance_usdm")
    try:
        hyperliquid = PublicCollector.create_default(
            hyperliquid_config,
            data_dir=settings.app.data_dir,
            request_timeout_seconds=settings.app.request_timeout_seconds,
            socket_factory=WebsocketClientFactory(
                queue_capacity=hyperliquid_config.queue_capacity
            ),
            sink=hyperliquid_sink,
        )
        binance = BinanceReferenceCollector.create_default(
            binance_config,
            data_dir=settings.app.data_dir,
            request_timeout_seconds=settings.app.request_timeout_seconds,
            sink=binance_sink,
        )
    except BaseException:
        if hyperliquid is not None:
            _close_preserving_active_exception(hyperliquid.close)
        else:
            _close_preserving_active_exception(hyperliquid_sink.close)
        if binance is not None:
            _close_preserving_active_exception(binance.close)
        else:
            _close_preserving_active_exception(binance_sink.close)
        _close_preserving_active_exception(writer.close)
        raise

    runtime = MultiVenueCollector(
        hyperliquid=hyperliquid,
        binance=binance,
        writer=writer,
    )
    try:
        with _cooperative_signal_handlers(runtime.stop):
            runtime.run(
                duration_seconds=None if duration_seconds == 0 else duration_seconds
            )
    except KeyboardInterrupt:
        runtime.stop()
        console.print("Arrêt demandé; fermeture coordonnée des deux venues et flush final.")
    finally:
        _close_preserving_active_exception(runtime.close)

    now = datetime.now(tz=UTC)
    console.print_json(
        json.dumps(
            {
                "mode": "readonly",
                "orders_enabled": False,
                "lake": str((settings.app.data_dir / "lake").resolve()),
                "venues": {
                    "hyperliquid": hyperliquid.metrics.as_dict(now),
                    "binance_usdm": dict(binance.metrics),
                },
            }
        )
    )


@app.command()
def replay(
    fixture_dir: Annotated[
        Path,
        typer.Argument(exists=True, file_okay=False, dir_okay=True, readable=True),
    ],
    output: Annotated[Path, typer.Option(help="Racine du lake replay")] = Path("data/replay-lake"),
    received_at: Annotated[
        str, typer.Option(help="Horloge UTC logique et déterministe")
    ] = "2026-08-11T23:21:00+00:00",
) -> None:
    """Rejoue des fixtures publiques sans construire de client réseau."""
    from hyperlab.collector.models import ParsedRecord
    from hyperlab.collector.replay import replay_fixture
    from hyperlab.collector.storage import BatchingLakeSink

    try:
        logical_time = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
    except ValueError:
        raise typer.BadParameter("received-at must be a valid ISO 8601 timestamp") from None
    if logical_time.tzinfo is None or logical_time.utcoffset() != UTC.utcoffset(logical_time):
        raise typer.BadParameter("received-at doit être un horodatage UTC explicite")
    logical_time = logical_time.astimezone(UTC)
    sink = BatchingLakeSink(output, batch_size=500, queue_capacity=10_000, persistent_dedup=False)
    rows_written = 0

    def add_and_flush(record: ParsedRecord) -> bool:
        nonlocal rows_written
        added = sink.add(record)
        if sink.should_flush:
            rows_written += sink.flush().row_count
        return added

    try:
        summary = replay_fixture(fixture_dir, add_and_flush, lambda: logical_time)
        rows_written += sink.flush().row_count
    finally:
        _close_preserving_active_exception(sink.close)
    summary.update({"network_enabled": False, "rows_written": rows_written})
    console.print_json(json.dumps(summary))


@app.command()
def status() -> None:
    settings = _settings()
    payload: dict[str, object] = dict(database_status(settings.app.data_dir / "hyperlab.sqlite3"))
    runtime_path = settings.app.data_dir / "runtime_status.json"
    if runtime_path.exists():
        try:
            payload["runtime"] = json.loads(runtime_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload["runtime"] = {"ok": False, "error": "runtime_status.json illisible"}
    reference_runtime_path = settings.app.data_dir / "runtime_status_binance_usdm.json"
    if reference_runtime_path.exists():
        try:
            payload["reference_runtime"] = json.loads(reference_runtime_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload["reference_runtime"] = {
                "ok": False,
                "error": "runtime_status_binance_usdm.json illisible",
            }
    console.print_json(json.dumps(payload))


@app.command()
def serve(
    host: Annotated[str, typer.Option()] = "0.0.0.0",
    port: Annotated[int, typer.Option(min=1, max=65_535)] = 8000,
) -> None:
    """Lance le dashboard local read-only."""
    import uvicorn

    from hyperlab.dashboard.app import create_app

    settings = _settings()
    uvicorn.run(create_app(data_dir=settings.app.data_dir), host=host, port=port)


if __name__ == "__main__":
    app()
