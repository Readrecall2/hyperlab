from __future__ import annotations

import json
import os
import platform
import signal
import sys
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from types import FrameType, MappingProxyType
from typing import TYPE_CHECKING, Annotated, Any

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from hyperlab import __version__
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

if TYPE_CHECKING:
    from hyperlab.paper.models import PaperRunConfig
    from hyperlab.paper.runner import FrozenPaperStrategy
    from hyperlab.paper.runtime import NormalizedPublicMarketSource

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
operations_app = typer.Typer(
    name="ops",
    help="Sauvegarde, restauration et contrôles fail-closed du déploiement read-only.",
    no_args_is_help=True,
)
app.add_typer(operations_app, name="ops")
paper_app = typer.Typer(
    name="paper",
    help="Supervision locale du moteur paper-only Phase 12.",
    no_args_is_help=True,
)
app.add_typer(paper_app, name="paper")


@dataclass(frozen=True, slots=True)
class _ApprovedPaperRuntimeFactories:
    candidate_id: str
    config_hash: str
    admission_manifest_path: Path
    admission_manifest_sha256: str
    admission_evidence_root: Path
    strategy_factory: Callable[[PaperRunConfig], FrozenPaperStrategy]
    source_factory: Callable[[PaperRunConfig], NormalizedPublicMarketSource]

    def __post_init__(self) -> None:
        if not self.candidate_id.strip():
            raise ValueError("approved paper candidate_id cannot be empty")
        for label, digest in (
            ("config_hash", self.config_hash),
            ("admission_manifest_sha256", self.admission_manifest_sha256),
        ):
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ValueError(f"approved paper {label} must be a lowercase SHA-256")
        if not isinstance(self.admission_manifest_path, Path):
            raise TypeError("approved paper admission_manifest_path must be a Path")
        if not isinstance(self.admission_evidence_root, Path):
            raise TypeError("approved paper admission_evidence_root must be a Path")


# Deliberately empty until a frozen strategy artifact and a normalized public
# source adapter have both passed an explicit review.  The CLI never imports a
# user-supplied module or constructs a strategy from an arbitrary name.
_APPROVED_PAPER_RUNTIMES: Mapping[str, _ApprovedPaperRuntimeFactories] = MappingProxyType({})

# Intentionally empty and non-authorizing. A future candidate must add a concrete
# measured-result protocol whose canonical Gate B/C decision is derived by core;
# merely registering a callback or returning PASS booleans must never admit it.
_TRUSTED_PAPER_SEMANTIC_EVALUATORS: Mapping[str, None] = MappingProxyType({})


def _production_semantic_admission_blockers(candidate_id: str) -> tuple[str, ...]:
    if candidate_id not in _TRUSTED_PAPER_SEMANTIC_EVALUATORS:
        return ("NO_TRUSTED_CANDIDATE_SEMANTIC_EVALUATOR",)
    return ("SEMANTIC_EVALUATOR_PROTOCOL_NOT_IMPLEMENTED",)


def _settings() -> Settings:
    config = Path(os.getenv("HYPERLAB_CONFIG", str(CONFIG)))
    if not config.exists():
        raise typer.BadParameter(f"Configuration introuvable: {config.resolve()}")
    return load_settings(config)


def _configured_directory(name: str, default: Path) -> Path:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip()
    if not normalized:
        raise typer.BadParameter(f"{name} ne peut pas être vide")
    return Path(normalized)


def _strict_persistence_enabled() -> bool:
    value = os.getenv("HYPERLAB_REQUIRE_PERSISTENT_LAYOUT")
    if value is None or value == "0":
        return False
    if value != "1":
        raise typer.BadParameter("HYPERLAB_REQUIRE_PERSISTENT_LAYOUT doit valoir 0 ou 1")
    return True


def _validate_service_mounts(settings: Settings, *, service: str) -> bool:
    strict = _strict_persistence_enabled()
    if not strict:
        return False
    from hyperlab.operations import DeploymentIntegrityError, validate_service_persistence

    config = Path(os.getenv("HYPERLAB_CONFIG", str(CONFIG)))
    try:
        validate_service_persistence(settings.app.data_dir, config, service=service)
    except (DeploymentIntegrityError, OSError, ValueError) as exc:
        raise typer.BadParameter(f"persistance {service} refusée: {exc}") from None
    return True


def _invalidate_collector_readiness_if_strict() -> None:
    if not _strict_persistence_enabled():
        return
    from hyperlab.operations import DeploymentIntegrityError, publish_collector_starting_status

    data_root = _configured_directory("HYPERLAB_DATA_DIR", Path("data"))
    try:
        publish_collector_starting_status(data_root)
    except (DeploymentIntegrityError, OSError, ValueError) as exc:
        raise typer.BadParameter(f"statut de démarrage refusé: {exc}") from None


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


@operations_app.command("check-layout")
def operations_check_layout(
    data_root: Annotated[Path, typer.Argument(exists=True, file_okay=False, dir_okay=True)],
    writable: Annotated[
        bool,
        typer.Option("--writable/--read-only", help="Exige une écriture durable de contrôle."),
    ] = False,
) -> None:
    """Vérifie les volumes persistants explicites et la configuration read-only."""

    from hyperlab.operations import DeploymentIntegrityError, validate_persistent_layout

    try:
        payload = validate_persistent_layout(data_root, require_writable=writable)
    except (DeploymentIntegrityError, OSError, ValueError) as exc:
        typer.echo(f"PERSISTENCE_UNHEALTHY: {exc}", err=True)
        raise typer.Exit(2) from None
    console.print_json(json.dumps(payload))


@operations_app.command("backup")
def operations_backup(
    data_root: Annotated[Path, typer.Argument(exists=True, file_okay=False, dir_okay=True)],
    backup_root: Annotated[
        Path | None,
        typer.Option(help="Répertoire de sauvegarde; défaut: DATA_ROOT/backups."),
    ] = None,
    backup_id: Annotated[str | None, typer.Option(help="Identifiant stable optionnel.")] = None,
) -> None:
    """Crée une sauvegarde complète après arrêt propre et verrouillage exclusif du lake."""

    from hyperlab.operations import DeploymentIntegrityError, create_backup

    try:
        result = create_backup(data_root, backup_root=backup_root, backup_id=backup_id)
    except (DeploymentIntegrityError, FileExistsError, OSError, ValueError) as exc:
        typer.echo(f"BACKUP_REFUSED: {exc}", err=True)
        raise typer.Exit(2) from None
    console.print_json(json.dumps(result.as_dict()))


@operations_app.command("export-parquet")
def operations_export_parquet(
    data_root: Annotated[Path, typer.Argument(exists=True, file_okay=False, dir_okay=True)],
    output_name: Annotated[str, typer.Argument(help="Nom simple du rapport .parquet")],
    record_type: Annotated[str, typer.Option("--type", help="Type de données exact")],
    venue: Annotated[str | None, typer.Option(help="Venue exacte")] = None,
    asset: Annotated[str | None, typer.Option(help="Actif exact")] = None,
    start: Annotated[str | None, typer.Option(help="Date UTC incluse YYYY-MM-DD")] = None,
    end: Annotated[str | None, typer.Option(help="Date UTC incluse YYYY-MM-DD")] = None,
    schema_version: Annotated[int | None, typer.Option(min=1)] = None,
) -> None:
    """Exporte un Parquet vérifié après arrêt propre et verrouillage exclusif du lake."""

    from hyperlab.operations import DeploymentIntegrityError, create_parquet_export

    try:
        payload = create_parquet_export(
            data_root,
            output_name=output_name,
            record_type=record_type,
            venue=venue,
            asset=asset,
            start=start,
            end=end,
            schema_version=schema_version,
        )
    except (DeploymentIntegrityError, FileExistsError, OSError, ValueError) as exc:
        typer.echo(f"EXPORT_REFUSED: {exc}", err=True)
        raise typer.Exit(2) from None
    console.print_json(json.dumps(payload))


@operations_app.command("verify-backup")
def operations_verify_backup(
    backup: Annotated[Path, typer.Argument(exists=True, file_okay=False, dir_okay=True)],
) -> None:
    """Vérifie le marqueur complet, les hashes, SQLite, Parquet et la configuration."""

    from hyperlab.operations import DeploymentIntegrityError, verify_backup

    try:
        result = verify_backup(backup)
    except (DeploymentIntegrityError, OSError, ValueError) as exc:
        typer.echo(f"BACKUP_INVALID: {exc}", err=True)
        raise typer.Exit(2) from None
    console.print_json(json.dumps(result.as_dict()))


@operations_app.command("restore")
def operations_restore(
    backup: Annotated[Path, typer.Argument(exists=True, file_okay=False, dir_okay=True)],
    target: Annotated[Path, typer.Argument(help="Nouvelle racine inexistante; aucun merge autorisé.")],
) -> None:
    """Restaure vers une nouvelle racine, sans écraser ni fusionner l'état actif."""

    from hyperlab.operations import DeploymentIntegrityError, restore_backup

    try:
        result = restore_backup(backup, target)
    except (DeploymentIntegrityError, FileExistsError, OSError, ValueError) as exc:
        typer.echo(f"RESTORE_REFUSED: {exc}", err=True)
        raise typer.Exit(2) from None
    console.print_json(json.dumps(result.as_dict()))


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
        console.print(
            f"[bold red]Refus : HyperLab {__version__} n'autorise que readonly/research.[/bold red]"
        )
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

    _invalidate_collector_readiness_if_strict()
    settings = _settings()
    strict_persistence = _validate_service_mounts(settings, service="collector")
    if settings.app.mode not in {"readonly", "research"}:
        raise typer.BadParameter(
            f"Le collecteur {__version__} refuse tout mode autre que readonly/research"
        )
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
        validate_storage_integrity=strict_persistence,
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


@app.command("diagnose-binance-http")
def diagnose_binance_http(
    persistent_samples: Annotated[
        int,
        typer.Option(
            min=1,
            max=240,
            help="Mesures successives sur une seule session persistante",
        ),
    ] = 10,
    fresh_samples: Annotated[
        int,
        typer.Option(
            min=1,
            max=3,
            help="Connexions neuves séparées, bornées indépendamment",
        ),
    ] = 1,
    interval_seconds: Annotated[
        float,
        typer.Option(
            min=0.0,
            max=10.0,
            help="Pause entre mesures persistantes",
        ),
    ] = 1.0,
) -> None:
    """Compare DNS, pair/POP et timings fresh/persistants sans modifier le runtime."""
    from hyperlab.venues.binance import diagnose_binance_http_paths

    settings = _settings()
    if settings.app.mode not in {"readonly", "research"}:
        raise typer.BadParameter("Le diagnostic HTTP refuse tout mode non readonly/research")
    runtime_path = settings.app.data_dir / "runtime_status_binance_usdm.json"
    runtime_status: Mapping[str, object] | None = None
    runtime_status_error: str | None = None
    if runtime_path.exists():
        try:
            loaded = json.loads(runtime_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            runtime_status_error = f"{type(exc).__name__}: {exc}"
        else:
            if isinstance(loaded, dict):
                runtime_status = loaded
            else:
                runtime_status_error = "runtime status root is not an object"
    payload = diagnose_binance_http_paths(
        samples=persistent_samples,
        fresh_sample_count=fresh_samples,
        interval_seconds=interval_seconds,
        timeout_seconds=settings.app.request_timeout_seconds,
        runtime_status=runtime_status,
    )
    payload["runtime_status_path"] = str(runtime_path.resolve())
    payload["runtime_status_read_error"] = runtime_status_error
    console.print_json(json.dumps(payload))


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
    from hyperlab.collector.websocket import WebsocketClientFactory
    from hyperlab.collector.writer_process import CoordinatedWriterProcess
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

    writer = CoordinatedWriterProcess(
        settings.app.data_dir / "lake",
        venues=("hyperliquid", "binance_usdm"),
        batch_size=batch_size,
        queue_capacity=(hyperliquid_config.queue_capacity + binance_config.queue_capacity),
        venue_capacity_rows={
            "hyperliquid": hyperliquid_config.queue_capacity,
            "binance_usdm": binance_config.queue_capacity,
        },
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
            socket_factory=WebsocketClientFactory(queue_capacity=hyperliquid_config.queue_capacity),
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
            runtime.run(duration_seconds=None if duration_seconds == 0 else duration_seconds)
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
                "observability": {
                    "writer": writer.metrics_snapshot(),
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


def _paper_database_path(database: Path | None) -> Path:
    return (
        database
        if database is not None
        else _settings().app.data_dir / "paper" / "paper.sqlite3"
    )


def _strict_paper_config_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_paper_config_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON number {value!r}")


def _load_frozen_paper_config(path: Path) -> PaperRunConfig:
    from hyperlab.backtest.protocol import canonical_json
    from hyperlab.paper.models import PaperRunConfig

    if not path.is_file():
        raise typer.BadParameter(f"Artefact de configuration paper introuvable: {path.resolve()}")
    try:
        raw = path.read_bytes()
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_strict_paper_config_object,
            parse_constant=_reject_paper_config_constant,
        )
    except (OSError, UnicodeDecodeError, ValueError) as error:
        raise typer.BadParameter(f"Artefact paper illisible: {error}") from None
    if not isinstance(payload, dict):
        raise typer.BadParameter("L'artefact paper doit contenir un objet JSON canonique")
    try:
        config = PaperRunConfig.from_dict(payload)
    except (KeyError, TypeError, ValueError) as error:
        raise typer.BadParameter(f"Artefact paper invalide: {error}") from None
    if raw != canonical_json(config.to_dict()).encode("utf-8"):
        raise typer.BadParameter(
            "L'artefact paper n'est pas le snapshot canonique complet de PaperRunConfig"
        )
    return config


def _verify_approved_paper_admission(
    approval: _ApprovedPaperRuntimeFactories,
    frozen: PaperRunConfig,
    config_artifact: Path,
) -> None:
    """Reject until byte bindings and a measured semantic protocol both exist."""
    import hashlib

    from hyperlab.paper.admission import (
        AdmissionManifestError,
        load_admission_manifest,
        verify_admission_manifest_file,
    )

    verification = verify_admission_manifest_file(
        approval.admission_manifest_path,
        evidence_root=approval.admission_evidence_root,
    )
    if verification.manifest_sha256 != approval.admission_manifest_sha256:
        raise typer.BadParameter(
            "Le manifeste d'admission paper ne correspond pas au SHA-256 approuvé"
        )
    if verification.blockers:
        detail = "; ".join(
            f"{blocker.code}@{blocker.location}" for blocker in verification.blockers
        )
        raise typer.BadParameter(f"Admission paper bloquée par les artefacts: {detail}")
    try:
        manifest = load_admission_manifest(approval.admission_manifest_path)
        config_artifact_sha256 = hashlib.sha256(config_artifact.read_bytes()).hexdigest()
    except (AdmissionManifestError, OSError) as error:
        raise typer.BadParameter(f"Admission paper illisible: {error}") from None

    blockers: list[str] = []
    if manifest.candidate_id != approval.candidate_id:
        blockers.append("candidate identity differs from the approved registration")
    if frozen.run_kind != "VALIDATION" or not frozen.economically_eligible:
        blockers.append("only an economically eligible VALIDATION config may be approved")
    if frozen.economic_prerequisites_evidence_hash != manifest.evidence.gate_c_report.sha256:
        blockers.append("frozen Gate B/C receipt differs from the bound Gate C report")
    if frozen.strategy_hash not in {
        artifact.sha256 for artifact in manifest.evidence.strategy
    }:
        blockers.append("frozen strategy hash is not among the bound strategy artifacts")

    frozen_identity = manifest.identities.frozen_config
    if frozen_identity.identity != frozen.config_hash:
        blockers.append("frozen config identity differs from PaperRunConfig.config_hash")
    if frozen_identity.artifact.sha256 != config_artifact_sha256:
        blockers.append("frozen config bytes differ from the admission artifact")

    source_identity = manifest.identities.market_source
    if source_identity.identity != frozen.data_source:
        blockers.append("public source identity differs from PaperRunConfig.data_source")
    if source_identity.artifact.sha256 != frozen.data_hash:
        blockers.append("public source artifact differs from PaperRunConfig.data_hash")

    cost_schedule = frozen.execution.cost_schedule
    if (
        cost_schedule is None
        or cost_schedule.calibration_evidence_hash
        != manifest.identities.cost_schedule.artifact.sha256
    ):
        blockers.append("cost schedule evidence differs from the bound cost artifact")

    calibration_hashes = {
        artifact.sha256 for artifact in manifest.evidence.calibration
    }
    data_hashes = {artifact.sha256 for artifact in manifest.evidence.data}
    required_calibrations = {
        "data": frozen.data_calibration_evidence_hash,
        "execution": frozen.execution.calibration_evidence_hash,
        "maker_fill": frozen.execution.maker_fill.calibration_evidence_hash,
    }
    for label, digest in required_calibrations.items():
        if digest not in calibration_hashes | data_hashes:
            blockers.append(f"{label} calibration evidence is not byte-bound")

    blockers.extend(_production_semantic_admission_blockers(manifest.candidate_id))

    final_verification = verify_admission_manifest_file(
        approval.admission_manifest_path,
        evidence_root=approval.admission_evidence_root,
    )
    if (
        final_verification.manifest_sha256 != approval.admission_manifest_sha256
        or final_verification.blockers
    ):
        blockers.append("admission artifacts changed during admission verification")
    if blockers:
        raise typer.BadParameter("Admission paper bloquée: " + "; ".join(blockers))


def _reverify_gate_admission(config: PaperRunConfig) -> tuple[bool, str]:
    """Report the compiled-admission blocker for the read-only Gate diagnostic."""

    from hyperlab.paper.admission import (
        load_admission_manifest,
        verify_admission_manifest_file,
    )

    approval = _APPROVED_PAPER_RUNTIMES.get(config.config_hash)
    if approval is None or approval.config_hash != config.config_hash:
        return False, "NO_COMPILED_APPROVAL"
    semantic_blockers = _production_semantic_admission_blockers(approval.candidate_id)
    if semantic_blockers:
        return False, semantic_blockers[0]
    try:
        verification = verify_admission_manifest_file(
            approval.admission_manifest_path,
            evidence_root=approval.admission_evidence_root,
        )
        if (
            verification.manifest_sha256 != approval.admission_manifest_sha256
            or verification.blockers
        ):
            return False, "APPROVED_ARTIFACT_REVERIFICATION_FAILED"
        manifest = load_admission_manifest(approval.admission_manifest_path)
        relative = PurePosixPath(
            manifest.identities.frozen_config.artifact.relative_path
        )
        config_artifact = approval.admission_evidence_root.joinpath(*relative.parts)
        artifact_config = _load_frozen_paper_config(config_artifact)
        if (
            artifact_config.config_hash != config.config_hash
            or artifact_config.run_id != config.run_id
        ):
            return False, "FROZEN_CONFIG_IDENTITY_MISMATCH"
        _verify_approved_paper_admission(approval, artifact_config, config_artifact)
    except Exception:
        return False, "APPROVED_ADMISSION_REVERIFICATION_ERROR"
    return True, "VERIFIED"


def _load_stored_paper_config(database: Path, run_id: str) -> PaperRunConfig:
    from hyperlab.paper.models import PaperRunConfig
    from hyperlab.paper.store import PaperStore

    if not database.is_file():
        raise typer.BadParameter(f"Store paper introuvable: {database.resolve()}")
    store = PaperStore(database, initialize=False)
    run = store.get_run(run_id)
    try:
        config = PaperRunConfig.from_dict(run.config_snapshot)
    except (KeyError, TypeError, ValueError) as error:
        raise typer.BadParameter(f"Snapshot paper durable invalide: {error}") from None
    if config.config_hash != run.config_hash or config.run_id != run.run_id:
        raise typer.BadParameter("Le snapshot paper durable ne correspond pas à son run_id")
    return config


def _paper_as_of(value: str | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise typer.BadParameter("as-of doit être un horodatage ISO 8601 UTC") from None
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise typer.BadParameter("as-of doit être un horodatage UTC explicite")
    return parsed.astimezone(UTC)


@paper_app.command("status")
def paper_status(
    database: Annotated[
        Path | None,
        typer.Option(help="Store SQLite paper; défaut: HYPERLAB_DATA_DIR/paper/paper.sqlite3"),
    ] = None,
    run_id: Annotated[str | None, typer.Option(help="Run déterministe à détailler")] = None,
) -> None:
    """Lit l'état paper durable sans créer ni modifier le store."""
    from hyperlab.paper.store import PaperStore

    resolved = _paper_database_path(database)
    if not resolved.is_file():
        raise typer.BadParameter(f"Store paper introuvable: {resolved.resolve()}")
    store = PaperStore(resolved, initialize=False)
    if run_id is not None:
        run = store.get_run(run_id)
        integrity = store.inspect_integrity_readonly(run_id)
        if integrity.ok:
            payload: dict[str, object] = dict(store.read_snapshot(run_id))
            payload.update(
                {"integrity": "VERIFIED_READONLY", "orders_enabled": False}
            )
        else:
            payload = {
                "alerts": [],
                "commit_head_hash": run.commit_head_hash,
                "commit_sequence": run.commit_sequence,
                "config_hash": run.config_hash,
                "event_head_hash": run.event_head_hash,
                "event_sequence": run.event_sequence,
                "integrity": "FAILED_READONLY",
                "integrity_issue_codes": [issue.code for issue in integrity.issues],
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "projection": None,
                "run_id": run.run_id,
                "status": "MANUAL_REVIEW",
            }
    else:
        runs: list[dict[str, object]] = []
        for run in store.list_runs():
            integrity = store.inspect_integrity_readonly(run.run_id)
            runs.append(
                {
                    "commit_sequence": run.commit_sequence,
                    "config_hash": run.config_hash,
                    "event_sequence": run.event_sequence,
                    "integrity": (
                        "VERIFIED_READONLY" if integrity.ok else "FAILED_READONLY"
                    ),
                    "run_id": run.run_id,
                    "status": run.status if integrity.ok else "MANUAL_REVIEW",
                }
            )
        payload = {
            "database": str(resolved.resolve()),
            "mode": "PAPER_ONLY",
            "orders_enabled": False,
            "runs": runs,
        }
    console.print_json(json.dumps(payload))


@paper_app.command("gate")
def paper_gate(
    run_id: Annotated[str, typer.Argument(help="Run paper déterministe")],
    database: Annotated[
        Path | None,
        typer.Option(help="Store SQLite paper; défaut: HYPERLAB_DATA_DIR/paper/paper.sqlite3"),
    ] = None,
) -> None:
    """Évalue Gate D depuis le journal autoritaire sans modifier le store."""
    from hyperlab.paper.gate import PaperGateEvidence, evaluate_paper_gate
    from hyperlab.paper.models import PaperRunConfig
    from hyperlab.paper.store import PaperStore

    resolved = _paper_database_path(database)
    if not resolved.is_file():
        raise typer.BadParameter(f"Store paper introuvable: {resolved.resolve()}")
    store = PaperStore(resolved, initialize=False)
    run = store.get_run(run_id)
    config = PaperRunConfig.from_dict(run.config_snapshot)
    if config.config_hash != run.config_hash or config.run_id != run.run_id:
        raise typer.BadParameter("Le snapshot paper durable ne correspond pas à son run_id")
    _, admission_status = _reverify_gate_admission(config)
    evaluated_at = datetime.now(tz=UTC)
    result = evaluate_paper_gate(
        store,
        run_id,
        PaperGateEvidence(as_of=evaluated_at),
    )
    payload = result.to_dict()
    payload.update(
        {
            "admission_status": admission_status,
            "blockers": list(result.reasons),
            "mode": "PAPER_ONLY",
            "orders_enabled": False,
            "passed": result.eligible,
        }
    )
    console.print_json(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    if not result.eligible:
        raise typer.Exit(2)


@paper_app.command("replay")
def paper_replay(
    run_id: Annotated[str, typer.Argument(help="Run paper déterministe")],
    database: Annotated[
        Path | None,
        typer.Option(help="Store SQLite paper; défaut: HYPERLAB_DATA_DIR/paper/paper.sqlite3"),
    ] = None,
) -> None:
    """Vérifie en lecture seule le replay exact de la projection durable."""
    from hyperlab.paper.runtime import replay_paper_run
    from hyperlab.paper.store import PaperStore

    resolved = _paper_database_path(database)
    if not resolved.is_file():
        raise typer.BadParameter(f"Store paper introuvable: {resolved.resolve()}")
    result = replay_paper_run(PaperStore(resolved, initialize=False), run_id)
    console.print_json(json.dumps(result.to_dict()))


@paper_app.command("reconcile")
def paper_reconcile(
    run_id: Annotated[str, typer.Argument(help="Run paper déterministe")],
    database: Annotated[
        Path | None,
        typer.Option(help="Store SQLite paper; défaut: HYPERLAB_DATA_DIR/paper/paper.sqlite3"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="Horodatage logique UTC; défaut: horloge UTC courante"),
    ] = None,
) -> None:
    """Restaure puis réconcilie le ledger paper avant toute nouvelle admission."""
    from hyperlab.paper.engine import PaperEngine
    from hyperlab.paper.store import PaperStore

    resolved = _paper_database_path(database)
    config = _load_stored_paper_config(resolved, run_id)
    store = PaperStore(resolved, initialize=False)
    projection = store.get_projection(run_id)
    logical_time = _paper_as_of(as_of)
    if projection.last_received_at is not None and logical_time < projection.last_received_at:
        raise typer.BadParameter("as-of précède le dernier événement durable du run paper")
    engine = PaperEngine(store, config)
    engine.start()
    result = engine.reconcile(as_of=logical_time)
    console.print_json(
        json.dumps(
            {
                "config_hash": config.config_hash,
                "event_sequence": result.projection.last_sequence,
                "idempotent": result.append.idempotent,
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "reconciled": result.projection.reconciled,
                "run_id": config.run_id,
                "state": result.projection.state.value,
            }
        )
    )


@paper_app.command("run")
def paper_run(
    config_artifact: Annotated[
        Path,
        typer.Argument(help="Snapshot JSON canonique et approuvé de PaperRunConfig"),
    ],
    database: Annotated[
        Path | None,
        typer.Option(help="Store SQLite paper; défaut: HYPERLAB_DATA_DIR/paper/paper.sqlite3"),
    ] = None,
    timer_interval_seconds: Annotated[
        float,
        typer.Option(min=0.001, help="Cadence des contrôles de timeout paper"),
    ] = 1.0,
    source_poll_timeout_seconds: Annotated[
        float,
        typer.Option(min=0.001, help="Attente maximale de la source publique normalisée"),
    ] = 0.25,
) -> None:
    """Exécute exclusivement une liaison paper pré-approuvée et compilée dans ce checkout."""
    from hyperlab.paper.engine import PaperEngine
    from hyperlab.paper.runtime import PaperRuntime, PaperRuntimeConfig
    from hyperlab.paper.store import PaperStore

    settings = _settings()
    if settings.app.mode not in {"readonly", "research"}:
        raise typer.BadParameter("Le runtime paper refuse tout HYPERLAB_MODE non readonly/research")
    frozen = _load_frozen_paper_config(config_artifact)
    approval = _APPROVED_PAPER_RUNTIMES.get(frozen.config_hash)
    if approval is None or approval.config_hash != frozen.config_hash:
        raise typer.BadParameter(
            "Aucune liaison figée stratégie + source publique n'est approuvée pour ce config_hash"
        )

    _verify_approved_paper_admission(approval, frozen, config_artifact)
    resolved = _paper_database_path(database)
    # Approval is checked before this point, so an unapproved invocation cannot
    # create state.  An approved first run still needs the authoritative schema.
    store = PaperStore(resolved)
    runtime: PaperRuntime | None = None
    source: NormalizedPublicMarketSource | None = None
    try:
        strategy = approval.strategy_factory(frozen)
        source = approval.source_factory(frozen)
        runtime = PaperRuntime(
            PaperEngine(store, frozen),
            strategy,
            source,
            config=PaperRuntimeConfig(
                timer_interval_seconds=timer_interval_seconds,
                source_poll_timeout_seconds=source_poll_timeout_seconds,
                mode=settings.app.mode,
            ),
        )
        with _cooperative_signal_handlers(runtime.stop):
            projection = runtime.run_forever()
    except KeyboardInterrupt:
        if runtime is not None:
            runtime.stop()
        console.print("Arrêt demandé; fermeture propre du runtime paper-only.")
        projection = store.get_projection(frozen.run_id)
    finally:
        if runtime is not None:
            _close_preserving_active_exception(runtime.close)
        elif source is not None:
            _close_preserving_active_exception(source.close)
        _close_preserving_active_exception(store.close)
    console.print_json(
        json.dumps(
            {
                "config_hash": frozen.config_hash,
                "event_sequence": projection.last_sequence,
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "run_id": frozen.run_id,
                "state": projection.state.value,
            }
        )
    )


@app.command()
def serve(
    host: Annotated[str, typer.Option()] = "127.0.0.1",
    port: Annotated[int, typer.Option(min=1, max=65_535)] = 8000,
) -> None:
    """Lance le dashboard local read-only."""
    import uvicorn

    from hyperlab.dashboard.app import create_app

    settings = _settings()
    _validate_service_mounts(settings, service="dashboard")
    data_dir = settings.app.data_dir
    dashboard = create_app(
        data_dir=data_dir,
        runtime_dir=_configured_directory("HYPERLAB_RUNTIME_DIR", data_dir),
        reports_dir=_configured_directory("HYPERLAB_REPORTS_DIR", data_dir / "reports"),
        paper_dir=_configured_directory("HYPERLAB_PAPER_DIR", data_dir / "paper"),
    )
    uvicorn.run(dashboard, host=host, port=port)


if __name__ == "__main__":
    app()
