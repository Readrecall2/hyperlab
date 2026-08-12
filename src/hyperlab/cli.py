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

import typer
from rich.console import Console
from rich.table import Table

from hyperlab.backtest.carry import (
    audit_carry_panel,
    carry_stress_scenarios,
    evaluate_carry_gate,
    write_carry_report,
)
from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.report import write_comparison_report
from hyperlab.backtest.workflow import ResearchWorkflowSpec, run_research_workflow
from hyperlab.config import Settings, load_settings
from hyperlab.data.cli import data_app
from hyperlab.data.io import load_panel_csv, save_panel_csv
from hyperlab.data.synthetic import generate_demo_panel, generate_microstructure_demo
from hyperlab.models import BacktestResult
from hyperlab.storage.sqlite import database_status, save_carry_snapshots
from hyperlab.strategies.market_making import InventoryAwareMarketMaker
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
    console.print(
        "[yellow]Ces résultats synthétiques valident l'installation, jamais une rentabilité.[/yellow]"
    )


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
    stress_scenarios = carry_stress_scenarios() if strategy == "cash_and_carry" else None

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
            final_liquidation_bars=2 if strategy == "cash_and_carry" else 0,
        ),
        output_dir=output,
        registry_path=research.registry_path,
        stress_scenarios=stress_scenarios,
        final_reporter=phase05_reporter if strategy == "cash_and_carry" else None,
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
        console.print(f"Rapport et gate Phase 05 : {artifacts.supplemental_report_path.resolve()}")


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
