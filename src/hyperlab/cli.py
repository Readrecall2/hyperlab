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
from pathlib import Path
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
from hyperlab.environment_authorization import (
    REAL_MONEY_EXECUTION_ENABLED_IN_BUILD,
    AuthorizationManifestError,
    AuthorizationPurpose,
    EnvironmentClass,
    EnvironmentReadinessManifest,
    EvidenceCheck,
    compiled_evidence_verifier_status,
    current_paper_release_code_sha256,
    current_paper_runtime_environment_sha256,
    issue_environment_receipt,
    profile_for,
    receipt_scope_blockers,
    verify_environment_readiness,
)
from hyperlab.environment_authorization import (
    paper_release_identity_candidate as _paper_release_identity_candidate,
)
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
    from hyperlab.paper.engine import PaperCommandResult
    from hyperlab.paper.models import PaperProjection, PaperRunConfig
    from hyperlab.paper.runner import FrozenPaperStrategy
    from hyperlab.paper.runtime import NormalizedPublicMarketSource
    from hyperlab.paper.store import PaperStore

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
gate_model_app = typer.Typer(
    name="gate-model",
    help="Inspection read-only des exigences et manifestes liés à un environnement.",
    no_args_is_help=True,
)
app.add_typer(gate_model_app, name="gate-model")
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
    config_artifact_path: Path
    readiness_manifest_path: Path
    readiness_manifest_sha256: str
    readiness_profile_sha256: str
    readiness_evidence_root: Path
    strategy_factory: Callable[
        [PaperRunConfig],
        FrozenPaperStrategy | tuple[FrozenPaperStrategy, ...],
    ]
    source_factory: Callable[[PaperRunConfig], NormalizedPublicMarketSource]

    def __post_init__(self) -> None:
        if not self.candidate_id.strip():
            raise ValueError("approved paper candidate_id cannot be empty")
        for label, digest in (
            ("config_hash", self.config_hash),
            ("readiness_manifest_sha256", self.readiness_manifest_sha256),
            ("readiness_profile_sha256", self.readiness_profile_sha256),
        ):
            if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
                raise ValueError(f"approved paper {label} must be a lowercase SHA-256")
        if not isinstance(self.config_artifact_path, Path):
            raise TypeError("approved paper config_artifact_path must be a Path")
        if not isinstance(self.readiness_manifest_path, Path):
            raise TypeError("approved paper readiness_manifest_path must be a Path")
        if not isinstance(self.readiness_evidence_root, Path):
            raise TypeError("approved paper readiness_evidence_root must be a Path")


_PHASE12_PAPER_CANDIDATE_ID = "phase08-robust-pairs-btc-eth-paper-v1"
_PHASE12_MULTISTRATEGY_CANDIDATE_ID = "phase08-phase05-multistrategy-paper-v1"
_PHASE12_PAPER_ARTIFACT_ROOT = Path("config/paper/phase08-phase05-multistrategy-paper-v1")
_PHASE12_PAPER_CONFIG_ARTIFACT = _PHASE12_PAPER_ARTIFACT_ROOT / "paper-config.json"
_PHASE12_PAPER_READINESS_MANIFEST = _PHASE12_PAPER_ARTIFACT_ROOT / "readiness-manifest.json"
_PHASE12_PAPER_EVIDENCE_ROOT = _PHASE12_PAPER_ARTIFACT_ROOT
_PHASE12_PAPER_RUNTIME_TIMER_INTERVAL_SECONDS = 1.0
_PHASE12_PAPER_RUNTIME_SOURCE_POLL_TIMEOUT_SECONDS = 0.25
_PHASE12_PAPER_CONFIG_HASH = "4f081a7c8ae57e51cb8b0185fc4a46baa65e49e778b85868f2b02b9bc4a23934"
_PHASE12_PAPER_READINESS_MANIFEST_SHA256 = "82f818253081e142351bbbd873148dfb8377985ba43ff154e8edb0df36a185e6"
_PHASE12_PAPER_READINESS_PROFILE_SHA256 = "e727a03939928ea6de0201a7c58c542519669a6ec4f1575be89f3eaf10f0136a"
_PHASE12_MULTISTRATEGY_CONFIG_HASH = "a6ba9e1984895e739e4712008f8be353b06133c91dfc223ead6b7d2a0c489850"
_PHASE12_MULTISTRATEGY_READINESS_MANIFEST_SHA256 = "d2db411464a1f494361baadfe6ff70c48e70b667262d447722405cd6820cb1e8"
_PHASE12_MULTISTRATEGY_READINESS_PROFILE_SHA256 = "e727a03939928ea6de0201a7c58c542519669a6ec4f1575be89f3eaf10f0136a"


def _phase12_paper_strategy_factory(
    config: PaperRunConfig,
) -> tuple[FrozenPaperStrategy, ...]:
    from hyperlab.paper.phase05_portfolio import build_phase05_phase08_paper_foundation

    foundation = build_phase05_phase08_paper_foundation(
        runtime_status_path=(
            _settings().app.data_dir / "paper" / "phase12-multistrategy-source-status.json"
        ),
        validation_started_at=config.validation_started_at,
        release_code_sha256=config.release_code_sha256,
        runtime_environment_sha256=config.runtime_environment_sha256,
    )
    try:
        expected = tuple(item.strategy_config_hash for item in config.strategy_configs)
        actual = tuple(
            str(getattr(item, "strategy_config_hash", "")) for item in foundation.strategies
        )
        if config.schema_version != 3 or actual != expected:
            raise ValueError("frozen Phase 05 + Phase 08 strategies differ from PaperRunConfig")
        return foundation.strategies
    finally:
        foundation.source.close()


def _phase12_paper_source_factory(
    config: PaperRunConfig,
) -> NormalizedPublicMarketSource:
    from hyperlab.paper.collector_source import (
        PHASE12_PHASE05_PUBLIC_SOURCE_NAME,
        HyperliquidPaperPublicSource,
    )

    if config.data_source == PHASE12_PHASE05_PUBLIC_SOURCE_NAME:
        source = HyperliquidPaperPublicSource.create_mainnet_portfolio(
            runtime_status_path=(
                _settings().app.data_dir / "paper" / "phase12-multistrategy-source-status.json"
            )
        )
    else:
        source = HyperliquidPaperPublicSource.create_mainnet(
            runtime_status_path=(
                _settings().app.data_dir / "paper" / "phase12-public-source-status.json"
            )
        )
    descriptor = source.descriptor
    if config.data_source != descriptor.source or config.data_hash != descriptor.data_hash:
        source.close()
        raise ValueError("frozen public source descriptor differs from PaperRunConfig")
    return source


# One exact current candidate only, separate from the frozen historical V9 evidence.
# The CLI never imports a user-supplied module or resolves an arbitrary strategy.
_APPROVED_PAPER_RUNTIMES: Mapping[str, _ApprovedPaperRuntimeFactories] = MappingProxyType(
    {
        _PHASE12_MULTISTRATEGY_CONFIG_HASH: _ApprovedPaperRuntimeFactories(
            candidate_id=_PHASE12_MULTISTRATEGY_CANDIDATE_ID,
            config_hash=_PHASE12_MULTISTRATEGY_CONFIG_HASH,
            config_artifact_path=_PHASE12_PAPER_CONFIG_ARTIFACT,
            readiness_manifest_path=_PHASE12_PAPER_READINESS_MANIFEST,
            readiness_manifest_sha256=_PHASE12_MULTISTRATEGY_READINESS_MANIFEST_SHA256,
            readiness_profile_sha256=_PHASE12_MULTISTRATEGY_READINESS_PROFILE_SHA256,
            readiness_evidence_root=_PHASE12_PAPER_EVIDENCE_ROOT,
            strategy_factory=_phase12_paper_strategy_factory,
            source_factory=_phase12_paper_source_factory,
        )
    }
)

# Intentionally empty and non-authorizing. A future candidate must add a concrete
# measured-result protocol whose canonical Gate B/C decision is derived by core;
# merely registering a callback or returning PASS booleans must never admit it.
_TRUSTED_PAPER_SEMANTIC_EVALUATORS: Mapping[str, None] = MappingProxyType({})


def _production_semantic_admission_blockers(candidate_id: str) -> tuple[str, ...]:
    if candidate_id not in _TRUSTED_PAPER_SEMANTIC_EVALUATORS:
        return ("NO_TRUSTED_CANDIDATE_SEMANTIC_EVALUATOR",)
    return ("SEMANTIC_EVALUATOR_PROTOCOL_NOT_IMPLEMENTED",)


def _parse_environment_class(value: str) -> EnvironmentClass:
    try:
        return EnvironmentClass(value)
    except ValueError:
        allowed = ", ".join(item.value for item in EnvironmentClass)
        raise typer.BadParameter(f"environment must be one exact compiled identity: {allowed}") from None


@gate_model_app.command("requirements")
def gate_model_requirements(
    environment: Annotated[
        str,
        typer.Argument(help="Identité exacte: RESEARCH_REPLAY, PAPER, TESTNET, MICRO_MAINNET ou MAINNET"),
    ],
) -> None:
    """Publie le profil compilé sans créer de reçu ni modifier un artefact."""

    profile = profile_for(_parse_environment_class(environment))
    payload = profile.to_dict()
    payload.update(
        {
            "profile_sha256": profile.profile_sha256,
            "real_money_execution_enabled_in_build": REAL_MONEY_EXECUTION_ENABLED_IN_BUILD,
            "semantic_verifiers": compiled_evidence_verifier_status(profile.environment),
            "status": "REQUIREMENTS",
        }
    )
    console.print_json(json.dumps(payload, ensure_ascii=False, sort_keys=True))


@gate_model_app.command("check")
def gate_model_check(
    manifest_path: Annotated[
        Path,
        typer.Argument(help="Manifeste JSON canonique EnvironmentReadinessManifest"),
    ],
    evidence_root: Annotated[
        Path,
        typer.Option(help="Racine immuable contenant les preuves byte-bound"),
    ],
) -> None:
    """Vérifie un manifeste en lecture seule; un reçu n'existe que pour READY."""

    import hashlib

    try:
        raw = manifest_path.read_bytes()
    except OSError as error:
        payload: dict[str, object] = {
            "blockers": [
                {
                    "code": "MANIFEST_UNREADABLE",
                    "location": "$",
                    "message": str(error),
                }
            ],
            "manifest_sha256": None,
            "ready": False,
            "status": "BLOCKED",
        }
        console.print_json(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        raise typer.Exit(2) from None
    try:
        manifest = EnvironmentReadinessManifest.from_json_bytes(raw, require_canonical=True)
    except AuthorizationManifestError as error:
        payload = {
            "blockers": [
                {
                    "code": error.code,
                    "location": error.location,
                    "message": error.message,
                }
            ],
            "manifest_sha256": hashlib.sha256(raw).hexdigest(),
            "ready": False,
            "status": "BLOCKED",
        }
        console.print_json(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        raise typer.Exit(2) from None

    decision = verify_environment_readiness(manifest, evidence_root=evidence_root)
    result: dict[str, object] = {
        "authorizes_real_money": False,
        "blockers": [
            {
                "code": blocker.code,
                "location": blocker.location,
                "message": blocker.message,
            }
            for blocker in decision.blockers
        ],
        "environment": manifest.environment.value,
        "manifest_sha256": decision.manifest_sha256,
        "profile_sha256": decision.profile_sha256,
        "purpose": manifest.purpose.value,
        "ready": decision.ready,
        "semantic_verifiers": compiled_evidence_verifier_status(manifest.environment),
        "status": "READY" if decision.ready else "BLOCKED",
        "verifier_set_sha256": decision.verifier_set_sha256,
    }
    if decision.ready:
        receipt = issue_environment_receipt(decision)
        receipt_payload = receipt.to_dict()
        receipt_payload.update(
            {
                "authorizes_real_money": receipt.authorizes_real_money,
                "receipt_sha256": receipt.receipt_sha256,
            }
        )
        result["authorizes_real_money"] = receipt.authorizes_real_money
        result["receipt"] = receipt_payload
    console.print_json(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if not decision.ready:
        raise typer.Exit(2)


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
            round_trip_fees_bps=(2.0 * (settings.costs.spot_fee_bps + settings.costs.perp_fee_bps)),
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
                panel.liquidation_usd.loc[final_index].copy() if panel.liquidation_usd is not None else None
            ),
            available_at=(
                panel.available_at.loc[final_index].copy() if panel.available_at is not None else None
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
        elif any(result.diagnostics.get("audit_status") != "CALIBRATED" for result in (base_result, ranking)):
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
    output: Annotated[Path, typer.Option(help="Dossier du rapport Phase 08")] = Path("reports/pairs"),
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
        typer.Option(help="Export Phase 09 point-in-time avec volume, OI, funding et liquidations"),
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
    output: Annotated[Path, typer.Option(help="Dossier du rapport Phase 09")] = Path("reports/momentum"),
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
    output: Annotated[Path, typer.Option(help="Dossier du rapport Phase 11")] = Path("reports/market-making"),
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
        calibration_status=("CALIBRATED" if calibration_evidence_hash is not None else "UNCALIBRATED"),
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
        raise typer.BadParameter("la matrice de panne exige 24 heures d'incident puis une barre de reprise")
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
        if resolved_outage_start.tz is None or resolved_outage_start.utcoffset() != pd.Timedelta(0):
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
        raise typer.BadParameter(f"Le collecteur {__version__} refuse tout mode autre que readonly/research")
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
    return database if database is not None else _settings().app.data_dir / "paper" / "paper.sqlite3"


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
        raise typer.BadParameter("L'artefact paper n'est pas le snapshot canonique complet de PaperRunConfig")
    return config


def _verify_approved_paper_readiness(
    approval: _ApprovedPaperRuntimeFactories,
    frozen: PaperRunConfig,
    config_artifact: Path,
) -> None:
    """Verify exact PAPER technical readiness without consulting economic gates."""

    import hashlib

    from hyperlab.backtest.protocol import canonical_sha256

    try:
        manifest_bytes = approval.readiness_manifest_path.read_bytes()
        manifest = EnvironmentReadinessManifest.from_json_bytes(
            manifest_bytes,
            require_canonical=True,
        )
        config_artifact_sha256 = hashlib.sha256(config_artifact.read_bytes()).hexdigest()
    except (AuthorizationManifestError, OSError) as error:
        raise typer.BadParameter(f"Readiness paper illisible: {error}") from None
    if manifest.manifest_sha256 != approval.readiness_manifest_sha256:
        raise typer.BadParameter("Le manifeste de readiness paper ne correspond pas au SHA-256 approuvé")

    decision = verify_environment_readiness(
        manifest,
        evidence_root=approval.readiness_evidence_root,
    )
    if decision.blockers:
        detail = "; ".join(f"{blocker.code}@{blocker.location}" for blocker in decision.blockers)
        raise typer.BadParameter(f"Readiness paper bloquée par les artefacts: {detail}")
    if decision.profile_sha256 != approval.readiness_profile_sha256:
        raise typer.BadParameter(
            "Le profil readiness paper et ses vérificateurs ne correspondent pas au profil approuvé"
        )

    blockers: list[str] = []
    try:
        release_code_sha256 = current_paper_release_code_sha256(
            candidate_id=_paper_release_identity_candidate(approval.candidate_id),
        )
    except (OSError, TypeError, ValueError) as error:
        release_code_sha256 = None
        blockers.append(f"current release-code digest unavailable: {error}")
    try:
        runtime_environment_sha256 = current_paper_runtime_environment_sha256(
            candidate_id=_paper_release_identity_candidate(approval.candidate_id),
        )
    except (OSError, TypeError, ValueError) as error:
        runtime_environment_sha256 = None
        blockers.append(
            f"current runtime-environment digest unavailable: {error}"
        )
    if release_code_sha256 != frozen.release_code_sha256:
        blockers.append("frozen release_code_sha256 differs from the current reviewed checkout")
    if runtime_environment_sha256 != frozen.runtime_environment_sha256:
        blockers.append("frozen runtime_environment_sha256 differs from the current runtime")
    if (
        manifest.environment is not EnvironmentClass.PAPER
        or manifest.purpose is not AuthorizationPurpose.PAPER_RUNTIME
    ):
        blockers.append("readiness receipt is not scoped to PAPER/PAPER_RUNTIME")
    if manifest.subject.candidate_id != approval.candidate_id:
        blockers.append("candidate identity differs from the approved registration")
    expected_subject = {
        "build_hash": frozen.engine_build_hash,
        "config_hash": frozen.config_hash,
        "risk_limits_hash": canonical_sha256(frozen.risk.to_dict()),
        "source_identity": frozen.data_source,
        "strategy_hash": frozen.strategy_hash,
    }
    for label, expected in expected_subject.items():
        if getattr(manifest.subject, label) != expected:
            blockers.append(f"readiness subject {label} differs from the frozen paper config")
    if approval.config_hash != frozen.config_hash:
        blockers.append("approved registration config_hash differs from the frozen paper config")
    if frozen.schema_version not in {2, 3} or frozen.environment != "PAPER":
        blockers.append("paper runtime requires schema v2/v3 with explicit PAPER environment")
    if frozen.schema_version == 3 and (
        len(frozen.strategy_configs) < 2
        or frozen.data_calibration_status != "UNCALIBRATED"
        or frozen.execution.calibration_status != "UNCALIBRATED"
        or frozen.economically_eligible
    ):
        blockers.append(
            "multi-strategy technical Paper must remain explicitly uncalibrated and non-economic"
        )
    if frozen.run_kind not in {"TECHNICAL", "VALIDATION"}:
        blockers.append("paper runtime requires a TECHNICAL or VALIDATION run_kind")
    if not frozen.required_instruments:
        blockers.append("paper runtime requires a non-empty frozen instrument universe")
    if (
        frozen.runtime_timer_interval_seconds != _PHASE12_PAPER_RUNTIME_TIMER_INTERVAL_SECONDS
        or frozen.runtime_source_poll_timeout_seconds != _PHASE12_PAPER_RUNTIME_SOURCE_POLL_TIMEOUT_SECONDS
    ):
        blockers.append("paper runtime cadence differs from the exact compiled Phase 12 cadence")

    if EvidenceCheck.FROZEN_STRATEGY_CONFIG not in manifest.evidence:
        blockers.append("compiled FROZEN_STRATEGY_CONFIG evidence is missing")
    if EvidenceCheck.PUBLIC_MARKET_SOURCE not in manifest.evidence:
        blockers.append("compiled PUBLIC_MARKET_SOURCE evidence is missing")

    cost_schedule = frozen.execution.cost_schedule
    if cost_schedule is None and frozen.schema_version == 2:
        blockers.append("paper runtime requires a point-in-time cost schedule")
    elif cost_schedule is not None:
        for instrument in frozen.required_instruments:
            try:
                cost_schedule.lookup(pd.Timestamp(frozen.validation_started_at), instrument)
            except ValueError as error:
                blockers.append(str(error))

    receipt = issue_environment_receipt(decision)
    scope_blockers = receipt_scope_blockers(
        receipt,
        environment=EnvironmentClass.PAPER,
        purpose=AuthorizationPurpose.PAPER_RUNTIME,
        config_hash=frozen.config_hash,
    )
    blockers.extend(f"{blocker.code}@{blocker.location}" for blocker in scope_blockers)
    if receipt.authorizes_real_money:
        blockers.append("PAPER readiness receipt must never authorize real money")

    try:
        final_manifest_bytes = approval.readiness_manifest_path.read_bytes()
        final_manifest = EnvironmentReadinessManifest.from_json_bytes(
            final_manifest_bytes,
            require_canonical=True,
        )
        final_release_code_sha256 = current_paper_release_code_sha256(
            candidate_id=_paper_release_identity_candidate(approval.candidate_id),
        )
        final_runtime_environment_sha256 = current_paper_runtime_environment_sha256(
            candidate_id=_paper_release_identity_candidate(approval.candidate_id),
        )
        final_decision = verify_environment_readiness(
            final_manifest,
            evidence_root=approval.readiness_evidence_root,
        )
        final_config_sha256 = hashlib.sha256(config_artifact.read_bytes()).hexdigest()
    except (AuthorizationManifestError, OSError, TypeError, ValueError) as error:
        blockers.append(f"readiness artifacts became unreadable: {error}")
    else:
        if (
            final_manifest.manifest_sha256 != approval.readiness_manifest_sha256
            or final_manifest.to_dict() != manifest.to_dict()
            or final_decision.blockers
            or final_decision.profile_sha256 != approval.readiness_profile_sha256
            or final_config_sha256 != config_artifact_sha256
            or final_release_code_sha256 != release_code_sha256
            or final_runtime_environment_sha256 != runtime_environment_sha256
        ):
            blockers.append("readiness artifacts changed during readiness verification")
    if blockers:
        raise typer.BadParameter("Readiness paper bloquée: " + "; ".join(blockers))


def _reverify_gate_readiness(config: PaperRunConfig) -> tuple[bool, str]:
    """Report the compiled PAPER readiness status for the read-only Gate diagnostic."""

    import hashlib

    approval = _APPROVED_PAPER_RUNTIMES.get(config.config_hash)
    if approval is None or approval.config_hash != config.config_hash:
        return False, "NO_COMPILED_READINESS"
    try:
        manifest = EnvironmentReadinessManifest.from_json_bytes(
            approval.readiness_manifest_path.read_bytes(),
            require_canonical=True,
        )
        if (
            manifest.manifest_sha256 != approval.readiness_manifest_sha256
            or manifest.environment is not EnvironmentClass.PAPER
            or manifest.purpose is not AuthorizationPurpose.PAPER_RUNTIME
        ):
            return False, "APPROVED_READINESS_REVERIFICATION_FAILED"
        decision = verify_environment_readiness(
            manifest,
            evidence_root=approval.readiness_evidence_root,
        )
        if decision.blockers:
            return False, "APPROVED_READINESS_REVERIFICATION_FAILED"
        if decision.profile_sha256 != approval.readiness_profile_sha256:
            return False, "APPROVED_READINESS_PROFILE_MISMATCH"
        config_artifact = approval.config_artifact_path
        if hashlib.sha256(config_artifact.read_bytes()).hexdigest() != approval.config_hash:
            return False, "FROZEN_CONFIG_ARTIFACT_HASH_MISMATCH"
        artifact_config = _load_frozen_paper_config(config_artifact)
        if artifact_config.config_hash != config.config_hash or artifact_config.run_id != config.run_id:
            return False, "FROZEN_CONFIG_IDENTITY_MISMATCH"
        _verify_approved_paper_readiness(approval, artifact_config, config_artifact)
    except Exception:
        return False, "APPROVED_READINESS_REVERIFICATION_ERROR"
    return True, "VERIFIED"


def _load_stored_paper_config(database: Path, run_id: str) -> PaperRunConfig:
    from hyperlab.paper.models import PaperRunConfig
    from hyperlab.paper.store import PaperStore

    if not database.is_file():
        raise typer.BadParameter(f"Store paper introuvable: {database.resolve()}")
    store = PaperStore(database, initialize=False)
    try:
        run = store.get_run(run_id)
        try:
            config = PaperRunConfig.from_dict(run.config_snapshot)
        except (KeyError, TypeError, ValueError) as error:
            raise typer.BadParameter(f"Snapshot paper durable invalide: {error}") from None
        if config.config_hash != run.config_hash or config.run_id != run.run_id:
            raise typer.BadParameter("Le snapshot paper durable ne correspond pas à son run_id")
        return config
    finally:
        _close_preserving_active_exception(store.close)


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


def _paper_runtime_settings() -> Settings:
    settings = _settings()
    if settings.app.mode not in {"readonly", "research"}:
        raise typer.BadParameter("Le runtime paper refuse tout HYPERLAB_MODE non readonly/research")
    return settings


def _paper_release_candidate_for_config(config: PaperRunConfig) -> str:
    approval = _APPROVED_PAPER_RUNTIMES.get(config.config_hash)
    if approval is not None and approval.config_hash == config.config_hash:
        return approval.candidate_id

    canonical_approval = _APPROVED_PAPER_RUNTIMES.get(
        _PHASE12_MULTISTRATEGY_CONFIG_HASH
    )
    if canonical_approval is not None:
        canonical_config = _load_frozen_paper_config(
            canonical_approval.config_artifact_path
        )
        canonical_payload = canonical_config.to_dict()
        operator_payload = config.to_dict()
        canonical_payload.pop("runtime_environment_sha256")
        operator_payload.pop("runtime_environment_sha256")
        if operator_payload == canonical_payload:
            return canonical_approval.candidate_id

    return _PHASE12_PAPER_CANDIDATE_ID


def _require_current_paper_release(config: PaperRunConfig) -> None:
    candidate_id = _paper_release_identity_candidate(
        _paper_release_candidate_for_config(config)
    )
    try:
        current_release_code_sha256 = current_paper_release_code_sha256(
            candidate_id=candidate_id,
        )
    except (OSError, TypeError, ValueError) as error:
        raise typer.BadParameter(f"Le digest release-code paper courant est invérifiable: {error}") from None
    if current_release_code_sha256 != config.release_code_sha256:
        raise typer.BadParameter("Le code Paper courant diffère du release_code_sha256 durable")
    try:
        current_runtime_environment_sha256 = (
            current_paper_runtime_environment_sha256(candidate_id=candidate_id)
        )
    except (OSError, TypeError, ValueError) as error:
        raise typer.BadParameter(
            f"Paper runtime-environment digest is unverifiable: {error}"
        ) from None
    if (
        current_runtime_environment_sha256
        != config.runtime_environment_sha256
    ):
        raise typer.BadParameter(
            "Paper runtime environment differs from durable "
            "runtime_environment_sha256"
        )


def _approved_paper_runtime_for(
    config: PaperRunConfig,
    config_artifact: Path | None = None,
) -> _ApprovedPaperRuntimeFactories:
    approval = _APPROVED_PAPER_RUNTIMES.get(config.config_hash)
    if approval is not None and approval.config_hash == config.config_hash:
        return approval

    canonical_approval = _APPROVED_PAPER_RUNTIMES.get(_PHASE12_MULTISTRATEGY_CONFIG_HASH)
    if canonical_approval is None or config_artifact is None:
        raise typer.BadParameter(
            "Aucune liaison figee strategie + source publique n'est approuvee pour ce config_hash"
        )
    if config_artifact.name != _PHASE12_PAPER_CONFIG_ARTIFACT.name:
        raise typer.BadParameter("Le bundle operateur Paper doit utiliser le nom canonique paper-config.json")
    canonical_config = _load_frozen_paper_config(
        canonical_approval.config_artifact_path
    )
    if (
        canonical_config.config_hash != _PHASE12_MULTISTRATEGY_CONFIG_HASH
        or canonical_approval.config_hash != _PHASE12_MULTISTRATEGY_CONFIG_HASH
    ):
        raise typer.BadParameter(
            "La configuration Paper compilee de reference a derive"
        )

    canonical_payload = canonical_config.to_dict()
    operator_payload = config.to_dict()
    canonical_payload.pop("runtime_environment_sha256")
    operator_payload.pop("runtime_environment_sha256")
    if operator_payload != canonical_payload:
        raise typer.BadParameter(
            "Aucune liaison fig\u00e9e strat\u00e9gie + source publique n'est approuv\u00e9e "
            "pour ce config_hash"
        )

    manifest_path = config_artifact.parent / "readiness-manifest.json"
    try:
        manifest = EnvironmentReadinessManifest.from_json_bytes(
            manifest_path.read_bytes(),
            require_canonical=True,
        )
    except (AuthorizationManifestError, OSError) as error:
        raise typer.BadParameter(
            f"Bundle operateur Paper illisible: {error}"
        ) from None

    return _ApprovedPaperRuntimeFactories(
        candidate_id=canonical_approval.candidate_id,
        config_hash=config.config_hash,
        config_artifact_path=config_artifact,
        readiness_manifest_path=manifest_path,
        readiness_manifest_sha256=manifest.manifest_sha256,
        readiness_profile_sha256=(canonical_approval.readiness_profile_sha256),
        readiness_evidence_root=config_artifact.parent,
        strategy_factory=canonical_approval.strategy_factory,
        source_factory=canonical_approval.source_factory,
    )


def _paper_exact_operator_reason(value: str, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise typer.BadParameter(f"{label} ne peut pas etre vide")
    if value != value.strip():
        raise typer.BadParameter(f"{label} doit etre exact, sans espace initial ou final")
    if len(value) > 1_024:
        raise typer.BadParameter(f"{label} depasse 1024 caracteres")
    return value


def _paper_operator_artifact_hash(
    *,
    action: str,
    config: PaperRunConfig,
    reason: str,
    as_of: datetime,
    incident_artifact_hash: str | None = None,
) -> str:
    from hyperlab.paper.models import deterministic_id, utc_text

    components: list[object] = [
        action,
        config.run_id,
        config.config_hash,
        utc_text(as_of),
        reason,
    ]
    if incident_artifact_hash is not None:
        components.append(incident_artifact_hash)
    return deterministic_id("paper_operator_artifact_v1", *components)


def _paper_reviewed_incident_summary(
    projection: PaperProjection,
) -> tuple[int, str | None]:
    from hyperlab.paper.models import utc_text

    return (
        projection.critical_incident_count,
        (
            utc_text(projection.last_critical_incident_at)
            if projection.last_critical_incident_at is not None
            else None
        ),
    )


def _paper_resume_incident_artifact_hash(
    config: PaperRunConfig,
    projection: PaperProjection,
) -> str:
    from hyperlab.paper.models import deterministic_id, utc_text

    critical_incident_count, last_critical_incident_at = (
        _paper_reviewed_incident_summary(projection)
    )
    return deterministic_id(
        "paper_resume_incident_artifact_v2",
        config.run_id,
        config.config_hash,
        projection.state.value,
        utc_text(projection.state_since or config.validation_started_at),
        critical_incident_count,
        last_critical_incident_at or "NO_CRITICAL_INCIDENT",
    )


def _paper_operator_time(
    projection: PaperProjection,
    value: str | None,
) -> datetime:
    logical_time = _paper_as_of(value)
    if projection.last_received_at is not None and logical_time < projection.last_received_at:
        raise typer.BadParameter("as-of precede le dernier evenement durable du run paper")
    return logical_time


@paper_app.command("preflight")
def paper_preflight(
    config_artifact: Annotated[
        Path,
        typer.Argument(help="Snapshot JSON canonique approuve; defaut: artefact Phase 08 compile"),
    ] = _PHASE12_PAPER_CONFIG_ARTIFACT,
) -> None:
    """Verifie l'admission Paper et les identites sans store ni transport reseau."""

    _paper_runtime_settings()
    frozen = _load_frozen_paper_config(config_artifact)
    approval = _approved_paper_runtime_for(frozen, config_artifact)
    _verify_approved_paper_readiness(approval, frozen, config_artifact)

    source: NormalizedPublicMarketSource | None = None
    try:
        strategy_binding = approval.strategy_factory(frozen)
        strategies = strategy_binding if isinstance(strategy_binding, tuple) else (strategy_binding,)
        actual_identities = [
            {
                "strategy_hash": strategy.strategy_hash,
                "strategy_id": getattr(strategy, "strategy_id", None),
                "strategy_name": strategy.strategy_name,
            }
            for strategy in strategies
        ]
        expected_identities = [
            {
                "strategy_hash": item.strategy_hash,
                "strategy_id": item.strategy_id if frozen.schema_version == 3 else None,
                "strategy_name": item.strategy_name,
            }
            for item in frozen.strategy_configs
        ]
        if actual_identities != expected_identities:
            raise typer.BadParameter("Les strategies compilees different de la configuration Paper")
        source = approval.source_factory(frozen)
        descriptor = source.descriptor
        if descriptor.source != frozen.data_source or descriptor.data_hash != frozen.data_hash:
            raise typer.BadParameter("La source publique compilee differe de la configuration Paper")
        payload = {
            "authorization_purpose": "PAPER_RUNTIME",
            "authorizes_real_money": False,
            "candidate_id": approval.candidate_id,
            "config_artifact": str(config_artifact.resolve()),
            "config_hash": frozen.config_hash,
            "credential_scope": "NONE",
            "database_created": False,
            "environment": "PAPER",
            "execution_network": "NONE",
            "mode": "PAPER_ONLY",
            "orders_enabled": False,
            "public_source": asdict(descriptor),
            "public_transport_started": False,
            "readiness_manifest_sha256": approval.readiness_manifest_sha256,
            "readiness_profile_sha256": approval.readiness_profile_sha256,
            "run_id": frozen.run_id,
            "status": "READY",
            "strategies": actual_identities,
            "strategy_hash": frozen.strategy_hash,
            "strategy_name": frozen.strategy_name,
            "wallet_or_signer_required": False,
        }
    except typer.BadParameter:
        raise
    except (OSError, TypeError, ValueError) as error:
        raise typer.BadParameter(f"Preflight Paper bloque: {error}") from None
    finally:
        if source is not None:
            _close_preserving_active_exception(source.close)
    console.print_json(json.dumps(payload, ensure_ascii=False, sort_keys=True))


class _PaperStatusHeadChangedError(RuntimeError):
    """One bounded status read raced a durable Paper commit."""


def _paper_status_payload_once(
    store: PaperStore,
    *,
    database: Path,
    run_id: str | None,
    run_limit: int,
) -> dict[str, object]:
    from hyperlab.paper.reporting import paper_runtime_session_health

    if run_id is not None:
        run = store.get_run(run_id)
        integrity = store.inspect_head_integrity_readonly(run_id)
        if integrity.ok:
            alert_limit = 50
            recent_alerts = store.get_recent_alerts(
                run_id,
                limit=alert_limit + 1,
            )
            alerts_truncated = len(recent_alerts) > alert_limit
            projection = store.get_projection(run_id).to_dict()
            runtime_session = paper_runtime_session_health(projection)
            runtime_session["recent_incidents"] = [
                {
                    "alert": alert.alert,
                    "alert_id": alert.alert_id,
                    "code": alert.code,
                    "created_at": alert.created_at,
                    "event_sequence": alert.event_sequence,
                    "severity": alert.severity,
                }
                for alert in recent_alerts[-alert_limit:]
                if alert.code == "PAPER_RUNTIME_FAILURE"
            ]
            payload: dict[str, object] = {
                "alert_limit": alert_limit,
                "alerts": [alert.alert for alert in recent_alerts[-alert_limit:]],
                "alerts_truncated": alerts_truncated,
                "commit_head_hash": run.commit_head_hash,
                "commit_sequence": run.commit_sequence,
                "config_hash": run.config_hash,
                "event_head_hash": run.event_head_hash,
                "event_sequence": run.event_sequence,
                "head_read_attempt_limit": 2,
                "integrity": "HEAD_ANCHORS_VERIFIED_READONLY",
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "projection": projection,
                "run_id": run.run_id,
                "runtime_session": runtime_session,
                "same_head_assembly": True,
                "status": run.status,
            }
        else:
            payload = {
                "alerts": [],
                "commit_head_hash": run.commit_head_hash,
                "commit_sequence": run.commit_sequence,
                "config_hash": run.config_hash,
                "event_head_hash": run.event_head_hash,
                "event_sequence": run.event_sequence,
                "head_read_attempt_limit": 2,
                "integrity": "HEAD_ANCHORS_FAILED_READONLY",
                "integrity_issue_codes": [issue.code for issue in integrity.issues],
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "projection": None,
                "run_id": run.run_id,
                "same_head_assembly": True,
                "status": "MANUAL_REVIEW",
            }
        if store.get_run(run_id).head_identity != run.head_identity:
            raise _PaperStatusHeadChangedError
        return payload

    listed_runs = store.list_runs(limit=run_limit + 1)
    runs_truncated = len(listed_runs) > run_limit
    selected_runs = listed_runs[-run_limit:]
    runs: list[dict[str, object]] = []
    for run in selected_runs:
        integrity = store.inspect_head_integrity_readonly(run.run_id)
        listed_projection = store.get_projection_payload(run.run_id) if integrity.ok else None
        runs.append(
            {
                "commit_sequence": run.commit_sequence,
                "config_hash": run.config_hash,
                "event_sequence": run.event_sequence,
                "integrity": (
                    "HEAD_ANCHORS_VERIFIED_READONLY" if integrity.ok else "HEAD_ANCHORS_FAILED_READONLY"
                ),
                "run_id": run.run_id,
                "runtime_session": (
                    paper_runtime_session_health(listed_projection)
                    if listed_projection is not None
                    else None
                ),
                "status": run.status if integrity.ok else "MANUAL_REVIEW",
            }
        )
    final_listed_runs = store.list_runs(limit=run_limit + 1)
    if tuple(run.head_identity for run in final_listed_runs) != tuple(
        run.head_identity for run in listed_runs
    ):
        raise _PaperStatusHeadChangedError
    return {
        "database": str(database.resolve()),
        "head_read_attempt_limit": 2,
        "mode": "PAPER_ONLY",
        "orders_enabled": False,
        "run_limit": run_limit,
        "runs": runs,
        "runs_truncated": runs_truncated,
        "same_head_assembly": True,
    }


def _paper_status_payload(
    store: PaperStore,
    *,
    database: Path,
    run_id: str | None,
    run_limit: int,
) -> dict[str, object]:
    for _attempt in range(2):
        try:
            return _paper_status_payload_once(
                store,
                database=database,
                run_id=run_id,
                run_limit=run_limit,
            )
        except _PaperStatusHeadChangedError:
            continue
    raise typer.BadParameter("HEAD_CHANGED_RETRY: durable Paper head changed during two reads")


@paper_app.command("status")
def paper_status(
    database: Annotated[
        Path | None,
        typer.Option(help="Store SQLite paper; défaut: HYPERLAB_DATA_DIR/paper/paper.sqlite3"),
    ] = None,
    run_id: Annotated[str | None, typer.Option(help="Run déterministe à détailler")] = None,
    run_limit: Annotated[
        int,
        typer.Option(min=1, max=100, help="Runs récents retournés en mode liste"),
    ] = 50,
) -> None:
    """Lit l'état paper durable sans créer ni modifier le store."""
    from hyperlab.paper.store import PaperStore

    resolved = _paper_database_path(database)
    if not resolved.is_file():
        raise typer.BadParameter(f"Store paper introuvable: {resolved.resolve()}")
    store = PaperStore(resolved, initialize=False)
    payload = _paper_status_payload(
        store,
        database=resolved,
        run_id=run_id,
        run_limit=run_limit,
    )
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
    _, readiness_status = _reverify_gate_readiness(config)
    evaluated_at = datetime.now(tz=UTC)
    result = evaluate_paper_gate(
        store,
        run_id,
        PaperGateEvidence(as_of=evaluated_at),
    )
    payload = result.to_dict()
    payload.update(
        {
            "authorization_purpose": "PAPER_RUNTIME",
            "authorizes_real_money": False,
            "blockers": list(result.reasons),
            "environment": "PAPER",
            "mode": "PAPER_ONLY",
            "orders_enabled": False,
            "passed": result.eligible,
            "readiness_status": readiness_status,
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
    _paper_runtime_settings()
    from hyperlab.paper.runtime import (
        PaperAdmissionError,
        PaperRuntimeLease,
        replay_paper_run,
    )
    from hyperlab.paper.store import PaperStore

    resolved = _paper_database_path(database)
    if not resolved.is_file():
        raise typer.BadParameter(f"Store paper introuvable: {resolved.resolve()}")
    config = _load_stored_paper_config(resolved, run_id)
    _require_current_paper_release(config)
    try:
        with PaperRuntimeLease(resolved, run_id):
            _require_current_paper_release(config)
            store = PaperStore(resolved, initialize=False)
            try:
                result = replay_paper_run(store, run_id)
            finally:
                _close_preserving_active_exception(store.close)
    except PaperAdmissionError as error:
        raise typer.BadParameter(f"Paper replay blocked: {error}") from None
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

    _paper_runtime_settings()
    from hyperlab.paper.engine import PaperEngine
    from hyperlab.paper.runtime import PaperAdmissionError, PaperRuntimeLease
    from hyperlab.paper.store import PaperStore

    resolved = _paper_database_path(database)
    config = _load_stored_paper_config(resolved, run_id)
    _require_current_paper_release(config)
    logical_time = _paper_as_of(as_of)
    try:
        with PaperRuntimeLease(resolved, run_id):
            _require_current_paper_release(config)
            store = PaperStore(resolved, initialize=False)
            try:
                projection = store.get_projection(run_id)
                if projection.last_received_at is not None and logical_time < projection.last_received_at:
                    raise typer.BadParameter("as-of précède le dernier événement durable du run paper")
                engine = PaperEngine(store, config)
                engine.start()
                result = engine.reconcile(as_of=logical_time)
            finally:
                _close_preserving_active_exception(store.close)
    except PaperAdmissionError as error:
        raise typer.BadParameter(f"Réconciliation paper bloquée: {error}") from None
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


@paper_app.command("report")
def paper_report(
    run_id: Annotated[str, typer.Argument(help="Run paper déterministe")],
    database: Annotated[
        Path | None,
        typer.Option(help="Store SQLite paper; défaut: HYPERLAB_DATA_DIR/paper/paper.sqlite3"),
    ] = None,
    after_sequence: Annotated[
        int,
        typer.Option(min=0, help="Premier curseur de timeline exclusif"),
    ] = 0,
    timeline_limit: Annotated[
        int,
        typer.Option(min=1, max=500, help="événements de timeline retournés"),
    ] = 100,
    day_limit: Annotated[
        int,
        typer.Option(min=1, max=366, help="Jours UTC retournés"),
    ] = 31,
    alert_limit: Annotated[
        int,
        typer.Option(min=1, max=200, help="Alertes récentes retournées"),
    ] = 50,
) -> None:
    """Produit un rapport borné et strictement read-only depuis le journal."""

    _paper_runtime_settings()
    from hyperlab.paper.reporting import (
        PaperReportIntegrityError,
        build_paper_report,
    )
    from hyperlab.paper.store import PaperStore

    resolved = _paper_database_path(database)
    if not resolved.is_file():
        raise typer.BadParameter(f"Store paper introuvable: {resolved.resolve()}")
    store = PaperStore(resolved, initialize=False)
    try:
        payload = build_paper_report(
            store,
            run_id,
            after_sequence=after_sequence,
            timeline_limit=timeline_limit,
            day_limit=day_limit,
            alert_limit=alert_limit,
        )
    except (OSError, PaperReportIntegrityError, ValueError) as error:
        raise typer.BadParameter(f"Rapport paper bloqué: {error}") from None
    finally:
        store.close()
    console.print_json(json.dumps(payload, ensure_ascii=False, sort_keys=True))


@paper_app.command("pause")
def paper_pause(
    run_id: Annotated[str, typer.Argument(help="Run paper déterministe")],
    reason: Annotated[
        str,
        typer.Option("--reason", help="Motif opérateur exact, journalisé"),
    ],
    database: Annotated[
        Path | None,
        typer.Option(help="Store SQLite paper; défaut: HYPERLAB_DATA_DIR/paper/paper.sqlite3"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="Horodatage logique UTC; défaut: horloge UTC courante"),
    ] = None,
) -> None:
    """Suspend durablement les nouvelles entrées simulées, sans transport."""

    _paper_runtime_settings()
    from hyperlab.paper.engine import PaperEngine
    from hyperlab.paper.store import PaperStore

    normalized_reason = _paper_exact_operator_reason(reason, label="reason")
    resolved = _paper_database_path(database)
    config = _load_stored_paper_config(resolved, run_id)
    store = PaperStore(resolved, initialize=False)
    try:
        projection = store.get_projection(run_id)
        logical_time = _paper_operator_time(projection, as_of)
        artifact_hash = _paper_operator_artifact_hash(
            action="PAUSE",
            config=config,
            reason=normalized_reason,
            as_of=logical_time,
        )
        engine = PaperEngine(store, config)
        engine.start()
        result = engine.pause(
            as_of=logical_time,
            reason=normalized_reason,
            operator_artifact_hash=artifact_hash,
        )
    except (OSError, ValueError) as error:
        raise typer.BadParameter(f"Pause paper bloquée: {error}") from None
    finally:
        store.close()
    console.print_json(
        json.dumps(
            {
                "action": "PAUSE",
                "config_hash": config.config_hash,
                "event_sequence": result.projection.last_sequence,
                "mode": "PAPER_ONLY",
                "operator_artifact_hash": artifact_hash,
                "orders_enabled": False,
                "run_id": run_id,
                "state": result.projection.state.value,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


def _paper_resume_from_store(
    *,
    store: PaperStore,
    config: PaperRunConfig,
    run_id: str,
    normalized_review: str,
    as_of: str | None,
    recovery_mode: str,
) -> tuple[PaperCommandResult, str, str]:
    from hyperlab.paper.engine import PaperEngine
    from hyperlab.paper.models import PaperState

    projection = store.get_projection(run_id)
    if projection.state is PaperState.MANUAL_REVIEW:
        raise typer.BadParameter("MANUAL_REVIEW est terminal et ne peut jamais être repris")
    if projection.state is not PaperState.PAUSED:
        raise typer.BadParameter("resume exige un état durable PAUSED")
    logical_time = _paper_operator_time(projection, as_of)
    reviewed_critical_incident_count = projection.critical_incident_count
    reviewed_last_critical_incident_at = projection.last_critical_incident_at
    incident_artifact_hash = _paper_resume_incident_artifact_hash(
        config,
        projection,
    )
    review_artifact_hash = _paper_operator_artifact_hash(
        action=f"RESUME_AFTER_REVIEW:{recovery_mode}",
        config=config,
        reason=normalized_review,
        as_of=logical_time,
        incident_artifact_hash=incident_artifact_hash,
    )
    engine = PaperEngine(store, config)
    engine.start()
    if engine.projection().state is PaperState.MANUAL_REVIEW:
        raise typer.BadParameter("MANUAL_REVIEW est terminal et ne peut jamais être repris")
    reconciliation = engine.reconcile(as_of=logical_time)
    if reconciliation.projection.state is PaperState.MANUAL_REVIEW:
        raise typer.BadParameter("La réconciliation a verrouillé MANUAL_REVIEW; reprise interdite")
    result = engine.resume_from_pause(
        as_of=logical_time,
        review_artifact_hash=review_artifact_hash,
        reviewed_critical_incident_count=reviewed_critical_incident_count,
        reviewed_last_critical_incident_at=reviewed_last_critical_incident_at,
        recovery_mode=recovery_mode,
    )
    return result, incident_artifact_hash, review_artifact_hash


@paper_app.command("resume")
def paper_resume(
    run_id: Annotated[str, typer.Argument(help="Run paper déterministe")],
    review_reason: Annotated[
        str,
        typer.Option("--review-reason", help="Conclusion exacte de la revue opérateur"),
    ],
    database: Annotated[
        Path | None,
        typer.Option(help="Store SQLite paper; défaut: HYPERLAB_DATA_DIR/paper/paper.sqlite3"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="Horodatage logique UTC; défaut: horloge UTC courante"),
    ] = None,
    offline_unclosed_recovery: Annotated[
        bool,
        typer.Option(
            "--offline-unclosed-recovery",
            help=(
                "Reprise explicite d'une session runtime durable non fermée; "
                "exige que le runtime soit arrêté"
            ),
        ),
    ] = False,
) -> None:
    """Réconcilie puis reprend un PAUSED selon un mode de revue explicite."""

    _paper_runtime_settings()
    from hyperlab.paper.runtime import PaperAdmissionError, PaperRuntimeLease
    from hyperlab.paper.store import PaperStore

    normalized_review = _paper_exact_operator_reason(
        review_reason,
        label="review-reason",
    )
    resolved = _paper_database_path(database)
    config = _load_stored_paper_config(resolved, run_id)
    _require_current_paper_release(config)
    recovery_mode = (
        "OFFLINE_UNCLOSED_SESSION" if offline_unclosed_recovery else "STANDARD"
    )
    try:
        if offline_unclosed_recovery:
            with PaperRuntimeLease(resolved, run_id):
                leased_config = _load_stored_paper_config(resolved, run_id)
                _require_current_paper_release(leased_config)
                if leased_config.to_dict() != config.to_dict():
                    raise typer.BadParameter(
                        "Le snapshot paper durable a changé avant la reprise offline"
                    )
                store = PaperStore(resolved, initialize=False)
                try:
                    result, incident_artifact_hash, review_artifact_hash = (
                        _paper_resume_from_store(
                            store=store,
                            config=leased_config,
                            run_id=run_id,
                            normalized_review=normalized_review,
                            as_of=as_of,
                            recovery_mode=recovery_mode,
                        )
                    )
                finally:
                    _close_preserving_active_exception(store.close)
        else:
            store = PaperStore(resolved, initialize=False)
            try:
                result, incident_artifact_hash, review_artifact_hash = (
                    _paper_resume_from_store(
                        store=store,
                        config=config,
                        run_id=run_id,
                        normalized_review=normalized_review,
                        as_of=as_of,
                        recovery_mode=recovery_mode,
                    )
                )
            finally:
                _close_preserving_active_exception(store.close)
    except typer.BadParameter:
        raise
    except (OSError, PaperAdmissionError, ValueError) as error:
        raise typer.BadParameter(f"Reprise paper bloquée: {error}") from None
    console.print_json(
        json.dumps(
            {
                "action": "RESUME_AFTER_REVIEW",
                "config_hash": config.config_hash,
                "event_sequence": result.projection.last_sequence,
                "incident_artifact_hash": incident_artifact_hash,
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "reconciled": result.projection.reconciled,
                "recovery_mode": recovery_mode,
                "review_artifact_hash": review_artifact_hash,
                "run_id": run_id,
                "state": result.projection.state.value,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


@paper_app.command("kill")
def paper_kill(
    run_id: Annotated[str, typer.Argument(help="Run paper déterministe")],
    reason: Annotated[
        str,
        typer.Option("--reason", help="Motif opérateur exact, journalisé"),
    ],
    confirm_run_id: Annotated[
        str,
        typer.Option(
            "--confirm-run-id",
            help="Confirmation irréversible: répéter exactement le run_id",
        ),
    ],
    database: Annotated[
        Path | None,
        typer.Option(help="Store SQLite paper; défaut: HYPERLAB_DATA_DIR/paper/paper.sqlite3"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="Horodatage logique UTC; défaut: horloge UTC courante"),
    ] = None,
) -> None:
    """Verrouille irréversiblement MANUAL_REVIEW et termine les ordres simulés."""

    _paper_runtime_settings()
    from hyperlab.paper.engine import PaperEngine
    from hyperlab.paper.store import PaperStore

    normalized_reason = _paper_exact_operator_reason(reason, label="reason")
    if confirm_run_id != run_id:
        raise typer.BadParameter("confirm-run-id doit correspondre exactement au run_id")
    resolved = _paper_database_path(database)
    config = _load_stored_paper_config(resolved, run_id)
    store = PaperStore(resolved, initialize=False)
    try:
        projection = store.get_projection(run_id)
        logical_time = _paper_operator_time(projection, as_of)
        artifact_hash = _paper_operator_artifact_hash(
            action="KILL",
            config=config,
            reason=normalized_reason,
            as_of=logical_time,
        )
        engine = PaperEngine(store, config)
        engine.start()
        result = engine.kill(
            as_of=logical_time,
            reason=normalized_reason,
            operator_artifact_hash=artifact_hash,
        )
    except (OSError, ValueError) as error:
        raise typer.BadParameter(f"Kill paper bloqué: {error}") from None
    finally:
        store.close()
    console.print_json(
        json.dumps(
            {
                "action": "KILL",
                "config_hash": config.config_hash,
                "event_sequence": result.projection.last_sequence,
                "mode": "PAPER_ONLY",
                "operator_artifact_hash": artifact_hash,
                "orders_enabled": False,
                "resumable": False,
                "run_id": run_id,
                "state": result.projection.state.value,
                "terminal": True,
            },
            ensure_ascii=False,
            sort_keys=True,
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
        float | None,
        typer.Option(min=0.001, help="Doit égaler la cadence figée du PaperRunConfig"),
    ] = None,
    source_poll_timeout_seconds: Annotated[
        float | None,
        typer.Option(min=0.001, help="Doit égaler l'attente source figée du PaperRunConfig"),
    ] = None,
) -> None:
    """Exécute exclusivement une liaison paper pré-approuvée et compilée dans ce checkout."""
    from hyperlab.paper.engine import PaperEngine
    from hyperlab.paper.runtime import (
        PaperRuntime,
        PaperRuntimeConfig,
        PaperStartupInterrupted,
    )
    from hyperlab.paper.store import PaperStore

    settings = _settings()
    if settings.app.mode not in {"readonly", "research"}:
        raise typer.BadParameter("Le runtime paper refuse tout HYPERLAB_MODE non readonly/research")
    frozen = _load_frozen_paper_config(config_artifact)
    approval = _approved_paper_runtime_for(frozen, config_artifact)
    if approval is None or approval.config_hash != frozen.config_hash:
        raise typer.BadParameter(
            "Aucune liaison figée stratégie + source publique n'est approuvée pour ce config_hash"
        )
    if timer_interval_seconds is not None and timer_interval_seconds != frozen.runtime_timer_interval_seconds:
        raise typer.BadParameter("timer-interval-seconds diffère de la cadence figée du PaperRunConfig")
    if (
        source_poll_timeout_seconds is not None
        and source_poll_timeout_seconds != frozen.runtime_source_poll_timeout_seconds
    ):
        raise typer.BadParameter("source-poll-timeout-seconds diffère de l'attente figée du PaperRunConfig")
    frozen_timer_interval_seconds = frozen.runtime_timer_interval_seconds
    frozen_source_poll_timeout_seconds = frozen.runtime_source_poll_timeout_seconds

    _verify_approved_paper_readiness(approval, frozen, config_artifact)
    resolved = _paper_database_path(database)
    # Readiness is checked before this point, so an unapproved invocation cannot
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
                timer_interval_seconds=frozen_timer_interval_seconds,
                source_poll_timeout_seconds=frozen_source_poll_timeout_seconds,
                mode=settings.app.mode,
            ),
        )
        with _cooperative_signal_handlers(runtime.stop):
            projection = runtime.run_forever()
    except (KeyboardInterrupt, PaperStartupInterrupted):
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
                "authorization_purpose": "PAPER_RUNTIME",
                "authorizes_real_money": False,
                "config_hash": frozen.config_hash,
                "environment": "PAPER",
                "event_sequence": projection.last_sequence,
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "run_id": frozen.run_id,
                "runtime_source_poll_timeout_seconds": frozen_source_poll_timeout_seconds,
                "runtime_timer_interval_seconds": frozen_timer_interval_seconds,
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
