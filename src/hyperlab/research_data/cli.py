from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer

from .adapters import KalshiPublicAdapter, PolymarketPublicAdapter
from .canonical import canonical_json_bytes, canonical_value, decode_canonical_json
from .envelope import Venue
from .h1_campaign import collect_h1_campaign, prepare_h1_campaign
from .lighter_report import (
    LIGHTER_GREEN,
    LIGHTER_OFFICIAL_READONLY_WS_ACCESS_GREEN,
    LIGHTER_OFFICIAL_WS_PUBLIC_ACCESS_GREEN,
    LIGHTER_PUBLIC_ACCESS_EXHAUSTED_OFFICIAL_PATHS,
    write_lighter_access_completion_report,
    write_lighter_probe_report,
)
from .prediction_bundle import (
    PredictionBundleSource,
    PredictionUnavailableSource,
    build_prediction_research_bundle,
    verify_prediction_research_bundle,
)
from .prediction_candidate import (
    CandidatePreregistration,
    PredictionCollectionPlan,
    prepare_prediction_campaign,
)
from .prediction_contracts import OfficialPublicContract
from .probe import ProbeConfig, recover_public_probe_output, run_public_probe

research_data_app = typer.Typer(
    name="research-data",
    help="Collecte locale bornée PUBLIC_DATA_ONLY et outils offline du Research Data Plane V1.",
    no_args_is_help=True,
)


def _utc_option(value: str, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise typer.BadParameter(f"{label} doit être ISO-8601 avec fuseau") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise typer.BadParameter(f"{label} doit inclure un fuseau")
    return parsed.astimezone(UTC)


def _raise_for_probe_terminal_health(terminal_health: str) -> None:
    exit_code = {
        "PUBLIC_SOURCE_UNAVAILABLE": 3,
        "PUBLIC_SOURCE_INVALID": 4,
        "BACKPRESSURE_LIMIT_REACHED": 5,
        "CONTINUITY_BROKEN_FROZEN": 5,
        "CONTINUITY_UNKNOWN_AFTER_RECONNECT_FROZEN": 5,
        "INTERRUPTED_RECOVERABLE": 130,
    }.get(terminal_health)
    if exit_code is not None:
        raise typer.Exit(exit_code)


@research_data_app.command("h1-prepare")
def h1_prepare(
    campaign_root: Annotated[
        Path,
        typer.Option("--campaign-root", help="Nouveau répertoire de campagne H1."),
    ],
    starts_at_utc: Annotated[
        str,
        typer.Option("--starts-at-utc", help="Début UTC ISO-8601 figé avant collecte."),
    ],
    fee_reviewed_at_utc: Annotated[
        str,
        typer.Option("--fee-reviewed-at-utc", help="Revue humaine UTC des frais publics."),
    ],
    config: Annotated[
        Path,
        typer.Option("--config", help="Politique H1 préenregistrée."),
    ] = Path("config/research/hyperliquid-h1-ghost-v1.json"),
    fee_artifact: Annotated[
        Path,
        typer.Option("--fee-artifact", help="Artefact public tier-0 revu point-in-time."),
    ] = Path("config/paper/hyperliquid-tier0-fees-2026-08-16.json"),
) -> None:
    """Fige une campagne prospective sans démarrer de transport."""

    preflight = {
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "completion_signal": "campaign-manifest.json + campaign-manifest.sha256",
        "ctrl_c": "interruption avant publication laisse au plus un répertoire incomplet local",
        "execution_location": f"Windows PowerShell local:{Path.cwd().absolute()}",
        "expected_duration_seconds": 1,
        "max_duration_seconds": 30,
        "monitoring": str(campaign_root.absolute() / "state" / "health.json"),
        "prompt_behavior": "NO_PROMPT",
    }
    typer.echo(json.dumps(preflight, ensure_ascii=False, sort_keys=True))
    try:
        result = prepare_h1_campaign(
            campaign_root.absolute(),
            config_path=config.absolute(),
            fee_artifact_path=fee_artifact.absolute(),
            starts_at_utc=_utc_option(starts_at_utc, label="--starts-at-utc"),
            fee_reviewed_at_utc=_utc_option(fee_reviewed_at_utc, label="--fee-reviewed-at-utc"),
        )
    except (FileExistsError, OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(
        json.dumps(
            {
                "campaign_id": result.campaign_id,
                "manifest_sha256": result.manifest_sha256,
                "status": "PREPARED_NOT_STARTED",
            },
            sort_keys=True,
        )
    )


@research_data_app.command("h1-collect")
def h1_collect(
    campaign_root: Annotated[
        Path,
        typer.Option("--campaign-root", help="Campagne H1 préalablement figée."),
    ],
    config: Annotated[
        Path,
        typer.Option("--config", help="Politique H1 exactement figée."),
    ] = Path("config/research/hyperliquid-h1-ghost-v1.json"),
    resume: Annotated[
        bool,
        typer.Option("--resume", help="Reprend explicitement une chaîne Research existante."),
    ] = False,
) -> None:
    """Collecte publique H1 reprenable; aucune capacité privée ou d'exécution."""

    preflight = {
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "completion_signal": "state/health.json avec terminal_health final",
        "ctrl_c": "clôture le tail authentifié; reprise explicite avec --resume",
        "execution_location": f"Windows PowerShell local:{Path.cwd().absolute()}",
        "expected_duration": "7-14 jours",
        "max_duration": "14 jours plus finalisation bornée",
        "monitoring": str(campaign_root.absolute() / "state" / "health.json"),
        "prompt_behavior": "NO_PROMPT",
    }
    typer.echo(json.dumps(preflight, ensure_ascii=False, sort_keys=True))

    def progress(health: object) -> None:
        typer.echo(json.dumps(health, ensure_ascii=False, sort_keys=True))

    try:
        health = collect_h1_campaign(
            campaign_root.absolute(),
            config_path=config.absolute(),
            resume=resume,
            progress=progress,
        )
    except (FileExistsError, FileNotFoundError, OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(json.dumps(health, ensure_ascii=False, sort_keys=True))
    terminal = health["terminal_health"]
    if terminal == "INTERRUPTED_RECOVERABLE":
        raise typer.Exit(130)
    if terminal in {
        "FINAL_THRESHOLD_REPLAY_INVALID_FAIL_CLOSED",
        "MAX_BYTES_REACHED",
        "PUBLIC_SOURCE_INVALID_FAIL_CLOSED",
        "PUBLIC_SOURCE_UNAVAILABLE_RECOVERABLE",
        "THRESHOLD_CANDIDATE_NOT_FINAL_RESUME_REQUIRED",
    }:
        raise typer.Exit(4)


@research_data_app.command("lighter-report")
def lighter_report(
    output_root: Annotated[
        Path,
        typer.Option(
            "--output-root",
            help="Sortie existante d'un probe Lighter; vérification strictement offline.",
        ),
    ],
) -> None:
    """Authenticate one Lighter manifest and publish its deterministic bounded report."""

    if not output_root.is_dir():
        raise typer.BadParameter("--output-root doit être un probe Lighter existant")
    report = write_lighter_probe_report(output_root.absolute())
    typer.echo(json.dumps(report, ensure_ascii=False, sort_keys=True))
    if report["verdict"] != LIGHTER_GREEN:
        raise typer.Exit(3)


@research_data_app.command("lighter-access-completion")
def lighter_access_completion(
    output_root: Annotated[
        Path,
        typer.Option(
            "--output-root",
            help="Nouveau répertoire immuable du complément officiel WebSocket Lighter.",
        ),
    ],
) -> None:
    """Run the one-shot, two-handshake-max Lighter public WebSocket completion."""

    if output_root.exists():
        raise typer.BadParameter("--output-root doit être neuf")
    resolved_output = output_root.absolute()
    preflight = {
        "average_duration": "NOT_ESTIMABLE_NO_PRIOR_SUCCESSFUL_COMPLETION",
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "collection_max_duration_seconds": 600,
        "completion_signal": ("reports/lighter-official-public-access-completion-v1.json + verdict"),
        "ctrl_c": ("clôture les frames admises en état récupérable; aucune nouvelle tentative"),
        "execution_location": f"Windows PowerShell local:{Path.cwd().absolute()}",
        "handshake_sequence": [
            "wss://mainnet.zklighter.elliot.ai/stream",
            "wss://mainnet.zklighter.elliot.ai/stream?readonly=true IF_NORMAL_FAILS_BEFORE_COLLECTION",
        ],
        "market_index": 0,
        "max_bytes": 64 * 1024 * 1024,
        "max_frames": 5_000,
        "max_segments": 4,
        "maximum_wall_clock_seconds_including_terminalization": 615,
        "monitoring": str(resolved_output / "reports" / "health.json"),
        "output_root": str(resolved_output),
        "prompt_behavior": "NO_PROMPT",
        "proxy_policy": "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED",
        "retry_policy": "NO_AUTOMATIC_RETRY_OR_RECONNECT",
    }
    typer.echo(json.dumps(preflight, ensure_ascii=False, sort_keys=True))

    def _progress(frame_count: int) -> None:
        typer.echo(json.dumps({"frames": frame_count, "status": "RUNNING"}, sort_keys=True))

    report = run_public_probe(
        ProbeConfig(
            output_root=resolved_output,
            venue=Venue.LIGHTER,
            feeds=("order_book", "ticker", "market_stats", "trades"),
            instruments=("0",),
            census_limit=0,
            duration_seconds=600,
            max_bytes=64 * 1024 * 1024,
            max_segment_bytes=16 * 1024 * 1024,
            rotation_seconds=150.0,
            progress_interval_seconds=10.0,
            max_frames=5_000,
            max_segments=4,
        ),
        progress=_progress,
    )
    completion = write_lighter_access_completion_report(resolved_output)
    typer.echo(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    typer.echo(json.dumps(completion, ensure_ascii=False, sort_keys=True))
    verdict = completion["verdict"]
    if verdict in {
        LIGHTER_OFFICIAL_WS_PUBLIC_ACCESS_GREEN,
        LIGHTER_OFFICIAL_READONLY_WS_ACCESS_GREEN,
    }:
        return
    if verdict == LIGHTER_PUBLIC_ACCESS_EXHAUSTED_OFFICIAL_PATHS:
        raise typer.Exit(3)
    raise typer.Exit(4)


def _csv(value: str, *, label: str) -> tuple[str, ...]:
    items = tuple(item.strip() for item in value.split(",") if item.strip())
    if not items:
        raise typer.BadParameter(f"{label} doit contenir au moins une valeur explicite")
    if len(set(items)) != len(items):
        raise typer.BadParameter(f"{label} contient un doublon")
    return items


@research_data_app.command("probe")
def probe(
    output_root: Annotated[
        Path,
        typer.Option(
            "--output-root",
            help="Nouveau répertoire obligatoire; raw/ et reports/ y seront créés.",
        ),
    ],
    venue: Annotated[Venue, typer.Option("--venue", help="Venue publique obligatoire.")],
    feeds: Annotated[
        str,
        typer.Option(
            "--feeds",
            help="Feeds explicites séparés par des virgules; aucun défaut caché.",
        ),
    ],
    duration_seconds: Annotated[
        int,
        typer.Option(
            "--duration-seconds",
            min=120,
            max=600,
            help="Durée: Lighter 120-600 s; autres venues 120-300 s.",
        ),
    ],
    instruments: Annotated[
        str,
        typer.Option(
            "--instruments",
            help="Instruments, token IDs ou tickers explicites séparés par des virgules.",
        ),
    ] = "",
    census_limit: Annotated[
        int,
        typer.Option(
            "--census-limit",
            min=0,
            max=100,
            help="Census public borné utilisé seulement sans instruments explicites.",
        ),
    ] = 0,
    max_bytes: Annotated[
        int,
        typer.Option("--max-bytes", min=4096, help="Borne physique stricte des segments raw."),
    ] = 64 * 1024 * 1024,
    segment_bytes: Annotated[
        int,
        typer.Option("--segment-bytes", min=1024, help="Rotation déterministe par taille logique."),
    ] = 4 * 1024 * 1024,
    rotation_seconds: Annotated[
        float,
        typer.Option("--rotation-seconds", min=1.0, help="Rotation déterministe par durée monotone."),
    ] = 30.0,
    progress_interval: Annotated[
        float,
        typer.Option("--progress-interval", min=1.0, help="Intervalle d'observabilité console."),
    ] = 10.0,
    max_frames: Annotated[
        int,
        typer.Option("--max-frames", min=1, max=50_000, help="Borne stricte de frames admises."),
    ] = 5_000,
    max_segments: Annotated[
        int,
        typer.Option("--max-segments", min=1, max=100, help="Borne stricte de segments publiés."),
    ] = 4,
    max_network_calls: Annotated[
        int,
        typer.Option(
            "--max-network-calls",
            min=1,
            max=1_000,
            help="Borne stricte cumulée des requêtes HTTP et handshakes WebSocket.",
        ),
    ] = 100,
) -> None:
    """Probe. Exit: 0 complete, 3 unavailable, 4 invalid, 5 backpressure, 130 interrupted."""

    selected_feeds = _csv(feeds, label="feeds")
    selected_instruments = () if not instruments.strip() else _csv(instruments, label="instruments")
    if selected_instruments and census_limit:
        raise typer.BadParameter("choisir instruments explicites OU census borné, pas les deux")
    if not selected_instruments and census_limit <= 0:
        raise typer.BadParameter("instruments explicites ou --census-limit positif requis")
    if output_root.exists():
        raise typer.BadParameter("--output-root doit être neuf")
    if segment_bytes > max_bytes:
        raise typer.BadParameter("--segment-bytes ne peut pas dépasser --max-bytes")
    resolved_output = output_root.absolute()
    preflight = {
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "completion_signal": "reports/result.json + terminal_health",
        "ctrl_c": "clôture les frames admises ou laisse un état recoverable; aucun ordre externe",
        "execution_location": f"Windows PowerShell local:{Path.cwd().absolute()}",
        "expected_duration_seconds": duration_seconds,
        "feeds": list(selected_feeds),
        "instruments": list(selected_instruments),
        "census_limit": census_limit,
        "collection_max_duration_seconds": duration_seconds,
        "terminalization_allowance_seconds": 15,
        "max_bytes": max_bytes,
        "max_frames": max_frames,
        "max_network_calls": max_network_calls,
        "max_segments": max_segments,
        "monitoring": str(resolved_output / "reports" / "health.json"),
        "output_root": str(resolved_output),
        "prompt_behavior": "NO_PROMPT",
        "proxy_policy": "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED",
        "segment_bytes": segment_bytes,
        "rotation_seconds": rotation_seconds,
        "venue": venue.value,
    }
    typer.echo(json.dumps(preflight, ensure_ascii=False, sort_keys=True))

    last_50k = 0

    def _progress(frame_count: int) -> None:
        nonlocal last_50k
        typer.echo(json.dumps({"frames": frame_count, "status": "RUNNING"}, sort_keys=True))
        current_50k = frame_count // 50_000
        if current_50k > last_50k:
            typer.echo(
                json.dumps(
                    {"records_checkpoint": current_50k * 50_000, "status": "RUNNING"},
                    sort_keys=True,
                )
            )
            last_50k = current_50k

    try:
        config = ProbeConfig(
            output_root=resolved_output,
            venue=venue,
            feeds=selected_feeds,
            instruments=selected_instruments,
            census_limit=census_limit,
            duration_seconds=duration_seconds,
            max_bytes=max_bytes,
            max_segment_bytes=segment_bytes,
            rotation_seconds=rotation_seconds,
            progress_interval_seconds=progress_interval,
            max_frames=max_frames,
            max_segments=max_segments,
            max_network_calls=max_network_calls,
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error
    try:
        report = run_public_probe(config, progress=_progress)
    except FileExistsError as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    _raise_for_probe_terminal_health(report.terminal_health)


def _prediction_contracts(
    polymarket_contract: Path,
    kalshi_contract: Path,
    candidate_config: Path,
) -> tuple[
    OfficialPublicContract,
    OfficialPublicContract,
    CandidatePreregistration,
]:
    try:
        polymarket = OfficialPublicContract.from_path(polymarket_contract)
        kalshi = OfficialPublicContract.from_path(kalshi_contract)
        candidate = CandidatePreregistration.from_path(candidate_config)
    except (OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    if polymarket.venue is not Venue.POLYMARKET or kalshi.venue is not Venue.KALSHI:
        raise typer.BadParameter("les contrats officiels sont associés aux mauvaises venues")
    return polymarket, kalshi, candidate


def _validate_prediction_shard_window(
    *,
    now: datetime,
    scheduled_start: datetime,
    collection_duration_seconds: int,
    cadence_seconds: int,
) -> None:
    if now < scheduled_start:
        raise typer.BadParameter("prediction shard cannot begin before its scheduled start")
    slot_end = scheduled_start + timedelta(seconds=cadence_seconds)
    if now + timedelta(seconds=collection_duration_seconds) > slot_end:
        raise typer.BadParameter(
            "prediction shard lacks its full frozen collection duration before slot end; "
            "backfill is forbidden"
        )


def _datetime_utc_ns(value: datetime) -> int:
    normalized = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = normalized - epoch
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _campaign_binding(
    *,
    campaign_manifest: Path,
    venue: Venue,
    polymarket: OfficialPublicContract,
    kalshi: OfficialPublicContract,
    candidate: CandidatePreregistration,
    shard_ordinal: int,
) -> tuple[str, str, str, PredictionCollectionPlan, str, int]:
    try:
        decoded = json.loads(campaign_manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise typer.BadParameter("campaign manifest must be strict UTF-8 JSON") from error
    if not isinstance(decoded, dict):
        raise typer.BadParameter("campaign manifest must be an object")
    declared_hash = decoded.get("manifest_sha256")
    if not isinstance(declared_hash, str):
        raise typer.BadParameter("campaign manifest hash is missing")
    body = {key: value for key, value in decoded.items() if key != "manifest_sha256"}
    canonical = canonical_value(body)
    if not isinstance(canonical, dict):
        raise typer.BadParameter("campaign manifest body is invalid")
    actual_hash = hashlib.sha256(canonical_json_bytes(canonical)).hexdigest()
    if actual_hash != declared_hash:
        raise typer.BadParameter("campaign manifest self-hash diverged")
    if (
        decoded.get("boundary") != "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
        or decoded.get("status") != "AWAITING_HUMAN_EXECUTION"
        or decoded.get("candidate_id") != candidate.candidate_id
        or decoded.get("candidate_config_sha256") != candidate.config_sha256
        or decoded.get("vps_or_h1_path") != "NONE"
    ):
        raise typer.BadParameter("campaign manifest boundary or candidate binding diverged")
    contract_map = decoded.get("contracts")
    expected_contracts = {
        Venue.POLYMARKET.value: polymarket.contract_sha256,
        Venue.KALSHI.value: kalshi.contract_sha256,
    }
    if contract_map != expected_contracts:
        raise typer.BadParameter("campaign manifest official contract binding diverged")
    if decoded.get("runner_policy_sha256") != candidate.runner_policy.policy_sha256:
        raise typer.BadParameter("campaign manifest runner policy binding diverged")
    if (
        decoded.get("prospective_shard_policy")
        != candidate.prospective_shard_policy.to_dict()
        or decoded.get("prospective_shard_policy_sha256")
        != candidate.prospective_shard_policy.policy_sha256
    ):
        raise typer.BadParameter("campaign manifest prospective shard policy diverged")
    campaign_id = decoded.get("campaign_id")
    if type(campaign_id) is not str or not campaign_id:
        raise typer.BadParameter("campaign manifest campaign id is missing")
    expected_plans = {
        item.value: candidate.collection_plans[item].to_dict(campaign_id=campaign_id)
        for item in (Venue.POLYMARKET, Venue.KALSHI)
    }
    if decoded.get("collection_plans") != expected_plans:
        raise typer.BadParameter("campaign manifest collection plans diverged")
    starts_at = _utc_option(str(decoded.get("starts_at_utc")), label="campaign start")
    try:
        scheduled_start = candidate.prospective_shard_policy.scheduled_start(
            starts_at,
            shard_ordinal,
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error
    _validate_prediction_shard_window(
        now=datetime.now(UTC),
        scheduled_start=scheduled_start,
        collection_duration_seconds=candidate.prospective_shard_policy.collection_duration_seconds,
        cadence_seconds=candidate.prospective_shard_policy.cadence_seconds,
    )
    selected_contract = polymarket if venue is Venue.POLYMARKET else kalshi
    base_collection_id = candidate.collection_plans[venue].collection_id(campaign_id)
    collection_id = candidate.prospective_shard_policy.collection_id(
        base_collection_id=base_collection_id,
        campaign_manifest_sha256=declared_hash,
        venue=venue,
        ordinal=shard_ordinal,
        scheduled_start=scheduled_start,
    )
    return (
        declared_hash,
        selected_contract.contract_sha256,
        candidate.config_sha256,
        candidate.collection_plans[venue],
        collection_id,
        _datetime_utc_ns(
            scheduled_start
            + timedelta(seconds=candidate.prospective_shard_policy.cadence_seconds)
        ),
    )


@research_data_app.command("prediction-contract-check")
def prediction_contract_check(
    polymarket_contract: Annotated[
        Path,
        typer.Option("--polymarket-contract", help="Contrat officiel Polymarket versionné."),
    ] = Path("config/research/polymarket-public-contract-v1.json"),
    kalshi_contract: Annotated[
        Path,
        typer.Option("--kalshi-contract", help="Contrat officiel Kalshi versionné."),
    ] = Path("config/research/kalshi-public-contract-v1.json"),
    candidate_config: Annotated[
        Path,
        typer.Option("--candidate-config", help="Préinscription candidate scellée."),
    ] = Path("config/research/prediction-markets-candidate-v1.json"),
) -> None:
    """Valide offline les contrats documentaires; ne sonde aucun endpoint."""

    polymarket, kalshi, candidate = _prediction_contracts(
        polymarket_contract.absolute(),
        kalshi_contract.absolute(),
        candidate_config.absolute(),
    )
    typer.echo(
        json.dumps(
            {
                "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
                "candidate_config_sha256": candidate.config_sha256,
                "economic_evidence_status": candidate.economic_status,
                "kalshi_contract_sha256": kalshi.contract_sha256,
                "network_executed": False,
                "polymarket_contract_sha256": polymarket.contract_sha256,
                "status": candidate.status,
            },
            sort_keys=True,
        )
    )


@research_data_app.command("prediction-census")
def prediction_census(
    venue: Annotated[Venue, typer.Option("--venue", help="polymarket ou kalshi.")],
    limit: Annotated[int, typer.Option("--limit", min=1, max=100)] = 20,
    cursor: Annotated[
        str | None,
        typer.Option("--cursor", help="Curseur opaque explicite; aucun suivi caché."),
    ] = None,
) -> None:
    """Affiche le plan de census officiel; aucune requête réseau n'est exécutée."""

    if venue is Venue.POLYMARKET:
        request = PolymarketPublicAdapter().market_census_request(limit=limit, after_cursor=cursor)
    elif venue is Venue.KALSHI:
        request = KalshiPublicAdapter().market_census_request(limit=limit, cursor=cursor)
    else:
        raise typer.BadParameter("--venue doit être polymarket ou kalshi")
    typer.echo(
        json.dumps(
            {
                "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
                "method": request.method,
                "network_executed": False,
                "query": dict(request.query),
                "status": "CENSUS_PLAN_ONLY",
                "url": request.url,
            },
            sort_keys=True,
        )
    )


@research_data_app.command("prediction-prepare")
def prediction_prepare(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Nouveau répertoire de campagne prospective."),
    ],
    campaign_id: Annotated[
        str,
        typer.Option("--campaign-id", help="Identité unique choisie avant collecte."),
    ],
    starts_at_utc: Annotated[
        str,
        typer.Option("--starts-at-utc", help="Début prospectif UTC ISO-8601."),
    ],
    polymarket_contract: Annotated[
        Path,
        typer.Option("--polymarket-contract"),
    ] = Path("config/research/polymarket-public-contract-v1.json"),
    kalshi_contract: Annotated[
        Path,
        typer.Option("--kalshi-contract"),
    ] = Path("config/research/kalshi-public-contract-v1.json"),
    candidate_config: Annotated[
        Path,
        typer.Option("--candidate-config"),
    ] = Path("config/research/prediction-markets-candidate-v1.json"),
) -> None:
    """Prépare un pack local AWAITING_HUMAN_EXECUTION sans lancer de collecte."""

    polymarket, kalshi, candidate = _prediction_contracts(
        polymarket_contract.absolute(),
        kalshi_contract.absolute(),
        candidate_config.absolute(),
    )
    starts = _utc_option(starts_at_utc, label="--starts-at-utc")
    try:
        result = prepare_prediction_campaign(
            output_root=output_root.absolute(),
            campaign_id=campaign_id,
            starts_at_utc=starts.isoformat().replace("+00:00", "Z"),
            preregistration=candidate,
            contracts=(polymarket, kalshi),
        )
    except (FileExistsError, OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(json.dumps(result, ensure_ascii=False, sort_keys=True))


@research_data_app.command("prediction-collect")
def prediction_collect(
    output_root: Annotated[Path, typer.Option("--output-root")],
    venue: Annotated[Venue, typer.Option("--venue")],
    campaign_manifest: Annotated[Path, typer.Option("--campaign-manifest")],
    feeds: Annotated[str, typer.Option("--feeds")],
    shard_ordinal: Annotated[int, typer.Option("--shard-ordinal", min=0)],
    instruments: Annotated[str, typer.Option("--instruments")] = "",
    census_limit: Annotated[int, typer.Option("--census-limit", min=0, max=100)] = 0,
    duration_seconds: Annotated[int, typer.Option("--duration-seconds", min=120, max=300)] = 120,
    max_network_calls: Annotated[int, typer.Option("--max-network-calls", min=1, max=1000)] = 40,
    max_frames: Annotated[int, typer.Option("--max-frames", min=1, max=50000)] = 250,
    max_bytes: Annotated[int, typer.Option("--max-bytes", min=4096)] = 8 * 1024 * 1024,
    polymarket_contract: Annotated[Path, typer.Option("--polymarket-contract")] = Path(
        "config/research/polymarket-public-contract-v1.json"
    ),
    kalshi_contract: Annotated[Path, typer.Option("--kalshi-contract")] = Path(
        "config/research/kalshi-public-contract-v1.json"
    ),
    candidate_config: Annotated[Path, typer.Option("--candidate-config")] = Path(
        "config/research/prediction-markets-candidate-v1.json"
    ),
) -> None:
    """Collecte publique directe et bornée; aucun ordre, compte ou credential."""

    if venue not in {Venue.POLYMARKET, Venue.KALSHI}:
        raise typer.BadParameter("--venue doit être polymarket ou kalshi")
    polymarket, kalshi, candidate = _prediction_contracts(
        polymarket_contract.absolute(),
        kalshi_contract.absolute(),
        candidate_config.absolute(),
    )
    (
        campaign_hash,
        contract_hash,
        candidate_hash,
        collection_plan,
        collection_id,
        collection_cutoff_utc_ns_exclusive,
    ) = (
        _campaign_binding(
        campaign_manifest=campaign_manifest.absolute(),
        venue=venue,
        polymarket=polymarket,
        kalshi=kalshi,
        candidate=candidate,
        shard_ordinal=shard_ordinal,
        )
    )
    selected_feeds = _csv(feeds, label="feeds")
    if selected_feeds != collection_plan.feeds:
        raise typer.BadParameter("prediction campaign feeds diverge from the preregistered plan")
    selected_instruments = () if not instruments.strip() else _csv(instruments, label="instruments")
    if selected_instruments or census_limit != collection_plan.census_limit:
        raise typer.BadParameter("prediction campaign instrument census diverges from the plan")
    supplied_bounds = (
        duration_seconds,
        max_network_calls,
        max_frames,
        max_bytes,
    )
    planned_bounds = (
        collection_plan.duration_seconds,
        collection_plan.max_network_calls,
        collection_plan.max_frames,
        collection_plan.max_bytes,
    )
    if supplied_bounds != planned_bounds:
        raise typer.BadParameter("prediction campaign budgets diverge from the preregistered plan")
    config = ProbeConfig(
        output_root=output_root.absolute(),
        venue=venue,
        feeds=selected_feeds,
        instruments=selected_instruments,
        census_limit=census_limit,
        duration_seconds=duration_seconds,
        max_bytes=max_bytes,
        max_segment_bytes=collection_plan.max_segment_bytes,
        rotation_seconds=collection_plan.rotation_seconds,
        progress_interval_seconds=collection_plan.progress_interval_seconds,
        collection_id=collection_id,
        max_frames=max_frames,
        max_segments=collection_plan.max_segments,
        max_network_calls=max_network_calls,
        campaign_manifest_sha256=campaign_hash,
        official_contract_sha256=contract_hash,
        candidate_config_sha256=candidate_hash,
        collection_cutoff_utc_ns_exclusive=collection_cutoff_utc_ns_exclusive,
    )
    preflight = {
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "completion_signal": "reports/result.json + terminal_health",
        "ctrl_c": "clôture les frames admises; aucun ordre ni mutation externe",
        "execution_location": f"Windows PowerShell local:{Path.cwd().absolute()}",
        "expected_duration_seconds": duration_seconds,
        "max_duration_seconds": duration_seconds + 15,
        "monitoring": str(output_root.absolute() / "reports" / "health.json"),
        "network_call_cap": max_network_calls,
        "prompt_behavior": "NO_PROMPT",
        "proxy_policy": "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED",
    }
    typer.echo(json.dumps(preflight, ensure_ascii=False, sort_keys=True))
    try:
        report = run_public_probe(config)
    except (FileExistsError, OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    _raise_for_probe_terminal_health(report.terminal_health)


@research_data_app.command("prediction-recover")
def prediction_recover(
    output_root: Annotated[Path, typer.Option("--output-root")],
    venue: Annotated[Venue, typer.Option("--venue")],
    requested_duration_seconds: Annotated[int, typer.Option("--requested-duration-seconds", min=1, max=300)],
    terminal_health: Annotated[str, typer.Option("--terminal-health")],
    error: Annotated[str, typer.Option("--error")],
) -> None:
    """Authentifie exclusivement les segments déjà publiés après interruption."""

    if venue not in {Venue.POLYMARKET, Venue.KALSHI}:
        raise typer.BadParameter("--venue doit être polymarket ou kalshi")
    try:
        report = recover_public_probe_output(
            output_root.absolute(),
            venue=venue,
            requested_duration_seconds=requested_duration_seconds,
            terminal_health=terminal_health,
            error=error,
        )
    except (LookupError, OSError, ValueError) as caught:
        raise typer.BadParameter(str(caught)) from caught
    typer.echo(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))


def _canonical_campaign(path: Path) -> Mapping[str, object]:
    try:
        raw = path.read_bytes()
        if raw.endswith(b"\n"):
            raw = raw[:-1]
        decoded = decode_canonical_json(raw, require_canonical=True)
    except (OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    if not isinstance(decoded, dict):
        raise typer.BadParameter("prediction campaign manifest must be a canonical object")
    return decoded


@research_data_app.command("prediction-bundle-build")
def prediction_bundle_build(
    output_root: Annotated[Path, typer.Option("--output-root")],
    campaign_manifest: Annotated[Path, typer.Option("--campaign-manifest")],
    collection_roots: Annotated[
        str,
        typer.Option(
            "--collection-roots",
            help="Racines de probes publics terminalisés, séparées par des virgules.",
        ),
    ] = "",
    unavailable_roots: Annotated[
        str,
        typer.Option(
            "--unavailable-roots",
            help=(
                "Racines exactes des reçus terminaux exclus, zéro-frame ou raw positif "
                "fail-closed, séparées par des virgules."
            ),
        ),
    ] = "",
    polymarket_contract: Annotated[Path, typer.Option("--polymarket-contract")] = Path(
        "config/research/polymarket-public-contract-v1.json"
    ),
    kalshi_contract: Annotated[Path, typer.Option("--kalshi-contract")] = Path(
        "config/research/kalshi-public-contract-v1.json"
    ),
    candidate_config: Annotated[Path, typer.Option("--candidate-config")] = Path(
        "config/research/prediction-markets-candidate-v1.json"
    ),
) -> None:
    """Construit un bundle immuable uniquement depuis les probes publics authentifiés."""

    polymarket, kalshi, candidate = _prediction_contracts(
        polymarket_contract.absolute(),
        kalshi_contract.absolute(),
        candidate_config.absolute(),
    )
    roots = () if not collection_roots.strip() else _csv(
        collection_roots,
        label="collection roots",
    )
    unavailable = () if not unavailable_roots.strip() else _csv(
        unavailable_roots,
        label="unavailable roots",
    )
    preflight = {
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "completion_signal": "bundle-manifest.json vérifié par reconstruction raw",
        "ctrl_c": "interrompt la copie locale; aucun transport ni ordre externe",
        "execution_location": f"Windows PowerShell local:{Path.cwd().absolute()}",
        "max_duration_seconds": 3600,
        "network_executed": False,
        "output_root": str(output_root.absolute()),
        "prompt_behavior": "NO_PROMPT",
    }
    typer.echo(json.dumps(preflight, ensure_ascii=False, sort_keys=True))
    try:
        verified = build_prediction_research_bundle(
            output_root=output_root.absolute(),
            sources=tuple(
                PredictionBundleSource.from_probe_output(Path(item).absolute())
                for item in roots
            ),
            preregistration=candidate,
            campaign_manifest=_canonical_campaign(campaign_manifest.absolute()),
            contracts={
                Venue.POLYMARKET: polymarket,
                Venue.KALSHI: kalshi,
            },
            unavailable_sources=tuple(
                PredictionUnavailableSource.from_probe_output(Path(item).absolute())
                for item in unavailable
            ),
        )
    except (FileExistsError, LookupError, OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(
        json.dumps(
            {
                "bundle_sha256": verified.bundle_sha256,
                "economic_claim": "NONE_RESEARCH_MECHANISM_ONLY",
                "prospective_slot_coverage": verified.prospective_slot_coverage,
                "source_status_by_venue": {
                    venue.value: status
                    for venue, status in verified.source_status_by_venue.items()
                },
                "status": "VERIFIED_RAW_REBUILD_COMPLETE",
            },
            sort_keys=True,
        )
    )


@research_data_app.command("prediction-bundle-verify")
def prediction_bundle_verify(
    bundle_root: Annotated[Path, typer.Option("--bundle-root")],
    expected_bundle_sha256: Annotated[
        str,
        typer.Option("--expected-bundle-sha256"),
    ],
) -> None:
    """Reconstruit offline chaque dérivé et refuse toute substitution."""

    try:
        verified = verify_prediction_research_bundle(
            bundle_root.absolute(),
            expected_bundle_sha256=expected_bundle_sha256,
        )
    except (LookupError, OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(
        json.dumps(
            {
                "bundle_sha256": verified.bundle_sha256,
                "network_executed": False,
                "prospective_slot_coverage": verified.prospective_slot_coverage,
                "status": "VERIFIED_RAW_REBUILD_COMPLETE",
            },
            sort_keys=True,
        )
    )


__all__ = ["research_data_app"]
