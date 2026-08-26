from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

from .envelope import Venue
from .h1_campaign import collect_h1_campaign, prepare_h1_campaign
from .lighter_report import LIGHTER_GREEN, write_lighter_probe_report
from .probe import ProbeConfig, run_public_probe

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
            fee_reviewed_at_utc=_utc_option(
                fee_reviewed_at_utc, label="--fee-reviewed-at-utc"
            ),
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
        typer.Option(
            "--segment-bytes", min=1024, help="Rotation déterministe par taille logique."
        ),
    ] = 4 * 1024 * 1024,
    rotation_seconds: Annotated[
        float,
        typer.Option(
            "--rotation-seconds", min=1.0, help="Rotation déterministe par durée monotone."
        ),
    ] = 30.0,
    progress_interval: Annotated[
        float,
        typer.Option(
            "--progress-interval", min=1.0, help="Intervalle d'observabilité console."
        ),
    ] = 10.0,
    max_frames: Annotated[
        int,
        typer.Option(
            "--max-frames", min=1, max=50_000, help="Borne stricte de frames admises."
        ),
    ] = 5_000,
    max_segments: Annotated[
        int,
        typer.Option(
            "--max-segments", min=1, max=100, help="Borne stricte de segments publiés."
        ),
    ] = 4,
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
        "max_segments": max_segments,
        "monitoring": str(resolved_output / "reports" / "health.json"),
        "output_root": str(resolved_output),
        "prompt_behavior": "NO_PROMPT",
        "proxy_policy": (
            "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED"
            if venue is Venue.LIGHTER
            else "VENUE_DEFAULT"
        ),
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
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error
    try:
        report = run_public_probe(config, progress=_progress)
    except FileExistsError as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    if report.terminal_health == "PUBLIC_SOURCE_UNAVAILABLE":
        raise typer.Exit(3)
    if report.terminal_health == "PUBLIC_SOURCE_INVALID":
        raise typer.Exit(4)
    if report.terminal_health == "BACKPRESSURE_LIMIT_REACHED":
        raise typer.Exit(5)
    if report.terminal_health == "INTERRUPTED_RECOVERABLE":
        raise typer.Exit(130)


__all__ = ["research_data_app"]
