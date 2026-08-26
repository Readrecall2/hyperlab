from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from .envelope import Venue
from .probe import ProbeConfig, run_public_probe

research_data_app = typer.Typer(
    name="research-data",
    help="Collecte locale bornée PUBLIC_DATA_ONLY et outils offline du Research Data Plane V1.",
    no_args_is_help=True,
)


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
            max=300,
            help="Durée obligatoire d'un probe public: 120 à 300 secondes.",
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
        "max_duration_seconds": duration_seconds + 15,
        "max_bytes": max_bytes,
        "monitoring": str(resolved_output / "reports" / "health.json"),
        "output_root": str(resolved_output),
        "prompt_behavior": "NO_PROMPT",
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
