from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hyperlab.research_data.canonical import canonical_json_bytes

from .h1 import H1PolicyConfig, replay_h1_research_manifest
from .models import BOUNDARY
from .replay import GhostFixture, GhostReplay, replay_research_manifest

ghost_app = typer.Typer(
    name="ghost",
    help="Replay local venue-neutral strictement GHOST_ONLY, sans route d'ordre.",
    no_args_is_help=True,
)


@ghost_app.command("h1-replay")
def h1_replay(
    research_root: Annotated[
        Path,
        typer.Option("--research-root", help="Racine read-only de segments Research authentifiés."),
    ],
    manifest_sha256: Annotated[
        str,
        typer.Option("--manifest-sha256", help="Manifest Research explicite et authentifié."),
    ],
    config: Annotated[
        Path,
        typer.Option("--config", help="Politique H1 préenregistrée avant observation."),
    ] = Path("config/research/hyperliquid-h1-ghost-v1.json"),
    output: Annotated[
        Path,
        typer.Option("--output", help="Rapport H1 JSON canonique déterministe."),
    ] = Path("reports/ghost/hyperliquid-h1-ghost-v1.json"),
) -> None:
    """Replay H1 local sur données Research authentifiées, sans transport externe."""

    preflight = {
        "boundary": BOUNDARY,
        "completion_signal": f"rapport H1 canonique écrit:{output.absolute()}",
        "ctrl_c": "interrompt le calcul local; aucune mutation des segments Research",
        "execution_location": f"Windows PowerShell local:{Path.cwd().absolute()}",
        "expected_duration_seconds": 30,
        "max_duration_seconds": 3600,
        "monitoring": "sortie terminal locale et rapport final",
        "prompt_behavior": "NO_PROMPT",
    }
    typer.echo(json.dumps(preflight, ensure_ascii=False, sort_keys=True))
    try:
        report = replay_h1_research_manifest(
            research_root,
            manifest_sha256,
            config=H1PolicyConfig.from_path(config),
        )
    except (OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    body = report.canonical_bytes() + b"\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and output.read_bytes() != body:
        raise typer.BadParameter("refus d'écraser un rapport H1 différent")
    output.write_bytes(body)
    typer.echo(
        json.dumps(
            {
                "economic_status": report.economic_status,
                "report_sha256": report.report_sha256,
                "status": report.technical_verdict,
            },
            sort_keys=True,
        )
    )


@ghost_app.command("replay")
def replay(
    fixture: Annotated[
        Path | None,
        typer.Option("--fixture", help="Fixture canonique locale explicitement SYNTHETIC/FIXTURE."),
    ] = None,
    research_root: Annotated[
        Path | None,
        typer.Option("--research-root", help="Racine read-only des segments Research existants."),
    ] = None,
    manifest_sha256: Annotated[
        str | None,
        typer.Option("--manifest-sha256", help="Manifest Research explicite et authentifié."),
    ] = None,
    output: Annotated[
        Path,
        typer.Option("--output", help="Rapport JSON canonique déterministe."),
    ] = Path("reports/ghost/base-realism-ghost-v1.json"),
) -> None:
    direct = fixture is not None
    manifested = research_root is not None or manifest_sha256 is not None
    if direct == manifested:
        raise typer.BadParameter(
            "choisir exactement --fixture ou --research-root avec --manifest-sha256"
        )
    if manifested and (research_root is None or manifest_sha256 is None):
        raise typer.BadParameter("--research-root et --manifest-sha256 sont indissociables")
    preflight = {
        "boundary": BOUNDARY,
        "completion_signal": f"rapport canonique écrit:{output.absolute()}",
        "ctrl_c": "interrompt le calcul local; aucun ordre externe ni mutation Research",
        "execution_location": f"Windows PowerShell local:{Path.cwd().absolute()}",
        "expected_duration_seconds": 1,
        "max_duration_seconds": 30,
        "monitoring": "sortie terminal locale et rapport final",
        "prompt_behavior": "NO_PROMPT",
    }
    typer.echo(json.dumps(preflight, ensure_ascii=False, sort_keys=True))
    try:
        if fixture is not None:
            report = GhostReplay(GhostFixture.from_bytes(fixture.read_bytes())).run()
        else:
            assert research_root is not None and manifest_sha256 is not None
            report = replay_research_manifest(research_root, manifest_sha256)
    except (OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    body = canonical_json_bytes(report.to_dict()) + b"\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and output.read_bytes() != body:
        raise typer.BadParameter("refus d'écraser un rapport différent")
    output.write_bytes(body)
    typer.echo(
        json.dumps(
            {
                "boundary": report.boundary,
                "report_sha256": report.report_sha256,
                "status": "COMPLETE",
            },
            sort_keys=True,
        )
    )


__all__ = ["ghost_app"]
