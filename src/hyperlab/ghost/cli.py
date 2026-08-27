from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.prediction_bundle import (
    evaluate_verified_prediction_bundle,
    replay_verified_prediction_bundle,
    verify_prediction_campaign_replay_artifact,
    verify_prediction_research_bundle,
)

from .h1 import H1PolicyConfig, replay_h1_research_manifest
from .models import BOUNDARY
from .prediction import replay_prediction_fixture
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
        raise typer.BadParameter("choisir exactement --fixture ou --research-root avec --manifest-sha256")
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


def _write_immutable_report(output: Path, body: bytes, *, label: str) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and output.read_bytes() != body:
        raise typer.BadParameter(f"refus d'écraser un rapport {label} différent")
    output.write_bytes(body)


@ghost_app.command("prediction-replay")
def prediction_replay(
    fixture: Annotated[
        Path | None,
        typer.Option(
            "--fixture",
            help="Fixture locale canonique explicitement SYNTHETIC/FIXTURE.",
        ),
    ] = None,
    bundle_root: Annotated[
        Path | None,
        typer.Option(
            "--bundle-root",
            help="Bundle Research authentifié reconstruit depuis le raw.",
        ),
    ] = None,
    expected_bundle_sha256: Annotated[
        str | None,
        typer.Option(
            "--expected-bundle-sha256",
            help="Pin SHA-256 obligatoire pour tout bundle Research.",
        ),
    ] = None,
    output: Annotated[
        Path,
        typer.Option("--output", help="Rapport Ghost prédictif canonique."),
    ] = Path("reports/ghost/prediction-markets-ghost-v1.json"),
) -> None:
    """Replay déterministe prédictif offline, sans transport ni ordre."""

    if (fixture is None) == (bundle_root is None):
        raise typer.BadParameter("choisir exactement --fixture ou --bundle-root")
    if bundle_root is not None and expected_bundle_sha256 is None:
        raise typer.BadParameter("--expected-bundle-sha256 est obligatoire avec --bundle-root")
    if fixture is not None and expected_bundle_sha256 is not None:
        raise typer.BadParameter("le pin bundle ne s'applique pas à une fixture")

    preflight = {
        "boundary": BOUNDARY,
        "completion_signal": f"rapport prediction canonique écrit:{output.absolute()}",
        "ctrl_c": "interrompt le calcul local; aucune mutation Research ou externe",
        "execution_location": f"Windows PowerShell local:{Path.cwd().absolute()}",
        "expected_duration_seconds": 1,
        "max_duration_seconds": 30,
        "monitoring": "sortie terminal locale et rapport final",
        "prompt_behavior": "NO_PROMPT",
    }
    typer.echo(json.dumps(preflight, ensure_ascii=False, sort_keys=True))
    try:
        if fixture is not None:
            fixture_report = replay_prediction_fixture(fixture.read_bytes())
            report_bytes = canonical_json_bytes(fixture_report.to_dict()) + b"\n"
            report_sha256 = fixture_report.report_sha256
            status = fixture_report.status
        else:
            assert bundle_root is not None
            assert expected_bundle_sha256 is not None
            campaign_report = replay_verified_prediction_bundle(
                verify_prediction_research_bundle(
                    bundle_root.absolute(),
                    expected_bundle_sha256=expected_bundle_sha256,
                )
            )
            report_bytes = canonical_json_bytes(campaign_report.to_dict()) + b"\n"
            report_sha256 = campaign_report.report_sha256
            status = "COMPLETE_RAW_REBUILT"
    except (OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    _write_immutable_report(
        output,
        report_bytes,
        label="prediction",
    )
    typer.echo(
        json.dumps(
            {
                "economic_claim": "NONE_RESEARCH_MECHANISM_ONLY",
                "report_sha256": report_sha256,
                "status": status,
            },
            sort_keys=True,
        )
    )


@ghost_app.command("prediction-evaluate")
def prediction_evaluate(
    bundle_root: Annotated[
        Path,
        typer.Option("--bundle-root", help="Bundle Research authentifié à reconstruire."),
    ],
    campaign_replay: Annotated[
        Path,
        typer.Option(
            "--campaign-replay",
            help="Rapport replay canonique devant être byte-identical au rebuild raw.",
        ),
    ],
    expected_bundle_sha256: Annotated[
        str,
        typer.Option("--expected-bundle-sha256"),
    ],
    output: Annotated[
        Path,
        typer.Option("--output", help="Rapport d'évaluation canonique."),
    ] = Path("reports/ghost/prediction-markets-evaluation-v1.json"),
) -> None:
    """Reconstruit puis évalue OOS; le statut source vient du bundle authentifié."""

    try:
        verified = verify_prediction_research_bundle(
            bundle_root.absolute(),
            expected_bundle_sha256=expected_bundle_sha256,
        )
        rebuilt_replay = verify_prediction_campaign_replay_artifact(
            verified,
            campaign_replay.read_bytes(),
        )
        report = evaluate_verified_prediction_bundle(
            verified,
            rebuilt_replay,
        )
    except (OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    _write_immutable_report(
        output,
        canonical_json_bytes(report) + b"\n",
        label="prediction evaluation",
    )
    typer.echo(json.dumps(report, ensure_ascii=False, sort_keys=True))


__all__ = ["ghost_app"]
