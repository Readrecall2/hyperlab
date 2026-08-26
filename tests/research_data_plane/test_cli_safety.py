from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

import hyperlab.research_data.cli as research_cli
from hyperlab.cli import app
from hyperlab.research_data.probe import ProbeReport


def test_research_data_cli_requires_explicit_bounded_operator_contract() -> None:
    runner = CliRunner()
    help_result = runner.invoke(app, ["research-data", "probe", "--help"])
    assert help_result.exit_code == 0
    for option in (
        "--output-root",
        "--venue",
        "--feeds",
        "--instruments",
        "--census-limit",
        "--duration-seconds",
        "--max-bytes",
        "--segment-bytes",
        "--rotation-seconds",
        "--progress-interval",
    ):
        assert option in help_result.output

    missing_scope = runner.invoke(
        app,
        [
            "research-data",
            "probe",
            "--output-root",
            "fixture-output",
            "--venue",
            "hyperliquid",
            "--feeds",
            "bbo",
            "--duration-seconds",
            "120",
        ],
    )
    assert missing_scope.exit_code != 0
    assert "instruments explicites ou --census-limit" in missing_scope.output


def test_research_data_cli_announces_safety_and_emits_terminal_json(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "probe"

    def fake_probe(config, **_kwargs):
        return ProbeReport(
            schema_version=1,
            boundary="PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
            venue=config.venue.value,
            terminal_health="COMPLETE",
            collection_id="fixture-collection",
            requested_duration_seconds=config.duration_seconds,
            elapsed_ms=1,
            frames=1,
            segments=1,
            bytes=100,
            gaps=0,
            duplicates=0,
            reconnects=0,
            queue_high_water=1,
            source_timestamp_min_ns=1,
            source_timestamp_max_ns=1,
            manifest_sha256="a" * 64,
            root_sha256="b" * 64,
            limitations=(),
            error=None,
        )

    monkeypatch.setattr(research_cli, "run_public_probe", fake_probe)
    result = CliRunner().invoke(
        app,
        [
            "research-data",
            "probe",
            "--output-root",
            str(output),
            "--venue",
            "hyperliquid",
            "--feeds",
            "bbo,l2_book,trades",
            "--instruments",
            "BTC",
            "--duration-seconds",
            "120",
            "--max-bytes",
            "1048576",
            "--segment-bytes",
            "65536",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Windows PowerShell local" in result.output
    assert '"prompt_behavior": "NO_PROMPT"' in result.output
    assert '"ctrl_c"' in result.output
    assert '"completion_signal"' in result.output
    assert '"terminal_health": "COMPLETE"' in result.output


def test_research_data_cli_uses_documented_public_unavailable_exit_code(
    tmp_path: Path, monkeypatch
) -> None:
    def fake_probe(config, **_kwargs):
        return ProbeReport(
            schema_version=1,
            boundary="PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
            venue=config.venue.value,
            terminal_health="PUBLIC_SOURCE_UNAVAILABLE",
            collection_id="fixture-unavailable",
            requested_duration_seconds=config.duration_seconds,
            elapsed_ms=1,
            frames=0,
            segments=0,
            bytes=0,
            gaps=0,
            duplicates=0,
            reconnects=0,
            queue_high_water=0,
            source_timestamp_min_ns=None,
            source_timestamp_max_ns=None,
            manifest_sha256=None,
            root_sha256=None,
            limitations=(),
            error="ConnectionError:SYNTHETIC/FIXTURE source unavailable",
        )

    monkeypatch.setattr(research_cli, "run_public_probe", fake_probe)
    result = CliRunner().invoke(
        app,
        [
            "research-data",
            "probe",
            "--output-root",
            str(tmp_path / "unavailable"),
            "--venue",
            "polymarket",
            "--feeds",
            "metadata",
            "--census-limit",
            "1",
            "--duration-seconds",
            "120",
        ],
    )
    assert result.exit_code == 3, result.output
    assert '"terminal_health": "PUBLIC_SOURCE_UNAVAILABLE"' in result.output
