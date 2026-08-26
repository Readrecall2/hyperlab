from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from hyperlab.cli import app
from hyperlab.ghost.replay import GhostFixture, GhostReplay, replay_research_manifest
from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    SessionEnvelopeFactory,
    Venue,
)
from hyperlab.research_data.segments import ResearchSegmentWriter

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "ghost" / "base_realism_v1.json"


def test_fixture_replay_is_byte_identical_and_reconciles_pnl_and_exposure() -> None:
    fixture = GhostFixture.from_bytes(FIXTURE.read_bytes())
    first = GhostReplay(fixture).run()
    second = GhostReplay(fixture).run()

    assert first.canonical_bytes() == second.canonical_bytes()
    assert first.report_sha256 == second.report_sha256
    assert first.boundary == "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
    assert first.fixture_label == SYNTHETIC_FIXTURE_LABEL
    assert first.provenance.adapter_id == "canonical-direct-fixture-v1"
    assert first.pnl.reconciliation_difference == 0
    assert first.exposure.reconciliation_difference == 0
    assert first.pnl.inventory == Decimal("-203")
    assert first.pnl.hedge == Decimal("206")
    assert first.pnl.fees == Decimal("-0.2045")
    assert first.pnl.opportunity_cost < 0
    assert first.pnl.reward == first.pnl.rebate == 0
    assert first.orders[0].filled_quantity == Decimal("2")
    assert first.orders[0].level_count == 2
    assert first.orders[1].depends_on_filled_quantity == Decimal("2")
    assert first.groups[0].status == "COMPLETE"
    assert first.economic_claim == "NONE_RESEARCH_MECHANISM_ONLY"


def test_research_manifest_replay_authenticates_existing_segment_contract(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    payload = FIXTURE.read_bytes()
    factory = SessionEnvelopeFactory(
        venue=Venue.HYPERLIQUID,
        collector_identity="ghost-fixture-collector-v1",
        session_identity="ghost-fixture-session",
        source_metadata_version="ghost-fixture-schema-v1",
        provenance=CaptureProvenance(
            collection_id="ghost-fixture-collection",
            source_url="fixture://ghost/base-realism-v1",
            transport="FIXTURE",
            fixture_label=SYNTHETIC_FIXTURE_LABEL,
        ),
    )
    envelope = factory.make(
        feed_type="ghost_fixture",
        instrument_id="HL:BTC:perp",
        market_id=None,
        source_timestamp_ns=90,
        receive_timestamp_utc_ns=100,
        receive_monotonic_ns=100,
        raw_payload=payload,
        source_event_id="base-realism-fixture-v1",
    )
    writer = ResearchSegmentWriter(
        root,
        collection_id="ghost-fixture-collection",
        max_segment_bytes=1_000_000,
        rotation_seconds=60,
        max_total_bytes=2_000_000,
    )
    writer.append(envelope)
    manifest = writer.close()
    assert manifest is not None

    report = replay_research_manifest(root, manifest.manifest_sha256)
    assert report.provenance.raw_manifest_sha256 == manifest.manifest_sha256
    assert report.provenance.raw_root_sha256 == manifest.root_sha256
    assert report.provenance.adapter_id == "canonical-ghost-fixture-envelope-v1"
    assert report.provenance.segment_sha256s == tuple(
        item.physical_sha256 for item in manifest.segments
    )
    assert report.pnl.reconciliation_difference == 0


def test_cli_is_ghost_only_and_writes_canonical_deterministic_report(tmp_path: Path) -> None:
    output = tmp_path / "report.json"
    result = CliRunner().invoke(
        app,
        ["ghost", "replay", "--fixture", str(FIXTURE), "--output", str(output)],
    )
    assert result.exit_code == 0, result.output
    assert "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY" in result.output
    assert "NO_PROMPT" in result.output
    assert "aucun ordre externe" in result.output
    raw = output.read_bytes()
    assert raw.endswith(b"\n")
    report = GhostReplay(GhostFixture.from_bytes(FIXTURE.read_bytes())).run()
    assert canonical_json_bytes(report.to_dict()) + b"\n" == raw
