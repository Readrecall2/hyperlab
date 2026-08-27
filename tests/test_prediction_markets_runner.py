from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hyperlab.research_data.envelope import Venue
from ops.prediction_markets_launch_v1 import runner

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_PACK = (
    ROOT
    / "ops"
    / "prediction_markets_candidate_v1"
    / "prediction-markets-v1-20260901t000000z-aa60c0ff"
)


def _campaign(tmp_path: Path) -> runner.CampaignContext:
    campaign = tmp_path / "campaign"
    campaign.mkdir()
    for name in ("campaign-manifest.json", "campaign-manifest.sha256"):
        (campaign / name).write_bytes((CANDIDATE_PACK / name).read_bytes())
    return runner.load_campaign_context(campaign, ROOT)


def test_campaign_context_reuses_authenticated_candidate_and_672_slot_schedule(tmp_path: Path) -> None:
    context = _campaign(tmp_path)
    assert context.manifest["candidate_config_sha256"] == (
        "aa60c0ff0ef95813d79f56b6ea93a31952061b562905dc9729162f7b16e41964"
    )
    assert context.expected_slots == 672
    assert context.cadence_seconds == 3600
    assert context.duration_seconds == 120
    assert set(context.contracts) == {Venue.POLYMARKET, Venue.KALSHI}


def test_persistent_runner_refuses_a_handoff_changed_after_preflight(tmp_path: Path) -> None:
    path = tmp_path / "handoff.json"
    payload = runner.canonical_json_bytes({"boundary": runner.BOUNDARY}) + b"\n"
    path.write_bytes(payload)
    path.with_suffix(".sha256").write_text(
        f"{runner.sha256_bytes(payload)}  handoff.json\n",
        encoding="ascii",
    )
    assert runner._pinned_object(path) == {"boundary": runner.BOUNDARY}
    path.write_bytes(payload + b" ")
    with pytest.raises(runner.RunnerError, match="physical SHA-256 diverged"):
        runner._pinned_object(path)


def test_schedule_waits_runs_marks_gaps_without_backfill_and_completes(tmp_path: Path) -> None:
    context = _campaign(tmp_path)
    before = runner.schedule_decision(context, [], now=context.start - timedelta(seconds=5))
    assert before.action == "WAIT_FOR_START"
    assert before.wait_seconds == 5
    current = runner.schedule_decision(context, [], now=context.start + timedelta(seconds=1))
    assert current.action == "RUN_SLOT" and current.ordinal == 0
    late = runner.schedule_decision(
        context,
        [],
        now=context.start + timedelta(hours=2, minutes=59),
    )
    assert late.action == "MISSED_CURRENT_SLOT"
    assert late.missing_ordinals == (0, 1, 2)
    complete = runner.schedule_decision(context, [], now=context.end + timedelta(seconds=1))
    assert complete.action == "COMPLETE_WINDOW"
    assert len(complete.missing_ordinals) == 672


def test_terminal_ledger_is_append_only_hash_chained_and_rejects_tamper(tmp_path: Path) -> None:
    ledger = tmp_path / "polymarket" / "ledger.jsonl"
    context = _campaign(tmp_path)
    first = runner.append_ledger(
        ledger,
        runner._missed_entry(context, Venue.POLYMARKET, 0, context.start),
    )
    second = runner.append_ledger(
        ledger,
        runner._missed_entry(context, Venue.POLYMARKET, 1, context.start + timedelta(hours=1)),
    )
    assert second["previous_entry_sha256"] == first["entry_sha256"]
    assert [row["ordinal"] for row in runner.read_ledger(ledger)] == [0, 1]
    with pytest.raises(runner.RunnerError, match="already"):
        runner.append_ledger(
            ledger,
            runner._missed_entry(context, Venue.POLYMARKET, 1, context.start),
        )
    lines = ledger.read_text(encoding="utf-8").splitlines()
    value = json.loads(lines[0])
    value["terminal_health"] = "COMPLETE"
    lines[0] = json.dumps(value, separators=(",", ":"), sort_keys=True)
    ledger.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(runner.RunnerError, match="chain diverged"):
        runner.read_ledger(ledger)


def test_missing_slot_metrics_are_unknown_not_invented_zero(tmp_path: Path) -> None:
    context = _campaign(tmp_path)
    entry = runner._missed_entry(context, Venue.KALSHI, 8, datetime.now(UTC))
    for field in ("bytes", "duplicates", "frames", "gaps", "reconnects", "segments"):
        assert entry[field] is None
    assert entry["terminal_health"] == "MISSING_SLOT_NO_BACKFILL"


def test_capacity_reserves_full_h1_budget_remaining_prediction_and_margin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign = tmp_path / "campaign"
    campaign.mkdir()

    class Usage:
        total = 300_000_000_000
        used = 100_000_000_000
        free = 200_000_000_000

    monkeypatch.setattr(runner.shutil, "disk_usage", lambda _path: Usage())
    result = runner.capacity_snapshot(
        campaign_root=campaign,
        h1_reserved_bytes=144 * 1024**3,
        prediction_maximum_raw_bytes=21 * 1024**3,
        safety_margin_bytes=16 * 1024**3,
        accounted_bytes=0,
    )
    assert result["required_free_bytes"] == 181 * 1024**3
    assert result["admitted"] is True
    Usage.free = 180 * 1024**3
    refused = runner.capacity_snapshot(
        campaign_root=campaign,
        h1_reserved_bytes=144 * 1024**3,
        prediction_maximum_raw_bytes=21 * 1024**3,
        safety_margin_bytes=16 * 1024**3,
        accounted_bytes=0,
    )
    assert refused["admitted"] is False


def test_capacity_accounting_never_reads_or_depends_on_the_other_venue_ledger(
    tmp_path: Path,
) -> None:
    context = _campaign(tmp_path)
    other = context.campaign_root / "kalshi" / "ledger.jsonl"
    other.parent.mkdir()
    other.write_text("tampered-other-venue-ledger\n", encoding="utf-8")
    service = runner.VenueRunner(
        context=context,
        source_root=ROOT,
        python=Path("/source/.venv/bin/python"),
        venue=Venue.POLYMARKET,
        h1_reserved_bytes=144 * 1024**3,
        prediction_maximum_raw_bytes=21 * 1024**3,
        safety_margin_bytes=16 * 1024**3,
    )
    assert service._accounted_bytes() == 0


def test_terminal_integrity_failure_is_published_only_to_its_venue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _campaign(tmp_path)
    service = runner.VenueRunner(
        context=context,
        source_root=ROOT,
        python=Path("/source/.venv/bin/python"),
        venue=Venue.POLYMARKET,
        h1_reserved_bytes=144 * 1024**3,
        prediction_maximum_raw_bytes=21 * 1024**3,
        safety_margin_bytes=16 * 1024**3,
    )

    def fail_owned() -> int:
        raise runner.RunnerError("synthetic authenticated failure")

    monkeypatch.setattr(service, "_run_owned", fail_owned)
    with pytest.raises(runner.RunnerError, match="synthetic authenticated failure"):
        service.run()
    state = json.loads(service.state_path.read_text(encoding="utf-8"))
    assert state["lifecycle"] == "INTEGRITY_FAILED"
    assert state["recorded_slots"] is None
    assert state["capacity"] is None
    assert not (context.campaign_root / "kalshi" / "state.json").exists()


def test_probe_commands_bind_exact_venue_plan_and_unique_output_root(tmp_path: Path) -> None:
    context = _campaign(tmp_path)
    output = context.campaign_root / "polymarket" / "runs" / "shard-0000"
    command = runner._probe_command(
        python=Path("/source/.venv/bin/python"),
        source_root=ROOT,
        context=context,
        venue=Venue.POLYMARKET,
        ordinal=0,
        output_root=output,
    )
    assert command.count("--venue") == 1
    assert command[command.index("--venue") + 1] == "polymarket"
    assert command[command.index("--shard-ordinal") + 1] == "0"
    assert command[command.index("--duration-seconds") + 1] == "120"
    assert command[command.index("--max-bytes") + 1] == str(16 * 1024**2)
    assert command[-1] == str(output)
    assert not output.exists()


def test_recovery_never_reissues_prediction_collect_for_existing_shard() -> None:
    source = (ROOT / "ops" / "prediction_markets_launch_v1" / "runner.py").read_text(
        encoding="utf-8"
    )
    existing_branch = source.split("if output_root.exists():", maxsplit=1)[1].split(
        "else:", maxsplit=1
    )[0]
    assert "_recover_command" in existing_branch
    assert "_probe_command" not in existing_branch
    assert "PROCESS_ERROR_NO_TERMINAL_RECEIPT" in source
    assert "NO_RETRY" not in source or "retry" not in existing_branch.lower()
