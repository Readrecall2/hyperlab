from __future__ import annotations

import json
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

import hyperlab.research_data.cli as research_cli
import hyperlab.research_data.probe as probe_module
from hyperlab.cli import app
from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    Venue,
)
from hyperlab.research_data.prediction_candidate import (
    CandidatePreregistration,
    prepare_prediction_campaign,
)
from hyperlab.research_data.prediction_contracts import OfficialPublicContract
from hyperlab.research_data.segments import ResearchDataCapacityError
from ops.prediction_markets_launch_v1 import runner

ROOT = Path(__file__).resolve().parents[1]


def test_runner_startup_admission_refusal_exits_four_before_runner_or_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff_path = tmp_path / "handoff.json"
    handoff_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(
        runner,
        "_pinned_object",
        lambda _path: {
            "boundary": runner.BOUNDARY,
            "campaign_root": str(tmp_path / "campaign"),
        },
    )
    monkeypatch.setattr(
        runner,
        "runner_startup_admission",
        lambda *_args, **_kwargs: {
            "errors": ["NTP is not synchronized for runner startup"],
            "startup_admissible": False,
        },
    )

    def forbidden_runner(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("venue runner or collection child must not be created")

    monkeypatch.setattr(runner, "VenueRunner", forbidden_runner)
    with pytest.raises(runner.RunnerError, match="before slot selection"):
        runner.run_from_handoff(handoff_path, Venue.POLYMARKET)
    with pytest.raises(SystemExit) as stopped:
        runner.main(["--handoff", str(handoff_path), "--venue", "polymarket"])
    assert stopped.value.code == 4


def _campaign(tmp_path: Path) -> runner.CampaignContext:
    campaign = tmp_path / "campaign"
    candidate = CandidatePreregistration.from_path(
        ROOT / "config/research/prediction-markets-candidate-v1.json"
    )
    contracts = tuple(
        OfficialPublicContract.from_path(
            ROOT / f"config/research/{venue}-public-contract-v1.json"
        )
        for venue in ("polymarket", "kalshi")
    )
    prepare_prediction_campaign(
        output_root=campaign,
        campaign_id="synthetic-fixture-current-contract-campaign-v1",
        starts_at_utc="2026-09-01T00:00:00Z",
        preregistration=candidate,
        contracts=contracts,
    )
    return runner.load_campaign_context(campaign, ROOT)


def _current_synthetic_campaign(tmp_path: Path) -> runner.CampaignContext:
    candidate = CandidatePreregistration.from_path(
        ROOT / "config/research/prediction-markets-candidate-v1.json"
    )
    contracts = tuple(
        OfficialPublicContract.from_path(ROOT / f"config/research/{venue}-public-contract-v1.json")
        for venue in ("polymarket", "kalshi")
    )
    campaign = tmp_path / "synthetic-fixture-campaign"
    prepare_prediction_campaign(
        output_root=campaign,
        campaign_id="synthetic-fixture-receipt-auth-v1",
        starts_at_utc=(datetime.now(UTC) - timedelta(seconds=1))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        preregistration=candidate,
        contracts=contracts,
    )
    return runner.load_campaign_context(campaign, ROOT)


def _synthetic_capacity_transport(config, **kwargs):
    factory = kwargs["factory"]
    writer = kwargs["writer"]
    counters = kwargs["counters"]
    for index, feed in enumerate(config.feeds, start=1):
        payload = canonical_json_bytes(
            {
                "feed": feed,
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                "venue": config.venue.value,
            }
        )
        envelope = factory.make(
            feed_type=feed,
            instrument_id=f"SYNTHETIC-{config.venue.value}-INSTRUMENT",
            market_id=f"SYNTHETIC-{config.venue.value}-MARKET",
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            source_event_id=f"SYNTHETIC-FIXTURE-{feed}-{index}",
            raw_payload=payload,
            provenance=CaptureProvenance(
                factory.provenance.collection_id,
                f"fixture://prediction-markets/{config.venue.value.lower()}/{feed}",
                "FIXTURE",
                SYNTHETIC_FIXTURE_LABEL,
            ),
        )
        writer.append(envelope)
        counters.observe(envelope)
    raise ResearchDataCapacityError("SYNTHETIC/FIXTURE bounded raw-byte budget reached")


def _synthetic_public_source_invalid_transport(config, **kwargs):
    factory = kwargs["factory"]
    writer = kwargs["writer"]
    counters = kwargs["counters"]
    for index, feed in enumerate(config.feeds, start=1):
        payload = canonical_json_bytes(
            {
                "feed": feed,
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                "public_source_invalid_oracle": True,
                "venue": config.venue.value,
            }
        )
        envelope = factory.make(
            feed_type=feed,
            instrument_id=f"SYNTHETIC-{config.venue.value}-INSTRUMENT",
            market_id=f"SYNTHETIC-{config.venue.value}-MARKET",
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            source_event_id=f"SYNTHETIC-INVALID-{feed}-{index}",
            raw_payload=payload,
            provenance=CaptureProvenance(
                factory.provenance.collection_id,
                f"fixture://prediction-markets/{config.venue.value.lower()}/{feed}",
                "FIXTURE",
                SYNTHETIC_FIXTURE_LABEL,
            ),
        )
        writer.append(envelope)
        counters.observe(envelope)
    if config.venue is Venue.KALSHI:
        raise ValueError("source timestamp must be absent or a non-negative UTC epoch value")
    raise ValueError("Polymarket CLOB market-info identity graph diverged")


def _synthetic_late_public_source_unavailable_transport(config, **kwargs):
    factory = kwargs["factory"]
    writer = kwargs["writer"]
    counters = kwargs["counters"]
    for index, feed in enumerate(config.feeds, start=1):
        payload = canonical_json_bytes(
            {
                "feed": feed,
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                "late_public_source_unavailable": True,
                "venue": config.venue.value,
            }
        )
        envelope = factory.make(
            feed_type=feed,
            instrument_id=f"SYNTHETIC-{config.venue.value}-INSTRUMENT",
            market_id=f"SYNTHETIC-{config.venue.value}-MARKET",
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            source_event_id=f"SYNTHETIC-LATE-UNAVAILABLE-{feed}-{index}",
            raw_payload=payload,
            provenance=CaptureProvenance(
                factory.provenance.collection_id,
                f"fixture://prediction-markets/{config.venue.value.lower()}/{feed}",
                "FIXTURE",
                SYNTHETIC_FIXTURE_LABEL,
            ),
        )
        writer.append(envelope)
        counters.observe(envelope)
    raise ConnectionError("SYNTHETIC/FIXTURE late public source disconnect")


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
        context=context,
        venue=Venue.POLYMARKET,
    )
    second = runner.append_ledger(
        ledger,
        runner._missed_entry(context, Venue.POLYMARKET, 1, context.start + timedelta(hours=1)),
        context=context,
        venue=Venue.POLYMARKET,
    )
    assert second["previous_entry_sha256"] == first["entry_sha256"]
    assert [row["ordinal"] for row in runner.read_ledger(ledger)] == [0, 1]
    with pytest.raises(runner.RunnerError, match="already"):
        runner.append_ledger(
            ledger,
            runner._missed_entry(context, Venue.POLYMARKET, 1, context.start),
            context=context,
            venue=Venue.POLYMARKET,
        )
    lines = ledger.read_text(encoding="utf-8").splitlines()
    value = json.loads(lines[0])
    value["terminal_health"] = "COMPLETE"
    lines[0] = json.dumps(value, separators=(",", ":"), sort_keys=True)
    ledger.write_bytes(("\n".join(lines) + "\n").encode("utf-8"))
    with pytest.raises(runner.RunnerError, match="chain diverged"):
        runner.read_ledger(ledger)


def test_service_ledger_rejects_cross_venue_and_rechained_semantic_tamper(
    tmp_path: Path,
) -> None:
    context = _campaign(tmp_path)
    polymarket = tmp_path / "semantic" / "polymarket.jsonl"
    runner.append_ledger(
        polymarket,
        runner._missed_entry(context, Venue.POLYMARKET, 0, context.start),
        context=context,
        venue=Venue.POLYMARKET,
    )
    rows = runner.read_ledger(polymarket)
    with pytest.raises(runner.RunnerError, match="campaign binding"):
        runner._validate_service_ledger(rows, context=context, venue=Venue.KALSHI)

    body = runner._ledger_body(rows[0])
    body["source_usable"] = True
    body["economic_eligible"] = True
    altered = {
        **body,
        "entry_sha256": runner.sha256_bytes(runner.canonical_json_bytes(body)),
    }
    rechained = tmp_path / "semantic" / "rechained.jsonl"
    rechained.write_bytes(runner.canonical_json_bytes(altered) + b"\n")
    assert len(runner.read_ledger(rechained)) == 1
    with pytest.raises(runner.RunnerError, match="missing-slot ledger"):
        runner._validate_service_ledger(
            runner.read_ledger(rechained),
            context=context,
            venue=Venue.POLYMARKET,
        )

    crlf = tmp_path / "semantic" / "crlf.jsonl"
    crlf.write_bytes(polymarket.read_bytes().replace(b"\n", b"\r\n"))
    with pytest.raises(runner.RunnerError, match="physical framing"):
        runner.read_ledger(crlf)
    noncanonical = tmp_path / "semantic" / "noncanonical.jsonl"
    noncanonical.write_bytes((json.dumps(rows[0], sort_keys=True) + "\n").encode("utf-8"))
    with pytest.raises(runner.RunnerError, match="not canonical JSON"):
        runner.read_ledger(noncanonical)


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


def test_capacity_refusal_publishes_the_exact_denied_snapshot_without_reread(
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
    denied = {
        "admitted": False,
        "available_bytes": 194_347_270_143,
        "h1_reserved_bytes": 144 * 1024**3,
        "prediction_remaining_bytes": 21 * 1024**3,
        "required_free_bytes": 181 * 1024**3,
        "safety_margin_bytes": 16 * 1024**3,
    }
    admitted = {**denied, "admitted": True, "available_bytes": 300 * 1024**3}
    snapshots = iter((denied, admitted))
    calls = 0

    def sequenced_snapshot(**_kwargs: object) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return next(snapshots)

    monkeypatch.setattr(runner, "capacity_snapshot", sequenced_snapshot)
    monkeypatch.setattr(
        runner,
        "schedule_decision",
        lambda *_args, **_kwargs: runner.ScheduleDecision("RUN_SLOT", 0, (), 0.0),
    )
    assert service._run_owned() == 4
    assert calls == 1
    state = json.loads(service.state_path.read_bytes())
    assert state["lifecycle"] == "CAPACITY_REFUSED"
    assert state["capacity"] == denied
    assert state["active_ordinal"] is None
    assert not service.runs_root.exists()


def test_capacity_rechecked_immediately_before_slot_and_denial_never_spawns_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _campaign(tmp_path)
    service = runner.VenueRunner(
        context=context,
        source_root=ROOT,
        python=Path("/source/.venv/bin/python"),
        venue=Venue.KALSHI,
        h1_reserved_bytes=144 * 1024**3,
        prediction_maximum_raw_bytes=21 * 1024**3,
        safety_margin_bytes=16 * 1024**3,
    )
    admitted = {
        "admitted": True,
        "available_bytes": 300 * 1024**3,
        "h1_reserved_bytes": 144 * 1024**3,
        "prediction_remaining_bytes": 21 * 1024**3,
        "required_free_bytes": 181 * 1024**3,
        "safety_margin_bytes": 16 * 1024**3,
    }
    denied = {**admitted, "admitted": False, "available_bytes": 180 * 1024**3}
    snapshots = iter((admitted, denied))
    calls = 0

    def sequenced_snapshot(**_kwargs: object) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return next(snapshots)

    monkeypatch.setattr(runner, "capacity_snapshot", sequenced_snapshot)
    monkeypatch.setattr(
        runner,
        "schedule_decision",
        lambda *_args, **_kwargs: runner.ScheduleDecision("RUN_SLOT", 0, (), 0.0),
    )
    monkeypatch.setattr(
        runner.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("capacity-denied slot spawned a child"),
    )
    assert service._run_owned() == 4
    assert calls == 2
    state = json.loads(service.state_path.read_bytes())
    assert state["lifecycle"] == "CAPACITY_REFUSED"
    assert state["capacity"] == denied
    assert state["active_ordinal"] is None
    assert not service.runs_root.exists()


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


def test_two_venue_cli_runner_authenticates_capacity_receipts_and_resume_never_replays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _current_synthetic_campaign(tmp_path)

    class Usage:
        total = 400 * 1024**3
        used = 100 * 1024**3
        free = 300 * 1024**3

    class ClosedFixtureSession:
        def get(self, *_args, **_kwargs):  # pragma: no cover - fail-closed network guard
            raise AssertionError("synthetic fixture attempted a public HTTP request")

        post = get

        def close(self) -> None:
            return None

    actual_run_public_probe = probe_module.run_public_probe

    def fixture_run_public_probe(config, **_kwargs):
        return actual_run_public_probe(config, http_session_factory=ClosedFixtureSession)

    monkeypatch.setattr(probe_module, "_polymarket_probe", _synthetic_capacity_transport)
    monkeypatch.setattr(probe_module, "_kalshi_probe", _synthetic_capacity_transport)
    monkeypatch.setattr(research_cli, "run_public_probe", fixture_run_public_probe)
    monkeypatch.setattr(runner.shutil, "disk_usage", lambda _path: Usage())

    invocations: list[tuple[str, ...]] = []

    class CliProcess:
        def __init__(self, command, *, cwd, env):
            assert Path(cwd) == ROOT
            assert env["PYTHONNOUSERSITE"] == "1"
            arguments = tuple(str(item) for item in command)
            invocations.append(arguments)
            completed = CliRunner().invoke(app, list(arguments[3:]))
            self.returncode = completed.exit_code
            self.output = completed.output

        def wait(self) -> int:
            return self.returncode

        def poll(self) -> int:
            return self.returncode

        def send_signal(self, _signal: int) -> None:  # pragma: no cover - not used by this fixture
            raise AssertionError("completed synthetic fixture process received a signal")

    monkeypatch.setattr(runner.subprocess, "Popen", CliProcess)
    services: dict[Venue, runner.VenueRunner] = {}
    for venue in (Venue.POLYMARKET, Venue.KALSHI):
        service = runner.VenueRunner(
            context=context,
            source_root=ROOT,
            python=Path("/synthetic-fixture/.venv/bin/python"),
            venue=venue,
            h1_reserved_bytes=144 * 1024**3,
            prediction_maximum_raw_bytes=21 * 1024**3,
            safety_margin_bytes=16 * 1024**3,
        )
        service._run_slot(0)
        services[venue] = service
        ledger = runner.read_ledger(service.ledger_path)
        assert len(ledger) == 1
        assert ledger[0]["ordinal"] == 0
        assert ledger[0]["terminal_health"] == "MAX_BYTES_REACHED"
        assert ledger[0]["error"] == (
            "SYNTHETIC/FIXTURE bounded raw-byte budget reached"
        )
        output_root = next(service.runs_root.iterdir())
        authenticated = runner._validate_result(output_root, context, venue, ordinal=0)
        assert authenticated["terminal_health"] == "MAX_BYTES_REACHED"

    assert len(invocations) == 2
    original_schedule_decision = runner.schedule_decision
    for venue, previous in services.items():
        resumed = runner.VenueRunner(
            context=context,
            source_root=ROOT,
            python=Path("/synthetic-fixture/.venv/bin/python"),
            venue=venue,
            h1_reserved_bytes=144 * 1024**3,
            prediction_maximum_raw_bytes=21 * 1024**3,
            safety_margin_bytes=16 * 1024**3,
        )

        def stop_after_resume_decision(
            resume_context,
            ledger,
            *,
            now,
            resumed_service=resumed,
        ):
            del now
            decision = original_schedule_decision(
                resume_context,
                ledger,
                now=resume_context.start + timedelta(seconds=10),
            )
            assert decision.action == "WAIT_NEXT_SLOT"
            resumed_service.stop_requested = True
            return decision

        with monkeypatch.context() as resume_patch:
            resume_patch.setattr(runner, "schedule_decision", stop_after_resume_decision)
            assert resumed.run() == 130
        assert runner.read_ledger(previous.ledger_path)[0]["ordinal"] == 0
    assert len(invocations) == 2


def test_two_real_error_shapes_are_terminally_ledgered_without_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _current_synthetic_campaign(tmp_path)

    class Usage:
        total = 400 * 1024**3
        used = 100 * 1024**3
        free = 300 * 1024**3

    class ClosedFixtureSession:
        def get(self, *_args, **_kwargs):  # pragma: no cover - fail-closed guard
            raise AssertionError("invalid-source fixture attempted public HTTP")

        post = get

        def close(self) -> None:
            return None

    actual_run_public_probe = probe_module.run_public_probe

    def fixture_run_public_probe(config, **_kwargs):
        return actual_run_public_probe(config, http_session_factory=ClosedFixtureSession)

    monkeypatch.setattr(
        probe_module,
        "_polymarket_probe",
        _synthetic_public_source_invalid_transport,
    )
    monkeypatch.setattr(
        probe_module,
        "_kalshi_probe",
        _synthetic_public_source_invalid_transport,
    )
    monkeypatch.setattr(research_cli, "run_public_probe", fixture_run_public_probe)
    monkeypatch.setattr(runner.shutil, "disk_usage", lambda _path: Usage())
    invocations: list[tuple[str, ...]] = []
    child_exit_codes: list[int] = []

    class CliProcess:
        def __init__(self, command, *, cwd, env):
            assert Path(cwd) == ROOT
            assert env["PYTHONNOUSERSITE"] == "1"
            arguments = tuple(str(item) for item in command)
            invocations.append(arguments)
            completed = CliRunner().invoke(app, list(arguments[3:]))
            self.returncode = completed.exit_code
            child_exit_codes.append(completed.exit_code)

        def wait(self) -> int:
            return self.returncode

        def poll(self) -> int:
            return self.returncode

        def send_signal(self, _signal: int) -> None:  # pragma: no cover
            raise AssertionError("completed invalid-source fixture received a signal")

    monkeypatch.setattr(runner.subprocess, "Popen", CliProcess)
    services: dict[Venue, runner.VenueRunner] = {}
    for venue in (Venue.POLYMARKET, Venue.KALSHI):
        service = runner.VenueRunner(
            context=context,
            source_root=ROOT,
            python=Path("/synthetic-fixture/.venv/bin/python"),
            venue=venue,
            h1_reserved_bytes=144 * 1024**3,
            prediction_maximum_raw_bytes=21 * 1024**3,
            safety_margin_bytes=16 * 1024**3,
        )
        service._run_slot(0)
        service._publish("WAITING_NEXT_SLOT")
        rows = service._ledger()
        assert len(rows) == 1
        assert rows[0]["terminal_health"] == "PUBLIC_SOURCE_INVALID"
        assert rows[0]["receipt_classification"] == (
            "CAMPAIGN_BOUND_EXPLICIT_GAP_EXCLUDED_FROM_ECONOMICS"
        )
        assert rows[0]["source_usable"] is False
        assert rows[0]["economic_eligible"] is False
        assert rows[0]["frames"] > 0
        assert len(str(rows[0]["manifest_sha256"])) == 64
        state = json.loads(service.state_path.read_text(encoding="utf-8"))
        assert state["lifecycle"] == "WAITING_NEXT_SLOT"
        assert state["recorded_slots"] == 1
        assert state["data_quality"] == {
            "alert": True,
            "count": 1,
            "error": rows[0]["error"],
            "latest_ordinal": 0,
            "source_usable": False,
            "terminal_health": "PUBLIC_SOURCE_INVALID",
            "terminal_result_sha256": rows[0]["terminal_result_sha256"],
        }
        services[venue] = service

    assert child_exit_codes == [4, 4]
    assert len(invocations) == 2
    original_schedule_decision = runner.schedule_decision
    for venue, previous in services.items():
        resumed = runner.VenueRunner(
            context=context,
            source_root=ROOT,
            python=Path("/synthetic-fixture/.venv/bin/python"),
            venue=venue,
            h1_reserved_bytes=144 * 1024**3,
            prediction_maximum_raw_bytes=21 * 1024**3,
            safety_margin_bytes=16 * 1024**3,
        )

        def stop_after_resume_decision(
            resume_context,
            ledger,
            *,
            now,
            resumed_service=resumed,
        ):
            del now
            decision = original_schedule_decision(
                resume_context,
                ledger,
                now=resume_context.start + timedelta(seconds=10),
            )
            assert decision.action == "WAIT_NEXT_SLOT"
            resumed_service.stop_requested = True
            return decision

        with monkeypatch.context() as resume_patch:
            resume_patch.setattr(runner, "schedule_decision", stop_after_resume_decision)
            assert resumed.run() == 130
        assert previous._ledger()[0]["ordinal"] == 0
    assert len(invocations) == 2


def test_late_public_source_unavailability_is_terminally_excluded_without_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _current_synthetic_campaign(tmp_path)

    class Usage:
        total = 400 * 1024**3
        used = 100 * 1024**3
        free = 300 * 1024**3

    class ClosedFixtureSession:
        def get(self, *_args, **_kwargs):  # pragma: no cover - fail-closed guard
            raise AssertionError("late-unavailability fixture attempted public HTTP")

        post = get

        def close(self) -> None:
            return None

    actual_run_public_probe = probe_module.run_public_probe

    def fixture_run_public_probe(config, **_kwargs):
        return actual_run_public_probe(config, http_session_factory=ClosedFixtureSession)

    monkeypatch.setattr(
        probe_module,
        "_polymarket_probe",
        _synthetic_late_public_source_unavailable_transport,
    )
    monkeypatch.setattr(
        probe_module,
        "_kalshi_probe",
        _synthetic_late_public_source_unavailable_transport,
    )
    monkeypatch.setattr(research_cli, "run_public_probe", fixture_run_public_probe)
    monkeypatch.setattr(runner.shutil, "disk_usage", lambda _path: Usage())
    invocations: list[tuple[str, ...]] = []
    child_exit_codes: list[int] = []

    class CliProcess:
        def __init__(self, command, *, cwd, env):
            assert Path(cwd) == ROOT
            assert env["PYTHONNOUSERSITE"] == "1"
            arguments = tuple(str(item) for item in command)
            invocations.append(arguments)
            completed = CliRunner().invoke(app, list(arguments[3:]))
            self.returncode = completed.exit_code
            child_exit_codes.append(completed.exit_code)

        def wait(self) -> int:
            return self.returncode

        def poll(self) -> int:
            return self.returncode

        def send_signal(self, _signal: int) -> None:  # pragma: no cover
            raise AssertionError("completed unavailable-source fixture received a signal")

    monkeypatch.setattr(runner.subprocess, "Popen", CliProcess)
    services: dict[Venue, runner.VenueRunner] = {}
    for venue in (Venue.POLYMARKET, Venue.KALSHI):
        service = runner.VenueRunner(
            context=context,
            source_root=ROOT,
            python=Path("/synthetic-fixture/.venv/bin/python"),
            venue=venue,
            h1_reserved_bytes=144 * 1024**3,
            prediction_maximum_raw_bytes=21 * 1024**3,
            safety_margin_bytes=16 * 1024**3,
        )
        service._run_slot(0)
        service._publish("WAITING_NEXT_SLOT")
        rows = service._ledger()
        assert len(rows) == 1
        row = rows[0]
        assert row["terminal_health"] == "PUBLIC_SOURCE_UNAVAILABLE"
        assert row["receipt_classification"] == (
            "CAMPAIGN_BOUND_EXPLICIT_GAP_EXCLUDED_FROM_ECONOMICS"
        )
        assert row["source_usable"] is False
        assert row["economic_eligible"] is False
        assert row["frames"] > 0
        assert row["bytes"] > 0
        assert row["segments"] > 0
        for field in (
            "manifest_sha256",
            "probe_binding_sha256",
            "root_sha256",
            "terminal_result_sha256",
        ):
            assert len(str(row[field])) == 64
        assert isinstance(row["error"], str) and row["error"]
        state = json.loads(service.state_path.read_text(encoding="utf-8"))
        assert state["lifecycle"] == "WAITING_NEXT_SLOT"
        assert state["recorded_slots"] == 1
        assert state["last_terminal"] == "PUBLIC_SOURCE_UNAVAILABLE"
        assert state["data_quality"] is None
        services[venue] = service

    assert child_exit_codes == [3, 3]
    assert len(invocations) == 2
    original_schedule_decision = runner.schedule_decision
    for venue, previous in services.items():
        resumed = runner.VenueRunner(
            context=context,
            source_root=ROOT,
            python=Path("/synthetic-fixture/.venv/bin/python"),
            venue=venue,
            h1_reserved_bytes=144 * 1024**3,
            prediction_maximum_raw_bytes=21 * 1024**3,
            safety_margin_bytes=16 * 1024**3,
        )

        def stop_after_resume_decision(
            resume_context,
            ledger,
            *,
            now,
            resumed_service=resumed,
        ):
            del now
            decision = original_schedule_decision(
                resume_context,
                ledger,
                now=resume_context.start + timedelta(seconds=10),
            )
            assert decision.action == "WAIT_NEXT_SLOT"
            resumed_service.stop_requested = True
            return decision

        with monkeypatch.context() as resume_patch:
            resume_patch.setattr(runner, "schedule_decision", stop_after_resume_decision)
            assert resumed.run() == 130
        assert previous._ledger()[0]["ordinal"] == 0
    assert len(invocations) == 2


def test_terminal_receipt_rejects_raw_corruption_and_divergent_collection_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _current_synthetic_campaign(tmp_path)

    class ClosedFixtureSession:
        def close(self) -> None:
            return None

    actual_run_public_probe = probe_module.run_public_probe
    monkeypatch.setattr(probe_module, "_polymarket_probe", _synthetic_capacity_transport)
    result = actual_run_public_probe(
        probe_module.ProbeConfig(
            output_root=tmp_path / "collection",
            venue=Venue.POLYMARKET,
            feeds=context.preregistration.collection_plans[Venue.POLYMARKET].feeds,
            instruments=(),
            census_limit=context.preregistration.collection_plans[Venue.POLYMARKET].census_limit,
            duration_seconds=context.duration_seconds,
            max_bytes=context.preregistration.collection_plans[Venue.POLYMARKET].max_bytes,
            max_segment_bytes=context.preregistration.collection_plans[
                Venue.POLYMARKET
            ].max_segment_bytes,
            rotation_seconds=context.preregistration.collection_plans[
                Venue.POLYMARKET
            ].rotation_seconds,
            progress_interval_seconds=context.preregistration.collection_plans[
                Venue.POLYMARKET
            ].progress_interval_seconds,
            collection_id=context.preregistration.prospective_shard_policy.collection_id(
                base_collection_id=context.preregistration.collection_plans[
                    Venue.POLYMARKET
                ].collection_id(str(context.manifest["campaign_id"])),
                campaign_manifest_sha256=str(context.manifest["manifest_sha256"]),
                venue=Venue.POLYMARKET,
                ordinal=0,
                scheduled_start=context.start,
            ),
            max_frames=context.preregistration.collection_plans[Venue.POLYMARKET].max_frames,
            max_segments=context.preregistration.collection_plans[
                Venue.POLYMARKET
            ].max_segments,
            max_network_calls=context.preregistration.collection_plans[
                Venue.POLYMARKET
            ].max_network_calls,
            campaign_manifest_sha256=str(context.manifest["manifest_sha256"]),
            official_contract_sha256=context.contracts[Venue.POLYMARKET].contract_sha256,
            candidate_config_sha256=context.preregistration.config_sha256,
            collection_cutoff_utc_ns_exclusive=(
                runner._datetime_utc_ns(context.start)
                + context.cadence_seconds * 1_000_000_000
            ),
        ),
        http_session_factory=ClosedFixtureSession,
    )
    assert result.terminal_health == "MAX_BYTES_REACHED"
    output_root = tmp_path / "collection"
    assert runner._validate_result(
        output_root,
        context,
        Venue.POLYMARKET,
        ordinal=0,
    )["terminal_health"] == "MAX_BYTES_REACHED"

    result_path = output_root / "reports" / "result.json"
    original = result_path.read_bytes()
    missing_capacity_error = json.loads(original)
    missing_capacity_error["error"] = None
    result_path.write_bytes(canonical_json_bytes(missing_capacity_error))
    with pytest.raises(runner.RunnerError, match="not admissible"):
        runner._validate_result(output_root, context, Venue.POLYMARKET, ordinal=0)

    inconsistent_complete = json.loads(original)
    inconsistent_complete["terminal_health"] = "COMPLETE"
    result_path.write_bytes(canonical_json_bytes(inconsistent_complete))
    with pytest.raises(runner.RunnerError, match="not admissible"):
        runner._validate_result(output_root, context, Venue.POLYMARKET, ordinal=0)

    corrupted = json.loads(original)
    corrupted["root_sha256"] = "0" * 64
    result_path.write_bytes(canonical_json_bytes(corrupted))
    with pytest.raises(runner.RunnerError, match="authenticated raw manifest"):
        runner._validate_result(output_root, context, Venue.POLYMARKET, ordinal=0)
    result_path.write_bytes(original)

    plan = context.preregistration.collection_plans[Venue.POLYMARKET]
    divergent_plan = replace(plan, max_network_calls=plan.max_network_calls - 1)
    divergent_candidate = replace(
        context.preregistration,
        collection_plans={
            **context.preregistration.collection_plans,
            Venue.POLYMARKET: divergent_plan,
        },
    )
    divergent_context = replace(context, preregistration=divergent_candidate)
    with pytest.raises(runner.RunnerError, match="frozen plan"):
        runner._validate_result(
            output_root,
            divergent_context,
            Venue.POLYMARKET,
            ordinal=0,
        )
