from __future__ import annotations

import ast
import copy
import hashlib
import io
import json
import shutil
import sqlite3
import subprocess
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from hyperlab.paper import (
    PaperEngine,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    PaperStore,
)
from scripts import diagnose_paper_replay_copy as replay_diagnostic


class _AdvancingClock:
    def __init__(self, step: int) -> None:
        self.value = 0
        self.step = step

    def __call__(self) -> int:
        self.value += self.step
        return self.value

    def advance(self, amount: int) -> None:
        self.value += amount


def _install_v3_clocks(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[_AdvancingClock, _AdvancingClock]:
    wall = _AdvancingClock(1)
    cpu = _AdvancingClock(1)
    monkeypatch.setattr(replay_diagnostic, "perf_counter_ns", wall)
    monkeypatch.setattr(replay_diagnostic, "thread_time_ns", cpu)
    return wall, cpu


def _complete_v3_input(
    profiler: replay_diagnostic.ReplayProfiler,
    wall: _AdvancingClock,
    cpu: _AdvancingClock,
    *,
    commit_sequence: int,
    input_type: str = "TIMER",
    extra_nanoseconds: int = 0,
    nested_operation: bool = False,
) -> None:
    if "replay_store_generation" not in profiler.phase_starts:
        profiler.start_phase("replay_store_generation")
        for phase_operation in (
            "replay_store_setup",
            "source_input_fetch",
            "replay_store_post_inputs",
        ):
            with profiler.replay_operation(phase_operation):
                wall.advance(1_000)
                cpu.advance(250)
    profiler.begin_replay_input(
        commit_sequence=commit_sequence,
        input_type=input_type,
    )
    profiler.transition_replay_operation(None)
    with profiler.replay_parent_scope("business_reducer"):
        with profiler.replay_operation("input_business_logic"):
            wall.advance(10_000 + extra_nanoseconds)
            cpu.advance(2_500 + extra_nanoseconds // 4)
            if nested_operation:
                with profiler.replay_operation(
                    "historical_projection_before_lookup"
                ):
                    wall.advance(2_000)
                    cpu.advance(500)
                wall.advance(2_000)
                cpu.advance(500)
        if input_type == "PUBLIC_FUNDING_SETTLEMENT":
            after_commit_sequence = max(0, commit_sequence - 2)
            query_metadata = {
                "after_commit_sequence": after_commit_sequence,
                "has_input_type_filter": True,
                "parameter_count": 3,
                "query_shape": "paper_inbox_run_after_commit_input_type_ordered",
            }
            profiler.begin_funding_lookup()
            try:
                with profiler.replay_parent_scope("funding_lookup"):
                    for operation in (
                        "filtered_input_query_prepare",
                        "filtered_input_sqlite_execute",
                        "filtered_input_fetch",
                    ):
                        profiler.observe_historical_replay(
                            operation,
                            "begin",
                            query_metadata,
                        )
                        wall.advance(2_000)
                        cpu.advance(500)
                        profiler.observe_historical_replay(
                            operation,
                            "end",
                            query_metadata,
                        )
                    profiler.observe_historical_replay(
                        "filtered_input_row_reconstruct",
                        "begin",
                        {},
                    )
                    wall.advance(2_000)
                    cpu.advance(500)
                    profiler.observe_historical_replay(
                        "filtered_input_row_reconstruct",
                        "end",
                        {
                            "commit_sequence": commit_sequence,
                            "payload_json_characters": 128,
                        },
                    )
                    with profiler.replay_operation("funding_json_decode"):
                        wall.advance(2_000)
                        cpu.advance(500)
                    with profiler.replay_operation("funding_lookup_residual"):
                        wall.advance(2_000)
                        cpu.advance(500)
            finally:
                profiler.finish_funding_lookup()
        if input_type != "RUN_START":
            with profiler.replay_parent_scope("engine_commit"):
                with profiler.replay_operation("input_commit_prepare"):
                    wall.advance(2_000)
                    cpu.advance(500)
                with profiler.replay_parent_scope("store_append"):
                    for operation in (
                        "append_input_canonicalization",
                        "append_prepare_alerts",
                        "append_prepare_events",
                        "append_prepare_ledger",
                        "append_projection_canonicalization",
                        "append_projection_history_storage",
                    ):
                        with profiler.replay_operation(operation):
                            wall.advance(2_000)
                            cpu.advance(500)
                    with (
                        profiler.replay_parent_scope("replay_validation"),
                        profiler.replay_operation("validation_residual"),
                    ):
                        wall.advance(2_000)
                        cpu.advance(500)
                        for operation in (
                            "validation_alert_comparison",
                            "validation_alert_expected_canonicalization",
                            "validation_alert_supplied_canonicalization",
                            "validation_apply_events",
                            "validation_expected_ledger",
                            "validation_ledger_comparison",
                            "validation_ledger_expected_canonicalization",
                            "validation_ledger_supplied_canonicalization",
                            "validation_projection_canonicalization",
                            "validation_projection_comparison",
                            "validation_projection_decode",
                            "validation_projection_query",
                            "validation_projection_reconstruction",
                        ):
                            with profiler.replay_operation(operation):
                                wall.advance(2_000)
                                cpu.advance(500)
                    with profiler.replay_operation(
                        "diagnostic_post_commit_accounting"
                    ):
                        wall.advance(2_000)
                        cpu.advance(500)
                with profiler.replay_operation("input_commit_return"):
                    wall.advance(2_000)
                    cpu.advance(500)
            with profiler.replay_operation("input_result_return"):
                wall.advance(2_000)
                cpu.advance(500)
    profiler.counters["target_append_transaction_count"] += 1
    profiler.finish_replay_input(completed=True)

def _paper_config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="diagnostic_v3_synthetic_fixture",
        strategy_hash="a" * 64,
        parameters={"fixture": "SYNTHETIC_V3_LOCAL_ONLY"},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(
            calibration_status="SYNTHETIC",
            source="deterministic-local-v3-test",
        ),
        risk=PaperRiskLimits(),
        seed=31,
        initial_cash=Decimal("100000"),
        validation_started_at=datetime(2026, 8, 23, tzinfo=UTC),
        data_calibration_status="SYNTHETIC",
        data_source="deterministic-local-v3-test",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _create_eqp_database(path: Path, *, run_id: str, secret_payload: str) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE paper_inbox (
                run_id TEXT NOT NULL,
                input_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                commit_sequence INTEGER NOT NULL,
                PRIMARY KEY (run_id, input_id)
            );
            CREATE INDEX idx_paper_inbox_run_commit
            ON paper_inbox (run_id, commit_sequence, input_id);
            """
        )
        connection.execute(
            """
            INSERT INTO paper_inbox (
                run_id, input_id, payload_json, commit_sequence
            ) VALUES (?, ?, ?, ?)
            """,
            (
                run_id,
                "input-secret-marker",
                json.dumps(
                    {
                        "input_type": "RECONCILE",
                        "private_marker": secret_payload,
                    }
                ),
                1,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _overhead_observations(repetitions: int) -> list[dict[str, object]]:
    schedule = replay_diagnostic._replay_overhead_schedule(repetitions)
    occurrence = {"OFF": 0, "V2": 0, "V3": 0}
    wall_base = {"OFF": 10.0, "V2": 12.0, "V3": 15.0}
    cpu_base = {"OFF": 5.0, "V2": 6.0, "V3": 8.0}
    observations: list[dict[str, object]] = []
    for ordinal, mode in enumerate(schedule):
        sample = occurrence[mode]
        occurrence[mode] += 1
        observations.append(
            {
                "cpu_seconds": cpu_base[mode] + sample,
                "input_count": 10,
                "input_type_counts": {"RUN_START": 1, "TIMER": 9},
                "input_type_wall_seconds": (
                    {}
                    if mode == "OFF"
                    else {
                        "RUN_START": 0.2 + 0.1 * (mode == "V3") + sample * 0.01,
                        "TIMER": 1.8 + 0.9 * (mode == "V3") + sample * 0.01,
                    }
                ),
                "logical_result_identity": "a" * 64,
                "mode": mode,
                "peak_rss_bytes": 1000 + sample,
                "run_ordinal": ordinal,
                "store_bytes": 4096,
                "store_initial_identity": "b" * 64,
                "wall_seconds": wall_base[mode] + sample,
                "workload_identity": "c" * 64,
            }
        )
    return observations


def test_v3_matrix_exclusive_conservation_and_parent_scope_accounting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")

    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=1,
        input_type="RUN_START",
    )
    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=2,
        extra_nanoseconds=1_000,
        nested_operation=True,
    )
    snapshot = profiler.replay_timing_v3_snapshot()
    matrix = {
        (entry["input_type"], entry["operation"]): entry
        for entry in snapshot["input_type_operation_matrix"]
    }

    outer = matrix[("TIMER", "input_business_logic")]
    nested = matrix[(
        "TIMER",
        "historical_projection_before_lookup",
    )]
    assert outer["span_count"] == 2
    assert nested["span_count"] == 1
    assert snapshot["over_attributed_operation_wall_nanoseconds"] == 0
    assert snapshot["conservation_delta_wall_nanoseconds"] == 0
    assert (
        snapshot["completed_input_wall_nanoseconds"]
        == snapshot["exclusive_operation_wall_nanoseconds"]
        + snapshot["input_residual_wall_nanoseconds"]
    )

    scopes = {entry["scope"]: entry for entry in snapshot["parent_scopes"]}
    assert scopes["business_reducer"]["parent"] == "input_dispatch"
    assert scopes["engine_commit"]["parent"] == "business_reducer"
    for entry in scopes.values():
        assert entry["inclusive_wall_nanoseconds"] == (
            entry["children_inclusive_wall_nanoseconds"]
            + entry["self_wall_nanoseconds"]
        )
        assert entry["unattributed_wall_nanoseconds"] == max(
            0,
            entry["self_wall_nanoseconds"]
            - entry["direct_operation_wall_nanoseconds"],
        )
        assert entry["accounting_delta_wall_nanoseconds"] == 0
    assert snapshot["instrumentation_complete"] is True


def test_v3_required_coverage_cannot_be_closed_by_residual(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")
    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=1,
        input_type="RUN_START",
    )
    _complete_v3_input(profiler, wall, cpu, commit_sequence=2)

    complete = profiler.replay_timing_v3_snapshot()
    assert complete["required_coverage_complete"] is True
    assert complete["unattributed_wall_within_tolerance"] is True
    assert complete["instrumentation_complete"] is True

    del profiler._v3_operation_matrix[("TIMER", "validation_projection_query")]
    incomplete = profiler.replay_timing_v3_snapshot()
    missing = {
        item["operation"]: item["missing_input_count"]
        for item in incomplete["required_operation_coverage"]
    }
    assert incomplete["conservation_delta_wall_nanoseconds"] == 0
    assert missing["validation_projection_query"] == 1
    assert incomplete["required_coverage_complete"] is False
    assert incomplete["instrumentation_complete"] is False


def test_v3_unattributed_fraction_tolerance_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")
    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=1,
        input_type="RUN_START",
    )
    exclusive = sum(
        int(values["wall_nanoseconds"])
        for values in profiler._v3_operation_matrix.values()
    )
    type_timing = profiler._v3_input_type_timings["RUN_START"]

    below = exclusive + int(
        exclusive
        * replay_diagnostic._REPLAY_V3_UNATTRIBUTED_WALL_FRACTION_TOLERANCE
        / 2
    )
    profiler._v3_completed_input_wall_ns = below
    type_timing["wall_nanoseconds"] = below
    type_timing["max_wall_nanoseconds"] = below
    phase_operation_wall = sum(
        int(values["wall_nanoseconds"])
        for values in profiler._v3_phase_operation_timings.values()
    )
    profiler._v3_replay_phase_wall_ns = below + phase_operation_wall
    below_snapshot = profiler.replay_timing_v3_snapshot()
    assert below_snapshot["unattributed_wall_within_tolerance"] is True
    assert below_snapshot["instrumentation_complete"] is True

    above = exclusive * 2
    profiler._v3_completed_input_wall_ns = above
    type_timing["wall_nanoseconds"] = above
    type_timing["max_wall_nanoseconds"] = above
    profiler._v3_replay_phase_wall_ns = above + phase_operation_wall
    above_snapshot = profiler.replay_timing_v3_snapshot()
    assert above_snapshot["conservation_delta_wall_nanoseconds"] == 0
    assert above_snapshot["input_residual_wall_fraction"] == pytest.approx(0.5)
    assert above_snapshot["unattributed_wall_within_tolerance"] is False
    assert above_snapshot["instrumentation_complete"] is False


def test_v3_phase_residual_and_run_start_scope_gates_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")
    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=1,
        input_type="RUN_START",
    )
    _complete_v3_input(profiler, wall, cpu, commit_sequence=2)

    complete = profiler.replay_timing_v3_snapshot()
    assert complete["instrumentation_complete"] is True
    attributed = (
        int(complete["exclusive_operation_wall_nanoseconds"])
        + int(complete["phase_operation_wall_nanoseconds"])
        + int(complete["phase_known_diagnostic_wall_nanoseconds"])
    )
    profiler._v3_replay_phase_wall_ns = attributed * 10
    phase_incomplete = profiler.replay_timing_v3_snapshot()
    assert phase_incomplete["input_residual_wall_fraction"] < 0.01
    assert phase_incomplete["phase_unattributed_wall_fraction"] == pytest.approx(
        0.9
    )
    assert phase_incomplete["unattributed_wall_within_tolerance"] is False
    assert phase_incomplete["instrumentation_complete"] is False

    second_run_start = replay_diagnostic.ReplayProfiler(
        100,
        instrumentation_mode="V3",
    )
    _complete_v3_input(
        second_run_start,
        wall,
        cpu,
        commit_sequence=1,
        input_type="RUN_START",
    )
    _complete_v3_input(
        second_run_start,
        wall,
        cpu,
        commit_sequence=2,
        input_type="RUN_START",
    )
    duplicate_snapshot = second_run_start.replay_timing_v3_snapshot()
    assert duplicate_snapshot["required_coverage_complete"] is False
    assert duplicate_snapshot["instrumentation_complete"] is False

    compensated_scope = replay_diagnostic.ReplayProfiler(
        100,
        instrumentation_mode="V3",
    )
    _complete_v3_input(
        compensated_scope,
        wall,
        cpu,
        commit_sequence=1,
        input_type="RUN_START",
    )
    _complete_v3_input(compensated_scope, wall, cpu, commit_sequence=2)
    engine_scope = compensated_scope._v3_parent_scope_timings["engine_commit"]
    assert engine_scope["affected_input_count"] == 1
    assert engine_scope["run_start_affected_input_count"] == 0
    engine_scope["run_start_affected_input_count"] = 1

    scope_incomplete = compensated_scope.replay_timing_v3_snapshot()
    engine_coverage = next(
        item
        for item in scope_incomplete["required_scope_coverage"]
        if item["scope"] == "engine_commit"
    )
    assert engine_coverage["affected_input_count"] == 0
    assert engine_coverage["missing_input_count"] == 1
    assert engine_coverage["unexpected_population_input_count"] == 1
    assert scope_incomplete["required_coverage_complete"] is False
    assert scope_incomplete["instrumentation_complete"] is False

def test_v3_top_eight_is_deterministic_and_tail_sixteen_keeps_full_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")

    for commit_sequence in range(1, 25):
        _complete_v3_input(
            profiler,
            wall,
            cpu,
            commit_sequence=commit_sequence,
            input_type="TIMER",
            extra_nanoseconds=commit_sequence * 1_000,
        )

    snapshot = profiler.replay_timing_v3_snapshot()
    slowest = snapshot["slowest_completed_inputs"]
    tail = snapshot["completed_input_tail"]
    assert [item["commit_sequence"] for item in slowest] == list(range(24, 16, -1))
    assert [item["commit_sequence"] for item in tail] == list(range(9, 25))
    for item in [*slowest, *tail]:
        assert set(item) == {
            "alert_count",
            "commit_sequence",
            "event_count",
            "funding_lookup",
            "input_type",
            "ledger_entry_count",
            "operations",
            "projection_after_characters",
            "projection_before_characters",
            "projection_characters",
            "query_fingerprints",
            "scopes",
            "thread_cpu_nanoseconds",
            "wall_nanoseconds",
            "zlib_bytes",
        }
        assert {entry["operation"] for entry in item["operations"]} >= {
            "input_business_logic",
            "input_commit_prepare",
            "input_dispatch_prepare",
        }


def test_v3_top_eight_retains_all_three_slowest_funding_lookups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")
    funding_sequences = {3, 7, 11}

    for commit_sequence in range(1, 13):
        is_funding = commit_sequence in funding_sequences
        _complete_v3_input(
            profiler,
            wall,
            cpu,
            commit_sequence=commit_sequence,
            input_type=(
                "PUBLIC_FUNDING_SETTLEMENT" if is_funding else "TIMER"
            ),
            extra_nanoseconds=(
                1_000_000 + commit_sequence if is_funding else commit_sequence
            ),
        )

    slowest = profiler.replay_timing_v3_snapshot()["slowest_completed_inputs"]
    retained_funding = {
        int(item["commit_sequence"]): item
        for item in slowest
        if item["input_type"] == "PUBLIC_FUNDING_SETTLEMENT"
    }
    assert set(retained_funding) == funding_sequences
    for item in retained_funding.values():
        assert isinstance(item["funding_lookup"], dict)
        assert item["funding_lookup"]["rows_returned"] == 1
        assert {
            operation["operation"] for operation in item["operations"]
        } >= {
            "funding_json_decode",
            "funding_lookup_residual",
            "funding_query_prepare",
            "funding_reconstruct_canonicalize",
            "funding_sqlite_execute",
            "funding_sqlite_fetch",
        }

def test_off_mode_never_calls_nanosecond_clocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_clock() -> int:
        raise AssertionError("OFF instrumentation called a nanosecond clock")

    monkeypatch.setattr(replay_diagnostic, "perf_counter_ns", forbidden_clock)
    monkeypatch.setattr(replay_diagnostic, "thread_time_ns", forbidden_clock)
    monkeypatch.setattr(
        replay_diagnostic,
        "_capture_sanitized_funding_eqp",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("OFF instrumentation attempted EQP capture")
        ),
    )
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="OFF")
    profiler.set_source_replay_tail(
        expected_target_commits=1,
        first_record=(1, "RUN_START"),
        records=[(1, "RUN_START")],
    )

    profiler.begin_replay_input(commit_sequence=1, input_type="RUN_START")
    assert profiler.replay_progress_snapshot()["active_input"]["wall_nanoseconds"] == 0
    with profiler.replay_operation("input_business_logic"):
        pass
    with profiler.replay_parent_scope("business_reducer"):
        pass
    profiler.begin_funding_lookup()
    profiler.observe_historical_replay("UNKNOWN", "invalid", {})
    profiler.observe_funding_record(object(), after_commit_sequence=0)
    profiler.capture_funding_eqp(Path("must-not-open.sqlite3"), run_id="0" * 64)
    profiler.finish_funding_lookup()
    profiler.counters["target_append_transaction_count"] += 1
    profiler.finish_replay_input(completed=True)
    summary = profiler.summary()

    assert summary["instrumentation_mode"] == "OFF"
    assert "replay_timing_v2" not in summary
    assert "replay_timing_v3" not in summary
    assert summary["replay_progress"]["input_type_counts"] == {"RUN_START": 1}


def test_v3_resolve_inputs_refuses_copy_and_scratch_symlinks_before_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_copy = tmp_path / "copy.sqlite3"
    forbidden_original = tmp_path / "original.sqlite3"
    scratch_root = tmp_path / "scratch"
    args = replay_diagnostic._parser().parse_args(
        [
            "--database-copy",
            str(database_copy),
            "--forbid-original",
            str(forbidden_original),
            "--scratch-root",
            str(scratch_root),
            "--run-id",
            "1" * 64,
            "--expected-sha256",
            "2" * 64,
        ]
    )
    symlink_target = {"path": database_copy}
    original_is_symlink = Path.is_symlink

    def selected_is_symlink(path: Path) -> bool:
        return path == symlink_target["path"] or original_is_symlink(path)

    monkeypatch.setattr(Path, "is_symlink", selected_is_symlink)
    with pytest.raises(replay_diagnostic.DiagnosticRefusal) as copy_refusal:
        replay_diagnostic._resolve_inputs(args)
    assert copy_refusal.value.status == "REFUSED_COPY_SYMLINK"

    symlink_target["path"] = scratch_root
    with pytest.raises(replay_diagnostic.DiagnosticRefusal) as scratch_refusal:
        replay_diagnostic._resolve_inputs(args)
    assert scratch_refusal.value.status == "REFUSED_SCRATCH_SYMLINK"


def test_v3_script_has_no_private_api_wallet_signer_or_order_route() -> None:
    source = Path(replay_diagnostic.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    } | {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }

    assert not any(
        module == "hyperliquid.exchange"
        or module.startswith("hyperliquid.exchange.")
        for module in imported_modules
    )
    assert imported_modules.isdisjoint(
        {"aiohttp", "httpx", "requests", "socket", "websockets"}
    )
    assert called_names.isdisjoint(
        {
            "Exchange",
            "cancel_order",
            "place_order",
            "sign_order",
            "wallet",
        }
    )
    assert '"orders_enabled": False' in source
    assert '"authorizes_real_money": False' in source

def test_funding_eqp_is_query_only_bounded_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "eqp-secret-path.sqlite3"
    run_id = "d" * 64
    payload_secret = "DO_NOT_EMIT_PAYLOAD_9f44"
    _create_eqp_database(database, run_id=run_id, secret_payload=payload_secret)
    before = _sha256(database)

    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")
    profiler.set_source_replay_tail(
        expected_target_commits=2,
        first_record=(1, "RUN_START"),
        records=[(1, "RUN_START"), (2, "TIMER")],
    )
    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=1,
        input_type="RUN_START",
    )
    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=2,
        input_type="TIMER",
        extra_nanoseconds=500,
    )
    profiler.observe_projection_history_storage(
        projection_characters=10,
        zlib_bytes=5,
    )
    profiler.target_database_path = database
    profiler.capture_funding_eqp(database, run_id=run_id)
    progress = profiler.replay_progress_snapshot()
    timing = profiler.replay_timing_v3_snapshot()
    captured = timing["funding_lookup"]

    assert _sha256(database) == before
    assert captured["eqp_capture_connection_count"] == 1
    assert captured["eqp_capture_query_only_verified"] is True
    assert captured["eqp_capture_thread_cpu_nanoseconds"] == 1
    assert captured["eqp_capture_total_changes"] == 0
    assert captured["eqp_capture_wall_nanoseconds"] == 1
    assert captured["eqp_capture_phase_wall_share"] == pytest.approx(
        1 / timing["replay_store_phase_wall_nanoseconds"]
    )
    assert timing["phase_known_diagnostic_wall_nanoseconds"] == 1
    assert (
        timing[
            "phase_unattributed_excluding_known_diagnostics_wall_nanoseconds"
        ]
        == timing["phase_unattributed_wall_nanoseconds"]
    )
    assert (
        timing["phase_over_attributed_known_diagnostic_wall_nanoseconds"]
        == 0
    )
    assert 1 <= len(captured["eqp"]) <= replay_diagnostic._REPLAY_V3_EQP_LIMIT
    assert all(
        set(row)
        == {
            "access",
            "covering_index",
            "index_name",
            "parent_id",
            "select_id",
            "uses_temp_btree",
        }
        for row in captured["eqp"]
    )
    serialized = json.dumps(captured, sort_keys=True)
    assert str(database) not in serialized
    assert run_id not in serialized
    assert payload_secret not in serialized
    assert "input-secret-marker" not in serialized
    assert "EXPLAIN QUERY PLAN" not in serialized
    assert "SELECT *" not in serialized
    assert "json_extract" not in serialized
    assert replay_diagnostic._is_replay_progress(
        progress,
        terminal=True,
        target_commits=2,
        expected_commits=2,
    )
    assert replay_diagnostic._is_replay_timing_v3(
        timing,
        terminal=True,
        progress=progress,
    )

def test_v3_worker_protocol_counts_paper_store_and_eqp_connections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "4" * 64
    config_hash = "5" * 64
    event_head_hash = "6" * 64
    commit_head_hash = "7" * 64
    projection_hash = "8" * 64
    database = tmp_path / "worker-eqp.sqlite3"
    _create_eqp_database(database, run_id=run_id, secret_payload="LOCAL_ONLY")
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")
    profiler.target_database_bytes = 1
    profiler.set_source_replay_tail(
        expected_target_commits=2,
        first_record=(1, "RUN_START"),
        records=[(1, "RUN_START"), (2, "TIMER")],
    )
    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=1,
        input_type="RUN_START",
    )
    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=2,
        input_type="TIMER",
    )
    profiler.observe_projection_history_storage(
        projection_characters=10,
        zlib_bytes=5,
    )
    profiler.capture_funding_eqp(database, run_id=run_id)
    progress = profiler.replay_progress_snapshot()
    timing = profiler.replay_timing_v3_snapshot()
    head_identity: list[object] = [
        run_id,
        config_hash,
        "FLAT",
        2,
        event_head_hash,
        2,
        commit_head_hash,
        2,
        projection_hash,
    ]
    profile = {
        "counters": {},
        "instrumentation_mode": "V3",
        "logical_row_counts_note": (
            "integrity row hooks and append arguments; not physical SQLite scans"
        ),
        "phase_counters": {},
        "phase_timings": {
            phase: {
                "counters": {},
                "cpu_seconds": 0.0,
                "peak_rss_bytes": 0,
                "peak_rss_source": "resource-getrusage",
                "status": "completed",
                "wall_seconds": 0.0,
            }
            for phase in replay_diagnostic._EXPECTED_PHASES
        },
        "replay_progress": progress,
        "replay_timing_v3": timing,
        "sqlite_sql_text_tracing": (
            "disabled_to_avoid_expanded_payload_materialization"
        ),
    }
    worker = {
        "authorizes_real_money": False,
        "bounded_historical_prefix_certification_count": 0,
        "config_hash": config_hash,
        "event": "worker_result",
        "event_count": 2,
        "event_head_hash": event_head_hash,
        "historical_ledger_reconciliation_count": 2,
        "mode": "PAPER_ONLY",
        "orders_enabled": False,
        "peak_rss_bytes": 0,
        "peak_rss_source": "resource-getrusage",
        "profile": profile,
        "projection_hash": projection_hash,
        "projection_history_decode_count": 0,
        "replay_cpu_seconds": 0.0,
        "replay_wall_seconds": 0.0,
        "run_id": run_id,
        "source_head_identity": head_identity,
        "source_open_mode": replay_diagnostic._SOURCE_OPEN_MODE,
        "source_projection_hash": projection_hash,
        "source_query_only_verified": True,
        "source_sqlite_connection_count": 1,
        "source_write_connection_attempts": 0,
        "status": "WORKER_COMPLETE",
        "target_database_bytes": 1,
        "target_head_identity": head_identity,
        "target_initial_identity": "9" * 64,
        "target_logical_transaction_counts": {
            "append_atomic": 2,
            "create_run": 1,
        },
        "target_paper_store_sqlite_connection_count": 1,
        "target_projection_hash": projection_hash,
        "target_sqlite_connection_count": 2,
    }

    assert set(worker) == replay_diagnostic._WORKER_RESULT_FIELDS
    assert (
        replay_diagnostic._worker_result_protocol_failure(
            worker,
            expected_run_id=run_id,
            expected_instrumentation_mode="V3",
        )
        is None
    )
    wrong_total = copy.deepcopy(worker)
    wrong_total["target_sqlite_connection_count"] = 1
    assert replay_diagnostic._worker_result_protocol_failure(
        wrong_total,
        expected_run_id=run_id,
        expected_instrumentation_mode="V3",
    ) is not None
    wrong_store_count = copy.deepcopy(worker)
    wrong_store_count["target_paper_store_sqlite_connection_count"] = 2
    assert replay_diagnostic._worker_result_protocol_failure(
        wrong_store_count,
        expected_run_id=run_id,
        expected_instrumentation_mode="V3",
    ) is not None

def test_overhead_schedule_report_and_identity_rejection() -> None:
    assert replay_diagnostic._replay_overhead_schedule(3) == (
        "OFF",
        "V2",
        "V3",
        "V3",
        "V2",
        "OFF",
        "OFF",
        "V2",
        "V3",
    )
    observations = _overhead_observations(3)
    report = replay_diagnostic._build_replay_overhead_report(
        observations,
        repetitions=3,
        projection_commits=100,
    )

    assert report["per_mode"]["OFF"]["wall_seconds"] == {
        "mad": 1.0,
        "median": 11.0,
        "samples": [10.0, 11.0, 12.0],
    }
    assert report["overhead_vs_off"]["V2"]["wall_seconds_absolute"] == 2.0
    assert report["overhead_vs_off"]["V3"]["wall_seconds_absolute"] == 5.0
    assert report["overhead_vs_off"]["V3"]["wall_seconds_per_commit"] == 0.5
    assert report["overhead_vs_off"]["V3"]["projected_wall_seconds"] == 50.0
    assert report["overhead_by_input_type"]["v3_minus_v2_measured"]["TIMER"][
        "measured_v3_minus_v2_wall_seconds"
    ] == pytest.approx(0.9)

    divergent = copy.deepcopy(observations)
    divergent[4]["logical_result_identity"] = "f" * 64
    with pytest.raises(
        ValueError,
        match="exact logical workload/result",
    ):
        replay_diagnostic._build_replay_overhead_report(
            divergent,
            repetitions=3,
        )


def test_v3_validator_and_authenticated_heartbeat_accept_then_reject_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")
    profiler.set_source_replay_tail(
        expected_target_commits=1,
        first_record=(1, "RUN_START"),
        records=[(1, "RUN_START")],
    )
    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=1,
        input_type="RUN_START",
        extra_nanoseconds=500,
    )
    progress = profiler.replay_progress_snapshot()
    timing = profiler.replay_timing_v3_snapshot()

    assert replay_diagnostic._is_replay_timing_v3(
        timing,
        terminal=False,
        progress=progress,
    )
    mutated = copy.deepcopy(timing)
    mutated["exclusive_operation_wall_nanoseconds"] += 1
    assert not replay_diagnostic._is_replay_timing_v3(
        mutated,
        terminal=False,
        progress=progress,
    )
    mutated = copy.deepcopy(timing)
    mutated["required_operation_coverage"][0]["missing_input_count"] += 1
    assert not replay_diagnostic._is_replay_timing_v3(
        mutated,
        terminal=False,
        progress=progress,
    )
    mutated = copy.deepcopy(timing)
    mutated["input_residual_wall_fraction"] += 0.01
    assert not replay_diagnostic._is_replay_timing_v3(
        mutated,
        terminal=False,
        progress=progress,
    )
    mutated = copy.deepcopy(timing)
    mutated["phase_known_diagnostic_wall_nanoseconds"] += 1
    assert not replay_diagnostic._is_replay_timing_v3(
        mutated,
        terminal=False,
        progress=progress,
    )
    mutated = copy.deepcopy(timing)
    business_scope = next(
        item
        for item in mutated["parent_scopes"]
        if item["scope"] == "business_reducer"
    )
    business_scope["run_start_affected_input_count"] = 0
    assert not replay_diagnostic._is_replay_timing_v3(
        mutated,
        terminal=False,
        progress=progress,
    )

    token = "local-v3-protocol-token"
    line = json.dumps(
        {
            "_worker_protocol_token": token,
            "elapsed_seconds": 1.0,
            "event": "phase_heartbeat",
            "peak_rss_bytes": None,
            "peak_rss_source": "peak-rss-unavailable",
            "phase": "replay_store_generation",
            "phase_cpu_seconds": 0.5,
            "phase_wall_seconds": 1.0,
            "replay_progress": progress,
            "replay_timing_v3": timing,
            "rows_observed": 0,
            "sequence": 1,
            "target_commits": 1,
        }
    )
    decoded, failure = replay_diagnostic._decode_worker_line(
        line,
        expected_run_id="e" * 64,
        expected_token=token,
        expected_instrumentation_mode="V3",
    )

    assert failure is None
    assert decoded is not None
    assert decoded["replay_progress"] == progress
    assert decoded["replay_timing_v3"] == timing
    assert "replay_timing_v2" not in decoded


def test_private_store_observer_is_replay_only_structural_and_result_neutral(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    normal = PaperStore(tmp_path / "normal.sqlite3")
    try:
        with pytest.raises(
            ValueError,
            match="requires a temporary replay store",
        ):
            normal._set_historical_replay_observer(lambda *_: None)
    finally:
        normal.close()

    config = _paper_config()
    owner = TemporaryDirectory(dir=tmp_path)
    replay_store = PaperStore._create_temporary_historical_replay(
        owner,
        filename="observer-replay.sqlite3",
    )
    try:
        def forbidden_observation(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("absent observer entered diagnostic context")

        with monkeypatch.context() as no_observer_patch:
            no_observer_patch.setattr(
                PaperStore,
                "_historical_replay_observation",
                staticmethod(forbidden_observation),
            )
            PaperEngine._for_historical_replay(replay_store, config).start()
            baseline = tuple(
                replay_store.iter_inputs(
                    config.run_id,
                    input_type="RUN_START",
                )
            )
        events: list[tuple[str, str, dict[str, object]]] = []

        def observer(
            operation: str,
            state: str,
            metadata: Mapping[str, object],
        ) -> None:
            events.append((operation, state, dict(metadata)))

        assert replay_store._set_historical_replay_observer(observer) is None
        observed = tuple(
            replay_store.iter_inputs(config.run_id, input_type="RUN_START")
        )
        assert replay_store._set_historical_replay_observer(None) is observer

        assert observed == baseline
        assert len(observed) == 1
        assert [(operation, state) for operation, state, _ in events] == [
            ("filtered_input_query_prepare", "begin"),
            ("filtered_input_query_prepare", "end"),
            ("filtered_input_sqlite_execute", "begin"),
            ("filtered_input_sqlite_execute", "end"),
            ("filtered_input_fetch", "begin"),
            ("filtered_input_fetch", "end"),
            ("filtered_input_row_reconstruct", "begin"),
            ("filtered_input_row_reconstruct", "end"),
            ("filtered_input_fetch", "begin"),
            ("filtered_input_fetch", "end"),
        ]
        allowed_metadata = {
            "after_commit_sequence",
            "commit_sequence",
            "has_input_type_filter",
            "parameter_count",
            "payload_json_characters",
            "query_shape",
            "row_returned",
        }
        assert all(set(metadata) <= allowed_metadata for _, _, metadata in events)
        serialized = json.dumps(events, sort_keys=True)
        assert config.run_id not in serialized
        assert str(replay_store.path) not in serialized
        assert "SELECT " not in serialized
        assert "payload_hash" not in serialized
    finally:
        replay_store.close()
        owner.cleanup()


def test_profiler_progress_is_logically_invariant_off_v2_v3(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiles: dict[str, replay_diagnostic.ReplayProfiler] = {}

    for mode in ("OFF", "V2", "V3"):
        profiler = replay_diagnostic.ReplayProfiler(
            100,
            instrumentation_mode=mode,
        )
        records = [(1, "RUN_START"), (2, "TIMER")]
        profiler.set_source_replay_tail(
            expected_target_commits=2,
            first_record=records[0],
            records=records,
        )
        for commit_sequence, input_type in records:
            if mode == "V3":
                _complete_v3_input(
                    profiler,
                    wall,
                    cpu,
                    commit_sequence=commit_sequence,
                    input_type=input_type,
                    extra_nanoseconds=commit_sequence * 100,
                )
            else:
                profiler.begin_replay_input(
                    commit_sequence=commit_sequence,
                    input_type=input_type,
                )
                profiler.counters["target_append_transaction_count"] += 1
                profiler.finish_replay_input(completed=True)
        profiles[mode] = profiler

    off_progress = profiles["OFF"].replay_progress_snapshot()
    assert profiles["V2"].replay_progress_snapshot() == off_progress
    assert profiles["V3"].replay_progress_snapshot() == off_progress
    assert "replay_timing_v2" not in profiles["OFF"].summary()
    assert "replay_timing_v3" not in profiles["OFF"].summary()
    assert "replay_timing_v2" in profiles["V2"].summary()
    assert "replay_timing_v3" in profiles["V3"].summary()

def test_supervisor_timeout_v3_preserves_last_progress_and_timing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original = tmp_path / "paper-original.sqlite3"
    config = _paper_config()
    source = PaperStore(original)
    try:
        PaperEngine(source, config).start()
    finally:
        source.close()
    database_copy = tmp_path / "paper-explicit-copy.sqlite3"
    shutil.copy2(original, database_copy)
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    before = database_copy.read_bytes()

    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")
    profiler.set_source_replay_tail(
        expected_target_commits=1,
        first_record=(1, "RUN_START"),
        records=[(1, "RUN_START")],
    )
    _complete_v3_input(
        profiler,
        wall,
        cpu,
        commit_sequence=1,
        input_type="RUN_START",
        extra_nanoseconds=500,
    )
    expected_progress = profiler.replay_progress_snapshot()
    expected_timing = profiler.replay_timing_v3_snapshot()
    assert replay_diagnostic._is_replay_timing_v3(
        expected_timing,
        terminal=False,
        progress=expected_progress,
    )

    args = replay_diagnostic._parser().parse_args(
        [
            "--database-copy",
            str(database_copy),
            "--forbid-original",
            str(original),
            "--scratch-root",
            str(scratch_root),
            "--run-id",
            config.run_id,
            "--expected-sha256",
            _sha256(database_copy),
            "--wall-limit-seconds",
            "60",
            "--progress-every-rows",
            "100",
            "--instrumentation-mode",
            "V3",
        ]
    )
    clock: dict[str, object] = {"armed": False, "checks": 0}
    process_holder: list[object] = []

    class FakeProcess:
        def __init__(self, token: str) -> None:
            records = (
                {
                    "_worker_protocol_token": token,
                    "elapsed_seconds": 1.0,
                    "event": "phase_started",
                    "phase": "replay_store_generation",
                    "sequence": 1,
                },
                {
                    "_worker_protocol_token": token,
                    "elapsed_seconds": 6.0,
                    "event": "phase_heartbeat",
                    "peak_rss_bytes": 0,
                    "peak_rss_source": "resource-getrusage",
                    "phase": "replay_store_generation",
                    "phase_cpu_seconds": 4.0,
                    "phase_wall_seconds": 5.0,
                    "replay_progress": expected_progress,
                    "replay_timing_v3": expected_timing,
                    "rows_observed": 10,
                    "sequence": 2,
                    "target_commits": 1,
                },
            )
            self.stdout = io.StringIO(
                "\n".join(json.dumps(record) for record in records) + "\n"
            )
            self.return_code: int | None = None
            self.terminated = False

        def poll(self) -> int | None:
            return self.return_code

        def terminate(self) -> None:
            self.terminated = True
            self.return_code = -15
            clock["armed"] = False

        def kill(self) -> None:
            self.return_code = -9
            clock["armed"] = False

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            assert self.return_code is not None
            return self.return_code

    class InlineThread:
        def __init__(
            self,
            *,
            target: Callable[..., object],
            args: tuple[object, ...],
            daemon: bool,
        ) -> None:
            del daemon
            self._target = target
            self._args = args

        def start(self) -> None:
            self._target(*self._args)
            clock["armed"] = True

        def join(self, timeout: float | None = None) -> None:
            del timeout

    def fake_popen(command: list[str], **kwargs: object) -> FakeProcess:
        assert kwargs["stderr"] is subprocess.STDOUT
        assert kwargs["stdout"] is subprocess.PIPE
        assert command[command.index("--instrumentation-mode") + 1] == "V3"
        token = command[command.index("--_worker-token") + 1]
        process = FakeProcess(token)
        process_holder.append(process)
        return process

    def fake_perf_counter() -> float:
        if not bool(clock["armed"]):
            return 0.0
        clock["checks"] = int(clock["checks"]) + 1
        return 100.0 if int(clock["checks"]) >= 2 else 0.0

    monkeypatch.setattr(replay_diagnostic, "perf_counter", fake_perf_counter)
    monkeypatch.setattr(replay_diagnostic.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(replay_diagnostic.threading, "Thread", InlineThread)

    return_code = replay_diagnostic._supervise_locked(args, "delete")

    emitted = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.strip()
    ]
    heartbeat = next(
        record for record in emitted if record["event"] == "phase_heartbeat"
    )
    worker_timeout = next(
        record for record in emitted if record["event"] == "worker_timeout"
    )
    result = emitted[-1]
    assert return_code == 124
    assert result["event"] == "diagnostic_result"
    assert result["status"] == "DIAGNOSTIC_TIMEOUT"
    assert heartbeat["replay_progress"] == expected_progress
    assert heartbeat["replay_timing_v3"] == expected_timing
    assert "replay_timing_v2" not in heartbeat
    assert worker_timeout["last_replay_progress"] == expected_progress
    assert worker_timeout["last_replay_timing_v3"] == expected_timing
    assert worker_timeout["last_replay_timing_v2"] is None
    assert result["last_replay_progress"] == expected_progress
    assert result["last_replay_timing_v3"] == expected_timing
    assert result["last_replay_timing_v2"] is None
    assert result["last_worker_phase"] == "replay_store_generation"
    assert result["last_worker_sequence"] == 2
    assert isinstance(process_holder[0], FakeProcess)
    assert process_holder[0].terminated is True
    assert database_copy.read_bytes() == before
    assert not tuple(scratch_root.iterdir())

def _overhead_terminal_fixture(mode: str) -> dict[str, object]:
    run_id = "4" * 64
    config_hash = "5" * 64
    event_head_hash = "6" * 64
    commit_head_hash = "7" * 64
    projection_hash = "8" * 64
    head_identity: list[object] = [
        run_id,
        config_hash,
        "FLAT",
        10,
        event_head_hash,
        10,
        commit_head_hash,
        10,
        projection_hash,
    ]
    profile: dict[str, object] = {
        "replay_progress": {
            "completed_target_commits": 10,
            "input_type_counts": {"RUN_START": 1, "TIMER": 9},
        }
    }
    if mode == "V2":
        profile["replay_timing_v2"] = {
            "input_type_timings": {
                "RUN_START": {"wall_seconds": 0.2},
                "TIMER": {"wall_seconds": 1.8},
            }
        }
    elif mode == "V3":
        profile["replay_timing_v3"] = {
            "input_type_totals": [
                {
                    "input_type": "RUN_START",
                    "wall_nanoseconds": 300_000_000,
                },
                {
                    "input_type": "TIMER",
                    "wall_nanoseconds": 2_700_000_000,
                },
            ]
        }
    terminal = {
        name: None
        for name in replay_diagnostic._WORKER_RESULT_FIELDS
        if name not in {"event", "status"}
    }
    terminal.update(
        {
            "authorizes_real_money": False,
            "config_hash": config_hash,
            "event": "diagnostic_result",
            "event_count": 10,
            "event_head_hash": event_head_hash,
            "instrumentation_mode": mode,
            "mode": "PAPER_ONLY",
            "orders_enabled": False,
            "peak_rss_bytes": 1_000,
            "profile": profile,
            "projection_hash": projection_hash,
            "replay_cpu_seconds": 5.0,
            "replay_wall_seconds": 10.0,
            "run_id": run_id,
            "source_head_identity": head_identity,
            "source_sha256_after": "1" * 64,
            "source_sha256_before": "1" * 64,
            "source_sha256_unchanged": True,
            "source_sidecars_observed": [],
            "source_stat_after": {
                "device": 1,
                "inode": 2,
                "mode": 3,
                "mtime_ns": 4,
                "size": 5,
            },
            "source_stat_before": {
                "device": 1,
                "inode": 2,
                "mode": 3,
                "mtime_ns": 4,
                "size": 5,
            },
            "source_stat_unchanged": True,
            "status": "REPLAY_EXACT",
            "target_database_bytes": 4_096,
            "target_head_identity": head_identity,
            "target_initial_identity": "3" * 64,
            "target_logical_transaction_counts": {
                "append_atomic": 10,
                "create_run": 1,
            },
            "target_projection_hash": projection_hash,
        }
    )
    return terminal


@pytest.mark.parametrize(
    ("mode", "expected_type_wall"),
    [
        ("OFF", {}),
        ("V2", {"RUN_START": 0.2, "TIMER": 1.8}),
        ("V3", {"RUN_START": 0.3, "TIMER": 2.7}),
    ],
)
def test_overhead_observation_is_derived_from_terminal_fields(
    mode: str,
    expected_type_wall: dict[str, float],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def accept_worker_result(
        *_args: object,
        **_kwargs: object,
    ) -> None:
        return None

    monkeypatch.setattr(
        replay_diagnostic,
        "_worker_result_protocol_failure",
        accept_worker_result,
    )
    terminal = _overhead_terminal_fixture(mode)
    observation = replay_diagnostic._overhead_observation_from_terminal(
        terminal,
        expected_run_id="4" * 64,
        expected_mode=mode,
        expected_source_sha256="1" * 64,
        script_sha256="2" * 64,
        run_ordinal=7,
    )

    assert observation["mode"] == mode
    assert observation["run_ordinal"] == 7
    assert observation["wall_seconds"] == 10.0
    assert observation["cpu_seconds"] == 5.0
    assert observation["peak_rss_bytes"] == 1_000
    assert observation["store_bytes"] == 4_096
    assert observation["input_count"] == 10
    assert observation["input_type_counts"] == {"RUN_START": 1, "TIMER": 9}
    assert observation["input_type_wall_seconds"] == expected_type_wall
    assert observation["store_initial_identity"] == "3" * 64
    assert replay_diagnostic._is_sha256(observation["workload_identity"])
    assert replay_diagnostic._is_sha256(observation["logical_result_identity"])

    if mode == "V2":
        changed_workload = copy.deepcopy(terminal)
        changed_workload["source_head_identity"][5] = 11
        changed_workload_observation = (
            replay_diagnostic._overhead_observation_from_terminal(
                changed_workload,
                expected_run_id="4" * 64,
                expected_mode=mode,
                expected_source_sha256="1" * 64,
                script_sha256="2" * 64,
                run_ordinal=7,
            )
        )
        assert (
            changed_workload_observation["workload_identity"]
            != observation["workload_identity"]
        )
        changed_result = copy.deepcopy(terminal)
        changed_result["target_head_identity"][7] = 11
        changed_result_observation = (
            replay_diagnostic._overhead_observation_from_terminal(
                changed_result,
                expected_run_id="4" * 64,
                expected_mode=mode,
                expected_source_sha256="1" * 64,
                script_sha256="2" * 64,
                run_ordinal=7,
            )
        )
        assert (
            changed_result_observation["logical_result_identity"]
            != observation["logical_result_identity"]
        )


class _CompletedOverheadProcess:
    def __init__(self, lines: list[str]) -> None:
        self.stdout = io.StringIO("\n".join(lines) + "\n")

    def poll(self) -> int:
        return 0

    def terminate(self) -> None:
        raise AssertionError("completed overhead child must not be terminated")

    def kill(self) -> None:
        raise AssertionError("completed overhead child must not be killed")

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return 0


def _overhead_args(*, repetitions: int | None) -> object:
    arguments = [
        "--database-copy",
        "synthetic-copy.sqlite3",
        "--forbid-original",
        "synthetic-original.sqlite3",
        "--scratch-root",
        "synthetic-scratch",
        "--run-id",
        "4" * 64,
        "--expected-sha256",
        "1" * 64,
        "--wall-limit-seconds",
        "60",
    ]
    if repetitions is not None:
        arguments.extend(["--overhead-repetitions", str(repetitions)])
    return replay_diagnostic._parser().parse_args(arguments)


def test_local_overhead_runner_uses_alternating_fresh_children_without_forwarding(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observations = _overhead_observations(3)
    schedule = replay_diagnostic._replay_overhead_schedule(3)
    commands: list[list[str]] = []

    def fake_popen(
        command: list[str],
        **kwargs: object,
    ) -> _CompletedOverheadProcess:
        ordinal = len(commands)
        commands.append(command)
        assert "--overhead-repetitions" not in command
        assert command[command.index("--instrumentation-mode") + 1] == schedule[ordinal]
        assert kwargs["stdout"] is subprocess.PIPE
        assert kwargs["stderr"] is subprocess.STDOUT
        assert kwargs["text"] is True
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        assert replay_diagnostic._WORKER_TOKEN_ENV not in environment
        return _CompletedOverheadProcess(
            [
                json.dumps(
                    {
                        "event": "phase_progress",
                        "path": "DO_NOT_FORWARD_PATH",
                        "payload": "DO_NOT_FORWARD_PAYLOAD",
                    }
                ),
                json.dumps(
                    {
                        "event": "diagnostic_result",
                        "mode": "PAPER_ONLY",
                        "status": "REPLAY_EXACT",
                    }
                ),
            ]
        )

    def fake_observation(
        terminal: Mapping[str, object],
        *,
        expected_run_id: str,
        expected_mode: str,
        expected_source_sha256: str,
        script_sha256: str,
        run_ordinal: int,
    ) -> dict[str, object]:
        assert terminal["event"] == "diagnostic_result"
        assert expected_run_id == "4" * 64
        assert expected_mode == schedule[run_ordinal]
        assert expected_source_sha256 == "1" * 64
        assert script_sha256 == "2" * 64
        return observations[run_ordinal]

    monkeypatch.setattr(replay_diagnostic.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(replay_diagnostic, "_sha256", lambda _path: "2" * 64)
    monkeypatch.setattr(
        replay_diagnostic,
        "_overhead_observation_from_terminal",
        fake_observation,
    )

    assert _overhead_args(repetitions=None).overhead_repetitions is None
    return_code = replay_diagnostic._run_local_replay_overhead(
        _overhead_args(repetitions=3)
    )

    assert return_code == 0
    assert len(commands) == 9
    records = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.strip()
    ]
    assert [record["event"] for record in records].count(
        "overhead_run_started"
    ) == 9
    assert [record["event"] for record in records].count(
        "overhead_run_finished"
    ) == 9
    assert records[-1]["event"] == "overhead_result"
    assert records[-1]["status"] == "OVERHEAD_COMPLETE"
    assert records[-1]["mode"] == "PAPER_ONLY"
    assert records[-1]["mode_order"] == list(schedule)
    serialized = json.dumps(records, sort_keys=True)
    assert "DO_NOT_FORWARD_PATH" not in serialized
    assert "DO_NOT_FORWARD_PAYLOAD" not in serialized


class _RunningOverheadProcess:
    def __init__(self, stdout: object) -> None:
        self.stdout = stdout
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return 1 if self.terminated or self.killed else None

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.terminated = True
        return 1


class _InterruptingOverheadStream:
    def readline(self, _limit: int) -> str:
        raise KeyboardInterrupt


def test_overhead_child_output_is_bounded_and_interrupt_cleans_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversized = _RunningOverheadProcess(
        io.StringIO(
            "x" * (replay_diagnostic._MAX_WORKER_LINE_CHARACTERS + 1)
            + "\n"
        )
    )
    monkeypatch.setattr(
        replay_diagnostic.subprocess,
        "Popen",
        lambda *_args, **_kwargs: oversized,
    )
    with pytest.raises(
        replay_diagnostic.DiagnosticRefusal,
        match="bounded capture",
    ):
        replay_diagnostic._run_overhead_child(
            ["fake-overhead-child"],
            environment={},
        )
    assert oversized.terminated is True

    interrupted = _RunningOverheadProcess(_InterruptingOverheadStream())
    monkeypatch.setattr(
        replay_diagnostic.subprocess,
        "Popen",
        lambda *_args, **_kwargs: interrupted,
    )
    with pytest.raises(KeyboardInterrupt):
        replay_diagnostic._run_overhead_child(
            ["fake-overhead-child"],
            environment={},
        )
    assert interrupted.terminated is True