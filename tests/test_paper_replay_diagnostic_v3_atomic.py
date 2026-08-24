from __future__ import annotations

import threading
from collections import Counter
from collections.abc import Mapping
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
from hyperlab.paper import store as store_module
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
    wall = _AdvancingClock(100)
    cpu = _AdvancingClock(25)
    monkeypatch.setattr(replay_diagnostic, "perf_counter_ns", wall)
    monkeypatch.setattr(replay_diagnostic, "thread_time_ns", cpu)
    return wall, cpu


def _config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="diagnostic_v3_atomic_fixture",
        strategy_hash="a" * 64,
        parameters={"fixture": "SYNTHETIC_V3_ATOMIC_LOCAL_ONLY"},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(
            calibration_status="SYNTHETIC",
            source="deterministic-local-v3-atomic-test",
        ),
        risk=PaperRiskLimits(),
        seed=37,
        initial_cash=Decimal("100000"),
        validation_started_at=datetime(2026, 8, 24, tzinfo=UTC),
        data_calibration_status="SYNTHETIC",
        data_source="deterministic-local-v3-atomic-test",
    )


def _historical_identity(store: PaperStore, run_id: str) -> tuple[object, ...]:
    run = store.get_run(run_id)
    projection = store.get_projection(run_id)
    inputs = tuple(
        (
            record.input_id,
            record.payload,
            record.payload_hash,
            record.first_event_sequence,
            record.last_event_sequence,
            record.commit_sequence,
            record.commit_hash,
        )
        for record in store.iter_inputs(run_id)
    )
    return (
        run.head_identity,
        projection.canonical_hash,
        projection.to_dict(),
        inputs,
    )


def test_v3_unknown_input_and_operation_are_counted_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")

    profiler.begin_replay_input(
        commit_sequence=1,
        input_type="NOT_IN_THE_BOUNDED_TAXONOMY",
    )
    profiler.transition_replay_operation("not_in_the_bounded_taxonomy")
    wall.advance(1_000)
    cpu.advance(250)
    profiler.counters["target_append_transaction_count"] += 1
    profiler.finish_replay_input(completed=True)
    snapshot = profiler.replay_timing_v3_snapshot()

    totals = {
        entry["input_type"]: entry
        for entry in snapshot["input_type_totals"]
    }
    matrix = snapshot["input_type_operation_matrix"]
    unknown_spans = sum(
        entry["span_count"]
        for entry in matrix
        if entry["input_type"] == "UNKNOWN"
        or entry["operation"] == "UNKNOWN"
    )
    assert snapshot["completed_input_count"] == 1
    assert snapshot["unknown_input_count"] == 1
    assert totals["UNKNOWN"]["span_count"] == 1
    assert any(
        entry["input_type"] == "UNKNOWN"
        and entry["operation"] == "UNKNOWN"
        for entry in matrix
    )
    assert unknown_spans > 0
    assert snapshot["unknown_operation_span_count"] == unknown_spans
    assert snapshot["instrumentation_complete"] is False


def test_v3_progress_and_timing_snapshots_are_atomic_under_rlock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall, cpu = _install_v3_clocks(monkeypatch)
    profiler = replay_diagnostic.ReplayProfiler(100, instrumentation_mode="V3")
    profiler.begin_replay_input(commit_sequence=1, input_type="RUN_START")
    wall.advance(500)
    cpu.advance(125)

    progress_copied = threading.Event()
    finish_entered = threading.Event()
    finish_done = threading.Event()
    original_progress = profiler.replay_progress_snapshot
    original_finish = profiler.finish_replay_input

    def observed_progress() -> dict[str, object]:
        progress = original_progress()
        progress_copied.set()
        assert finish_entered.wait(timeout=2.0)
        assert finish_done.is_set() is False
        return progress

    def observed_finish(*, completed: bool) -> None:
        finish_entered.set()
        try:
            original_finish(completed=completed)
        finally:
            finish_done.set()

    monkeypatch.setattr(profiler, "replay_progress_snapshot", observed_progress)
    monkeypatch.setattr(profiler, "finish_replay_input", observed_finish)

    def finish_input() -> None:
        assert progress_copied.wait(timeout=2.0)
        profiler.counters["target_append_transaction_count"] += 1
        profiler.finish_replay_input(completed=True)

    finisher = threading.Thread(target=finish_input, daemon=True)
    finisher.start()
    try:
        progress, timing = profiler._replay_diagnostic_snapshots()
    finally:
        finisher.join(timeout=2.0)

    assert finish_entered.is_set() is True
    assert finish_done.is_set() is True
    assert finisher.is_alive() is False
    assert timing is not None
    assert progress["completed_target_commits"] == 0
    assert timing["completed_input_count"] == 0
    assert progress["completed_target_commits"] == timing["completed_input_count"]

    monkeypatch.setattr(profiler, "replay_progress_snapshot", original_progress)
    monkeypatch.setattr(profiler, "finish_replay_input", original_finish)
    completed_progress, completed_timing = profiler._replay_diagnostic_snapshots()
    assert completed_timing is not None
    assert completed_progress["completed_target_commits"] == 1
    assert completed_timing["completed_input_count"] == 1


def test_historical_append_validation_observer_is_balanced_and_result_neutral(
    tmp_path: Path,
) -> None:
    config = _config()
    baseline_owner = TemporaryDirectory(dir=tmp_path)
    observed_owner = TemporaryDirectory(dir=tmp_path)
    baseline_store = PaperStore._create_temporary_historical_replay(
        baseline_owner,
        filename="baseline.sqlite3",
    )
    observed_store = PaperStore._create_temporary_historical_replay(
        observed_owner,
        filename="observed.sqlite3",
    )
    observations: list[tuple[str, str, dict[str, object]]] = []

    def observer(
        operation: str,
        state: str,
        metadata: Mapping[str, object],
    ) -> None:
        observations.append((operation, state, dict(metadata)))

    try:
        PaperEngine._for_historical_replay(baseline_store, config).start()
        observed_store._set_historical_replay_observer(observer)
        PaperEngine._for_historical_replay(observed_store, config).start()
        observed_store._set_historical_replay_observer(None)

        assert _historical_identity(observed_store, config.run_id) == (
            _historical_identity(baseline_store, config.run_id)
        )
        counts = Counter((operation, state) for operation, state, _ in observations)
        operations = {operation for operation, _, _ in observations}
        expected_operations = {
            "validation_alert_comparison",
            "validation_alert_expected_canonicalization",
            "validation_alert_supplied_canonicalization",
            "validation_event_apply",
            "validation_ledger_comparison",
            "validation_ledger_expected_canonicalization",
            "validation_ledger_reconstruction",
            "validation_ledger_supplied_canonicalization",
            "validation_projection_canonicalization",
            "validation_projection_comparison",
            "validation_projection_record_decode",
            "validation_projection_reconstruction",
            "validation_projection_sqlite_load",
            "validation_state_reconstruction",
        }
        assert operations == expected_operations
        assert all(
            counts[(operation, "begin")] == counts[(operation, "end")] == 1
            for operation in expected_operations
        )
        assert all(metadata == {} for _, _, metadata in observations)
    finally:
        observed_store.close()
        baseline_store.close()
        observed_owner.cleanup()
        baseline_owner.cleanup()

def test_filtered_input_observer_balances_when_row_reconstruction_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    owner = TemporaryDirectory(dir=tmp_path)
    store = PaperStore._create_temporary_historical_replay(
        owner,
        filename="row-failure.sqlite3",
    )
    observations: list[tuple[str, str]] = []
    sentinel = RuntimeError("synthetic row reconstruction failure")

    def observer(
        operation: str,
        state: str,
        _metadata: Mapping[str, object],
    ) -> None:
        observations.append((operation, state))

    def fail_input_from_row(
        _store: PaperStore,
        _row: object,
    ) -> object:
        raise sentinel

    try:
        PaperEngine._for_historical_replay(store, config).start()
        store._set_historical_replay_observer(observer)
        monkeypatch.setattr(PaperStore, "_input_from_row", fail_input_from_row)
        iterator = iter(store.iter_inputs(config.run_id, input_type="RUN_START"))
        with pytest.raises(RuntimeError) as raised:
            next(iterator)
        assert raised.value is sentinel
        counts = Counter(observations)
        for operation in (
            "filtered_input_query_prepare",
            "filtered_input_sqlite_execute",
            "filtered_input_fetch",
            "filtered_input_row_reconstruct",
        ):
            assert counts[(operation, "begin")] == counts[(operation, "end")]
        assert counts[("filtered_input_row_reconstruct", "begin")] == 1
    finally:
        store._set_historical_replay_observer(None)
        store.close()
        owner.cleanup()


def test_validation_observer_balances_and_preserves_primary_cause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    owner = TemporaryDirectory(dir=tmp_path)
    store = PaperStore._create_temporary_historical_replay(
        owner,
        filename="validation-failure.sqlite3",
    )
    observations: list[tuple[str, str]] = []
    sentinel = ValueError("synthetic durable projection decode failure")
    original_json_object = store_module._json_object

    def observer(
        operation: str,
        state: str,
        _metadata: Mapping[str, object],
    ) -> None:
        observations.append((operation, state))

    def failing_json_object(value: str, *, label: str) -> dict[str, object]:
        if label == "durable projection":
            raise sentinel
        return original_json_object(value, label=label)

    try:
        store._set_historical_replay_observer(observer)
        monkeypatch.setattr(store_module, "_json_object", failing_json_object)
        with pytest.raises(store_module.AppendConflictError) as raised:
            PaperEngine._for_historical_replay(store, config).start()
        assert raised.value.__cause__ is sentinel
        counts = Counter(observations)
        assert counts[("validation_projection_record_decode", "begin")] == 1
        assert counts[("validation_projection_record_decode", "end")] == 1
        assert all(
            counts[(operation, "begin")] == counts[(operation, "end")]
            for operation, _state in observations
        )
    finally:
        store._set_historical_replay_observer(None)
        store.close()
        owner.cleanup()
