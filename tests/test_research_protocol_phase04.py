from __future__ import annotations

import dataclasses
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from hyperlab.backtest.protocol import (
    FinalTestLock,
    FinalTestState,
    SplitPlan,
    TimeRange,
    WalkForwardSpec,
    canonical_json,
)
from hyperlab.backtest.registry import (
    ObjectiveDirection,
    ResearchRegistry,
    SelectionObjective,
    ValidationResult,
    VariantSpec,
    select_best_variant,
)


def _range(start_day: int, end_day: int) -> TimeRange:
    return TimeRange(
        datetime(2026, 1, start_day, tzinfo=UTC),
        datetime(2026, 1, end_day, tzinfo=UTC),
    )


def _plan() -> SplitPlan:
    return SplitPlan(
        train=_range(1, 10),
        validation=_range(11, 20),
        test=_range(21, 30),
        dataset_hash="d" * 64,
    )


def _variant(plan: SplitPlan, *, threshold: float = 1.5) -> VariantSpec:
    return VariantSpec(
        strategy_name="momentum_regime",
        parameters={
            "lookback_hours": 72,
            "filters": {"threshold": threshold, "assets": ["BTC", "ETH"]},
        },
        split_hash=plan.canonical_hash,
        data_hash=plan.dataset_hash,
        code_hash="c" * 64,
        objective=SelectionObjective("sharpe", ObjectiveDirection.MAXIMIZE),
        seed=42,
        scenario="realistic",
    )


def _register_and_select(
    registry: ResearchRegistry,
    plan: SplitPlan,
    variant: VariantSpec,
) -> ValidationResult:
    registry.register_plan(plan)
    registry.register_variant(variant)
    event = registry.record_success(
        variant.variant_hash,
        {variant.objective.metric: -0.25},
        split="walk_forward",
        provenance={"window_hash": "wf-window-1"},
    )
    result = ValidationResult.from_event(event)
    registry.record_selection(
        result,
        objective=variant.objective,
        selection_view=plan.selection_view,
    )
    return result


def test_split_plan_requires_bounded_utc_ranges_and_has_stable_hash() -> None:
    plan = _plan()
    same_data_different_order = {
        "validation": plan.validation.to_dict(),
        "train": plan.train.to_dict(),
        "dataset_hash": plan.dataset_hash,
        "test": plan.test.to_dict(),
        "schema_version": 1,
    }

    assert plan.canonical_json() == canonical_json(same_data_different_order)
    assert len(plan.canonical_hash) == 64
    assert plan.canonical_hash == _plan().canonical_hash

    with pytest.raises(ValueError, match="UTC"):
        TimeRange(datetime(2026, 1, 1), datetime(2026, 1, 2))
    with pytest.raises(ValueError, match="UTC"):
        TimeRange(
            datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            datetime(2026, 1, 2, tzinfo=timezone(timedelta(hours=1))),
        )
    with pytest.raises(ValueError, match="non-empty"):
        TimeRange(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC))
    with pytest.raises(ValueError, match="overlap"):
        SplitPlan(_range(1, 10), _range(9, 20), _range(21, 30), "dataset")


def test_selection_view_structurally_hides_final_test() -> None:
    plan = _plan()
    view = plan.selection_view

    assert view() is view
    assert view.exposes_final_test is False
    assert "test" not in view.to_dict()
    assert "final_test" not in view.to_dict()
    assert "2026-01-21" not in view.canonical_json()
    assert {field.name for field in dataclasses.fields(view)} == {
        "train",
        "validation",
        "dataset_hash",
        "plan_hash",
    }


def test_split_plan_requires_a_cryptographic_dataset_hash() -> None:
    with pytest.raises(ValueError, match="dataset_hash must be a lowercase SHA-256 digest"):
        SplitPlan(
            train=_range(1, 10),
            validation=_range(11, 20),
            test=_range(21, 30),
            dataset_hash="dataset-label",
        )


def test_final_test_is_revealed_once_only_after_freezing_one_variant() -> None:
    plan = _plan()
    lock = FinalTestLock(plan)

    with pytest.raises(RuntimeError, match="freeze"):
        lock.reveal_final_test("premature")
    token = lock.freeze_variant("variant-hash")
    assert lock.state == FinalTestState.VARIANT_FROZEN
    assert lock.frozen_variant_hash == "variant-hash"
    with pytest.raises(RuntimeError, match="exactly one"):
        lock.freeze_variant("another-variant")
    with pytest.raises(PermissionError, match="invalid"):
        lock.reveal_final_test("wrong-token")

    assert lock.reveal_final_test(token) == plan.test
    assert lock.was_revealed
    with pytest.raises(RuntimeError, match="already"):
        lock.reveal_final_test(token)


def test_walk_forward_is_chronological_nonempty_and_respects_embargo() -> None:
    bounds = TimeRange(
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 16, tzinfo=UTC),
    )
    spec = WalkForwardSpec(
        bounds=bounds,
        train_window=timedelta(days=5),
        embargo=timedelta(days=1),
        validation_window=timedelta(days=2),
        step=timedelta(days=2),
    )
    windows = spec.windows()

    assert len(windows) == 4
    assert all(window.train.duration > timedelta(0) for window in windows)
    assert all(window.validation.duration > timedelta(0) for window in windows)
    assert all(window.validation.start - window.train.end == timedelta(days=1) for window in windows)
    assert [window.validation.start for window in windows] == sorted(
        window.validation.start for window in windows
    )
    assert all(window.validation.end <= bounds.end for window in windows)

    expanding = WalkForwardSpec(
        bounds=bounds,
        train_window=timedelta(days=5),
        validation_window=timedelta(days=2),
        step=timedelta(days=2),
        expanding=True,
    ).windows()
    assert all(window.train.start == bounds.start for window in expanding)
    assert expanding[-1].train.duration > expanding[0].train.duration


@pytest.mark.parametrize(
    "kwargs",
    [
        {"train_window": timedelta(0)},
        {"validation_window": timedelta(0)},
        {"step": timedelta(0)},
        {"embargo": timedelta(seconds=-1)},
        {"train_window": timedelta(days=14)},
    ],
)
def test_walk_forward_rejects_invalid_or_empty_segments(kwargs: dict[str, timedelta]) -> None:
    values = {
        "bounds": TimeRange(
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 15, tzinfo=UTC),
        ),
        "train_window": timedelta(days=5),
        "validation_window": timedelta(days=2),
        "step": timedelta(days=1),
        "embargo": timedelta(0),
    }
    values.update(kwargs)
    with pytest.raises(ValueError):
        WalkForwardSpec(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("forbidden", ["target_return", "monthly_target", "return_target"])
def test_variant_rejects_return_targets_recursively(forbidden: str) -> None:
    plan = _plan()
    with pytest.raises(ValueError, match="forbidden return target"):
        VariantSpec(
            strategy_name="strategy",
            parameters={"nested": [{"deeper": {forbidden: 0.05}}]},
            split_hash=plan.canonical_hash,
            data_hash=plan.dataset_hash,
            code_hash="c" * 64,
            objective=SelectionObjective("sharpe"),
        )


def test_variant_hash_captures_complete_immutable_parameters_and_objective_has_no_target() -> None:
    plan = _plan()
    original = {"lookback": 24, "nested": {"threshold": 1.5}}
    variant = VariantSpec(
        strategy_name="strategy",
        parameters=original,
        split_hash=plan.canonical_hash,
        data_hash=plan.dataset_hash,
        code_hash="c" * 64,
        objective=SelectionObjective("sharpe"),
    )
    original["nested"]["threshold"] = 99.0  # type: ignore[index]

    assert variant.to_dict()["parameters"] == {"lookback": 24, "nested": {"threshold": 1.5}}
    assert variant.variant_hash == variant.canonical_hash
    assert _variant(plan, threshold=1.5).variant_hash != _variant(plan, threshold=2.0).variant_hash
    assert {field.name for field in dataclasses.fields(SelectionObjective)} == {"metric", "direction"}


@pytest.mark.parametrize(("field", "value"), [("data_hash", "dataset"), ("code_hash", "commit")])
def test_variant_requires_cryptographic_data_and_code_hashes(field: str, value: str) -> None:
    plan = _plan()
    values = {
        "strategy_name": "strategy",
        "parameters": {},
        "split_hash": plan.canonical_hash,
        "data_hash": plan.dataset_hash,
        "code_hash": "c" * 64,
        "objective": SelectionObjective("sharpe"),
    }
    values[field] = value

    with pytest.raises(ValueError, match=f"{field} must be a lowercase SHA-256 digest"):
        VariantSpec(**values)  # type: ignore[arg-type]


def test_selection_uses_validation_results_only_and_keeps_negative_scores() -> None:
    objective = SelectionObjective("net_return", "maximize")
    view = _plan().selection_view
    losing = ValidationResult("losing", {"net_return": -0.20}, view.plan_hash, view.dataset_hash)
    less_bad = ValidationResult("less-bad", {"net_return": -0.05}, view.plan_hash, view.dataset_hash)

    assert select_best_variant([losing, less_bad], objective, selection_view=view) is less_bad
    with pytest.raises(TypeError, match="ValidationResult"):
        select_best_variant(  # type: ignore[list-item]
            [{"variant_hash": "test", "metrics": {"net_return": 99.0}}],
            objective,
            selection_view=view,
        )
    with pytest.raises(ValueError, match="lacks objective"):
        select_best_variant(
            [ValidationResult("missing", {"sharpe": 1.0}, view.plan_hash, view.dataset_hash)],
            objective,
            selection_view=view,
        )

    with pytest.raises(ValueError, match="different split plan"):
        select_best_variant(
            [ValidationResult("leaked", {"net_return": 99.0}, "other-plan", view.dataset_hash)],
            objective,
            selection_view=view,
        )


def test_jsonl_registry_appends_variant_then_success_loss_and_failure(tmp_path: Path) -> None:
    now = datetime(2026, 8, 12, 12, tzinfo=UTC)
    path = tmp_path / "variants.jsonl"
    registry = ResearchRegistry(path, clock=lambda: now)
    plan = _plan()
    variant = _variant(plan)

    with pytest.raises(ValueError, match="registered"):
        registry.record_success(variant.variant_hash, {"net_return": -0.10})
    registry.register_plan(plan)
    registered_hash = registry.register_variant(variant)
    first_bytes = path.read_bytes()
    loss_event = registry.record_success(registered_hash, {"net_return": -0.25})
    after_loss_bytes = path.read_bytes()
    failure_event = registry.record_failure(
        registered_hash,
        RuntimeError("calibration failed"),
        split="stress",
        metrics={"net_return": -0.40},
    )

    assert after_loss_bytes.startswith(first_bytes)
    assert path.read_bytes().startswith(after_loss_bytes)
    assert loss_event["metrics"] == {"net_return": -0.25}
    assert failure_event["status"] == "failure"
    events = registry.events()
    assert [event["event_type"] for event in events] == [
        "plan_created",
        "variant_registered",
        "result_recorded",
        "result_recorded",
    ]
    assert [event["sequence"] for event in events] == [1, 2, 3, 4]
    assert events[2]["status"] == "success"
    assert events[2]["metrics"] == {"net_return": -0.25}
    assert events[3]["metrics"] == {"net_return": -0.4}
    assert all(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines())


def test_registry_refuses_duplicate_variants_and_detects_tampering(tmp_path: Path) -> None:
    path = tmp_path / "variants.jsonl"
    registry = ResearchRegistry(path)
    plan = _plan()
    variant = _variant(plan)
    registry.register_plan(plan)
    registry.register_variant(variant)

    with pytest.raises(ValueError, match="already registered"):
        registry.register_variant(variant)

    lines = path.read_text(encoding="utf-8").splitlines()
    line = json.loads(lines[-1])
    line["variant_hash"] = "tampered"
    lines[-1] = json.dumps(line)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="invalid event hash"):
        registry.events()


def test_registry_requires_revealed_matching_lock_for_one_final_result(tmp_path: Path) -> None:
    registry = ResearchRegistry(tmp_path / "variants.jsonl")
    plan = _plan()
    variant = _variant(plan)
    _register_and_select(registry, plan, variant)
    lock = FinalTestLock(plan)

    with pytest.raises(ValueError, match="record_final_success"):
        registry.record_success(variant.variant_hash, {"net_return": 0.1}, split="final_test")
    with pytest.raises(ValueError, match="must be revealed"):
        registry.record_final_success(variant.variant_hash, {"net_return": 0.1}, lock=lock)

    token = registry.freeze_final_test(variant.variant_hash, lock)
    registry.reveal_final_test(variant.variant_hash, lock, token)
    event = registry.record_final_success(
        variant.variant_hash,
        {"net_return": -0.1},
        lock=lock,
    )
    assert event["split"] == "final_test"
    assert event["plan_hash"] == plan.canonical_hash
    assert event["metrics"] == {"net_return": -0.1}
    with pytest.raises(ValueError, match="duplicates a variant/split result"):
        registry.record_final_success(variant.variant_hash, {"net_return": 0.2}, lock=lock)


def test_reveal_rejects_a_lock_frozen_for_another_variant(tmp_path: Path) -> None:
    registry = ResearchRegistry(tmp_path / "variants.jsonl")
    plan = _plan()
    selected = _variant(plan)
    _register_and_select(registry, plan, selected)
    persisted_lock = FinalTestLock(plan)
    registry.freeze_final_test(selected.variant_hash, persisted_lock)

    other_lock = FinalTestLock(plan)
    other_token = other_lock.freeze_variant("other-variant")
    with pytest.raises(ValueError, match="different variant"):
        registry.reveal_final_test(selected.variant_hash, other_lock, other_token)

    assert all(event["event_type"] != "final_test_revealed" for event in registry.events())


def test_final_result_is_bound_to_its_split_and_other_plans_remain_recordable(
    tmp_path: Path,
) -> None:
    registry = ResearchRegistry(tmp_path / "variants.jsonl")
    first_plan = _plan()
    first_variant = _variant(first_plan)
    _register_and_select(registry, first_plan, first_variant)

    mismatched_plan = SplitPlan(
        train=first_plan.train,
        validation=first_plan.validation,
        test=first_plan.test,
        dataset_hash="e" * 64,
    )
    mismatched_lock = FinalTestLock(mismatched_plan)
    with pytest.raises(ValueError, match="different split plan"):
        registry.freeze_final_test(first_variant.variant_hash, mismatched_lock)

    first_lock = FinalTestLock(first_plan)
    token = registry.freeze_final_test(first_variant.variant_hash, first_lock)
    registry.reveal_final_test(first_variant.variant_hash, first_lock, token)
    registry.record_final_success(first_variant.variant_hash, {"net_return": -0.1}, lock=first_lock)

    second_variant = _variant(mismatched_plan, threshold=2.0)
    _register_and_select(registry, mismatched_plan, second_variant)
    second_lock = FinalTestLock(mismatched_plan)
    token = registry.freeze_final_test(second_variant.variant_hash, second_lock)
    registry.reveal_final_test(second_variant.variant_hash, second_lock, token)
    second_event = registry.record_final_success(
        second_variant.variant_hash,
        {"net_return": -0.2},
        lock=second_lock,
    )
    assert second_event["plan_hash"] == mismatched_plan.canonical_hash


@pytest.mark.parametrize(
    "forbidden",
    [
        "targetReturn",
        "returnTarget",
        "annual_return_target",
        "target_cagr_return",
        "profit_target",
        "distance_to_5pct_return",
    ],
)
def test_return_target_guard_rejects_naming_variants(forbidden: str) -> None:
    with pytest.raises(ValueError, match="forbidden return target"):
        VariantSpec(
            "strategy",
            {forbidden: 0.05},
            "split",
            "data",
            "code",
            SelectionObjective("sharpe"),
        )


def test_freeze_and_reveal_are_persisted_before_final_result(tmp_path: Path) -> None:
    registry = ResearchRegistry(tmp_path / "variants.jsonl")
    plan = _plan()
    variant = _variant(plan)
    _register_and_select(registry, plan, variant)
    lock = FinalTestLock(plan)
    token = registry.freeze_final_test(variant.variant_hash, lock)
    revealed = registry.reveal_final_test(variant.variant_hash, lock, token)
    assert revealed == plan.test

    events = registry.events()
    assert [event["event_type"] for event in events] == [
        "plan_created",
        "variant_registered",
        "result_recorded",
        "variant_selected",
        "final_test_frozen",
        "final_test_revealed",
    ]
    registry.record_final_success(variant.variant_hash, {"net_return": -0.1}, lock=lock)


def test_plan_is_persisted_and_bound_before_any_variant(tmp_path: Path) -> None:
    registry = ResearchRegistry(tmp_path / "variants.jsonl")
    plan = _plan()
    variant = _variant(plan)

    with pytest.raises(ValueError, match="before its plan"):
        registry.register_variant(variant)
    assert registry.register_plan(plan) == plan.canonical_hash
    plan_event = registry.events()[0]
    assert plan_event["event_type"] == "plan_created"
    assert plan_event["plan"] == plan.to_dict()
    assert plan_event["plan_hash"] == plan.canonical_hash
    assert plan.artifact() == {"plan": plan.to_dict(), "plan_hash": plan.canonical_hash}

    wrong_data = dataclasses.replace(variant, data_hash="e" * 64)
    with pytest.raises(ValueError, match="wrong data"):
        registry.register_variant(wrong_data)


def test_result_provenance_selection_and_duplicate_split_guard(tmp_path: Path) -> None:
    registry = ResearchRegistry(tmp_path / "variants.jsonl")
    plan = _plan()
    variant = _variant(plan)
    registry.register_plan(plan)
    registry.register_variant(variant)
    event = registry.record_success(
        variant.variant_hash,
        {"sharpe": -1.25},
        split="walk_forward",
        provenance={"window_hash": "window-01", "seed": 42},
    )
    result = ValidationResult.from_event(event)

    assert result.event_hash == event["event_hash"]
    assert result.plan_hash == plan.canonical_hash
    assert result.data_hash == plan.dataset_hash
    assert result.evaluation_split == "walk_forward"
    assert result.window_hash == "window-01"
    assert event["provenance"] == {"seed": 42, "window_hash": "window-01"}
    selected = registry.record_selection(
        result,
        objective=variant.objective,
        selection_view=plan.selection_view,
    )
    assert selected["event_type"] == "variant_selected"
    assert selected["result_event_hash"] == event["event_hash"]

    forged = ValidationResult(
        variant_hash=variant.variant_hash,
        metrics={"sharpe": 999.0},
        split_hash=plan.canonical_hash,
        data_hash=plan.dataset_hash,
        evaluation_split="walk_forward",
        event_hash=str(event["event_hash"]),
        window_hash="window-01",
    )
    with pytest.raises(ValueError, match="persisted event provenance"):
        registry.record_selection(
            forged,
            objective=variant.objective,
            selection_view=plan.selection_view,
        )

    with pytest.raises(ValueError, match="duplicates a variant/split result"):
        registry.record_failure(
            variant.variant_hash,
            "late failure",
            split="walk_forward",
        )


def test_failure_and_interruption_are_first_class_terminal_results(tmp_path: Path) -> None:
    registry = ResearchRegistry(tmp_path / "variants.jsonl")
    plan = _plan()
    failed = _variant(plan, threshold=1.0)
    interrupted = _variant(plan, threshold=2.0)
    registry.register_plan(plan)
    registry.register_variant(failed)
    registry.register_variant(interrupted)

    failure = registry.record_failure(failed.variant_hash, RuntimeError("fit failed"))
    stopped = registry.record_interrupted(interrupted.variant_hash, "operator stop")
    assert failure["status"] == "failure"
    assert failure["error"] == {"message": "fit failed", "type": "RuntimeError"}
    assert stopped["status"] == "interrupted"
    assert stopped["error"] == {"message": "operator stop", "type": "str"}


def test_empty_keyboard_interrupt_is_still_recorded(tmp_path: Path) -> None:
    registry = ResearchRegistry(tmp_path / "variants.jsonl")
    plan = _plan()
    variant = _variant(plan)
    registry.register_plan(plan)
    registry.register_variant(variant)

    event = registry.record_interrupted(variant.variant_hash, KeyboardInterrupt())

    assert event["status"] == "interrupted"
    assert event["error"] == {"message": "KeyboardInterrupt", "type": "KeyboardInterrupt"}


def test_validation_result_ignores_encoded_non_finite_metrics() -> None:
    plan = _plan()
    event: dict[str, object] = {
        "event_type": "result_recorded",
        "status": "success",
        "variant_hash": "variant",
        "metrics": {
            "sharpe": 1.25,
            "sortino": {"non_finite": "infinity"},
        },
        "plan_hash": plan.canonical_hash,
        "data_hash": plan.dataset_hash,
        "split": "validation",
        "event_hash": "a" * 64,
        "provenance": {},
    }
    result = ValidationResult.from_event(event)
    assert dict(result.metrics) == {"sharpe": 1.25}


def test_committed_head_detects_complete_terminal_line_truncation(tmp_path: Path) -> None:
    path = tmp_path / "variants.jsonl"
    registry = ResearchRegistry(path)
    plan = _plan()
    registry.register_plan(plan)
    registry.register_variant(_variant(plan))
    complete_lines = path.read_bytes().splitlines(keepends=True)

    path.write_bytes(b"".join(complete_lines[:-1]))
    with pytest.raises(ValueError, match=r"committed head.*truncation"):
        registry.events()


def test_registry_serializes_concurrent_process_style_writers(tmp_path: Path) -> None:
    path = tmp_path / "variants.jsonl"
    ResearchRegistry(path).register_plan(_plan())
    variants = [_variant(_plan(), threshold=float(index)) for index in range(1, 9)]

    def register(variant: VariantSpec) -> str:
        return ResearchRegistry(path).register_variant(variant)

    with ThreadPoolExecutor(max_workers=4) as pool:
        hashes = list(pool.map(register, variants))

    events = ResearchRegistry(path).events()
    assert len(set(hashes)) == len(variants)
    assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))
    assert sum(event["event_type"] == "variant_registered" for event in events) == len(variants)


@pytest.mark.parametrize(
    "parameters",
    [
        {"objective": {"metric": "return", "target": 0.05}},
        {"optimizer": "distanceToReturnTarget"},
        {"desiredAnnualizedYield": 0.05},
    ],
)
def test_return_target_guard_rejects_structural_and_value_aliases(
    parameters: dict[str, object],
) -> None:
    plan = _plan()
    with pytest.raises(ValueError, match="forbidden return target"):
        VariantSpec(
            "strategy",
            parameters,
            plan.canonical_hash,
            plan.dataset_hash,
            "code",
            SelectionObjective("sharpe"),
        )


def test_selection_objective_is_strictly_allowlisted() -> None:
    with pytest.raises(ValueError, match="allowlisted"):
        SelectionObjective("custom_targetish_score")
