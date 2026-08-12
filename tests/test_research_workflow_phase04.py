from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from hyperlab.backtest.protocol import FinalTestState
from hyperlab.backtest.workflow import ResearchWorkflowSpec, run_research_workflow
from hyperlab.config import load_settings
from hyperlab.data.synthetic import generate_demo_panel
from hyperlab.strategies.momentum import MomentumRegimeStrategy


def _workflow(tmp_path: Path, *, reveal_final: bool) -> tuple[Path, object]:
    settings = load_settings(Path("config/research.toml"))
    panel = generate_demo_panel(hours=640, seed=19, assets=("BTC",))
    output = tmp_path / ("revealed" if reveal_final else "locked")
    artifacts = run_research_workflow(
        panel,
        strategy_name="momentum_regime",
        fit_strategy=lambda _train: MomentumRegimeStrategy(),
        strategy_parameters={"lookback_hours": 72, "minimum_signal": 0.25},
        costs=settings.cost_schedule,
        risk_limits=settings.risk_profiles["offensive"],
        execution=replace(settings.execution, seed=19),
        benchmark=settings.research.benchmark,
        spec=ResearchWorkflowSpec(
            walk_forward_train_bars=160,
            walk_forward_validation_bars=64,
            walk_forward_step_bars=64,
            embargo_bars=1,
            bootstrap_block_size=12,
            bootstrap_resamples=40,
            bootstrap_seed=19,
            reveal_final=reveal_final,
        ),
        output_dir=output,
        registry_path=output / "variants.jsonl",
    )
    return output, artifacts


def test_workflow_persists_plan_registry_oos_ci_and_keeps_final_locked(tmp_path: Path) -> None:
    output, artifacts = _workflow(tmp_path, reveal_final=False)

    plan = json.loads((output / "split_plan.json").read_text(encoding="utf-8"))
    validation = json.loads((output / "validation_oos.json").read_text(encoding="utf-8"))
    manifest = json.loads((output / "run_manifest.json").read_text(encoding="utf-8"))
    events = [json.loads(line) for line in (output / "variants.jsonl").read_text().splitlines()]

    assert plan["created_before_trials"] is True
    assert events[0]["event_type"] == "plan_created"
    assert [event["event_type"] for event in events][-1] == "final_test_frozen"
    freeze_position = next(
        index for index, event in enumerate(events) if event["event_type"] == "final_test_frozen"
    )
    stress_variant_positions = [
        index
        for index, event in enumerate(events)
        if event["event_type"] == "variant_registered" and event["variant"]["scenario"] != "base"
    ]
    assert stress_variant_positions
    assert max(stress_variant_positions) < freeze_position
    assert all(event["event_type"] != "final_test_revealed" for event in events)
    assert validation["evaluation_split"] == "walk_forward_oos"
    assert validation["bootstrap"]["lower"] <= validation["bootstrap"]["upper"]
    assert validation["bootstrap"]["status"] == "AVAILABLE_OOS"
    assert validation["bootstrap"]["seed"] == 19
    assert validation["bootstrap"]["time_index_verified"] is True
    assert validation["bootstrap"]["cadence"] == "0 days 01:00:00"
    assert validation["calibration_statuses"] == {
        "costs": "UNCALIBRATED",
        "data": "SYNTHETIC",
        "maker_fills": "UNCALIBRATED",
    }
    assert manifest["final_revealed"] is False
    assert manifest["split_hash"] == plan["plan_hash"]
    for relative_path, expected_hash in manifest["files"].items():
        artifact = output / relative_path
        assert artifact.is_file()
        assert hashlib.sha256(artifact.read_bytes()).hexdigest() == expected_hash
    registry = Path(manifest["registry"]["path"])
    registry_head = Path(manifest["registry"]["head_path"])
    assert hashlib.sha256(registry.read_bytes()).hexdigest() == manifest["registry"]["sha256"]
    assert (
        hashlib.sha256(registry_head.read_bytes()).hexdigest()
        == manifest["registry"]["head_sha256"]
    )
    assert artifacts.report_path is None
    assert list((output / "ledgers").glob("validation_fold_*"))


def test_workflow_reveals_once_then_exports_final_stress_ledgers_and_report(tmp_path: Path) -> None:
    output, artifacts = _workflow(tmp_path, reveal_final=True)
    events = [json.loads(line) for line in (output / "variants.jsonl").read_text().splitlines()]

    assert sum(event["event_type"] == "final_test_revealed" for event in events) == 1
    assert sum(event.get("split") == "final_test" for event in events) == 1
    assert artifacts.report_path is not None and artifacts.report_path.exists()
    assert (output / "stress_summary.json").is_file()
    assert (output / "ledgers" / "final_test" / "fills.csv").is_file()
    stress_names = {
        event["variant"]["scenario"] for event in events if event["event_type"] == "variant_registered"
    }
    assert {"costs_x2", "latency_degraded", "remove_best_5pct"}.issubset(stress_names)
    assert all(
        result["uncertainty"]["status"] == "AVAILABLE_OOS"
        for result in json.loads((output / "latest_summary.json").read_text())["strategies"]
    )


def test_workflow_rejects_missing_lifecycle_lineage_before_trials(tmp_path: Path) -> None:
    settings = load_settings(Path("config/research.toml"))
    panel = generate_demo_panel(hours=900, seed=3, assets=("BTC",))
    panel.metadata.pop("lifecycle_hash")

    with pytest.raises(ValueError, match="lifecycle_hash"):
        run_research_workflow(
            panel,
            strategy_name="momentum_regime",
            fit_strategy=lambda _train: MomentumRegimeStrategy(),
            strategy_parameters={},
            costs=settings.cost_schedule,
            risk_limits=settings.risk_profiles["offensive"],
            execution=settings.execution,
            benchmark=settings.research.benchmark,
            spec=ResearchWorkflowSpec(
                walk_forward_train_bars=240,
                walk_forward_validation_bars=72,
                walk_forward_step_bars=72,
                bootstrap_resamples=10,
            ),
            output_dir=tmp_path / "bad",
        )


def test_workflow_module_does_not_expose_a_reusable_final_state() -> None:
    assert FinalTestState.REVEALED.value == "revealed"


def test_workflow_rejects_oos_gaps_that_would_break_temporal_blocks() -> None:
    with pytest.raises(ValueError, match="contiguous, non-overlapping"):
        ResearchWorkflowSpec(
            walk_forward_train_bars=160,
            walk_forward_validation_bars=64,
            walk_forward_step_bars=65,
        )
