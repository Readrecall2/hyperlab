from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from typer.testing import CliRunner

from hyperlab import cli as cli_module
from hyperlab.analysis.reporting import write_lead_lag_artifacts
from hyperlab.cli import app

runner = CliRunner()


def _input_paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    root = tmp_path / "lake"
    root.mkdir()
    gate_report = tmp_path / "gate.json"
    gate_report.write_text("{}\n", encoding="utf-8")
    config = tmp_path / "study.toml"
    config.write_text("[study]\n", encoding="utf-8")
    return root, gate_report, config


def _arguments(
    root: Path,
    gate_report: Path,
    config: Path,
    output: Path,
) -> list[str]:
    return [
        "lead-lag-study",
        str(root),
        "--gate-report",
        str(gate_report),
        "--config",
        str(config),
        "--output",
        str(output),
    ]


def test_lead_lag_help_exposes_only_offline_study_inputs() -> None:
    result = runner.invoke(app, ["lead-lag-study", "--help"], env={"COLUMNS": "180"})

    assert result.exit_code == 0
    for value in ("root", "--gate-report", "--config", "--output"):
        assert value in result.output
    for forbidden in ("--network", "--duration", "--collect", "--trade"):
        assert forbidden not in result.output


def test_lead_lag_refuses_output_inside_lake_before_loading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, gate_report, config = _input_paths(tmp_path)
    output = root / "analysis"
    monkeypatch.setattr(
        cli_module,
        "load_lead_lag_config",
        lambda _path: pytest.fail("unsafe output must be rejected before config loading"),
    )

    result = runner.invoke(app, _arguments(root, gate_report, config, output))

    assert result.exit_code == 2
    assert "LEAD_LAG_OUTPUT_REFUSED [inside_lake]" in result.output
    assert "Traceback" not in result.output
    assert not output.exists()


def test_lead_lag_refuses_every_existing_output_before_loading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, gate_report, config = _input_paths(tmp_path)
    output = tmp_path / "existing-report"
    output.mkdir()
    monkeypatch.setattr(
        cli_module,
        "load_lead_lag_config",
        lambda _path: pytest.fail("existing output must be rejected before config loading"),
    )

    result = runner.invoke(app, _arguments(root, gate_report, config, output))

    assert result.exit_code == 2
    assert "LEAD_LAG_OUTPUT_REFUSED [already_exists]" in result.output
    assert "Traceback" not in result.output
    assert list(output.iterdir()) == []


def test_lead_lag_gate_failure_creates_no_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, gate_report, config_path = _input_paths(tmp_path)
    output = tmp_path / "report"
    config = object()
    monkeypatch.setattr(cli_module, "load_lead_lag_config", lambda _path: config)
    monkeypatch.setattr(
        cli_module,
        "run_bounded_lead_lag_study",
        lambda *_args: (_ for _ in ()).throw(
            ValueError("technical_capture_gate must PASS")
        ),
    )

    result = runner.invoke(app, _arguments(root, gate_report, config_path, output))

    assert result.exit_code == 2
    assert "technical_capture_gate must PASS" in result.output
    assert "Traceback" not in result.output
    assert not output.exists()


def test_lead_lag_command_uses_bounded_pipeline_exclusively(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, gate_report, config_path = _input_paths(tmp_path)
    output = tmp_path / "report"
    config = object()
    calls: list[str] = []

    def load_config(path: Path) -> object:
        assert path == config_path
        calls.append("config")
        return config

    def bounded(
        path: Path,
        gate: Path,
        loaded_config: object,
        target: Path,
    ) -> dict[str, Path]:
        assert path == root
        assert gate == gate_report
        assert loaded_config is config
        assert target == output
        calls.append("bounded_gate_analysis_publication")
        return {"result": output / "result.json"}

    monkeypatch.setattr(cli_module, "load_lead_lag_config", load_config)
    monkeypatch.setattr(cli_module, "run_bounded_lead_lag_study", bounded)
    monkeypatch.setattr(
        cli_module,
        "load_validated_lead_lag_window",
        lambda *_args: pytest.fail("production CLI must not invoke the pandas loader"),
    )
    monkeypatch.setattr(
        cli_module,
        "analyze_lead_lag",
        lambda *_args: pytest.fail("production CLI must not invoke the pandas oracle"),
    )
    monkeypatch.setattr(
        cli_module,
        "write_lead_lag_artifacts",
        lambda *_args: pytest.fail("production CLI must not invoke the v1 writer"),
    )

    result = runner.invoke(app, _arguments(root, gate_report, config_path, output))

    assert result.exit_code == 0
    assert calls == ["config", "bounded_gate_analysis_publication"]
    assert "EVENT_REPLAY_RESEARCH_ONLY" in result.output
    assert "NOT_ADMISSIBLE" in result.output


@dataclass(frozen=True)
class _FakeAnalysis:
    summary: dict[str, object]
    metrics: pd.DataFrame
    bucket_metrics: pd.DataFrame
    events: pd.DataFrame
    controls: pd.DataFrame

    def as_dict(self) -> dict[str, object]:
        return {
            "summary": self.summary,
            "metrics": self.metrics.to_dict(orient="records"),
            "bucket_metrics": self.bucket_metrics.to_dict(orient="records"),
            "events": self.events.to_dict(orient="records"),
            "controls": self.controls.to_dict(orient="records"),
        }


def test_artifact_writer_binds_every_table_and_preserves_all_metric_rows(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    output = tmp_path / "report"
    start = datetime(2026, 8, 14, tzinfo=UTC)
    analysis = _FakeAnalysis(
        summary={"event_count": 1, "warnings": ["Synthetic fixture for artifact testing only."]},
        metrics=pd.DataFrame(
            [
                {"asset": "BTC", "horizon_ms": 50, "variant": "raw"},
                {"asset": "BTC", "horizon_ms": 50, "variant": "momentum_controlled"},
            ]
        ),
        bucket_metrics=pd.DataFrame(
            [{"asset": "BTC", "horizon_ms": 50, "variant": "raw", "bucket": "00:00"}]
        ),
        events=pd.DataFrame(
            [
                {
                    "asset": "BTC",
                    "signal_time": start,
                    "horizon_ms": 50,
                    "response_bps": 0.2,
                    "lineage": {"frame": 1},
                }
            ]
        ),
        controls=pd.DataFrame([{"control": "future_data", "status": "PASS"}]),
    )
    window = SimpleNamespace(
        root=root,
        gate_report_sha256="1" * 64,
        canonical_gate_sha256="2" * 64,
        manifest_fingerprint="3" * 64,
        selected_manifest_entries=({"relative_data_path": "venue=synthetic/part.parquet"},),
        start=start,
        end=start + timedelta(hours=6),
        assets=("BTC", "ETH"),
    )
    config = {
        "config_hash": "4" * 64,
        "horizons_ms": [50, 100, 250, 500, 1000, 2000, 5000],
        "execution_scenarios": [{"calibration_status": "UNCALIBRATED"}],
    }
    original_events = analysis.events.copy(deep=True)

    artifacts = write_lead_lag_artifacts(analysis, window, config, output)

    assert set(artifacts) == {"result", "report", "metrics", "controls", "events"}
    assert all(path.is_file() for path in artifacts.values())
    with artifacts["metrics"].open(encoding="utf-8", newline="") as handle:
        metric_rows = list(csv.DictReader(handle))
    assert len(metric_rows) == 3
    assert {row["metric_scope"] for row in metric_rows} == {"aggregate", "bucket"}
    assert {row["variant"] for row in metric_rows} == {"raw", "momentum_controlled"}
    assert {row["config_sha256"] for row in metric_rows} == {"4" * 64}
    assert {row["gate_report_sha256"] for row in metric_rows} == {"1" * 64}
    assert {row["manifest_fingerprint"] for row in metric_rows} == {"3" * 64}

    controls = pd.read_csv(artifacts["controls"])
    events = pd.read_parquet(artifacts["events"])
    pd.testing.assert_frame_equal(analysis.events, original_events)
    assert "research_status" not in analysis.events
    assert analysis.events.at[0, "lineage"] == {"frame": 1}
    assert events.at[0, "lineage"] == '{"frame":1}'
    for frame in (controls, events):
        assert set(frame["research_status"]) == {"EVENT_REPLAY_RESEARCH_ONLY"}
        assert set(frame["source_time_lead_status"]) == {"NOT_ADMISSIBLE"}
        assert set(frame["canonical_gate_sha256"]) == {"2" * 64}

    result_bytes = artifacts["result"].read_bytes()
    result_payload = json.loads(result_bytes)
    assert result_bytes.endswith(b"\n")
    assert result_payload["provenance"]["config_sha256"] == "4" * 64
    assert result_payload["artifact_rows"] == {
        "bucket_metrics": 1,
        "controls": 1,
        "events": 1,
        "metrics": 2,
    }
    assert result_payload["source_time_lead_status"] == "NOT_ADMISSIBLE"
    report = artifacts["report"].read_text(encoding="utf-8")
    assert "six-hour capture is not evidence" in report
    assert "no symmetric Hyperliquid clock calibration" in report
    assert "UNCALIBRATED" in report
