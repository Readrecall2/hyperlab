from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from hyperlab.analysis.lead_lag import LeadLagConfig
from hyperlab.analysis.streaming_reporting import (
    StreamingEventArtifact,
    StreamingLeadLagAnalysis,
    finalize_streaming_publication,
    write_streaming_metadata_artifacts,
)


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _window(root: Path, selected: Path) -> SimpleNamespace:
    start = datetime(2026, 8, 15, tzinfo=UTC)
    return SimpleNamespace(
        root=root,
        start=start,
        end=start + timedelta(hours=6),
        assets=("BTC", "ETH"),
        gate_report_sha256="1" * 64,
        semantic_gate_sha256="2" * 64,
        semantic_gate_canonicalizer_version="phase10_semantic_gate_payload_v1",
        excluded_json_pointers=("/observability",),
        manifest_fingerprint="3" * 64,
        selected_manifests_sha256=_hash(selected),
        selected_manifest_count=1,
    )


def test_v2_metadata_binds_raw_semantic_and_manifest_evidence(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    staging = tmp_path / ".report.phase10-streaming.tmp"
    staging.mkdir()
    selected = staging / "selected_manifests.jsonl"
    selected.write_text('{"relative_data_path":"part.parquet"}\n', encoding="utf-8")
    events = staging / "events.parquet"
    events.write_bytes(b"deterministic-test-parquet")
    window = _window(root, selected)
    config = LeadLagConfig(randomization_resamples=19)
    analysis = StreamingLeadLagAnalysis(
        summary={"warnings": [], "persisted_event_row_count": 1},
        metrics=pd.DataFrame([{"analysis_kind": "information", "asset": "BTC"}]),
        bucket_metrics=pd.DataFrame(
            [{"analysis_kind": "information", "asset": "BTC", "time_bucket": window.start}]
        ),
        controls=pd.DataFrame([{"control_type": "negative_lag", "asset": "BTC"}]),
        event_row_count=1,
    )
    artifact = StreamingEventArtifact(
        row_count=1,
        size_bytes=events.stat().st_size,
        logical_sha256="4" * 64,
        file_sha256=_hash(events),
    )

    paths = write_streaming_metadata_artifacts(
        staging=staging,
        analysis=analysis,
        window=window,
        config=config,
        event_artifact=artifact,
        resource_observability={
            "semantic": False,
            "chunks_processed": 1,
            "elapsed_seconds_by_phase": {"analysis": 0.25},
        },
    )

    assert set(paths) == {
        "result",
        "report",
        "metrics",
        "controls",
        "events",
        "selected_manifests",
        "observability",
    }
    result = json.loads(paths["result"].read_bytes())
    assert result["artifact_schema_version"] == 2
    assert result["provenance"]["gate_report_sha256"] == "1" * 64
    assert result["provenance"]["semantic_gate_sha256"] == "2" * 64
    assert result["provenance"]["semantic_gate_excluded_json_pointers"] == [
        "/observability"
    ]
    assert len(result["analysis_semantic_sha256"]) == 64
    assert result["analysis"]["metrics"] == [
        {"analysis_kind": "information", "asset": "BTC"}
    ]
    assert result["analysis"]["bucket_metrics"] == [
        {
            "analysis_kind": "information",
            "asset": "BTC",
            "time_bucket": "2026-08-15T00:00:00Z",
        }
    ]
    assert result["analysis"]["controls"] == [
        {"asset": "BTC", "control_type": "negative_lag"}
    ]
    assert "elapsed_seconds_by_phase" not in result["resource_observability"]
    runtime = json.loads(paths["observability"].read_bytes())
    assert runtime["semantic"] is False
    assert runtime["resource_observability"]["elapsed_seconds_by_phase"] == {
        "analysis": 0.25
    }
    with paths["metrics"].open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert {row["metric_scope"] for row in rows} == {"aggregate", "bucket"}
    assert {row["semantic_gate_sha256"] for row in rows} == {"2" * 64}
    assert {row["selected_manifests_sha256"] for row in rows} == {
        window.selected_manifests_sha256
    }
    by_scope = {row["metric_scope"]: row for row in rows}
    assert by_scope["aggregate"]["analysis_kind"] == "information"
    assert by_scope["aggregate"]["time_bucket"] == ""
    assert by_scope["bucket"]["time_bucket"] == "2026-08-15T00:00:00Z"
    with paths["controls"].open(encoding="utf-8", newline="") as handle:
        controls = list(csv.DictReader(handle))
    assert [(row["control_type"], row["asset"]) for row in controls] == [
        ("negative_lag", "BTC")
    ]
    report = paths["report"].read_text(encoding="utf-8")
    assert "- Aggregate metric rows: `1`" in report
    assert "- Bucket metric rows: `1`" in report
    assert "- Event rows: `1`" in report
    assert "- Control rows: `1`" in report


def test_failed_final_revalidation_leaves_no_completed_report(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    staging = tmp_path / ".report.phase10-streaming.tmp"
    staging.mkdir()
    for name in (
        "result.json",
        "report.md",
        "metrics.csv",
        "controls.csv",
        "events.parquet",
        "selected_manifests.jsonl",
        "observability.json",
    ):
        (staging / name).write_bytes(b"fixture")
    output = tmp_path / "report"

    with pytest.raises(RuntimeError, match="gate changed"):
        finalize_streaming_publication(
            staging=staging,
            output=output,
            root=root,
            verify_inputs_unchanged=lambda: (_ for _ in ()).throw(
                RuntimeError("gate changed")
            ),
        )

    assert not output.exists()
    assert staging.exists()


def test_runtime_timings_do_not_change_the_deterministic_analysis_hash(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    config = LeadLagConfig(randomization_resamples=19)
    analysis = StreamingLeadLagAnalysis(
        summary={"warnings": [], "persisted_event_row_count": 1},
        metrics=pd.DataFrame([{"analysis_kind": "information", "asset": "BTC"}]),
        bucket_metrics=pd.DataFrame(),
        controls=pd.DataFrame(),
        event_row_count=1,
    )
    hashes: list[str] = []
    deterministic_files: list[tuple[bytes, bytes, bytes]] = []
    result_bytes: list[bytes] = []
    for index, elapsed in enumerate((0.1, 9.9)):
        staging = tmp_path / f"staging-{index}"
        staging.mkdir()
        selected = staging / "selected_manifests.jsonl"
        selected.write_text('{"relative_data_path":"part.parquet"}\n', encoding="utf-8")
        events = staging / "events.parquet"
        events.write_bytes(b"deterministic-test-parquet")
        window = _window(root, selected)
        artifact = StreamingEventArtifact(
            row_count=1,
            size_bytes=events.stat().st_size,
            logical_sha256="4" * 64,
            file_sha256=_hash(events),
        )
        paths = write_streaming_metadata_artifacts(
            staging=staging,
            analysis=analysis,
            window=window,
            config=config,
            event_artifact=artifact,
            resource_observability={
                "semantic": False,
                "chunks_processed": 1,
                "disk_preflight": {
                    "available_bytes": int(elapsed * 1_000),
                    "projected_remaining_bytes": int(elapsed * 100),
                    "projected_required_bytes": 123,
                },
                "source": {
                    "phase_timings_seconds": {"scan": elapsed},
                    "source_spool_preflight": {
                        "available_bytes": int(elapsed * 2_000),
                        "projected_remaining_bytes": int(elapsed * 200),
                        "estimated_source_spool_bytes": 456,
                    },
                },
                "elapsed_seconds_by_phase": {"analysis": elapsed},
            },
        )
        payload = json.loads(paths["result"].read_bytes())
        hashes.append(payload["analysis_semantic_sha256"])
        deterministic_files.append(
            (
                paths["metrics"].read_bytes(),
                paths["controls"].read_bytes(),
                paths["report"].read_bytes(),
            )
        )
        result_bytes.append(paths["result"].read_bytes())

    assert hashes[0] == hashes[1]
    assert deterministic_files[0] == deterministic_files[1]
    assert result_bytes[0] == result_bytes[1]
    assert (
        (tmp_path / "staging-0" / "observability.json").read_bytes()
        != (tmp_path / "staging-1" / "observability.json").read_bytes()
    )
