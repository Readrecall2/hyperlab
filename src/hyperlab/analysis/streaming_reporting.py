from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import shutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TypeAlias, cast

import numpy as np
import pandas as pd

from hyperlab.analysis.lead_lag import LeadLagConfig
from hyperlab.analysis.reporting import (
    RESEARCH_STATUS,
    SIX_HOUR_LIMIT,
    SOURCE_TIME_LEAD_STATUS,
    SOURCE_TIME_LIMIT,
    UNCALIBRATED_LIMIT,
)

ARTIFACT_SCHEMA_VERSION = 2

JsonValue: TypeAlias = (
    bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"] | None
)


class StreamingPublicationError(ValueError):
    """Raised when a bounded report cannot be published atomically."""


@dataclass(frozen=True, slots=True)
class StreamingLeadLagAnalysis:
    summary: Mapping[str, object]
    metrics: pd.DataFrame
    bucket_metrics: pd.DataFrame
    controls: pd.DataFrame
    event_row_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "summary": _json_ready(dict(self.summary)),
            "metrics": _frame_records(self.metrics),
            "bucket_metrics": _frame_records(self.bucket_metrics),
            "controls": _frame_records(self.controls),
            "event_row_count": self.event_row_count,
        }


@dataclass(frozen=True, slots=True)
class StreamingEventArtifact:
    row_count: int
    size_bytes: int
    logical_sha256: str
    file_sha256: str


def _json_ready(value: object) -> JsonValue:
    serializer = getattr(value, "as_dict", None)
    if callable(serializer):
        return _json_ready(serializer())
    if is_dataclass(value) and not isinstance(value, type):
        return _json_ready(asdict(value))
    if isinstance(value, pd.DataFrame):
        return cast(JsonValue, _frame_records(value))
    if isinstance(value, pd.Series):
        return _json_ready(value.to_list())
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Enum):
        return _json_ready(value.value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, str):
        return value
    if isinstance(value, (pd.Timestamp, datetime, date)):
        timestamp = value.isoformat()
        return timestamp.replace("+00:00", "Z")
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        number = float(value)
        return number if math.isfinite(number) else None
    item = getattr(value, "item", None)
    if callable(item):
        return _json_ready(item())
    raise TypeError(f"unsupported artifact value: {type(value).__name__}")


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            _json_ready(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _sha256_file(path: Path, *, block_bytes: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(block_bytes):
            digest.update(block)
    return digest.hexdigest()


def _require_sha256(value: object, label: str) -> str:
    text = str(value).lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise StreamingPublicationError(f"{label} must be lowercase SHA-256 hex")
    return text


def _field(value: object, name: str, default: object = None) -> object:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _sequence(value: object, *, label: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise StreamingPublicationError(f"{label} must be a sequence")
    return cast(Sequence[object], value)


def _frame_records(frame: pd.DataFrame) -> list[dict[str, JsonValue]]:
    result: list[dict[str, JsonValue]] = []
    for row in frame.to_dict(orient="records"):
        result.append({str(key): _json_ready(item) for key, item in row.items()})
    return result


def evidence_bindings(window: object, config: LeadLagConfig) -> dict[str, str]:
    pointers = _field(window, "excluded_json_pointers")
    if not isinstance(pointers, Sequence) or isinstance(
        pointers, (str, bytes, bytearray)
    ):
        raise StreamingPublicationError("window must expose excluded_json_pointers")
    return {
        "artifact_schema_version": str(ARTIFACT_SCHEMA_VERSION),
        "streaming_resource_model_version": config.streaming_resource_model_version,
        "research_status": RESEARCH_STATUS,
        "source_time_lead_status": SOURCE_TIME_LEAD_STATUS,
        "config_sha256": _require_sha256(config.config_hash, "config_sha256"),
        "gate_report_sha256": _require_sha256(
            _field(window, "gate_report_sha256"), "gate_report_sha256"
        ),
        "semantic_gate_sha256": _require_sha256(
            _field(window, "semantic_gate_sha256"), "semantic_gate_sha256"
        ),
        "semantic_gate_canonicalizer_version": str(
            _field(window, "semantic_gate_canonicalizer_version")
        ),
        "semantic_gate_excluded_json_pointers": json.dumps(
            [str(item) for item in pointers],
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ),
        "manifest_fingerprint": _require_sha256(
            _field(window, "manifest_fingerprint"), "manifest_fingerprint"
        ),
        "selected_manifests_sha256": _require_sha256(
            _field(window, "selected_manifests_sha256"),
            "selected_manifests_sha256",
        ),
        "selected_manifest_count": str(
            int(str(_field(window, "selected_manifest_count")))
        ),
    }


def _bind_rows(
    rows: Sequence[Mapping[str, object]], bindings: Mapping[str, str]
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for position, row in enumerate(rows):
        bound = {str(key): _json_ready(item) for key, item in row.items()}
        for key, value in bindings.items():
            existing = bound.get(key)
            if existing is not None and str(existing) != value:
                raise StreamingPublicationError(
                    f"row {position} conflicts with evidence binding {key}"
                )
            bound[key] = value
        result.append(cast(dict[str, object], bound))
    return result


def _csv_cell(value: object) -> object:
    normalized = _json_ready(value)
    if normalized is None:
        return ""
    if isinstance(normalized, bool):
        return "true" if normalized else "false"
    if isinstance(normalized, float):
        return format(normalized, ".17g")
    if isinstance(normalized, (str, int)):
        return normalized
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _write_bytes(path: Path, payload: bytes) -> None:
    with path.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    fieldnames = sorted({str(key) for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        if fieldnames:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({key: _csv_cell(row.get(key)) for key in fieldnames})
        handle.flush()
        os.fsync(handle.fileno())


def _semantic_result_payload(payload: Mapping[str, object]) -> dict[str, object]:
    normalized = _json_ready(payload)
    if not isinstance(normalized, dict):
        raise StreamingPublicationError("result payload must be an object")
    normalized.pop("analysis_semantic_sha256", None)
    observability = normalized.get("resource_observability")
    if isinstance(observability, dict):
        observability.pop("elapsed_seconds_by_phase", None)
        source = observability.get("source")
        if isinstance(source, dict):
            source.pop("phase_timings_seconds", None)
    return cast(dict[str, object], normalized)


def _deterministic_observability(value: Mapping[str, object]) -> dict[str, object]:
    normalized = _json_ready(value)
    if not isinstance(normalized, dict):
        raise StreamingPublicationError("resource observability must be an object")
    normalized.pop("elapsed_seconds_by_phase", None)
    disk_preflight = normalized.get("disk_preflight")
    if isinstance(disk_preflight, dict):
        disk_preflight.pop("available_bytes", None)
        disk_preflight.pop("projected_remaining_bytes", None)
    source = normalized.get("source")
    if isinstance(source, dict):
        source.pop("phase_timings_seconds", None)
        source_preflight = source.get("source_spool_preflight")
        if isinstance(source_preflight, dict):
            source_preflight.pop("available_bytes", None)
            source_preflight.pop("projected_remaining_bytes", None)
    return cast(dict[str, object], normalized)


def _summary_lines(summary: Mapping[str, object]) -> list[str]:
    lines: list[str] = []
    normalized = _json_ready(summary)
    if not isinstance(normalized, dict):
        return lines
    for key in sorted(normalized):
        value = normalized[key]
        if value is None or isinstance(value, (str, int, float, bool)):
            lines.append(f"- `{key}`: `{value}`")
    return lines


def _markdown(
    *,
    analysis: StreamingLeadLagAnalysis,
    window: object,
    bindings: Mapping[str, str],
    observability: Mapping[str, object],
) -> str:
    warnings = list(
        dict.fromkeys(
            [
                *(
                    str(item)
                    for item in _sequence(
                        analysis.summary.get("warnings", ()),
                        label="analysis warnings",
                    )
                ),
                SOURCE_TIME_LIMIT,
                UNCALIBRATED_LIMIT,
                SIX_HOUR_LIMIT,
            ]
        )
    )
    assets = ",".join(
        str(item)
        for item in _sequence(_field(window, "assets", ()), label="window assets")
    )
    return "\n".join(
        (
            "# Phase 10 lead-lag event replay",
            "",
            f"Status: **{RESEARCH_STATUS}**",
            "",
            "This is an offline, read-only event study. It does not place or simulate a real order route.",
            "",
            "## Evidence binding",
            "",
            f"- Artifact schema: `{ARTIFACT_SCHEMA_VERSION}`",
            f"- Streaming resource model: `{bindings['streaming_resource_model_version']}`",
            f"- Config SHA-256: `{bindings['config_sha256']}`",
            f"- Exact gate-report SHA-256: `{bindings['gate_report_sha256']}`",
            f"- Semantic gate SHA-256: `{bindings['semantic_gate_sha256']}`",
            f"- Gate canonicalizer: `{bindings['semantic_gate_canonicalizer_version']}`",
            f"- Gate semantic exclusions: `{bindings['semantic_gate_excluded_json_pointers']}`",
            f"- Manifest fingerprint: `{bindings['manifest_fingerprint']}`",
            f"- Selected-manifest JSONL SHA-256: `{bindings['selected_manifests_sha256']}`",
            f"- Selected manifests: `{bindings['selected_manifest_count']}`",
            f"- Assets: `{assets}`",
            f"- Window: `{_json_ready(_field(window, 'start'))}` to `{_json_ready(_field(window, 'end'))}`",
            "",
            "## Output coverage",
            "",
            f"- Aggregate metric rows: `{len(analysis.metrics)}`",
            f"- Bucket metric rows: `{len(analysis.bucket_metrics)}`",
            f"- Event rows: `{analysis.event_row_count}`",
            f"- Control rows: `{len(analysis.controls)}`",
            f"- Source-time lead: **{SOURCE_TIME_LEAD_STATUS}**",
            "",
            "## Summary",
            "",
            *(_summary_lines(analysis.summary) or ["- No scalar summary fields were emitted."]),
            "",
            "## Bounded resource observability",
            "",
            *(
                f"- `{key}`: `{value}`"
                for key, value in sorted(observability.items())
                if value is None or isinstance(value, (str, int, float, bool))
            ),
            "",
            "## Interpretation limits",
            "",
            *(f"- {warning}" for warning in warnings),
            "",
            "## Files",
            "",
            "- `result.json`: canonical analysis, resource metadata, and evidence binding",
            "- `metrics.csv`: every aggregate and UTC-bucket metric cell",
            "- `controls.csv`: randomized, negative-lag, and reverse controls",
            "- `events.parquet`: canonically ordered causal event rows",
            "- `selected_manifests.jsonl`: streamed immutable input evidence",
            "- `observability.json`: non-semantic runtime phase timings and resource telemetry",
            "",
        )
    )


def write_streaming_metadata_artifacts(
    *,
    staging: Path,
    analysis: StreamingLeadLagAnalysis,
    window: object,
    config: LeadLagConfig,
    event_artifact: StreamingEventArtifact,
    resource_observability: Mapping[str, object],
) -> dict[str, Path]:
    """Write every small v2 artifact after the bounded event Parquet is durable."""

    staging = Path(staging)
    if not staging.is_dir():
        raise StreamingPublicationError("publication staging directory is missing")
    selected = staging / "selected_manifests.jsonl"
    events = staging / "events.parquet"
    if not selected.is_file() or not events.is_file():
        raise StreamingPublicationError(
            "staging must contain selected_manifests.jsonl and events.parquet"
        )
    if _sha256_file(selected) != str(_field(window, "selected_manifests_sha256")):
        raise StreamingPublicationError("selected-manifest JSONL changed in staging")
    if _sha256_file(events) != event_artifact.file_sha256:
        raise StreamingPublicationError("event Parquet changed before metadata binding")
    if event_artifact.row_count != analysis.event_row_count:
        raise StreamingPublicationError("event Parquet row count differs from analysis")

    bindings = evidence_bindings(window, config)
    metric_rows = _frame_records(analysis.metrics)
    bucket_rows = _frame_records(analysis.bucket_metrics)
    combined_metrics = [
        {**row, "metric_scope": "aggregate"} for row in metric_rows
    ] + [{**row, "metric_scope": "bucket"} for row in bucket_rows]
    bound_metrics = _bind_rows(combined_metrics, bindings)
    bound_controls = _bind_rows(_frame_records(analysis.controls), bindings)

    warnings = list(
        dict.fromkeys(
            [
                *(
                    str(item)
                    for item in _sequence(
                        analysis.summary.get("warnings", ()),
                        label="analysis warnings",
                    )
                ),
                SOURCE_TIME_LIMIT,
                UNCALIBRATED_LIMIT,
                SIX_HOUR_LIMIT,
            ]
        )
    )
    provenance: dict[str, object] = {
        **bindings,
        "semantic_gate_excluded_json_pointers": list(
            _sequence(
                _field(window, "excluded_json_pointers"),
                label="window excluded_json_pointers",
            )
        ),
        "selected_manifest_count": int(bindings["selected_manifest_count"]),
        "assets": list(
            _sequence(_field(window, "assets"), label="window assets")
        ),
        "start": _json_ready(_field(window, "start")),
        "end": _json_ready(_field(window, "end")),
    }
    deterministic_observability = _deterministic_observability(
        resource_observability
    )
    result: dict[str, object] = {
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "streaming_resource_model_version": config.streaming_resource_model_version,
        "research_status": RESEARCH_STATUS,
        "source_time_lead_status": SOURCE_TIME_LEAD_STATUS,
        "warnings": warnings,
        "provenance": provenance,
        "configuration": config.as_dict(),
        "analysis": analysis.as_dict(),
        "resource_observability": deterministic_observability,
        "runtime_observability_artifact": {
            "path": "observability.json",
            "semantic": False,
        },
        "artifact_rows": {
            "metrics": len(metric_rows),
            "bucket_metrics": len(bucket_rows),
            "events": event_artifact.row_count,
            "controls": len(bound_controls),
        },
        "event_artifact": {
            "file_sha256": event_artifact.file_sha256,
            "logical_sha256": event_artifact.logical_sha256,
            "size_bytes": event_artifact.size_bytes,
        },
    }
    semantic_hash = hashlib.sha256(
        _canonical_bytes(_semantic_result_payload(result))
    ).hexdigest()
    result["analysis_semantic_sha256"] = semantic_hash

    metrics = staging / "metrics.csv"
    controls = staging / "controls.csv"
    result_path = staging / "result.json"
    report = staging / "report.md"
    runtime_observability = staging / "observability.json"
    _write_csv(metrics, bound_metrics)
    _write_csv(controls, bound_controls)
    _write_bytes(result_path, _canonical_bytes(result))
    _write_bytes(
        runtime_observability,
        _canonical_bytes(
            {
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
                "semantic": False,
                "resource_observability": resource_observability,
            }
        ),
    )
    _write_bytes(
        report,
        _markdown(
            analysis=analysis,
            window=window,
            bindings=bindings,
            observability=resource_observability,
        ).encode("utf-8"),
    )
    return {
        "result": result_path,
        "report": report,
        "metrics": metrics,
        "controls": controls,
        "events": events,
        "selected_manifests": selected,
        "observability": runtime_observability,
    }


def _inside_root(root: Path, destination: Path) -> bool:
    resolved_root = root.resolve(strict=False)
    resolved_destination = destination.resolve(strict=False)
    return resolved_destination == resolved_root or resolved_destination.is_relative_to(
        resolved_root
    )


def validate_streaming_destination(root: Path, output: Path) -> None:
    if output.exists() or output.is_symlink():
        raise FileExistsError(
            f"lead-lag output directory must not already exist: {output}"
        )
    if _inside_root(Path(root), Path(output)):
        raise StreamingPublicationError(
            f"lead-lag output must be outside the immutable lake: {output}"
        )


def make_streaming_staging(output: Path) -> Path:
    """Create a deterministic hidden sibling only after gate admission."""

    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.with_name(f".{output.name}.phase10-streaming.tmp")
    if staging.exists() or staging.is_symlink():
        raise FileExistsError(f"stale lead-lag staging directory exists: {staging}")
    staging.mkdir(parents=False, exist_ok=False)
    return staging


def cleanup_streaming_staging(staging: Path) -> None:
    shutil.rmtree(staging, ignore_errors=True)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_through_rename(source: Path, destination: Path) -> None:
    if os.name != "nt":
        os.replace(source, destination)
        descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return

    import ctypes
    from ctypes import wintypes

    move_file_ex = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
    move_file_ex.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
    move_file_ex.restype = wintypes.BOOL
    movefile_write_through = 0x00000008
    if not move_file_ex(str(source), str(destination), movefile_write_through):
        error = ctypes.get_last_error()
        raise OSError(error, "MoveFileExW(MOVEFILE_WRITE_THROUGH) failed")


def finalize_streaming_publication(
    *,
    staging: Path,
    output: Path,
    root: Path,
    verify_inputs_unchanged: Callable[[], None],
) -> dict[str, Path]:
    """Revalidate evidence, remove scratch, and publish with one durable rename."""

    validate_streaming_destination(root, output)
    staging = Path(staging)
    scratch = staging / ".scratch"
    required = {
        "result": "result.json",
        "report": "report.md",
        "metrics": "metrics.csv",
        "controls": "controls.csv",
        "events": "events.parquet",
        "selected_manifests": "selected_manifests.jsonl",
        "observability": "observability.json",
    }
    for filename in required.values():
        path = staging / filename
        if not path.is_file():
            raise StreamingPublicationError(f"staging artifact is missing: {filename}")
        with path.open("r+b") as handle:
            handle.flush()
            os.fsync(handle.fileno())
    verify_inputs_unchanged()
    if scratch.exists():
        shutil.rmtree(scratch)
    _fsync_directory(staging)
    _atomic_write_through_rename(staging, output)
    return {name: output / filename for name, filename in required.items()}


__all__ = [
    "ARTIFACT_SCHEMA_VERSION",
    "StreamingEventArtifact",
    "StreamingLeadLagAnalysis",
    "StreamingPublicationError",
    "cleanup_streaming_staging",
    "evidence_bindings",
    "finalize_streaming_publication",
    "make_streaming_staging",
    "validate_streaming_destination",
    "write_streaming_metadata_artifacts",
]
