from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import cast
from uuid import uuid4

import pandas as pd

RESEARCH_STATUS = "EVENT_REPLAY_RESEARCH_ONLY"
SOURCE_TIME_LEAD_STATUS = "NOT_ADMISSIBLE"
SIX_HOUR_LIMIT = (
    "A six-hour capture is not evidence of long-run stability or profitability."
)
SOURCE_TIME_LIMIT = (
    "Source-time lead is NOT_ADMISSIBLE: no symmetric Hyperliquid clock calibration."
)
UNCALIBRATED_LIMIT = (
    "Execution scenarios are UNCALIBRATED research inputs, not verified current fees, "
    "latency, slippage, queue position, or fill behavior."
)


def _field(value: object, name: str, default: object = None) -> object:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _json_ready(value: object) -> object:
    serializer = getattr(value, "as_dict", None)
    if callable(serializer):
        return _json_ready(serializer())
    serializer = getattr(value, "to_dict", None)
    if callable(serializer) and not isinstance(value, pd.DataFrame):
        return _json_ready(serializer())
    if is_dataclass(value) and not isinstance(value, type):
        return _json_ready(asdict(value))
    if isinstance(value, pd.DataFrame):
        return _json_ready(value.to_dict(orient="records"))
    if isinstance(value, pd.Series):
        return _json_ready(value.to_list())
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return value.isoformat()
    if isinstance(value, Enum):
        return _json_ready(value.value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if value is pd.NA or value is pd.NaT:
        return None
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    item = getattr(value, "item", None)
    if callable(item):
        return _json_ready(item())
    raise TypeError(f"unsupported artifact value: {type(value).__name__}")


def _canonical_json(value: object) -> str:
    return (
        json.dumps(
            _json_ready(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )


def _sha256_text(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _require_sha256(value: object, label: str) -> str:
    text = str(value) if value is not None else ""
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text.lower()):
        raise ValueError(f"{label} must be a lowercase hexadecimal SHA-256 digest")
    return text.lower()


def _config_sha256(config: object) -> str:
    for name in (
        "config_hash",
        "config_sha256",
        "canonical_sha256",
        "sha256",
        "fingerprint",
    ):
        candidate = _field(config, name)
        if candidate is not None:
            return _require_sha256(candidate, name)
    return _sha256_text(config)


def _rows(value: object, *, label: str) -> list[dict[str, object]]:
    if value is None:
        return []
    if isinstance(value, pd.DataFrame):
        raw_rows: Sequence[object] = cast(
            Sequence[object], value.to_dict(orient="records")
        )
    else:
        to_pylist = getattr(value, "to_pylist", None)
        if callable(to_pylist):
            converted = to_pylist()
            if not isinstance(converted, Sequence) or isinstance(
                converted, (str, bytes, bytearray)
            ):
                raise TypeError(f"{label}.to_pylist() must return a sequence")
            raw_rows = cast(Sequence[object], converted)
        elif isinstance(value, Mapping) or (
            is_dataclass(value) and not isinstance(value, type)
        ):
            raw_rows = [value]
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            raw_rows = cast(Sequence[object], value)
        else:
            raw_rows = [value]

    rows: list[dict[str, object]] = []
    for position, row in enumerate(raw_rows):
        serialized = _json_ready(row)
        if not isinstance(serialized, Mapping):
            raise TypeError(f"{label}[{position}] must serialize to an object")
        rows.append({str(key): item for key, item in serialized.items()})
    return rows


def _analysis_payload(analysis: object) -> dict[str, object]:
    serialized = _json_ready(analysis)
    if not isinstance(serialized, Mapping):
        raise TypeError("lead-lag analysis must serialize to an object")
    return {str(key): value for key, value in serialized.items()}


def _deduplicated_warnings(analysis_payload: Mapping[str, object]) -> list[str]:
    warnings: list[str] = []
    for candidate in (
        analysis_payload.get("warnings"),
        _field(analysis_payload.get("summary", {}), "warnings"),
    ):
        if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes, bytearray)):
            warnings.extend(str(item) for item in candidate)
        elif isinstance(candidate, str):
            warnings.append(candidate)
    warnings.extend((SOURCE_TIME_LIMIT, UNCALIBRATED_LIMIT, SIX_HOUR_LIMIT))
    return list(dict.fromkeys(warnings))


def _selected_manifest_entries(window: object) -> list[object]:
    raw_entries = _field(window, "selected_manifest_entries")
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, (str, bytes, bytearray)):
        raise ValueError("validated lead-lag window must expose selected_manifest_entries")
    entries = _json_ready(raw_entries)
    if not isinstance(entries, list) or not entries:
        raise ValueError("validated lead-lag window must bind at least one manifest entry")
    return entries


def _provenance(window: object, config: object) -> dict[str, object]:
    entries = _selected_manifest_entries(window)
    assets = _json_ready(_field(window, "assets"))
    if not isinstance(assets, list) or not assets:
        raise ValueError("validated lead-lag window must expose non-empty assets")
    start = _json_ready(_field(window, "start"))
    end = _json_ready(_field(window, "end"))
    if not isinstance(start, str) or not isinstance(end, str):
        raise ValueError("validated lead-lag window must expose start and end timestamps")
    return {
        "config_sha256": _config_sha256(config),
        "gate_report_sha256": _require_sha256(
            _field(window, "gate_report_sha256"), "gate_report_sha256"
        ),
        "canonical_gate_sha256": _require_sha256(
            _field(window, "canonical_gate_sha256"), "canonical_gate_sha256"
        ),
        "manifest_fingerprint": _require_sha256(
            _field(window, "manifest_fingerprint"), "manifest_fingerprint"
        ),
        "selected_manifest_entries": entries,
        "selected_manifest_entry_count": len(entries),
        "assets": assets,
        "start": start,
        "end": end,
    }


def _row_bindings(provenance: Mapping[str, object]) -> dict[str, str]:
    return {
        "research_status": RESEARCH_STATUS,
        "source_time_lead_status": SOURCE_TIME_LEAD_STATUS,
        "config_sha256": str(provenance["config_sha256"]),
        "gate_report_sha256": str(provenance["gate_report_sha256"]),
        "canonical_gate_sha256": str(provenance["canonical_gate_sha256"]),
        "manifest_fingerprint": str(provenance["manifest_fingerprint"]),
    }


def _bind_rows(
    rows: Sequence[Mapping[str, object]],
    bindings: Mapping[str, str],
) -> list[dict[str, object]]:
    bound: list[dict[str, object]] = []
    for position, row in enumerate(rows):
        normalized = {str(key): _json_ready(item) for key, item in row.items()}
        for key, value in bindings.items():
            if key in normalized and normalized[key] != value:
                raise ValueError(f"row {position} conflicts with evidence binding {key}")
            normalized[key] = value
        bound.append(normalized)
    return bound


def _csv_cell(value: object) -> object:
    serialized = _json_ready(value)
    if serialized is None:
        return ""
    if isinstance(serialized, bool):
        return "true" if serialized else "false"
    if isinstance(serialized, float):
        return format(serialized, ".17g")
    if isinstance(serialized, (str, int)):
        return serialized
    return json.dumps(
        serialized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


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


def _event_frame(events: object, bindings: Mapping[str, str]) -> pd.DataFrame:
    if isinstance(events, pd.DataFrame):
        frame = events.copy(deep=False)
    else:
        to_pandas = getattr(events, "to_pandas", None)
        if callable(to_pandas):
            frame = to_pandas()
            if not isinstance(frame, pd.DataFrame):
                raise TypeError("events.to_pandas() must return a pandas DataFrame")
            frame = frame.copy(deep=False)
        else:
            frame = pd.DataFrame(_rows(events, label="events"))

    for column in frame.columns:
        if frame[column].dtype != object:
            continue
        frame[column] = frame[column].map(
            lambda value: _csv_cell(value)
            if isinstance(value, (Mapping, Sequence, Path, Enum, Decimal))
            and not isinstance(value, (str, bytes, bytearray))
            else value
        )
    for key, value in bindings.items():
        if key in frame.columns:
            existing = frame[key].drop_duplicates().tolist()
            if existing != [value]:
                raise ValueError(f"events conflict with evidence binding {key}")
        else:
            frame[key] = value
    return frame


def _write_text(path: Path, content: str) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def _summary_lines(summary: object) -> list[str]:
    serialized = _json_ready(summary)
    if not isinstance(serialized, Mapping):
        return []
    lines: list[str] = []
    for key in sorted(serialized):
        value = serialized[key]
        if value is None or isinstance(value, (str, int, float, bool)):
            lines.append(f"- `{key}`: `{value}`")
    return lines


def _report_markdown(
    *,
    analysis: object,
    provenance: Mapping[str, object],
    warnings: Sequence[str],
    metric_count: int,
    bucket_metric_count: int,
    event_count: int,
    control_count: int,
) -> str:
    summary = _field(analysis, "summary", {})
    summary_lines = _summary_lines(summary) or ["- No scalar summary fields were emitted."]
    assets = provenance["assets"]
    if not isinstance(assets, Sequence) or isinstance(assets, (str, bytes, bytearray)):
        raise TypeError("provenance assets must be a sequence")
    asset_text = ",".join(str(item) for item in assets)
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
            f"- Config SHA-256: `{provenance['config_sha256']}`",
            f"- Gate report SHA-256: `{provenance['gate_report_sha256']}`",
            f"- Canonical gate SHA-256: `{provenance['canonical_gate_sha256']}`",
            f"- Manifest fingerprint: `{provenance['manifest_fingerprint']}`",
            f"- Selected manifest entries: `{provenance['selected_manifest_entry_count']}`",
            f"- Assets: `{asset_text}`",
            f"- Window: `{provenance['start']}` to `{provenance['end']}`",
            "",
            "## Output coverage",
            "",
            f"- Aggregate metric rows: `{metric_count}`",
            f"- Bucket metric rows: `{bucket_metric_count}`",
            f"- Event rows: `{event_count}`",
            f"- Control rows: `{control_count}`",
            f"- Source-time lead: **{SOURCE_TIME_LEAD_STATUS}**",
            "",
            "## Summary",
            "",
            *summary_lines,
            "",
            "## Interpretation limits",
            "",
            *(f"- {warning}" for warning in warnings),
            "",
            "## Files",
            "",
            "- `result.json`: canonical analysis, preregistration, warnings, and evidence binding",
            "- `metrics.csv`: every aggregate variant and bucket row",
            "- `controls.csv`: negative controls and diagnostic controls",
            "- `events.parquet`: event-level causal replay output",
            "",
        )
    )


def _inside_root(root: Path, destination: Path) -> bool:
    resolved_root = root.resolve(strict=False)
    resolved_destination = destination.resolve(strict=False)
    return resolved_destination == resolved_root or resolved_destination.is_relative_to(resolved_root)


def _validate_destination(window: object, output: Path) -> None:
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"lead-lag output directory must not already exist: {output}")
    root = _field(window, "root")
    if isinstance(root, (str, Path)) and _inside_root(Path(root), output):
        raise ValueError(f"lead-lag output must be outside the immutable lake: {output}")


def write_lead_lag_artifacts(
    analysis: object,
    window: object,
    config: object,
    output: Path,
) -> dict[str, Path]:
    """Publish a complete evidence-bound report directory without mutating the lake."""

    _validate_destination(window, output)
    provenance = _provenance(window, config)
    bindings = _row_bindings(provenance)
    analysis_payload = _analysis_payload(analysis)
    warnings = _deduplicated_warnings(analysis_payload)

    metric_rows = _rows(_field(analysis, "metrics", ()), label="metrics")
    bucket_rows = _rows(_field(analysis, "bucket_metrics", ()), label="bucket_metrics")
    combined_metrics = [
        {**row, "metric_scope": "aggregate"} for row in metric_rows
    ] + [{**row, "metric_scope": "bucket"} for row in bucket_rows]
    bound_metrics = _bind_rows(combined_metrics, bindings)
    control_rows = _bind_rows(
        _rows(_field(analysis, "controls", ()), label="controls"), bindings
    )
    event_frame = _event_frame(_field(analysis, "events", ()), bindings)

    result_payload: dict[str, object] = {
        "artifact_schema_version": 1,
        "research_status": RESEARCH_STATUS,
        "source_time_lead_status": SOURCE_TIME_LEAD_STATUS,
        "warnings": warnings,
        "provenance": provenance,
        "configuration": _json_ready(config),
        "analysis": analysis_payload,
        "artifact_rows": {
            "metrics": len(metric_rows),
            "bucket_metrics": len(bucket_rows),
            "events": len(event_frame.index),
            "controls": len(control_rows),
        },
    }
    report = _report_markdown(
        analysis=analysis,
        provenance=provenance,
        warnings=warnings,
        metric_count=len(metric_rows),
        bucket_metric_count=len(bucket_rows),
        event_count=len(event_frame.index),
        control_count=len(control_rows),
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{uuid4().hex}.tmp")
    temporary.mkdir(parents=False, exist_ok=False)
    try:
        result_path = temporary / "result.json"
        report_path = temporary / "report.md"
        metrics_path = temporary / "metrics.csv"
        controls_path = temporary / "controls.csv"
        events_path = temporary / "events.parquet"
        _write_csv(metrics_path, bound_metrics)
        _write_csv(controls_path, control_rows)
        event_frame.to_parquet(events_path, index=False)
        _write_text(result_path, _canonical_json(result_payload))
        _write_text(report_path, report)
        temporary.rename(output)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise

    return {
        "result": output / "result.json",
        "report": output / "report.md",
        "metrics": output / "metrics.csv",
        "controls": output / "controls.csv",
        "events": output / "events.parquet",
    }


__all__ = [
    "RESEARCH_STATUS",
    "SOURCE_TIME_LEAD_STATUS",
    "write_lead_lag_artifacts",
]
