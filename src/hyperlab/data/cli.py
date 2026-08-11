from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import typer
from rich.console import Console
from rich.table import Table

data_app = typer.Typer(
    name="data",
    help="Valide, inventorie et exporte le lac de données local en lecture seule.",
    no_args_is_help=True,
)
console = Console()


# These small lazy adapters keep startup lightweight and are deliberate seams
# for CLI tests.
def validate_partition(path: Path) -> object:
    from hyperlab.data.lake import validate_partition as implementation

    return implementation(path)


def inventory_partitions(root: Path) -> object:
    from hyperlab.data.lake import inventory_partitions as implementation

    return implementation(root)


def daily_quality_report(root: Path, report_date: date) -> dict[str, object]:
    from hyperlab.data.quality import daily_quality_report as implementation

    return implementation(root, report_date)


def build_catalog(root: Path, database: Path) -> Path:
    from hyperlab.data.catalog import build_catalog as implementation

    return implementation(root, database)


def export_dataset(
    root: Path,
    output: Path,
    *,
    record_type: str | None = None,
    venue: str | None = None,
    asset: str | None = None,
    start: date | None = None,
    end: date | None = None,
    schema_version: int | None = None,
) -> object:
    from hyperlab.data.catalog import export_dataset as implementation

    return implementation(
        root,
        output,
        record_type=record_type,
        venue=venue,
        asset=asset,
        start=start,
        end=end,
        schema_version=schema_version,
        output_format=output.suffix.removeprefix("."),
    )


def _json_ready(value: object) -> object:
    serializer = getattr(value, "as_dict", None)
    if callable(serializer):
        return _json_ready(serializer())
    if is_dataclass(value) and not isinstance(value, type):
        return _json_ready(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return _json_ready(value.value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


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


def _write_report(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(_canonical_json(payload), encoding="utf-8", newline="\n")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _inside_lake(root: Path, destination: Path) -> bool:
    resolved_root = root.resolve(strict=False)
    resolved_destination = destination.resolve(strict=False)
    return resolved_destination == resolved_root or resolved_destination.is_relative_to(resolved_root)


def _require_outside_lake(root: Path, destination: Path, error_code: str) -> None:
    if _inside_lake(root, destination):
        raise ValueError(f"{error_code} [inside_lake] output={destination} root={root}")


def _parse_date(value: str | None, option: str) -> date | None:
    if value is None:
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"INVALID_DATE [{option}] value={value}; expected=YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"INVALID_DATE [{option}] value={value}; expected=YYYY-MM-DD")
    return parsed


def _manifest_payload(manifest: object) -> dict[str, object]:
    payload = _json_ready(manifest)
    if not isinstance(payload, dict):
        raise TypeError(f"partition manifest must serialize to an object, got {type(payload).__name__}")
    return payload


def _manifest_date(payload: Mapping[str, object]) -> str | None:
    partition = payload.get("partition")
    if isinstance(partition, Mapping):
        value = partition.get("date")
        return str(value) if value is not None else None
    value = payload.get("date")
    return str(value) if value is not None else None


def _manifest_sort_key(payload: Mapping[str, object]) -> str:
    relative_path = payload.get("relative_path")
    if relative_path is not None:
        return str(relative_path)
    partition = payload.get("partition")
    if isinstance(partition, Mapping):
        dimensions = (
            partition.get("venue", ""),
            partition.get("date", ""),
            partition.get("asset", ""),
            partition.get("record_type", ""),
            payload.get("data_file", ""),
        )
        return "\0".join(str(value) for value in dimensions)
    return _canonical_json(payload)


def _cross_gap_date(payload: object) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    partition = payload.get("partition")
    if not isinstance(partition, Mapping):
        return None
    value = partition.get("date")
    return None if value is None else str(value)


def _inventory_status(items: list[dict[str, object]], cross_segment_gaps: list[object]) -> str:
    if not items:
        return "missing"
    qualities = {str(item.get("quality", "unobservable")) for item in items}
    if cross_segment_gaps or "degraded" in qualities:
        return "degraded"
    if qualities != {"ok"}:
        return "unobservable"
    return "ok"


def _inventory_payload(inventory: object, selected_date: date | None) -> dict[str, object]:
    serialized = _json_ready(inventory)
    if isinstance(serialized, Mapping):
        raw_items = serialized.get("partitions", [])
        base = {str(key): value for key, value in serialized.items() if key != "partitions"}
    elif isinstance(serialized, list):
        raw_items = serialized
        base = {}
    else:
        raise TypeError(
            f"partition inventory must serialize to an object or array, got {type(serialized).__name__}"
        )
    if not isinstance(raw_items, list):
        raise TypeError("partition inventory field 'partitions' must be an array")
    items = [_manifest_payload(manifest) for manifest in raw_items]
    raw_cross_segment_gaps = base.get("cross_segment_gaps", [])
    if not isinstance(raw_cross_segment_gaps, list):
        raise TypeError("partition inventory field 'cross_segment_gaps' must be an array")
    cross_segment_gaps = list(raw_cross_segment_gaps)
    if selected_date is not None:
        expected = selected_date.isoformat()
        items = [item for item in items if _manifest_date(item) == expected]
        cross_segment_gaps = [gap for gap in cross_segment_gaps if _cross_gap_date(gap) == expected]
    items.sort(key=_manifest_sort_key)
    row_count = 0
    for item in items:
        value = item.get("row_count", 0)
        if isinstance(value, int) and not isinstance(value, bool):
            row_count += value
    return {
        **base,
        "catalog": "catalog.duckdb",
        "cross_segment_gaps": cross_segment_gaps,
        "partition_count": len(items),
        "row_count": row_count,
        "partitions": items,
        "status": _inventory_status(items, cross_segment_gaps),
    }


def _quality_state(payload: Mapping[str, object]) -> str:
    value = payload.get("quality", payload.get("status", "unobservable"))
    state = str(value)
    return state if state in {"ok", "degraded", "unobservable", "missing"} else "unobservable"


def _print_validation_summary(payload: Mapping[str, object], state: str) -> None:
    count = payload.get("partition_count", 0)
    if state == "ok":
        console.print(f"Validation réussie : {count} partition(s).")
    elif state == "degraded":
        console.print(f"Validation terminée avec une qualité dégradée : {count} partition(s).")
    elif state == "unobservable":
        console.print(f"Validation terminée avec une qualité non observable : {count} partition(s).")
    else:
        console.print("Validation terminée sans partition pour la période demandée.")


def _print_error(exc: BaseException) -> None:
    typer.echo(str(exc), err=True)


def _print_inventory_table(payload: Mapping[str, object]) -> None:
    table = Table(title="Inventaire HyperLab")
    for heading in ("Date", "Venue", "Actif", "Type", "Lignes", "Qualité", "Fichier"):
        table.add_column(heading)

    partitions = payload.get("partitions", [])
    if isinstance(partitions, list):
        for item in partitions:
            if not isinstance(item, Mapping):
                continue
            partition = item.get("partition")
            dimensions = partition if isinstance(partition, Mapping) else item
            table.add_row(
                str(dimensions.get("date", "")),
                str(dimensions.get("venue", "")),
                str(dimensions.get("asset", "")),
                str(dimensions.get("record_type", dimensions.get("type", ""))),
                str(item.get("row_count", "")),
                str(item.get("quality", "")),
                str(item.get("data_file", item.get("relative_path", ""))),
            )
    console.print(table)


@data_app.command("validate")
def validate_data(
    root: Annotated[
        Path,
        typer.Argument(exists=True, file_okay=False, dir_okay=True, readable=True),
    ],
    date_value: Annotated[str | None, typer.Option("--date", help="Journée UTC YYYY-MM-DD")] = None,
    report: Annotated[Path | None, typer.Option("--report", help="Rapport JSON canonique")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Sortie JSON canonique")] = False,
) -> None:
    """Valide les hashes, schémas et invariants de chaque partition."""
    try:
        if report is not None:
            _require_outside_lake(root, report, "REPORT_REFUSED")
        selected_date = _parse_date(date_value, "date")
        if selected_date is None:
            payload = _inventory_payload(inventory_partitions(root), None)
        else:
            payload = daily_quality_report(root, selected_date)
        if report is not None:
            _write_report(report, payload)
        state = _quality_state(payload)
        if json_output or (selected_date is not None and state == "missing"):
            typer.echo(_canonical_json(payload), nl=False)
        else:
            _print_validation_summary(payload, state)
        if report is not None and not json_output:
            console.print(f"Rapport : {report}")
        if selected_date is not None and state == "missing":
            _print_error(
                ValueError(
                    "DATA_QUALITY [missing] "
                    f"date={selected_date.isoformat()} partition_count={payload.get('partition_count', 0)}"
                )
            )
            raise typer.Exit(2)
    except (ImportError, OSError, TypeError, ValueError) as exc:
        _print_error(exc)
        raise typer.Exit(2) from None


@data_app.command("inventory")
def inventory_data(
    root: Annotated[
        Path,
        typer.Argument(exists=True, file_okay=False, dir_okay=True, readable=True),
    ],
    date_value: Annotated[str | None, typer.Option("--date", help="Journée UTC YYYY-MM-DD")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Sortie JSON canonique")] = False,
) -> None:
    """Inventorie les partitions sans exclure les actifs délistés."""
    try:
        selected_date = _parse_date(date_value, "date")
        manifests = inventory_partitions(root)
        payload = _inventory_payload(manifests, selected_date)
        build_catalog(root, root / "catalog.duckdb")
        if json_output:
            typer.echo(_canonical_json(payload), nl=False)
        else:
            _print_inventory_table(payload)
    except (ImportError, OSError, TypeError, ValueError) as exc:
        _print_error(exc)
        raise typer.Exit(2) from None


@data_app.command("export")
def export_data(
    root: Annotated[
        Path,
        typer.Argument(exists=True, file_okay=False, dir_okay=True, readable=True),
    ],
    output: Annotated[Path, typer.Argument(help="Nouveau fichier Parquet ou CSV")],
    record_type: Annotated[str | None, typer.Option("--type", help="Type de données")] = None,
    venue: Annotated[str | None, typer.Option(help="Venue exacte")] = None,
    asset: Annotated[str | None, typer.Option(help="Actif exact")] = None,
    start_value: Annotated[str | None, typer.Option("--start", help="Date UTC incluse")] = None,
    end_value: Annotated[str | None, typer.Option("--end", help="Date UTC incluse")] = None,
    schema_version: Annotated[
        int | None,
        typer.Option("--schema-version", min=1, help="Version exacte du schéma"),
    ] = None,
    output_format: Annotated[str, typer.Option("--format", help="parquet ou csv")] = "parquet",
) -> None:
    """Exporte sans modifier ni remplir les sources, avec une sortie explicitement ordonnée."""
    try:
        _require_outside_lake(root, output, "EXPORT_REFUSED")
        normalized_format = output_format.strip().lower()
        if normalized_format not in {"parquet", "csv"}:
            raise ValueError(
                f"EXPORT_REFUSED [unsupported_format] format={output_format}; expected=parquet|csv"
            )
        expected_suffix = f".{normalized_format}"
        if output.suffix.lower() != expected_suffix:
            raise ValueError(
                "EXPORT_REFUSED [format_mismatch] "
                f"output={output} format={normalized_format}; expected_suffix={expected_suffix}"
            )
        if output.exists():
            raise ValueError(f"EXPORT_REFUSED [output_exists] output={output}")

        start = _parse_date(start_value, "start")
        end = _parse_date(end_value, "end")
        if start is not None and end is not None and start > end:
            raise ValueError(f"EXPORT_REFUSED [invalid_range] start={start} end={end}")

        # Inventory validates every source manifest before DuckDB sees the files.
        inventory_partitions(root)
        build_catalog(root, root / "catalog.duckdb")
        result = export_dataset(
            root,
            output,
            record_type=record_type,
            venue=venue,
            asset=asset,
            start=start,
            end=end,
            schema_version=schema_version,
        )
        payload = _json_ready(result)
        row_count = payload.get("row_count", "?") if isinstance(payload, Mapping) else "?"
        console.print(f"Export créé : {output} ({row_count} ligne(s)).")
    except (ImportError, OSError, TypeError, ValueError) as exc:
        _print_error(exc)
        raise typer.Exit(2) from None
