from __future__ import annotations

import csv
import hashlib
import os
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from hyperlab.data.lake import DataLakeError, PartitionManifest, inventory_partitions
from hyperlab.data.schema import RecordType, SchemaSpec, schema_for


@dataclass(frozen=True, slots=True)
class ExportResult:
    output: Path
    output_format: str
    row_count: int
    sha256: str
    source_hashes: tuple[str, ...]
    filters: dict[str, str | int | None]

    def as_dict(self) -> dict[str, object]:
        return {
            "output": str(self.output),
            "format": self.output_format,
            "row_count": self.row_count,
            "sha256": self.sha256,
            "source_hashes": list(self.source_hashes),
            "filters": self.filters,
        }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_catalog(root: Path, database: Path) -> Path:
    """Rebuild a derived DuckDB catalog from hash-validated immutable partitions."""

    try:
        import duckdb
    except ImportError:
        raise DataLakeError(
            "DuckDB catalog support is optional; install hyperlab[research]"
        ) from None

    inventory = inventory_partitions(root)
    database_target = database.resolve()
    immutable_artifacts = {
        (root / relative_path).resolve()
        for manifest in inventory.partitions
        for relative_path in (
            manifest.relative_data_path,
            manifest.relative_manifest_path,
        )
    }
    if database_target in immutable_artifacts:
        raise DataLakeError(
            f"refusing to replace immutable lake artifact: {database_target}"
        )
    grouped: dict[tuple[str, int], list[PartitionManifest]] = defaultdict(list)
    versions_by_type: dict[str, set[int]] = defaultdict(set)
    for manifest in inventory.partitions:
        grouped[(manifest.schema_name, manifest.schema_version)].append(manifest)
        versions_by_type[manifest.schema_name].add(manifest.schema_version)

    database.parent.mkdir(parents=True, exist_ok=True)
    temporary = database.parent / f".{database.name}.{uuid.uuid4().hex}.tmp"
    try:
        with duckdb.connect(str(temporary)) as connection:
            connection.execute(
                """
                CREATE TABLE hyperlab_partitions (
                    relative_path VARCHAR NOT NULL,
                    sha256 VARCHAR NOT NULL,
                    row_count UBIGINT NOT NULL,
                    venue VARCHAR NOT NULL,
                    partition_date DATE NOT NULL,
                    asset VARCHAR NOT NULL,
                    record_type VARCHAR NOT NULL,
                    schema_version USMALLINT NOT NULL,
                    quality VARCHAR NOT NULL
                )
                """
            )
            rows = [
                (
                    manifest.relative_data_path.as_posix(),
                    manifest.sha256,
                    manifest.row_count,
                    manifest.partition.venue,
                    str(manifest.partition.date),
                    manifest.partition.asset,
                    manifest.schema_name,
                    manifest.schema_version,
                    manifest.quality,
                )
                for manifest in inventory.partitions
            ]
            if rows:
                connection.executemany(
                    "INSERT INTO hyperlab_partitions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
            for (record_type, version), manifests in sorted(grouped.items()):
                version_table = f"{record_type}_v{version}"
                for index, manifest in enumerate(
                    sorted(
                        manifests,
                        key=lambda item: item.relative_data_path.as_posix(),
                    )
                ):
                    table, _ = _read_hashed_table(root, manifest)
                    registered_name = "_hyperlab_verified_segment"
                    connection.register(registered_name, table)
                    try:
                        if index == 0:
                            connection.execute(
                                f'CREATE TABLE "{version_table}" AS '
                                f'SELECT * FROM "{registered_name}"'
                            )
                        else:
                            connection.execute(
                                f'INSERT INTO "{version_table}" '
                                f'SELECT * FROM "{registered_name}"'
                            )
                    finally:
                        connection.unregister(registered_name)
                        del table
            for record_type, versions in sorted(versions_by_type.items()):
                if len(versions) == 1:
                    version = next(iter(versions))
                    connection.execute(
                        f'CREATE VIEW "{record_type}" AS '
                        f'SELECT * FROM "{record_type}_v{version}"'
                    )
        os.replace(temporary, database)
    finally:
        temporary.unlink(missing_ok=True)
    return database


def _optional_date(value: Date | str | None, name: str) -> Date | None:
    if value is None or isinstance(value, Date):
        return value
    try:
        return Date.fromisoformat(value)
    except ValueError:
        raise DataLakeError(f"invalid {name} date: {value!r}") from None


def _partition_date(manifest: PartitionManifest) -> Date:
    value = manifest.partition.date
    return Date.fromisoformat(value) if isinstance(value, str) else value


def _normalize_format(output: Path, output_format: str | None) -> str:
    inferred = output.suffix.removeprefix(".").lower()
    normalized = (output_format or inferred).lower()
    if normalized not in {"parquet", "csv"}:
        raise DataLakeError("export format must be 'parquet' or 'csv'")
    expected_suffix = f".{normalized}"
    if output.suffix.lower() != expected_suffix:
        raise DataLakeError(f"output suffix must be {expected_suffix} for {normalized} export")
    return normalized


def _select_manifests(
    root: Path,
    *,
    record_type: RecordType | str | None,
    venue: str | None,
    asset: str | None,
    start: Date | str | None,
    end: Date | str | None,
    schema_version: int | None,
) -> tuple[PartitionManifest, ...]:
    normalized_type: RecordType | None
    if record_type is None:
        normalized_type = None
    else:
        try:
            normalized_type = (
                record_type if isinstance(record_type, RecordType) else RecordType(record_type)
            )
        except ValueError:
            raise DataLakeError(f"invalid export record type: {record_type!r}") from None
    start_date = _optional_date(start, "start")
    end_date = _optional_date(end, "end")
    if start_date is not None and end_date is not None and end_date < start_date:
        raise DataLakeError("export end date precedes start date")
    if schema_version is not None and schema_version <= 0:
        raise DataLakeError("schema_version must be positive")
    manifests = inventory_partitions(root).partitions
    return tuple(
        manifest
        for manifest in manifests
        if (normalized_type is None or manifest.partition.record_type == normalized_type)
        and (venue is None or manifest.partition.venue == venue)
        and (asset is None or manifest.partition.asset == asset)
        and (start_date is None or _partition_date(manifest) >= start_date)
        and (end_date is None or _partition_date(manifest) <= end_date)
        and (schema_version is None or manifest.schema_version == schema_version)
    )


def _manifest_dimension_key(manifest: PartitionManifest) -> tuple[str, Date, str, str]:
    return (
        manifest.partition.venue,
        _partition_date(manifest),
        manifest.partition.asset,
        str(manifest.partition.record_type),
    )


def _sort_table(table: pa.Table, spec: SchemaSpec) -> pa.Table:
    columns = tuple(dict.fromkeys((*spec.order_key, *spec.primary_key)))
    return table.sort_by([(name, "ascending") for name in columns])


def _read_hashed_table(
    root: Path,
    manifest: PartitionManifest,
) -> tuple[pa.Table, str]:
    """Hash and decode one immutable byte string, closing the validation/read race."""

    path = root / manifest.relative_data_path
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise DataLakeError(f"cannot read export source {path}: {exc}") from None
    digest = hashlib.sha256(payload).hexdigest()
    if digest != manifest.sha256:
        raise DataLakeError(
            "CORRUPT_PARTITION [hash_mismatch] "
            f"partition={manifest.relative_data_path.as_posix()} "
            f"expected_sha256={manifest.sha256} actual_sha256={digest}"
        )
    try:
        table = pq.ParquetFile(pa.BufferReader(payload)).read()
    except Exception as exc:
        raise DataLakeError(
            f"invalid Parquet export source {manifest.relative_data_path.as_posix()}: {exc}"
        ) from None
    return table, digest


def _load_sorted_export(
    root: Path,
    manifests: tuple[PartitionManifest, ...],
    spec: SchemaSpec,
) -> tuple[pa.Table, tuple[str, ...]]:
    grouped: dict[tuple[str, Date, str, str], list[pa.Table]] = defaultdict(list)
    source_hashes: list[str] = []
    for manifest in sorted(
        manifests,
        key=lambda item: (_manifest_dimension_key(item), item.relative_data_path.as_posix()),
    ):
        table, digest = _read_hashed_table(root, manifest)
        grouped[_manifest_dimension_key(manifest)].append(table)
        source_hashes.append(digest)

    dimension_tables = [
        _sort_table(pa.concat_tables(grouped[dimension]), spec)
        for dimension in sorted(grouped)
    ]
    return pa.concat_tables(dimension_tables), tuple(sorted(source_hashes))


def _publish_export(temporary: Path, output: Path) -> None:
    with temporary.open("r+b") as stream:
        os.fsync(stream.fileno())
    try:
        os.link(temporary, output)
    except FileExistsError:
        raise DataLakeError(f"output already exists: {output}") from None
    finally:
        temporary.unlink(missing_ok=True)
    if os.name != "nt":
        descriptor = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _csv_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _write_csv(table: pa.Table, path: Path) -> None:
    """Write CSV without consulting a platform timezone database."""

    with path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, dialect="excel", lineterminator="\n")
        writer.writerow(table.column_names)
        for batch in table.to_batches(max_chunksize=65_536):
            columns = [batch.column(index).to_pylist() for index in range(batch.num_columns)]
            for row in zip(*columns, strict=True):
                writer.writerow(_csv_value(value) for value in row)


def export_dataset(
    root: Path,
    output: Path,
    *,
    output_format: str | None = None,
    record_type: RecordType | str | None = None,
    venue: str | None = None,
    asset: str | None = None,
    start: Date | str | None = None,
    end: Date | str | None = None,
    schema_version: int | None = None,
) -> ExportResult:
    """Validate sources and produce one deterministic, null-preserving research export."""

    resolved_root = root.resolve()
    resolved_output = output.resolve()
    if resolved_output == resolved_root or resolved_root in resolved_output.parents:
        raise DataLakeError(
            f"refusing to export inside immutable data lake: {resolved_output}"
        )
    if output.exists():
        raise DataLakeError(f"output already exists: {output}")
    normalized_format = _normalize_format(output, output_format)
    manifests = _select_manifests(
        root,
        record_type=record_type,
        venue=venue,
        asset=asset,
        start=start,
        end=end,
        schema_version=schema_version,
    )
    if not manifests:
        raise DataLakeError("no validated partitions match the export filters")
    schema_pairs = {(manifest.schema_name, manifest.schema_version) for manifest in manifests}
    if len(schema_pairs) != 1:
        raise DataLakeError(
            "an export must select exactly one record type and schema version"
        )
    schema_name, selected_version = next(iter(schema_pairs))
    try:
        spec = schema_for(schema_name, selected_version)
    except ValueError as exc:
        raise DataLakeError(str(exc)) from None
    table, source_hashes = _load_sorted_export(root, manifests, spec)

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.parent / f".{output.name}.{uuid.uuid4().hex}.tmp"
    try:
        if normalized_format == "parquet":
            pq.write_table(
                table.combine_chunks(),
                temporary,
                compression="zstd",
                version="2.6",
                data_page_version="2.0",
                use_dictionary=False,
                write_statistics=True,
                row_group_size=65_536,
                store_schema=True,
            )
        else:
            _write_csv(table, temporary)
        digest = _sha256(temporary)
        row_count = table.num_rows
        _publish_export(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return ExportResult(
        output=output,
        output_format=normalized_format,
        row_count=row_count,
        sha256=digest,
        source_hashes=source_hashes,
        filters={
            "record_type": None if record_type is None else str(record_type),
            "venue": venue,
            "asset": asset,
            "start": None if start is None else str(start),
            "end": None if end is None else str(end),
            "schema_version": selected_version,
        },
    )
