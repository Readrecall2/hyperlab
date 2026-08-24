from __future__ import annotations

import ctypes
import hashlib
import json
import os
import sqlite3
import stat
import zlib
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from ctypes import wintypes
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from itertools import groupby
from pathlib import Path
from time import monotonic
from typing import Any, cast

from hyperlab.backtest.protocol import JsonValue, canonical_json, canonical_sha256
from hyperlab.paper.models import PaperRunConfig, PaperState, deterministic_id
from hyperlab.paper.store import PAPER_STORE_SCHEMA_SQL, SCHEMA_VERSION, STORE_SCHEMA_VERSION

GOLDEN_FORMAT = "hyperlab-paper-golden-v3-v1"
GOLDEN_TOOL_VERSION = 1
GOLDEN_STREAM_NAMES = (
    "schema",
    "run",
    "inbox",
    "events",
    "ledger_transactions",
    "ledger_entries",
    "alerts",
    "commits",
    "projection_history",
    "projection_current",
    "runtime_sessions",
    "incidents",
    "heads",
)

_HEX_DIGEST_LENGTH = 64
_SQLITE_SIDECAR_SUFFIXES = ("-journal", "-shm", "-wal")
_DEFAULT_SHARD_ROWS = 100_000
_DEFAULT_SHARD_BYTES = 64 * 1024 * 1024
_PROGRESS_ROW_INTERVAL = 1_000
_PROGRESS_HEARTBEAT_SECONDS = 30.0

_TABLE_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "paper_schema": ("singleton", "version", "created_at"),
    "paper_runs": (
        "run_id",
        "schema_version",
        "config_json",
        "config_hash",
        "seed_text",
        "status",
        "created_at",
        "event_count",
        "event_head_hash",
        "commit_count",
        "commit_head_hash",
        "projection_revision",
        "projection_hash",
    ),
    "paper_inbox": (
        "run_id",
        "input_id",
        "payload_json",
        "payload_hash",
        "first_event_sequence",
        "last_event_sequence",
        "commit_sequence",
        "commit_hash",
        "created_at",
    ),
    "paper_events": (
        "run_id",
        "sequence",
        "event_id",
        "event_type",
        "payload_json",
        "payload_hash",
        "previous_hash",
        "event_hash",
        "input_id",
        "created_at",
    ),
    "paper_ledger_transactions": (
        "run_id",
        "transaction_id",
        "input_id",
        "event_sequence",
        "entry_count",
        "transaction_hash",
        "created_at",
    ),
    "paper_ledger_entries": (
        "run_id",
        "transaction_id",
        "entry_index",
        "entry_id",
        "event_id",
        "account",
        "unit",
        "amount_text",
        "payload_json",
        "entry_hash",
    ),
    "paper_projections": (
        "run_id",
        "revision",
        "event_sequence",
        "event_head_hash",
        "status",
        "effective_status",
        "payload_json",
        "projection_hash",
        "updated_at",
    ),
    "paper_projection_history": (
        "run_id",
        "revision",
        "input_id",
        "event_sequence",
        "event_head_hash",
        "status",
        "payload_json",
        "payload_zlib",
        "payload_codec",
        "last_received_at",
        "utc_date",
        "projection_hash",
        "created_at",
    ),
    "paper_alerts": (
        "run_id",
        "alert_id",
        "commit_sequence",
        "event_sequence",
        "severity",
        "code",
        "payload_json",
        "payload_hash",
        "created_at",
    ),
    "paper_commits": (
        "run_id",
        "commit_sequence",
        "input_id",
        "first_event_sequence",
        "last_event_sequence",
        "event_hashes_json",
        "ledger_hashes_json",
        "projection_revision",
        "projection_hash",
        "alert_hashes_json",
        "previous_commit_hash",
        "commit_hash",
        "created_at",
    ),
}

_REQUIRED_INDEXES = frozenset(
    {
        "paper_alerts_commit_idx",
        "paper_alerts_run_severity_sequence_idx",
        "paper_alerts_sequence_idx",
        "paper_events_input_idx",
        "paper_ledger_event_idx",
        "paper_ledger_transactions_input_idx",
    }
)
_REQUIRED_TRIGGERS = frozenset(
    {
        "paper_alerts_no_delete",
        "paper_alerts_no_update",
        "paper_commits_no_delete",
        "paper_commits_no_update",
        "paper_events_no_delete",
        "paper_events_no_update",
        "paper_inbox_no_delete",
        "paper_inbox_no_update",
        "paper_ledger_entries_no_delete",
        "paper_ledger_entries_no_update",
        "paper_ledger_transactions_no_delete",
        "paper_ledger_transactions_no_update",
        "paper_projection_history_no_delete",
        "paper_projection_history_no_update",
        "paper_runs_config_immutable",
        "paper_runs_no_delete",
    }
)

_STREAM_ORDERS: Mapping[str, tuple[str, ...]] = {
    "schema": ("kind", "name"),
    "run": ("run_id",),
    "inbox": ("commit_sequence", "input_id"),
    "events": ("sequence",),
    "ledger_transactions": ("commit_sequence", "component_ordinal", "transaction_id"),
    "ledger_entries": (
        "commit_sequence",
        "transaction_ordinal",
        "entry_index",
        "entry_id",
    ),
    "alerts": (
        "committed_before_uncommitted",
        "commit_sequence_or_event_sequence",
        "component_ordinal_or_alert_id",
    ),
    "commits": ("commit_sequence",),
    "projection_history": ("revision",),
    "projection_current": ("run_id",),
    "runtime_sessions": ("commit_sequence", "input_id"),
    "incidents": (
        "committed_before_uncommitted",
        "commit_sequence_or_event_sequence",
        "component_ordinal_or_alert_id",
    ),
    "heads": ("run_id",),
}

ProgressCallback = Callable[[Mapping[str, object]], None]
IdentityValue = int | str
RowIdentity = tuple[IdentityValue, ...]


class GoldenRefusal(ValueError):
    """The requested source or output boundary is unsafe or inconsistent."""


class GoldenVerificationError(ValueError):
    """A purported Golden export is incomplete, malformed, or unauthenticated."""


class GoldenDifferentialError(ValueError):
    """Two Golden logical histories are not exactly identical."""


@dataclass(frozen=True, slots=True)
class SourceStat:
    size: int
    mtime_ns: int
    ctime_ns: int
    mode: int
    device: int
    inode: int
    link_count: int
    file_attributes: int
    readonly: bool

    @classmethod
    def read(cls, path: Path) -> SourceStat:
        value = path.stat()
        attributes = int(getattr(value, "st_file_attributes", 0))
        readonly_attribute = bool(
            attributes
            and hasattr(stat, "FILE_ATTRIBUTE_READONLY")
            and attributes & int(stat.FILE_ATTRIBUTE_READONLY)
        )
        writable_mode = bool(value.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
        return cls(
            size=int(value.st_size),
            mtime_ns=int(value.st_mtime_ns),
            ctime_ns=int(value.st_ctime_ns),
            mode=int(value.st_mode),
            device=int(value.st_dev),
            inode=int(value.st_ino),
            link_count=int(value.st_nlink),
            file_attributes=attributes,
            readonly=readonly_attribute or not writable_mode,
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "ctime_ns": self.ctime_ns,
            "device": self.device,
            "file_attributes": self.file_attributes,
            "inode": self.inode,
            "link_count": self.link_count,
            "mode": self.mode,
            "mtime_ns": self.mtime_ns,
            "readonly": self.readonly,
            "size": self.size,
        }


@dataclass(frozen=True, slots=True)
class SourceFingerprint:
    realpath: str
    sha256: str
    stat: SourceStat
    sidecars: tuple[str, ...]

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "realpath": self.realpath,
            "sha256": self.sha256,
            "sidecars": list(self.sidecars),
            "stat": self.stat.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class GoldenExportResult:
    output_root: Path
    manifest_path: Path
    complete_path: Path
    root_hash: str
    census_status: str
    source_sha256: str

    def to_dict(self) -> dict[str, object]:
        return {
            "census_status": self.census_status,
            "complete_path": str(self.complete_path),
            "manifest_path": str(self.manifest_path),
            "output_root": str(self.output_root),
            "root_hash": self.root_hash,
            "source_sha256": self.source_sha256,
        }


@dataclass(frozen=True, slots=True)
class GoldenVerification:
    export_root: Path
    root_hash: str
    manifest: dict[str, Any]

    def to_dict(self) -> dict[str, object]:
        return {
            "export_root": str(self.export_root),
            "root_hash": self.root_hash,
            "status": "GOLDEN_V3_EXPORT_VERIFIED",
        }


@dataclass(frozen=True, slots=True)
class GoldenDifferentialResult:
    expected_root: Path
    actual_root: Path
    root_hash: str

    def to_dict(self) -> dict[str, object]:
        return {
            "actual_root": str(self.actual_root),
            "expected_root": str(self.expected_root),
            "root_hash": self.root_hash,
            "status": "GOLDEN_V3_EXPORTS_EXACT",
        }


def _identifier(value: object, *, label: str) -> str:
    if not isinstance(value, str):
        raise GoldenRefusal(f"{label} must be a string")
    normalized = value.strip()
    if not normalized or any(character.isspace() for character in normalized):
        raise GoldenRefusal(f"{label} must be a stable non-empty identifier")
    return normalized


def _digest_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or len(value) != _HEX_DIGEST_LENGTH:
        raise GoldenRefusal(f"{label} must be a 64-character digest")
    try:
        int(value, 16)
    except ValueError as error:
        raise GoldenRefusal(f"{label} must be hexadecimal") from error
    if value != value.lower():
        raise GoldenRefusal(f"{label} must use lowercase hexadecimal")
    return value


def _canonical_line(value: object) -> bytes:
    return canonical_json(value).encode("utf-8") + b"\n"


def _json_object(value: object, *, label: str) -> dict[str, JsonValue]:
    try:
        decoded = json.loads(str(value))
    except (TypeError, json.JSONDecodeError) as error:
        raise GoldenRefusal(f"{label} is not valid JSON") from error
    if not isinstance(decoded, dict):
        raise GoldenRefusal(f"{label} must be a JSON object")
    return cast(dict[str, JsonValue], decoded)


def _json_string_list(value: object, *, label: str) -> list[str]:
    try:
        decoded = json.loads(str(value))
    except (TypeError, json.JSONDecodeError) as error:
        raise GoldenRefusal(f"{label} is not valid JSON") from error
    if not isinstance(decoded, list) or any(not isinstance(item, str) for item in decoded):
        raise GoldenRefusal(f"{label} must be a JSON string list")
    return cast(list[str], decoded)


def _json_array(values: Iterable[JsonValue]) -> list[JsonValue]:
    return list(values)


def _required_json_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise GoldenVerificationError(f"{label} must be an integer")
    return value


def _canonical_payload(value: object, payload_hash: object, *, label: str) -> dict[str, JsonValue]:
    payload = _json_object(value, label=label)
    if canonical_json(payload) != str(value):
        raise GoldenRefusal(f"{label} is not canonical JSON")
    if canonical_sha256(payload) != str(payload_hash):
        raise GoldenRefusal(f"{label} hash differs")
    return payload


def _projection_history_payload(row: sqlite3.Row) -> dict[str, JsonValue]:
    codec = str(row["payload_codec"])
    if codec == "json":
        payload_json = str(row["payload_json"])
    elif codec == "zlib-json-v1":
        raw = row["payload_zlib"]
        if not isinstance(raw, (bytes, bytearray, memoryview)):
            raise GoldenRefusal(f"projection revision {row['revision']} has no compressed payload")
        try:
            payload_json = zlib.decompress(bytes(raw)).decode("utf-8")
        except (zlib.error, UnicodeDecodeError) as error:
            raise GoldenRefusal(
                f"projection revision {row['revision']} has an invalid compressed payload"
            ) from error
    else:
        raise GoldenRefusal(f"projection revision {row['revision']} uses unsupported codec {codec!r}")
    payload = _json_object(payload_json, label=f"projection revision {row['revision']}")
    if canonical_json(payload) != payload_json:
        raise GoldenRefusal(f"projection revision {row['revision']} is not canonical")
    if canonical_sha256(payload) != str(row["projection_hash"]):
        raise GoldenRefusal(f"projection revision {row['revision']} hash differs")
    return payload


def _event_genesis(run_id: str, config_hash: str) -> str:
    return canonical_sha256(
        {
            "config_hash": config_hash,
            "domain": "hyperlab-paper-event-genesis-v1",
            "run_id": run_id,
        }
    )


def _commit_genesis(run_id: str, config_hash: str) -> str:
    return canonical_sha256(
        {
            "config_hash": config_hash,
            "domain": "hyperlab-paper-commit-genesis-v1",
            "run_id": run_id,
        }
    )


def _event_hash(*, sequence: int, payload: Mapping[str, JsonValue], previous_hash: str) -> str:
    return canonical_sha256(
        {
            **payload,
            "previous_event_hash": previous_hash,
            "sequence": sequence,
        }
    )


def _ledger_transaction_hash(
    run_id: str,
    transaction_id: str,
    entry_hashes: Sequence[str],
) -> str:
    return canonical_sha256(
        {
            "domain": "hyperlab-paper-ledger-transaction-v1",
            "entry_hashes": list(entry_hashes),
            "run_id": run_id,
            "transaction_id": transaction_id,
        }
    )


def _commit_hash(
    *,
    run_id: str,
    commit_sequence: int,
    input_id: str,
    first_event_sequence: int | None,
    last_event_sequence: int,
    event_hashes: Sequence[str],
    ledger_hashes: Sequence[str],
    projection_revision: int,
    projection_hash: str,
    alert_hashes: Sequence[str],
    previous_commit_hash: str,
) -> str:
    return canonical_sha256(
        {
            "alert_hashes": list(alert_hashes),
            "commit_sequence": commit_sequence,
            "domain": "hyperlab-paper-commit-v1",
            "event_hashes": list(event_hashes),
            "first_event_sequence": first_event_sequence,
            "input_id": input_id,
            "last_event_sequence": last_event_sequence,
            "ledger_hashes": list(ledger_hashes),
            "previous_commit_hash": previous_commit_hash,
            "projection_hash": projection_hash,
            "projection_revision": projection_revision,
            "run_id": run_id,
        }
    )


def _normalized_absolute(path: Path) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def validate_new_auxiliary_path(
    path: Path | str,
    *,
    forbidden_paths: Iterable[Path | str],
    label: str,
    required_suffix: str | None = ".jsonl",
    require_existing_parent: bool = True,
) -> Path:
    """Validate a new non-corpus artifact path without following reparses."""

    candidate = Path(os.path.abspath(os.fspath(Path(path).expanduser())))
    if candidate.exists() or candidate.is_symlink():
        raise GoldenRefusal(f"{label} already exists; refusing overwrite: {candidate}")
    if required_suffix is not None and candidate.suffix.lower() != required_suffix.lower():
        raise GoldenRefusal(f"{label} must use the {required_suffix} suffix")
    if _has_reparse_component(candidate):
        raise GoldenRefusal(f"{label} contains a symlink, junction, or reparse path")
    if require_existing_parent and not candidate.parent.is_dir():
        raise GoldenRefusal(f"{label} parent must be an existing directory")
    normalized_candidate = _normalized_absolute(candidate)
    for forbidden in forbidden_paths:
        normalized_forbidden = _normalized_absolute(Path(forbidden).expanduser())
        if (
            normalized_candidate == normalized_forbidden
            or normalized_candidate.startswith(normalized_forbidden + os.sep)
            or normalized_forbidden.startswith(normalized_candidate + os.sep)
        ):
            raise GoldenRefusal(f"{label} collides with a protected path")
    return candidate


def _has_reparse_component(path: Path) -> bool:
    absolute = Path(os.path.abspath(os.fspath(path)))
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        if not current.exists() and not current.is_symlink():
            break
        try:
            value = current.lstat()
        except OSError:
            return True
        attributes = int(getattr(value, "st_file_attributes", 0))
        if current.is_symlink():
            return True
        if (
            attributes
            and hasattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT")
            and attributes & int(stat.FILE_ATTRIBUTE_REPARSE_POINT)
        ):
            return True
    return False


def _same_file(left: Path, right: Path) -> bool:
    if _normalized_absolute(left) == _normalized_absolute(right):
        return True
    if not left.exists() or not right.exists():
        return False
    try:
        return os.path.samefile(left, right)
    except OSError as error:
        raise GoldenRefusal("source/sentinel same-file identity could not be established") from error


def _sqlite_sidecars(path: Path) -> tuple[Path, ...]:
    return tuple(Path(f"{path}{suffix}") for suffix in _SQLITE_SIDECAR_SUFFIXES if Path(f"{path}{suffix}").exists())


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fingerprint_source(path: Path) -> SourceFingerprint:
    before = SourceStat.read(path)
    digest = _sha256_file(path)
    after = SourceStat.read(path)
    if before != after:
        raise GoldenRefusal("source stat changed while its SHA-256 was computed")
    sidecars = _sqlite_sidecars(path)
    return SourceFingerprint(
        realpath=str(path.resolve(strict=True)),
        sha256=digest,
        stat=after,
        sidecars=tuple(candidate.name for candidate in sidecars),
    )


def _validate_paths(
    source: Path,
    output_root: Path,
    sentinel: Path,
    *,
    require_readonly: bool,
) -> tuple[Path, Path, Path, SourceFingerprint]:
    if not source.exists() or not source.is_file():
        raise GoldenRefusal("source must be an existing regular SQLite file")
    if source.is_symlink() or _has_reparse_component(source):
        raise GoldenRefusal("source symlink, junction, or reparse paths are forbidden")
    resolved_source = source.resolve(strict=True)
    if sentinel.is_symlink() or _has_reparse_component(sentinel):
        raise GoldenRefusal("forbidden sentinel symlink, junction, or reparse paths are forbidden")
    if _same_file(resolved_source, sentinel):
        raise GoldenRefusal("source is the same path or file as the forbidden sentinel")
    if output_root.exists() or output_root.is_symlink():
        raise GoldenRefusal("output candidate already exists; refusing overwrite")
    if _has_reparse_component(output_root.parent):
        raise GoldenRefusal("output parent contains a symlink, junction, or reparse component")
    normalized_output = _normalized_absolute(output_root)
    normalized_source = _normalized_absolute(resolved_source)
    normalized_sentinel = _normalized_absolute(sentinel)
    if normalized_output in {normalized_source, normalized_sentinel}:
        raise GoldenRefusal("output collides with the source or forbidden sentinel")
    if normalized_source.startswith(normalized_output + os.sep):
        raise GoldenRefusal("output may not contain the source")
    source_stat = SourceStat.read(resolved_source)
    if source_stat.link_count != 1:
        raise GoldenRefusal("source hardlinks are forbidden")
    if require_readonly and not source_stat.readonly:
        raise GoldenRefusal("source does not have a read-only file attribute or mode")
    if _sqlite_sidecars(resolved_source):
        raise GoldenRefusal("source SQLite sidecars are forbidden for this cold-copy export")
    fingerprint = _fingerprint_source(resolved_source)
    if fingerprint.sidecars:
        raise GoldenRefusal("source SQLite sidecars appeared during fingerprinting")
    return resolved_source, output_root.absolute(), sentinel.absolute(), fingerprint


@contextmanager
def _readonly_snapshot(source: Path) -> Iterator[sqlite3.Connection]:
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"{source.as_uri()}?mode=ro",
            uri=True,
            timeout=30.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA query_only=ON")
        query_only = connection.execute("PRAGMA query_only").fetchone()
        if query_only is None or int(query_only[0]) != 1:
            raise GoldenRefusal("source SQLite connection did not enter query_only mode")
        connection.execute("BEGIN")
        connection.execute("SELECT 1 FROM sqlite_schema LIMIT 1").fetchone()
        if not connection.in_transaction:
            raise GoldenRefusal("source SQLite read transaction was not established")
        database = connection.execute("PRAGMA database_list").fetchone()
        if database is None or not _same_file(source, Path(str(database[2]))):
            raise GoldenRefusal("source SQLite connection resolved to an unexpected file")
        if _sqlite_sidecars(source):
            raise GoldenRefusal("source SQLite sidecar appeared while acquiring the read snapshot")
        yield connection
    except sqlite3.Error as error:
        raise GoldenRefusal(f"source SQLite read-only snapshot failed: {type(error).__name__}: {error}") from error
    finally:
        if connection is not None:
            if connection.in_transaction:
                connection.rollback()
            connection.close()


@lru_cache(maxsize=1)
def _canonical_sqlite_schema() -> tuple[dict[str, JsonValue], ...]:
    """Return SQLite''s exact representation of the canonical PaperStore-v3 DDL."""

    connection = sqlite3.connect(":memory:", isolation_level=None)
    try:
        connection.row_factory = sqlite3.Row
        connection.executescript(PAPER_STORE_SCHEMA_SQL)
        return _sqlite_schema_records(connection)
    finally:
        connection.close()


def _sqlite_schema_records(
    connection: sqlite3.Connection,
) -> tuple[dict[str, JsonValue], ...]:
    return tuple(
        {
            "name": str(row["name"]),
            "sql": str(row["sql"]) if row["sql"] is not None else None,
            "table": str(row["tbl_name"]),
            "type": str(row["type"]),
        }
        for row in connection.execute(
            """
            SELECT type, name, tbl_name, sql
            FROM sqlite_schema
            WHERE name LIKE 'paper_%' OR tbl_name LIKE 'paper_%'
            ORDER BY type, name, tbl_name
            """
        )
    )


def _schema_rows(connection: sqlite3.Connection) -> tuple[dict[str, JsonValue], ...]:
    user_version_row = connection.execute("PRAGMA user_version").fetchone()
    if user_version_row is None or int(user_version_row[0]) != STORE_SCHEMA_VERSION:
        raise GoldenRefusal("source store PRAGMA user_version is not PaperStore v3")
    metadata = connection.execute(
        "SELECT singleton, version, created_at FROM paper_schema"
    ).fetchall()
    if len(metadata) != 1 or int(metadata[0]["singleton"]) != 1:
        raise GoldenRefusal("paper_schema must contain exactly its singleton metadata row")
    if int(metadata[0]["version"]) != STORE_SCHEMA_VERSION:
        raise GoldenRefusal("paper_schema.version disagrees with PaperStore v3")
    canonical_schema = _canonical_sqlite_schema()
    observed_schema = _sqlite_schema_records(connection)
    if observed_schema != canonical_schema:
        expected_by_key = {
            (str(row["type"]), str(row["name"]), str(row["table"])): row
            for row in canonical_schema
        }
        observed_by_key = {
            (str(row["type"]), str(row["name"]), str(row["table"])): row
            for row in observed_schema
        }
        missing = sorted(set(expected_by_key) - set(observed_by_key))
        extra = sorted(set(observed_by_key) - set(expected_by_key))
        changed = sorted(
            key
            for key in set(expected_by_key) & set(observed_by_key)
            if expected_by_key[key] != observed_by_key[key]
        )
        raise GoldenRefusal(
            "source sqlite_schema DDL differs from the exact PaperStore v3 contract: "
            f"missing={missing}, extra={extra}, changed={changed}"
        )
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'paper_%'"
        )
    }
    if tables != set(_TABLE_COLUMNS):
        raise GoldenRefusal(
            f"source paper table set differs: missing={sorted(set(_TABLE_COLUMNS) - tables)}, "
            f"extra={sorted(tables - set(_TABLE_COLUMNS))}"
        )
    indexes = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='index' "
            "AND name LIKE 'paper_%' AND name NOT LIKE 'sqlite_autoindex_%'"
        )
    }
    if indexes != _REQUIRED_INDEXES:
        raise GoldenRefusal(
            f"source PaperStore index set differs: missing={sorted(_REQUIRED_INDEXES - indexes)}, "
            f"extra={sorted(indexes - _REQUIRED_INDEXES)}"
        )
    triggers = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='trigger' AND name LIKE 'paper_%'"
        )
    }
    if triggers != _REQUIRED_TRIGGERS:
        raise GoldenRefusal(
            f"source PaperStore trigger set differs: missing={sorted(_REQUIRED_TRIGGERS - triggers)}, "
            f"extra={sorted(triggers - _REQUIRED_TRIGGERS)}"
        )

    result: list[dict[str, JsonValue]] = [
        {
            "created_at": str(metadata[0]["created_at"]),
            "kind": "metadata",
            "name": "paper_schema",
            "pragma_user_version": int(user_version_row[0]),
            "sqlite_schema_sha256": canonical_sha256(
                {
                    "domain": "hyperlab-paper-store-v3-sqlite-schema-v1",
                    "objects": observed_schema,
                }
            ),
            "singleton": int(metadata[0]["singleton"]),
            "version": int(metadata[0]["version"]),
        }
    ]
    for table in sorted(_TABLE_COLUMNS):
        columns = connection.execute(f"PRAGMA table_info('{table}')").fetchall()
        names = tuple(str(row["name"]) for row in columns)
        if names != _TABLE_COLUMNS[table]:
            raise GoldenRefusal(f"source {table} columns differ from the exact PaperStore v3 contract")
        column_records: list[JsonValue] = [
            {
                "cid": int(row["cid"]),
                "default": (str(row["dflt_value"]) if row["dflt_value"] is not None else None),
                "name": str(row["name"]),
                "not_null": bool(row["notnull"]),
                "primary_key_ordinal": int(row["pk"]),
                "type": str(row["type"]),
            }
            for row in columns
        ]
        foreign_keys: list[JsonValue] = [
            {
                "from": str(row["from"]),
                "id": int(row["id"]),
                "match": str(row["match"]),
                "on_delete": str(row["on_delete"]),
                "on_update": str(row["on_update"]),
                "sequence": int(row["seq"]),
                "table": str(row["table"]),
                "to": str(row["to"]),
            }
            for row in connection.execute(f"PRAGMA foreign_key_list('{table}')")
        ]
        explicit_indexes: list[JsonValue] = []
        for index in connection.execute(f"PRAGMA index_list('{table}')"):
            name = str(index["name"])
            if name.startswith("sqlite_autoindex_"):
                continue
            explicit_indexes.append(
                {
                    "columns": [
                        str(item["name"])
                        for item in connection.execute(f"PRAGMA index_info('{name}')")
                    ],
                    "name": name,
                    "partial": bool(index["partial"]),
                    "unique": bool(index["unique"]),
                }
            )
        result.append(
            {
                "columns": column_records,
                "foreign_keys": foreign_keys,
                "indexes": sorted(
                    explicit_indexes,
                    key=lambda value: str(cast(dict[str, JsonValue], value)["name"]),
                ),
                "kind": "table",
                "name": table,
                "triggers": _json_array(
                    sorted(
                        str(row[0])
                        for row in connection.execute(
                            "SELECT name FROM sqlite_schema WHERE type='trigger' AND tbl_name=?",
                            (table,),
                        )
                    )
                ),
            }
        )
    return tuple(result)


def _run_row(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
    rows = connection.execute("SELECT * FROM paper_runs WHERE run_id=?", (run_id,)).fetchall()
    if len(rows) != 1:
        raise GoldenRefusal(f"expected run_id {run_id} does not identify exactly one paper run")
    return rows[0]


def _alert_rows(connection: sqlite3.Connection, run_id: str) -> Iterator[dict[str, JsonValue]]:
    committed = connection.execute(
        """
        SELECT
            commit_row.commit_sequence AS owning_commit_sequence,
            CAST(component.key AS INTEGER) AS component_ordinal,
            alert.*
        FROM paper_commits AS commit_row
        JOIN json_each(commit_row.alert_hashes_json) AS component
        JOIN paper_alerts AS alert
          ON alert.run_id=commit_row.run_id
         AND alert.commit_sequence=commit_row.commit_sequence
         AND alert.payload_hash=component.value
        WHERE commit_row.run_id=?
        ORDER BY commit_row.commit_sequence, CAST(component.key AS INTEGER)
        """,
        (run_id,),
    )
    for row in committed:
        yield {
            "alert_id": str(row["alert_id"]),
            "code": str(row["code"]),
            "commit_sequence": int(row["commit_sequence"]),
            "component_ordinal": int(row["component_ordinal"]),
            "created_at": str(row["created_at"]),
            "event_sequence": int(row["event_sequence"]),
            "payload": _json_object(row["payload_json"], label=f"alert {row['alert_id']}"),
            "payload_hash": str(row["payload_hash"]),
            "run_id": str(row["run_id"]),
            "severity": str(row["severity"]),
        }
    uncommitted = connection.execute(
        """
        SELECT * FROM paper_alerts
        WHERE run_id=? AND commit_sequence IS NULL
        ORDER BY event_sequence, alert_id
        """,
        (run_id,),
    )
    for row in uncommitted:
        yield {
            "alert_id": str(row["alert_id"]),
            "code": str(row["code"]),
            "commit_sequence": None,
            "component_ordinal": None,
            "created_at": str(row["created_at"]),
            "event_sequence": int(row["event_sequence"]),
            "payload": _json_object(row["payload_json"], label=f"alert {row['alert_id']}"),
            "payload_hash": str(row["payload_hash"]),
            "run_id": str(row["run_id"]),
            "severity": str(row["severity"]),
        }


def iter_sqlite_logical_stream(
    connection: sqlite3.Connection,
    run_id: str,
    stream_name: str,
) -> Iterator[dict[str, JsonValue]]:
    """Project one exact logical stream from an already-owned SQLite connection.

    The caller owns connection lifetime and transaction policy. Golden source
    export calls this only inside one ``mode=ro``/``query_only`` transaction;
    replay differential may call it for a disposable target store.
    """

    normalized_run_id = _identifier(run_id, label="run_id")
    if stream_name not in GOLDEN_STREAM_NAMES:
        raise ValueError(f"unknown Golden V3 stream {stream_name!r}")
    connection.row_factory = sqlite3.Row

    if stream_name == "schema":
        yield from _schema_rows(connection)
        return
    if stream_name == "run":
        row = _run_row(connection, normalized_run_id)
        yield {
            "commit_count": int(row["commit_count"]),
            "commit_head_hash": str(row["commit_head_hash"]),
            "config": _json_object(row["config_json"], label="run config"),
            "config_hash": str(row["config_hash"]),
            "created_at": str(row["created_at"]),
            "event_count": int(row["event_count"]),
            "event_head_hash": str(row["event_head_hash"]),
            "projection_hash": str(row["projection_hash"]),
            "projection_revision": int(row["projection_revision"]),
            "run_id": str(row["run_id"]),
            "schema_version": int(row["schema_version"]),
            "seed_text": (str(row["seed_text"]) if row["seed_text"] is not None else None),
            "status": str(row["status"]),
        }
        return
    if stream_name == "inbox":
        rows = connection.execute(
            "SELECT * FROM paper_inbox WHERE run_id=? ORDER BY commit_sequence, input_id",
            (normalized_run_id,),
        )
        for row in rows:
            yield {
                "commit_hash": str(row["commit_hash"]),
                "commit_sequence": int(row["commit_sequence"]),
                "created_at": str(row["created_at"]),
                "first_event_sequence": (
                    int(row["first_event_sequence"])
                    if row["first_event_sequence"] is not None
                    else None
                ),
                "input_id": str(row["input_id"]),
                "last_event_sequence": int(row["last_event_sequence"]),
                "payload": _json_object(row["payload_json"], label=f"input {row['input_id']}"),
                "payload_hash": str(row["payload_hash"]),
                "run_id": str(row["run_id"]),
            }
        return
    if stream_name == "events":
        rows = connection.execute(
            "SELECT * FROM paper_events WHERE run_id=? ORDER BY sequence",
            (normalized_run_id,),
        )
        for row in rows:
            yield {
                "created_at": str(row["created_at"]),
                "event_hash": str(row["event_hash"]),
                "event_id": str(row["event_id"]),
                "event_type": str(row["event_type"]),
                "input_id": str(row["input_id"]),
                "payload": _json_object(row["payload_json"], label=f"event {row['sequence']}"),
                "payload_hash": str(row["payload_hash"]),
                "previous_hash": str(row["previous_hash"]),
                "run_id": str(row["run_id"]),
                "sequence": int(row["sequence"]),
            }
        return
    if stream_name == "ledger_transactions":
        rows = connection.execute(
            """
            SELECT
                commit_row.commit_sequence AS owning_commit_sequence,
                CAST(component.key AS INTEGER) AS component_ordinal,
                ledger.*
            FROM paper_commits AS commit_row
            JOIN json_each(commit_row.ledger_hashes_json) AS component
            JOIN paper_ledger_transactions AS ledger
              ON ledger.run_id=commit_row.run_id
             AND ledger.input_id=commit_row.input_id
             AND ledger.transaction_hash=component.value
            WHERE commit_row.run_id=?
            ORDER BY commit_row.commit_sequence, CAST(component.key AS INTEGER)
            """,
            (normalized_run_id,),
        )
        for row in rows:
            yield {
                "commit_sequence": int(row["owning_commit_sequence"]),
                "component_ordinal": int(row["component_ordinal"]),
                "created_at": str(row["created_at"]),
                "entry_count": int(row["entry_count"]),
                "event_sequence": int(row["event_sequence"]),
                "input_id": str(row["input_id"]),
                "run_id": str(row["run_id"]),
                "transaction_hash": str(row["transaction_hash"]),
                "transaction_id": str(row["transaction_id"]),
            }
        return
    if stream_name == "ledger_entries":
        rows = connection.execute(
            """
            SELECT
                commit_row.commit_sequence AS owning_commit_sequence,
                CAST(component.key AS INTEGER) AS transaction_ordinal,
                ledger_entry.*
            FROM paper_commits AS commit_row
            JOIN json_each(commit_row.ledger_hashes_json) AS component
            JOIN paper_ledger_transactions AS ledger_transaction
              ON ledger_transaction.run_id=commit_row.run_id
             AND ledger_transaction.input_id=commit_row.input_id
             AND ledger_transaction.transaction_hash=component.value
            JOIN paper_ledger_entries AS ledger_entry
              ON ledger_entry.run_id=ledger_transaction.run_id
             AND ledger_entry.transaction_id=ledger_transaction.transaction_id
            WHERE commit_row.run_id=?
            ORDER BY
                commit_row.commit_sequence,
                CAST(component.key AS INTEGER),
                ledger_entry.entry_index
            """,
            (normalized_run_id,),
        )
        for row in rows:
            yield {
                "account": str(row["account"]),
                "amount_text": str(row["amount_text"]),
                "commit_sequence": int(row["owning_commit_sequence"]),
                "entry_hash": str(row["entry_hash"]),
                "entry_id": str(row["entry_id"]),
                "entry_index": int(row["entry_index"]),
                "event_id": (str(row["event_id"]) if row["event_id"] is not None else None),
                "payload": _json_object(row["payload_json"], label=f"ledger entry {row['entry_id']}"),
                "run_id": str(row["run_id"]),
                "transaction_id": str(row["transaction_id"]),
                "transaction_ordinal": int(row["transaction_ordinal"]),
                "unit": str(row["unit"]),
            }
        return
    if stream_name == "alerts":
        yield from _alert_rows(connection, normalized_run_id)
        return
    if stream_name == "commits":
        rows = connection.execute(
            "SELECT * FROM paper_commits WHERE run_id=? ORDER BY commit_sequence",
            (normalized_run_id,),
        )
        for row in rows:
            sequence = int(row["commit_sequence"])
            yield {
                "alert_hashes": _json_array(
                    _json_string_list(
                        row["alert_hashes_json"], label=f"commit {sequence} alert hashes"
                    )
                ),
                "commit_hash": str(row["commit_hash"]),
                "commit_sequence": sequence,
                "created_at": str(row["created_at"]),
                "event_hashes": _json_array(
                    _json_string_list(
                        row["event_hashes_json"], label=f"commit {sequence} event hashes"
                    )
                ),
                "first_event_sequence": (
                    int(row["first_event_sequence"])
                    if row["first_event_sequence"] is not None
                    else None
                ),
                "input_id": str(row["input_id"]),
                "last_event_sequence": int(row["last_event_sequence"]),
                "ledger_hashes": _json_array(
                    _json_string_list(
                        row["ledger_hashes_json"], label=f"commit {sequence} ledger hashes"
                    )
                ),
                "previous_commit_hash": str(row["previous_commit_hash"]),
                "projection_hash": str(row["projection_hash"]),
                "projection_revision": int(row["projection_revision"]),
                "run_id": str(row["run_id"]),
            }
        return
    if stream_name == "projection_history":
        rows = connection.execute(
            "SELECT * FROM paper_projection_history WHERE run_id=? ORDER BY revision",
            (normalized_run_id,),
        )
        for row in rows:
            yield {
                "created_at": str(row["created_at"]),
                "event_head_hash": str(row["event_head_hash"]),
                "event_sequence": int(row["event_sequence"]),
                "input_id": (str(row["input_id"]) if row["input_id"] is not None else None),
                "last_received_at": (
                    str(row["last_received_at"]) if row["last_received_at"] is not None else None
                ),
                "payload": _projection_history_payload(row),
                "payload_codec": str(row["payload_codec"]),
                "projection_hash": str(row["projection_hash"]),
                "revision": int(row["revision"]),
                "run_id": str(row["run_id"]),
                "status": str(row["status"]),
                "utc_date": (str(row["utc_date"]) if row["utc_date"] is not None else None),
            }
        return
    if stream_name == "projection_current":
        projection_rows = connection.execute(
            "SELECT * FROM paper_projections WHERE run_id=?",
            (normalized_run_id,),
        ).fetchall()
        if len(projection_rows) != 1:
            raise GoldenRefusal("selected run must have exactly one current projection")
        projection_row = projection_rows[0]
        yield {
            "effective_status": str(projection_row["effective_status"]),
            "event_head_hash": str(projection_row["event_head_hash"]),
            "event_sequence": int(projection_row["event_sequence"]),
            "payload": _json_object(projection_row["payload_json"], label="current projection"),
            "projection_hash": str(projection_row["projection_hash"]),
            "revision": int(projection_row["revision"]),
            "run_id": str(projection_row["run_id"]),
            "status": str(projection_row["status"]),
            "updated_at": str(projection_row["updated_at"]),
        }
        return
    if stream_name == "runtime_sessions":
        for inbox_row in iter_sqlite_logical_stream(connection, normalized_run_id, "inbox"):
            payload = cast(dict[str, JsonValue], inbox_row["payload"])
            if payload.get("input_type") not in {
                "RUNTIME_SESSION_STARTED",
                "RUNTIME_SESSION_STOPPED",
            }:
                continue
            yield {
                "commit_hash": inbox_row["commit_hash"],
                "commit_sequence": inbox_row["commit_sequence"],
                "input_id": inbox_row["input_id"],
                "payload": payload,
                "payload_hash": inbox_row["payload_hash"],
                "run_id": inbox_row["run_id"],
            }
        return
    if stream_name == "incidents":
        for alert_row in _alert_rows(connection, normalized_run_id):
            code = str(alert_row["code"])
            if str(alert_row["severity"]) != "CRITICAL" and alert_row[
                "commit_sequence"
            ] is not None and not (
                code.endswith("INTEGRITY_FAILURE") or code.endswith("CONFLICT")
            ):
                continue
            yield dict(alert_row)
        return
    if stream_name == "heads":
        run = _run_row(connection, normalized_run_id)
        first_input = connection.execute(
            "SELECT input_id, commit_hash FROM paper_inbox WHERE run_id=? ORDER BY commit_sequence LIMIT 1",
            (normalized_run_id,),
        ).fetchone()
        last_input = connection.execute(
            "SELECT input_id, commit_hash FROM paper_inbox WHERE run_id=? ORDER BY commit_sequence DESC LIMIT 1",
            (normalized_run_id,),
        ).fetchone()
        yield {
            "commit_count": int(run["commit_count"]),
            "commit_head_hash": str(run["commit_head_hash"]),
            "config_hash": str(run["config_hash"]),
            "event_count": int(run["event_count"]),
            "event_head_hash": str(run["event_head_hash"]),
            "first_commit_hash": (
                str(first_input["commit_hash"]) if first_input is not None else None
            ),
            "first_input_id": (str(first_input["input_id"]) if first_input is not None else None),
            "last_commit_hash": (str(last_input["commit_hash"]) if last_input is not None else None),
            "last_input_id": (str(last_input["input_id"]) if last_input is not None else None),
            "projection_hash": str(run["projection_hash"]),
            "projection_revision": int(run["projection_revision"]),
            "run_id": str(run["run_id"]),
            "status": str(run["status"]),
        }
        return
    raise AssertionError("exhaustive Golden stream dispatch")


def golden_replay_semantic_row(
    stream_name: str,
    row: Mapping[str, object],
) -> dict[str, JsonValue]:
    """Return the exact row identity compared after deterministic replay.

    SQL persistence timestamps and projection-history physical codec are source
    provenance, not replay semantics. Event/business timestamps inside payloads
    remain fully authoritative.
    """

    if stream_name not in GOLDEN_STREAM_NAMES:
        raise ValueError(f"unknown Golden V3 stream {stream_name!r}")
    decoded = json.loads(canonical_json(dict(row)))
    if not isinstance(decoded, dict):
        raise TypeError("Golden logical row must be an object")
    semantic = cast(dict[str, JsonValue], decoded)
    semantic.pop("created_at", None)
    semantic.pop("updated_at", None)
    if stream_name == "projection_history":
        semantic.pop("payload_codec", None)
    return semantic


def _row_identity(stream_name: str, row: Mapping[str, object]) -> RowIdentity:
    if stream_name == "schema":
        return (str(row["kind"]), str(row["name"]))
    if stream_name in {"run", "projection_current", "heads"}:
        return (str(row["run_id"]),)
    if stream_name in {"inbox", "runtime_sessions"}:
        return (int(cast(int, row["commit_sequence"])), str(row["input_id"]))
    if stream_name == "events":
        return (int(cast(int, row["sequence"])),)
    if stream_name == "ledger_transactions":
        return (
            int(cast(int, row["commit_sequence"])),
            int(cast(int, row["component_ordinal"])),
            str(row["transaction_id"]),
        )
    if stream_name == "ledger_entries":
        return (
            int(cast(int, row["commit_sequence"])),
            int(cast(int, row["transaction_ordinal"])),
            int(cast(int, row["entry_index"])),
            str(row["entry_id"]),
        )
    if stream_name in {"alerts", "incidents"}:
        commit = row.get("commit_sequence")
        if commit is not None:
            return (0, int(cast(int, commit)), int(cast(int, row["component_ordinal"])), str(row["alert_id"]))
        return (1, int(cast(int, row["event_sequence"])), str(row["alert_id"]))
    if stream_name == "commits":
        return (int(cast(int, row["commit_sequence"])),)
    if stream_name == "projection_history":
        return (int(cast(int, row["revision"])),)
    raise AssertionError("exhaustive Golden row identity")


def _validate_component_coverage(connection: sqlite3.Connection, run_id: str) -> None:
    component_specs = (
        (
            "event",
            "event_hashes_json",
            """
            SELECT count(*)
            FROM paper_commits AS commit_row
            JOIN json_each(commit_row.event_hashes_json) AS component
            JOIN paper_events AS event
              ON event.run_id=commit_row.run_id
             AND event.input_id=commit_row.input_id
             AND event.event_hash=component.value
            WHERE commit_row.run_id=?
            """,
            "paper_events",
            "",
        ),
        (
            "ledger",
            "ledger_hashes_json",
            """
            SELECT count(*)
            FROM paper_commits AS commit_row
            JOIN json_each(commit_row.ledger_hashes_json) AS component
            JOIN paper_ledger_transactions AS ledger
              ON ledger.run_id=commit_row.run_id
             AND ledger.input_id=commit_row.input_id
             AND ledger.transaction_hash=component.value
            WHERE commit_row.run_id=?
            """,
            "paper_ledger_transactions",
            "",
        ),
        (
            "alert",
            "alert_hashes_json",
            """
            SELECT count(*)
            FROM paper_commits AS commit_row
            JOIN json_each(commit_row.alert_hashes_json) AS component
            JOIN paper_alerts AS alert
              ON alert.run_id=commit_row.run_id
             AND alert.commit_sequence=commit_row.commit_sequence
             AND alert.payload_hash=component.value
            WHERE commit_row.run_id=?
            """,
            "paper_alerts",
            "AND commit_sequence IS NOT NULL",
        ),
    )
    for label, json_column, joined_sql, table, extra_where in component_specs:
        manifest_count = int(
            connection.execute(
                f"SELECT COALESCE(sum(json_array_length({json_column})),0) "
                "FROM paper_commits WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]
        )
        joined_count = int(connection.execute(joined_sql, (run_id,)).fetchone()[0])
        table_count = int(
            connection.execute(
                f"SELECT count(*) FROM {table} WHERE run_id=? {extra_where}",
                (run_id,),
            ).fetchone()[0]
        )
        if manifest_count != joined_count or joined_count != table_count:
            raise GoldenRefusal(f"commit {label} component coverage is not one-to-one")


def _validate_and_census(connection: sqlite3.Connection, run_id: str) -> dict[str, JsonValue]:
    integrity_rows = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
    if integrity_rows != ["ok"]:
        detail = integrity_rows[0] if integrity_rows else "no result"
        raise GoldenRefusal(f"SQLite integrity_check failed: {detail}")
    foreign_key_rows = list(connection.execute("PRAGMA foreign_key_check"))
    if foreign_key_rows:
        raise GoldenRefusal(
            "SQLite foreign_key_check found "
            f"{len(foreign_key_rows)} violation(s)"
        )
    _schema_rows(connection)
    run = _run_row(connection, run_id)
    if int(run["schema_version"]) != SCHEMA_VERSION:
        raise GoldenRefusal("selected run has an unsupported run-record schema")
    if str(run["status"]) not in {state.value for state in PaperState}:
        raise GoldenRefusal("selected run has an unknown terminal state")
    config = _canonical_payload(run["config_json"], run["config_hash"], label="run config")
    try:
        parsed_config = PaperRunConfig.from_dict(cast(Mapping[str, object], config))
    except (KeyError, TypeError, ValueError) as error:
        raise GoldenRefusal(f"selected run config cannot be reconstructed: {error}") from error
    if parsed_config.config_hash != str(run["config_hash"]) or parsed_config.run_id != run_id:
        raise GoldenRefusal("selected run config is not bound to run_id/config_hash")
    seed_text = str(run["seed_text"]) if run["seed_text"] is not None else None
    if seed_text != str(parsed_config.seed):
        raise GoldenRefusal("paper_runs.seed_text differs from the canonical run config")

    input_type_counts: defaultdict[str, int] = defaultdict(int)
    strategy_decisions: defaultdict[str, int] = defaultdict(int)
    inbox_count = 0
    first_input: dict[str, JsonValue] | None = None
    last_commit_hash: str | None = None
    for expected_commit, row in enumerate(
        iter_sqlite_logical_stream(connection, run_id, "inbox"),
        start=1,
    ):
        inbox_count += 1
        if int(cast(int, row["commit_sequence"])) != expected_commit:
            raise GoldenRefusal("paper inbox commit sequence is not continuous from one")
        payload = cast(dict[str, JsonValue], row["payload"])
        if canonical_sha256(payload) != row["payload_hash"]:
            raise GoldenRefusal(f"input {row['input_id']} payload hash differs")
        input_type = str(payload.get("input_type", ""))
        input_type_counts[input_type] += 1
        if input_type == "STRATEGY_DECISION":
            decision = payload.get("decision")
            if isinstance(decision, dict):
                strategy_decisions[str(decision.get("strategy_id", "LEGACY"))] += 1
        if first_input is None:
            first_input = row
        last_commit_hash = str(row["commit_hash"])
    if inbox_count != int(run["commit_count"]):
        raise GoldenRefusal("paper inbox count differs from the run commit head")
    expected_run_start = {
        "config_hash": str(run["config_hash"]),
        "input_type": "RUN_START",
        "run_id": run_id,
    }
    expected_run_start_id = deterministic_id("paper_input_run_started", run_id)
    if (
        first_input is None
        or first_input["input_id"] != expected_run_start_id
        or first_input["payload"] != expected_run_start
        or first_input["payload_hash"] != canonical_sha256(expected_run_start)
        or first_input["commit_sequence"] != 1
    ):
        raise GoldenRefusal("first canonical input is not the exact RUN_START prefix")

    event_type_counts: defaultdict[str, int] = defaultdict(int)
    expected_previous = _event_genesis(run_id, str(run["config_hash"]))
    event_count = 0
    for expected_sequence, row in enumerate(
        iter_sqlite_logical_stream(connection, run_id, "events"),
        start=1,
    ):
        event_count += 1
        sequence = int(cast(int, row["sequence"]))
        if sequence != expected_sequence:
            raise GoldenRefusal("paper event sequence is not continuous from one")
        payload = cast(dict[str, JsonValue], row["payload"])
        if canonical_sha256(payload) != row["payload_hash"]:
            raise GoldenRefusal(f"event {sequence} payload hash differs")
        if (
            payload.get("event_id") != row["event_id"]
            or payload.get("event_type") != row["event_type"]
            or payload.get("run_id") != run_id
        ):
            raise GoldenRefusal(f"event {sequence} columns differ from its payload")
        if row["previous_hash"] != expected_previous:
            raise GoldenRefusal(f"event {sequence} previous hash differs")
        calculated = _event_hash(
            sequence=sequence,
            payload=payload,
            previous_hash=str(row["previous_hash"]),
        )
        if calculated != row["event_hash"]:
            raise GoldenRefusal(f"event {sequence} chain hash differs")
        expected_previous = str(row["event_hash"])
        event_type_counts[str(row["event_type"])] += 1
    if event_count != int(run["event_count"]) or expected_previous != str(run["event_head_hash"]):
        raise GoldenRefusal("paper event count/head differs from the verified event chain")
    if event_type_counts.get("RUN_STARTED", 0) != 1:
        raise GoldenRefusal("event history must contain exactly one RUN_STARTED event")

    _validate_component_coverage(connection, run_id)
    commit_count = 0
    previous_commit = _commit_genesis(run_id, str(run["config_hash"]))
    prior_last_sequence = 0
    commit_rows = connection.execute(
        """
        SELECT
            commit_row.*,
            inbox.input_id AS inbox_input_id,
            inbox.commit_hash AS inbox_commit_hash,
            inbox.first_event_sequence AS inbox_first_event_sequence,
            inbox.last_event_sequence AS inbox_last_event_sequence,
            history.input_id AS history_input_id,
            history.revision AS history_revision,
            history.projection_hash AS history_projection_hash
        FROM paper_commits AS commit_row
        LEFT JOIN paper_inbox AS inbox
          ON inbox.run_id=commit_row.run_id
         AND inbox.commit_sequence=commit_row.commit_sequence
        LEFT JOIN paper_projection_history AS history
          ON history.run_id=commit_row.run_id
         AND history.revision=commit_row.commit_sequence
        WHERE commit_row.run_id=?
        ORDER BY commit_row.commit_sequence
        """,
        (run_id,),
    )
    for expected_sequence, row in enumerate(commit_rows, start=1):
        commit_count += 1
        sequence = int(row["commit_sequence"])
        if sequence != expected_sequence:
            raise GoldenRefusal("paper commit sequence is not continuous from one")
        event_hashes = _json_string_list(row["event_hashes_json"], label=f"commit {sequence} events")
        ledger_hashes = _json_string_list(
            row["ledger_hashes_json"], label=f"commit {sequence} ledger"
        )
        alert_hashes = _json_string_list(row["alert_hashes_json"], label=f"commit {sequence} alerts")
        first_event = (
            int(row["first_event_sequence"]) if row["first_event_sequence"] is not None else None
        )
        last_event = int(row["last_event_sequence"])
        if first_event is None or first_event != prior_last_sequence + 1:
            raise GoldenRefusal(f"commit {sequence} does not begin at the next event")
        if last_event != first_event + len(event_hashes) - 1:
            raise GoldenRefusal(f"commit {sequence} event range differs from its event hashes")
        prior_last_sequence = last_event
        if str(row["previous_commit_hash"]) != previous_commit:
            raise GoldenRefusal(f"commit {sequence} previous hash differs")
        calculated = _commit_hash(
            run_id=run_id,
            commit_sequence=sequence,
            input_id=str(row["input_id"]),
            first_event_sequence=first_event,
            last_event_sequence=last_event,
            event_hashes=event_hashes,
            ledger_hashes=ledger_hashes,
            projection_revision=int(row["projection_revision"]),
            projection_hash=str(row["projection_hash"]),
            alert_hashes=alert_hashes,
            previous_commit_hash=str(row["previous_commit_hash"]),
        )
        if calculated != str(row["commit_hash"]):
            raise GoldenRefusal(f"commit {sequence} hash differs")
        if (
            row["inbox_input_id"] is None
            or str(row["inbox_input_id"]) != str(row["input_id"])
            or str(row["inbox_commit_hash"]) != str(row["commit_hash"])
            or (
                int(row["inbox_first_event_sequence"])
                if row["inbox_first_event_sequence"] is not None
                else None
            )
            != first_event
            or int(row["inbox_last_event_sequence"]) != last_event
        ):
            raise GoldenRefusal(f"commit {sequence} differs from its inbox row")
        if (
            row["history_revision"] is None
            or int(row["projection_revision"]) != sequence
            or int(row["history_revision"]) != sequence
            or str(row["history_input_id"]) != str(row["input_id"])
            or str(row["history_projection_hash"]) != str(row["projection_hash"])
        ):
            raise GoldenRefusal(f"commit {sequence} differs from projection history")
        previous_commit = str(row["commit_hash"])
    if (
        commit_count != int(run["commit_count"])
        or prior_last_sequence != event_count
        or previous_commit != str(run["commit_head_hash"])
        or last_commit_hash != str(run["commit_head_hash"])
    ):
        raise GoldenRefusal("paper commit count/head does not cover the complete event journal")

    transaction_rows = iter_sqlite_logical_stream(connection, run_id, "ledger_transactions")
    entry_groups = groupby(
        iter_sqlite_logical_stream(connection, run_id, "ledger_entries"),
        key=lambda row: (
            int(cast(int, row["commit_sequence"])),
            int(cast(int, row["transaction_ordinal"])),
            str(row["transaction_id"]),
        ),
    )
    next_entry_group = next(entry_groups, None)
    transaction_count = 0
    ledger_entry_count = 0
    for transaction in transaction_rows:
        transaction_count += 1
        key = (
            int(cast(int, transaction["commit_sequence"])),
            int(cast(int, transaction["component_ordinal"])),
            str(transaction["transaction_id"]),
        )
        if next_entry_group is None or next_entry_group[0] != key:
            raise GoldenRefusal(f"ledger transaction {transaction['transaction_id']} has no exact entry group")
        entries = list(next_entry_group[1])
        next_entry_group = next(entry_groups, None)
        if len(entries) != int(cast(int, transaction["entry_count"])):
            raise GoldenRefusal(f"ledger transaction {transaction['transaction_id']} entry count differs")
        entry_hashes: list[str] = []
        balances: defaultdict[str, Decimal] = defaultdict(Decimal)
        event_ids: set[str] = set()
        for expected_index, entry in enumerate(entries):
            ledger_entry_count += 1
            if int(cast(int, entry["entry_index"])) != expected_index:
                raise GoldenRefusal(f"ledger transaction {transaction['transaction_id']} has an entry gap")
            payload = cast(dict[str, JsonValue], entry["payload"])
            if canonical_sha256(payload) != entry["entry_hash"]:
                raise GoldenRefusal(f"ledger entry {entry['entry_id']} hash differs")
            if (
                payload.get("entry_id") != entry["entry_id"]
                or payload.get("run_id") != run_id
                or payload.get("transaction_id") != transaction["transaction_id"]
                or payload.get("event_id") != entry["event_id"]
                or payload.get("account") != entry["account"]
                or payload.get("currency") != entry["unit"]
            ):
                raise GoldenRefusal(f"ledger entry {entry['entry_id']} metadata differs")
            try:
                amount = Decimal(str(entry["amount_text"]))
            except InvalidOperation as error:
                raise GoldenRefusal(f"ledger entry {entry['entry_id']} amount is invalid") from error
            if not amount.is_finite() or str(payload.get("amount")) != str(entry["amount_text"]):
                raise GoldenRefusal(f"ledger entry {entry['entry_id']} amount differs")
            balances[str(entry["unit"])] += amount
            if entry["event_id"] is None:
                raise GoldenRefusal(f"ledger entry {entry['entry_id']} has no event")
            event_ids.add(str(entry["event_id"]))
            entry_hashes.append(str(entry["entry_hash"]))
        if any(balance != 0 for balance in balances.values()) or len(event_ids) != 1:
            raise GoldenRefusal(f"ledger transaction {transaction['transaction_id']} is not exactly balanced")
        if _ledger_transaction_hash(run_id, str(transaction["transaction_id"]), entry_hashes) != str(
            transaction["transaction_hash"]
        ):
            raise GoldenRefusal(f"ledger transaction {transaction['transaction_id']} hash differs")
        event_sequence = connection.execute(
            "SELECT sequence FROM paper_events WHERE run_id=? AND event_id=?",
            (run_id, next(iter(event_ids))),
        ).fetchone()
        if event_sequence is None or int(event_sequence[0]) != int(cast(int, transaction["event_sequence"])):
            raise GoldenRefusal(f"ledger transaction {transaction['transaction_id']} event binding differs")
    if next_entry_group is not None:
        raise GoldenRefusal("ledger entries contain an orphan transaction group")
    raw_transaction_count = int(
        connection.execute(
            "SELECT count(*) FROM paper_ledger_transactions WHERE run_id=?", (run_id,)
        ).fetchone()[0]
    )
    raw_entry_count = int(
        connection.execute("SELECT count(*) FROM paper_ledger_entries WHERE run_id=?", (run_id,)).fetchone()[0]
    )
    if transaction_count != raw_transaction_count or ledger_entry_count != raw_entry_count:
        raise GoldenRefusal("logical ledger stream does not cover every source ledger row")

    alert_code_counts: defaultdict[str, int] = defaultdict(int)
    alert_severity_counts: defaultdict[str, int] = defaultdict(int)
    alert_count = 0
    uncommitted_alert_count = 0
    critical_alert_count = 0
    last_critical_received_at: str | None = None
    for alert in iter_sqlite_logical_stream(connection, run_id, "alerts"):
        alert_count += 1
        payload = cast(dict[str, JsonValue], alert["payload"])
        if canonical_sha256(payload) != alert["payload_hash"]:
            raise GoldenRefusal(f"alert {alert['alert_id']} hash differs")
        if payload.get("code") != alert["code"] or payload.get("severity") != alert["severity"]:
            raise GoldenRefusal(f"alert {alert['alert_id']} metadata differs")
        alert_code_counts[str(alert["code"])] += 1
        alert_severity_counts[str(alert["severity"])] += 1
        if alert["commit_sequence"] is None:
            uncommitted_alert_count += 1
        elif alert["severity"] == "CRITICAL":
            critical_alert_count += 1
            event = connection.execute(
                "SELECT event_type, payload_json FROM paper_events WHERE run_id=? AND sequence=?",
                (run_id, int(cast(int, alert["event_sequence"]))),
            ).fetchone()
            if event is None or str(event["event_type"]) != "ALERT_RAISED":
                raise GoldenRefusal(f"committed alert {alert['alert_id']} has no ALERT_RAISED event")
            event_payload = _json_object(event["payload_json"], label=f"alert event {alert['event_sequence']}")
            last_critical_received_at = str(event_payload.get("received_at"))
    raw_alert_count = int(
        connection.execute("SELECT count(*) FROM paper_alerts WHERE run_id=?", (run_id,)).fetchone()[0]
    )
    if alert_count != raw_alert_count:
        raise GoldenRefusal("logical alert stream does not cover every committed and uncommitted alert")

    history_count = 0
    latest_history: dict[str, JsonValue] | None = None
    for expected_revision, history in enumerate(
        iter_sqlite_logical_stream(connection, run_id, "projection_history")
    ):
        history_count += 1
        if int(cast(int, history["revision"])) != expected_revision:
            raise GoldenRefusal("projection history is not continuous from revision zero")
        payload = cast(dict[str, JsonValue], history["payload"])
        if canonical_sha256(payload) != history["projection_hash"]:
            raise GoldenRefusal(f"projection revision {expected_revision} hash differs")
        if expected_revision == 0 and (
            history["input_id"] is not None
            or history["event_sequence"] != 0
            or history["event_head_hash"] != _event_genesis(run_id, str(run["config_hash"]))
        ):
            raise GoldenRefusal("projection revision zero is not the exact genesis projection")
        latest_history = history
    if history_count != commit_count + 1 or latest_history is None:
        raise GoldenRefusal("projection history count differs from commit count plus revision zero")
    current_rows = list(iter_sqlite_logical_stream(connection, run_id, "projection_current"))
    if len(current_rows) != 1:
        raise GoldenRefusal("selected run must have exactly one current projection")
    current = current_rows[0]
    current_payload = cast(dict[str, JsonValue], current["payload"])
    if canonical_json(current_payload) != str(
        connection.execute(
            "SELECT payload_json FROM paper_projections WHERE run_id=?", (run_id,)
        ).fetchone()[0]
    ):
        raise GoldenRefusal("current projection is not canonical JSON")
    if canonical_sha256(current_payload) != current["projection_hash"]:
        raise GoldenRefusal("current projection hash differs")
    if (
        current["revision"] != latest_history["revision"]
        or current["event_sequence"] != latest_history["event_sequence"]
        or current["event_head_hash"] != latest_history["event_head_hash"]
        or current["projection_hash"] != latest_history["projection_hash"]
        or current["payload"] != latest_history["payload"]
        or current["revision"] != int(run["projection_revision"])
        or current["projection_hash"] != str(run["projection_hash"])
    ):
        raise GoldenRefusal("current projection, latest history, and run heads differ")

    raw_incident_count = current_payload.get("critical_incident_count", 0)
    raw_last_incident = current_payload.get("last_critical_incident_at")
    if (
        isinstance(raw_incident_count, bool)
        or not isinstance(raw_incident_count, int)
        or raw_incident_count != critical_alert_count
        or (critical_alert_count == 0) != (raw_last_incident is None)
        or (critical_alert_count > 0 and raw_last_incident != last_critical_received_at)
    ):
        raise GoldenRefusal("projection critical-incident summary differs from committed CRITICAL alerts")

    strategy_ids = [item.strategy_id for item in parsed_config.strategy_configs]
    coverage_gaps: list[str] = ["REPLAY_NOT_PERFORMED"]
    required_strategies = {"phase05_cash_and_carry", "phase08_robust_pairs"}
    if not required_strategies.issubset(strategy_ids):
        coverage_gaps.append("PHASE05_PHASE08_NOT_BOTH_FROZEN")
    if any(strategy_decisions.get(strategy_id, 0) == 0 for strategy_id in required_strategies):
        coverage_gaps.append("PHASE05_PHASE08_DECISIONS_NOT_BOTH_OBSERVED")
    if event_type_counts.get("ORDER_FILLED", 0) + event_type_counts.get("ORDER_PARTIALLY_FILLED", 0) == 0:
        coverage_gaps.append("NO_FILL_COVERAGE")
    if input_type_counts.get("PUBLIC_FUNDING_SETTLEMENT", 0) == 0:
        coverage_gaps.append("NO_FUNDING_SETTLEMENT_COVERAGE")
    if input_type_counts.get("TIMER", 0) == 0:
        coverage_gaps.append("NO_TIMER_COVERAGE")
    if input_type_counts.get("RECONCILE", 0) == 0 or event_type_counts.get(
        "RECONCILIATION_SUCCEEDED", 0
    ) == 0:
        coverage_gaps.append("NO_SUCCESSFUL_RECONCILE_COVERAGE")
    if uncommitted_alert_count:
        coverage_gaps.append("UNCOMMITTED_ALERTS_PRESENT")
    if any(code.endswith("INTEGRITY_FAILURE") or code.endswith("CONFLICT") for code in alert_code_counts):
        coverage_gaps.append("NON_REPLAYABLE_GUARD_ALERT_PRESENT")

    runtime_start_count = input_type_counts.get("RUNTIME_SESSION_STARTED", 0)
    runtime_stop_count = input_type_counts.get("RUNTIME_SESSION_STOPPED", 0)
    return {
        "alert_code_counts": dict(sorted(alert_code_counts.items())),
        "alert_count": alert_count,
        "alert_severity_counts": dict(sorted(alert_severity_counts.items())),
        "commit_count": commit_count,
        "coverage_gaps": _json_array(sorted(set(coverage_gaps))),
        "event_count": event_count,
        "event_type_counts": dict(sorted(event_type_counts.items())),
        "input_type_counts": dict(sorted(input_type_counts.items())),
        "integrity_status": "PASS",
        "ledger_entry_count": ledger_entry_count,
        "ledger_transaction_count": transaction_count,
        "projection_history_count": history_count,
        "runtime_session_start_count": runtime_start_count,
        "runtime_session_stop_count": runtime_stop_count,
        "sqlite_foreign_key_violation_count": len(foreign_key_rows),
        "sqlite_integrity_check": integrity_rows[0],
        "status": "GOLDEN_CANDIDATE",
        "strategy_decision_counts": dict(sorted(strategy_decisions.items())),
        "strategy_ids": _json_array(strategy_ids),
        "terminal_projection_state": current_payload.get("state"),
        "uncommitted_alert_count": uncommitted_alert_count,
    }


def _fsync_directory(path: Path) -> None:
    """Durably publish directory entries on POSIX and Windows or fail closed."""

    if os.name != "nt":
        descriptor = os.open(
            path,
            os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0)),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    flush_file_buffers = kernel32.FlushFileBuffers
    flush_file_buffers.argtypes = [wintypes.HANDLE]
    flush_file_buffers.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL

    handle = create_file(
        str(path),
        0x40000000,  # GENERIC_WRITE, required by FlushFileBuffers.
        0x00000007,  # FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE.
        None,
        3,  # OPEN_EXISTING.
        0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS permits directory handles.
        None,
    )
    invalid_handle = wintypes.HANDLE(-1).value
    if handle == invalid_handle:
        raise ctypes.WinError(ctypes.get_last_error())

    flushed = False
    flush_error = 0
    try:
        flushed = bool(flush_file_buffers(handle))
        flush_error = ctypes.get_last_error() if not flushed else 0
    finally:
        closed = bool(close_handle(handle))
        close_error = ctypes.get_last_error() if not closed else 0
    if not flushed:
        raise ctypes.WinError(flush_error)
    if not closed:
        raise ctypes.WinError(close_error)


def _mkdir_durable(path: Path, *, exist_ok: bool) -> bool:
    """Create every missing directory entry and durably publish it in order."""

    if path.exists():
        path.mkdir(exist_ok=exist_ok)
        return False

    missing: list[Path] = []
    current = path
    while not current.exists():
        parent = current.parent
        if parent == current:
            raise FileNotFoundError(f"no existing ancestor for directory: {path}")
        missing.append(current)
        current = parent
    if not current.is_dir():
        raise NotADirectoryError(f"directory ancestor is not a directory: {current}")

    for directory in reversed(missing):
        directory.mkdir(exist_ok=False)
        _fsync_directory(directory.parent)
    return True


def _write_new_file(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _write_atomic_new(path: Path, payload: bytes) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    _write_new_file(temporary, payload)
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _emit_progress(progress: ProgressCallback | None, **fields: object) -> None:
    if progress is not None:
        progress(fields)


def _export_stream(
    output_root: Path,
    stream_name: str,
    rows: Iterable[dict[str, JsonValue]],
    *,
    shard_rows: int,
    shard_bytes: int,
    progress: ProgressCallback | None,
) -> dict[str, JsonValue]:
    stream_hasher = hashlib.sha256()
    replay_hasher = hashlib.sha256()
    stream_size = 0
    replay_size = 0
    row_count = 0
    first_identity: RowIdentity | None = None
    last_identity: RowIdentity | None = None
    shards: list[JsonValue] = []
    shard_index = 0
    shard_handle: Any | None = None
    shard_path: Path | None = None
    shard_hasher = hashlib.sha256()
    shard_count = 0
    shard_size = 0
    shard_first: RowIdentity | None = None
    shard_last: RowIdentity | None = None
    last_progress_at = monotonic()

    def close_shard(*, publish: bool) -> None:
        nonlocal shard_handle, shard_path, shard_hasher, shard_count, shard_size
        nonlocal shard_first, shard_last
        if shard_handle is None or shard_path is None:
            return
        shard_handle.flush()
        os.fsync(shard_handle.fileno())
        shard_handle.close()
        if publish:
            shards.append(
                {
                    "first_identity": (
                        _json_array(shard_first) if shard_first is not None else None
                    ),
                    "last_identity": (
                        _json_array(shard_last) if shard_last is not None else None
                    ),
                    "logical_sha256": shard_hasher.hexdigest(),
                    "logical_size": shard_size,
                    "path": shard_path.relative_to(output_root).as_posix(),
                    "physical_sha256": shard_hasher.hexdigest(),
                    "physical_size": shard_size,
                    "row_count": shard_count,
                }
            )
        shard_handle = None
        shard_path = None
        shard_hasher = hashlib.sha256()
        shard_count = 0
        shard_size = 0
        shard_first = None
        shard_last = None

    try:
        for row in rows:
            line = _canonical_line(row)
            semantic_line = _canonical_line(golden_replay_semantic_row(stream_name, row))
            if shard_handle is not None and (
                shard_count >= shard_rows or (shard_count > 0 and shard_size + len(line) > shard_bytes)
            ):
                close_shard(publish=True)
            if shard_handle is None:
                shard_index += 1
                shard_path = output_root / "streams" / f"{stream_name}-{shard_index:06d}.jsonl"
                shard_handle = shard_path.open("xb")
            identity = _row_identity(stream_name, row)
            if last_identity is not None and identity <= last_identity:
                raise GoldenRefusal(f"{stream_name} rows are not in strict canonical order")
            if first_identity is None:
                first_identity = identity
            last_identity = identity
            if shard_first is None:
                shard_first = identity
            shard_last = identity
            shard_handle.write(line)
            shard_hasher.update(line)
            shard_count += 1
            shard_size += len(line)
            stream_hasher.update(line)
            replay_hasher.update(semantic_line)
            stream_size += len(line)
            replay_size += len(semantic_line)
            row_count += 1
            progress_at = monotonic()
            if (
                row_count == 1
                or row_count % _PROGRESS_ROW_INTERVAL == 0
                or progress_at - last_progress_at >= _PROGRESS_HEARTBEAT_SECONDS
            ):
                _emit_progress(
                    progress,
                    bytes_completed=stream_size,
                    last_identity=_json_array(identity),
                    phase="stream",
                    rows_completed=row_count,
                    stream=stream_name,
                )
                last_progress_at = progress_at
        close_shard(publish=True)
    except BaseException:
        close_shard(publish=False)
        raise
    return {
        "first_identity": (
            _json_array(first_identity) if first_identity is not None else None
        ),
        "last_identity": _json_array(last_identity) if last_identity is not None else None,
        "logical_sha256": stream_hasher.hexdigest(),
        "logical_size": stream_size,
        "order": list(_STREAM_ORDERS[stream_name]),
        "replay_sha256": replay_hasher.hexdigest(),
        "replay_size": replay_size,
        "row_count": row_count,
        "shards": shards,
    }


def _manifest_root(manifest: Mapping[str, object]) -> str:
    material = dict(manifest)
    material.pop("root_hash", None)
    return canonical_sha256(
        {
            "domain": "hyperlab-paper-golden-v3-root-v1",
            "manifest": material,
        }
    )


def export_golden_v3(
    source: Path | str,
    output_root: Path | str,
    run_id: str,
    *,
    sentinel_path: Path | str,
    require_readonly: bool = True,
    shard_rows: int = _DEFAULT_SHARD_ROWS,
    shard_bytes: int = _DEFAULT_SHARD_BYTES,
    progress: ProgressCallback | None = None,
    expected_source_size: int | None = None,
    expected_source_sha256: str | None = None,
) -> GoldenExportResult:
    """Export one complete PaperStore-v3 run through one coherent read snapshot."""

    normalized_run_id = _digest_text(run_id, label="run_id")
    if isinstance(shard_rows, bool) or not isinstance(shard_rows, int) or shard_rows <= 0:
        raise ValueError("shard_rows must be a positive integer")
    if isinstance(shard_bytes, bool) or not isinstance(shard_bytes, int) or shard_bytes <= 0:
        raise ValueError("shard_bytes must be a positive integer")
    if progress is not None and not callable(progress):
        raise TypeError("progress must be callable or None")
    if expected_source_size is not None and (
        isinstance(expected_source_size, bool)
        or not isinstance(expected_source_size, int)
        or expected_source_size <= 0
    ):
        raise ValueError("expected_source_size must be a positive integer or None")
    if expected_source_sha256 is not None:
        expected_source_sha256 = _digest_text(
            expected_source_sha256,
            label="expected_source_sha256",
        )

    _emit_progress(progress, phase="preflight", rows_completed=0)
    resolved_source, candidate, _sentinel, before = _validate_paths(
        Path(source),
        Path(output_root),
        Path(sentinel_path),
        require_readonly=require_readonly,
    )
    if expected_source_size is not None and before.stat.size != expected_source_size:
        raise GoldenRefusal("source size differs from expected_source_size")
    if expected_source_sha256 is not None and before.sha256 != expected_source_sha256:
        raise GoldenRefusal("source SHA-256 differs from expected_source_sha256")

    _mkdir_durable(candidate, exist_ok=False)
    streams_root = candidate / "streams"
    streams_root.mkdir()
    incomplete_path = candidate / "INCOMPLETE"
    _write_new_file(
        incomplete_path,
        _canonical_line(
            {
                "format": GOLDEN_FORMAT,
                "run_id": normalized_run_id,
                "status": "INCOMPLETE",
            }
        ),
    )
    _fsync_directory(candidate)

    with _readonly_snapshot(resolved_source) as connection:
        census = _validate_and_census(connection, normalized_run_id)
        _emit_progress(progress, phase="census", rows_completed=0, status=census["status"])
        stream_manifests: dict[str, JsonValue] = {}
        for stream_name in GOLDEN_STREAM_NAMES:
            stream_manifests[stream_name] = _export_stream(
                candidate,
                stream_name,
                iter_sqlite_logical_stream(connection, normalized_run_id, stream_name),
                shard_rows=shard_rows,
                shard_bytes=shard_bytes,
                progress=progress,
            )
    _fsync_directory(streams_root)

    if _sqlite_sidecars(resolved_source):
        raise GoldenRefusal("source SQLite sidecars appeared during Golden extraction")
    after = _fingerprint_source(resolved_source)
    if after.sha256 != before.sha256 or after.stat != before.stat or after.sidecars != before.sidecars:
        raise GoldenRefusal("source stat or SHA-256 changed during Golden extraction")
    manifest: dict[str, object] = {
        "canonicalization": {
            "encoding": "UTF-8",
            "json": "sorted-keys,compact-separators,ensure-ascii-false,finite-values",
            "line_ending": "LF",
            "physical_compression": "none",
            "replay_excludes": [
                "sql-created_at",
                "sql-updated_at",
                "projection-history-payload-codec",
                "projection-history-compressed-bytes",
            ],
        },
        "census": census,
        "format": GOLDEN_FORMAT,
        "run_id": normalized_run_id,
        "schema_version": GOLDEN_TOOL_VERSION,
        "source": before.to_dict(),
        "source_unchanged": True,
        "streams": stream_manifests,
        "tool_version": GOLDEN_TOOL_VERSION,
    }
    root_hash = _manifest_root(manifest)
    manifest["root_hash"] = root_hash
    manifest_payload = _canonical_line(manifest)
    manifest_path = candidate / "manifest.json"
    _write_atomic_new(manifest_path, manifest_payload)
    complete_payload = _canonical_line(
        {
            "format": GOLDEN_FORMAT,
            "manifest_sha256": hashlib.sha256(manifest_payload).hexdigest(),
            "root_hash": root_hash,
            "run_id": normalized_run_id,
            "status": "COMPLETE",
        }
    )
    with incomplete_path.open("wb") as handle:
        handle.write(complete_payload)
        handle.flush()
        os.fsync(handle.fileno())
    complete_path = candidate / "COMPLETE"
    os.replace(incomplete_path, complete_path)
    _fsync_directory(candidate)
    _emit_progress(progress, phase="complete", rows_completed=0, root_hash=root_hash)
    return GoldenExportResult(
        output_root=candidate,
        manifest_path=manifest_path,
        complete_path=complete_path,
        root_hash=root_hash,
        census_status=str(census["status"]),
        source_sha256=before.sha256,
    )


def _read_json_file(path: Path, *, label: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise GoldenVerificationError(f"{label} is missing or unreadable") from error
    if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
        raise GoldenVerificationError(f"{label} must be one canonical LF-terminated JSON record")
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GoldenVerificationError(f"{label} is not valid UTF-8 JSON") from error
    if not isinstance(decoded, dict) or _canonical_line(decoded) != raw:
        raise GoldenVerificationError(f"{label} is not canonical JSON")
    return cast(dict[str, Any], decoded)


def load_golden_manifest(
    export_root: Path | str,
    *,
    require_complete: bool = True,
) -> dict[str, Any]:
    root = Path(export_root)
    if not root.is_dir() or root.is_symlink() or _has_reparse_component(root):
        raise GoldenVerificationError("Golden export root is missing or uses a symlink/reparse path")
    if require_complete and not (root / "COMPLETE").is_file():
        raise GoldenVerificationError("Golden export is incomplete because COMPLETE is missing")
    return _read_json_file(root / "manifest.json", label="manifest")


def _manifest_streams(manifest: Mapping[str, Any]) -> Mapping[str, Mapping[str, Any]]:
    raw = manifest.get("streams")
    if not isinstance(raw, dict) or set(raw) != set(GOLDEN_STREAM_NAMES):
        raise GoldenVerificationError("manifest stream set differs from the Golden V3 contract")
    if any(not isinstance(value, dict) for value in raw.values()):
        raise GoldenVerificationError("manifest stream descriptors must be objects")
    return cast(Mapping[str, Mapping[str, Any]], raw)


def _safe_shard_path(root: Path, raw_path: object) -> Path:
    if not isinstance(raw_path, str):
        raise GoldenVerificationError("manifest shard path must be a string")
    relative = Path(raw_path)
    if relative.is_absolute() or ".." in relative.parts or not raw_path.endswith(".jsonl"):
        raise GoldenVerificationError("manifest shard path escapes the export or has a wrong suffix")
    candidate = root / relative
    if candidate.is_symlink() or _has_reparse_component(candidate):
        raise GoldenVerificationError("manifest shard uses a symlink, junction, or reparse path")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        raise GoldenVerificationError("manifest shard is missing") from error
    if root.resolve(strict=True) not in resolved.parents:
        raise GoldenVerificationError("manifest shard escapes the export root")
    return resolved


def _iter_stream_rows_unverified(
    root: Path,
    stream_name: str,
    stream: Mapping[str, Any],
) -> Iterator[dict[str, JsonValue]]:
    shards = stream.get("shards")
    if not isinstance(shards, list):
        raise GoldenVerificationError(f"manifest {stream_name} shards must be an array")
    for shard in shards:
        if not isinstance(shard, dict):
            raise GoldenVerificationError(f"manifest {stream_name} shard descriptor is invalid")
        path = _safe_shard_path(root, shard.get("path"))
        try:
            raw = path.read_bytes()
        except OSError as error:
            raise GoldenVerificationError(f"{stream_name} shard is unreadable") from error
        if not raw or not raw.endswith(b"\n"):
            raise GoldenVerificationError(f"{stream_name} shard is empty or truncated")
        if len(raw) != _required_json_int(
            shard.get("physical_size"),
            label=f"{stream_name}.shard.physical_size",
        ):
            raise GoldenVerificationError(f"{stream_name} shard physical size differs")
        physical_hash = hashlib.sha256(raw).hexdigest()
        if physical_hash != shard.get("physical_sha256"):
            raise GoldenVerificationError(f"{stream_name} shard physical hash differs")
        lines = raw.splitlines(keepends=True)
        if len(lines) != _required_json_int(
            shard.get("row_count"),
            label=f"{stream_name}.shard.row_count",
        ):
            raise GoldenVerificationError(f"{stream_name} shard row count differs")
        shard_first: RowIdentity | None = None
        shard_last: RowIdentity | None = None
        logical = hashlib.sha256()
        for line in lines:
            try:
                decoded = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise GoldenVerificationError(f"{stream_name} shard contains invalid JSON") from error
            if not isinstance(decoded, dict) or _canonical_line(decoded) != line:
                raise GoldenVerificationError(f"{stream_name} shard contains a noncanonical row")
            row = cast(dict[str, JsonValue], decoded)
            identity = _row_identity(stream_name, row)
            if shard_last is not None and identity <= shard_last:
                raise GoldenVerificationError(f"{stream_name} shard row order differs")
            if shard_first is None:
                shard_first = identity
            shard_last = identity
            logical.update(line)
            yield row
        if logical.hexdigest() != shard.get("logical_sha256"):
            raise GoldenVerificationError(f"{stream_name} shard logical hash differs")
        if len(raw) != _required_json_int(
            shard.get("logical_size"),
            label=f"{stream_name}.shard.logical_size",
        ):
            raise GoldenVerificationError(f"{stream_name} shard logical size differs")
        if shard_first is None or shard_last is None:
            raise GoldenVerificationError(f"{stream_name} shard is empty")
        if _json_array(shard_first) != shard.get("first_identity") or _json_array(
            shard_last
        ) != shard.get("last_identity"):
            raise GoldenVerificationError(f"{stream_name} shard boundary identity differs")


def _verify_stream(
    root: Path,
    stream_name: str,
    stream: Mapping[str, Any],
) -> set[Path]:
    if stream.get("order") != list(_STREAM_ORDERS[stream_name]):
        raise GoldenVerificationError(f"manifest {stream_name} canonical order differs")
    logical = hashlib.sha256()
    replay = hashlib.sha256()
    logical_size = 0
    replay_size = 0
    count = 0
    first: RowIdentity | None = None
    last: RowIdentity | None = None
    expected_paths: set[Path] = set()
    shards = stream.get("shards")
    if not isinstance(shards, list):
        raise GoldenVerificationError(f"manifest {stream_name} shards must be an array")
    for shard in shards:
        if not isinstance(shard, dict):
            raise GoldenVerificationError(f"manifest {stream_name} shard descriptor is invalid")
        path = _safe_shard_path(root, shard.get("path"))
        if path in expected_paths:
            raise GoldenVerificationError(f"manifest {stream_name} repeats a shard path")
        expected_paths.add(path)
    for row in _iter_stream_rows_unverified(root, stream_name, stream):
        identity = _row_identity(stream_name, row)
        if last is not None and identity <= last:
            raise GoldenVerificationError(f"{stream_name} rows are reordered or duplicated")
        if first is None:
            first = identity
        last = identity
        line = _canonical_line(row)
        semantic_line = _canonical_line(golden_replay_semantic_row(stream_name, row))
        logical.update(line)
        replay.update(semantic_line)
        logical_size += len(line)
        replay_size += len(semantic_line)
        count += 1
    if count != _required_json_int(
        stream.get("row_count"), label=f"{stream_name}.row_count"
    ):
        raise GoldenVerificationError(f"{stream_name} manifest row count differs")
    if logical.hexdigest() != stream.get("logical_sha256"):
        raise GoldenVerificationError(f"{stream_name} manifest logical hash differs")
    if replay.hexdigest() != stream.get("replay_sha256"):
        raise GoldenVerificationError(f"{stream_name} manifest replay hash differs")
    if logical_size != _required_json_int(
        stream.get("logical_size"), label=f"{stream_name}.logical_size"
    ):
        raise GoldenVerificationError(f"{stream_name} manifest logical size differs")
    if replay_size != _required_json_int(
        stream.get("replay_size"), label=f"{stream_name}.replay_size"
    ):
        raise GoldenVerificationError(f"{stream_name} manifest replay size differs")
    if (_json_array(first) if first is not None else None) != stream.get("first_identity"):
        raise GoldenVerificationError(f"{stream_name} first identity differs")
    if (_json_array(last) if last is not None else None) != stream.get("last_identity"):
        raise GoldenVerificationError(f"{stream_name} last identity differs")
    if stream_name in {"inbox", "commits"}:
        sequences = [
            _required_json_int(
                row["commit_sequence"], label=f"{stream_name}.commit_sequence"
            )
            for row in _iter_stream_rows_unverified(root, stream_name, stream)
        ]
        if sequences != list(range(1, len(sequences) + 1)):
            raise GoldenVerificationError(f"{stream_name} sequence has a gap")
    elif stream_name == "events":
        sequences = [
            _required_json_int(row["sequence"], label="events.sequence")
            for row in _iter_stream_rows_unverified(root, stream_name, stream)
        ]
        if sequences != list(range(1, len(sequences) + 1)):
            raise GoldenVerificationError("events sequence has a gap")
    elif stream_name == "projection_history":
        revisions = [
            _required_json_int(row["revision"], label="projection_history.revision")
            for row in _iter_stream_rows_unverified(root, stream_name, stream)
        ]
        if not revisions or revisions != list(range(len(revisions))):
            raise GoldenVerificationError("projection history is missing revision zero or has a gap")
    return expected_paths


def _verify_no_extras(root: Path, expected_files: set[Path]) -> None:
    actual_files: set[Path] = set()
    actual_directories: set[Path] = set()
    for path in root.rglob("*"):
        if path.is_symlink() or _has_reparse_component(path):
            raise GoldenVerificationError("Golden export contains a symlink, junction, or reparse path")
        resolved = path.resolve(strict=True)
        if path.is_file():
            actual_files.add(resolved)
        elif path.is_dir():
            actual_directories.add(resolved)
        else:
            raise GoldenVerificationError("Golden export contains an unsupported filesystem object")
    if actual_files != expected_files:
        raise GoldenVerificationError("Golden export has missing or extra files")
    expected_directories = {(root / "streams").resolve(strict=True)}
    if actual_directories != expected_directories:
        raise GoldenVerificationError("Golden export has missing or extra directories")


def verify_golden_v3(
    export_root: Path | str,
    *,
    pin_path: Path | str | None = None,
) -> GoldenVerification:
    """Verify COMPLETE, manifest/root, every shard, logical order, and optional pin."""

    root = Path(export_root)
    manifest = load_golden_manifest(root, require_complete=True)
    complete = _read_json_file(root / "COMPLETE", label="COMPLETE marker")
    manifest_path = (root / "manifest.json").resolve(strict=True)
    complete_path = (root / "COMPLETE").resolve(strict=True)
    manifest_payload = _canonical_line(manifest)
    if manifest.get("format") != GOLDEN_FORMAT or complete.get("format") != GOLDEN_FORMAT:
        raise GoldenVerificationError("manifest/COMPLETE format is not Golden V3")
    calculated_root = _manifest_root(manifest)
    if manifest.get("root_hash") != calculated_root:
        raise GoldenVerificationError("manifest root hash differs")
    if (
        complete.get("status") != "COMPLETE"
        or complete.get("root_hash") != calculated_root
        or complete.get("run_id") != manifest.get("run_id")
        or complete.get("manifest_sha256") != hashlib.sha256(manifest_payload).hexdigest()
    ):
        raise GoldenVerificationError("COMPLETE marker does not authenticate the manifest/root")
    streams = _manifest_streams(manifest)
    expected_files = {manifest_path, complete_path}
    for stream_name in GOLDEN_STREAM_NAMES:
        expected_files.update(_verify_stream(root, stream_name, streams[stream_name]))
    _verify_no_extras(root, expected_files)

    if pin_path is not None:
        pin = Path(pin_path)
        if pin.is_symlink() or _has_reparse_component(pin):
            raise GoldenVerificationError(
                "external pin contains a symlink, junction, or reparse path"
            )
        if not pin.is_file():
            raise GoldenVerificationError("external pin is not a regular file")
        pin_stat = SourceStat.read(pin)
        if pin_stat.link_count != 1:
            raise GoldenVerificationError("external pin hardlinks are forbidden")
        resolved_pin = pin.resolve(strict=True)
        if root.resolve(strict=True) in resolved_pin.parents:
            raise GoldenVerificationError("external pin must be distinct from the Golden export")
        external = _read_json_file(pin, label="external pin")
        if not pin_stat.readonly:
            raise GoldenVerificationError("external pin is not read-only")
        if (
            external.get("format") != "hyperlab-paper-golden-v3-external-pin-v1"
            or external.get("root_hash") != calculated_root
            or external.get("run_id") != manifest.get("run_id")
            or external.get("manifest_sha256") != hashlib.sha256(manifest_payload).hexdigest()
        ):
            raise GoldenVerificationError("external pin root differs from the Golden manifest")
    return GoldenVerification(root.resolve(strict=True), calculated_root, manifest)


def _reuse_golden_verification(
    export_root: Path | str,
    verification: GoldenVerification | None,
) -> GoldenVerification:
    if verification is None:
        return verify_golden_v3(export_root)
    root = Path(export_root).resolve(strict=True)
    if verification.export_root != root:
        raise GoldenVerificationError(
            "verified Golden root differs from the requested export root"
        )
    current_manifest = load_golden_manifest(root, require_complete=True)
    if (
        current_manifest != verification.manifest
        or _manifest_root(current_manifest) != verification.root_hash
    ):
        raise GoldenVerificationError(
            "verified Golden manifest changed after exhaustive verification"
        )
    return verification


def iter_golden_stream(
    export_root: Path | str,
    stream_name: str,
    *,
    verification: GoldenVerification | None = None,
) -> Iterator[dict[str, JsonValue]]:
    """Verify an export, then iterate one canonical logical stream."""

    root = Path(export_root).resolve(strict=True)
    checked = verification or verify_golden_v3(root)
    if checked.export_root != root:
        raise GoldenVerificationError(
            "verified Golden root differs from the requested export root"
        )
    streams = _manifest_streams(checked.manifest)
    if stream_name not in streams:
        raise ValueError(f"unknown Golden V3 stream {stream_name!r}")
    yield from _iter_stream_rows_unverified(
        checked.export_root,
        stream_name,
        streams[stream_name],
    )


def compare_golden_exports(
    expected_root: Path | str,
    actual_root: Path | str,
    *,
    expected_verification: GoldenVerification | None = None,
    actual_verification: GoldenVerification | None = None,
) -> GoldenDifferentialResult:
    """Compare two complete exports and wrap every verification mismatch."""

    try:
        expected = _reuse_golden_verification(expected_root, expected_verification)
        actual = _reuse_golden_verification(actual_root, actual_verification)
    except GoldenVerificationError as error:
        raise GoldenDifferentialError(f"Golden export verification failed: {error}") from error
    expected_streams = _manifest_streams(expected.manifest)
    actual_streams = _manifest_streams(actual.manifest)
    mismatches: list[str] = []
    for stream_name in GOLDEN_STREAM_NAMES:
        for field in ("row_count", "logical_sha256", "replay_sha256"):
            if expected_streams[stream_name].get(field) != actual_streams[stream_name].get(field):
                mismatches.append(f"{stream_name}.{field}")
    if expected.root_hash != actual.root_hash:
        mismatches.append("root_hash")
    if mismatches:
        raise GoldenDifferentialError(
            "Golden logical histories differ: " + ", ".join(sorted(set(mismatches)))
        )
    return GoldenDifferentialResult(
        expected_root=expected.export_root,
        actual_root=actual.export_root,
        root_hash=expected.root_hash,
    )


def write_external_pin(
    export_root: Path | str,
    pin_path: Path | str,
    *,
    verification: GoldenVerification | None = None,
    forbidden_paths: Iterable[Path | str] = (),
) -> Path:
    """Write one distinct, local read-only pin for a verified Golden root."""

    verification = _reuse_golden_verification(export_root, verification)
    pin = validate_new_auxiliary_path(
        pin_path,
        forbidden_paths=(verification.export_root, *forbidden_paths),
        label="external pin",
        required_suffix=None,
        require_existing_parent=False,
    )
    _mkdir_durable(pin.parent, exist_ok=True)
    if _has_reparse_component(pin.parent):
        raise GoldenRefusal("external pin parent contains a symlink, junction, or reparse path")
    manifest_payload = (verification.export_root / "manifest.json").read_bytes()
    payload = _canonical_line(
        {
            "format": "hyperlab-paper-golden-v3-external-pin-v1",
            "manifest_sha256": hashlib.sha256(manifest_payload).hexdigest(),
            "root_hash": verification.root_hash,
            "run_id": verification.manifest["run_id"],
        }
    )
    _write_atomic_new(pin, payload)
    pin.chmod(stat.S_IREAD)
    if not SourceStat.read(pin).readonly:
        raise GoldenRefusal("external pin could not be made read-only")
    _fsync_directory(pin.parent)
    return pin


__all__ = [
    "GOLDEN_FORMAT",
    "GOLDEN_STREAM_NAMES",
    "GoldenDifferentialError",
    "GoldenDifferentialResult",
    "GoldenExportResult",
    "GoldenRefusal",
    "GoldenVerification",
    "GoldenVerificationError",
    "compare_golden_exports",
    "export_golden_v3",
    "golden_replay_semantic_row",
    "iter_golden_stream",
    "iter_sqlite_logical_stream",
    "load_golden_manifest",
    "validate_new_auxiliary_path",
    "verify_golden_v3",
    "write_external_pin",
]
