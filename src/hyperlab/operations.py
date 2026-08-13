from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tomllib
import uuid
from collections.abc import Iterator, Mapping
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from hyperlab import __version__
from hyperlab.collector.storage import (
    LakeWriterActiveError,
    ensure_storage_capacity,
    exclusive_lake_maintenance,
)
from hyperlab.config import load_settings
from hyperlab.data.lake import inventory_partitions
from hyperlab.storage.sqlite import write_runtime_status

BACKUP_SCHEMA_VERSION = 1
REQUIRED_PERSISTENT_DIRECTORIES = (
    "backups",
    "config",
    "market",
    "paper",
    "reports",
    "runtime",
)
VOLUME_MARKER_NAME = ".hyperlab-volume"
_VOLUME_MARKER_CONTENT = {
    name: f"hyperlab-{name}-volume-v1\n".encode() for name in REQUIRED_PERSISTENT_DIRECTORIES
}
_FORBIDDEN_SECRET_SUFFIXES = {".key", ".keystore", ".p12", ".pem", ".pfx"}
_BACKUP_ID = re.compile(r"[0-9A-Za-z][0-9A-Za-z._-]{0,95}")
_EXPORT_NAME = re.compile(r"[0-9A-Za-z][0-9A-Za-z._-]{0,95}\.parquet")
_PRIVATE_CREDENTIAL_NAME = "private" + "_key"
_SEED_CREDENTIAL_NAME = "seed" + "_phrase"
_SECRET_BYTE_MARKERS = tuple(
    marker.casefold().encode()
    for marker in (
        "api_key",
        "api-key",
        "mnemonic",
        _PRIVATE_CREDENTIAL_NAME,
        "private-key",
        _SEED_CREDENTIAL_NAME,
        "seed-phrase",
        "wallet_key",
        "wallet-key",
        "-----begin private key-----",
    )
)


class DeploymentIntegrityError(RuntimeError):
    """Persistent deployment state is incomplete, corrupt, or unsafe to copy."""


@dataclass(frozen=True, slots=True)
class BackupResult:
    path: Path
    manifest_sha256: str
    file_count: int
    total_bytes: int

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path.as_posix(),
            "manifest_sha256": self.manifest_sha256,
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
        }


def _canonical_json(payload: object) -> bytes:
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_file(path: Path) -> None:
    with path.open("r+b") as stream:
        os.fsync(stream.fileno())
    _fsync_directory(path.parent)


def _fsync_tree_directories(root: Path) -> None:
    directories = sorted(
        (path for path in root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for directory in directories:
        _fsync_directory(directory)
    _fsync_directory(root)


def _require_plain_directory(path: Path, *, label: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise DeploymentIntegrityError(f"{label} must be an existing non-symlink directory")
    return path.resolve()


def _require_volume_marker(path: Path, *, name: str) -> None:
    marker = path / VOLUME_MARKER_NAME
    try:
        content = marker.read_bytes()
    except OSError:
        raise DeploymentIntegrityError(f"persistent {name} volume marker is unavailable") from None
    canonical_content = content.replace(b"\r\n", b"\n")
    if (
        marker.is_symlink()
        or not marker.is_file()
        or canonical_content != _VOLUME_MARKER_CONTENT[name]
    ):
        raise DeploymentIntegrityError(f"persistent {name} volume marker is invalid")


def _validate_readonly_config(config: Path) -> None:
    if config.is_symlink() or not config.is_file():
        raise DeploymentIntegrityError("persistent config/research.toml is required")
    try:
        with config.open("rb") as stream:
            raw_config = tomllib.load(stream)
        raw_app = raw_config.get("app")
        if not isinstance(raw_app, Mapping) or raw_app.get("mode") != "readonly":
            raise DeploymentIntegrityError(
                "persistent config [app].mode must explicitly equal readonly"
            )
        settings = load_settings(config)
    except (OSError, TypeError, ValueError) as exc:
        raise DeploymentIntegrityError(
            f"persistent configuration is invalid: {type(exc).__name__}"
        ) from None
    if settings.app.mode != "readonly":
        raise DeploymentIntegrityError("Umbrel persistent configuration must use readonly mode")
    _guard_secret_persistence(Path("config/research.toml"), config)


def _probe_writable_directory(path: Path, *, name: str) -> None:
    probe = path / f".hyperlab-write-probe-{uuid.uuid4().hex}"
    try:
        with probe.open("xb") as stream:
            stream.write(b"persistent-storage-probe\n")
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as exc:
        raise DeploymentIntegrityError(
            f"persistent {name} directory is not durably writable: {type(exc).__name__}"
        ) from None
    finally:
        probe.unlink(missing_ok=True)
        _fsync_directory(path)


def validate_persistent_layout(root: Path, *, require_writable: bool = False) -> dict[str, object]:
    """Verify the explicit Umbrel persistence contract without inventing missing state."""

    resolved = _require_plain_directory(root, label="persistent root")
    for name in REQUIRED_PERSISTENT_DIRECTORIES:
        child = root / name
        child_resolved = _require_plain_directory(child, label=f"persistent {name} directory")
        if not child_resolved.is_relative_to(resolved):
            raise DeploymentIntegrityError(f"persistent {name} directory escapes its root")
        _require_volume_marker(child, name=name)
    config = root / "config" / "research.toml"
    _validate_readonly_config(config)

    if require_writable:
        for name in REQUIRED_PERSISTENT_DIRECTORIES:
            if name == "config":
                continue
            _probe_writable_directory(root / name, name=name)
    return {
        "ok": True,
        "mode": "readonly",
        "orders_enabled": False,
        "root": resolved.as_posix(),
        "directories": list(REQUIRED_PERSISTENT_DIRECTORIES),
    }


def validate_service_persistence(
    data_root: Path,
    config: Path,
    *,
    service: str,
) -> dict[str, object]:
    """Authenticate the exact bind mounts visible to a permanent service."""

    visible = {
        "collector": {"runtime": data_root, "market": data_root / "lake", "config": config.parent},
        "dashboard": {
            "runtime": data_root,
            "reports": data_root / "reports",
            "paper": data_root / "paper",
            "config": config.parent,
        },
    }
    if service not in visible:
        raise ValueError("service must be collector or dashboard")
    for name, path in visible[service].items():
        _require_plain_directory(path, label=f"{service} {name} mount")
        _require_volume_marker(path, name=name)
    _validate_readonly_config(config)
    if service == "collector":
        _probe_writable_directory(data_root, name="runtime")
    return {"ok": True, "service": service, "mode": "readonly", "orders_enabled": False}


def publish_collector_starting_status(data_root: Path) -> None:
    """Invalidate any prior ready status before configuration or sink startup."""

    _require_plain_directory(data_root, label="collector runtime mount")
    _require_volume_marker(data_root, name="runtime")
    write_runtime_status(
        data_root / "runtime_status.json",
        {
            "schema_version": 1,
            "ok": False,
            "mode": "readonly",
            "orders_enabled": False,
            "updated_at": datetime.now(tz=UTC).isoformat(),
            "pending_rows": 0,
            "metrics": {
                "state": "starting",
                "connection_alive": False,
                "stale_channels": ["collector_starting"],
            },
        },
    )


def _relative_files(root: Path, *, exclude_backups: bool) -> Iterator[tuple[Path, Path]]:
    resolved = root.resolve()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root)
        if (
            exclude_backups
            and relative.parts
            and relative.parts[0] == "backups"
            and relative != Path("backups") / VOLUME_MARKER_NAME
        ):
            continue
        if path.is_symlink():
            raise DeploymentIntegrityError(f"persistent symlink refused: {relative.as_posix()}")
        if path.is_dir():
            continue
        if not path.resolve().is_relative_to(resolved):
            raise DeploymentIntegrityError(f"persistent file escapes its root: {relative.as_posix()}")
        if path.name.startswith(".") and path.name.endswith(".tmp"):
            raise DeploymentIntegrityError(
                f"interrupted temporary artifact blocks backup: {relative.as_posix()}"
            )
        yield relative, path


def _guard_secret_persistence(relative: Path, path: Path) -> None:
    lowered = path.name.casefold()
    if (
        lowered == ".env"
        or lowered.startswith(".env.")
        or path.suffix.casefold() in _FORBIDDEN_SECRET_SUFFIXES
        or "keystore" in lowered
        or _PRIVATE_CREDENTIAL_NAME in lowered
        or _SEED_CREDENTIAL_NAME in lowered
        or "wallet" in lowered
    ):
        raise DeploymentIntegrityError(
            f"forbidden credential artifact in persistent state: {relative.as_posix()}"
        )
    overlap = max(len(marker) for marker in _SECRET_BYTE_MARKERS) - 1
    trailing = b""
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            searchable = (trailing + block).lower()
            if any(marker in searchable for marker in _SECRET_BYTE_MARKERS):
                raise DeploymentIntegrityError(
                    f"credential-like value detected in persistent state: {relative.as_posix()}"
                )
            trailing = searchable[-overlap:]


def _is_derived_or_transient(relative: Path) -> bool:
    if not relative.parts or relative.parts[0] != "market":
        return False
    return relative.name in {
        ".collector-observations.sqlite3",
        ".collector-observations.sqlite3-journal",
        ".collector-observations.sqlite3-shm",
        ".collector-observations.sqlite3-wal",
        ".collector-writer.lock",
        "catalog.duckdb",
    }


def _sqlite_sidecar_base(relative: Path) -> Path | None:
    for suffix in ("-journal", "-wal", "-shm"):
        if relative.name.endswith(suffix):
            return relative.with_name(relative.name.removesuffix(suffix))
    return None


def _snapshot_files(
    persistent_files: list[tuple[Path, Path]],
) -> list[tuple[Path, Path]]:
    relative_paths = {relative for relative, _ in persistent_files}
    selected: list[tuple[Path, Path]] = []
    for relative, source in persistent_files:
        if _is_derived_or_transient(relative):
            continue
        sidecar_base = _sqlite_sidecar_base(relative)
        if sidecar_base is not None:
            if sidecar_base.suffix.casefold() != ".sqlite3" or sidecar_base not in relative_paths:
                raise DeploymentIntegrityError(
                    f"orphan SQLite sidecar blocks backup: {relative.as_posix()}"
                )
            # The matching database is copied with SQLite's online backup API.
            continue
        selected.append((relative, source))
    return selected


def _sqlite_integrity(path: Path) -> None:
    uri = f"{path.resolve().as_uri()}?mode=ro"
    try:
        with closing(sqlite3.connect(uri, uri=True, isolation_level=None)) as connection:
            connection.execute("PRAGMA query_only=ON")
            result = connection.execute("PRAGMA integrity_check").fetchone()
            if result is None or result[0] != "ok":
                raise DeploymentIntegrityError(f"SQLite integrity check failed: {path.name}")
            foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchone()
            if foreign_keys is not None:
                raise DeploymentIntegrityError(f"SQLite foreign-key check failed: {path.name}")
            tables = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            forbidden = {
                "api_key",
                "mnemonic",
                _PRIVATE_CREDENTIAL_NAME,
                _SEED_CREDENTIAL_NAME,
                "wallet_key",
            }
            for (table,) in tables:
                escaped_table = str(table).replace("\"", "\"\"")
                columns = connection.execute(f'PRAGMA table_info("{escaped_table}")').fetchall()
                if {str(row[1]).casefold() for row in columns} & forbidden:
                    raise DeploymentIntegrityError(
                        f"credential-shaped SQLite column refused: {path.name}"
                    )
                cursor = connection.execute(f'SELECT * FROM "{escaped_table}"')
                while rows := cursor.fetchmany(1_000):
                    for row in rows:
                        for value in row:
                            if isinstance(value, str):
                                encoded = value.casefold().encode(errors="ignore")
                            elif isinstance(value, bytes):
                                encoded = value.lower()
                            else:
                                continue
                            if any(marker in encoded for marker in _SECRET_BYTE_MARKERS):
                                raise DeploymentIntegrityError(
                                    f"credential-shaped SQLite value refused: {path.name}"
                                )
    except sqlite3.DatabaseError as exc:
        raise DeploymentIntegrityError(f"SQLite database is unreadable: {path.name}") from exc


def _sqlite_backup(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_uri = f"{source.resolve().as_uri()}?mode=ro"
    try:
        with (
            closing(
                sqlite3.connect(source_uri, uri=True, isolation_level=None)
            ) as source_connection,
            closing(sqlite3.connect(destination, isolation_level=None)) as destination_connection,
        ):
            source_connection.execute("PRAGMA query_only=ON")
            source_connection.backup(destination_connection)
            destination_connection.execute("PRAGMA journal_mode=DELETE")
            destination_connection.execute("PRAGMA synchronous=FULL")
    except sqlite3.DatabaseError as exc:
        raise DeploymentIntegrityError(f"SQLite online backup failed: {source.name}") from exc
    for suffix in ("-journal", "-wal", "-shm"):
        destination.with_name(f"{destination.name}{suffix}").unlink(missing_ok=True)
    _sqlite_integrity(destination)
    _fsync_file(destination)


def _copy_snapshot_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.suffix.casefold() == ".sqlite3":
        _sqlite_backup(source, destination)
        return
    shutil.copyfile(source, destination)
    _fsync_file(destination)


def _write_canonical_json(path: Path, payload: object) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(_canonical_json(payload))
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _require_clean_session(root: Path) -> None:
    marker = root / "market" / ".collector-session.json"
    if not marker.exists():
        return
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise DeploymentIntegrityError("collector session marker is unreadable") from None
    if not isinstance(payload, Mapping) or payload.get("clean_shutdown") is not True:
        raise DeploymentIntegrityError(
            "collector did not complete a clean shutdown; restart recovery and validation are required"
        )


@contextmanager
def _maintenance_lock(root: Path) -> Iterator[None]:
    try:
        with exclusive_lake_maintenance(root):
            yield
    except LakeWriterActiveError as exc:
        raise DeploymentIntegrityError("collector writer is active; stop it cleanly before maintenance") from exc


def _validate_lake(root: Path) -> None:
    try:
        inventory_partitions(root / "market")
    except (OSError, TypeError, ValueError) as exc:
        raise DeploymentIntegrityError(f"Parquet lake validation failed: {type(exc).__name__}") from exc


def _backup_identifier(value: str | None) -> str:
    if value is None:
        stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"{stamp}-{uuid.uuid4().hex[:12]}"
    if _BACKUP_ID.fullmatch(value) is None:
        raise ValueError("backup id must contain only letters, digits, dot, underscore, or dash")
    return value


def create_parquet_export(
    data_root: Path,
    *,
    output_name: str,
    record_type: str,
    venue: str | None = None,
    asset: str | None = None,
    start: str | None = None,
    end: str | None = None,
    schema_version: int | None = None,
) -> dict[str, object]:
    """Publish one verified report export while excluding the live lake writer."""

    from hyperlab.data.catalog import export_dataset

    validate_persistent_layout(data_root)
    if _EXPORT_NAME.fullmatch(output_name) is None or output_name.startswith("."):
        raise ValueError("export name must be one plain, non-hidden .parquet file name")
    output = data_root / "reports" / output_name
    ensure_storage_capacity(data_root / "reports")
    with _maintenance_lock(data_root / "market"):
        _require_clean_session(data_root)
        _validate_lake(data_root)
        result = export_dataset(
            data_root / "market",
            output,
            output_format="parquet",
            record_type=record_type,
            venue=venue,
            asset=asset,
            start=start,
            end=end,
            schema_version=schema_version,
        )
        _fsync_file(output)
        _write_canonical_json(
            data_root / "reports" / "latest_summary.json",
            {
                "title": f"Export Parquet vérifié: {output_name}",
                "download_path": output_name,
                "sha256": result.sha256,
                "row_count": result.row_count,
                "source_hashes": list(result.source_hashes),
                "filters": result.filters,
                "mode": "readonly",
                "orders_enabled": False,
            },
        )
    return {
        "output": output.as_posix(),
        "sha256": result.sha256,
        "row_count": result.row_count,
        "source_hashes": list(result.source_hashes),
        "filters": result.filters,
        "mode": "readonly",
        "orders_enabled": False,
    }


def create_backup(
    data_root: Path,
    *,
    backup_root: Path | None = None,
    backup_id: str | None = None,
) -> BackupResult:
    """Create a verified, complete-only snapshot while excluding the active writer."""

    validate_persistent_layout(data_root)
    resolved_data = data_root.resolve()
    destination_root = (backup_root or data_root / "backups").resolve()
    _require_plain_directory(destination_root, label="backup root")
    if destination_root.is_relative_to(resolved_data) and destination_root != (
        resolved_data / "backups"
    ):
        raise DeploymentIntegrityError(
            "a backup inside persistent state is allowed only in its backups directory"
        )
    identifier = _backup_identifier(backup_id)
    final = destination_root / f"backup-{identifier}"
    staging = destination_root / f".partial-backup-{identifier}-{uuid.uuid4().hex[:8]}"
    if final.exists() or staging.exists():
        raise FileExistsError(f"backup destination already exists: {identifier}")

    with _maintenance_lock(data_root / "market"):
        _require_clean_session(data_root)
        _validate_lake(data_root)
        persistent_files = list(_relative_files(data_root, exclude_backups=True))
        files = _snapshot_files(persistent_files)
        for relative, source in persistent_files:
            if relative == Path("market/.collector-writer.lock"):
                continue
            if _sqlite_sidecar_base(relative) is not None:
                continue
            _guard_secret_persistence(relative, source)
            if source.suffix.casefold() == ".sqlite3":
                _sqlite_integrity(source)
        source_bytes = sum(source.stat().st_size for _, source in files)
        usage = shutil.disk_usage(destination_root)
        reserve = max(128 * 1024 * 1024, int(usage.total * 0.02))
        if usage.free - source_bytes < reserve:
            raise DeploymentIntegrityError(
                "insufficient free space for a complete backup plus the configured reserve"
            )

        payload_root = staging / "payload"
        payload_root.mkdir(parents=True)
        for name in REQUIRED_PERSISTENT_DIRECTORIES:
            (payload_root / name).mkdir()
        manifest_files: list[dict[str, object]] = []
        for relative, source in files:
            destination = payload_root / relative
            _copy_snapshot_file(source, destination)
            manifest_files.append(
                {
                    "path": relative.as_posix(),
                    "sha256": _sha256(destination),
                    "size_bytes": destination.stat().st_size,
                }
            )

        manifest: dict[str, object] = {
            "schema_version": BACKUP_SCHEMA_VERSION,
            "application_version": __version__,
            "backup_id": identifier,
            "created_at": datetime.now(tz=UTC).isoformat(),
            "mode": "readonly",
            "orders_enabled": False,
            "files": manifest_files,
        }
        manifest_bytes = _canonical_json(manifest)
        manifest_path = staging / "manifest.json"
        with manifest_path.open("xb") as stream:
            stream.write(manifest_bytes)
            stream.flush()
            os.fsync(stream.fileno())
        manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        complete_path = staging / "COMPLETE.json"
        with complete_path.open("xb") as stream:
            stream.write(
                _canonical_json(
                    {
                        "schema_version": BACKUP_SCHEMA_VERSION,
                        "manifest_sha256": manifest_sha256,
                    }
                )
            )
            stream.flush()
            os.fsync(stream.fileno())
        _fsync_tree_directories(payload_root)
        _fsync_directory(staging)
        verify_backup(staging, _allow_partial=True)
        staging.replace(final)
        _fsync_directory(destination_root)

    return BackupResult(
        path=final,
        manifest_sha256=manifest_sha256,
        file_count=len(manifest_files),
        total_bytes=source_bytes,
    )


def _read_backup_manifest(
    backup: Path,
    *,
    allow_partial: bool = False,
) -> tuple[dict[str, object], str]:
    _require_plain_directory(backup, label="backup")
    if not allow_partial and (
        backup.name.startswith(".partial-") or backup.name.startswith(".partial-backup-")
    ):
        raise DeploymentIntegrityError("partial backup is never restorable")
    manifest_path = backup / "manifest.json"
    complete_path = backup / "COMPLETE.json"
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes)
        complete = json.loads(complete_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeploymentIntegrityError("backup manifest or completion marker is unreadable") from exc
    if not isinstance(manifest, dict) or not isinstance(complete, dict):
        raise DeploymentIntegrityError("backup metadata roots must be objects")
    if manifest_bytes != _canonical_json(manifest):
        raise DeploymentIntegrityError("backup manifest is not canonical")
    digest = hashlib.sha256(manifest_bytes).hexdigest()
    if complete.get("manifest_sha256") != digest:
        raise DeploymentIntegrityError("backup completion marker does not match its manifest")
    if manifest.get("schema_version") != BACKUP_SCHEMA_VERSION:
        raise DeploymentIntegrityError("unsupported backup schema version")
    if manifest.get("mode") != "readonly" or manifest.get("orders_enabled") is not False:
        raise DeploymentIntegrityError("backup safety mode is incompatible")
    return manifest, digest


def _manifest_expectations(manifest: Mapping[str, object]) -> dict[str, tuple[str, int]]:
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list):
        raise DeploymentIntegrityError("backup manifest files must be an array")
    expected: dict[str, tuple[str, int]] = {}
    for item in raw_files:
        if not isinstance(item, Mapping):
            raise DeploymentIntegrityError("backup file entry must be an object")
        relative_text = item.get("path")
        digest = item.get("sha256")
        size = item.get("size_bytes")
        if not isinstance(relative_text, str) or not isinstance(digest, str) or not isinstance(size, int):
            raise DeploymentIntegrityError("backup file entry has invalid fields")
        relative = Path(relative_text)
        if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != relative_text:
            raise DeploymentIntegrityError("backup manifest contains an unsafe path")
        if relative_text in expected:
            raise DeploymentIntegrityError("backup manifest contains a duplicate path")
        expected[relative_text] = (digest, size)
    return expected


def _verify_payload_files(
    payload_root: Path,
    expected: Mapping[str, tuple[str, int]],
) -> None:
    payload_root = _require_plain_directory(payload_root, label="backup payload")
    actual = {
        relative.as_posix(): path
        for relative, path in _relative_files(payload_root, exclude_backups=False)
    }
    if set(actual) != set(expected):
        raise DeploymentIntegrityError("backup payload file set differs from its manifest")
    for relative_text, path in actual.items():
        digest, size = expected[relative_text]
        if path.stat().st_size != size or _sha256(path) != digest:
            raise DeploymentIntegrityError(f"backup payload hash mismatch: {relative_text}")
        _guard_secret_persistence(Path(relative_text), path)
        if path.suffix.casefold() == ".sqlite3":
            _sqlite_integrity(path)
    validate_persistent_layout(payload_root)
    _validate_lake(payload_root)


def verify_backup(backup: Path, *, _allow_partial: bool = False) -> BackupResult:
    manifest, manifest_sha256 = _read_backup_manifest(backup, allow_partial=_allow_partial)
    expected = _manifest_expectations(manifest)
    _verify_payload_files(backup / "payload", expected)
    return BackupResult(
        path=backup,
        manifest_sha256=manifest_sha256,
        file_count=len(expected),
        total_bytes=sum(size for _, size in expected.values()),
    )


def restore_backup(backup: Path, target: Path) -> BackupResult:
    """Restore into a new path only; live data is never merged or overwritten."""

    verified = verify_backup(backup)
    if target.exists():
        raise FileExistsError("restore target must not exist; partial/live restores are refused")
    if target.resolve().is_relative_to(backup.resolve()):
        raise DeploymentIntegrityError("restore target must be outside the immutable backup")
    target_parent = _require_plain_directory(target.parent, label="restore target parent")
    usage = shutil.disk_usage(target_parent)
    reserve = max(128 * 1024 * 1024, int(usage.total * 0.02))
    if usage.free - verified.total_bytes < reserve:
        raise DeploymentIntegrityError(
            "insufficient free space for a complete restore plus the configured reserve"
        )
    staging = target_parent / f".partial-restore-{uuid.uuid4().hex}"
    shutil.copytree(backup / "payload", staging, copy_function=shutil.copyfile)
    try:
        copied_manifest, copied_manifest_sha256 = _read_backup_manifest(backup)
        if copied_manifest_sha256 != verified.manifest_sha256:
            raise DeploymentIntegrityError("backup manifest changed during restore")
        _verify_payload_files(staging, _manifest_expectations(copied_manifest))
        for _relative, path in _relative_files(staging, exclude_backups=False):
            _fsync_file(path)
        _fsync_tree_directories(staging)
        staging.replace(target)
        _fsync_directory(target_parent)
    except BaseException:
        # Keep the hidden partial tree for forensics. It is never accepted as a
        # backup or a live target and can be removed explicitly after review.
        raise
    return BackupResult(
        path=target,
        manifest_sha256=verified.manifest_sha256,
        file_count=verified.file_count,
        total_bytes=verified.total_bytes,
    )
