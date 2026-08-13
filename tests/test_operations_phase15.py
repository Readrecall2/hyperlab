from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

import hyperlab.operations as operations_module
from hyperlab.collector.models import WireEnvelope
from hyperlab.collector.parser import parse_websocket_message
from hyperlab.collector.storage import BatchingLakeSink
from hyperlab.operations import (
    VOLUME_MARKER_NAME,
    DeploymentIntegrityError,
    create_backup,
    create_parquet_export,
    publish_collector_starting_status,
    restore_backup,
    validate_persistent_layout,
    validate_service_persistence,
    verify_backup,
)


def _layout(tmp_path: Path) -> Path:
    root = tmp_path / "persistent"
    for name in ("backups", "config", "market", "paper", "reports", "runtime"):
        (root / name).mkdir(parents=True, exist_ok=True)
        (root / name / VOLUME_MARKER_NAME).write_text(
            f"hyperlab-{name}-volume-v1\n",
            encoding="utf-8",
        )
    (root / "config" / "research.toml").write_text(
        '[app]\nmode = "readonly"\ndata_dir = "/data"\n',
        encoding="utf-8",
    )
    (root / "reports" / "summary.json").write_text(
        '{"status":"BLOCKED","orders_enabled":false}\n',
        encoding="utf-8",
    )
    with sqlite3.connect(root / "paper" / "paper.sqlite3") as connection:
        connection.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, status TEXT NOT NULL)")
        connection.execute("INSERT INTO audit VALUES (1, 'BLOCKED')")
    return root


def test_explicit_persistent_layout_is_required_and_readonly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _layout(tmp_path)
    synced: list[Path] = []
    monkeypatch.setattr(operations_module, "_fsync_directory", lambda path: synced.append(path))
    assert validate_persistent_layout(root, require_writable=True)["orders_enabled"] is False
    assert {root / name for name in ("backups", "market", "paper", "reports", "runtime")} <= set(
        synced
    )

    (root / "reports" / "summary.json").unlink()
    (root / "reports" / VOLUME_MARKER_NAME).unlink()
    (root / "reports").rmdir()
    with pytest.raises(DeploymentIntegrityError, match="reports"):
        validate_persistent_layout(root)


def test_persistent_toml_readonly_cannot_be_masked_by_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _layout(tmp_path)
    (root / "config" / "research.toml").write_text(
        '[app]\nmode = "research"\ndata_dir = "/data"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("HYPERLAB_MODE", "readonly")

    with pytest.raises(DeploymentIntegrityError, match="explicitly equal readonly"):
        validate_persistent_layout(root)


def test_persistent_volume_marker_mismatch_fails_closed(tmp_path: Path) -> None:
    root = _layout(tmp_path)
    (root / "market" / VOLUME_MARKER_NAME).write_text(
        "hyperlab-runtime-volume-v1\n",
        encoding="utf-8",
    )

    with pytest.raises(DeploymentIntegrityError, match="market volume marker is invalid"):
        validate_persistent_layout(root)


def test_service_mount_contract_and_starting_status_are_fail_closed(tmp_path: Path) -> None:
    full = _layout(tmp_path)
    runtime = full / "runtime"
    market_mount = runtime / "lake"
    reports_mount = runtime / "reports"
    paper_mount = runtime / "paper"
    for name, mount in (
        ("market", market_mount),
        ("reports", reports_mount),
        ("paper", paper_mount),
    ):
        mount.mkdir()
        (mount / VOLUME_MARKER_NAME).write_text(
            f"hyperlab-{name}-volume-v1\n",
            encoding="utf-8",
        )
    config = full / "config" / "research.toml"

    assert validate_service_persistence(runtime, config, service="collector")["ok"] is True
    assert validate_service_persistence(runtime, config, service="dashboard")["ok"] is True

    publish_collector_starting_status(runtime)
    status = json.loads((runtime / "runtime_status.json").read_text(encoding="utf-8"))
    assert status["ok"] is False
    assert status["orders_enabled"] is False
    assert status["metrics"]["state"] == "starting"

    (market_mount / VOLUME_MARKER_NAME).unlink()
    with pytest.raises(DeploymentIntegrityError, match="market volume marker is unavailable"):
        validate_service_persistence(runtime, config, service="collector")


def test_backup_verify_and_restore_are_complete_and_non_overwriting(tmp_path: Path) -> None:
    root = _layout(tmp_path)
    backup = create_backup(root, backup_id="phase15-test")

    assert backup.path.name == "backup-phase15-test"
    assert verify_backup(backup.path) == backup
    manifest = json.loads((backup.path / "manifest.json").read_text(encoding="utf-8"))
    paths = {item["path"] for item in manifest["files"]}
    assert "paper/paper.sqlite3" in paths
    assert "reports/summary.json" in paths
    assert {path for path in paths if path.startswith("backups/")} == {
        "backups/.hyperlab-volume"
    }
    assert not any("collector-observations" in path for path in paths)

    restored = tmp_path / "restored"
    restore_result = restore_backup(backup.path, restored)
    assert restore_result.manifest_sha256 == backup.manifest_sha256
    assert (restored / "reports" / "summary.json").read_text(encoding="utf-8").startswith("{")
    with sqlite3.connect(restored / "paper" / "paper.sqlite3") as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)

    with pytest.raises(FileExistsError, match="must not exist"):
        restore_backup(backup.path, restored)


def test_partial_or_tampered_backup_is_refused(tmp_path: Path) -> None:
    root = _layout(tmp_path)
    backup = create_backup(root, backup_id="tamper-test").path
    (backup / "payload" / "reports" / "summary.json").write_text("tampered", encoding="utf-8")

    with pytest.raises(DeploymentIntegrityError, match="hash mismatch"):
        verify_backup(backup)

    partial = root / "backups" / ".partial-backup-incomplete"
    partial.mkdir()
    with pytest.raises(DeploymentIntegrityError, match="partial backup"):
        verify_backup(partial)


def test_restore_rehashes_staging_after_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _layout(tmp_path)
    backup = create_backup(root, backup_id="restore-race").path
    original_copyfile = operations_module.shutil.copyfile

    def corrupting_copyfile(source: str | Path, target: str | Path) -> str:
        copied = original_copyfile(source, target)
        target_path = Path(target)
        if target_path.name == "summary.json":
            target_path.write_text("changed-during-copy", encoding="utf-8")
        return copied

    monkeypatch.setattr(operations_module.shutil, "copyfile", corrupting_copyfile)
    target = tmp_path / "must-not-publish"

    with pytest.raises(DeploymentIntegrityError, match="hash mismatch"):
        restore_backup(backup, target)
    assert not target.exists()


def test_backup_fails_closed_while_collector_holds_root_lock(tmp_path: Path) -> None:
    root = _layout(tmp_path)
    writer = BatchingLakeSink(
        root / "market",
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    try:
        with pytest.raises(DeploymentIntegrityError, match="writer is active"):
            create_backup(root, backup_id="must-fail")
    finally:
        writer.close()


def test_backup_uses_online_sqlite_snapshot_and_excludes_live_sidecars(tmp_path: Path) -> None:
    root = _layout(tmp_path)
    database = root / "paper" / "paper.sqlite3"
    connection = sqlite3.connect(database)
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        connection.execute("INSERT INTO audit VALUES (2, 'WAL_COMMITTED')")
        connection.commit()
        assert database.with_name(f"{database.name}-wal").exists()

        backup = create_backup(root, backup_id="sqlite-wal")
    finally:
        connection.close()

    manifest = json.loads((backup.path / "manifest.json").read_text(encoding="utf-8"))
    paths = {item["path"] for item in manifest["files"]}
    assert "paper/paper.sqlite3" in paths
    assert not any(path.endswith(("-wal", "-shm", "-journal")) for path in paths)
    with sqlite3.connect(backup.path / "payload" / "paper" / "paper.sqlite3") as restored:
        assert restored.execute("SELECT status FROM audit ORDER BY id").fetchall() == [
            ("BLOCKED",),
            ("WAL_COMMITTED",),
        ]


def test_backup_refuses_orphan_sqlite_sidecar(tmp_path: Path) -> None:
    root = _layout(tmp_path)
    (root / "paper" / "missing.sqlite3-wal").write_bytes(b"orphan")

    with pytest.raises(DeploymentIntegrityError, match="orphan SQLite sidecar"):
        create_backup(root, backup_id="orphan-sidecar")


def test_operational_parquet_export_is_locked_durable_and_dashboard_ready(tmp_path: Path) -> None:
    root = _layout(tmp_path)
    writer = BatchingLakeSink(
        root / "market",
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    parsed = parse_websocket_message(
        WireEnvelope(
            raw_message='{"channel":"pong"}',
            received_time=datetime(2026, 8, 13, 12, tzinfo=UTC),
            connection_id="phase15-export",
            connection_epoch=1,
            arrival_sequence=1,
        )
    )
    writer.add_many(parsed.records)
    writer.flush()
    try:
        with pytest.raises(DeploymentIntegrityError, match="writer is active"):
            create_parquet_export(
                root,
                output_name="blocked.parquet",
                record_type="wire_message",
            )
    finally:
        writer.close()

    payload = create_parquet_export(
        root,
        output_name="public-wire.parquet",
        record_type="wire_message",
    )

    exported = root / "reports" / "public-wire.parquet"
    summary = json.loads((root / "reports" / "latest_summary.json").read_text(encoding="utf-8"))
    assert exported.is_file()
    assert payload["orders_enabled"] is False
    assert payload["row_count"] == 1
    assert summary["download_path"] == exported.name
    assert summary["sha256"] == payload["sha256"]
    with pytest.raises(ValueError, match="plain"):
        create_parquet_export(
            root,
            output_name="../escape.parquet",
            record_type="wire_message",
        )


@pytest.mark.parametrize(
    "relative, content",
    [
        ("config/wallet.key", "not-even-a-real-key"),
        ("reports/leak.json", '{"private_key":"redacted-is-still-not-allowed"}'),
    ],
)
def test_backup_refuses_credential_shaped_persistent_artifacts(
    tmp_path: Path,
    relative: str,
    content: str,
) -> None:
    root = _layout(tmp_path)
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

    with pytest.raises(DeploymentIntegrityError, match="credential"):
        create_backup(root, backup_id="secret-refused")


def test_backup_scans_large_binary_and_sqlite_values_for_credentials(tmp_path: Path) -> None:
    root = _layout(tmp_path)
    binary = root / "reports" / "opaque.bin"
    binary.write_bytes(b"x" * (5 * 1024 * 1024) + b"private_key=forbidden")

    with pytest.raises(DeploymentIntegrityError, match="credential"):
        create_backup(root, backup_id="large-secret")

    binary.unlink()
    with sqlite3.connect(root / "paper" / "paper.sqlite3") as connection:
        connection.execute("INSERT INTO audit VALUES (2, 'private_key=forbidden')")

    with pytest.raises(DeploymentIntegrityError, match="credential"):
        create_backup(root, backup_id="sqlite-secret")
