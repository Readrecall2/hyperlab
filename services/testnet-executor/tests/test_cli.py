from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

from typer.testing import CliRunner

from hyperlab_testnet import cli
from hyperlab_testnet.build_identity import current_testnet_build_identity
from hyperlab_testnet.config import TestnetConfig as _TestnetConfig
from hyperlab_testnet.models import RuntimeState
from hyperlab_testnet.runtime import PreflightReport
from hyperlab_testnet.store import TestnetStore as _TestnetStore

runner = CliRunner()


def _config() -> _TestnetConfig:
    identity = current_testnet_build_identity()
    return _TestnetConfig(
        candidate_id="phase13-cli-synthetic",
        account_address="0x" + "31" * 20,
        api_wallet_address="0x" + "42" * 20,
        strategy_name=identity.strategy_name,
        strategy_hash=identity.strategy_hash,
        build_hash=identity.build_hash,
        source_identity=identity.source_identity,
        source_hash=identity.source_hash,
    )


def _file_hashes(directory: Path) -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.iterdir())
        if path.is_file()
    }


def test_cli_has_only_explicit_testnet_operator_commands() -> None:
    commands = {command.name for command in cli.app.registered_commands}

    assert commands == {
        "build-identity",
        "cancel",
        "evidence",
        "kill",
        "pause",
        "preflight",
        "reconcile",
        "run",
        "smoke-order",
        "status",
        "validate-software",
    }
    assert not commands & {"live", "mainnet", "micro-mainnet", "trade"}


def test_status_is_read_only_and_does_not_read_credentials_or_network(
    tmp_path: Path,
    monkeypatch: object,
) -> None:
    database = (tmp_path / "status.sqlite3").resolve()
    control = (tmp_path / "control").resolve()
    config = _config()
    with _TestnetStore(database, lease_root=control) as store:
        store.create_run(config)
    before = _file_hashes(tmp_path)
    monkeypatch.setattr(  # type: ignore[attr-defined]
        cli,
        "load_testnet_credentials",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("credentials read")),
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        cli,
        "RequestsJsonTransport",
        lambda: (_ for _ in ()).throw(AssertionError("network constructed")),
    )

    result = runner.invoke(
        cli.app,
        ["status", "--database", str(database), "--run-id", config.run_id],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["run_id"] == config.run_id
    assert payload["runtime_state"] == "STOPPED"
    assert payload["integrity_ok"] is True
    assert _file_hashes(tmp_path) == before


def test_status_refuses_missing_database_without_creating_it(tmp_path: Path) -> None:
    database = (tmp_path / "missing.sqlite3").resolve()

    result = runner.invoke(cli.app, ["status", "--database", str(database)])

    assert result.exit_code == 2
    assert not database.exists()


def test_preflight_uses_runtime_dry_run_and_reports_no_signed_action(
    tmp_path: Path,
    monkeypatch: object,
) -> None:
    calls: list[bool] = []

    class FakeRuntime:
        def start(self, *, dry_run: bool = False) -> PreflightReport:
            calls.append(dry_run)
            return PreflightReport("a" * 64, 7, "0x" + "42" * 20, 999_999, 7)

    class FakeContext:
        runtime = FakeRuntime()

        def close(self) -> None:
            return None

    monkeypatch.setattr(  # type: ignore[attr-defined]
        cli,
        "_build_online_context",
        lambda **kwargs: cast(cli._OnlineContext, FakeContext()),
    )

    result = runner.invoke(
        cli.app,
        [
            "preflight",
            "--config",
            str(tmp_path / "config.json"),
            "--receipt",
            str(tmp_path / "receipt.json"),
            "--manifest",
            str(tmp_path / "manifest.json"),
            "--evidence-root",
            str(tmp_path / "evidence"),
            "--validation-report",
            str(tmp_path / "software-validation.json"),
            "--database",
            str((tmp_path / "state.sqlite3").resolve()),
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [True]
    payload = json.loads(result.stdout)
    assert payload["mode"] == "PREFLIGHT_ONLY_NO_SIGNED_ACTION"


def test_action_commands_refuse_wrong_confirmation_before_bootstrap(
    tmp_path: Path,
    monkeypatch: object,
) -> None:
    built: list[bool] = []
    monkeypatch.setattr(  # type: ignore[attr-defined]
        cli,
        "_build_online_context",
        lambda **kwargs: built.append(True),
    )
    common = [
        "--config",
        str(tmp_path / "config.json"),
        "--receipt",
        str(tmp_path / "receipt.json"),
        "--manifest",
        str(tmp_path / "manifest.json"),
        "--evidence-root",
        str(tmp_path / "evidence"),
        "--validation-report",
        str(tmp_path / "software-validation.json"),
        "--database",
        str((tmp_path / "state.sqlite3").resolve()),
    ]

    smoke = runner.invoke(
        cli.app,
        [
            "smoke-order",
            *common,
            "--instrument",
            "HL:BTC:perp",
            "--side",
            "BUY",
            "--quantity",
            "0.001",
            "--limit-price",
            "1",
            "--time-in-force",
            "ALO",
            "--confirm",
            "WRONG",
        ],
    )
    cancel = runner.invoke(
        cli.app,
        [
            "cancel",
            *common,
            "--cloid",
            "0x" + "12" * 16,
            "--confirm",
            "WRONG",
        ],
    )

    assert smoke.exit_code == 2
    assert cancel.exit_code == 2
    assert built == []


def test_kill_persists_before_optional_network_protection(
    tmp_path: Path,
    monkeypatch: object,
) -> None:
    database = (tmp_path / "kill.sqlite3").resolve()
    control = (tmp_path / "control").resolve()
    config = _config()
    store = _TestnetStore(database, lease_root=control)
    store.create_run(config)
    monkeypatch.setattr(  # type: ignore[attr-defined]
        cli,
        "_open_existing_mutable_store",
        lambda path: store,
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        cli,
        "_build_adapter",
        lambda config: (_ for _ in ()).throw(AssertionError("network attempted")),
    )

    result = runner.invoke(
        cli.app,
        [
            "kill",
            "--database",
            str(database),
            "--run-id",
            config.run_id,
            "--confirm",
            "TESTNET-KILL",
        ],
    )

    assert result.exit_code == 3, result.output
    assert store.get_run(config.run_id).runtime_state is RuntimeState.KILLED
    assert store.account_kill_latched(config.run_id)
    payload = json.loads(result.stdout)
    assert payload["account_kill_latched"] is True
    assert payload["deadman_confirmed"] is False


def test_kill_wrong_confirmation_does_not_open_database(
    tmp_path: Path,
    monkeypatch: object,
) -> None:
    opened: list[bool] = []
    monkeypatch.setattr(  # type: ignore[attr-defined]
        cli,
        "_open_existing_mutable_store",
        lambda path: opened.append(True),
    )

    result = runner.invoke(
        cli.app,
        [
            "kill",
            "--database",
            str((tmp_path / "absent.sqlite3").resolve()),
            "--run-id",
            "a" * 64,
            "--confirm",
            "WRONG",
        ],
    )

    assert result.exit_code == 2
    assert opened == []
