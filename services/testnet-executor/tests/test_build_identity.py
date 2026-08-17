from __future__ import annotations

from pathlib import Path

import pytest

import hyperlab_testnet.build_identity as build_identity
from hyperlab_testnet.build_identity import (
    BuildIdentityError,
    current_testnet_build_identity,
    validate_runtime_identity,
    validate_runtime_process_boundary,
)
from hyperlab_testnet.config import TestnetConfig as _TestnetConfig


def _config() -> _TestnetConfig:
    identity = current_testnet_build_identity()
    return _TestnetConfig(
        candidate_id="phase13-synthetic-build-identity",
        account_address="0x" + "11" * 20,
        api_wallet_address="0x" + "22" * 20,
        strategy_name=identity.strategy_name,
        strategy_hash=identity.strategy_hash,
        build_hash=identity.build_hash,
        source_identity=identity.source_identity,
        source_hash=identity.source_hash,
    )


def test_runtime_identity_matches_only_the_running_source_and_dependencies() -> None:
    config = _config()
    observed = validate_runtime_identity(config)

    assert observed.build_hash == config.build_hash
    assert observed.source_hash == config.source_hash
    assert observed.strategy_hash == config.strategy_hash


def test_source_mutation_after_authorization_changes_build_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    original_read_bytes = Path.read_bytes

    def mutated_read_bytes(path: Path) -> bytes:
        payload = original_read_bytes(path)
        if path.name == "adapter.py" and path.parent.name == "hyperlab_testnet":
            return payload + b"\n# synthetic post-authorization mutation\n"
        return payload

    monkeypatch.setattr(Path, "read_bytes", mutated_read_bytes)
    mutated = current_testnet_build_identity()

    assert mutated.build_hash != config.build_hash
    with pytest.raises(BuildIdentityError, match="running build identity"):
        validate_runtime_identity(config, observed=mutated)


def test_caller_supplied_subject_hashes_cannot_mint_current_identity() -> None:
    config = _config()
    forged = _TestnetConfig(
        candidate_id=config.candidate_id,
        account_address=config.account_address,
        api_wallet_address=config.api_wallet_address,
        strategy_name=config.strategy_name,
        strategy_hash=config.strategy_hash,
        build_hash="0" * 64,
        source_identity=config.source_identity,
        source_hash=config.source_hash,
        risk_limits=config.risk_limits,
    )

    with pytest.raises(BuildIdentityError, match="running build identity"):
        validate_runtime_identity(forged)


def test_runtime_process_boundary_rejects_python_path_injection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYTHONPATH", "synthetic-shadow-path")

    with pytest.raises(BuildIdentityError, match="PYTHONPATH"):
        validate_runtime_process_boundary()


def test_runtime_process_boundary_binds_real_import_origins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in ("PYTHONHOME", "PYTHONPATH", "PYTHONUSERBASE"):
        monkeypatch.delenv(name, raising=False)

    artifacts = validate_runtime_process_boundary()

    assert {
        "eth_account",
        "hyperlab.environment_authorization",
        "hyperliquid.utils.signing",
        "hyperliquid.utils.types",
        "requests",
        "typer",
        "websocket",
    } == set(artifacts)
    assert all(len(value) == 64 for value in artifacts.values())


def test_installed_package_resolves_packaged_dependency_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = tmp_path / "site-packages" / "hyperlab_testnet"
    locks = package / "locks"
    locks.mkdir(parents=True)
    external = locks / "requirements-external.lock"
    build = locks / "requirements-build.lock"
    external.write_text("external synthetic lock\n", encoding="utf-8")
    build.write_text("build synthetic lock\n", encoding="utf-8")
    monkeypatch.setattr(build_identity, "__file__", str(package / "build_identity.py"))

    assert build_identity.external_lock_path() == external
    assert build_identity.build_lock_path() == build


def test_packaged_dependency_lock_must_be_regular(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = tmp_path / "site-packages" / "hyperlab_testnet"
    lock = package / "locks" / "requirements-external.lock"
    lock.mkdir(parents=True)
    monkeypatch.setattr(build_identity, "__file__", str(package / "build_identity.py"))

    with pytest.raises(BuildIdentityError, match="regular file"):
        build_identity.external_lock_path()
