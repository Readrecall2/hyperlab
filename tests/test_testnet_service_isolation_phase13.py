from __future__ import annotations

import ast
from pathlib import Path

import pytest

from hyperlab.environment_authorization import REAL_MONEY_EXECUTION_ENABLED_IN_BUILD

ROOT = Path(__file__).resolve().parents[1]
SERVICE = ROOT / "services" / "testnet-executor"


def test_root_release_and_umbrel_exclude_testnet_executor() -> None:
    root_project = (ROOT / "pyproject.toml").read_text(encoding="utf-8").casefold()
    service_project = (SERVICE / "pyproject.toml").read_text(encoding="utf-8").casefold()
    root_locks = "\n".join(
        (ROOT / name).read_text(encoding="utf-8").casefold()
        for name in ("requirements-runtime.lock", "requirements-ci.lock")
    )
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()

    assert 'version = "0.2.1"' in root_project
    assert 'version = "0.3.0.dev0"' in service_project
    assert '"hyperlab==0.2.1"' not in service_project
    assert "hyperliquid-python-sdk" not in root_project
    assert "hyperliquid-python-sdk" not in root_locks
    assert "hyperliquid-python-sdk==0.24.0" in service_project
    assert "services" in dockerignore
    assert "COPY src ./src" in dockerfile
    assert "COPY services" not in dockerfile
    assert "COPY ." not in dockerfile


def test_service_has_no_exchange_client_or_mainnet_endpoint_route() -> None:
    source_root = SERVICE / "src" / "hyperlab_testnet"
    source_text = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(source_root.glob("*.py"))
    )
    violations: list[str] = []
    for path in source_root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                modules = [node.module]
            else:
                continue
            violations.extend(
                f"{path.name}: {module}"
                for module in modules
                if module == "hyperliquid.exchange" or module.startswith("hyperliquid.exchange.")
            )

    assert violations == []
    assert "https://api.hyperliquid.xyz" not in source_text
    assert not REAL_MONEY_EXECUTION_ENABLED_IN_BUILD


def test_testnet_config_rejects_mainnet_and_ambiguous_identity() -> None:
    import sys

    sys.path.insert(0, str(SERVICE / "src"))
    try:
        from hyperlab_testnet.config import TestnetConfig, TestnetConfigError

        config = TestnetConfig(
            candidate_id="phase13-synthetic",
            account_address="0x" + "11" * 20,
            api_wallet_address="0x" + "22" * 20,
            strategy_name="manual-smoke-only",
            strategy_hash="3" * 64,
            build_hash="4" * 64,
            source_identity="hyperliquid-testnet-public",
            source_hash="5" * 64,
        )
        for field, value in (
            ("environment", "MAINNET"),
            ("purpose", "MAINNET_EXECUTION"),
            ("chain_identity", "mainnet"),
            ("http_endpoint", "https://api.hyperliquid.xyz"),
            ("http_endpoint", ""),
        ):
            payload = config.to_dict()
            payload[field] = value
            with pytest.raises(TestnetConfigError):
                TestnetConfig.from_mapping(payload)
    finally:
        sys.path.remove(str(SERVICE / "src"))
