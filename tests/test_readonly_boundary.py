from __future__ import annotations

import ast
from pathlib import Path

from hyperlab.cli import _secret_like_environment_variables


def test_source_tree_has_no_exchange_executor_import() -> None:
    root = Path(__file__).resolve().parents[1] / "src"
    content = "\n".join(path.read_text(encoding="utf-8") for path in root.rglob("*.py"))
    forbidden = ["from hyperliquid.exchange import", "import hyperliquid.exchange", "private_key", "seed_phrase"]
    for token in forbidden:
        assert token not in content


def test_backtest_package_has_no_network_or_venue_route_import() -> None:
    backtest_root = Path(__file__).resolve().parents[1] / "src" / "hyperlab" / "backtest"
    forbidden_roots = {
        "aiohttp",
        "httpx",
        "hyperlab.api",
        "hyperlab.venues",
        "hyperliquid",
        "requests",
        "socket",
        "urllib",
        "websocket",
        "websockets",
    }
    imported: list[tuple[Path, str]] = []
    for path in backtest_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend((path, alias.name) for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported.append((path, node.module))

    violations = [
        f"{path.name}: {module}"
        for path, module in imported
        if any(module == root or module.startswith(f"{root}.") for root in forbidden_roots)
    ]
    assert violations == []


def test_paper_package_has_no_network_wallet_signer_exchange_or_order_transport_import() -> None:
    paper_root = Path(__file__).resolve().parents[1] / "src" / "hyperlab" / "paper"
    forbidden_roots = {
        "aiohttp",
        "httpx",
        "hyperlab.api",
        "hyperlab.venues",
        "hyperliquid",
        "requests",
        "socket",
        "urllib",
        "websocket",
        "websockets",
    }
    forbidden_module_parts = {"exchange", "order_transport", "signer", "transport", "wallet"}
    forbidden_symbols = {
        "Exchange",
        "OrderTransport",
        "Signer",
        "Wallet",
        "cancel_order",
        "place_order",
        "send_order",
    }
    violations: list[str] = []
    for path in paper_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports = [(alias.name, alias.name.rsplit(".", 1)[-1]) for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imports = [(node.module, alias.name) for alias in node.names]
            else:
                continue
            for module, symbol in imports:
                module_parts = set(module.casefold().split("."))
                if (
                    any(module == root or module.startswith(f"{root}.") for root in forbidden_roots)
                    or module_parts & forbidden_module_parts
                    or symbol in forbidden_symbols
                ):
                    violations.append(f"{path.name}: {module} -> {symbol}")

    assert violations == []


def test_secret_diagnostic_covers_all_forbidden_credential_names() -> None:
    expected = {
        "EXCHANGE_API_KEY",
        "HYPERLIQUID_PRIVATE_KEY",
        "RECOVERY_MNEMONIC",
        "SEED_PHRASE",
        "TRADING_WALLET_KEY",
    }
    environment = {name: "" for name in expected}
    environment["HYPERLAB_DATA_DIR"] = "data"

    assert _secret_like_environment_variables(environment) == sorted(expected)
