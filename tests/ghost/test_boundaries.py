from __future__ import annotations

import ast
from pathlib import Path

from typer.testing import CliRunner

from hyperlab.cli import app

ROOT = Path(__file__).resolve().parents[2]
GHOST_ROOT = ROOT / "src" / "hyperlab" / "ghost"


def test_ghost_package_has_no_network_private_or_real_order_capability() -> None:
    imported_modules: set[str] = set()
    source = ""
    for path in sorted(GHOST_ROOT.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        source += text
        tree = ast.parse(text)
        imported_modules.update(
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        )
        imported_modules.update(
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        )

    assert "hyperliquid.exchange" not in imported_modules
    assert "requests" not in imported_modules
    assert "websocket" not in imported_modules
    lowered = source.lower()
    for forbidden in ("private_key", "seed_phrase", "signer", "wallet_key"):
        assert forbidden not in lowered


def test_ghost_cli_exposes_only_local_replay() -> None:
    result = CliRunner().invoke(app, ["ghost", "--help"])
    assert result.exit_code == 0
    assert "replay" in result.output
    for forbidden in (" live ", " trade ", " mainnet ", " order "):
        assert forbidden not in f" {result.output.lower()} "
