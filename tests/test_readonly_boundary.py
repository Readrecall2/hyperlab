from __future__ import annotations

from pathlib import Path

from hyperlab.cli import _secret_like_environment_variables


def test_source_tree_has_no_exchange_executor_import() -> None:
    root = Path(__file__).resolve().parents[1] / "src"
    content = "\n".join(path.read_text(encoding="utf-8") for path in root.rglob("*.py"))
    forbidden = ["from hyperliquid.exchange import", "import hyperliquid.exchange", "private_key", "seed_phrase"]
    for token in forbidden:
        assert token not in content


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
