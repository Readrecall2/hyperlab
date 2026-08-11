from __future__ import annotations

from pathlib import Path


def test_source_tree_has_no_exchange_executor_import() -> None:
    root = Path(__file__).resolve().parents[1] / "src"
    content = "\n".join(path.read_text(encoding="utf-8") for path in root.rglob("*.py"))
    forbidden = ["from hyperliquid.exchange import", "import hyperliquid.exchange", "private_key", "seed_phrase"]
    for token in forbidden:
        assert token not in content
