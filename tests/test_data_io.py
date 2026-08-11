from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hyperlab.data.io import save_panel_parquet
from hyperlab.models import MarketPanel


def _panel() -> MarketPanel:
    index = pd.date_range("2026-01-01", periods=2, freq="1h", tz="UTC")
    prices = pd.DataFrame({"HL:BTC:perp": [100.0, 101.0]}, index=index)
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    return MarketPanel(
        prices=prices,
        funding=zero.copy(),
        spreads_bps=zero.copy(),
        volume_usd=zero.copy(),
    )


def test_legacy_panel_parquet_export_never_overwrites(tmp_path: Path) -> None:
    output = tmp_path / "panel"
    save_panel_parquet(_panel(), output)
    original = (output / "prices.parquet").read_bytes()

    with pytest.raises(FileExistsError, match="immutable"):
        save_panel_parquet(_panel(), output)

    assert (output / "prices.parquet").read_bytes() == original


def test_legacy_panel_parquet_export_refuses_a_preexisting_empty_directory(
    tmp_path: Path,
) -> None:
    output = tmp_path / "reserved"
    output.mkdir()

    with pytest.raises(FileExistsError, match="immutable"):
        save_panel_parquet(_panel(), output)

    assert list(output.iterdir()) == []
