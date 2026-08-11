from __future__ import annotations

from pathlib import Path

import pytest

from hyperlab.config import load_settings


def _write_config(tmp_path: Path, mode: str) -> Path:
    path = tmp_path / "research.toml"
    path.write_text(f'[app]\nmode = "{mode}"\n', encoding="utf-8")
    return path


@pytest.mark.parametrize("mode", ["readonly", "research"])
def test_load_settings_accepts_only_safe_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    monkeypatch.delenv("HYPERLAB_MODE", raising=False)

    assert load_settings(_write_config(tmp_path, mode)).app.mode == mode


@pytest.mark.parametrize("mode", ["paper", "testnet", "mainnet", "live", "trade"])
def test_load_settings_rejects_execution_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    monkeypatch.delenv("HYPERLAB_MODE", raising=False)

    with pytest.raises(ValueError, match="only allows readonly/research"):
        load_settings(_write_config(tmp_path, mode))


def test_environment_cannot_enable_an_execution_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HYPERLAB_MODE", "mainnet")

    with pytest.raises(ValueError, match="only allows readonly/research"):
        load_settings(_write_config(tmp_path, "readonly"))
