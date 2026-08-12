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


def test_default_research_settings_have_locked_split_and_no_return_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HYPERLAB_MODE", raising=False)
    settings = load_settings(_write_config(tmp_path, "research"))

    assert settings.research.train_fraction == 0.60
    assert settings.research.validation_fraction == 0.20
    assert settings.research.walk_forward_step_bars == settings.research.walk_forward_validation_bars
    assert settings.research.benchmark.annual_rate == 0.045
    assert not hasattr(settings.research, "target_return")
