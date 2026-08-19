from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import hyperlab.cli as cli_module
import hyperlab.environment_authorization as authorization_module
from hyperlab.cli import app
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import PAPER_ENGINE_BUILD_HASH, PaperRunConfig
from hyperlab.paper.phase05_portfolio import build_phase05_phase08_paper_foundation
from hyperlab.paper.runtime import PaperAdmissionError, PaperRuntime, PaperRuntimeConfig
from hyperlab.paper.store import PaperStore

ROOT = Path(__file__).resolve().parents[1]
V10_ROOT = ROOT / "config" / "paper" / "phase08-phase05-multistrategy-paper-v1"
V9_CANDIDATE_ID = "phase08-robust-pairs-btc-eth-paper-v1"
V10_CANDIDATE_ID = "phase08-phase05-multistrategy-paper-v1"


def _v10_config() -> PaperRunConfig:
    return PaperRunConfig.from_dict(
        json.loads((V10_ROOT / "paper-config.json").read_bytes())
    )


def _runtime_verifier(config: PaperRunConfig) -> PaperRuntime:
    runtime = object.__new__(PaperRuntime)
    runtime.engine = SimpleNamespace(config=config)
    return runtime


def test_v9_runtime_release_verification_uses_historical_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = replace(
        _v10_config(),
        schema_version=2,
        strategies=(),
        engine_build_hash=PAPER_ENGINE_BUILD_HASH,
        release_code_sha256="1" * 64,
    )
    observed: list[str] = []

    def current_digest(*, candidate_id: str) -> str:
        observed.append(candidate_id)
        return config.release_code_sha256

    monkeypatch.setattr(
        authorization_module,
        "current_paper_release_code_sha256",
        current_digest,
    )

    _runtime_verifier(config)._verify_release_code()

    assert observed == [V9_CANDIDATE_ID]


def test_v10_runtime_release_verification_never_uses_default_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = replace(_v10_config(), release_code_sha256="2" * 64)
    observed: list[str] = []

    def current_digest(*, candidate_id: str) -> str:
        observed.append(candidate_id)
        if candidate_id == V9_CANDIDATE_ID:
            raise AssertionError("V10 must not use the historical default candidate")
        return config.release_code_sha256

    monkeypatch.setattr(
        authorization_module,
        "current_paper_release_code_sha256",
        current_digest,
    )

    _runtime_verifier(config)._verify_release_code()

    assert observed == [V10_CANDIDATE_ID]


def test_v10_runtime_environment_verification_uses_v10_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = replace(_v10_config(), runtime_environment_sha256="3" * 64)
    observed: list[str] = []

    def current_digest(*, candidate_id: str) -> str:
        observed.append(candidate_id)
        return config.runtime_environment_sha256

    monkeypatch.setattr(
        authorization_module,
        "current_paper_runtime_environment_sha256",
        current_digest,
    )

    _runtime_verifier(config)._verify_runtime_environment()

    assert observed == [V10_CANDIDATE_ID]


@pytest.mark.parametrize(
    ("field_name", "method_name", "message"),
    (
        ("release_code_sha256", "_verify_release_code", "release code differs"),
        (
            "runtime_environment_sha256",
            "_verify_runtime_environment",
            "runtime environment differs",
        ),
    ),
)
def test_wrong_v10_frozen_identity_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    method_name: str,
    message: str,
) -> None:
    config = replace(_v10_config(), **{field_name: "4" * 64})
    function_name = (
        "current_paper_release_code_sha256"
        if field_name == "release_code_sha256"
        else "current_paper_runtime_environment_sha256"
    )
    monkeypatch.setattr(
        authorization_module,
        function_name,
        lambda *, candidate_id: "5" * 64,
    )

    with pytest.raises(PaperAdmissionError, match=message):
        getattr(_runtime_verifier(config), method_name)()


def test_unknown_or_mismatched_candidate_identity_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="unsupported PAPER candidate identity"):
        authorization_module.paper_release_identity_candidate("unknown-paper-candidate")

    config = replace(_v10_config(), release_code_sha256="6" * 64)
    monkeypatch.setattr(
        authorization_module,
        "current_paper_release_code_sha256",
        lambda *, candidate_id: (
            "6" * 64 if candidate_id == V9_CANDIDATE_ID else "7" * 64
        ),
    )

    with pytest.raises(PaperAdmissionError, match="release code differs"):
        _runtime_verifier(config)._verify_release_code()


def test_ready_preflight_then_v10_runtime_construction_uses_same_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda: SimpleNamespace(
            app=SimpleNamespace(mode="readonly", data_dir=tmp_path / "data")
        ),
    )
    preflight = CliRunner().invoke(app, ["paper", "preflight"])
    assert preflight.exit_code == 0, preflight.output
    assert json.loads(preflight.stdout)["candidate_id"] == V10_CANDIDATE_ID

    config = _v10_config()
    foundation = build_phase05_phase08_paper_foundation(
        runtime_status_path=tmp_path / "source-status.json",
        validation_started_at=config.validation_started_at,
        release_code_sha256=config.release_code_sha256,
        runtime_environment_sha256=config.runtime_environment_sha256,
    )
    store = PaperStore(tmp_path / "paper.sqlite3")
    try:
        runtime = PaperRuntime(
            PaperEngine(store, config),
            foundation.strategies,
            foundation.source,
            config=PaperRuntimeConfig(
                timer_interval_seconds=config.runtime_timer_interval_seconds,
                source_poll_timeout_seconds=config.runtime_source_poll_timeout_seconds,
            ),
        )
        assert runtime.orders_enabled is False
    finally:
        foundation.source.close()
        store.close()
