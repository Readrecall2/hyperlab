from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pytest
import typer
from typer.main import get_command
from typer.testing import CliRunner

import hyperlab.cli as cli_module
from hyperlab.backtest.protocol import canonical_json
from hyperlab.cli import app
from hyperlab.paper.engine import PaperCommandResult, PaperEngine
from hyperlab.paper.models import (
    DecisionIntent,
    MarketEvent,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    PaperState,
    deterministic_id,
)
from hyperlab.paper.runner import PaperStrategyView
from hyperlab.paper.runtime import PaperRuntimeLease, PublicSourceDescriptor
from hyperlab.paper.store import PaperStore

_ROOT = Path(__file__).resolve().parents[1]
_START = datetime(2026, 8, 17, 9, tzinfo=UTC)
_STRATEGY_HASH = "a" * 64
_DATA_HASH = "b" * 64
_SOURCE = "phase12-cli-public-fixture"
_INSTRUMENT = "HL:BTC:perp"


class HoldStrategy:
    strategy_name = "phase12_cli_fixture"
    strategy_hash = _STRATEGY_HASH

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        del markets, view
        return None


class RecordingSource:
    def __init__(
        self,
        *,
        descriptor: PublicSourceDescriptor,
        on_start=None,  # type: ignore[no-untyped-def]
        interrupt_on_poll: bool = False,
    ) -> None:
        self._descriptor = descriptor
        self.on_start = on_start
        self.interrupt_on_poll = interrupt_on_poll
        self.started = False
        self.closed = False
        self.stopped = False

    @property
    def descriptor(self) -> PublicSourceDescriptor:
        return self._descriptor

    def start(self) -> None:
        self.started = True
        if callable(self.on_start):
            self.on_start()

    def poll(self, *, timeout_seconds: float) -> object | None:
        del timeout_seconds
        if self.interrupt_on_poll:
            raise KeyboardInterrupt
        return None

    def stop(self) -> None:
        self.stopped = True

    def close(self) -> None:
        self.closed = True


def _config(
    *,
    strategy_hash: str = _STRATEGY_HASH,
    data_hash: str = _DATA_HASH,
    data_source: str = _SOURCE,
    parameters: Mapping[str, object] | None = None,
) -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="phase12_cli_fixture",
        strategy_hash=strategy_hash,
        parameters=parameters or {"technical_only": True, "version": 1},
        data_hash=data_hash,
        execution=PaperExecutionConfig(
            maker_fee_bps=Decimal("1.5"),
            taker_fee_bps=Decimal("4.5"),
            calibration_status="UNCALIBRATED",
            source="public-tier0-technical-only",
        ),
        risk=PaperRiskLimits(stale_after_seconds=30),
        seed=12,
        initial_cash=Decimal("10000"),
        validation_started_at=_START,
        run_kind="TECHNICAL",
        data_calibration_status="UNCALIBRATED",
        data_source=data_source,
        required_instruments=(_INSTRUMENT,),
        minimum_validation_cycles=1,
    )


def _write_config(root: Path, config: PaperRunConfig) -> Path:
    path = root / "paper-config.json"
    path.write_text(canonical_json(config.to_dict()), encoding="utf-8")
    return path


def _approval(
    root: Path,
    config: PaperRunConfig,
    *,
    strategy_factory,
    source_factory,
) -> cli_module._ApprovedPaperRuntimeFactories:  # type: ignore[no-untyped-def]
    return cli_module._ApprovedPaperRuntimeFactories(
        candidate_id="phase08-robust-pairs-btc-eth-paper-v1",
        config_hash=config.config_hash,
        config_artifact_path=root / "paper-config.json",
        readiness_manifest_path=root / "readiness-manifest.json",
        readiness_manifest_sha256="c" * 64,
        readiness_profile_sha256="d" * 64,
        readiness_evidence_root=root,
        strategy_factory=strategy_factory,
        source_factory=source_factory,
    )


def _install_approval(
    monkeypatch: pytest.MonkeyPatch,
    approval: cli_module._ApprovedPaperRuntimeFactories,
) -> None:
    monkeypatch.setattr(
        cli_module,
        "_APPROVED_PAPER_RUNTIMES",
        MappingProxyType({approval.config_hash: approval}),
    )
    monkeypatch.setattr(cli_module, "_paper_runtime_settings", lambda: None)


def _market(at: datetime) -> MarketEvent:
    return MarketEvent.create(
        received_at=at,
        instrument=_INSTRUMENT,
        bid_price=Decimal("100"),
        ask_price=Decimal("101"),
        bid_depth=Decimal("10"),
        ask_depth=Decimal("10"),
        source_sequence=1,
    )


def _durable_run(root: Path) -> tuple[Path, PaperRunConfig]:
    database = root / "paper.sqlite3"
    config = _config()
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    engine.process_market(_market(_START + timedelta(seconds=1)))
    engine.reconcile(as_of=_START + timedelta(seconds=1))
    store.close()
    return database, config


def _unclosed_failure_run(
    root: Path,
) -> tuple[Path, PaperRunConfig, datetime]:
    database, config = _durable_run(root)
    store = PaperStore(database, initialize=False)
    engine = PaperEngine(store, config)
    engine.start()
    session_at = _START + timedelta(seconds=2)
    session_id = "9" * 64
    engine.start_runtime_session(
        as_of=session_at,
        session_id=session_id,
        generation=1,
    )
    failure_at = _START + timedelta(seconds=3)
    failure_artifact = deterministic_id(
        "paper_runtime_failure_v1",
        config.run_id,
        config.config_hash,
        "UNCLOSED_RUNTIME_SESSION",
        "UnclosedRuntimeSessionError",
        session_id,
    )
    engine.pause(
        as_of=failure_at,
        reason=(
            "terminal paper runtime failure: UNCLOSED_RUNTIME_SESSION: "
            "UnclosedRuntimeSessionError"
        ),
        operator_artifact_hash=failure_artifact,
        origin="PAPER_RUNTIME_FAILURE",
    )
    store.close()
    return database, config, failure_at


def _invoke_operator(
    action: str,
    run_id: str,
    database: Path,
    *,
    at: datetime,
    extra: list[str],
):  # type: ignore[no-untyped-def]
    return CliRunner().invoke(
        app,
        [
            "paper",
            action,
            run_id,
            "--database",
            str(database),
            "--as-of",
            at.isoformat(),
            *extra,
        ],
    )


def test_default_registry_contains_exactly_one_compiled_phase08_candidate() -> None:
    assert len(cli_module._APPROVED_PAPER_RUNTIMES) == 1
    approval = next(iter(cli_module._APPROVED_PAPER_RUNTIMES.values()))

    assert approval.candidate_id == "phase08-robust-pairs-btc-eth-paper-v1"
    assert approval.config_artifact_path == (
        Path("config/paper/phase08-robust-pairs-btc-eth-paper-v1") / "paper-config.json"
    )
    assert approval.readiness_manifest_path == (
        Path("config/paper/phase08-robust-pairs-btc-eth-paper-v1") / "readiness-manifest.json"
    )
    assert approval.readiness_evidence_root == Path("config/paper/phase08-robust-pairs-btc-eth-paper-v1")
    assert len(approval.config_hash) == 64
    assert set(approval.config_hash) <= set("0123456789abcdef")
    assert len(approval.readiness_manifest_sha256) == 64
    assert set(approval.readiness_manifest_sha256) <= set("0123456789abcdef")


def test_default_preflight_accepts_exact_compiled_candidate_without_transport_or_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status_path = tmp_path / "paper" / "phase12-public-source-status.json"
    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda: SimpleNamespace(app=SimpleNamespace(mode="readonly", data_dir=tmp_path)),
    )

    result = CliRunner().invoke(app, ["paper", "preflight"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["candidate_id"] == ("phase08-robust-pairs-btc-eth-paper-v1")
    assert payload["config_hash"] == cli_module._PHASE12_PAPER_CONFIG_HASH
    assert payload["public_source"] == {
        "bootstrap_timeout_seconds": 120.0,
        "data_hash": "f819fbd0a88841cfda22fbbe6a5966a86df0f4b1b453ff261e8095d59c2ddd7c",
        "public_only": True,
        "schema_version": 1,
        "source": "hyperliquid-mainnet-public-bbo-funding-v1",
        "source_kind": "PUBLIC_NORMALIZED",
    }
    assert payload["public_transport_started"] is False
    assert payload["database_created"] is False
    assert payload["credential_scope"] == "NONE"
    assert payload["orders_enabled"] is False
    assert status_path.exists() is False


def test_preflight_rejects_runtime_environment_drift_before_factories_or_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def forbidden(*_args: object, **_kwargs: object) -> object:
        calls.append("factory")
        raise AssertionError("runtime-environment drift must block before factories")

    monkeypatch.setattr(
        cli_module,
        "current_paper_runtime_environment_sha256",
        lambda: "0" * 64,
    )
    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda: SimpleNamespace(app=SimpleNamespace(mode="readonly", data_dir=tmp_path)),
    )
    monkeypatch.setattr(
        "hyperlab.paper.collector_source.HyperliquidPaperPublicSource.create_mainnet",
        forbidden,
    )
    monkeypatch.setattr("hyperlab.paper.store.PaperStore", forbidden)

    result = CliRunner().invoke(app, ["paper", "preflight"])

    assert result.exit_code == 2, result.output
    assert "runtime_environment_sha256" in result.output
    assert calls == []
    assert not (tmp_path / "paper.sqlite3").exists()


def test_preflight_blocks_before_strategy_source_or_store_when_authorization_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    artifact = _write_config(tmp_path, config)
    calls: list[str] = []

    def strategy_factory(_config: PaperRunConfig) -> HoldStrategy:
        calls.append("strategy")
        return HoldStrategy()

    def source_factory(_config: PaperRunConfig) -> RecordingSource:
        calls.append("source")
        return RecordingSource(descriptor=PublicSourceDescriptor(_SOURCE, _DATA_HASH))

    approval = _approval(
        tmp_path,
        config,
        strategy_factory=strategy_factory,
        source_factory=source_factory,
    )
    _install_approval(monkeypatch, approval)

    def blocked(*_args, **_kwargs) -> None:  # type: ignore[no-untyped-def]
        calls.append("authorization")
        raise typer.BadParameter("compiled evidence blocked")

    monkeypatch.setattr(cli_module, "_verify_approved_paper_readiness", blocked)

    result = CliRunner().invoke(app, ["paper", "preflight", str(artifact)])

    assert result.exit_code == 2, result.output
    assert "compiled evidence blocked" in result.output
    assert calls == ["authorization"]
    assert not (tmp_path / "paper.sqlite3").exists()


def test_preflight_is_ordered_authorization_then_lazy_factories_without_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    artifact = _write_config(tmp_path, config)
    database = tmp_path / "must-not-exist.sqlite3"
    calls: list[str] = []
    source = RecordingSource(
        descriptor=PublicSourceDescriptor(_SOURCE, _DATA_HASH),
        on_start=lambda: (_ for _ in ()).throw(AssertionError("preflight must never start a transport")),
    )

    def strategy_factory(_config: PaperRunConfig) -> HoldStrategy:
        calls.append("strategy")
        return HoldStrategy()

    def source_factory(_config: PaperRunConfig) -> RecordingSource:
        calls.append("source")
        return source

    approval = _approval(
        tmp_path,
        config,
        strategy_factory=strategy_factory,
        source_factory=source_factory,
    )
    _install_approval(monkeypatch, approval)

    def verified(*_args, **_kwargs) -> None:  # type: ignore[no-untyped-def]
        calls.append("authorization")

    monkeypatch.setattr(cli_module, "_verify_approved_paper_readiness", verified)

    result = CliRunner().invoke(app, ["paper", "preflight", str(artifact)])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert calls == ["authorization", "strategy", "source"]
    assert source.started is False
    assert source.closed is True
    assert payload["status"] == "READY"
    assert payload["credential_scope"] == "NONE"
    assert payload["execution_network"] == "NONE"
    assert payload["wallet_or_signer_required"] is False
    assert payload["public_transport_started"] is False
    assert payload["database_created"] is False
    assert payload["orders_enabled"] is False
    assert not database.exists()


def test_config_swap_changes_self_hash_and_blocks_before_semantic_or_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    approved = _config()
    swapped = replace(
        approved,
        parameters={"technical_only": True, "version": 2},
    )
    assert swapped.config_hash != approved.config_hash
    artifact = _write_config(tmp_path, swapped)
    calls: list[str] = []
    approval = _approval(
        tmp_path,
        approved,
        strategy_factory=lambda _config: calls.append("strategy"),
        source_factory=lambda _config: calls.append("source"),
    )
    _install_approval(monkeypatch, approval)
    monkeypatch.setattr(
        cli_module,
        "_verify_approved_paper_readiness",
        lambda *_args, **_kwargs: calls.append("semantic"),
    )

    result = CliRunner().invoke(app, ["paper", "preflight", str(artifact)])

    assert result.exit_code == 2, result.output
    assert "config_hash" in result.output
    assert calls == []


def test_source_descriptor_swap_closes_before_transport_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = RecordingSource(descriptor=PublicSourceDescriptor(_SOURCE, "f" * 64))
    monkeypatch.setattr(
        "hyperlab.paper.collector_source.HyperliquidPaperPublicSource.create_mainnet",
        lambda **_kwargs: source,
    )
    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda: SimpleNamespace(app=SimpleNamespace(data_dir=tmp_path)),
    )

    with pytest.raises(ValueError, match="descriptor differs"):
        cli_module._phase12_paper_source_factory(_config())

    assert source.started is False
    assert source.closed is True


def test_run_starts_public_source_only_after_authorization_and_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    artifact = _write_config(tmp_path, config)
    database = tmp_path / "paper.sqlite3"
    calls: list[str] = []

    def on_start() -> None:
        check = PaperStore(database, initialize=False)
        try:
            assert check.get_projection(config.run_id).reconciled is True
        finally:
            check.close()
        calls.append("source_start")

    source = RecordingSource(
        descriptor=PublicSourceDescriptor(_SOURCE, _DATA_HASH),
        on_start=on_start,
        interrupt_on_poll=True,
    )

    def strategy_factory(_config: PaperRunConfig) -> HoldStrategy:
        calls.append("strategy")
        return HoldStrategy()

    def source_factory(_config: PaperRunConfig) -> RecordingSource:
        calls.append("source_construct")
        return source

    approval = _approval(
        tmp_path,
        config,
        strategy_factory=strategy_factory,
        source_factory=source_factory,
    )
    monkeypatch.setattr(
        cli_module,
        "_APPROVED_PAPER_RUNTIMES",
        MappingProxyType({config.config_hash: approval}),
    )
    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda: SimpleNamespace(app=SimpleNamespace(mode="readonly", data_dir=tmp_path)),
    )

    def verified(*_args, **_kwargs) -> None:  # type: ignore[no-untyped-def]
        calls.append("authorization")

    monkeypatch.setattr(cli_module, "_verify_approved_paper_readiness", verified)

    result = CliRunner().invoke(
        app,
        ["paper", "run", str(artifact), "--database", str(database)],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        "authorization",
        "strategy",
        "source_construct",
        "source_start",
    ]
    assert source.started is True
    assert source.stopped is True
    assert source.closed is True


@pytest.mark.parametrize(
    ("flag", "value", "message"),
    [
        (
            "--timer-interval-seconds",
            "2",
            "timer-interval-seconds diffère",
        ),
        (
            "--source-poll-timeout-seconds",
            "0.5",
            "source-poll-timeout-seconds diffère",
        ),
    ],
)
def test_run_rejects_runtime_cadence_drift_before_authorization_or_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    flag: str,
    value: str,
    message: str,
) -> None:
    config = _config()
    artifact = _write_config(tmp_path, config)
    database = tmp_path / "must-not-exist.sqlite3"
    calls: list[str] = []
    approval = _approval(
        tmp_path,
        config,
        strategy_factory=lambda _config: calls.append("strategy"),
        source_factory=lambda _config: calls.append("source"),
    )
    _install_approval(monkeypatch, approval)
    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda: SimpleNamespace(app=SimpleNamespace(mode="readonly", data_dir=tmp_path)),
    )
    monkeypatch.setattr(
        cli_module,
        "_verify_approved_paper_readiness",
        lambda *_args, **_kwargs: calls.append("authorization"),
    )

    result = CliRunner().invoke(
        app,
        [
            "paper",
            "run",
            str(artifact),
            "--database",
            str(database),
            flag,
            value,
        ],
    )

    assert result.exit_code == 2, result.output
    assert message in result.output
    assert calls == []
    assert database.exists() is False


def test_standalone_replay_requires_stopped_runtime_lease_and_is_read_only(
    tmp_path: Path,
) -> None:
    database, config = _durable_run(tmp_path)
    before = database.read_bytes()

    with PaperRuntimeLease(database, config.run_id):
        blocked = CliRunner().invoke(
            app,
            [
                "paper",
                "replay",
                config.run_id,
                "--database",
                str(database),
            ],
        )

    assert blocked.exit_code == 2, blocked.output
    assert "already active" in blocked.output
    assert database.read_bytes() == before

    replayed = CliRunner().invoke(
        app,
        [
            "paper",
            "replay",
            config.run_id,
            "--database",
            str(database),
        ],
    )
    assert replayed.exit_code == 0, replayed.output
    assert json.loads(replayed.stdout)["status"] == "REPLAY_EXACT"
    assert database.read_bytes() == before


def test_standalone_reconcile_requires_stopped_runtime_and_reacquires_after_exit(
    tmp_path: Path,
) -> None:
    database, config = _durable_run(tmp_path)
    before = database.read_bytes()

    with PaperRuntimeLease(database, config.run_id):
        blocked = _invoke_operator(
            "reconcile",
            config.run_id,
            database,
            at=_START + timedelta(seconds=2),
            extra=[],
        )

    assert blocked.exit_code == 2, blocked.output
    assert "already active" in blocked.output
    assert database.read_bytes() == before

    reconciled = _invoke_operator(
        "reconcile",
        config.run_id,
        database,
        at=_START + timedelta(seconds=2),
        extra=[],
    )

    assert reconciled.exit_code == 0, reconciled.output
    payload = json.loads(reconciled.stdout)
    assert payload["run_id"] == config.run_id
    assert payload["reconciled"] is True


def test_standalone_reconcile_failure_releases_its_runtime_lease(
    tmp_path: Path,
) -> None:
    database, config = _durable_run(tmp_path)
    before = database.read_bytes()

    failed = _invoke_operator(
        "reconcile",
        config.run_id,
        database,
        at=_START,
        extra=[],
    )

    assert failed.exit_code == 2, failed.output
    assert "précède le dernier événement durable" in failed.output
    assert database.read_bytes() == before
    with PaperRuntimeLease(database, config.run_id):
        pass


def test_standalone_reconcile_rejects_stale_release_before_lease_or_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = replace(_config(), release_code_sha256="0" * 64)
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    engine.process_market(_market(_START + timedelta(seconds=1)))
    engine.reconcile(as_of=_START + timedelta(seconds=1))
    store.close()
    before = database.read_bytes()

    class ForbiddenLease:
        def __init__(self, _database: Path, _run_id: str) -> None:
            raise AssertionError("stale release must block before lease acquisition")

    monkeypatch.setattr(
        "hyperlab.paper.runtime.PaperRuntimeLease",
        ForbiddenLease,
    )

    result = _invoke_operator(
        "reconcile",
        config.run_id,
        database,
        at=_START + timedelta(seconds=2),
        extra=[],
    )

    assert result.exit_code == 2, result.output
    assert "diffère du release_code_sha256 durable" in result.output
    assert database.read_bytes() == before


def test_resume_rejects_stale_release_before_engine_or_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = replace(_config(), release_code_sha256="0" * 64)
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    engine.process_market(_market(_START + timedelta(seconds=1)))
    engine.reconcile(as_of=_START + timedelta(seconds=1))
    engine.pause(
        as_of=_START + timedelta(seconds=2),
        reason="operator inspection",
        operator_artifact_hash="f" * 64,
    )
    store.close()
    before = database.read_bytes()

    class ForbiddenEngine:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("stale release must block before engine construction")

    monkeypatch.setattr(
        "hyperlab.paper.engine.PaperEngine",
        ForbiddenEngine,
    )

    result = _invoke_operator(
        "resume",
        config.run_id,
        database,
        at=_START + timedelta(seconds=3),
        extra=["--review-reason", "public feed and ledger reviewed"],
    )

    assert result.exit_code == 2, result.output
    assert "release_code_sha256 durable" in result.output
    assert database.read_bytes() == before
    readonly = PaperStore(database, initialize=False)
    try:
        assert readonly.get_projection(config.run_id).state is PaperState.PAUSED
    finally:
        readonly.close()


def test_resume_rejects_runtime_environment_drift_before_engine_or_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = replace(_config(), runtime_environment_sha256="0" * 64)
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    engine.process_market(_market(_START + timedelta(seconds=1)))
    engine.reconcile(as_of=_START + timedelta(seconds=1))
    engine.pause(
        as_of=_START + timedelta(seconds=2),
        reason="operator inspection",
        operator_artifact_hash="f" * 64,
    )
    store.close()
    before = database.read_bytes()

    class ForbiddenEngine:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError(
                "runtime-environment drift must block before engine construction"
            )

    monkeypatch.setattr(
        "hyperlab.paper.engine.PaperEngine",
        ForbiddenEngine,
    )

    result = _invoke_operator(
        "resume",
        config.run_id,
        database,
        at=_START + timedelta(seconds=3),
        extra=["--review-reason", "public feed and ledger reviewed"],
    )

    assert result.exit_code == 2, result.output
    assert "runtime_environment_sha256" in result.output
    assert database.read_bytes() == before
    readonly = PaperStore(database, initialize=False)
    try:
        assert readonly.get_projection(config.run_id).state is PaperState.PAUSED
    finally:
        readonly.close()


def test_resume_atomically_rejects_a_newer_critical_incident(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, config = _durable_run(tmp_path)
    initial_store = PaperStore(database, initialize=False)
    initial_engine = PaperEngine(initial_store, config)
    initial_engine.start()
    initial_engine.pause(
        as_of=_START + timedelta(seconds=2),
        reason="initial runtime failure",
        operator_artifact_hash="d" * 64,
        origin="PAPER_RUNTIME_FAILURE",
    )
    initial_store.close()

    original_resume = PaperEngine.resume_from_pause
    injected = False

    def inject_new_failure(
        self: PaperEngine,
        *,
        as_of: datetime,
        review_artifact_hash: str,
        reviewed_critical_incident_count: int,
        reviewed_last_critical_incident_at: datetime | None,
        recovery_mode: str,
    ) -> PaperCommandResult:
        nonlocal injected
        if not injected:
            injected = True
            racing_store = PaperStore(database, initialize=False)
            try:
                racing_engine = PaperEngine(racing_store, config)
                racing_engine.start()
                racing_engine.pause(
                    as_of=as_of,
                    reason="newer runtime failure after operator review",
                    operator_artifact_hash="e" * 64,
                    origin="PAPER_RUNTIME_FAILURE",
                )
            finally:
                racing_store.close()
        return original_resume(
            self,
            as_of=as_of,
            review_artifact_hash=review_artifact_hash,
            reviewed_critical_incident_count=reviewed_critical_incident_count,
            reviewed_last_critical_incident_at=reviewed_last_critical_incident_at,
            recovery_mode=recovery_mode,
        )

    monkeypatch.setattr(PaperEngine, "resume_from_pause", inject_new_failure)

    result = _invoke_operator(
        "resume",
        config.run_id,
        database,
        at=_START + timedelta(seconds=3),
        extra=["--review-reason", "initial incident reviewed"],
    )

    assert result.exit_code == 2, result.output
    assert "differs" in result.output
    store = PaperStore(database, initialize=False)
    try:
        projection = store.get_projection(config.run_id)
        input_types = [
            record.payload["input_type"]
            for record in store.iter_inputs(config.run_id)
        ]
        assert projection.state is PaperState.PAUSED
        assert projection.critical_incident_count == 2
        assert "RESUME_AFTER_REVIEW" not in input_types
    finally:
        store.close()


def test_pause_then_reviewed_resume_is_audited_and_deterministic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, config = _durable_run(tmp_path)
    pause_at = _START + timedelta(seconds=2)
    pause = _invoke_operator(
        "pause",
        config.run_id,
        database,
        at=pause_at,
        extra=["--reason", "operator inspection"],
    )

    assert pause.exit_code == 0, pause.output
    pause_payload = json.loads(pause.stdout)
    assert pause_payload["state"] == "PAUSED"
    assert pause_payload["operator_artifact_hash"] == (
        cli_module._paper_operator_artifact_hash(
            action="PAUSE",
            config=config,
            reason="operator inspection",
            as_of=pause_at,
        )
    )

    class ForbiddenLease:
        def __init__(self, _database: Path, _run_id: str) -> None:
            raise AssertionError("standard resume must remain unleased")

    monkeypatch.setattr(
        "hyperlab.paper.runtime.PaperRuntimeLease",
        ForbiddenLease,
    )

    resume_at = _START + timedelta(seconds=3)
    resume = _invoke_operator(
        "resume",
        config.run_id,
        database,
        at=resume_at,
        extra=["--review-reason", "public feed and ledger reviewed"],
    )

    assert resume.exit_code == 0, resume.output
    resume_payload = json.loads(resume.stdout)
    assert resume_payload["recovery_mode"] == "STANDARD"
    assert resume_payload["state"] == "FLAT"
    assert resume_payload["reconciled"] is True
    assert len(resume_payload["incident_artifact_hash"]) == 64
    assert len(resume_payload["review_artifact_hash"]) == 64
    store = PaperStore(database, initialize=False)
    try:
        inputs = tuple(store.iter_inputs(config.run_id))
        input_types = [record.payload["input_type"] for record in inputs]
        assert "OPERATOR_PAUSE" in input_types
        assert "RESUME_AFTER_REVIEW" in input_types
        resume_input = next(
            record
            for record in inputs
            if record.payload["input_type"] == "RESUME_AFTER_REVIEW"
        )
        assert resume_input.payload["recovery_mode"] == "STANDARD"
        assert resume_input.payload["reviewed_critical_incident_count"] == 0
        assert resume_input.payload["reviewed_last_critical_incident_at"] is None
    finally:
        store.close()


def test_offline_unclosed_recovery_requires_lease_and_is_explicitly_audited(
    tmp_path: Path,
) -> None:
    database, config, failure_at = _unclosed_failure_run(tmp_path)
    recovery_at = failure_at + timedelta(minutes=5)

    result = _invoke_operator(
        "resume",
        config.run_id,
        database,
        at=recovery_at,
        extra=[
            "--review-reason",
            "unclosed runtime session and durable failure reviewed",
            "--offline-unclosed-recovery",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["state"] == "FLAT"
    assert payload["recovery_mode"] == "OFFLINE_UNCLOSED_SESSION"
    store = PaperStore(database, initialize=False)
    try:
        projection = store.get_projection(config.run_id)
        resume_input = next(
            record
            for record in store.iter_inputs(config.run_id)
            if record.payload["input_type"] == "RESUME_AFTER_REVIEW"
        )
        assert projection.runtime_session_active is True
        assert resume_input.payload["recovery_mode"] == "OFFLINE_UNCLOSED_SESSION"
        assert resume_input.payload["reviewed_critical_incident_count"] == 1
        assert resume_input.payload["reviewed_last_critical_incident_at"] == (
            failure_at.isoformat(timespec="microseconds").replace("+00:00", "Z")
        )
    finally:
        store.close()

    with PaperRuntimeLease(database, config.run_id):
        pass


def test_offline_unclosed_recovery_rejects_runtime_lease_contention_without_mutation(
    tmp_path: Path,
) -> None:
    database, config, failure_at = _unclosed_failure_run(tmp_path)
    before = database.read_bytes()

    with PaperRuntimeLease(database, config.run_id):
        result = _invoke_operator(
            "resume",
            config.run_id,
            database,
            at=failure_at + timedelta(minutes=5),
            extra=[
                "--review-reason",
                "unclosed runtime session reviewed",
                "--offline-unclosed-recovery",
            ],
        )

    assert result.exit_code == 2, result.output
    assert "already active" in result.output.lower()
    assert database.read_bytes() == before
    store = PaperStore(database, initialize=False)
    try:
        projection = store.get_projection(config.run_id)
        assert projection.state is PaperState.PAUSED
        assert projection.runtime_session_active is True
        assert all(
            record.payload["input_type"] != "RESUME_AFTER_REVIEW"
            for record in store.iter_inputs(config.run_id)
        )
    finally:
        store.close()


def test_kill_is_terminal_and_confirmation_mismatch_is_non_mutating(
    tmp_path: Path,
) -> None:
    database, config = _durable_run(tmp_path)
    before = database.read_bytes()
    refused = _invoke_operator(
        "kill",
        config.run_id,
        database,
        at=_START + timedelta(seconds=2),
        extra=[
            "--reason",
            "terminal operator stop",
            "--confirm-run-id",
            "wrong-run",
        ],
    )
    assert refused.exit_code == 2
    assert database.read_bytes() == before

    killed = _invoke_operator(
        "kill",
        config.run_id,
        database,
        at=_START + timedelta(seconds=2),
        extra=[
            "--reason",
            "terminal operator stop",
            "--confirm-run-id",
            config.run_id,
        ],
    )
    assert killed.exit_code == 0, killed.output
    payload = json.loads(killed.stdout)
    assert payload["state"] == "MANUAL_REVIEW"
    assert payload["terminal"] is True
    assert payload["resumable"] is False

    resumed = _invoke_operator(
        "resume",
        config.run_id,
        database,
        at=_START + timedelta(seconds=3),
        extra=["--review-reason", "attempted reset"],
    )
    assert resumed.exit_code == 2
    assert "terminal" in resumed.output
    store = PaperStore(database, initialize=False)
    try:
        assert store.get_projection(config.run_id).state is PaperState.MANUAL_REVIEW
    finally:
        store.close()


def test_report_command_is_bounded_read_only_and_does_not_construct_source(
    tmp_path: Path,
) -> None:
    database, config = _durable_run(tmp_path)
    before = {path.name: path.read_bytes() for path in tmp_path.glob("paper.sqlite3*") if path.is_file()}

    result = CliRunner().invoke(
        app,
        [
            "paper",
            "report",
            config.run_id,
            "--database",
            str(database),
            "--timeline-limit",
            "10",
            "--day-limit",
            "2",
            "--alert-limit",
            "5",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    after = {path.name: path.read_bytes() for path in tmp_path.glob("paper.sqlite3*") if path.is_file()}
    assert after == before
    assert payload["integrity"] == "HEAD_ANCHORS_VERIFIED_READONLY"
    assert payload["orders_enabled"] is False
    assert payload["classification"]["technical_only"] is True
    assert payload["classification"]["profitability_evidence"] is False


def test_real_registry_gate_and_report_load_the_raw_frozen_config(
    tmp_path: Path,
) -> None:
    config_artifact = (
        _ROOT / "config" / "paper" / "phase08-robust-pairs-btc-eth-paper-v1" / "paper-config.json"
    )
    raw_config = json.loads(config_artifact.read_text(encoding="utf-8"))
    config = PaperRunConfig.from_dict(raw_config)
    database = tmp_path / "paper.sqlite3"
    store = PaperStore(database)
    try:
        engine = PaperEngine(store, config)
        engine.start()
        engine.reconcile(as_of=config.validation_started_at)
    finally:
        store.close()

    gate = CliRunner().invoke(
        app,
        [
            "paper",
            "gate",
            config.run_id,
            "--database",
            str(database),
        ],
    )
    assert gate.exit_code == 2, gate.output
    gate_payload = json.loads(gate.stdout)
    assert gate_payload["readiness_status"] == "VERIFIED"
    assert gate_payload["authorizes_real_money"] is False

    report = CliRunner().invoke(
        app,
        [
            "paper",
            "report",
            config.run_id,
            "--database",
            str(database),
            "--timeline-limit",
            "10",
            "--day-limit",
            "2",
            "--alert-limit",
            "5",
        ],
    )
    assert report.exit_code == 0, report.output
    report_payload = json.loads(report.stdout)
    assert report_payload["identity"]["config_hash"] == config.config_hash
    assert report_payload["integrity"] == "HEAD_ANCHORS_VERIFIED_READONLY"
    assert report_payload["orders_enabled"] is False


@pytest.mark.parametrize("scope", ["single", "list"])
def test_status_uses_head_integrity_and_never_collects_full_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scope: str,
) -> None:
    database, config = _durable_run(tmp_path)
    list_limits: list[int | None] = []
    recent_limits: list[int] = []
    original_list_runs = PaperStore.list_runs
    original_recent_alerts = PaperStore.get_recent_alerts

    def forbid_full_history(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("live status may not verify or collect full history")

    def bounded_recent_alerts(
        store: PaperStore,
        run_id: str,
        *,
        limit: int,
    ):  # type: ignore[no-untyped-def]
        recent_limits.append(limit)
        return original_recent_alerts(store, run_id, limit=limit)

    def bounded_list_runs(
        store: PaperStore,
        *,
        limit: int | None = None,
    ):  # type: ignore[no-untyped-def]
        list_limits.append(limit)
        return original_list_runs(store, limit=limit)

    monkeypatch.setattr(
        PaperStore,
        "inspect_integrity_readonly",
        forbid_full_history,
    )
    monkeypatch.setattr(PaperStore, "read_snapshot", forbid_full_history)
    monkeypatch.setattr(PaperStore, "get_alerts", forbid_full_history)
    monkeypatch.setattr(PaperStore, "get_recent_alerts", bounded_recent_alerts)
    arguments = ["paper", "status", "--database", str(database)]
    monkeypatch.setattr(PaperStore, "list_runs", bounded_list_runs)
    if scope == "single":
        arguments.extend(["--run-id", config.run_id])

    result = CliRunner().invoke(app, arguments)

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    if scope == "single":
        assert payload["integrity"] == "HEAD_ANCHORS_VERIFIED_READONLY"
        assert payload["alert_limit"] == 50
        assert len(payload["alerts"]) <= 50
        assert recent_limits == [51]
        assert list_limits == []
    else:
        assert payload["runs"][0]["integrity"] == ("HEAD_ANCHORS_VERIFIED_READONLY")
        assert recent_limits == []

        assert payload["run_limit"] == 50
        assert payload["runs_truncated"] is False
        assert list_limits == [51, 51]


def test_status_exposes_active_unclosed_runtime_session_and_incident(tmp_path: Path) -> None:
    database, config, _failure_at = _unclosed_failure_run(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "paper",
            "status",
            "--database",
            str(database),
            "--run-id",
            config.run_id,
        ],
    )

    assert result.exit_code == 0, result.output
    session = json.loads(result.stdout)["runtime_session"]
    assert session["active"] is True
    assert session["unclosed"] is True
    assert session["generation"] == 1
    assert session["session_id"] == "9" * 64
    assert session["started_at"] == "2026-08-17T09:00:02.000000Z"
    assert session["stopped_at"] is None
    assert [incident["code"] for incident in session["recent_incidents"]] == [
        "PAPER_RUNTIME_FAILURE"
    ]
    assert {"pid", "process_id", "database_path", "lock_path"}.isdisjoint(session)


def test_status_retries_one_mid_read_commit_and_returns_one_coherent_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, config = _durable_run(tmp_path)
    writer_store = PaperStore(database, initialize=False)
    writer = PaperEngine(writer_store, config)
    writer.start()
    original_recent_alerts = PaperStore.get_recent_alerts
    calls = 0

    def race_once(
        store: PaperStore,
        run_id: str,
        *,
        limit: int,
    ):  # type: ignore[no-untyped-def]
        nonlocal calls
        alerts = original_recent_alerts(store, run_id, limit=limit)
        calls += 1
        if calls == 1:
            writer.reconcile(as_of=_START + timedelta(seconds=2))
        return alerts

    monkeypatch.setattr(PaperStore, "get_recent_alerts", race_once)
    result = CliRunner().invoke(
        app,
        [
            "paper",
            "status",
            "--database",
            str(database),
            "--run-id",
            config.run_id,
        ],
    )
    writer_store.close()

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert calls == 2
    assert payload["same_head_assembly"] is True
    assert payload["head_read_attempt_limit"] == 2
    assert payload["commit_head_hash"] == (
        PaperStore(database, initialize=False).get_run(config.run_id).commit_head_hash
    )


def test_status_fails_explicitly_when_both_head_attempts_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, config = _durable_run(tmp_path)
    writer_store = PaperStore(database, initialize=False)
    writer = PaperEngine(writer_store, config)
    writer.start()
    original_recent_alerts = PaperStore.get_recent_alerts
    calls = 0

    def race_every_attempt(
        store: PaperStore,
        run_id: str,
        *,
        limit: int,
    ):  # type: ignore[no-untyped-def]
        nonlocal calls
        alerts = original_recent_alerts(store, run_id, limit=limit)
        calls += 1
        writer.reconcile(as_of=_START + timedelta(seconds=2 + calls))
        return alerts

    monkeypatch.setattr(PaperStore, "get_recent_alerts", race_every_attempt)
    result = CliRunner().invoke(
        app,
        [
            "paper",
            "status",
            "--database",
            str(database),
            "--run-id",
            config.run_id,
        ],
    )
    writer_store.close()

    assert result.exit_code == 2, result.output
    assert "HEAD_CHANGED_RETRY" in result.output
    assert calls == 2


def test_status_list_retains_the_newest_runs_after_dropping_the_sentinel(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    store = PaperStore(database)
    for ordinal in range(52):
        store.create_run(
            f"bounded-status-{ordinal:02d}",
            {"fixture": ordinal},
            seed=ordinal,
            created_at=f"2026-08-17T08:{ordinal:02d}:00Z",
        )
    store.close()

    result = CliRunner().invoke(
        app,
        [
            "paper",
            "status",
            "--database",
            str(database),
            "--run-limit",
            "50",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["runs_truncated"] is True
    assert [run["run_id"] for run in payload["runs"]] == [
        f"bounded-status-{ordinal:02d}" for ordinal in range(2, 52)
    ]


@pytest.mark.parametrize("run_limit", ["0", "101"])
def test_status_rejects_run_limits_outside_the_compiled_bound(
    tmp_path: Path,
    run_limit: str,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "paper",
            "status",
            "--database",
            str(tmp_path / "not-opened.sqlite3"),
            "--run-limit",
            run_limit,
        ],
    )
    assert result.exit_code == 2


@pytest.mark.parametrize("mode", ["testnet", "mainnet"])
@pytest.mark.parametrize(
    "action",
    ["report", "replay", "reconcile", "pause", "resume", "kill"],
)
def test_operator_commands_reject_non_paper_modes_before_store_access(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    action: str,
) -> None:
    store_accessed = False

    def forbidden_database_path(_database: Path | None) -> Path:
        nonlocal store_accessed
        store_accessed = True
        raise AssertionError("mode guard must precede Paper store access")

    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda: SimpleNamespace(app=SimpleNamespace(mode=mode)),
    )
    monkeypatch.setattr(
        cli_module,
        "_paper_database_path",
        forbidden_database_path,
    )
    action_arguments = {
        "report": ["run-id"],
        "replay": ["run-id"],
        "reconcile": ["run-id"],
        "pause": ["run-id", "--reason", "operator pause"],
        "resume": ["run-id", "--review-reason", "review complete"],
        "kill": [
            "run-id",
            "--reason",
            "operator kill",
            "--confirm-run-id",
            "run-id",
        ],
    }

    result = CliRunner().invoke(
        app,
        ["paper", action, *action_arguments[action]],
    )

    assert result.exit_code == 2, result.output
    assert "readonly/research" in result.output
    assert store_accessed is False


def test_paper_command_surface_is_isolated_from_testnet_and_real_money_names() -> None:
    root = get_command(app)
    assert hasattr(root, "commands")
    paper = root.commands["paper"]
    assert hasattr(paper, "commands")

    expected = {
        "gate",
        "kill",
        "pause",
        "preflight",
        "reconcile",
        "replay",
        "report",
        "resume",
        "run",
        "status",
    }
    assert expected <= set(paper.commands)
    assert {
        "live",
        "trade",
        "testnet",
        "mainnet",
        "micro-mainnet",
    }.isdisjoint(paper.commands)
