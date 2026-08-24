from __future__ import annotations

import hashlib
import json
import sqlite3
import stat
from collections.abc import Mapping
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest
from pytest import MonkeyPatch
from test_paper_golden_v3 import _build_source
from test_paper_golden_v3_certification_real_gates import _build_full_coverage_source
from test_paper_multistrategy import (
    _BTC,
    _START,
    _portfolio_config,
    _strategy_config,
    _strategy_decision,
)

import hyperlab.paper.golden_v3_certification as certification
from hyperlab.backtest.protocol import canonical_json, canonical_sha256
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.golden_v3 import GoldenDifferentialError
from hyperlab.paper.golden_v3_certification import (
    GoldenCertificationError,
    GoldenCertificationResult,
    GoldenReplayDivergenceError,
    certify_golden_v3,
    verify_golden_v3_certification,
)
from hyperlab.paper.golden_v3_replay import (
    GoldenReplayError,
    GoldenReplayMismatchError,
)
from hyperlab.paper.models import DecisionAction, MarketEvent, OrderSide
from hyperlab.paper.store import PaperStore

_CLASSIFICATION_KEYS = (
    "BLOCKING_INTEGRITY_GATES",
    "COVERAGE_METADATA_NON_BLOCKING",
    "ECONOMIC_EVIDENCE",
    "golden_scope",
    "phase05_decision_coverage",
    "phase08_decision_coverage",
    "market_gap_coverage",
    "strategy_behavior_complete",
    "economic_evidence",
    "authorizes_real_money",
)
_TECHNICAL_SCOPE = "TECHNICAL_STORAGE_AND_REPLAY_ORACLE"


def _market(*, sequence: int, seconds: int, gap: bool = False) -> MarketEvent:
    return MarketEvent.create(
        received_at=_START + timedelta(seconds=seconds),
        instrument=_BTC,
        bid_price=Decimal("100"),
        ask_price=Decimal("100"),
        bid_depth=Decimal("1000"),
        ask_depth=Decimal("1000"),
        source_sequence=sequence,
        gap=gap,
        tradable=not gap,
        source_event_kind="gap" if gap else "bbo",
        source_connection_id="golden-v3-coverage-fixture",
        source_connection_epoch=1,
    )


def _build_phase08_only_source(database: Path, *, include_market_gap: bool) -> str:
    phase05 = _strategy_config("phase05_cash_and_carry", instrument=_BTC)
    phase08 = _strategy_config("phase08_robust_pairs", instrument=_BTC)
    config = _portfolio_config((phase08, phase05))
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()

    observed = _market(sequence=1, seconds=1)
    engine.process_market(observed)
    markets = {_BTC: observed}
    engine.submit_decision(
        _strategy_decision(
            config,
            phase08,
            markets,
            action=DecisionAction.ENTRY,
            order_specs=((_BTC, OrderSide.BUY, "0.20"),),
            decided_at=observed.received_at,
            ordinal=0,
        ),
        markets,
    )
    engine.process_market(_market(sequence=2, seconds=2))
    engine.post_funding(
        instrument=_BTC,
        amount=Decimal("0"),
        occurred_at=_START + timedelta(seconds=3),
        source_event_id="c" * 64,
        funding_rate=Decimal("0"),
        funding_interval_seconds=3600,
        rate_kind="SYNTHETIC_TEST_RATE",
        mark_price=Decimal("100"),
        source_mark_price=Decimal("100"),
        position_quantity=Decimal("0.20"),
        mark_source="PUBLIC_SETTLEMENT_MARK",
        source_observation_id="golden-v3-coverage-funding",
        received_at=_START + timedelta(seconds=3),
        processed_at=_START + timedelta(seconds=3),
    )
    engine.process_timer(as_of=_START + timedelta(seconds=4))
    assert engine.reconcile(as_of=_START + timedelta(seconds=5)).projection.reconciled is True
    if include_market_gap:
        assert engine.process_market(_market(sequence=3, seconds=6, gap=True)).projection.state.value == "PAUSED"
        assert [alert.code for alert in store.get_alerts(config.run_id)].count("MARKET_GAP") == 1

    assert store.inspect_integrity_readonly(config.run_id).ok is True
    store.close()
    return config.run_id


def _insert_uncommitted_market_gap(database: Path, run_id: str) -> None:
    payload = {
        "code": "MARKET_GAP",
        "message": "synthetic orphan MARKET_GAP coverage fixture",
        "run_id": run_id,
        "severity": "CRITICAL",
    }
    payload_json = canonical_json(payload)
    alert_id = canonical_sha256(
        {
            "domain": "hyperlab-golden-v3-orphan-market-gap-v1",
            "run_id": run_id,
        }
    )
    with sqlite3.connect(database) as connection:
        event_sequence = int(
            connection.execute(
                "SELECT event_count FROM paper_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]
        )
        connection.execute(
            """
            INSERT INTO paper_alerts (
                run_id, alert_id, commit_sequence, event_sequence, severity,
                code, payload_json, payload_hash, created_at
            ) VALUES (?, ?, NULL, ?, 'CRITICAL', 'MARKET_GAP', ?, ?, ?)
            """,
            (
                run_id,
                alert_id,
                event_sequence,
                payload_json,
                canonical_sha256(payload),
                _START.isoformat(),
            ),
        )


def _certify(source: Path, candidate: Path, run_id: str) -> GoldenCertificationResult:
    expected_size = source.stat().st_size
    expected_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    source.chmod(stat.S_IREAD)
    try:
        return certify_golden_v3(
            source,
            candidate,
            run_id,
            sentinel_path=source.parent / "forbidden-original.sqlite3",
            expected_source_size=expected_size,
            expected_source_sha256=expected_sha256,
            shard_rows=1_000,
            shard_bytes=1_000_000,
        )
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)


def _classification(candidate: Path, result: GoldenCertificationResult) -> Mapping[str, object]:
    verified = verify_golden_v3_certification(candidate)
    assert verified.certification_root_hash == result.certification_root_hash
    manifest = cast(
        dict[str, object],
        json.loads(result.manifest_path.read_text(encoding="utf-8")),
    )
    observed = {key: manifest[key] for key in _CLASSIFICATION_KEYS}
    result_payload = json.loads(
        (candidate / "results" / "coverage-classification.json").read_text(encoding="utf-8")
    )
    assert result_payload == observed
    assert observed["BLOCKING_INTEGRITY_GATES"] == []
    assert observed["golden_scope"] == _TECHNICAL_SCOPE
    assert observed["economic_evidence"] is False
    assert observed["authorizes_real_money"] is False
    economic = cast(Mapping[str, object], observed["ECONOMIC_EVIDENCE"])
    assert economic["economic_evidence"] is False
    assert economic["authorizes_real_money"] is False
    return observed


def test_phase08_only_decisions_certify_with_nonblocking_phase05_metadata(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "phase08-only.sqlite3"
    run_id = _build_phase08_only_source(source, include_market_gap=False)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)

    observed = _classification(candidate, _certify(source, candidate, run_id))

    assert observed["phase05_decision_coverage"] is False
    assert observed["phase08_decision_coverage"] is True
    assert observed["market_gap_coverage"] is False
    assert observed["strategy_behavior_complete"] is False
    metadata = cast(Mapping[str, object], observed["COVERAGE_METADATA_NON_BLOCKING"])
    assert metadata["phase05_decision_coverage"] is False
    assert metadata["phase08_decision_coverage"] is True


def test_durable_reproducible_market_gap_is_nonblocking_coverage_metadata(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "durable-market-gap.sqlite3"
    run_id = _build_phase08_only_source(source, include_market_gap=True)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)

    observed = _classification(candidate, _certify(source, candidate, run_id))

    assert observed["market_gap_coverage"] is True
    assert observed["strategy_behavior_complete"] is False
    metadata = cast(Mapping[str, object], observed["COVERAGE_METADATA_NON_BLOCKING"])
    assert metadata["market_gap_coverage"] is True


def test_uncommitted_orphan_market_gap_remains_an_integrity_blocker(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "orphan-market-gap.sqlite3"
    run_id = _build_source(source, include_unlinked_alert=False)
    _insert_uncommitted_market_gap(source, run_id)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)

    with pytest.raises(
        GoldenCertificationError,
        match=r"UNCOMMITTED_ALERTS_PRESENT|blocking integrity",
    ):
        _certify(source, candidate, run_id)

    assert not (candidate / "COMPLETE").exists()
    assert not (candidate / "manifests" / "certification-manifest.json").exists()
    assert not (candidate / "pin" / "certification.pin.json").exists()


def test_committed_market_gap_with_incoherent_event_binding_is_refused(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "incoherent-market-gap.sqlite3"
    run_id = _build_phase08_only_source(source, include_market_gap=True)
    with sqlite3.connect(source) as connection:
        trigger_row = connection.execute(
            """
            SELECT sql
            FROM sqlite_schema
            WHERE type='trigger' AND name='paper_alerts_no_update'
            """
        ).fetchone()
        assert trigger_row is not None and isinstance(trigger_row[0], str)
        trigger_sql = trigger_row[0]
        connection.execute("DROP TRIGGER paper_alerts_no_update")
        connection.execute(
            """
            UPDATE paper_alerts
            SET event_sequence=1
            WHERE run_id=? AND code='MARKET_GAP'
            """,
            (run_id,),
        )
        connection.execute(trigger_sql)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)

    with pytest.raises(
        GoldenCertificationError,
        match=r"MARKET_GAP|ALERT_RAISED|committed alert",
    ):
        _certify(source, candidate, run_id)

    assert not (candidate / "COMPLETE").exists()
    assert not (candidate / "manifests" / "certification-manifest.json").exists()
    assert not (candidate / "pin" / "certification.pin.json").exists()


def test_absent_phase05_and_phase08_behaviors_still_certify_only_a_structural_oracle(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "structural-only.sqlite3"
    run_id = _build_source(source, include_unlinked_alert=False)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)

    observed = _classification(candidate, _certify(source, candidate, run_id))

    assert observed["phase05_decision_coverage"] is False
    assert observed["phase08_decision_coverage"] is False
    assert observed["strategy_behavior_complete"] is False
    assert observed["economic_evidence"] is False
    assert observed["authorizes_real_money"] is False


@pytest.mark.parametrize("stage", ["dual_extraction", "replay"])
def test_ab_or_replay_divergence_remains_blocking(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    stage: str,
) -> None:
    source = tmp_path / f"{stage}.sqlite3"
    run_id, _, _ = _build_full_coverage_source(source)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)

    if stage == "dual_extraction":

        def diverged_exports(*_args: object, **_kwargs: object) -> object:
            raise GoldenDifferentialError("synthetic A/B divergence")

        monkeypatch.setattr(certification, "compare_golden_exports", diverged_exports)
        expected = r"A/B divergence"
    else:
        def diverged_replay(*_args: object, **_kwargs: object) -> Mapping[str, object]:
            raise GoldenReplayMismatchError("synthetic replay state mismatch")

        monkeypatch.setattr(certification, "replay_golden_v3", diverged_replay)
        expected = r"replay differential diverged"

    with pytest.raises(GoldenCertificationError, match=expected) as raised:
        _certify(source, candidate, run_id)
    if stage == "replay":
        assert isinstance(raised.value, GoldenReplayDivergenceError)

    assert not (candidate / "COMPLETE").exists()
    assert not (candidate / "manifests" / "certification-manifest.json").exists()
    assert not (candidate / "pin" / "certification.pin.json").exists()


def test_exact_status_with_truncated_replay_result_is_integrity_blocked(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "truncated-replay.sqlite3"
    run_id = _build_source(source, include_unlinked_alert=False)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)

    def truncated_replay(*_args: object, **_kwargs: object) -> Mapping[str, object]:
        return {
            "status": "REPLAY_DIFFERENTIAL_EXACT",
            "target_path": str(candidate / "results" / "dual-extraction.json"),
        }

    monkeypatch.setattr(certification, "replay_golden_v3", truncated_replay)
    with pytest.raises(
        GoldenCertificationError,
        match=r"replay result schema",
    ) as raised:
        _certify(source, candidate, run_id)

    assert not isinstance(raised.value, GoldenReplayDivergenceError)
    assert not (candidate / "COMPLETE").exists()


def test_structural_replay_error_is_integrity_blocked_not_diverged(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "structural-replay-error.sqlite3"
    run_id = _build_source(source, include_unlinked_alert=False)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)

    def structural_error(*_args: object, **_kwargs: object) -> Mapping[str, object]:
        raise GoldenReplayError("synthetic unsafe replay artifact path")

    monkeypatch.setattr(certification, "replay_golden_v3", structural_error)
    with pytest.raises(GoldenCertificationError, match="unsafe replay artifact") as raised:
        _certify(source, candidate, run_id)

    assert not isinstance(raised.value, GoldenReplayDivergenceError)
    assert not (candidate / "COMPLETE").exists()


def test_exact_replay_with_mutated_target_and_rebound_file_hash_is_integrity_blocked(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "mutated-replay-target.sqlite3"
    run_id = _build_source(source, include_unlinked_alert=False)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
    real_replay = certification.replay_golden_v3

    def mutated_replay(*args: object, **kwargs: object) -> Mapping[str, object]:
        result = dict(real_replay(*args, **kwargs))
        target = Path(cast(str, result["target_path"]))
        with sqlite3.connect(target) as connection:
            connection.execute(
                "UPDATE paper_runs SET event_head_hash=? WHERE run_id=?",
                ("f" * 64, run_id),
            )
        result["target_bytes"] = target.stat().st_size
        result["target_sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
        return result

    monkeypatch.setattr(certification, "replay_golden_v3", mutated_replay)
    with pytest.raises(
        GoldenCertificationError,
        match=r"integrity|head|stream",
    ):
        _certify(source, candidate, run_id)

    assert not (candidate / "COMPLETE").exists()
