from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.gate import PaperGateEvidence, evaluate_paper_gate
from hyperlab.paper.models import (
    MarketEvent,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    StoredPaperEvent,
)
from hyperlab.paper.store import ConcurrentWriteError, PaperStore


def _demo_config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="phase12_gate_snapshot_fixture",
        strategy_hash="a" * 64,
        parameters={"version": 1},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(),
        risk=PaperRiskLimits(),
        seed=12,
        initial_cash=Decimal("100000"),
        validation_started_at=datetime(2026, 8, 15, tzinfo=UTC),
    )


def test_gate_result_metrics_are_bound_to_one_read_only_durable_head(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _demo_config()
    store = PaperStore(database)
    PaperEngine(store, config).start()
    before = database.read_bytes()

    result = evaluate_paper_gate(
        store,
        config.run_id,
        PaperGateEvidence(
            as_of=config.validation_started_at + timedelta(days=1),
        ),
    )

    durable = store.get_run(config.run_id)
    assert result.snapshot.event_sequence == durable.event_sequence
    assert result.snapshot.event_head_hash == durable.event_head_hash
    assert result.snapshot.commit_sequence == durable.commit_sequence
    assert result.snapshot.commit_head_hash == durable.commit_head_hash
    assert result.snapshot.projection_revision == durable.projection_revision
    assert result.snapshot.projection_hash == durable.projection_hash
    assert result.to_dict()["metrics"] == result.snapshot.to_metrics_dict()
    assert result.to_dict()["run_id"] == config.run_id
    assert result.to_dict()["config_hash"] == config.config_hash
    assert result.checks["paper_readiness_receipt_bound"] is False
    assert result.checks["durable_runtime_source_attestation"] is False
    assert result.checks["gate_d_artifact_bytes_verified"] is False
    assert database.read_bytes() == before


def test_gate_reads_a_stable_zero_event_run_without_false_concurrent_write(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _demo_config()
    store = PaperStore(database)
    store.create_run(config)
    before = database.read_bytes()

    result = evaluate_paper_gate(
        store,
        config.run_id,
        PaperGateEvidence(
            as_of=config.validation_started_at + timedelta(days=1),
        ),
    )

    durable = store.get_run(config.run_id)
    projection = store.get_projection(config.run_id)
    assert result.eligible is False
    assert result.snapshot.event_sequence == 0
    assert result.snapshot.event_head_hash == durable.event_head_hash
    assert projection.last_sequence == 0
    assert projection.last_event_hash == durable.event_head_hash
    assert database.read_bytes() == before


def test_gate_fails_closed_when_the_durable_head_changes_mid_evaluation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _demo_config()
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    as_of = config.validation_started_at + timedelta(days=1)
    original_get_events = store.get_events
    mutated = False

    def get_events_then_append(
        run_id: str,
        *,
        after_sequence: int = 0,
        limit: int | None = None,
    ) -> tuple[StoredPaperEvent, ...]:
        nonlocal mutated
        events = original_get_events(
            run_id,
            after_sequence=after_sequence,
            limit=limit,
        )
        if not mutated:
            mutated = True
            engine.process_market(
                MarketEvent.create(
                    received_at=as_of,
                    instrument="HYPERLIQUID:BTC:perp",
                    bid_price=Decimal("100"),
                    ask_price=Decimal("101"),
                    bid_depth=Decimal("10"),
                    ask_depth=Decimal("10"),
                    source_sequence=1,
                )
            )
        return events

    initial_sequence = store.get_run(config.run_id).event_sequence
    monkeypatch.setattr(store, "get_events", get_events_then_append)

    with pytest.raises(ConcurrentWriteError, match="durable head changed"):
        evaluate_paper_gate(
            store,
            config.run_id,
            PaperGateEvidence(as_of=as_of),
        )

    assert store.get_run(config.run_id).event_sequence > initial_sequence
