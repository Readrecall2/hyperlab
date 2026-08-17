from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from hyperlab.dashboard.app import create_app
from hyperlab.paper import (
    DecisionAction,
    DecisionIntent,
    MarketEvent,
    OrderIntent,
    OrderSide,
    PaperEngine,
    PaperExecutionConfig,
    PaperOrderType,
    PaperRiskLimits,
    PaperRunConfig,
    PaperStore,
    TimeInForce,
    deterministic_id,
)
from hyperlab.paper.reporting import (
    PaperReportHeadChangedError,
    build_paper_report,
)

START = datetime(2026, 8, 17, tzinfo=UTC)
INSTRUMENT = "HYPERLIQUID:BTC:perp"


def _market(label: str, at: datetime, bid: str = "100", ask: str = "101") -> MarketEvent:
    return MarketEvent.create(
        received_at=at,
        instrument=INSTRUMENT,
        bid_price=Decimal(bid),
        ask_price=Decimal(ask),
        bid_depth=Decimal("100"),
        ask_depth=Decimal("100"),
        source_sequence=int(deterministic_id("report-market", label)[:8], 16),
    )


def _decision(
    config: PaperRunConfig,
    market: MarketEvent,
    action: DecisionAction,
    side: OrderSide,
) -> DecisionIntent:
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        market_event_id=market.event_id,
        action=action,
        ordinal=0,
    )
    order = OrderIntent.create(
        decision_id=decision_id,
        run_id=config.run_id,
        instrument=INSTRUMENT,
        side=side,
        quantity=Decimal("1"),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=market.received_at,
        ordinal=0,
        reduce_only=action is DecisionAction.EXIT,
    )
    return DecisionIntent(
        decision_id=decision_id,
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        action=action,
        decided_at=market.received_at,
        received_at=market.received_at,
        market_event_id=market.event_id,
        observed_event_ids=(market.event_id,),
        orders=(order,),
    )


def _completed_run(database: Path) -> tuple[PaperRunConfig, PaperStore]:
    config = PaperRunConfig(
        strategy_name="paper_reporting_fixture",
        strategy_hash="a" * 64,
        parameters={"fixture": "SYNTHETIC_TEST_ONLY"},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(
            maker_fee_bps=Decimal("1"),
            taker_fee_bps=Decimal("2"),
            calibration_status="SYNTHETIC",
            source="synthetic-report-fixture",
        ),
        risk=PaperRiskLimits(),
        seed=17,
        initial_cash=Decimal("100000"),
        validation_started_at=START,
        run_kind="TECHNICAL",
        data_calibration_status="SYNTHETIC",
        data_source="synthetic-report-fixture",
        required_instruments=(INSTRUMENT,),
    )
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    entry = _market("entry", START + timedelta(seconds=1))
    engine.submit_decision(_decision(config, entry, DecisionAction.ENTRY, OrderSide.BUY), entry)
    engine.process_market(_market("entry-fill", START + timedelta(seconds=2)))
    engine.post_funding(
        instrument=INSTRUMENT,
        amount=Decimal("0.5"),
        occurred_at=START + timedelta(seconds=3),
        source_event_id=deterministic_id("report-funding", "day-one"),
    )
    day_two = START + timedelta(days=1, seconds=1)
    exit_market = _market("exit", day_two, "102", "103")
    engine.submit_decision(
        _decision(config, exit_market, DecisionAction.EXIT, OrderSide.SELL),
        exit_market,
    )
    engine.process_market(_market("exit-fill", day_two + timedelta(seconds=1), "102", "103"))
    engine.reconcile(as_of=day_two + timedelta(seconds=2))
    return config, store


def test_report_exposes_exact_bounded_technical_metrics(tmp_path: Path) -> None:
    config, store = _completed_run(tmp_path / "paper.sqlite3")
    report = build_paper_report(store, config.run_id, timeline_limit=500)

    assert report["orders_enabled"] is False
    assert report["integrity"] == "HEAD_ANCHORS_VERIFIED_READONLY"
    assert report["integrity_scope"] == {
        "contract": "CURRENT_AND_APPEND_HEAD_V1",
        "head_read_attempt_limit": 2,
        "full_history_verified": False,
        "full_replay_verified": False,
        "same_head_assembly": True,
        "history_cost": "BOUNDED_OUTPUT_AND_CLIENT_MEMORY_SQL_WORK_SCALES_WITH_HISTORY",
        "stopped_runtime_full_verification": ["paper replay", "paper reconcile"],
        "stopped_runtime_required": True,
        "verified": [
            "required_schema_objects",
            "current_run_config",
            "current_event_head",
            "current_projection_and_history_head",
            "genesis_or_latest_commit_and_components",
            "immediate_predecessor_anchors",
            "same_head_before_after",
        ],
    }

    assert report["config"] == config.to_dict()
    assert report["classification"]["paper_classification"] == "PAPER_TECHNICAL"
    assert report["classification"]["profitability_evidence"] is False
    assert report["account"]["funding_net"] == "0.5"
    assert Decimal(report["account"]["fees"]) > 0
    assert report["account"]["positions"] == []
    assert report["runtime"]["source"]["event_count"] == 2
    assert report["runtime"]["source"]["status"] == "OBSERVED_AT_DURABLE_HEAD"
    assert report["runtime"]["source"]["current_wall_clock_evaluated"] is False
    assert report["runtime"]["source"]["freshness_scope"] == "DURABLE_HEAD_NOT_WALL_CLOCK"
    daily = report["daily"]["series"]
    assert [row["date"] for row in daily] == ["2026-08-17", "2026-08-18"]
    assert daily[0]["daily_funding"] == "0.5"
    fills = [item for item in report["timeline"]["items"] if item["event_type"] == "ORDER_FILLED"]
    assert len(fills) == 2
    assert all(fill["fee"] is not None for fill in fills)
    assert all(fill["slippage_bps"] is not None for fill in fills)
    assert all(fill["market_context"][0]["spread"] == "1" for fill in fills)

    first = build_paper_report(store, config.run_id, timeline_limit=2)
    assert first["timeline"]["has_more"] is True
    cursor = first["timeline"]["next_after_sequence"]
    second = build_paper_report(store, config.run_id, after_sequence=cursor, timeline_limit=2)
    assert second["timeline"]["items"][0]["sequence"] > cursor
    with pytest.raises(ValueError, match="timeline_limit"):
        build_paper_report(store, config.run_id, timeline_limit=501)


def test_report_exposes_bounded_runtime_session_timeline_and_incident(tmp_path: Path) -> None:
    config, store = _completed_run(tmp_path / "paper.sqlite3")
    engine = PaperEngine(store, config)
    session_id = "c" * 64
    engine.start_runtime_session(
        as_of=START + timedelta(days=1, seconds=4),
        session_id=session_id,
        generation=1,
    )
    engine.stop_runtime_session(
        as_of=START + timedelta(days=1, seconds=5),
        session_id=session_id,
        generation=1,
        reason="COOPERATIVE_STOP",
    )
    engine.pause(
        as_of=START + timedelta(days=1, seconds=6),
        reason="terminal paper runtime failure: TEST_PHASE: RuntimeError",
        operator_artifact_hash="d" * 64,
        origin="PAPER_RUNTIME_FAILURE",
    )

    report = build_paper_report(store, config.run_id, timeline_limit=500)
    session = report["runtime"]["session"]

    assert session["active"] is False
    assert session["unclosed"] is False
    assert session["generation"] == 1
    assert session["session_id"] == session_id
    assert session["started_at"] == "2026-08-18T00:00:04.000000Z"
    assert session["stopped_at"] == "2026-08-18T00:00:05.000000Z"
    assert [incident["code"] for incident in session["recent_incidents"]] == [
        "PAPER_RUNTIME_FAILURE"
    ]
    session_items = [
        item
        for item in report["timeline"]["items"]
        if item["event_type"] in {"RUNTIME_SESSION_STARTED", "RUNTIME_SESSION_STOPPED"}
    ]
    assert [item["input_type"] for item in session_items] == [
        "RUNTIME_SESSION_STARTED",
        "RUNTIME_SESSION_STOPPED",
    ]
    assert {"pid", "process_id", "database_path", "lock_path"}.isdisjoint(session)


def test_source_health_ages_fail_closed_at_the_durable_head(tmp_path: Path) -> None:
    config, store = _completed_run(tmp_path / "paper.sqlite3")
    engine = PaperEngine(store, config)
    timer_at = START + timedelta(days=1, seconds=40)
    engine.process_timer(as_of=timer_at)

    source = build_paper_report(store, config.run_id)["runtime"]["source"]

    assert source["status"] == "STALE_AT_DURABLE_HEAD_FAIL_CLOSED"
    assert float(source["freshness_age_seconds"]) > config.risk.stale_after_seconds
    assert source["current_wall_clock_evaluated"] is False


def test_report_retries_one_mid_read_commit_and_returns_one_coherent_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, store = _completed_run(tmp_path / "paper.sqlite3")
    writer = PaperEngine(store, config)
    writer.start()
    original_recent_alerts = store.get_recent_alerts
    calls = 0

    def race_once(
        run_id: str,
        *,
        limit: int,
    ):  # type: ignore[no-untyped-def]
        nonlocal calls
        alerts = original_recent_alerts(run_id, limit=limit)
        calls += 1
        if calls == 1:
            writer.reconcile(as_of=START + timedelta(days=1, seconds=4))
        return alerts

    monkeypatch.setattr(store, "get_recent_alerts", race_once)

    report = build_paper_report(store, config.run_id, timeline_limit=10)

    assert calls == 2
    assert report["identity"]["commit_head_hash"] == (store.get_run(config.run_id).commit_head_hash)
    assert report["integrity_scope"]["same_head_assembly"] is True
    assert report["runtime"]["state"] == "FLAT"


def test_report_fails_explicitly_when_both_head_attempts_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, store = _completed_run(tmp_path / "paper.sqlite3")
    writer = PaperEngine(store, config)
    writer.start()
    original_recent_alerts = store.get_recent_alerts
    calls = 0

    def race_every_attempt(
        run_id: str,
        *,
        limit: int,
    ):  # type: ignore[no-untyped-def]
        nonlocal calls
        alerts = original_recent_alerts(run_id, limit=limit)
        calls += 1
        writer.reconcile(as_of=START + timedelta(days=1, seconds=4 + calls))
        return alerts

    monkeypatch.setattr(store, "get_recent_alerts", race_every_attempt)

    with pytest.raises(
        PaperReportHeadChangedError,
        match=r"retry required.*durable head changed",
    ):
        build_paper_report(store, config.run_id, timeline_limit=10)

    assert calls == 2


def test_report_never_invokes_full_journal_integrity_collector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, store = _completed_run(tmp_path / "paper.sqlite3")

    def forbid_full_history(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("interactive reporting must not collect full journal history")

    monkeypatch.setattr(store, "inspect_integrity_readonly", forbid_full_history)
    monkeypatch.setattr(store, "_collect_integrity_issues", forbid_full_history)

    report = build_paper_report(store, config.run_id, timeline_limit=1, day_limit=1)

    assert report["integrity"] == "HEAD_ANCHORS_VERIFIED_READONLY"
    assert report["integrity_scope"]["full_history_verified"] is False
    assert report["timeline"]["returned"] <= 1
    assert report["daily"]["returned"] <= 1


def test_dashboard_report_is_get_only_and_does_not_mutate_sqlite(tmp_path: Path) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    database = paper_dir / "paper.sqlite3"
    config, _ = _completed_run(database)
    original = database.read_bytes()
    client = TestClient(create_app(data_dir=tmp_path))
    path = f"/api/paper/{config.run_id}/report"

    response = client.get(path, params={"timeline_limit": 3, "day_limit": 2})

    assert response.status_code == 200
    assert response.json()["identity"]["run_id"] == config.run_id
    assert client.post(path).status_code == 405
    assert client.get(path, params={"timeline_limit": 501}).status_code == 422
    assert client.get(f"/api/paper/{'f' * 64}/report").status_code == 404
    assert database.read_bytes() == original


def test_dashboard_report_labels_sustained_head_races_as_retry_not_corruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    database = paper_dir / "paper.sqlite3"
    config, writer_store = _completed_run(database)
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
        writer.reconcile(as_of=START + timedelta(days=1, seconds=4 + calls))
        return alerts

    monkeypatch.setattr(PaperStore, "get_recent_alerts", race_every_attempt)

    response = TestClient(create_app(data_dir=tmp_path)).get(f"/api/paper/{config.run_id}/report")
    writer_store.close()

    assert response.status_code == 409
    assert response.json() == {
        "integrity": "HEAD_CHANGED_RETRY",
        "orders_enabled": False,
        "retryable": True,
        "run_id": config.run_id,
        "status": "HEAD_CHANGED_RETRY",
    }
    assert calls == 2


def test_dashboard_report_refuses_corrupt_store_without_latching(tmp_path: Path) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    database = paper_dir / "paper.sqlite3"
    config, _ = _completed_run(database)
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER paper_events_no_update")
        connection.execute(
            "UPDATE paper_events SET payload_json='{}' WHERE run_id=? AND sequence=1",
            (config.run_id,),
        )
    damaged = database.read_bytes()

    response = TestClient(create_app(data_dir=tmp_path)).get(f"/api/paper/{config.run_id}/report")

    assert response.status_code == 503
    assert response.json() == {
        "integrity": "FAILED_READONLY",
        "orders_enabled": False,
        "run_id": config.run_id,
        "status": "MANUAL_REVIEW",
    }
    assert database.read_bytes() == damaged
