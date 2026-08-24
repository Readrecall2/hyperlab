from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest
from test_paper_engine_phase12 import _START, _config, _decision, _market
from test_paper_golden_v3 import _build_source, _export

from hyperlab.backtest.protocol import canonical_sha256
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.golden_v3 import GOLDEN_STREAM_NAMES
from hyperlab.paper.golden_v3_replay import (
    GoldenReplayError,
    _CanonicalInput,
    _dispatch_input,
    _LegacyObservationState,
    replay_golden_v3,
)
from hyperlab.paper.models import DecisionAction, OrderSide


def _utc(offset: int) -> str:
    return (_START + timedelta(seconds=offset)).isoformat()


_DISPATCH_MARKET = _market("golden-dispatch", _START + timedelta(seconds=1))
_DISPATCH_CONFIG = _config()
_DISPATCH_DECISION = _decision(
    _DISPATCH_CONFIG,
    _DISPATCH_MARKET,
    action=DecisionAction.ENTRY,
    side=OrderSide.BUY,
)


_SUPPORTED_DISPATCH_CASES: tuple[tuple[str, str, Mapping[str, object]], ...] = (
    (
        "RUNTIME_SESSION_STARTED",
        "start_runtime_session",
        {
            "input_type": "RUNTIME_SESSION_STARTED",
            "started_at": _utc(1),
            "session_id": "golden-session",
            "generation": 1,
            "replaces_unclosed_session_id": None,
        },
    ),
    (
        "RUNTIME_SESSION_STOPPED",
        "stop_runtime_session",
        {
            "input_type": "RUNTIME_SESSION_STOPPED",
            "stopped_at": _utc(2),
            "session_id": "golden-session",
            "generation": 1,
            "reason": "SYNTHETIC_TEST",
        },
    ),
    (
        "PUBLIC_MARKET_EVENT",
        "process_market",
        {
            "input_type": "PUBLIC_MARKET_EVENT",
            "market": _DISPATCH_MARKET.to_dict(),
            "processed_at": _utc(1),
            "execution_policy": "EXECUTE",
            "cash_math_version": 2,
        },
    ),
    (
        "STRATEGY_DECISION",
        "submit_decision",
        {
            "input_type": "STRATEGY_DECISION",
            "decision": _DISPATCH_DECISION.to_dict(),
            "markets": [_DISPATCH_MARKET.to_dict()],
            "processed_at": _utc(1),
        },
    ),
    (
        "CANCEL_REQUEST",
        "request_cancel",
        {
            "input_type": "CANCEL_REQUEST",
            "order_id": "golden-order",
            "requested_at": _utc(2),
        },
    ),
    (
        "PUBLIC_FUNDING_SETTLEMENT",
        "post_funding",
        {
            "input_type": "PUBLIC_FUNDING_SETTLEMENT",
            "instrument": _DISPATCH_MARKET.instrument,
            "amount": "0",
            "occurred_at": _utc(2),
            "source_event_id": "f" * 64,
            "processed_at": _utc(2),
            "cash_math_version": 2,
        },
    ),
    ("TIMER", "process_timer", {"input_type": "TIMER", "as_of": _utc(3)}),
    ("RECONCILE", "_reconcile", {"input_type": "RECONCILE", "as_of": _utc(4)}),
    (
        "STRESS_RESULT",
        "record_stress_result",
        {
            "input_type": "STRESS_RESULT",
            "artifact_hash": "a" * 64,
            "stressed_net_pnl": "-1.25",
            "evaluated_at": _utc(5),
        },
    ),
    (
        "RESILIENCE_EXERCISE",
        "record_resilience_exercise",
        {
            "input_type": "RESILIENCE_EXERCISE",
            "exercise": "SYNTHETIC_TEST",
            "artifact_hash": "b" * 64,
            "exercised_at": _utc(6),
        },
    ),
    (
        "OBSERVATION_COVERAGE",
        "record_observation_coverage",
        {
            "input_type": "OBSERVATION_COVERAGE",
            "artifact_hash": "c" * 64,
            "window_start": _utc(1),
            "window_end": _utc(6),
            "continuous": True,
            "recorded_at": _utc(7),
        },
    ),
    (
        "OPERATOR_PAUSE",
        "pause",
        {
            "input_type": "OPERATOR_PAUSE",
            "as_of": _utc(8),
            "reason": "SYNTHETIC_TEST",
            "operator_artifact_hash": "d" * 64,
        },
    ),
    (
        "PUBLIC_SOURCE_FAILURE",
        "pause",
        {
            "input_type": "PUBLIC_SOURCE_FAILURE",
            "as_of": _utc(8),
            "reason": "SYNTHETIC_TEST",
            "operator_artifact_hash": "e" * 64,
        },
    ),
    (
        "PAPER_RUNTIME_FAILURE",
        "pause",
        {
            "input_type": "PAPER_RUNTIME_FAILURE",
            "as_of": _utc(8),
            "reason": "SYNTHETIC_TEST",
            "operator_artifact_hash": "f" * 64,
        },
    ),
    (
        "STRATEGY_LOCAL_FAILURE",
        "record_strategy_failure",
        {
            "input_type": "STRATEGY_LOCAL_FAILURE",
            "strategy_id": "synthetic-strategy",
            "as_of": _utc(9),
            "phase": "DECIDE",
            "error_type": "SyntheticError",
            "market_event_ids": [_DISPATCH_MARKET.event_id],
        },
    ),
    (
        "PAPER_KILL",
        "kill",
        {
            "input_type": "PAPER_KILL",
            "as_of": _utc(10),
            "reason": "SYNTHETIC_TEST",
            "operator_artifact_hash": "1" * 64,
        },
    ),
    (
        "RESUME_AFTER_REVIEW",
        "resume_from_pause",
        {
            "input_type": "RESUME_AFTER_REVIEW",
            "as_of": _utc(11),
            "review_artifact_hash": "2" * 64,
            "reviewed_critical_incident_count": 0,
            "reviewed_last_critical_incident_at": None,
            "recovery_mode": "FLAT_CONTINUE",
        },
    ),
)


def _record(payload: Mapping[str, object]) -> _CanonicalInput:
    material = dict(payload)
    return _CanonicalInput(
        run_id=_DISPATCH_CONFIG.run_id,
        input_id=canonical_sha256(material),
        payload=material,
        payload_hash=canonical_sha256(material),
        first_event_sequence=1,
        last_event_sequence=1,
        commit_sequence=2,
        commit_hash="3" * 64,
    )


@pytest.mark.parametrize(
    ("input_type", "expected_method", "payload"),
    _SUPPORTED_DISPATCH_CASES,
    ids=[case[0] for case in _SUPPORTED_DISPATCH_CASES],
)
def test_every_supported_post_start_input_reaches_its_replay_dispatch_branch(
    input_type: str,
    expected_method: str,
    payload: Mapping[str, object],
) -> None:
    replay_engine = Mock(spec=PaperEngine)
    replay_engine.config = SimpleNamespace(schema_version=2)
    expected_result = object()
    cast(Mock, getattr(replay_engine, expected_method)).return_value = expected_result
    replay_engine._verified_historical_replay_prefix.return_value = object()

    result = _dispatch_input(
        replay_engine,
        _record(payload),
        legacy=_LegacyObservationState(events=iter(())),
    )

    assert payload["input_type"] == input_type
    assert result is expected_result
    cast(Mock, getattr(replay_engine, expected_method)).assert_called_once()


def test_dispatch_rejects_a_second_run_start() -> None:
    replay_engine = Mock(spec=PaperEngine)
    replay_engine.config = SimpleNamespace(schema_version=2)
    with pytest.raises(GoldenReplayError, match="RUN_START"):
        _dispatch_input(
            replay_engine,
            _record({"input_type": "RUN_START"}),
            legacy=_LegacyObservationState(events=iter(())),
        )


def test_end_to_end_replay_compares_every_manifest_row_in_all_thirteen_streams(
    tmp_path: Path,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source, include_unlinked_alert=False)
    export = tmp_path / "golden"
    scratch = tmp_path / "scratch"
    _export(source, export, run_id)

    result = replay_golden_v3(export, scratch)
    manifest = json.loads((export / "manifest.json").read_text(encoding="utf-8"))
    differential = cast(dict[str, object], result["differential"])
    compared = cast(dict[str, dict[str, object]], differential["streams"])
    manifest_streams = cast(dict[str, dict[str, object]], manifest["streams"])

    assert len(GOLDEN_STREAM_NAMES) == 13
    assert set(compared) == set(GOLDEN_STREAM_NAMES) == set(manifest_streams)
    assert {
        name: int(compared[name]["rows_compared"])
        for name in GOLDEN_STREAM_NAMES
    } == {
        name: int(manifest_streams[name]["row_count"])
        for name in GOLDEN_STREAM_NAMES
    }
    assert int(differential["rows_compared"]) == sum(
        int(manifest_streams[name]["row_count"])
        for name in GOLDEN_STREAM_NAMES
    )
