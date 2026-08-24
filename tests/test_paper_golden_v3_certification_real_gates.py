from __future__ import annotations

import hashlib
import json
import stat
from collections.abc import Callable
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

from pytest import MonkeyPatch
from test_paper_multistrategy import (
    _BTC,
    _START,
    _market,
    _portfolio_config,
    _strategy_config,
    _strategy_decision,
)

import hyperlab.paper.golden_v3_certification as certification
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.golden_v3_certification import (
    certify_golden_v3,
    verify_golden_v3_certification,
)
from hyperlab.paper.models import DecisionAction, OrderSide, PaperEventType
from hyperlab.paper.store import PaperStore


def _build_full_coverage_source(database: Path) -> tuple[str, int, str]:
    phase05 = _strategy_config("phase05_cash_and_carry", instrument=_BTC)
    phase08 = _strategy_config("phase08_robust_pairs", instrument=_BTC)
    config = _portfolio_config((phase08, phase05))
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()

    observed = _market("golden-real-gates-observed", _START + timedelta(seconds=1))
    engine.process_market(observed)
    markets = {_BTC: observed}
    engine.submit_decision(
        _strategy_decision(
            config,
            phase05,
            markets,
            action=DecisionAction.ENTRY,
            order_specs=((_BTC, OrderSide.BUY, "0.10"),),
            decided_at=observed.received_at,
            ordinal=0,
        ),
        markets,
    )
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

    filled_at = _START + timedelta(seconds=2)
    engine.process_market(_market("golden-real-gates-fill", filled_at))
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
        position_quantity=Decimal("0.30"),
        mark_source="PUBLIC_SETTLEMENT_MARK",
        source_observation_id="golden-v3-real-gates-funding",
        received_at=_START + timedelta(seconds=3),
        processed_at=_START + timedelta(seconds=3),
    )
    engine.process_timer(as_of=_START + timedelta(seconds=4))
    reconciled = engine.reconcile(as_of=_START + timedelta(seconds=5)).projection
    assert reconciled.reconciled is True

    event_types = {stored.event.event_type for stored in store.iter_events(config.run_id)}
    assert PaperEventType.ORDER_FILLED in event_types
    assert PaperEventType.RECONCILIATION_SUCCEEDED in event_types
    assert store.inspect_integrity_readonly(config.run_id).ok is True
    store.close()
    return (
        config.run_id,
        database.stat().st_size,
        hashlib.sha256(database.read_bytes()).hexdigest(),
    )


def test_certification_succeeds_through_the_real_coverage_gate(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id, expected_size, expected_sha256 = _build_full_coverage_source(source)
    candidate = tmp_path / "candidate"

    # Unit-test sandboxes cannot promise 30 GiB. Only the environmental disk
    # preflight is neutralized; production census/gap logic remains untouched.
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
    original_verify = certification.verify_golden_v3
    exhaustive_verifications = 0

    def counted_verify(*args: object, **kwargs: object) -> object:
        nonlocal exhaustive_verifications
        exhaustive_verifications += 1
        verify = cast(Callable[..., object], original_verify)
        return verify(*args, **kwargs)

    monkeypatch.setattr(certification, "verify_golden_v3", counted_verify)
    source.chmod(stat.S_IREAD)
    try:
        result = certify_golden_v3(
            source,
            candidate,
            run_id,
            sentinel_path=tmp_path / "forbidden-original.sqlite3",
            expected_source_size=expected_size,
            expected_source_sha256=expected_sha256,
            shard_rows=1_000,
            shard_bytes=1_000_000,
        )
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)

    assert result.status == "GOLDEN_V3_CERTIFIED"
    assert exhaustive_verifications == 4
    verified = verify_golden_v3_certification(candidate)
    assert exhaustive_verifications == 6
    manifest = cast(
        dict[str, object],
        json.loads(result.manifest_path.read_text(encoding="utf-8")),
    )
    census = cast(dict[str, object], manifest["census"])
    assert census["coverage_gaps"] == ["REPLAY_NOT_PERFORMED"]
    assert certification._blocking_census_gaps(census) == []
    assert manifest["coverage_gaps"] == []
    assert verified.certification_root_hash == result.certification_root_hash
