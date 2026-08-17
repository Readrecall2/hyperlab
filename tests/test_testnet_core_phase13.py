"""Synthetic-only safety coverage for the isolated Phase 13 Testnet core."""

from __future__ import annotations

import json
import os
import pickle
import re
import sqlite3
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

SERVICE_SRC = (
    Path(__file__).resolve().parents[1]
    / "services"
    / "testnet-executor"
    / "src"
)
sys.path.insert(0, str(SERVICE_SRC))

import hyperlab_testnet.store as testnet_store_module  # noqa: E402
from hyperlab_testnet.config import (  # noqa: E402
    TestnetConfig,
    TestnetConfigError,
    TestnetRiskLimits,
)
from hyperlab_testnet.credentials import (  # noqa: E402
    SecretSerializationError,
    TestnetCredentialError,
    load_testnet_credentials,
)
from hyperlab_testnet.models import (  # noqa: E402
    ActionAttemptStatus,
    ActionKind,
    OrderSide,
    OrderStatus,
    RuntimeState,
    TestnetOrder,
    TestnetOrderIntent,
    TimeInForce,
    legal_order_transition,
    require_order_transition,
)
from hyperlab_testnet.risk import (  # noqa: E402
    evaluate_action_rate,
    evaluate_order_risk,
    market_is_fresh,
    reconciliation_is_fresh,
)
from hyperlab_testnet.store import (  # noqa: E402
    AmbiguousActionReplayError,
    IdempotencyConflictError,
    IntegrityError,
    OrderProjectionUpdate,
    ReconciliationActionResolution,
    ReconciliationFill,
    ReconciliationIssue,
    RunConflictError,
    SecretPersistenceError,
    TestnetStore,
    WalletLeaseError,
    deterministic_action_id,
)

from hyperlab.backtest.protocol import canonical_json  # noqa: E402

# These domain class names begin with Testnet; they are not pytest tests.
TestnetConfig.__test__ = False  # type: ignore[attr-defined]
TestnetConfigError.__test__ = False  # type: ignore[attr-defined]
TestnetRiskLimits.__test__ = False  # type: ignore[attr-defined]
TestnetCredentialError.__test__ = False  # type: ignore[attr-defined]
TestnetOrder.__test__ = False  # type: ignore[attr-defined]
TestnetOrderIntent.__test__ = False  # type: ignore[attr-defined]
TestnetStore.__test__ = False  # type: ignore[attr-defined]

SYNTHETIC_ONLY = True
NOW = datetime(2026, 8, 17, 12, 0, tzinfo=UTC)
ACCOUNT = "0x" + "1" * 40
API_WALLET = "0x" + "2" * 40


def _config(
    *,
    risk_limits: TestnetRiskLimits | None = None,
    api_wallet_address: str = API_WALLET,
) -> TestnetConfig:
    return TestnetConfig(
        candidate_id="phase13-synthetic",
        account_address=ACCOUNT,
        api_wallet_address=api_wallet_address,
        strategy_name="synthetic-strategy",
        strategy_hash="3" * 64,
        build_hash="4" * 64,
        source_identity="synthetic-fixture",
        source_hash="5" * 64,
        risk_limits=risk_limits or TestnetRiskLimits(),
    )


def _intent(
    config: TestnetConfig,
    *,
    decision: str = "6",
    side: OrderSide = OrderSide.BUY,
    quantity: str = "1",
    price: str = "10",
    instrument: str = "HL:BTC:perp",
    ordinal: int = 0,
) -> TestnetOrderIntent:
    return TestnetOrderIntent.create(
        run_id=config.run_id,
        decision_id=decision * 64,
        instrument=instrument,
        side=side,
        quantity=Decimal(quantity),
        limit_price=Decimal(price),
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        created_at=NOW,
        ordinal=ordinal,
    )


def _apply_empty_reconciliation(
    store: TestnetStore,
    config: TestnetConfig,
    *,
    at: datetime = NOW,
    snapshot_id: str | None = None,
) -> None:
    store.apply_reconciliation(
        config.run_id,
        positions={},
        spot_balances={},
        equity=Decimal("1000"),
        withdrawable=Decimal("900"),
        open_orders=(),
        order_updates=(),
        fills=(),
        reconciled_at=at,
        source_cursor="synthetic-cursor",
        snapshot_id=snapshot_id,
    )


def _admitted_store(
    tmp_path: Path,
    *,
    name: str = "testnet.sqlite3",
    owner_id: str = "owner-a",
) -> tuple[TestnetStore, TestnetConfig]:
    config = _config()
    store = TestnetStore(
        tmp_path / name,
        lease_root=tmp_path / "wallet-leases",
        owner_id=owner_id,
    )
    store.create_run(config, created_at=NOW)
    store.acquire_wallet_lease(config.run_id, acquired_at=NOW)
    store.set_runtime_state(config.run_id, RuntimeState.STARTING, at=NOW)
    _apply_empty_reconciliation(store, config)
    store.set_runtime_state(config.run_id, RuntimeState.RUNNING, at=NOW)
    return store, config


def test_config_is_exact_canonical_testnet_and_allows_one_terminal_eol() -> None:
    config = _config()
    canonical = config.canonical_json_bytes()
    assert TestnetConfig.from_json_bytes(canonical) == config
    assert TestnetConfig.from_json_bytes(canonical + b"\n") == config
    assert TestnetConfig.from_json_bytes(canonical + b"\r\n") == config
    for invalid in (
        canonical + b"\n\n",
        b" " + canonical,
        canonical + b" ",
        json.dumps(config.to_dict(), indent=2).encode(),
    ):
        with pytest.raises(TestnetConfigError):
            TestnetConfig.from_json_bytes(invalid)

    payload = dict(config.to_dict())
    payload["http_endpoint"] = "https://api.hyperliquid.xyz"
    with pytest.raises(TestnetConfigError, match="http_endpoint"):
        TestnetConfig.from_json_bytes(canonical_json(payload).encode())
    payload = dict(config.to_dict())
    payload["unknown"] = "ambiguous"
    with pytest.raises(TestnetConfigError, match="unexpected"):
        TestnetConfig.from_json_bytes(canonical_json(payload).encode())

    subject = config.to_readiness_subject()
    assert set(subject) == {
        "build_hash",
        "candidate_id",
        "config_hash",
        "risk_limits_hash",
        "source_identity",
        "strategy_hash",
    }


def test_config_requires_position_quantity_limit_and_consistent_caps() -> None:
    config = _config()
    payload = dict(config.to_dict())
    risk = dict(config.risk_limits.to_dict())
    risk.pop("max_position_quantity")
    payload["risk_limits"] = risk
    with pytest.raises(TestnetConfigError, match="max_position_quantity"):
        TestnetConfig.from_json_bytes(canonical_json(payload).encode())
    with pytest.raises(TestnetConfigError, match="max_order_quantity"):
        TestnetRiskLimits(
            max_position_quantity=Decimal("1"),
            max_order_quantity=Decimal("2"),
        )

    ceilings: dict[str, object] = {
        "max_gross_notional": Decimal("1000.000001"),
        "max_position_notional": Decimal("500.000001"),
        "max_order_notional": Decimal("100.000001"),
        "max_position_quantity": Decimal("5.000001"),
        "max_order_quantity": Decimal("1.000001"),
        "max_concurrent_orders": 5,
        "submit_requests_per_minute": 13,
        "cancel_requests_per_minute": 25,
        "replace_requests_per_minute": 7,
        "market_stale_after_seconds": 6,
        "reconciliation_stale_after_seconds": 11,
        "deadman_interval_seconds": 31,
    }
    for field, value in ceilings.items():
        with pytest.raises(TestnetConfigError, match="compiled conservative Testnet ceiling"):
            TestnetRiskLimits(**{field: value})

    payload = dict(config.to_dict())
    risk = dict(config.risk_limits.to_dict())
    risk["max_gross_notional"] = "1e1000000000"
    payload["risk_limits"] = risk
    with pytest.raises((TestnetConfigError, ValueError), match="representation bound"):
        TestnetConfig.from_mapping(payload)


def test_credentials_are_dedicated_redacted_nonserializable_and_derived_checked(
    tmp_path: Path,
) -> None:
    config = _config()
    key = "ab" * 32
    environ = {
        "HYPERLAB_TESTNET_PRIVATE_KEY": key,
        "HYPERLAB_TESTNET_ACCOUNT_ADDRESS": ACCOUNT,
        "HYPERLAB_TESTNET_API_WALLET_ADDRESS": API_WALLET,
        "GITHUB_TOKEN": "unrelated-and-ignored",
    }
    credentials = load_testnet_credentials(config, environ=environ)
    assert key not in repr(credentials)
    assert key not in str(credentials.private_key)
    with pytest.raises(SecretSerializationError):
        pickle.dumps(credentials)
    with pytest.raises(SecretSerializationError):
        credentials.to_dict()
    credentials.validate_derived_api_wallet_address(API_WALLET.upper())
    with pytest.raises(TestnetCredentialError, match="does not match"):
        credentials.validate_derived_api_wallet_address("0x" + "7" * 40)

    ambiguous = dict(environ)
    ambiguous["HYPERLIQUID_PRIVATE_KEY"] = "cd" * 32
    with pytest.raises(TestnetCredentialError, match="HYPERLIQUID_PRIVATE_KEY"):
        load_testnet_credentials(config, environ=ambiguous)

    store = TestnetStore(tmp_path / "secret.sqlite3")
    store.create_run(config, created_at=NOW)
    before = store.get_run(config.run_id).audit_count
    with pytest.raises(SecretPersistenceError):
        store.append_audit(
            config.run_id,
            "SYNTHETIC_EVENT",
            {"private_key": key},
            created_at=NOW,
        )
    assert store.get_run(config.run_id).audit_count == before


def test_order_identity_strict_parse_invariants_and_testnet_fsm() -> None:
    config = _config()
    first = _intent(config)
    second = _intent(config)
    assert first == second
    assert re.fullmatch(r"0x[0-9a-f]{32}", first.cloid)
    assert TestnetOrderIntent.from_dict(first.to_dict()) == first

    string_boolean = dict(first.to_dict())
    string_boolean["reduce_only"] = "false"
    with pytest.raises(TypeError, match="boolean"):
        TestnetOrderIntent.from_dict(string_boolean)
    extra = dict(first.to_dict())
    extra["extra"] = "not allowed"
    with pytest.raises(ValueError, match="extra"):
        TestnetOrderIntent.from_dict(extra)

    for bad_instrument in (
        "BINANCE:BTC:perp",
        "HL:BTC:spot",
        "HL::perp",
        "HL:BTC USD:perp",
        "HL:BTC:USD:perp",
        "HL:BTC\n:perp",
    ):
        with pytest.raises(ValueError):
            _intent(config, instrument=bad_instrument)

    with pytest.raises(ValueError, match="FILLED"):
        TestnetOrder(first, OrderStatus.FILLED)
    with pytest.raises(ValueError, match="PARTIALLY_FILLED"):
        TestnetOrder(first, OrderStatus.PARTIALLY_FILLED)
    with pytest.raises(ValueError, match="average_fill_price"):
        TestnetOrder(
            first,
            OrderStatus.PARTIALLY_FILLED,
            filled_quantity=Decimal("0.5"),
        )
    complete = TestnetOrder(
        first,
        OrderStatus.FILLED,
        filled_quantity=Decimal("1"),
        average_fill_price=Decimal("10"),
    )
    assert complete.remaining_quantity == 0
    assert legal_order_transition(OrderStatus.SUBMITTED, OrderStatus.FILLED)
    assert not legal_order_transition(OrderStatus.FILLED, OrderStatus.OPEN)
    with pytest.raises(ValueError, match="illegal"):
        require_order_transition(OrderStatus.FILLED, OrderStatus.OPEN)


def test_risk_uses_fresh_authoritative_marks_and_conservative_prices() -> None:
    limits = TestnetRiskLimits()
    config = _config(risk_limits=limits)
    low_sell = _intent(
        config,
        side=OrderSide.SELL,
        quantity="0.5",
        price="1",
    )
    decision = evaluate_order_risk(
        low_sell,
        now=NOW,
        market_received_at=NOW,
        last_reconciled_at=NOW,
        runtime_state=RuntimeState.RUNNING,
        current_positions={"HL:BTC:perp": Decimal("1")},
        marks={"HL:BTC:perp": Decimal("100")},
        open_orders=(),
        submit_requests_in_last_minute=0,
        limits=limits,
    )
    assert decision.accepted
    assert decision.order_notional == Decimal("50")
    assert decision.projected_position_notional == Decimal("100")
    assert decision.projected_position_quantity == Decimal("1")
    assert decision.projected_gross_notional == Decimal("150")

    absurd_limit = _intent(config, quantity="0.1", price="10000")
    absurd = evaluate_order_risk(
        absurd_limit,
        now=NOW,
        market_received_at=NOW,
        last_reconciled_at=NOW,
        runtime_state=RuntimeState.RUNNING,
        current_positions={"HL:BTC:perp": Decimal("1")},
        marks={"HL:BTC:perp": Decimal("100")},
        open_orders=(),
        submit_requests_in_last_minute=0,
        limits=limits,
    )
    assert absurd.order_notional == Decimal("1000")
    assert absurd.projected_gross_notional == Decimal("1100")

    missing = evaluate_order_risk(
        low_sell,
        now=NOW,
        market_received_at=NOW,
        last_reconciled_at=NOW,
        runtime_state=RuntimeState.RUNNING,
        current_positions={"HL:BTC:perp": Decimal("1")},
        marks={},
        open_orders=(),
        submit_requests_in_last_minute=0,
        limits=limits,
    )
    assert not missing.accepted
    assert any("missing mark" in reason for reason in missing.reasons)
    assert not market_is_fresh(
        now=NOW,
        market_received_at=NOW - timedelta(seconds=6),
        limits=limits,
    )
    assert not reconciliation_is_fresh(
        now=NOW,
        last_reconciled_at=NOW - timedelta(seconds=11),
        limits=limits,
    )
    assert not evaluate_action_rate(
        ActionKind.SUBMIT,
        requests_in_last_minute=limits.submit_requests_per_minute,
        limits=limits,
    ).accepted
    with pytest.raises(ValueError):
        evaluate_action_rate(
            ActionKind.SUBMIT,
            requests_in_last_minute=1.5,  # type: ignore[arg-type]
            limits=limits,
        )


def test_risk_uses_independent_fill_envelopes_and_exact_replace_exclusion() -> None:
    limits = TestnetRiskLimits(
        max_position_quantity=Decimal("1"),
        max_order_quantity=Decimal("1"),
    )
    config = _config(risk_limits=limits)
    unknown_buy = TestnetOrder(
        _intent(
            config,
            decision="7",
            side=OrderSide.BUY,
            quantity="1",
            ordinal=1,
        ),
        OrderStatus.UNKNOWN,
        venue_order_id="unknown-buy",
    )
    cancel_pending_sell = TestnetOrder(
        _intent(
            config,
            decision="8",
            side=OrderSide.SELL,
            quantity="1",
            ordinal=2,
        ),
        OrderStatus.CANCEL_REQUESTED,
        venue_order_id="cancel-pending-sell",
    )
    candidate = _intent(
        config,
        decision="9",
        side=OrderSide.BUY,
        quantity="1",
        ordinal=3,
    )
    independent = evaluate_order_risk(
        candidate,
        now=NOW,
        market_received_at=NOW,
        last_reconciled_at=NOW,
        runtime_state=RuntimeState.RUNNING,
        current_positions={},
        marks={"HL:BTC:perp": Decimal("10")},
        open_orders=(unknown_buy, cancel_pending_sell),
        submit_requests_in_last_minute=0,
        limits=limits,
    )
    assert not independent.accepted
    assert independent.worst_long_quantity == Decimal("2")
    assert independent.worst_short_quantity == Decimal("-1")
    assert independent.projected_position_quantity == Decimal("2")
    assert any("position quantity" in reason for reason in independent.reasons)

    replacement = _intent(
        config,
        decision="a",
        side=OrderSide.BUY,
        quantity="1",
        ordinal=4,
    )
    original = TestnetOrder(
        _intent(
            config,
            decision="b",
            side=OrderSide.BUY,
            quantity="1",
            ordinal=5,
        ),
        OrderStatus.UNKNOWN,
        venue_order_id="replace-original",
    )
    replace_limits = TestnetRiskLimits(
        max_position_quantity=Decimal("1"),
        max_order_quantity=Decimal("1"),
        max_concurrent_orders=1,
    )
    base_arguments = {
        "now": NOW,
        "market_received_at": NOW,
        "last_reconciled_at": NOW,
        "runtime_state": RuntimeState.RUNNING,
        "current_positions": {},
        "marks": {"HL:BTC:perp": Decimal("10")},
        "open_orders": (original,),
        "submit_requests_in_last_minute": 0,
        "limits": replace_limits,
    }
    assert not evaluate_order_risk(replacement, **base_arguments).accepted
    excluded = evaluate_order_risk(
        replacement,
        replaced_order_id=original.intent.order_id,
        **base_arguments,
    )
    assert excluded.accepted
    assert excluded.worst_long_quantity == Decimal("1")
    with pytest.raises(ValueError, match="one exposure-reserving original"):
        evaluate_order_risk(
            replacement,
            replaced_order_id="f" * 64,
            **base_arguments,
        )


def test_store_admission_requires_lease_then_fresh_reconciliation(
    tmp_path: Path,
) -> None:
    config = _config()
    store = TestnetStore(
        tmp_path / "admission.sqlite3",
        lease_root=tmp_path / "control",
        owner_id="admission-owner",
    )
    store.create_run(config, created_at=NOW)
    with pytest.raises(WalletLeaseError):
        store.set_runtime_state(config.run_id, RuntimeState.STARTING, at=NOW)
    store.acquire_wallet_lease(config.run_id, acquired_at=NOW)
    with pytest.raises(RunConflictError, match="illegal"):
        store.set_runtime_state(config.run_id, RuntimeState.RUNNING, at=NOW)
    store.set_runtime_state(config.run_id, RuntimeState.STARTING, at=NOW)
    with pytest.raises(RunConflictError, match="reconciliation"):
        store.set_runtime_state(config.run_id, RuntimeState.RUNNING, at=NOW)
    _apply_empty_reconciliation(store, config)
    admitted = store.set_runtime_state(
        config.run_id,
        RuntimeState.RUNNING,
        at=NOW,
    )
    assert admitted.runtime_state is RuntimeState.RUNNING


def test_reserve_is_nonce_atomic_submitted_and_restart_never_resends(
    tmp_path: Path,
) -> None:
    store, config = _admitted_store(tmp_path)
    intent = _intent(config)
    store.create_order(intent)
    action_id = deterministic_action_id(
        config.run_id,
        ActionKind.SUBMIT,
        intent.order_id,
    )
    reserved = store.reserve_action(
        config.run_id,
        action_id=action_id,
        kind=ActionKind.SUBMIT,
        order_id=intent.order_id,
        payload={"cloid": intent.cloid, "operation": "order"},
        created_at=NOW + timedelta(seconds=1),
        expires_after_delta_ms=5_000,
    )
    assert reserved.status is ActionAttemptStatus.AMBIGUOUS
    assert reserved.payload["expires_after_ms"] == reserved.nonce + 5_000
    assert store.get_order(config.run_id, intent.order_id).status is OrderStatus.SUBMITTED
    with pytest.raises(AmbiguousActionReplayError, match="never resend"):
        store.reserve_action(
            config.run_id,
            action_id=action_id,
            kind=ActionKind.SUBMIT,
            order_id=intent.order_id,
            payload={"cloid": intent.cloid, "operation": "order"},
            created_at=NOW + timedelta(seconds=1),
            expires_after_delta_ms=5_000,
        )
    store.close()

    restarted = TestnetStore(
        tmp_path / "testnet.sqlite3",
        lease_root=tmp_path / "wallet-leases",
        owner_id="owner-after-crash",
    )
    restarted.acquire_wallet_lease(
        config.run_id,
        acquired_at=NOW + timedelta(seconds=2),
    )
    assert restarted.list_ambiguous_actions(config.run_id) == (reserved,)
    with pytest.raises(AmbiguousActionReplayError):
        restarted.reserve_action(
            config.run_id,
            action_id=action_id,
            kind=ActionKind.SUBMIT,
            order_id=intent.order_id,
            payload={"cloid": intent.cloid, "operation": "order"},
            created_at=NOW + timedelta(seconds=2),
            expires_after_delta_ms=5_000,
        )

    next_intent = _intent(config, decision="7", ordinal=1)
    restarted.create_order(next_intent)
    with pytest.raises(RunConflictError, match="after wallet lease"):
        restarted.reserve_action(
            config.run_id,
            action_id=deterministic_action_id(
                config.run_id,
                ActionKind.SUBMIT,
                next_intent.order_id,
            ),
            kind=ActionKind.SUBMIT,
            order_id=next_intent.order_id,
            payload={"cloid": next_intent.cloid},
            created_at=NOW + timedelta(seconds=2),
        )
    restarted.set_runtime_state(
        config.run_id,
        RuntimeState.STARTING,
        at=NOW + timedelta(seconds=2),
    )
    restarted.apply_reconciliation(
        config.run_id,
        positions={},
        spot_balances={},
        equity=Decimal("1000"),
        withdrawable=Decimal("900"),
        open_orders=({"cloid": intent.cloid, "oid": "venue-1"},),
        order_updates=(
            OrderProjectionUpdate(
                intent.order_id,
                OrderStatus.OPEN,
                venue_order_id="venue-1",
                filled_quantity=Decimal(0),
            ),
        ),
        fills=(),
        action_resolutions=(
            ReconciliationActionResolution(
                action_id,
                ActionAttemptStatus.CONFIRMED,
                {"authority": "openOrders", "oid": "venue-1"},
            ),
        ),
        reconciled_at=NOW + timedelta(seconds=3),
        source_cursor="restart-reconciliation",
    )
    restarted.set_runtime_state(
        config.run_id,
        RuntimeState.RUNNING,
        at=NOW + timedelta(seconds=3),
    )
    second = restarted.reserve_action(
        config.run_id,
        action_id=deterministic_action_id(
            config.run_id,
            ActionKind.SUBMIT,
            next_intent.order_id,
        ),
        kind=ActionKind.SUBMIT,
        order_id=next_intent.order_id,
        payload={"cloid": next_intent.cloid},
        created_at=NOW + timedelta(seconds=4),
    )
    assert second.nonce > reserved.nonce


def test_complete_action_and_stable_fill_do_not_double_count(
    tmp_path: Path,
) -> None:
    store, config = _admitted_store(tmp_path)
    intent = _intent(config)
    store.create_order(intent)
    action_id = deterministic_action_id(
        config.run_id,
        ActionKind.SUBMIT,
        intent.order_id,
    )
    store.reserve_action(
        config.run_id,
        action_id=action_id,
        kind=ActionKind.SUBMIT,
        order_id=intent.order_id,
        payload={"cloid": intent.cloid},
        created_at=NOW + timedelta(seconds=1),
    )
    update = OrderProjectionUpdate(
        intent.order_id,
        OrderStatus.FILLED,
        venue_order_id="venue-fill",
        filled_quantity=Decimal("1"),
        average_fill_price=Decimal("10"),
    )
    store.complete_action(
        config.run_id,
        action_id,
        ActionAttemptStatus.CONFIRMED,
        response={"authority": "exchange-response", "oid": "venue-fill"},
        order_updates=(update,),
        resolved_at=NOW + timedelta(seconds=2),
    )
    stable = dict(
        fill_id="fill-1",
        order_id=intent.order_id,
        venue_order_id="venue-fill",
        quantity=Decimal("1"),
        price=Decimal("10"),
        fee=Decimal("0.01"),
        payload={"tid": "synthetic-1", "venue_hash_digest": "8" * 64},
        received_at=NOW + timedelta(seconds=3),
    )
    store.record_fill(config.run_id, **stable)
    audit_count = store.get_run(config.run_id).audit_count
    store.record_fill(config.run_id, **stable)
    assert store.get_run(config.run_id).audit_count == audit_count
    order = store.get_order(config.run_id, intent.order_id)
    assert order.status is OrderStatus.FILLED
    assert order.filled_quantity == Decimal("1")
    assert order.average_fill_price == Decimal("10")
    assert len(store.list_fills(config.run_id)) == 1

    with pytest.raises(IdempotencyConflictError, match="projection"):
        store.complete_action(
            config.run_id,
            action_id,
            ActionAttemptStatus.CONFIRMED,
            response={"authority": "exchange-response", "oid": "venue-fill"},
            order_updates=(
                OrderProjectionUpdate(
                    intent.order_id,
                    OrderStatus.FILLED,
                    venue_order_id="venue-fill",
                    filled_quantity=Decimal("1"),
                    average_fill_price=Decimal("11"),
                ),
            ),
            resolved_at=NOW + timedelta(seconds=4),
        )
    assert store.get_run(config.run_id).runtime_state is RuntimeState.MANUAL_REVIEW


@pytest.mark.parametrize("terminal", [OrderStatus.CANCELLED, OrderStatus.EXPIRED])
def test_late_partial_and_final_fills_preserve_terminal_truth(
    tmp_path: Path,
    terminal: OrderStatus,
) -> None:
    store, config = _admitted_store(
        tmp_path,
        name=f"{terminal.value}.sqlite3",
        owner_id=f"owner-{terminal.value}",
    )
    intent = _intent(config)
    store.create_order(intent)
    store.transition_order(
        config.run_id,
        intent.order_id,
        OrderStatus.OPEN,
        at=NOW + timedelta(seconds=1),
        venue_order_id=f"oid-{terminal.value}",
    )
    store.transition_order(
        config.run_id,
        intent.order_id,
        terminal,
        at=NOW + timedelta(seconds=2),
    )
    for index, quantity in enumerate((Decimal("0.4"), Decimal("0.6")), start=1):
        fill = dict(
            fill_id=f"late-{terminal.value}-{index}",
            order_id=intent.order_id,
            venue_order_id=f"oid-{terminal.value}",
            quantity=quantity,
            price=Decimal("10"),
            fee=Decimal(0),
            payload={"tid": f"{terminal.value}-{index}"},
            received_at=NOW + timedelta(seconds=2 + index),
        )
        store.record_fill(config.run_id, **fill)
        store.record_fill(config.run_id, **fill)
    order = store.get_order(config.run_id, intent.order_id)
    assert order.status is terminal
    assert order.filled_quantity == intent.quantity
    assert len(store.list_fills(config.run_id)) == 2


def test_ambiguous_observation_is_atomic_idempotent_and_divergence_latches(
    tmp_path: Path,
) -> None:
    store, config = _admitted_store(tmp_path)
    intent = _intent(config)
    store.create_order(intent)
    action_id = deterministic_action_id(
        config.run_id,
        ActionKind.SUBMIT,
        intent.order_id,
    )
    store.reserve_action(
        config.run_id,
        action_id=action_id,
        kind=ActionKind.SUBMIT,
        order_id=intent.order_id,
        payload={"cloid": intent.cloid},
        created_at=NOW + timedelta(seconds=1),
    )
    update = OrderProjectionUpdate(
        intent.order_id,
        OrderStatus.UNKNOWN,
        filled_quantity=Decimal(0),
    )
    response = {"transport": "timeout-after-write", "wire_body_retained": False}
    observed = store.observe_ambiguous_action(
        config.run_id,
        action_id,
        response=response,
        order_updates=(update,),
        observed_at=NOW + timedelta(seconds=2),
    )
    assert observed.status is ActionAttemptStatus.AMBIGUOUS
    assert observed.response == response
    assert store.get_order(config.run_id, intent.order_id).status is OrderStatus.UNKNOWN
    count = store.get_run(config.run_id).audit_count
    assert (
        store.observe_ambiguous_action(
            config.run_id,
            action_id,
            response=response,
            order_updates=(update,),
            observed_at=NOW + timedelta(seconds=2),
        )
        == observed
    )
    assert store.get_run(config.run_id).audit_count == count
    with pytest.raises(IdempotencyConflictError, match="divergently"):
        store.observe_ambiguous_action(
            config.run_id,
            action_id,
            response={"transport": "different"},
            order_updates=(update,),
            observed_at=NOW + timedelta(seconds=3),
        )
    run = store.get_run(config.run_id)
    assert run.runtime_state is RuntimeState.MANUAL_REVIEW
    first_reason = run.state_reason
    store.set_runtime_state(
        config.run_id,
        RuntimeState.MANUAL_REVIEW,
        reason="SECOND_FAILURE",
        at=NOW + timedelta(seconds=4),
    )
    assert store.get_run(config.run_id).state_reason == first_reason


def test_reconciliation_replay_is_zero_side_effect_and_pointer_is_exact(
    tmp_path: Path,
) -> None:
    store, config = _admitted_store(tmp_path)
    same_time = NOW + timedelta(seconds=5)
    first = store.apply_reconciliation(
        config.run_id,
        positions={"HL:BTC:perp": Decimal("1")},
        spot_balances={"USDC": Decimal("100")},
        equity=Decimal("1001"),
        withdrawable=Decimal("901"),
        open_orders=(),
        order_updates=(),
        fills=(),
        reconciled_at=same_time,
        source_cursor="same-ms-a",
        snapshot_id="a" * 64,
    )
    second = store.apply_reconciliation(
        config.run_id,
        positions={"HL:BTC:perp": Decimal("2")},
        spot_balances={"USDC": Decimal("101")},
        equity=Decimal("1002"),
        withdrawable=Decimal("902"),
        open_orders=(),
        order_updates=(),
        fills=(),
        reconciled_at=same_time,
        source_cursor="same-ms-b",
        snapshot_id="b" * 64,
    )
    assert first.snapshot_id != second.snapshot_id
    before = store.get_run(config.run_id).audit_count
    replay = store.apply_reconciliation(
        config.run_id,
        positions={"HL:BTC:perp": Decimal("2")},
        spot_balances={"USDC": Decimal("101")},
        equity=Decimal("1002"),
        withdrawable=Decimal("902"),
        open_orders=(),
        order_updates=(),
        fills=(),
        reconciled_at=same_time,
        source_cursor="same-ms-b",
        snapshot_id="b" * 64,
    )
    assert replay == second
    assert store.get_run(config.run_id).audit_count == before
    assert store.latest_reconciled_snapshot(config.run_id) == second
    assert store.get_run(config.run_id).reconciliation_snapshot_id == second.snapshot_id

    unreconciled = store.record_remote_snapshot(
        config.run_id,
        positions={"HL:BTC:perp": Decimal("999")},
        spot_balances={},
        equity=Decimal("999"),
        withdrawable=Decimal("999"),
        open_orders=(),
        received_at=same_time + timedelta(seconds=1),
        source_cursor="read-only-newer",
        snapshot_id="f" * 64,
    )
    assert store.latest_remote_snapshot(config.run_id) == unreconciled
    assert store.latest_reconciled_snapshot(config.run_id) == second


def test_native_replace_releases_reused_oid_before_binding_and_fills(
    tmp_path: Path,
) -> None:
    store, config = _admitted_store(tmp_path)
    original = _intent(config, decision="6", ordinal=0)
    replacement = _intent(config, decision="7", price="11", ordinal=1)
    store.create_order(original)
    store.transition_order(
        config.run_id,
        original.order_id,
        OrderStatus.OPEN,
        at=NOW + timedelta(seconds=1),
        venue_order_id="native-reused-oid",
    )
    store.create_order(replacement)
    action_id = deterministic_action_id(
        config.run_id,
        ActionKind.REPLACE,
        replacement.order_id,
    )
    store.reserve_action(
        config.run_id,
        action_id=action_id,
        kind=ActionKind.REPLACE,
        order_id=replacement.order_id,
        payload={"cloid": replacement.cloid, "operation": "modify"},
        created_at=NOW + timedelta(seconds=2),
    )
    store.apply_reconciliation(
        config.run_id,
        positions={"HL:BTC:perp": Decimal("1")},
        spot_balances={"USDC": Decimal("100")},
        equity=Decimal("1001"),
        withdrawable=Decimal("900"),
        open_orders=(),
        # Reverse facade order deliberately: the store must phase the terminal
        # original before binding the replacement to the same native OID.
        order_updates=(
            OrderProjectionUpdate(
                replacement.order_id,
                OrderStatus.OPEN,
                venue_order_id="native-reused-oid",
                filled_quantity=Decimal(0),
            ),
            OrderProjectionUpdate(
                original.order_id,
                OrderStatus.CANCELLED,
                venue_order_id="native-reused-oid",
                filled_quantity=Decimal(0),
            ),
        ),
        fills=(
            ReconciliationFill(
                fill_id="native-replace-fill",
                order_id=replacement.order_id,
                venue_order_id="native-reused-oid",
                quantity=Decimal("1"),
                price=Decimal("11"),
                fee=Decimal("0.01"),
                payload={"tid": "native-replace-fill"},
                received_at=NOW + timedelta(seconds=3),
            ),
        ),
        action_resolutions=(
            ReconciliationActionResolution(
                action_id,
                ActionAttemptStatus.CONFIRMED,
                {"authority": "userFills", "oid": "native-reused-oid"},
            ),
        ),
        reconciled_at=NOW + timedelta(seconds=4),
        source_cursor="native-modify-reconciliation",
    )
    assert store.get_order(config.run_id, original.order_id).status is OrderStatus.CANCELLED
    durable_replacement = store.get_order(config.run_id, replacement.order_id)
    assert durable_replacement.status is OrderStatus.FILLED
    assert durable_replacement.filled_quantity == Decimal("1")
    assert durable_replacement.venue_order_id == "native-reused-oid"
    assert store.inspect_integrity_readonly(config.run_id).ok


@pytest.mark.parametrize(
    "fault_stage",
    ["after_snapshot", "after_runtime_latch", "after_audit"],
)
def test_reconciliation_failure_is_atomic_under_fault_injection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_stage: str,
) -> None:
    store, config = _admitted_store(tmp_path)
    intent = _intent(config)
    store.create_order(intent)
    before_run = store.get_run(config.run_id)
    before_snapshot = store.latest_remote_snapshot(config.run_id)
    update = OrderProjectionUpdate(
        intent.order_id,
        OrderStatus.OPEN,
        venue_order_id="discrepant-oid",
        filled_quantity=Decimal(0),
    )
    fill = ReconciliationFill(
        fill_id="discrepant-fill",
        order_id=intent.order_id,
        venue_order_id="discrepant-oid",
        quantity=Decimal("0.25"),
        price=Decimal("11"),
        fee=Decimal("0.01"),
        payload={"tid": "discrepant-fill"},
        received_at=NOW + timedelta(seconds=4),
    )
    issue = ReconciliationIssue(
        "POSITION_MISMATCH",
        {"instrument": "HL:BTC:perp", "remote_quantity": "0.25"},
    )
    original_fault_point = TestnetStore._reconciliation_failure_fault_point

    def inject_fault(self: TestnetStore, stage: str) -> None:
        del self
        if stage == fault_stage:
            raise RuntimeError(f"synthetic fault at {stage}")

    monkeypatch.setattr(
        TestnetStore,
        "_reconciliation_failure_fault_point",
        inject_fault,
    )
    failure_args = {
        "positions": {"HL:BTC:perp": Decimal("0.25")},
        "spot_balances": {"USDC": Decimal("100")},
        "equity": Decimal("1000"),
        "withdrawable": Decimal("900"),
        "open_orders": (
            {
                "cloid": intent.cloid,
                "limit_price": "11",
                "oid": "discrepant-oid",
                "quantity": "1",
                "status": "open",
            },
        ),
        "order_updates": (update,),
        "fills": (fill,),
        "issues": (issue,),
        "detected_at": NOW + timedelta(seconds=5),
        "source_cursor": "discrepant-full-input",
        "snapshot_id": "c" * 64,
    }
    with pytest.raises(RuntimeError, match="synthetic fault"):
        store.apply_reconciliation_failure(config.run_id, **failure_args)
    assert store.get_run(config.run_id) == before_run
    assert store.latest_remote_snapshot(config.run_id) == before_snapshot
    assert store.get_order(config.run_id, intent.order_id).status is OrderStatus.REQUESTED
    assert store.list_fills(config.run_id) == ()

    monkeypatch.setattr(
        TestnetStore,
        "_reconciliation_failure_fault_point",
        original_fault_point,
    )
    completed = store.apply_reconciliation_failure(config.run_id, **failure_args)
    assert not completed.reconciled
    observations = completed.payload["reconciliation_observations"]
    assert isinstance(observations, dict)
    assert observations["order_updates"][0]["status"] == OrderStatus.OPEN.value
    assert observations["fills"][0]["price"] == "11"
    run = store.get_run(config.run_id)
    assert run.runtime_state is RuntimeState.MANUAL_REVIEW
    assert run.state_reason == "RECONCILIATION_FAILURE"
    assert run.reconciliation_snapshot_id == before_run.reconciliation_snapshot_id
    assert store.get_order(config.run_id, intent.order_id).status is OrderStatus.REQUESTED
    assert store.list_fills(config.run_id) == ()
    count = run.audit_count
    assert store.apply_reconciliation_failure(config.run_id, **failure_args) == completed
    assert store.get_run(config.run_id).audit_count == count
    assert store.inspect_integrity_readonly(config.run_id).ok


def test_default_control_root_ignores_environment_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = testnet_store_module._default_control_root()
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "attacker-local"))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "attacker-xdg"))
    monkeypatch.setenv("HOME", str(tmp_path / "attacker-home"))
    second = testnet_store_module._default_control_root()
    assert second == first
    if os.name == "nt":
        assert second.parts[-3:] == ("HyperLab", "TestnetExecutor", "control-v1")
    else:
        assert second == Path("/var/lib/hyperlab/testnet-executor/control-v1")


def test_production_control_root_is_never_created_implicitly(tmp_path: Path) -> None:
    missing = tmp_path / "must-be-provisioned" / "control-v1"
    store = TestnetStore.__new__(TestnetStore)
    store._lease_root = missing
    store._production_control_root = True

    with pytest.raises(WalletLeaseError, match="provisioned"):
        store._ensure_control_root()
    assert not missing.exists()


def test_windows_control_path_refuses_reparse_component(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = tmp_path / "control-v1"
    control.mkdir()
    monkeypatch.setattr(
        testnet_store_module,
        "_windows_reparse_point",
        lambda path: path == control,
    )

    with pytest.raises(WalletLeaseError, match="reparse"):
        testnet_store_module._validate_windows_control_components(control)


def test_windows_control_security_rejects_foreign_writers_and_unsafe_acl() -> None:
    current = "S-1-5-21-111-222-333-1001"
    trusted = testnet_store_module._WindowsControlSecurity(
        owner_sid=current,
        current_sid=current,
        dacl_present=True,
        allowed_aces=(
            (current, 0x10000000),
            ("S-1-5-18", 0x10000000),
            ("S-1-5-32-544", 0x10000000),
            ("S-1-5-32-545", 0x00000001),
        ),
    )
    testnet_store_module._validate_windows_control_security(trusted)

    foreign_write = testnet_store_module._WindowsControlSecurity(
        owner_sid=current,
        current_sid=current,
        dacl_present=True,
        allowed_aces=(("S-1-5-32-545", 0x00000002),),
    )
    with pytest.raises(WalletLeaseError, match="another SID"):
        testnet_store_module._validate_windows_control_security(foreign_write)

    null_dacl = testnet_store_module._WindowsControlSecurity(
        owner_sid=current,
        current_sid=current,
        dacl_present=False,
        allowed_aces=(),
    )
    with pytest.raises(WalletLeaseError, match="null DACL"):
        testnet_store_module._validate_windows_control_security(null_dacl)

    unsupported = testnet_store_module._WindowsControlSecurity(
        owner_sid=current,
        current_sid=current,
        dacl_present=True,
        allowed_aces=(),
        unsupported_ace_types=(5,),
    )
    with pytest.raises(WalletLeaseError, match="unsupported ACE"):
        testnet_store_module._validate_windows_control_security(unsupported)


def test_windows_acl_ctypes_inspector_smoke_on_secured_temp_directory(
    tmp_path: Path,
) -> None:
    if os.name != "nt":
        facts = testnet_store_module._WindowsControlSecurity(
            owner_sid="S-1-5-21-111-222-333-1001",
            current_sid="S-1-5-21-111-222-333-1001",
            dacl_present=True,
            allowed_aces=(("S-1-5-21-111-222-333-1001", 0x10000000),),
        )
        testnet_store_module._validate_windows_control_security(facts)
        return

    control = tmp_path / "secured-control"
    control.mkdir()
    initial = testnet_store_module._inspect_windows_control_security(control)
    operator_sid = initial.current_sid
    commands = (
        ("icacls", str(control), "/inheritance:r"),
        (
            "icacls",
            str(control),
            "/grant:r",
            f"*{operator_sid}:(OI)(CI)F",
            "*S-1-5-18:(OI)(CI)F",
            "*S-1-5-32-544:(OI)(CI)F",
        ),
        ("icacls", str(control), "/setowner", f"*{operator_sid}"),
    )
    for command in commands:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr
    facts = testnet_store_module._inspect_windows_control_security(control)
    testnet_store_module._validate_windows_control_security(facts)


def test_windows_control_security_rejects_foreign_writable_project_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hyperlab = tmp_path / "HyperLab"
    executor = hyperlab / "TestnetExecutor"
    control = executor / "control-v1"
    control.mkdir(parents=True)
    current = "S-1-5-21-111-222-333-1001"
    secure = testnet_store_module._WindowsControlSecurity(
        owner_sid=current,
        current_sid=current,
        dacl_present=True,
        allowed_aces=((current, 0x10000000),),
    )
    weak_parent = testnet_store_module._WindowsControlSecurity(
        owner_sid=current,
        current_sid=current,
        dacl_present=True,
        allowed_aces=(("S-1-5-32-545", 0x00000002),),
    )

    monkeypatch.setattr(
        testnet_store_module,
        "_inspect_windows_control_security",
        lambda path: weak_parent if path == hyperlab else secure,
    )
    with pytest.raises(WalletLeaseError, match="another SID"):
        testnet_store_module._validate_windows_project_security(control)


def test_account_lease_is_exclusive_across_api_wallets_and_databases(
    tmp_path: Path,
) -> None:
    control = tmp_path / "control"
    first_config = _config()
    second_config = _config(api_wallet_address="0x" + "9" * 40)
    first = TestnetStore(
        tmp_path / "first.sqlite3",
        lease_root=control,
        owner_id="first-owner",
    )
    second = TestnetStore(
        tmp_path / "second.sqlite3",
        lease_root=control,
        owner_id="second-owner",
    )
    first.create_run(first_config, created_at=NOW)
    second.create_run(second_config, created_at=NOW)
    first.acquire_wallet_lease(first_config.run_id, acquired_at=NOW)
    with pytest.raises(WalletLeaseError, match="another owner"):
        second.acquire_wallet_lease(second_config.run_id, acquired_at=NOW)
    first.close()
    assert second.acquire_wallet_lease(
        second_config.run_id,
        acquired_at=NOW + timedelta(seconds=1),
    ).run_id == second_config.run_id


def test_api_wallet_nonce_is_global_across_databases_and_backward_clock(
    tmp_path: Path,
) -> None:
    control = tmp_path / "control"
    config = _config()
    first = TestnetStore(
        tmp_path / "first.sqlite3",
        lease_root=control,
        owner_id="first-owner",
    )
    first.create_run(config, created_at=NOW)
    first.acquire_wallet_lease(config.run_id, acquired_at=NOW)
    first.set_runtime_state(config.run_id, RuntimeState.STARTING, at=NOW)
    _apply_empty_reconciliation(first, config, at=NOW)
    first.set_runtime_state(config.run_id, RuntimeState.RUNNING, at=NOW)
    first_intent = _intent(config)
    first.create_order(first_intent)
    first_action = first.reserve_action(
        config.run_id,
        action_id=deterministic_action_id(
            config.run_id,
            ActionKind.SUBMIT,
            first_intent.order_id,
        ),
        kind=ActionKind.SUBMIT,
        order_id=first_intent.order_id,
        payload={"cloid": first_intent.cloid},
        created_at=NOW,
    )
    first.close()

    backward = NOW - timedelta(days=30)
    second = TestnetStore(
        tmp_path / "second.sqlite3",
        lease_root=control,
        owner_id="second-owner",
    )
    second.create_run(config, created_at=backward)
    second.acquire_wallet_lease(config.run_id, acquired_at=backward)
    second.set_runtime_state(config.run_id, RuntimeState.STARTING, at=backward)
    _apply_empty_reconciliation(second, config, at=backward)
    second.set_runtime_state(config.run_id, RuntimeState.RUNNING, at=backward)
    second_intent = _intent(config, decision="7")
    second.create_order(second_intent)
    second_action = second.reserve_action(
        config.run_id,
        action_id=deterministic_action_id(
            config.run_id,
            ActionKind.SUBMIT,
            second_intent.order_id,
        ),
        kind=ActionKind.SUBMIT,
        order_id=second_intent.order_id,
        payload={"cloid": second_intent.cloid},
        created_at=backward,
    )
    assert second_action.nonce == first_action.nonce + 1
    assert second.get_run(config.run_id).last_nonce == second_action.nonce


def test_account_rate_limit_is_global_across_sequential_databases(
    tmp_path: Path,
) -> None:
    control = tmp_path / "control"
    config = _config(risk_limits=TestnetRiskLimits(submit_requests_per_minute=1))
    first = TestnetStore(
        tmp_path / "rate-first.sqlite3",
        lease_root=control,
        owner_id="rate-first-owner",
    )
    first.create_run(config, created_at=NOW)
    first.acquire_wallet_lease(config.run_id, acquired_at=NOW)
    first.set_runtime_state(config.run_id, RuntimeState.STARTING, at=NOW)
    _apply_empty_reconciliation(first, config, at=NOW)
    first.set_runtime_state(config.run_id, RuntimeState.RUNNING, at=NOW)
    first_intent = _intent(config)
    first.create_order(first_intent)
    first.reserve_action(
        config.run_id,
        action_id=deterministic_action_id(
            config.run_id,
            ActionKind.SUBMIT,
            first_intent.order_id,
        ),
        kind=ActionKind.SUBMIT,
        order_id=first_intent.order_id,
        payload={"cloid": first_intent.cloid},
        created_at=NOW,
    )
    first.close()

    second = TestnetStore(
        tmp_path / "rate-second.sqlite3",
        lease_root=control,
        owner_id="rate-second-owner",
    )
    second.create_run(config, created_at=NOW + timedelta(seconds=10))
    second.acquire_wallet_lease(
        config.run_id,
        acquired_at=NOW + timedelta(seconds=10),
    )
    second.set_runtime_state(
        config.run_id,
        RuntimeState.STARTING,
        at=NOW + timedelta(seconds=10),
    )
    _apply_empty_reconciliation(second, config, at=NOW + timedelta(seconds=10))
    second.set_runtime_state(
        config.run_id,
        RuntimeState.RUNNING,
        at=NOW + timedelta(seconds=10),
    )
    second_intent = _intent(config, decision="7")
    second.create_order(second_intent)
    second_action_id = deterministic_action_id(
        config.run_id,
        ActionKind.SUBMIT,
        second_intent.order_id,
    )
    with pytest.raises(RunConflictError, match="account-scoped submit request rate"):
        second.reserve_action(
            config.run_id,
            action_id=second_action_id,
            kind=ActionKind.SUBMIT,
            order_id=second_intent.order_id,
            payload={"cloid": second_intent.cloid},
            created_at=NOW + timedelta(seconds=10),
        )
    assert second.count_actions_since(
        config.run_id,
        ActionKind.SUBMIT,
        since=NOW - timedelta(seconds=1),
    ) == 1
    assert second.get_order(
        config.run_id,
        second_intent.order_id,
    ).status is OrderStatus.REQUESTED

    admitted = second.reserve_action(
        config.run_id,
        action_id=second_action_id,
        kind=ActionKind.SUBMIT,
        order_id=second_intent.order_id,
        payload={"cloid": second_intent.cloid},
        created_at=NOW + timedelta(seconds=61),
    )
    assert admitted.status is ActionAttemptStatus.AMBIGUOUS


def test_account_action_identity_tombstone_blocks_cross_database_replay_forever(
    tmp_path: Path,
) -> None:
    control = tmp_path / "control"
    config = _config()
    intent = _intent(config)
    action_id = deterministic_action_id(
        config.run_id,
        ActionKind.SUBMIT,
        intent.order_id,
    )
    first = TestnetStore(
        tmp_path / "identity-first.sqlite3",
        lease_root=control,
        owner_id="identity-first-owner",
    )
    first.create_run(config, created_at=NOW)
    first.acquire_wallet_lease(config.run_id, acquired_at=NOW)
    first.set_runtime_state(config.run_id, RuntimeState.STARTING, at=NOW)
    _apply_empty_reconciliation(first, config, at=NOW)
    first.set_runtime_state(config.run_id, RuntimeState.RUNNING, at=NOW)
    first.create_order(intent)
    first.reserve_action(
        config.run_id,
        action_id=action_id,
        kind=ActionKind.SUBMIT,
        order_id=intent.order_id,
        payload={"cloid": intent.cloid},
        created_at=NOW,
    )
    first.close()

    second = TestnetStore(
        tmp_path / "identity-second.sqlite3",
        lease_root=control,
        owner_id="identity-second-owner",
    )
    second.create_run(config, created_at=NOW + timedelta(seconds=10))
    second.acquire_wallet_lease(
        config.run_id,
        acquired_at=NOW + timedelta(seconds=10),
    )
    second.set_runtime_state(
        config.run_id,
        RuntimeState.STARTING,
        at=NOW + timedelta(seconds=10),
    )
    _apply_empty_reconciliation(second, config, at=NOW + timedelta(seconds=10))
    second.set_runtime_state(
        config.run_id,
        RuntimeState.RUNNING,
        at=NOW + timedelta(seconds=10),
    )
    second.create_order(intent)
    for attempted_at in (
        NOW + timedelta(seconds=10),
        NOW + timedelta(seconds=61),
    ):
        with pytest.raises(AmbiguousActionReplayError, match="already burned"):
            second.reserve_action(
                config.run_id,
                action_id=action_id,
                kind=ActionKind.SUBMIT,
                order_id=intent.order_id,
                payload={"cloid": intent.cloid},
                created_at=attempted_at,
            )
    assert second.get_order(
        config.run_id,
        intent.order_id,
    ).status is OrderStatus.REQUESTED
    assert second.list_ambiguous_actions(config.run_id) == ()
    assert second.account_action_identity_usage(config.run_id) == (1, 100_000)


def test_account_rate_crash_overcounts_but_protective_dms_remains_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = tmp_path / "control"
    config = _config(risk_limits=TestnetRiskLimits(submit_requests_per_minute=1))
    store = TestnetStore(
        tmp_path / "rate-crash.sqlite3",
        lease_root=control,
        owner_id="rate-crash-owner",
    )
    store.create_run(config, created_at=NOW)
    store.acquire_wallet_lease(config.run_id, acquired_at=NOW)
    store.set_runtime_state(config.run_id, RuntimeState.STARTING, at=NOW)
    _apply_empty_reconciliation(store, config, at=NOW)
    store.set_runtime_state(config.run_id, RuntimeState.RUNNING, at=NOW)
    first_intent = _intent(config)
    store.create_order(first_intent)

    def crash_after_rate_burn(point: str, kind: ActionKind, action_id: str) -> None:
        assert point == "AFTER_GLOBAL_RATE_BURN_BEFORE_SQLITE"
        assert kind is ActionKind.SUBMIT
        assert len(action_id) == 64
        raise RuntimeError("synthetic crash after global rate burn")

    monkeypatch.setattr(
        TestnetStore,
        "_rate_burn_fault_point",
        staticmethod(crash_after_rate_burn),
    )
    with pytest.raises(RuntimeError, match="synthetic crash"):
        store.reserve_action(
            config.run_id,
            action_id=deterministic_action_id(
                config.run_id,
                ActionKind.SUBMIT,
                first_intent.order_id,
            ),
            kind=ActionKind.SUBMIT,
            order_id=first_intent.order_id,
            payload={"cloid": first_intent.cloid},
            created_at=NOW,
        )
    assert store.get_order(
        config.run_id,
        first_intent.order_id,
    ).status is OrderStatus.REQUESTED
    assert store.list_ambiguous_actions(config.run_id) == ()

    monkeypatch.setattr(
        TestnetStore,
        "_rate_burn_fault_point",
        staticmethod(lambda point, kind, action_id: None),
    )
    with pytest.raises(AmbiguousActionReplayError, match="already burned"):
        store.reserve_action(
            config.run_id,
            action_id=deterministic_action_id(
                config.run_id,
                ActionKind.SUBMIT,
                first_intent.order_id,
            ),
            kind=ActionKind.SUBMIT,
            order_id=first_intent.order_id,
            payload={"cloid": first_intent.cloid},
            created_at=NOW + timedelta(seconds=1),
        )
    second_intent = _intent(config, decision="7")
    store.create_order(second_intent)
    with pytest.raises(RunConflictError, match="account-scoped submit request rate"):
        store.reserve_action(
            config.run_id,
            action_id=deterministic_action_id(
                config.run_id,
                ActionKind.SUBMIT,
                second_intent.order_id,
            ),
            kind=ActionKind.SUBMIT,
            order_id=second_intent.order_id,
            payload={"cloid": second_intent.cloid},
            created_at=NOW + timedelta(seconds=1),
        )

    deadman = store.reserve_action(
        config.run_id,
        action_id=deterministic_action_id(
            config.run_id,
            ActionKind.SCHEDULE_CANCEL,
            None,
            ordinal=99,
        ),
        kind=ActionKind.SCHEDULE_CANCEL,
        order_id=None,
        payload={"cancel_at_ms": int(NOW.timestamp() * 1000) + 30_000},
        created_at=NOW + timedelta(seconds=1),
    )
    assert deadman.status is ActionAttemptStatus.AMBIGUOUS
    event = next(
        item
        for item in store.get_audit_events(config.run_id)
        if item.event_type == "ACTION_RESERVED_AMBIGUOUS"
    )
    assert event.payload["account_rate_lane"] == "PROTECTIVE_EXEMPT"
    assert event.payload["account_rate_count"] is None


def test_api_wallet_nonce_survives_same_database_restart(tmp_path: Path) -> None:
    store, config = _admitted_store(tmp_path)
    first_intent = _intent(config)
    store.create_order(first_intent)
    first = store.reserve_action(
        config.run_id,
        action_id=deterministic_action_id(
            config.run_id,
            ActionKind.SUBMIT,
            first_intent.order_id,
        ),
        kind=ActionKind.SUBMIT,
        order_id=first_intent.order_id,
        payload={"cloid": first_intent.cloid},
        created_at=NOW,
    )
    store.close()

    restarted = TestnetStore(
        tmp_path / "testnet.sqlite3",
        lease_root=tmp_path / "wallet-leases",
        owner_id="restart-owner",
    )
    restarted.acquire_wallet_lease(config.run_id, acquired_at=NOW)
    second_intent = _intent(config, decision="7")
    restarted.create_order(second_intent)
    second = restarted.reserve_action(
        config.run_id,
        action_id=deterministic_action_id(
            config.run_id,
            ActionKind.SUBMIT,
            second_intent.order_id,
        ),
        kind=ActionKind.SUBMIT,
        order_id=second_intent.order_id,
        payload={"cloid": second_intent.cloid},
        created_at=NOW,
    )
    assert second.nonce == first.nonce + 1


def test_nonce_burn_crash_leaves_safe_gap_and_audits_next_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, config = _admitted_store(tmp_path)
    intent = _intent(config)
    store.create_order(intent)
    action_id = deterministic_action_id(
        config.run_id,
        ActionKind.SUBMIT,
        intent.order_id,
    )
    burned: list[int] = []

    def crash_after_burn(point: str, nonce: int) -> None:
        assert point == "AFTER_GLOBAL_BURN_BEFORE_SQLITE"
        burned.append(nonce)
        raise RuntimeError("synthetic crash after global nonce burn")

    monkeypatch.setattr(
        TestnetStore,
        "_nonce_burn_fault_point",
        staticmethod(crash_after_burn),
    )
    with pytest.raises(RuntimeError, match="synthetic crash"):
        store.reserve_action(
            config.run_id,
            action_id=action_id,
            kind=ActionKind.SUBMIT,
            order_id=intent.order_id,
            payload={"cloid": intent.cloid},
            created_at=NOW,
        )
    assert store.get_order(config.run_id, intent.order_id).status is OrderStatus.REQUESTED
    assert store.list_ambiguous_actions(config.run_id) == ()

    monkeypatch.setattr(
        TestnetStore,
        "_nonce_burn_fault_point",
        staticmethod(lambda point, nonce: None),
    )
    with pytest.raises(AmbiguousActionReplayError, match="already burned"):
        store.reserve_action(
            config.run_id,
            action_id=action_id,
            kind=ActionKind.SUBMIT,
            order_id=intent.order_id,
            payload={"cloid": intent.cloid},
            created_at=NOW,
        )
    next_intent = _intent(config, decision="7")
    store.create_order(next_intent)
    reserved = store.reserve_action(
        config.run_id,
        action_id=deterministic_action_id(
            config.run_id,
            ActionKind.SUBMIT,
            next_intent.order_id,
        ),
        kind=ActionKind.SUBMIT,
        order_id=next_intent.order_id,
        payload={"cloid": next_intent.cloid},
        created_at=NOW,
    )
    assert reserved.nonce == burned[0] + 1
    event = next(
        item
        for item in store.get_audit_events(config.run_id)
        if item.event_type == "ACTION_RESERVED_AMBIGUOUS"
    )
    assert event.payload["global_wallet_watermark_burned_before_sqlite"] is True
    assert event.payload["previous_wallet_nonce"] == burned[0]


@pytest.mark.parametrize(
    ("target", "reason"),
    (
        (RuntimeState.PAUSED, "RACE_PAUSE"),
        (RuntimeState.MANUAL_REVIEW, "RACE_MANUAL"),
        (RuntimeState.STOPPED, None),
    ),
)
def test_protective_state_serializes_against_final_send(
    tmp_path: Path,
    target: RuntimeState,
    reason: str | None,
) -> None:
    store, config = _admitted_store(tmp_path)
    intent = _intent(config)
    store.create_order(intent)
    action_id = deterministic_action_id(
        config.run_id,
        ActionKind.SUBMIT,
        intent.order_id,
    )
    store.reserve_action(
        config.run_id,
        action_id=action_id,
        kind=ActionKind.SUBMIT,
        order_id=intent.order_id,
        payload={"cloid": intent.cloid},
        created_at=NOW,
    )
    started = threading.Event()
    completed = threading.Event()
    failures: list[BaseException] = []

    def latch_state() -> None:
        started.set()
        try:
            store.set_runtime_state(
                config.run_id,
                target,
                reason=reason,
                at=NOW + timedelta(seconds=1),
            )
        except BaseException as error:
            failures.append(error)
        finally:
            completed.set()

    thread = threading.Thread(target=latch_state, daemon=True)
    with store.final_send_permit(config.run_id, action_id):
        thread.start()
        assert started.wait(1)
        assert not completed.wait(0.1)
    assert completed.wait(2)
    thread.join(timeout=1)
    assert not failures
    assert store.get_run(config.run_id).runtime_state is target
    with (
        pytest.raises(RunConflictError, match="runtime state"),
        store.final_send_permit(config.run_id, action_id),
    ):
        pytest.fail("protective state must block every later final-send permit")


def test_kill_latch_fsyncs_parent_before_send_gate_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, config = _admitted_store(tmp_path)
    ordering: list[str] = []
    original_fsync = testnet_store_module._fsync_directory
    original_publish = testnet_store_module._publish_control_file_exclusive
    original_release = testnet_store_module._release_os_lock

    def observe_publish(source: Path, target: Path) -> None:
        ordering.append("publish-write-through")
        original_publish(source, target)

    def observe_fsync(path: Path) -> None:
        assert path == tmp_path / "wallet-leases"
        ordering.append("fsync-parent")
        original_fsync(path)

    def observe_release(stream: object) -> None:
        ordering.append("release-gate")
        original_release(stream)

    monkeypatch.setattr(testnet_store_module, "_fsync_directory", observe_fsync)
    monkeypatch.setattr(
        testnet_store_module,
        "_publish_control_file_exclusive",
        observe_publish,
    )
    monkeypatch.setattr(testnet_store_module, "_release_os_lock", observe_release)
    store.kill(
        config.run_id,
        reason="FSYNC_KILL",
        at=NOW + timedelta(seconds=1),
    )
    assert ordering.index("publish-write-through") < ordering.index("fsync-parent")
    assert ordering.index("fsync-parent") < ordering.index("release-gate")
    assert store.account_kill_latched(config.run_id)


def test_global_kill_blocks_new_run_but_allows_protective_deadman(
    tmp_path: Path,
) -> None:
    store, config = _admitted_store(tmp_path)
    intent = _intent(config)
    store.create_order(intent)
    killed = store.kill(
        config.run_id,
        reason="OPERATOR_KILL",
        at=NOW + timedelta(seconds=1),
    )
    assert killed.runtime_state is RuntimeState.KILLED
    assert store.account_kill_latched(config.run_id)
    with pytest.raises(RunConflictError, match="kill latch"):
        store.reserve_action(
            config.run_id,
            action_id=deterministic_action_id(
                config.run_id,
                ActionKind.SUBMIT,
                intent.order_id,
            ),
            kind=ActionKind.SUBMIT,
            order_id=intent.order_id,
            payload={"cloid": intent.cloid},
            created_at=NOW + timedelta(seconds=2),
        )
    deadman = store.reserve_action(
        config.run_id,
        action_id=deterministic_action_id(
            config.run_id,
            ActionKind.SCHEDULE_CANCEL,
            None,
        ),
        kind=ActionKind.SCHEDULE_CANCEL,
        order_id=None,
        payload={"operation": "scheduleCancel"},
        created_at=NOW + timedelta(seconds=2),
        expires_after_delta_ms=1_000,
    )
    assert deadman.status is ActionAttemptStatus.AMBIGUOUS

    store.close()
    other_config = _config(api_wallet_address="0x" + "9" * 40)
    other = TestnetStore(
        tmp_path / "other.sqlite3",
        lease_root=tmp_path / "wallet-leases",
        owner_id="other-owner",
    )
    other.create_run(other_config, created_at=NOW)
    other.acquire_wallet_lease(
        other_config.run_id,
        acquired_at=NOW + timedelta(seconds=3),
    )
    assert other.account_kill_latched(other_config.run_id)
    with pytest.raises(RunConflictError, match="kill latch"):
        other.set_runtime_state(
            other_config.run_id,
            RuntimeState.STARTING,
            at=NOW + timedelta(seconds=3),
        )
    protective = other.reserve_action(
        other_config.run_id,
        action_id=deterministic_action_id(
            other_config.run_id,
            ActionKind.SCHEDULE_CANCEL,
            None,
        ),
        kind=ActionKind.SCHEDULE_CANCEL,
        order_id=None,
        payload={"operation": "scheduleCancel"},
        created_at=NOW + timedelta(seconds=3),
    )
    assert protective.nonce > 0


def test_account_kill_serializes_against_final_send_across_store_owners(
    tmp_path: Path,
) -> None:
    store, config = _admitted_store(tmp_path, owner_id="sender-owner")
    intent = _intent(config)
    store.create_order(intent)
    action_id = deterministic_action_id(
        config.run_id,
        ActionKind.SUBMIT,
        intent.order_id,
    )
    store.reserve_action(
        config.run_id,
        action_id=action_id,
        kind=ActionKind.SUBMIT,
        order_id=intent.order_id,
        payload={"cloid": intent.cloid},
        created_at=NOW + timedelta(seconds=1),
    )
    killer = TestnetStore(
        store.path,
        lease_root=tmp_path / "wallet-leases",
        owner_id="killer-owner",
    )
    started = threading.Event()
    completed = threading.Event()
    failures: list[BaseException] = []

    def latch_kill() -> None:
        started.set()
        try:
            killer.kill(
                config.run_id,
                reason="RACE_KILL",
                at=NOW + timedelta(seconds=2),
            )
        except BaseException as error:
            failures.append(error)
        finally:
            completed.set()

    thread = threading.Thread(target=latch_kill, daemon=True)
    with store.final_send_permit(config.run_id, action_id) as permitted:
        assert permitted.action_id == action_id
        thread.start()
        assert started.wait(1)
        assert not completed.wait(0.1)
        assert not store.account_kill_latched(config.run_id)
    assert completed.wait(2)
    thread.join(timeout=1)
    assert not failures
    assert store.account_kill_latched(config.run_id)
    assert store.get_run(config.run_id).runtime_state is RuntimeState.KILLED
    with (
        pytest.raises(RunConflictError, match="kill latch"),
        store.final_send_permit(config.run_id, action_id),
    ):
        pytest.fail("killed account must never receive a final send permit")


def test_malformed_kill_latch_and_broken_symlink_fail_closed(
    tmp_path: Path,
) -> None:
    for suffix, make_latch in (
        ("malformed", lambda path: path.write_text("not-json", encoding="utf-8")),
        (
            "broken-link",
            lambda path: os.symlink(path.with_suffix(".missing"), path),
        ),
    ):
        config = _config(api_wallet_address="0x" + ("8" if suffix == "malformed" else "7") * 40)
        control = tmp_path / suffix / "control"
        store = TestnetStore(
            tmp_path / suffix / "state.sqlite3",
            lease_root=control,
            owner_id=f"owner-{suffix}",
        )
        store.create_run(config, created_at=NOW)
        lease = store.acquire_wallet_lease(config.run_id, acquired_at=NOW)
        store.release_wallet_lease(config.run_id)
        latch = control / f"{lease.wallet_scope_hash}.killed.json"
        try:
            make_latch(latch)
        except OSError:
            if suffix == "broken-link":
                continue
            raise
        assert store.account_kill_latched(config.run_id)
        store.acquire_wallet_lease(config.run_id, acquired_at=NOW)
        with pytest.raises(RunConflictError, match="kill latch"):
            store.set_runtime_state(config.run_id, RuntimeState.STARTING, at=NOW)


def test_readonly_open_never_creates_or_mutates(tmp_path: Path) -> None:
    missing = tmp_path / "missing-parent" / "missing.sqlite3"
    with pytest.raises(FileNotFoundError):
        TestnetStore.open_existing_readonly(missing)
    assert not missing.parent.exists()

    store, config = _admitted_store(tmp_path)
    store.close()
    path = tmp_path / "testnet.sqlite3"
    before_bytes = path.read_bytes()
    before_mtime = path.stat().st_mtime_ns
    readonly = TestnetStore.open_existing_readonly(path)
    assert readonly.list_runs() == (config.run_id,)
    assert readonly.inspect_integrity_readonly(config.run_id).ok
    with pytest.raises(Exception, match="read-only"):
        readonly.pause(config.run_id, reason="SHOULD_FAIL", at=NOW)
    readonly.close()
    assert path.read_bytes() == before_bytes
    assert path.stat().st_mtime_ns == before_mtime


def test_secret_screen_rejects_untyped_hash_names_and_raw_key_shapes(
    tmp_path: Path,
) -> None:
    store, config = _admitted_store(tmp_path)
    for secret in ("ab" * 32, "0x" + "ab" * 32):
        with pytest.raises(SecretPersistenceError):
            store.append_audit(
                config.run_id,
                "SECRET_SHAPE_TEST",
                {"value": secret},
                created_at=NOW,
            )
        with pytest.raises(SecretPersistenceError):
            store.append_audit(
                config.run_id,
                "SECRET_SHAPE_TEST",
                {"foo_hash": secret.removeprefix("0x")},
                created_at=NOW,
            )
    event = store.append_audit(
        config.run_id,
        "VENUE_HASH_DIGEST",
        {"venue_hash_digest": "a" * 64},
        created_at=NOW,
    )
    assert event.payload["venue_hash_digest"] == "a" * 64

    intent = _intent(config)
    store.create_order(intent)
    for secret in ("cd" * 32, "0x" + "cd" * 32):
        with pytest.raises(SecretPersistenceError):
            store.reserve_action(
                config.run_id,
                action_id=deterministic_action_id(
                    config.run_id,
                    ActionKind.SUBMIT,
                    intent.order_id,
                ),
                kind=ActionKind.SUBMIT,
                order_id=intent.order_id,
                payload={"value": secret},
                created_at=NOW + timedelta(seconds=1),
            )
        with pytest.raises(SecretPersistenceError):
            store.record_fill(
                config.run_id,
                fill_id="secret-fill",
                order_id=intent.order_id,
                venue_order_id="secret-venue",
                quantity=Decimal("1"),
                price=Decimal("10"),
                fee=Decimal(0),
                payload={"value": secret},
                received_at=NOW + timedelta(seconds=1),
            )
        with pytest.raises(SecretPersistenceError):
            store.record_remote_snapshot(
                config.run_id,
                positions={},
                spot_balances={},
                equity=Decimal("1"),
                withdrawable=Decimal("1"),
                open_orders=({"value": secret},),
                received_at=NOW + timedelta(seconds=1),
            )
    assert store.get_order(config.run_id, intent.order_id).status is OrderStatus.REQUESTED


def test_partial_response_and_stable_fill_redelivery_use_max_not_sum(
    tmp_path: Path,
) -> None:
    store, config = _admitted_store(tmp_path)
    intent = _intent(config)
    store.create_order(intent)
    action_id = deterministic_action_id(
        config.run_id,
        ActionKind.SUBMIT,
        intent.order_id,
    )
    store.reserve_action(
        config.run_id,
        action_id=action_id,
        kind=ActionKind.SUBMIT,
        order_id=intent.order_id,
        payload={"cloid": intent.cloid},
        created_at=NOW + timedelta(seconds=1),
    )
    store.complete_action(
        config.run_id,
        action_id,
        ActionAttemptStatus.CONFIRMED,
        response={"authority": "immediate-partial"},
        order_updates=(
            OrderProjectionUpdate(
                intent.order_id,
                OrderStatus.PARTIALLY_FILLED,
                venue_order_id="partial-oid",
                filled_quantity=Decimal("0.4"),
                average_fill_price=Decimal("10"),
            ),
        ),
        resolved_at=NOW + timedelta(seconds=2),
    )
    first = ReconciliationFill(
        fill_id="stable-partial-1",
        order_id=intent.order_id,
        venue_order_id="partial-oid",
        quantity=Decimal("0.2"),
        price=Decimal("10"),
        fee=Decimal(0),
        payload={"tid": "stable-partial-1"},
        received_at=NOW + timedelta(seconds=3),
    )
    assert first.price == Decimal("10")
    for _ in range(2):
        store.record_fill(
            config.run_id,
            fill_id=first.fill_id,
            order_id=first.order_id,
            venue_order_id=first.venue_order_id,
            quantity=first.quantity,
            price=first.price,
            fee=first.fee,
            payload=first.payload,
            received_at=first.received_at,
        )
    assert store.get_order(config.run_id, intent.order_id).filled_quantity == Decimal(
        "0.4"
    )
    store.record_fill(
        config.run_id,
        fill_id="stable-partial-2",
        order_id=intent.order_id,
        venue_order_id="partial-oid",
        quantity=Decimal("0.2"),
        price=Decimal("10"),
        fee=Decimal(0),
        payload={"tid": "stable-partial-2"},
        received_at=NOW + timedelta(seconds=4),
    )
    assert store.get_order(config.run_id, intent.order_id).filled_quantity == Decimal(
        "0.4"
    )
    store.record_fill(
        config.run_id,
        fill_id="stable-partial-3",
        order_id=intent.order_id,
        venue_order_id="partial-oid",
        quantity=Decimal("0.6"),
        price=Decimal("10"),
        fee=Decimal(0),
        payload={"tid": "stable-partial-3"},
        received_at=NOW + timedelta(seconds=5),
    )
    assert store.get_order(config.run_id, intent.order_id).filled_quantity == Decimal(
        "1"
    )


def test_divergent_stable_fill_latches_manual_review(tmp_path: Path) -> None:
    store, config = _admitted_store(tmp_path)
    intent = _intent(config)
    store.create_order(intent)
    store.transition_order(
        config.run_id,
        intent.order_id,
        OrderStatus.OPEN,
        at=NOW + timedelta(seconds=1),
        venue_order_id="fill-conflict-oid",
    )
    base = dict(
        fill_id="divergent-fill",
        order_id=intent.order_id,
        venue_order_id="fill-conflict-oid",
        quantity=Decimal("0.5"),
        fee=Decimal(0),
        payload={"tid": "divergent-fill"},
        received_at=NOW + timedelta(seconds=2),
    )
    store.record_fill(config.run_id, price=Decimal("10"), **base)
    with pytest.raises(IdempotencyConflictError, match="divergent"):
        store.record_fill(config.run_id, price=Decimal("11"), **base)
    assert store.get_run(config.run_id).runtime_state is RuntimeState.MANUAL_REVIEW


def test_pause_reason_is_stable_bounded_and_durable(tmp_path: Path) -> None:
    store, config = _admitted_store(tmp_path)
    with pytest.raises(ValueError, match="stable reason code"):
        store.pause(
            config.run_id,
            reason="free text could leak secrets",
            at=NOW + timedelta(seconds=1),
        )
    paused = store.pause(
        config.run_id,
        reason="OPERATOR_PAUSE",
        at=NOW + timedelta(seconds=1),
    )
    assert paused.runtime_state is RuntimeState.PAUSED
    store.close()
    restarted = TestnetStore(
        tmp_path / "testnet.sqlite3",
        lease_root=tmp_path / "wallet-leases",
        owner_id="pause-restart",
    )
    assert restarted.get_run(config.run_id).runtime_state is RuntimeState.PAUSED
    stopped_config = _config(api_wallet_address="0x" + "9" * 40)
    stopped = TestnetStore(
        tmp_path / "stopped.sqlite3",
        lease_root=tmp_path / "separate-control",
    )
    stopped.create_run(stopped_config, created_at=NOW)
    assert (
        stopped.set_runtime_state(
            stopped_config.run_id,
            RuntimeState.MANUAL_REVIEW,
            reason="PREFLIGHT_FAILURE",
            at=NOW,
        ).runtime_state
        is RuntimeState.MANUAL_REVIEW
    )


@pytest.mark.parametrize(
    ("tamper_sql", "expected_code"),
    [
        (
            "UPDATE testnet_runs SET runtime_state='PAUSED', state_reason='TAMPER' ",
            "RUNTIME_PROJECTION",
        ),
        (
            "UPDATE testnet_actions SET status='REJECTED' ",
            "ACTION_PROJECTION",
        ),
        (
            "UPDATE testnet_orders SET venue_order_id='tampered-oid' ",
            "ORDER_PROJECTION",
        ),
        (
            "UPDATE testnet_runs SET reconciliation_snapshot_id='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' ",
            "RECONCILIATION_ID_PROJECTION",
        ),
        (
            "DROP TRIGGER testnet_audit_no_update",
            "SCHEMA_TRIGGER_MISSING",
        ),
    ],
)
def test_integrity_detects_mutable_projection_and_schema_tamper(
    tmp_path: Path,
    tamper_sql: str,
    expected_code: str,
) -> None:
    store, config = _admitted_store(tmp_path)
    intent = _intent(config)
    store.create_order(intent)
    action_id = deterministic_action_id(
        config.run_id,
        ActionKind.SUBMIT,
        intent.order_id,
    )
    store.reserve_action(
        config.run_id,
        action_id=action_id,
        kind=ActionKind.SUBMIT,
        order_id=intent.order_id,
        payload={"cloid": intent.cloid},
        created_at=NOW + timedelta(seconds=1),
    )
    store.complete_action(
        config.run_id,
        action_id,
        ActionAttemptStatus.CONFIRMED,
        response={"authority": "synthetic"},
        order_updates=(
            OrderProjectionUpdate(
                intent.order_id,
                OrderStatus.OPEN,
                venue_order_id="integrity-oid",
                filled_quantity=Decimal(0),
            ),
        ),
        resolved_at=NOW + timedelta(seconds=2),
    )
    with sqlite3.connect(store.path) as connection:
        if tamper_sql.startswith("DROP"):
            connection.execute(tamper_sql)
        else:
            connection.execute(tamper_sql + "WHERE run_id=?", (config.run_id,))
        connection.commit()
    report = store.inspect_integrity_readonly(config.run_id)
    assert not report.ok
    assert expected_code in {issue.code for issue in report.issues}
    with pytest.raises(IntegrityError):
        store.verify_integrity(config.run_id)
    assert store.get_run(config.run_id).runtime_state is RuntimeState.MANUAL_REVIEW
