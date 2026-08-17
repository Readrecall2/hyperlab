"""SYNTHETIC-ONLY Phase 13 execution safety tests; never contacts a venue."""

from __future__ import annotations

import inspect
import json
import secrets
import traceback
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from hyperlab_testnet.adapter import (
    TESTNET_API_ORIGIN,
    TESTNET_WEBSOCKET_URL,
    ActionOutcome,
    AdapterError,
    EndpointIsolationError,
    HttpResult,
    HyperliquidTestnetAdapter,
    OutcomeKind,
    PerpAssetConstraints,
    ReadTransportError,
    RequestsJsonTransport,
    parse_all_mids,
    perp_constraints_from_meta,
    verify_extra_agent_scope,
)
from hyperlab_testnet.adapter import (
    testnet_signer_from_secret as make_testnet_signer,
)
from hyperlab_testnet.canonical import deterministic_id
from hyperlab_testnet.config import TestnetConfig, TestnetRiskLimits
from hyperlab_testnet.engine import (
    ActionRecord,
    ActionRequiresReconciliation,
    ExecutionError,
    TestnetExecutionEngine,
)
from hyperlab_testnet.models import (
    ActionAttemptStatus,
    ActionKind,
    OrderSide,
    OrderStatus,
    RuntimeState,
    TestnetOrder,
    TestnetOrderIntent,
    TimeInForce,
)
from hyperlab_testnet.reconciliation import (
    ExchangeFirstReconciler,
    LocalSnapshot,
    ReconciliationError,
    RemoteFill,
    RemoteOrder,
    RemoteSnapshot,
    RunScopedStore,
    fetch_user_fills_contiguous,
    parse_user_fills,
    plan_reconciliation,
)
from hyperlab_testnet.runtime import (
    EventSourceDisconnected,
    EventSourceError,
    RuntimeErrorClosed,
    TestnetPreflight,
    TestnetRuntime,
    TestnetUserEventSource,
    WebsocketClientConnector,
)
from hyperlab_testnet.store import TestnetStore

_NOW = datetime(2026, 8, 17, 2, 0, tzinfo=UTC)
_ACCOUNT = "0x" + "1" * 40
_API_WALLET = "0x" + "2" * 40


class MutableClock:
    def __init__(self, value: datetime = _NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value

    def advance_ms(self, milliseconds: int) -> None:
        self.value += timedelta(milliseconds=milliseconds)


class MutableMonotonic:
    def __init__(self) -> None:
        self.value = 0.0
        self.waits: list[float] = []

    def __call__(self) -> float:
        return self.value

    def wait(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.value += seconds


def _config() -> TestnetConfig:
    return TestnetConfig(
        candidate_id="synthetic-phase13-candidate",
        account_address=_ACCOUNT,
        api_wallet_address=_API_WALLET,
        strategy_name="synthetic-manual-testnet-smoke",
        strategy_hash="a" * 64,
        build_hash="b" * 64,
        source_identity="synthetic-testnet-source",
        source_hash="c" * 64,
        risk_limits=TestnetRiskLimits(
            max_gross_notional=Decimal("1000"),
            max_position_notional=Decimal("500"),
            max_order_notional=Decimal("100"),
            max_position_quantity=Decimal("5"),
            max_order_quantity=Decimal("1"),
        ),
    )


def _intent(
    run_id: str,
    *,
    ordinal: int = 0,
    coin: str = "BTC",
    quantity: str = "1",
    price: str = "10",
    created_at: datetime = _NOW,
) -> TestnetOrderIntent:
    return TestnetOrderIntent.create(
        run_id=run_id,
        decision_id=deterministic_id("synthetic_phase13_decision", ordinal),
        instrument=f"HL:{coin}:perp",
        side=OrderSide.BUY,
        quantity=quantity,
        limit_price=price,
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        created_at=created_at,
        ordinal=ordinal,
    )


def _remote_order(
    intent: TestnetOrderIntent,
    *,
    oid: str = "7",
    status: OrderStatus = OrderStatus.OPEN,
    cloid: str | None = None,
) -> RemoteOrder:
    return RemoteOrder(
        coin=intent.instrument.split(":")[1],
        oid=oid,
        cloid=intent.cloid if cloid is None else cloid,
        side=OrderSide.BUY,
        limit_price=intent.limit_price,
        original_quantity=intent.quantity,
        remaining_quantity=(intent.quantity if status is OrderStatus.OPEN else Decimal(0)),
        status=status,
    )


def _local(
    *orders: TestnetOrder,
    ambiguous_actions: tuple[Any, ...] = (),
) -> LocalSnapshot:
    return LocalSnapshot(
        orders=orders,
        fill_ids=frozenset(),
        positions={},
        spot_balances={},
        equity=Decimal("100"),
        fill_cursor_ms=int(_NOW.timestamp() * 1_000),
        fill_overlap_ids=frozenset(),
        ambiguous_actions=ambiguous_actions,
    )


def _remote(
    *,
    captured_at_ms: int | None = None,
    open_orders: tuple[RemoteOrder, ...] = (),
    order_statuses: dict[str, RemoteOrder | None] | None = None,
    fills: tuple[RemoteFill, ...] = (),
    positions: dict[str, Decimal] | None = None,
) -> RemoteSnapshot:
    captured = captured_at_ms or int(_NOW.timestamp() * 1_000) + 10_000
    return RemoteSnapshot(
        captured_at_ms=captured,
        fill_start_ms=captured - 1,
        open_orders=open_orders,
        order_statuses=order_statuses or {},
        fills=fills,
        positions=positions or {},
        spot_balances={},
        equity=Decimal("100"),
        withdrawable=Decimal("100"),
    )


class FakeSigner:
    address = _API_WALLET

    def sign_message(self, message: object) -> object:
        return message


class AdapterHttp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.fail_exchange = False
        self.meta: object = {"universe": [{"name": "BTC", "szDecimals": 3}]}

    def post_json(
        self,
        *,
        url: str,
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> HttpResult:
        del timeout_seconds
        self.calls.append((url, dict(payload)))
        if url.endswith("/info"):
            return HttpResult(200, self.meta)
        if self.fail_exchange:
            raise TimeoutError("synthetic transport loss")
        action = payload.get("action")
        if isinstance(action, dict) and action.get("type") == "cancelByCloid":
            return HttpResult(
                200,
                {
                    "response": {
                        "data": {"statuses": ["success"]},
                        "type": "order",
                    },
                    "status": "ok",
                },
            )
        return HttpResult(
            200,
            {
                "response": {
                    "data": {"statuses": [{"resting": {"oid": 42}}]},
                    "type": "order",
                },
                "status": "ok",
            },
        )


def _adapter(http: AdapterHttp) -> tuple[HyperliquidTestnetAdapter, list[tuple[object, ...]]]:
    signing_calls: list[tuple[object, ...]] = []

    def sign_l1(*args: object) -> object:
        signing_calls.append(args)
        return {"synthetic": "signature-not-persisted"}

    adapter = HyperliquidTestnetAdapter(
        origin=TESTNET_API_ORIGIN,
        account_address=_ACCOUNT,
        api_wallet_address=_API_WALLET,
        signer=FakeSigner(),
        asset_constraints_by_coin={
            "BTC": PerpAssetConstraints("BTC", 0, 3),
        },
        http=http,
        timeout_seconds=2,
        action_ttl_ms=5_000,
        sign_l1=sign_l1,
    )
    return adapter, signing_calls


def test_meta_constraints_agent_scope_and_signed_endpoint_are_fail_closed() -> None:
    constraints = perp_constraints_from_meta({"universe": [{"name": "BTC", "szDecimals": 3}]})
    assert constraints["BTC"] == PerpAssetConstraints("BTC", 0, 3)
    with pytest.raises(TypeError):
        constraints["ETH"] = PerpAssetConstraints("ETH", 1, 2)  # type: ignore[index]

    now_ms = int(_NOW.timestamp() * 1_000)
    verified = verify_extra_agent_scope(
        [
            {"address": _API_WALLET, "name": "phase13", "validUntil": now_ms + 60_000},
            {"address": "0x" + "3" * 40, "name": "old", "validUntil": now_ms - 1},
        ],
        expected_api_wallet_address=_API_WALLET,
        now_ms=now_ms,
    )
    assert verified.address == _API_WALLET
    with pytest.raises(ValueError, match="only active"):
        verify_extra_agent_scope(
            [
                {"address": _API_WALLET, "name": "phase13", "validUntil": now_ms + 60_000},
                {"address": "0x" + "3" * 40, "name": "other", "validUntil": now_ms + 60_000},
            ],
            expected_api_wallet_address=_API_WALLET,
            now_ms=now_ms,
        )

    http = AdapterHttp()
    adapter, signing_calls = _adapter(http)
    with pytest.raises(AdapterError, match="constraint verification"):
        adapter.submit_order(
            _intent("d" * 64, quantity="0.001", price="123.45"),
            nonce=now_ms,
            constraint_verification=object(),
        )
    assert http.calls == []
    constraint_verification = adapter.verify_live_constraints()
    outcome = adapter.submit_order(
        _intent("d" * 64, quantity="0.001", price="123.45"),
        nonce=now_ms,
        constraint_verification=constraint_verification,
    )
    assert outcome.kind is OutcomeKind.RESTING
    assert http.calls[0] == (
        TESTNET_API_ORIGIN + "/info",
        {"type": "meta"},
    )
    exchange_url, exchange_payload = http.calls[1]
    assert exchange_url == TESTNET_API_ORIGIN + "/exchange"
    assert exchange_payload["vaultAddress"] is None
    assert exchange_payload["expiresAfter"] == now_ms + 5_000
    assert signing_calls[0][2] is None
    assert signing_calls[0][4:] == (now_ms + 5_000, False)
    with pytest.raises(AdapterError, match="constraint verification"):
        adapter.submit_order(
            _intent("d" * 64, quantity="0.001", price="123.45"),
            nonce=now_ms + 1,
            constraint_verification=constraint_verification,
        )
    assert len(http.calls) == 2
    assert len(signing_calls) == 1
    assert "vault_address" not in inspect.signature(HyperliquidTestnetAdapter).parameters

    with pytest.raises(EndpointIsolationError):
        HyperliquidTestnetAdapter(
            origin="https://api.hyperliquid.xyz",
            account_address=_ACCOUNT,
            api_wallet_address=_API_WALLET,
            signer=FakeSigner(),
            asset_constraints_by_coin={"BTC": PerpAssetConstraints("BTC", 0, 3)},
            http=http,
            timeout_seconds=2,
            action_ttl_ms=5_000,
        )


def test_live_meta_mutation_precision_and_transport_ambiguity_block_signing() -> None:
    http = AdapterHttp()
    adapter, signing_calls = _adapter(http)
    http.meta = {"universe": [{"name": "BTC", "szDecimals": 4}]}
    with pytest.raises(AdapterError, match="constraints changed"):
        adapter.verify_live_constraints()
    assert signing_calls == []

    http.meta = {"universe": [{"name": "BTC", "szDecimals": 3}]}
    constraint_verification = adapter.verify_live_constraints()
    with pytest.raises(ValueError, match="szDecimals"):
        adapter.submit_order(
            _intent("d" * 64, quantity="0.0001"),
            nonce=int(_NOW.timestamp() * 1_000),
            constraint_verification=constraint_verification,
        )
    assert signing_calls == []

    http.fail_exchange = True
    constraint_verification = adapter.verify_live_constraints()
    outcome = adapter.submit_order(
        _intent("d" * 64, quantity="0.001"),
        nonce=int(_NOW.timestamp() * 1_000),
        constraint_verification=constraint_verification,
    )
    assert outcome.kind is OutcomeKind.AMBIGUOUS
    assert len(signing_calls) == 1


def test_protective_cancel_uses_frozen_asset_without_live_meta_read() -> None:
    http = AdapterHttp()
    adapter, signing_calls = _adapter(http)
    http.meta = {"universe": [{"name": "BTC", "szDecimals": 99}]}

    outcome = adapter.cancel_by_cloid(
        coin="BTC",
        cloid="0x" + "d" * 32,
        nonce=int(_NOW.timestamp() * 1_000),
    )

    assert outcome.kind is OutcomeKind.CANCELLED
    assert len(http.calls) == 1
    exchange_url, exchange_payload = http.calls[0]
    assert exchange_url == TESTNET_API_ORIGIN + "/exchange"
    action = exchange_payload["action"]
    assert isinstance(action, dict)
    assert action == {
        "type": "cancelByCloid",
        "cancels": [{"asset": 0, "cloid": "0x" + "d" * 32}],
    }
    assert len(signing_calls) == 1


def test_secret_constructor_has_no_cause_or_secret_traceback() -> None:
    signer = make_testnet_signer(secrets.token_bytes(32))
    assert repr(signer) == "TestnetSigner(<redacted>)"
    synthetic_invalid = "synthetic-" + secrets.token_hex(24)
    with pytest.raises(ValueError) as caught:
        make_testnet_signer(synthetic_invalid)
    rendered = "".join(
        traceback.format_exception(
            type(caught.value),
            caught.value,
            caught.value.__traceback__,
        )
    )
    assert caught.value.__cause__ is None
    assert synthetic_invalid not in rendered
    assert synthetic_invalid[:12] not in rendered
    assert synthetic_invalid[-12:] not in rendered


class FakeResponse:
    def __init__(
        self,
        content: bytes,
        *,
        url: str,
        content_length: str | None = None,
    ) -> None:
        self.content = content
        self.url = url
        self.status_code = 200
        self.is_redirect = False
        self.headers = {"Content-Length": content_length} if content_length is not None else {}
        self.closed = False

    def iter_content(self, *, chunk_size: int) -> object:
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset : offset + chunk_size]

    def close(self) -> None:
        self.closed = True


class FakeSession:
    trust_env = True

    def __init__(self, content: bytes) -> None:
        self.content = content

    def post(self, url: str, **kwargs: object) -> FakeResponse:
        assert kwargs["allow_redirects"] is False
        assert kwargs["stream"] is True
        return FakeResponse(self.content, url=url)


@pytest.mark.parametrize(
    "encoded",
    [
        b'{"status":"ok","status":"err"}',
        b'{"value":NaN}',
        b'{"value":Infinity}',
    ],
)
def test_raw_http_transport_rejects_noncanonical_venue_json(encoded: bytes) -> None:
    transport = RequestsJsonTransport()
    transport._session = FakeSession(encoded)  # type: ignore[assignment]
    result = transport.post_json(
        url=TESTNET_API_ORIGIN + "/info",
        payload={"type": "meta"},
        timeout_seconds=1,
    )
    assert result.payload is None


def test_raw_http_transport_refuses_oversize_content_length_before_read() -> None:
    transport = RequestsJsonTransport()

    class OversizeSession(FakeSession):
        def post(self, url: str, **kwargs: object) -> FakeResponse:
            assert kwargs["stream"] is True
            return FakeResponse(
                b"",
                url=url,
                content_length=str(16 * 1024 * 1024 + 1),
            )

    transport._session = OversizeSession(b"")  # type: ignore[assignment]
    with pytest.raises(ReadTransportError, match="size limit"):
        transport.post_json(
            url=TESTNET_API_ORIGIN + "/info",
            payload={"type": "meta"},
            timeout_seconds=1,
        )


def test_raw_http_transport_bounds_chunked_body_and_refuses_endpoint_swap() -> None:
    transport = RequestsJsonTransport()

    class ChunkedResponse(FakeResponse):
        def iter_content(self, *, chunk_size: int) -> object:
            del chunk_size
            yield b"x" * (8 * 1024 * 1024)
            yield b"y" * (8 * 1024 * 1024 + 1)

    chunked = ChunkedResponse(b"", url=TESTNET_API_ORIGIN + "/info")

    class ChunkedSession(FakeSession):
        def post(self, url: str, **kwargs: object) -> FakeResponse:
            assert kwargs["stream"] is True
            assert url == TESTNET_API_ORIGIN + "/info"
            return chunked

    transport._session = ChunkedSession(b"")  # type: ignore[assignment]
    with pytest.raises(ReadTransportError, match="size limit"):
        transport.post_json(
            url=TESTNET_API_ORIGIN + "/info",
            payload={"type": "meta"},
            timeout_seconds=1,
        )
    assert chunked.closed

    swapped = FakeResponse(b"{}", url="https://api.hyperliquid.xyz/info")

    class SwappedSession(FakeSession):
        def post(self, url: str, **kwargs: object) -> FakeResponse:
            del url, kwargs
            return swapped

    transport._session = SwappedSession(b"")  # type: ignore[assignment]
    with pytest.raises(EndpointIsolationError):
        transport.post_json(
            url=TESTNET_API_ORIGIN + "/info",
            payload={"type": "meta"},
            timeout_seconds=1,
        )
    assert swapped.closed


def _fill_payload(
    tid: object,
    *,
    timestamp_ms: int,
    fee: str = "0.01",
    cloid: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "coin": "BTC",
        "fee": fee,
        "hash": "0x" + "ab" * 32,
        "oid": 7,
        "px": "10",
        "side": "B",
        "sz": "0.1",
        "tid": tid,
        "time": timestamp_ms,
    }
    if cloid is not None:
        payload["cloid"] = cloid
    return payload


def test_fill_identity_is_stable_and_signed_maker_rebate_is_preserved() -> None:
    timestamp_ms = int(_NOW.timestamp() * 1_000)
    parsed = parse_user_fills([_fill_payload("venue-tid-1", timestamp_ms=timestamp_ms, fee="-0.002")])
    assert parsed[0].fee == Decimal("-0.002")
    assert parsed[0].venue_hash == "0x" + "ab" * 32

    missing_tid = _fill_payload("venue-tid-1", timestamp_ms=timestamp_ms)
    missing_tid.pop("tid")
    with pytest.raises(ReconciliationError, match="tid is required"):
        parse_user_fills([missing_tid, dict(missing_tid)])


@pytest.mark.parametrize("field", ["px", "sz", "fee"])
def test_remote_decimal_exponents_are_bounded_before_canonical_format(field: str) -> None:
    timestamp_ms = int(_NOW.timestamp() * 1_000)
    payload = _fill_payload("bounded-remote-decimal", timestamp_ms=timestamp_ms)
    payload[field] = "1e1000"
    with pytest.raises(ReconciliationError, match="must be a decimal"):
        parse_user_fills([payload])

    with pytest.raises(ValueError, match="not a decimal"):
        parse_all_mids({"BTC": "1e1000"})


class FillWindowAdapter:
    def __init__(self, payload: list[dict[str, object]]) -> None:
        self.payload = payload

    def read_user_fills_by_time(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> object:
        return [item for item in self.payload if start_time_ms <= int(item["time"]) <= end_time_ms]


def test_fill_cursor_requires_every_boundary_anchor_and_refuses_full_single_ms() -> None:
    timestamp_ms = int(_NOW.timestamp() * 1_000)
    first = parse_user_fills([_fill_payload("first", timestamp_ms=timestamp_ms)])[0]
    second = parse_user_fills([_fill_payload("second", timestamp_ms=timestamp_ms)])[0]
    with pytest.raises(ReconciliationError, match="overlap anchor"):
        fetch_user_fills_contiguous(
            FillWindowAdapter([_fill_payload("first", timestamp_ms=timestamp_ms)]),
            start_time_ms=timestamp_ms,
            end_time_ms=timestamp_ms,
            required_overlap_ids=frozenset({first.fill_id, second.fill_id}),
        )

    page = [_fill_payload(index, timestamp_ms=timestamp_ms) for index in range(2_000)]
    with pytest.raises(ReconciliationError, match="page limit"):
        fetch_user_fills_contiguous(
            FillWindowAdapter(page),
            start_time_ms=timestamp_ms,
            end_time_ms=timestamp_ms,
        )


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("oid", "8", "ORDER_OID_DIVERGENCE"),
        ("coin", "ETH", "ORDER_INSTRUMENT_DIVERGENCE"),
        ("side", OrderSide.SELL, "ORDER_SIDE_DIVERGENCE"),
        ("original_quantity", Decimal("2"), "ORDER_SIZE_DIVERGENCE"),
        ("limit_price", Decimal("11"), "ORDER_LIMIT_PRICE_DIVERGENCE"),
    ],
)
def test_terminal_order_status_requires_exact_economic_identity(
    field: str,
    value: object,
    code: str,
) -> None:
    intent = _intent("d" * 64)
    local_order = TestnetOrder(
        intent,
        OrderStatus.OPEN,
        venue_order_id="7",
        updated_at=_NOW,
    )
    terminal = _remote_order(intent, status=OrderStatus.CANCELLED)
    mutated = replace(terminal, **{field: value})
    plan = plan_reconciliation(
        _local(local_order),
        _remote(order_statuses={intent.cloid: mutated}),
    )
    assert code in {issue.code for issue in plan.issues}


def test_open_status_snapshot_race_and_native_replace_oid_ownership() -> None:
    intent = _intent("d" * 64)
    open_local = TestnetOrder(
        intent,
        OrderStatus.OPEN,
        venue_order_id="7",
        updated_at=_NOW,
    )
    open_race = plan_reconciliation(
        _local(open_local),
        _remote(order_statuses={intent.cloid: _remote_order(intent)}),
    )
    assert "OPEN_STATUS_ABSENT_FROM_OPEN_ORDERS" in {issue.code for issue in open_race.issues}

    replacement_intent = _intent("d" * 64, ordinal=1, price="11")
    original = TestnetOrder(
        intent,
        OrderStatus.CANCELLED,
        venue_order_id="7",
        updated_at=_NOW,
    )
    replacement_order = TestnetOrder(
        replacement_intent,
        OrderStatus.OPEN,
        venue_order_id="7",
        updated_at=_NOW,
    )
    observed = _remote_order(replacement_intent)
    clean = plan_reconciliation(
        _local(original, replacement_order),
        _remote(
            open_orders=(observed,),
            order_statuses={replacement_intent.cloid: observed},
        ),
    )
    assert clean.clean

    ambiguous_fill = RemoteFill(
        fill_id="synthetic-fill-with-stable-id",
        coin="BTC",
        oid="7",
        cloid=None,
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        price=Decimal("11"),
        fee=Decimal("-0.001"),
        timestamp_ms=int(_NOW.timestamp() * 1_000),
    )
    ambiguous = plan_reconciliation(
        _local(original, replacement_order),
        _remote(
            open_orders=(observed,),
            order_statuses={replacement_intent.cloid: observed},
            fills=(ambiguous_fill,),
        ),
    )
    assert "AMBIGUOUS_REMOTE_FILL_REUSED_OID" in {issue.code for issue in ambiguous.issues}


def _activate_store(
    store: TestnetStore,
    facade: RunScopedStore,
    clock: MutableClock,
) -> None:
    facade.acquire_writer_lease()
    facade.set_runtime_state(RuntimeState.STARTING, reason="SYNTHETIC_START")
    captured = int(clock().timestamp() * 1_000) + 1_000
    local = facade.local_snapshot()
    remote = _remote(captured_at_ms=captured)
    plan = plan_reconciliation(local, remote)
    assert plan.clean
    facade.apply_reconciliation(remote, plan)
    clock.advance_ms(1_000)
    facade.set_runtime_state(RuntimeState.RUNNING, reason="SYNTHETIC_RECONCILED")


@pytest.mark.parametrize(
    ("terminal_status", "expected_action_status", "fill_quantity"),
    [
        (OrderStatus.CANCELLED, ActionAttemptStatus.CONFIRMED, Decimal("0.25")),
        (OrderStatus.FILLED, ActionAttemptStatus.REJECTED, Decimal("1")),
        (OrderStatus.EXPIRED, ActionAttemptStatus.REJECTED, Decimal("0.25")),
        (OrderStatus.REJECTED, ActionAttemptStatus.REJECTED, Decimal("0")),
    ],
)
def test_ambiguous_cancel_terminal_truth_resolves_with_atomic_fill_projection(
    tmp_path: Path,
    terminal_status: OrderStatus,
    expected_action_status: ActionAttemptStatus,
    fill_quantity: Decimal,
) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / f"cancel-{terminal_status.value.lower()}.sqlite3",
        lease_root=tmp_path / f"cancel-{terminal_status.value.lower()}-control",
        owner_id=f"synthetic-cancel-{terminal_status.value.lower()}",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)
    try:
        intent = _intent(config.run_id, created_at=clock())
        facade.persist_intent(intent)
        opened = TestnetOrder(
            intent,
            OrderStatus.OPEN,
            venue_order_id="7",
            updated_at=clock(),
        )
        facade.update_order(
            opened,
            audit_kind="SYNTHETIC_OPEN_BEFORE_CANCEL_CRASH",
            audit_payload={"cloid": intent.cloid},
        )
        action_id = deterministic_id(
            "synthetic_phase13_crashed_cancel",
            terminal_status.value,
        )
        facade.prepare_action(
            action_id=action_id,
            kind=ActionKind.CANCEL,
            cloid=intent.cloid,
            replacement_cloid=None,
            minimum_nonce=int(clock().timestamp() * 1_000),
            expires_after_delta_ms=5_000,
        )
        pending = replace(opened, status=OrderStatus.CANCEL_REQUESTED, updated_at=clock())
        facade.update_order(
            pending,
            audit_kind="SYNTHETIC_CANCEL_CRASH_WINDOW",
            audit_payload={"action_id": action_id, "cloid": intent.cloid},
        )

        captured_at_ms = int(clock().timestamp() * 1_000) + 1_000
        terminal = _remote_order(intent, status=terminal_status)
        fills = (
            RemoteFill(
                fill_id=f"synthetic-terminal-{terminal_status.value.lower()}-fill",
                coin="BTC",
                oid="7",
                cloid=intent.cloid,
                side=OrderSide.BUY,
                quantity=fill_quantity,
                price=Decimal("10"),
                fee=Decimal("-0.001"),
                timestamp_ms=captured_at_ms,
            ),
        ) if fill_quantity > 0 else ()
        positions = {"HL:BTC:perp": fill_quantity} if fill_quantity > 0 else {}
        local = facade.local_snapshot()

        identity_mismatch = plan_reconciliation(
            local,
            _remote(
                captured_at_ms=captured_at_ms,
                order_statuses={intent.cloid: replace(terminal, oid="8")},
                fills=fills,
                positions=positions,
            ),
        )
        assert not identity_mismatch.clean
        assert identity_mismatch.action_resolutions == ()

        if fill_quantity > 0:
            missing_fill = plan_reconciliation(
                local,
                _remote(
                    captured_at_ms=captured_at_ms,
                    order_statuses={intent.cloid: terminal},
                    positions=positions,
                ),
            )
            assert not missing_fill.clean
            assert missing_fill.action_resolutions == ()

        remote = _remote(
            captured_at_ms=captured_at_ms,
            order_statuses={intent.cloid: terminal},
            fills=fills,
            positions=positions,
        )
        plan = plan_reconciliation(local, remote)
        assert plan.clean
        assert len(plan.action_resolutions) == 1
        decision = plan.action_resolutions[0]
        assert decision.status is expected_action_status
        assert decision.proof == {
            "captured_at_ms": captured_at_ms,
            "cloid": intent.cloid,
            "code": (
                "AUTHORITATIVE_CANCELLED_STATUS"
                if terminal_status is OrderStatus.CANCELLED
                else "AUTHORITATIVE_CANCEL_TERMINAL_STATUS"
            ),
            "oid": "7",
            "snapshot_hash": plan.snapshot_hash,
            "status": terminal_status.value,
        }

        facade.apply_reconciliation(remote, plan)
        durable_action = store.get_action(config.run_id, action_id)
        assert durable_action.status is expected_action_status
        assert durable_action.response == decision.proof
        durable_order = store.get_order(config.run_id, intent.order_id)
        assert durable_order.status is terminal_status
        assert durable_order.filled_quantity == fill_quantity
        assert store.list_ambiguous_actions(config.run_id) == ()
    finally:
        facade.release_writer_lease()
        store.close()


class EngineAdapter:
    action_ttl_ms = 5_000

    def __init__(self) -> None:
        self.submit_calls = 0
        self.replace_calls = 0
        self.deadman_calls = 0

    def verify_live_constraints(self) -> object:
        return object()

    def read_all_mids(self) -> object:
        return {"BTC": "10", "ETH": "10"}

    def submit_order(
        self,
        intent: TestnetOrderIntent,
        *,
        nonce: int,
        constraint_verification: object,
    ) -> ActionOutcome:
        del intent, nonce, constraint_verification
        self.submit_calls += 1
        return ActionOutcome(OutcomeKind.AMBIGUOUS, "SYNTHETIC_RESPONSE_LOST")

    def cancel_by_cloid(self, *, coin: str, cloid: str, nonce: int) -> ActionOutcome:
        del coin, cloid, nonce
        return ActionOutcome(OutcomeKind.CANCELLED, "SYNTHETIC_CANCELLED")

    def replace_order(
        self,
        *,
        original_cloid: str,
        replacement: TestnetOrderIntent,
        nonce: int,
        constraint_verification: object,
    ) -> ActionOutcome:
        del original_cloid, replacement, nonce, constraint_verification
        self.replace_calls += 1
        return ActionOutcome(OutcomeKind.REPLACED, "SYNTHETIC_REPLACED", venue_order_id="7")

    def schedule_cancel(self, *, cancel_at_ms: int, nonce: int) -> ActionOutcome:
        del cancel_at_ms, nonce
        self.deadman_calls += 1
        return ActionOutcome(OutcomeKind.DEADMAN_ARMED, "SYNTHETIC_DEADMAN_ARMED")


class ImmediateFillAdapter(EngineAdapter):
    def __init__(self, filled_quantity: Decimal) -> None:
        super().__init__()
        self._filled_quantity = filled_quantity

    def _outcome(self) -> ActionOutcome:
        return ActionOutcome(
            OutcomeKind.FILLED,
            "SYNTHETIC_IMMEDIATE_FILL_PROJECTION",
            venue_order_id="7",
            filled_quantity=self._filled_quantity,
            average_fill_price=Decimal("10"),
        )

    def submit_order(
        self,
        intent: TestnetOrderIntent,
        *,
        nonce: int,
        constraint_verification: object,
    ) -> ActionOutcome:
        del intent, nonce, constraint_verification
        self.submit_calls += 1
        return self._outcome()

    def replace_order(
        self,
        *,
        original_cloid: str,
        replacement: TestnetOrderIntent,
        nonce: int,
        constraint_verification: object,
    ) -> ActionOutcome:
        del original_cloid, replacement, nonce, constraint_verification
        self.replace_calls += 1
        return self._outcome()


class SequencedDeadmanAdapter(EngineAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.deadlines: list[int] = []
        self._outcomes = [
            ActionOutcome(OutcomeKind.DEADMAN_ARMED, "SYNTHETIC_DMS_ARMED_1"),
            ActionOutcome(OutcomeKind.REJECTED, "SYNTHETIC_DMS_REJECTED_2"),
            ActionOutcome(OutcomeKind.DEADMAN_ARMED, "SYNTHETIC_DMS_ARMED_3"),
        ]

    def schedule_cancel(self, *, cancel_at_ms: int, nonce: int) -> ActionOutcome:
        del nonce
        self.deadman_calls += 1
        self.deadlines.append(cancel_at_ms)
        return self._outcomes.pop(0)


class RejectingDeadmanAdapter(EngineAdapter):
    def schedule_cancel(self, *, cancel_at_ms: int, nonce: int) -> ActionOutcome:
        del cancel_at_ms, nonce
        self.deadman_calls += 1
        return ActionOutcome(OutcomeKind.REJECTED, "SYNTHETIC_DMS_REJECTED")


class NoopReconciler:
    def reconcile(self, *, captured_at_ms: int) -> object:
        return captured_at_ms


class KillBeforePermitStore:
    def __init__(
        self,
        facade: RunScopedStore,
        killer: TestnetStore,
        *,
        run_id: str,
        clock: MutableClock,
    ) -> None:
        self._facade = facade
        self._killer = killer
        self._run_id = run_id
        self._clock = clock
        self.triggered = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self._facade, name)

    def final_send_permit(self, action_id: str) -> object:
        self.triggered += 1
        self._killer.set_runtime_state(
            self._run_id,
            RuntimeState.KILLED,
            reason="SYNTHETIC_KILL_BEFORE_SEND",
            at=self._clock(),
        )
        return self._facade.final_send_permit(action_id)


class ClockAdvancingConstraintAdapter(EngineAdapter):
    def __init__(self, clock: MutableClock, *, meta_delay_ms: int) -> None:
        super().__init__()
        self._clock = clock
        self._meta_delay_ms = meta_delay_ms
        self.events: list[str] = []

    def verify_live_constraints(self) -> object:
        self.events.append("meta")
        self._clock.advance_ms(self._meta_delay_ms)
        return object()

    def read_all_mids(self) -> object:
        self.events.append("marks")
        return super().read_all_mids()

    def submit_order(
        self,
        intent: TestnetOrderIntent,
        *,
        nonce: int,
        constraint_verification: object,
    ) -> ActionOutcome:
        del intent, nonce, constraint_verification
        self.events.append("exchange")
        self.submit_calls += 1
        return ActionOutcome(OutcomeKind.RESTING, "SYNTHETIC_RESTING", venue_order_id="7")

    def replace_order(
        self,
        *,
        original_cloid: str,
        replacement: TestnetOrderIntent,
        nonce: int,
        constraint_verification: object,
    ) -> ActionOutcome:
        del original_cloid, replacement, nonce, constraint_verification
        self.events.append("exchange")
        self.replace_calls += 1
        return ActionOutcome(OutcomeKind.REPLACED, "SYNTHETIC_REPLACED", venue_order_id="7")


class AdvanceBeforePermitStore:
    def __init__(
        self,
        facade: RunScopedStore,
        clock: MutableClock,
        *,
        delay_ms: int,
    ) -> None:
        self._facade = facade
        self._clock = clock
        self._delay_ms = delay_ms
        self.triggered = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self._facade, name)

    def final_send_permit(self, action_id: str) -> object:
        self.triggered += 1
        self._clock.advance_ms(self._delay_ms)
        return self._facade.final_send_permit(action_id)


class EmptyRemoteAdapter:
    def __init__(self) -> None:
        self.open_reads = 0

    def read_open_orders(self) -> object:
        self.open_reads += 1
        return []

    def read_user_fills_by_time(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> object:
        del start_time_ms, end_time_ms
        return []

    def read_clearinghouse_state(self) -> object:
        return {
            "assetPositions": [],
            "marginSummary": {"accountValue": "100"},
            "withdrawable": "100",
        }

    def read_spot_clearinghouse_state(self) -> object:
        return {"balances": []}

    def read_order_status(self, cloid: str) -> object:
        del cloid
        return {"status": "unknownOid"}


@pytest.mark.parametrize("operation", ("submit", "replace"))
@pytest.mark.parametrize(
    ("meta_delay_ms", "expected_send"),
    (
        (5_001, True),
        (10_001, False),
    ),
)
def test_live_meta_refresh_precedes_marks_at_both_freshness_thresholds(
    tmp_path: Path,
    operation: str,
    meta_delay_ms: int,
    expected_send: bool,
) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / f"meta-order-{operation}-{meta_delay_ms}.sqlite3",
        lease_root=tmp_path / f"meta-order-{operation}-{meta_delay_ms}-control",
        owner_id=f"synthetic-meta-order-{operation}-{meta_delay_ms}",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)
    adapter = ClockAdvancingConstraintAdapter(clock, meta_delay_ms=meta_delay_ms)
    engine = TestnetExecutionEngine(
        adapter=adapter,
        store=facade,
        limits=config.risk_limits,
        clock=clock,
        reconciler=NoopReconciler(),
    )
    original_intent: TestnetOrderIntent | None = None
    if operation == "replace":
        original_intent = _intent(config.run_id, ordinal=10, created_at=clock())
        original = facade.persist_intent(original_intent)
        store.transition_order(
            config.run_id,
            original.intent.order_id,
            OrderStatus.OPEN,
            at=clock(),
            venue_order_id="7",
        )
    target = _intent(config.run_id, ordinal=11, created_at=clock())

    if expected_send:
        if original_intent is None:
            result = engine.submit(target, market_received_at=clock())
        else:
            result = engine.replace(
                original_cloid=original_intent.cloid,
                replacement=target,
                market_received_at=clock(),
            )
        assert result.order is not None
        assert result.order.status is OrderStatus.OPEN
    else:
        with pytest.raises(ExecutionError):
            if original_intent is None:
                engine.submit(target, market_received_at=clock())
            else:
                engine.replace(
                    original_cloid=original_intent.cloid,
                    replacement=target,
                    market_received_at=clock(),
                )

    assert adapter.events == (
        ["meta", "marks", "exchange"] if expected_send else ["meta", "marks"]
    )
    assert adapter.submit_calls + adapter.replace_calls == int(expected_send)
    durable_target = store.get_order(config.run_id, target.order_id)
    assert durable_target.status is (
        OrderStatus.OPEN if expected_send else OrderStatus.INVALID
    )
    facade.release_writer_lease()
    store.close()


@pytest.mark.parametrize("operation", ("submit", "replace"))
@pytest.mark.parametrize(
    ("stale_source", "reconciliation_pre_age_ms", "permit_delay_ms"),
    (
        ("market", 0, 5_001),
        ("reconciliation", 9_001, 1_000),
    ),
)
def test_final_send_permit_rechecks_freshness_before_exchange_post(
    tmp_path: Path,
    operation: str,
    stale_source: str,
    reconciliation_pre_age_ms: int,
    permit_delay_ms: int,
) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / f"final-fresh-{operation}-{stale_source}.sqlite3",
        lease_root=tmp_path / f"final-fresh-{operation}-{stale_source}-control",
        owner_id=f"synthetic-final-fresh-{operation}-{stale_source}",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)
    clock.advance_ms(reconciliation_pre_age_ms)
    delayed_store = AdvanceBeforePermitStore(
        facade,
        clock,
        delay_ms=permit_delay_ms,
    )
    adapter = ClockAdvancingConstraintAdapter(clock, meta_delay_ms=0)
    engine = TestnetExecutionEngine(
        adapter=adapter,
        store=delayed_store,  # type: ignore[arg-type]
        limits=config.risk_limits,
        clock=clock,
        reconciler=NoopReconciler(),
    )
    original_intent: TestnetOrderIntent | None = None
    if operation == "replace":
        original_intent = _intent(config.run_id, ordinal=20, created_at=clock())
        original = facade.persist_intent(original_intent)
        store.transition_order(
            config.run_id,
            original.intent.order_id,
            OrderStatus.OPEN,
            at=clock(),
            venue_order_id="7",
        )
    target = _intent(config.run_id, ordinal=21, created_at=clock())
    if original_intent is None:
        action_id = deterministic_id(
            "hyperliquid_testnet_action_v1",
            ActionKind.SUBMIT.value,
            target.cloid,
            None,
            None,
        )
    else:
        action_id = deterministic_id(
            "hyperliquid_testnet_action_v1",
            ActionKind.REPLACE.value,
            original_intent.cloid,
            target.cloid,
            None,
        )

    with pytest.raises(ExecutionError):
        if original_intent is None:
            engine.submit(target, market_received_at=clock())
        else:
            engine.replace(
                original_cloid=original_intent.cloid,
                replacement=target,
                market_received_at=clock(),
            )

    assert delayed_store.triggered == 1
    assert adapter.events == ["meta", "marks"]
    assert adapter.submit_calls + adapter.replace_calls == 0
    assert store.get_action(config.run_id, action_id).status is (
        ActionAttemptStatus.RESOLVED_NOT_SENT
    )
    assert store.get_order(config.run_id, target.order_id).status is OrderStatus.INVALID
    assert store.get_run(config.run_id).runtime_state is RuntimeState.PAUSED
    facade.release_writer_lease()
    store.close()


def test_real_store_lost_submit_is_not_resent_and_absence_remains_manual(
    tmp_path: Path,
) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / "executor.sqlite3",
        lease_root=tmp_path / "control",
        owner_id="synthetic-owner-a",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)
    baseline_cursor = facade.local_snapshot().fill_cursor_ms

    adapter = EngineAdapter()
    engine = TestnetExecutionEngine(
        adapter=adapter,
        store=facade,
        limits=config.risk_limits,
        clock=clock,
        reconciler=NoopReconciler(),
    )
    intent = _intent(config.run_id, created_at=clock())
    result = engine.submit(
        intent,
        market_received_at=clock() + timedelta(days=365),
    )
    assert result.action.status is ActionAttemptStatus.AMBIGUOUS
    durable = store.get_action(config.run_id, result.action.action_id)
    assert durable.payload["expires_after_ms"] == durable.nonce + adapter.action_ttl_ms
    assert durable.response is not None
    assert durable.response["code"] == "SYNTHETIC_RESPONSE_LOST"
    assert store.get_order(config.run_id, intent.order_id).status is OrderStatus.UNKNOWN
    assert store.get_run(config.run_id).runtime_state is RuntimeState.PAUSED

    with pytest.raises(ActionRequiresReconciliation):
        engine.submit(intent, market_received_at=clock() - timedelta(days=365))
    assert adapter.submit_calls == 1

    clock.advance_ms(adapter.action_ttl_ms + 1)
    remote_adapter = EmptyRemoteAdapter()
    reconciler = ExchangeFirstReconciler(remote_adapter, facade)
    plan = reconciler.reconcile(captured_at_ms=int(clock().timestamp() * 1_000))
    assert not plan.clean
    assert plan.action_resolutions == ()
    assert remote_adapter.open_reads == 1
    assert len(store.list_ambiguous_actions(config.run_id)) == 1
    assert store.get_order(config.run_id, intent.order_id).status is OrderStatus.UNKNOWN
    assert store.get_run(config.run_id).runtime_state is RuntimeState.MANUAL_REVIEW
    assert facade.local_snapshot().fill_cursor_ms == baseline_cursor
    facade.release_writer_lease()
    store.close()


def test_wall_clock_jump_cannot_prove_absence_and_delayed_apply_wins() -> None:
    run_id = "d" * 64
    intent = _intent(run_id)
    local_order = TestnetOrder(
        intent,
        OrderStatus.UNKNOWN,
        updated_at=_NOW,
    )
    action = ActionRecord(
        action_id="f" * 64,
        kind=ActionKind.SUBMIT,
        cloid=intent.cloid,
        replacement_cloid=None,
        nonce=int(_NOW.timestamp() * 1_000),
        expires_after_ms=int(_NOW.timestamp() * 1_000) + 5_000,
    )
    local = _local(local_order, ambiguous_actions=(action,))
    jumped = _remote(
        captured_at_ms=int((_NOW + timedelta(days=365)).timestamp() * 1_000),
        order_statuses={intent.cloid: None},
    )
    premature = plan_reconciliation(local, jumped)
    assert not premature.clean
    assert premature.action_resolutions == ()
    assert "AMBIGUOUS_SUBMIT_UNRESOLVED" in {issue.code for issue in premature.issues}

    appeared_order = _remote_order(intent)
    appeared = _remote(
        captured_at_ms=jumped.captured_at_ms + 1,
        open_orders=(appeared_order,),
        order_statuses={intent.cloid: appeared_order},
    )
    recovered = plan_reconciliation(local, appeared)
    assert recovered.clean
    assert recovered.action_resolutions[0].status is ActionAttemptStatus.CONFIRMED


def test_discrepant_reconciliation_is_atomic_complete_and_idempotent(
    tmp_path: Path,
) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / "reconciliation-failure.sqlite3",
        lease_root=tmp_path / "reconciliation-failure-control",
        owner_id="synthetic-reconciliation-failure",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)

    unknown_intent = _intent(config.run_id, ordinal=9, created_at=clock())
    open_order = _remote_order(unknown_intent, oid="99")
    captured_at_ms = int(clock().timestamp() * 1_000) + 1_000
    fill = RemoteFill(
        fill_id="synthetic-unowned-stable-tid",
        coin="BTC",
        oid="99",
        cloid=unknown_intent.cloid,
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        price=Decimal("10"),
        fee=Decimal("-0.001"),
        timestamp_ms=captured_at_ms,
    )
    remote = _remote(
        captured_at_ms=captured_at_ms,
        open_orders=(open_order,),
        order_statuses={
            unknown_intent.cloid: replace(
                open_order,
                status=OrderStatus.CANCELLED,
                remaining_quantity=Decimal(0),
            )
        },
        fills=(fill,),
        positions={"HL:BTC:perp": Decimal("0.1")},
    )
    plan = plan_reconciliation(facade.local_snapshot(), remote)
    assert plan.issues
    facade.apply_reconciliation(remote, plan)

    assert store.get_run(config.run_id).runtime_state is RuntimeState.MANUAL_REVIEW
    snapshot = store.latest_remote_snapshot(config.run_id)
    assert snapshot is not None
    assert snapshot.reconciled is False
    assert snapshot.payload["source_cursor"] == f"fills-ms:{captured_at_ms}"
    observed_orders = snapshot.payload["open_orders"]
    assert isinstance(observed_orders, list)
    terminal = next(
        item
        for item in observed_orders
        if isinstance(item, dict) and item.get("observation_kind") == "ORDER_STATUS"
    )
    assert terminal["coin"] == "BTC"
    assert terminal["limit_price"] == "10"
    assert terminal["original_quantity"] == "1"
    observations = snapshot.payload["reconciliation_observations"]
    assert isinstance(observations, dict)
    observed_fills = observations["fills"]
    assert isinstance(observed_fills, list)
    assert observed_fills[0]["fill_id"] == fill.fill_id
    assert observed_fills[0]["fee"] == "-0.001"
    assert observed_fills[0]["payload"]["ownership"] == "UNOWNED"
    before = tuple(
        event
        for event in store.get_audit_events(config.run_id)
        if event.event_type == "RECONCILIATION_FAILURE_APPLIED"
    )
    assert len(before) == 1
    facade.apply_reconciliation(remote, plan)
    after = tuple(
        event
        for event in store.get_audit_events(config.run_id)
        if event.event_type == "RECONCILIATION_FAILURE_APPLIED"
    )
    assert after == before
    facade.release_writer_lease()
    store.close()

    rollback_store = TestnetStore(
        tmp_path / "reconciliation-failure-rollback.sqlite3",
        lease_root=tmp_path / "reconciliation-failure-rollback-control",
        owner_id="synthetic-reconciliation-failure-rollback",
    )
    rollback_store.create_run(config, created_at=clock())
    rollback_facade = RunScopedStore(
        rollback_store,
        run_id=config.run_id,
        clock=clock,
    )
    _activate_store(rollback_store, rollback_facade, clock)
    baseline_snapshot = rollback_store.latest_remote_snapshot(config.run_id)
    assert baseline_snapshot is not None

    def fail_after_latch(stage: str) -> None:
        if stage == "after_runtime_latch":
            raise RuntimeError("synthetic reconciliation transaction fault")

    rollback_store._reconciliation_failure_fault_point = fail_after_latch  # type: ignore[method-assign]
    rollback_plan = plan_reconciliation(rollback_facade.local_snapshot(), remote)
    with pytest.raises(RuntimeError, match="transaction fault"):
        rollback_facade.apply_reconciliation(remote, rollback_plan)
    assert rollback_store.latest_remote_snapshot(config.run_id) == baseline_snapshot
    assert rollback_store.get_run(config.run_id).runtime_state is RuntimeState.RUNNING
    assert not any(
        event.event_type == "RECONCILIATION_FAILURE_APPLIED"
        for event in rollback_store.get_audit_events(config.run_id)
    )
    rollback_facade.release_writer_lease()
    rollback_store.close()


@pytest.mark.parametrize(
    ("fill_quantity", "terminal", "expected_status"),
    [
        (Decimal("0.4"), False, OrderStatus.PARTIALLY_FILLED),
        (Decimal("1"), True, OrderStatus.FILLED),
    ],
)
def test_real_store_partial_and_full_fills_project_once_after_stable_ledger(
    tmp_path: Path,
    fill_quantity: Decimal,
    terminal: bool,
    expected_status: OrderStatus,
) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / f"fill-{expected_status.value}.sqlite3",
        lease_root=tmp_path / f"fill-{expected_status.value}-control",
        owner_id=f"synthetic-fill-{expected_status.value}",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)
    intent = _intent(config.run_id, created_at=clock())
    facade.persist_intent(intent)
    for status in (
        OrderStatus.SUBMITTED,
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.OPEN,
    ):
        facade.update_order(
            TestnetOrder(
                intent,
                status,
                venue_order_id="7" if status is not OrderStatus.SUBMITTED else None,
                updated_at=clock(),
            ),
            audit_kind="SYNTHETIC_ACTIVE_ORDER",
            audit_payload={"status": status.value},
        )

    captured_at_ms = int(clock().timestamp() * 1_000) + 1_000
    fill = RemoteFill(
        fill_id=f"synthetic-{expected_status.value.lower()}-tid",
        coin="BTC",
        oid="7",
        cloid=intent.cloid,
        side=OrderSide.BUY,
        quantity=fill_quantity,
        price=Decimal("10"),
        fee=Decimal("-0.001"),
        timestamp_ms=captured_at_ms,
    )
    if terminal:
        terminal_order = _remote_order(
            intent,
            status=OrderStatus.FILLED,
        )
        open_orders: tuple[RemoteOrder, ...] = ()
        statuses = {intent.cloid: terminal_order}
    else:
        partial_order = replace(
            _remote_order(intent),
            remaining_quantity=intent.quantity - fill_quantity,
        )
        open_orders = (partial_order,)
        statuses = {intent.cloid: partial_order}
    remote = _remote(
        captured_at_ms=captured_at_ms,
        open_orders=open_orders,
        order_statuses=statuses,
        fills=(fill,),
        positions={"HL:BTC:perp": fill_quantity},
    )
    plan = plan_reconciliation(facade.local_snapshot(), remote)
    assert plan.clean
    facade.apply_reconciliation(remote, plan)
    durable = store.get_order(config.run_id, intent.order_id)
    assert durable.status is expected_status
    assert durable.filled_quantity == fill_quantity
    assert durable.average_fill_price == Decimal("10")
    assert len(store.list_fills(config.run_id)) == 1
    assert facade.execution_snapshot().positions == {
        "HL:BTC:perp": fill_quantity,
    }
    facade.apply_reconciliation(remote, plan)
    assert len(store.list_fills(config.run_id)) == 1
    facade.release_writer_lease()
    store.close()


@pytest.mark.parametrize("operation", ["submit", "replace"])
@pytest.mark.parametrize("immediate_quantity", [Decimal("0.4"), Decimal("1")])
def test_immediate_submit_and_replace_projection_does_not_double_split_rest_fills(
    tmp_path: Path,
    operation: str,
    immediate_quantity: Decimal,
) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / f"immediate-{operation}-{immediate_quantity}.sqlite3",
        lease_root=tmp_path / f"immediate-{operation}-{immediate_quantity}-control",
        owner_id=f"synthetic-immediate-{operation}-{immediate_quantity}",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)
    adapter = ImmediateFillAdapter(immediate_quantity)
    engine = TestnetExecutionEngine(
        adapter=adapter,
        store=facade,
        limits=config.risk_limits,
        clock=clock,
        reconciler=NoopReconciler(),
    )
    if operation == "replace":
        original_intent = _intent(config.run_id, created_at=clock())
        facade.persist_intent(original_intent)
        for status in (
            OrderStatus.SUBMITTED,
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.OPEN,
        ):
            facade.update_order(
                TestnetOrder(
                    original_intent,
                    status,
                    venue_order_id=("7" if status is not OrderStatus.SUBMITTED else None),
                    updated_at=clock(),
                ),
                audit_kind="SYNTHETIC_REPLACE_ORIGINAL",
                audit_payload={"status": status.value},
            )
        intent = _intent(config.run_id, ordinal=1, created_at=clock())
        result = engine.replace(
            original_cloid=original_intent.cloid,
            replacement=intent,
            market_received_at=clock(),
        )
    else:
        intent = _intent(config.run_id, created_at=clock())
        result = engine.submit(intent, market_received_at=clock())
    assert result.order.filled_quantity == immediate_quantity
    assert store.list_fills(config.run_id) == ()

    piece = immediate_quantity / 2
    for ordinal, cumulative_position in (
        (1, piece),
        (2, immediate_quantity),
    ):
        captured_at_ms = int(clock().timestamp() * 1_000) + ordinal * 1_000
        fill = RemoteFill(
            fill_id=f"synthetic-{operation}-{immediate_quantity}-{ordinal}-tid",
            coin="BTC",
            oid="7",
            cloid=intent.cloid,
            side=OrderSide.BUY,
            quantity=piece,
            price=Decimal("10"),
            fee=Decimal("-0.001"),
            timestamp_ms=captured_at_ms,
        )
        if immediate_quantity == intent.quantity:
            terminal_order = _remote_order(intent, status=OrderStatus.FILLED)
            open_orders: tuple[RemoteOrder, ...] = ()
            statuses = {intent.cloid: terminal_order}
        else:
            partial_order = replace(
                _remote_order(intent),
                remaining_quantity=intent.quantity - immediate_quantity,
            )
            open_orders = (partial_order,)
            statuses = {intent.cloid: partial_order}
        remote = _remote(
            captured_at_ms=captured_at_ms,
            open_orders=open_orders,
            order_statuses=statuses,
            fills=(fill,),
            positions={"HL:BTC:perp": cumulative_position},
        )
        plan = plan_reconciliation(facade.local_snapshot(), remote)
        assert plan.clean
        facade.apply_reconciliation(remote, plan)
        durable = store.get_order(config.run_id, intent.order_id)
        assert durable.filled_quantity == immediate_quantity
        assert durable.average_fill_price == Decimal("10")

    assert (
        sum(
            (fill.quantity for fill in store.list_fills(config.run_id)),
            start=Decimal(0),
        )
        == immediate_quantity
    )
    facade.apply_reconciliation(remote, plan)
    assert len(store.list_fills(config.run_id)) == 2
    facade.release_writer_lease()
    store.close()


def test_replace_prevalidation_leaves_no_phantom_reserving_order(tmp_path: Path) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / "replace.sqlite3",
        lease_root=tmp_path / "replace-control",
        owner_id="synthetic-owner-replace",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)
    adapter = EngineAdapter()
    engine = TestnetExecutionEngine(
        adapter=adapter,
        store=facade,
        limits=config.risk_limits,
        clock=clock,
        reconciler=NoopReconciler(),
    )

    missing_replacement = _intent(config.run_id, ordinal=1, created_at=clock())
    with pytest.raises(ExecutionError):
        engine.replace(
            original_cloid="0x" + "f" * 32,
            replacement=missing_replacement,
            market_received_at=clock(),
        )
    assert facade.get_order(missing_replacement.cloid) is None

    original_intent = _intent(config.run_id, ordinal=2, created_at=clock())
    original = facade.persist_intent(original_intent)
    store.transition_order(
        config.run_id,
        original.intent.order_id,
        OrderStatus.OPEN,
        at=clock(),
        venue_order_id="7",
    )
    wrong_instrument = _intent(
        config.run_id,
        ordinal=3,
        coin="ETH",
        created_at=clock(),
    )
    with pytest.raises(ExecutionError):
        engine.replace(
            original_cloid=original_intent.cloid,
            replacement=wrong_instrument,
            market_received_at=clock(),
        )
    assert facade.get_order(wrong_instrument.cloid) is None

    store.transition_order(
        config.run_id,
        original.intent.order_id,
        OrderStatus.CANCELLED,
        at=clock(),
    )
    terminal_replacement = _intent(config.run_id, ordinal=4, created_at=clock())
    with pytest.raises(ExecutionError):
        engine.replace(
            original_cloid=original_intent.cloid,
            replacement=terminal_replacement,
            market_received_at=clock(),
        )
    assert facade.get_order(terminal_replacement.cloid) is None
    facade.release_writer_lease()
    store.close()


@pytest.mark.parametrize("operation", ["submit", "replace"])
def test_cross_store_kill_wins_after_reservation_and_prevents_post(
    tmp_path: Path,
    operation: str,
) -> None:
    config = _config()
    clock = MutableClock()
    control = tmp_path / f"{operation}-race-control"
    owner = TestnetStore(
        tmp_path / f"{operation}-owner.sqlite3",
        lease_root=control,
        owner_id=f"synthetic-{operation}-owner",
    )
    killer = TestnetStore(
        tmp_path / f"{operation}-killer.sqlite3",
        lease_root=control,
        owner_id=f"synthetic-{operation}-killer",
    )
    owner.create_run(config, created_at=clock())
    killer.create_run(config, created_at=clock())
    facade = RunScopedStore(owner, run_id=config.run_id, clock=clock)
    _activate_store(owner, facade, clock)
    race_store = KillBeforePermitStore(
        facade,
        killer,
        run_id=config.run_id,
        clock=clock,
    )
    adapter = EngineAdapter()
    engine = TestnetExecutionEngine(
        adapter=adapter,
        store=race_store,  # type: ignore[arg-type]
        limits=config.risk_limits,
        clock=clock,
        reconciler=NoopReconciler(),
    )

    if operation == "submit":
        target = _intent(config.run_id, created_at=clock())
        with pytest.raises(ExecutionError):
            engine.submit(target, market_received_at=clock())
        assert adapter.submit_calls == 0
    else:
        original_intent = _intent(config.run_id, ordinal=1, created_at=clock())
        original = facade.persist_intent(original_intent)
        owner.transition_order(
            config.run_id,
            original.intent.order_id,
            OrderStatus.OPEN,
            at=clock(),
            venue_order_id="7",
        )
        target = _intent(config.run_id, ordinal=2, price="11", created_at=clock())
        with pytest.raises(ExecutionError):
            engine.replace(
                original_cloid=original_intent.cloid,
                replacement=target,
                market_received_at=clock(),
            )
        assert adapter.replace_calls == 0

    assert race_store.triggered == 1
    assert owner.get_order(config.run_id, target.order_id).status is OrderStatus.INVALID
    assert owner.list_ambiguous_actions(config.run_id) == ()
    assert owner.get_run(config.run_id).runtime_state is RuntimeState.KILLED
    assert owner.account_kill_latched(config.run_id)
    facade.release_writer_lease()
    owner.close()
    killer.close()


class FakeRawSocket:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeTlsSocket(FakeRawSocket):
    pass


class FakeSslContext:
    def __init__(self, tls_socket: FakeTlsSocket) -> None:
        self.tls_socket = tls_socket
        self.server_hostname: str | None = None

    def wrap_socket(
        self,
        raw_socket: FakeRawSocket,
        *,
        server_hostname: str,
    ) -> FakeTlsSocket:
        del raw_socket
        self.server_hostname = server_hostname
        return self.tls_socket


def test_websocket_connector_uses_exact_direct_tls_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import websocket

    import hyperlab_testnet.runtime as runtime_module

    raw_socket = FakeRawSocket()
    tls_socket = FakeTlsSocket()
    context = FakeSslContext(tls_socket)
    connected: list[tuple[tuple[str, int], float]] = []
    websocket_kwargs: dict[str, object] = {}

    def create_connection(
        address: tuple[str, int],
        *,
        timeout: float,
    ) -> FakeRawSocket:
        connected.append((address, timeout))
        return raw_socket

    def websocket_create(url: str, **kwargs: object) -> object:
        websocket_kwargs.update(kwargs)
        return SimpleNamespace(
            handshake_response=SimpleNamespace(status=101),
            close=lambda: None,
        )

    monkeypatch.setenv("HTTPS_PROXY", "http://poison.invalid:8888")
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setattr(runtime_module.socket, "create_connection", create_connection)
    monkeypatch.setattr(runtime_module.ssl, "create_default_context", lambda: context)
    monkeypatch.setattr(websocket, "create_connection", websocket_create)

    connection = WebsocketClientConnector().connect(
        url=TESTNET_WEBSOCKET_URL,
        timeout_seconds=3,
    )
    assert connection.handshake_status == 101
    assert connected == [(("api.hyperliquid-testnet.xyz", 443), 3)]
    assert context.server_hostname == "api.hyperliquid-testnet.xyz"
    assert websocket_kwargs["socket"] is tls_socket
    assert websocket_kwargs["redirect_limit"] == 0
    assert "http_proxy_host" not in websocket_kwargs


class AckConnection:
    handshake_status = 101

    def __init__(self, frames: list[object]) -> None:
        self.frames = list(frames)
        self.sent: list[str] = []
        self.closed = False

    def send(self, payload: str) -> object:
        self.sent.append(payload)
        return None

    def recv(self) -> object:
        return self.frames.pop(0)

    def close(self) -> object:
        self.closed = True
        return None


class AckConnector:
    def __init__(self, connection: AckConnection) -> None:
        self.connection = connection

    def connect(self, *, url: str, timeout_seconds: float) -> AckConnection:
        assert url == TESTNET_WEBSOCKET_URL
        assert timeout_seconds == 2
        return self.connection


def _ack(subscription_type: str, *, user: str = _ACCOUNT) -> str:
    return json.dumps(
        {
            "channel": "subscriptionResponse",
            "data": {
                "method": "subscribe",
                "subscription": {"type": subscription_type, "user": user},
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def test_user_stream_requires_both_exact_scoped_subscription_acks() -> None:
    connection = AckConnection([_ack("orderUpdates"), _ack("userFills")])
    source = TestnetUserEventSource(
        endpoint=TESTNET_WEBSOCKET_URL,
        account_address=_ACCOUNT,
        connector=AckConnector(connection),
        timeout_seconds=2,
    )
    source.connect()
    assert len(connection.sent) == 2
    source.close()

    wrong_scope = AckConnection([_ack("orderUpdates", user="0x" + "3" * 40), _ack("userFills")])
    refused = TestnetUserEventSource(
        endpoint=TESTNET_WEBSOCKET_URL,
        account_address=_ACCOUNT,
        connector=AckConnector(wrong_scope),
        timeout_seconds=2,
    )
    with pytest.raises(EventSourceError, match="scope differs"):
        refused.connect()
    assert wrong_scope.closed

    duplicate = AckConnection([_ack("orderUpdates"), _ack("orderUpdates")])
    duplicate_source = TestnetUserEventSource(
        endpoint=TESTNET_WEBSOCKET_URL,
        account_address=_ACCOUNT,
        connector=AckConnector(duplicate),
        timeout_seconds=2,
    )
    with pytest.raises(EventSourceError, match="scope differs"):
        duplicate_source.connect()
    assert duplicate.closed

    explicit_error = AckConnection(['{"channel":"error","data":{"message":"synthetic refusal"}}'])
    error_source = TestnetUserEventSource(
        endpoint=TESTNET_WEBSOCKET_URL,
        account_address=_ACCOUNT,
        connector=AckConnector(explicit_error),
        timeout_seconds=2,
    )
    with pytest.raises(EventSourceError, match="acknowledgement is invalid"):
        error_source.connect()
    assert explicit_error.closed

    oversize = AckConnection([b"x" * (4 * 1024 * 1024 + 1)])
    oversized_source = TestnetUserEventSource(
        endpoint=TESTNET_WEBSOCKET_URL,
        account_address=_ACCOUNT,
        connector=AckConnector(oversize),
        timeout_seconds=2,
    )
    with pytest.raises(EventSourceDisconnected):
        oversized_source.connect()
    assert oversize.closed


def test_ambiguous_native_replace_recovers_lineage_and_rejects_no_cloid_fill() -> None:
    run_id = "d" * 64
    original_intent = _intent(run_id)
    replacement_intent = _intent(run_id, ordinal=1, price="11")
    original = TestnetOrder(
        original_intent,
        OrderStatus.UNKNOWN,
        venue_order_id="7",
        updated_at=_NOW,
    )
    replacement_order = TestnetOrder(
        replacement_intent,
        OrderStatus.UNKNOWN,
        updated_at=_NOW,
    )
    action = ActionRecord(
        action_id="e" * 64,
        kind=ActionKind.REPLACE,
        cloid=original_intent.cloid,
        replacement_cloid=replacement_intent.cloid,
        nonce=int(_NOW.timestamp() * 1_000),
        expires_after_ms=int(_NOW.timestamp() * 1_000) + 5_000,
    )
    observed = _remote_order(replacement_intent, oid="7")
    remote = _remote(
        open_orders=(observed,),
        order_statuses={
            original_intent.cloid: None,
            replacement_intent.cloid: observed,
        },
    )
    recovered = plan_reconciliation(
        _local(original, replacement_order, ambiguous_actions=(action,)),
        remote,
    )
    assert recovered.clean
    assert recovered.action_resolutions[0].status is ActionAttemptStatus.CONFIRMED

    rejected_replacement = replace(
        observed,
        status=OrderStatus.REJECTED,
        remaining_quantity=Decimal(0),
    )
    rejected_without_original = plan_reconciliation(
        _local(original, replacement_order, ambiguous_actions=(action,)),
        _remote(
            order_statuses={
                original_intent.cloid: None,
                replacement_intent.cloid: rejected_replacement,
            }
        ),
    )
    assert "REJECTED_REPLACE_ORIGINAL_NOT_OPEN" in {issue.code for issue in rejected_without_original.issues}
    original_open = _remote_order(original_intent, oid="7")
    rejected_with_original = plan_reconciliation(
        _local(original, replacement_order, ambiguous_actions=(action,)),
        _remote(
            open_orders=(original_open,),
            order_statuses={
                original_intent.cloid: original_open,
                replacement_intent.cloid: rejected_replacement,
            },
        ),
    )
    assert rejected_with_original.clean
    assert rejected_with_original.action_resolutions[0].status is ActionAttemptStatus.REJECTED

    no_cloid_fill = RemoteFill(
        fill_id="synthetic-amend-fill",
        coin="BTC",
        oid="7",
        cloid=None,
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        price=Decimal("11"),
        fee=Decimal("0"),
        timestamp_ms=remote.captured_at_ms,
    )
    blocked = plan_reconciliation(
        _local(original, replacement_order, ambiguous_actions=(action,)),
        replace(remote, fills=(no_cloid_fill,), positions={"HL:BTC:perp": Decimal("0.1")}),
    )
    assert "AMBIGUOUS_REMOTE_FILL_DURING_REPLACE" in {issue.code for issue in blocked.issues}


class RuntimeEventSource:
    endpoint = TESTNET_WEBSOCKET_URL

    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1

    def connect(self) -> None:
        return

    def poll(self) -> object:
        raise AssertionError("external kill must be handled before WebSocket polling")


def test_cross_database_account_kill_forces_owner_deadman_and_releases_lease(
    tmp_path: Path,
) -> None:
    config = _config()
    clock = MutableClock()
    control = tmp_path / "global-control"
    store_a = TestnetStore(
        tmp_path / "owner.sqlite3",
        lease_root=control,
        owner_id="synthetic-running-owner",
    )
    store_b = TestnetStore(
        tmp_path / "killer.sqlite3",
        lease_root=control,
        owner_id="synthetic-kill-requester",
    )
    store_a.create_run(config, created_at=clock())
    store_b.create_run(config, created_at=clock())
    facade_a = RunScopedStore(store_a, run_id=config.run_id, clock=clock)
    _activate_store(store_a, facade_a, clock)
    adapter = EngineAdapter()
    engine = TestnetExecutionEngine(
        adapter=adapter,
        store=facade_a,
        limits=config.risk_limits,
        clock=clock,
        reconciler=NoopReconciler(),
    )
    source = RuntimeEventSource()
    runtime = TestnetRuntime(
        config=config,
        store=facade_a,
        preflight=SimpleNamespace(),
        reconciler=SimpleNamespace(),
        engine=engine,
        event_source=source,
        clock=clock,
    )

    store_b.set_runtime_state(
        config.run_id,
        RuntimeState.KILLED,
        reason="SYNTHETIC_EXTERNAL_KILL",
        at=clock(),
    )
    assert facade_a.account_kill_latched()
    with pytest.raises(RuntimeErrorClosed, match="external kill"):
        runtime.poll_once()
    assert adapter.deadman_calls == 1
    assert store_a.get_run(config.run_id).runtime_state is RuntimeState.KILLED
    assert store_a.wallet_lease(config.run_id) is None
    assert source.closed == 1
    store_a.close()
    store_b.close()


def test_emergency_deadman_is_deadline_scoped_and_rearms_after_rejection(
    tmp_path: Path,
) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / "dms-generations.sqlite3",
        lease_root=tmp_path / "dms-generations-control",
        owner_id="synthetic-dms-generations",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)
    adapter = SequencedDeadmanAdapter()
    engine = TestnetExecutionEngine(
        adapter=adapter,
        store=facade,
        limits=config.risk_limits,
        clock=clock,
        reconciler=NoopReconciler(),
    )
    base = int(clock().timestamp() * 1_000)
    first = engine.kill(cancel_at_ms=base + 30_000)
    assert first.outcome is not None
    assert first.outcome.kind is OutcomeKind.DEADMAN_ARMED
    concurrent_same = engine.enforce_persisted_kill(cancel_at_ms=base + 30_000)
    assert concurrent_same.reused
    assert concurrent_same.action.action_id == first.action.action_id
    assert adapter.deadman_calls == 1

    rejected = engine.enforce_persisted_kill(cancel_at_ms=base + 60_000)
    assert rejected.outcome is not None
    assert rejected.outcome.kind is OutcomeKind.REJECTED
    rejected_same = engine.enforce_persisted_kill(cancel_at_ms=base + 60_000)
    assert rejected_same.reused
    assert rejected_same.outcome is not None
    assert rejected_same.outcome.kind is OutcomeKind.REJECTED
    assert adapter.deadman_calls == 2

    rearmed = engine.enforce_persisted_kill(cancel_at_ms=base + 90_000)
    assert rearmed.outcome is not None
    assert rearmed.outcome.kind is OutcomeKind.DEADMAN_ARMED
    assert rearmed.action.action_id not in {
        first.action.action_id,
        rejected.action.action_id,
    }
    assert adapter.deadlines == [
        base + 30_000,
        base + 60_000,
        base + 90_000,
    ]
    facade.release_writer_lease()
    store.close()


def test_runtime_does_not_report_external_kill_protected_when_dms_rejected(
    tmp_path: Path,
) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / "rejected-external-dms.sqlite3",
        lease_root=tmp_path / "rejected-external-dms-control",
        owner_id="synthetic-rejected-external-dms",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)
    facade.set_runtime_state(RuntimeState.KILLED, reason="SYNTHETIC_EXTERNAL_KILL")
    adapter = RejectingDeadmanAdapter()
    engine = TestnetExecutionEngine(
        adapter=adapter,
        store=facade,
        limits=config.risk_limits,
        clock=clock,
        reconciler=NoopReconciler(),
    )
    source = RuntimeEventSource()
    runtime = TestnetRuntime(
        config=config,
        store=facade,
        preflight=SimpleNamespace(),
        reconciler=SimpleNamespace(),
        engine=engine,
        event_source=source,
        clock=clock,
    )
    with pytest.raises(RuntimeErrorClosed, match="was not confirmed"):
        runtime.poll_once()
    assert adapter.deadman_calls == 1
    assert store.wallet_lease(config.run_id) is None
    assert any(
        event.event_type == "TESTNET_EXTERNAL_KILL_DMS_UNCONFIRMED"
        for event in store.get_audit_events(config.run_id)
    )
    store.close()


def test_runtime_stop_preserves_external_pause_and_releases_lease(
    tmp_path: Path,
) -> None:
    config = _config()
    clock = MutableClock()
    store = TestnetStore(
        tmp_path / "paused.sqlite3",
        lease_root=tmp_path / "paused-control",
        owner_id="synthetic-paused-owner",
    )
    store.create_run(config, created_at=clock())
    facade = RunScopedStore(store, run_id=config.run_id, clock=clock)
    _activate_store(store, facade, clock)
    source = RuntimeEventSource()
    runtime = TestnetRuntime(
        config=config,
        store=facade,
        preflight=SimpleNamespace(),
        reconciler=SimpleNamespace(),
        engine=SimpleNamespace(),
        event_source=source,
        clock=clock,
    )
    facade.set_runtime_state(RuntimeState.PAUSED, reason="SYNTHETIC_EXTERNAL_PAUSE")
    runtime.stop(reason="SYNTHETIC_STOP")
    assert store.get_run(config.run_id).runtime_state is RuntimeState.PAUSED
    assert store.wallet_lease(config.run_id) is None
    store.close()


class PreflightStore:
    def verify_integrity(self) -> bool:
        return True

    def append_audit(self, kind: str, payload: object) -> None:
        del kind, payload


class PreflightAdapter:
    origin = TESTNET_API_ORIGIN
    account_address = _ACCOUNT
    api_wallet_address = _API_WALLET


def test_preflight_rejects_arbitrary_authorization_object_before_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hyperlab_testnet.runtime as runtime_module

    monkeypatch.setattr(runtime_module, "validate_runtime_identity", lambda config: config)
    preflight = TestnetPreflight(
        config=_config(),
        adapter=PreflightAdapter(),  # type: ignore[arg-type]
        store=PreflightStore(),  # type: ignore[arg-type]
        authorization_check=lambda: object(),  # type: ignore[arg-type,return-value]
        authorization_evidence={
            "validation_id": "d" * 64,
            "validation_report_sha256": "e" * 64,
        },
        clock_ms=lambda: int(_NOW.timestamp() * 1_000),
    )
    with pytest.raises(RuntimeErrorClosed, match="wrong type"):
        preflight.run()

    with pytest.raises(ValueError, match="lowercase SHA-256"):
        TestnetPreflight(
            config=_config(),
            adapter=PreflightAdapter(),  # type: ignore[arg-type]
            store=PreflightStore(),  # type: ignore[arg-type]
            authorization_check=lambda: object(),  # type: ignore[arg-type,return-value]
            authorization_evidence={
                "validation_id": "not-a-hash",
                "validation_report_sha256": "e" * 64,
            },
            clock_ms=lambda: int(_NOW.timestamp() * 1_000),
        )
