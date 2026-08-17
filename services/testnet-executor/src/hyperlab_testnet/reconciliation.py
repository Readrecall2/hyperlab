"""Exchange-first Testnet snapshot parsing and idempotent reconciliation."""

from __future__ import annotations

import re
import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Protocol, cast

from .adapter import OutcomeKind
from .canonical import JsonValue, canonical_sha256, decimal_text, decimal_value, deterministic_id
from .engine import ActionRecord, ExecutionSnapshot, FinalSendRefused, PreparedAction
from .models import (
    ActionAttemptStatus,
    ActionKind,
    OrderSide,
    OrderStatus,
    RuntimeState,
    TestnetOrder,
    TestnetOrderIntent,
    validate_cloid,
)
from .store import (
    ActionAttemptRecord,
    OrderProjectionUpdate,
    ReconciliationActionResolution,
    ReconciliationFill,
    TestnetStore,
    TestnetStoreError,
)
from .store import (
    ReconciliationIssue as StoreReconciliationIssue,
)

_FILL_PAGE_LIMIT = 2_000
_FILL_RETENTION_LIMIT = 10_000
_MAX_FILL_WINDOWS = 256
_VENUE_HASH_RE = re.compile(r"0x[0-9a-fA-F]{64}\Z")


class ReconciliationError(RuntimeError):
    pass


def _decimal(
    value: object,
    *,
    label: str,
    non_negative: bool = False,
    positive: bool = False,
) -> Decimal:
    if isinstance(value, bool):
        raise ReconciliationError(f"{label} must be a decimal")
    if positive and non_negative:
        raise ValueError("decimal parser flags are mutually exclusive")
    try:
        result = decimal_value(
            str(value),
            label=label,
            positive=positive,
            non_negative=non_negative,
        )
    except (TypeError, ValueError):
        raise ReconciliationError(f"{label} must be a decimal") from None
    return result


def _oid(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ReconciliationError("remote oid has an invalid type")
    result = str(value)
    if not result.isdigit():
        raise ReconciliationError("remote oid is not a non-negative integer")
    return result


def _coin(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or ":" in value:
        raise ReconciliationError("remote coin is not an exact venue identifier")
    return value


def _side(value: object) -> OrderSide:
    if value in {"B", "BUY", "Buy"}:
        return OrderSide.BUY
    if value in {"A", "SELL", "Sell"}:
        return OrderSide.SELL
    raise ReconciliationError("remote side is unknown")


def _canonical_instrument(coin: str) -> str:
    return f"HL:{coin}:perp"


@dataclass(frozen=True, slots=True)
class RemoteOrder:
    coin: str
    oid: str
    cloid: str | None
    side: OrderSide
    limit_price: Decimal
    original_quantity: Decimal
    remaining_quantity: Decimal
    status: OrderStatus


@dataclass(frozen=True, slots=True)
class RemoteFill:
    fill_id: str
    coin: str
    oid: str
    cloid: str | None
    side: OrderSide
    quantity: Decimal
    price: Decimal
    fee: Decimal
    timestamp_ms: int
    venue_hash: str | None = None


@dataclass(frozen=True, slots=True)
class RemoteSnapshot:
    captured_at_ms: int
    fill_start_ms: int
    open_orders: tuple[RemoteOrder, ...]
    order_statuses: Mapping[str, RemoteOrder | None]
    fills: tuple[RemoteFill, ...]
    positions: Mapping[str, Decimal]
    spot_balances: Mapping[str, Decimal]
    equity: Decimal
    withdrawable: Decimal

    def __post_init__(self) -> None:
        if (
            isinstance(self.captured_at_ms, bool)
            or not isinstance(self.captured_at_ms, int)
            or self.captured_at_ms <= 0
        ):
            raise ValueError("captured_at_ms must be a positive integer")
        if (
            isinstance(self.fill_start_ms, bool)
            or not isinstance(self.fill_start_ms, int)
            or not 0 < self.fill_start_ms <= self.captured_at_ms
        ):
            raise ValueError("fill_start_ms must be within the captured snapshot range")
        object.__setattr__(self, "order_statuses", MappingProxyType(dict(self.order_statuses)))
        object.__setattr__(self, "positions", MappingProxyType(dict(self.positions)))
        object.__setattr__(self, "spot_balances", MappingProxyType(dict(self.spot_balances)))


@dataclass(frozen=True, slots=True)
class LocalSnapshot:
    orders: tuple[TestnetOrder, ...]
    fill_ids: frozenset[str]
    positions: Mapping[str, Decimal]
    spot_balances: Mapping[str, Decimal]
    equity: Decimal | None
    fill_cursor_ms: int
    fill_overlap_ids: frozenset[str]
    ambiguous_actions: tuple[ActionRecord, ...] = ()
    stable_fill_quantities: Mapping[str, Decimal] = field(default_factory=dict)
    stable_fill_notionals: Mapping[str, Decimal] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (
            isinstance(self.fill_cursor_ms, bool)
            or not isinstance(self.fill_cursor_ms, int)
            or self.fill_cursor_ms <= 0
        ):
            raise ValueError("fill_cursor_ms must be a durable positive integer")
        object.__setattr__(self, "orders", tuple(self.orders))
        object.__setattr__(self, "fill_ids", frozenset(self.fill_ids))
        object.__setattr__(self, "fill_overlap_ids", frozenset(self.fill_overlap_ids))
        object.__setattr__(self, "ambiguous_actions", tuple(self.ambiguous_actions))
        if not self.fill_overlap_ids.issubset(self.fill_ids):
            raise ValueError("fill overlap anchors must already be durably deduplicated")
        object.__setattr__(self, "positions", MappingProxyType(dict(self.positions)))
        object.__setattr__(self, "spot_balances", MappingProxyType(dict(self.spot_balances)))
        stable_quantities = {
            order_id: _decimal(
                quantity,
                label=f"stable fill quantity {order_id}",
                non_negative=True,
            )
            for order_id, quantity in self.stable_fill_quantities.items()
        }
        stable_notionals = {
            order_id: _decimal(
                notional,
                label=f"stable fill notional {order_id}",
                non_negative=True,
            )
            for order_id, notional in self.stable_fill_notionals.items()
        }
        if set(stable_quantities) != set(stable_notionals):
            raise ValueError("stable fill quantity/notional keys must match exactly")
        object.__setattr__(
            self,
            "stable_fill_quantities",
            MappingProxyType(stable_quantities),
        )
        object.__setattr__(
            self,
            "stable_fill_notionals",
            MappingProxyType(stable_notionals),
        )


@dataclass(frozen=True, slots=True)
class ReconciliationEvent:
    event_id: str
    kind: str
    payload: Mapping[str, JsonValue]

    @classmethod
    def create(cls, kind: str, payload: Mapping[str, JsonValue]) -> ReconciliationEvent:
        frozen = MappingProxyType(dict(payload))
        return cls(
            event_id=deterministic_id("testnet_reconciliation_event_v1", kind, dict(frozen)),
            kind=kind,
            payload=frozen,
        )


@dataclass(frozen=True, slots=True)
class ReconciliationIssue:
    code: str
    identity: str


@dataclass(frozen=True, slots=True)
class ReconciliationActionDecision:
    action_id: str
    status: ActionAttemptStatus
    proof: Mapping[str, JsonValue]

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", ActionAttemptStatus(self.status))
        if self.status is ActionAttemptStatus.AMBIGUOUS:
            raise ValueError("action reconciliation decision cannot remain ambiguous")
        if not self.proof:
            raise ValueError("action reconciliation decision requires proof")
        object.__setattr__(self, "proof", MappingProxyType(dict(self.proof)))


@dataclass(frozen=True, slots=True)
class ReconciliationPlan:
    snapshot_hash: str
    events: tuple[ReconciliationEvent, ...]
    issues: tuple[ReconciliationIssue, ...]
    action_resolutions: tuple[ReconciliationActionDecision, ...] = ()

    @property
    def clean(self) -> bool:
        return not self.issues

    @property
    def required_runtime_state(self) -> RuntimeState:
        return RuntimeState.RUNNING if self.clean else RuntimeState.MANUAL_REVIEW


class SnapshotAdapter(Protocol):
    def read_open_orders(self) -> object: ...

    def read_user_fills_by_time(self, *, start_time_ms: int, end_time_ms: int) -> object: ...

    def read_clearinghouse_state(self) -> object: ...

    def read_spot_clearinghouse_state(self) -> object: ...

    def read_order_status(self, cloid: str) -> object: ...


class ReconciliationStore(Protocol):
    def local_snapshot(self) -> LocalSnapshot: ...

    def record_reconciliation_issue(self, issue: ReconciliationIssue) -> None: ...

    def apply_reconciliation(
        self,
        remote: RemoteSnapshot,
        plan: ReconciliationPlan,
    ) -> None:
        """Atomically persist snapshot, fills/projections/events and freshness if clean."""
        ...

    def set_runtime_state(self, state: RuntimeState, *, reason: str) -> None: ...


def parse_open_orders(payload: object) -> tuple[RemoteOrder, ...]:
    if not isinstance(payload, list):
        raise ReconciliationError("openOrders response must be a list")
    orders: list[RemoteOrder] = []
    for item in payload:
        if not isinstance(item, Mapping):
            raise ReconciliationError("openOrders item must be an object")
        raw_cloid = item.get("cloid")
        cloid = None if raw_cloid in {None, ""} else validate_cloid(str(raw_cloid))
        original = _decimal(item.get("origSz", item.get("sz")), label="origSz", non_negative=True)
        remaining = _decimal(item.get("sz"), label="sz", non_negative=True)
        if remaining > original or original <= 0:
            raise ReconciliationError("remote open-order quantities are inconsistent")
        orders.append(
            RemoteOrder(
                coin=_coin(item.get("coin")),
                oid=_oid(item.get("oid")),
                cloid=cloid,
                side=_side(item.get("side")),
                limit_price=_decimal(item.get("limitPx"), label="limitPx", positive=True),
                original_quantity=original,
                remaining_quantity=remaining,
                status=OrderStatus.OPEN,
            )
        )
    return tuple(sorted(orders, key=lambda order: (order.cloid or "", order.oid)))


_TERMINAL_STATUS = {
    "open": OrderStatus.OPEN,
    "filled": OrderStatus.FILLED,
    "canceled": OrderStatus.CANCELLED,
    "cancelled": OrderStatus.CANCELLED,
    "rejected": OrderStatus.REJECTED,
    "expired": OrderStatus.EXPIRED,
}


def parse_order_status(payload: object, *, queried_cloid: str) -> RemoteOrder | None:
    cloid = validate_cloid(queried_cloid)
    if not isinstance(payload, Mapping):
        raise ReconciliationError("orderStatus response must be an object")
    marker = payload.get("status")
    if marker == "unknownOid":
        return None
    if marker != "order" or not isinstance(payload.get("order"), Mapping):
        raise ReconciliationError("orderStatus response has an unknown shape")
    wrapper = cast(Mapping[str, object], payload["order"])
    order_payload = wrapper.get("order")
    status_text = wrapper.get("status")
    if not isinstance(order_payload, Mapping) or not isinstance(status_text, str):
        raise ReconciliationError("orderStatus order wrapper is invalid")
    status = _TERMINAL_STATUS.get(status_text)
    if status is None:
        status = OrderStatus.UNKNOWN
    raw_cloid = order_payload.get("cloid")
    observed_cloid = cloid if raw_cloid in {None, ""} else validate_cloid(str(raw_cloid))
    if observed_cloid != cloid:
        raise ReconciliationError("orderStatus CLOID differs from the query")
    original = _decimal(
        order_payload.get("origSz", order_payload.get("sz")),
        label="orderStatus.origSz",
        non_negative=True,
    )
    remaining = _decimal(
        order_payload.get("sz", "0"),
        label="orderStatus.sz",
        non_negative=True,
    )
    if status in {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}:
        remaining = Decimal(0)
    return RemoteOrder(
        coin=_coin(order_payload.get("coin")),
        oid=_oid(order_payload.get("oid")),
        cloid=observed_cloid,
        side=_side(order_payload.get("side")),
        limit_price=_decimal(
            order_payload.get("limitPx"),
            label="orderStatus.limitPx",
            positive=True,
        ),
        original_quantity=original,
        remaining_quantity=remaining,
        status=status,
    )


def parse_user_fills(payload: object) -> tuple[RemoteFill, ...]:
    if not isinstance(payload, list):
        raise ReconciliationError("userFills response must be a list")
    fills: dict[str, RemoteFill] = {}
    for item in payload:
        if not isinstance(item, Mapping):
            raise ReconciliationError("userFills item must be an object")
        coin = _coin(item.get("coin"))
        oid = _oid(item.get("oid"))
        timestamp = item.get("time")
        if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp <= 0:
            raise ReconciliationError("fill time must be a positive millisecond integer")
        raw_cloid = item.get("cloid")
        cloid = None if raw_cloid in {None, ""} else validate_cloid(str(raw_cloid))
        stable = item.get("tid")
        if isinstance(stable, bool) or not isinstance(stable, (int, str)) or not str(stable):
            raise ReconciliationError("fill tid is required for stable deduplication")
        raw_hash = item.get("hash")
        if raw_hash in {None, ""}:
            venue_hash = None
        elif not isinstance(raw_hash, str) or _VENUE_HASH_RE.fullmatch(raw_hash) is None:
            raise ReconciliationError("fill hash has an invalid shape")
        else:
            venue_hash = raw_hash.lower()
        fill_id = deterministic_id("hyperliquid_testnet_fill_v1", str(stable), oid)
        fill = RemoteFill(
            fill_id=fill_id,
            coin=coin,
            oid=oid,
            cloid=cloid,
            side=_side(item.get("side")),
            quantity=_decimal(item.get("sz"), label="fill.sz", positive=True),
            price=_decimal(item.get("px"), label="fill.px", positive=True),
            fee=_decimal(item.get("fee", "0"), label="fill.fee"),
            timestamp_ms=timestamp,
            venue_hash=venue_hash,
        )
        previous = fills.get(fill_id)
        if previous is not None and previous != fill:
            raise ReconciliationError("duplicate fill identity has divergent content")
        fills[fill_id] = fill
    return tuple(sorted(fills.values(), key=lambda fill: (fill.timestamp_ms, fill.fill_id)))


def fetch_user_fills_contiguous(
    adapter: SnapshotAdapter,
    *,
    start_time_ms: int,
    end_time_ms: int,
    required_overlap_ids: frozenset[str] = frozenset(),
) -> tuple[RemoteFill, ...]:
    """Split every full capped window until continuity is proven or refused."""

    if (
        isinstance(start_time_ms, bool)
        or isinstance(end_time_ms, bool)
        or not isinstance(start_time_ms, int)
        or not isinstance(end_time_ms, int)
        or start_time_ms <= 0
        or end_time_ms < start_time_ms
    ):
        raise ReconciliationError("fill cursor range is invalid")
    pending: list[tuple[int, int]] = [(start_time_ms, end_time_ms)]
    fills: dict[str, RemoteFill] = {}
    windows = 0
    while pending:
        lower, upper = pending.pop()
        windows += 1
        if windows > _MAX_FILL_WINDOWS:
            raise ReconciliationError("fill continuity requires too many bounded windows")
        page = parse_user_fills(
            adapter.read_user_fills_by_time(
                start_time_ms=lower,
                end_time_ms=upper,
            )
        )
        if any(not lower <= fill.timestamp_ms <= upper for fill in page):
            raise ReconciliationError("userFillsByTime returned a fill outside the query window")
        if len(page) >= _FILL_PAGE_LIMIT:
            if lower == upper:
                raise ReconciliationError("fill continuity is uncertain at the venue page limit")
            midpoint = lower + (upper - lower) // 2
            pending.append((midpoint + 1, upper))
            pending.append((lower, midpoint))
            continue
        for fill in page:
            previous = fills.get(fill.fill_id)
            if previous is not None and previous != fill:
                raise ReconciliationError("overlapped fill identity has divergent content")
            fills[fill.fill_id] = fill
    if len(fills) >= _FILL_RETENTION_LIMIT:
        raise ReconciliationError("fill continuity may exceed the venue 10000-fill retention")
    if required_overlap_ids and not required_overlap_ids.issubset(fills):
        raise ReconciliationError("durable fill overlap anchor is absent from venue history")
    return tuple(sorted(fills.values(), key=lambda fill: (fill.timestamp_ms, fill.fill_id)))


def parse_clearinghouse_state(payload: object) -> tuple[dict[str, Decimal], Decimal, Decimal]:
    if not isinstance(payload, Mapping):
        raise ReconciliationError("clearinghouseState response must be an object")
    positions_raw = payload.get("assetPositions")
    summary = payload.get("marginSummary")
    if not isinstance(positions_raw, list) or not isinstance(summary, Mapping):
        raise ReconciliationError("clearinghouseState lacks positions or marginSummary")
    positions: dict[str, Decimal] = {}
    seen_positions: set[str] = set()
    for item in positions_raw:
        if not isinstance(item, Mapping) or not isinstance(item.get("position"), Mapping):
            raise ReconciliationError("clearinghouseState position is invalid")
        position = cast(Mapping[str, object], item["position"])
        coin = _coin(position.get("coin"))
        quantity = _decimal(position.get("szi"), label="position.szi")
        instrument = _canonical_instrument(coin)
        if instrument in seen_positions:
            raise ReconciliationError("clearinghouseState contains duplicate positions")
        seen_positions.add(instrument)
        if quantity != 0:
            positions[instrument] = quantity
    equity = _decimal(summary.get("accountValue"), label="accountValue", non_negative=True)
    withdrawable = _decimal(payload.get("withdrawable"), label="withdrawable", non_negative=True)
    return positions, equity, withdrawable


def parse_spot_clearinghouse_state(payload: object) -> dict[str, Decimal]:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("balances"), list):
        raise ReconciliationError("spotClearinghouseState response must contain balances")
    balances: dict[str, Decimal] = {}
    seen_balances: set[str] = set()
    for item in cast(list[object], payload["balances"]):
        if not isinstance(item, Mapping):
            raise ReconciliationError("spot balance must be an object")
        coin = _coin(item.get("coin"))
        total = _decimal(item.get("total"), label="spot total", non_negative=True)
        if coin in seen_balances:
            raise ReconciliationError("spotClearinghouseState contains duplicate balances")
        seen_balances.add(coin)
        if total != 0:
            balances[coin] = total
    return balances


def fetch_remote_snapshot(
    adapter: SnapshotAdapter,
    *,
    queried_cloids: Sequence[str],
    fill_cursor_ms: int,
    fill_overlap_ids: frozenset[str],
    captured_at_ms: int,
) -> RemoteSnapshot:
    open_orders = parse_open_orders(adapter.read_open_orders())
    fill_start_ms = max(1, fill_cursor_ms - 1)
    fills = fetch_user_fills_contiguous(
        adapter,
        start_time_ms=fill_start_ms,
        end_time_ms=captured_at_ms,
        required_overlap_ids=fill_overlap_ids,
    )
    positions, equity, withdrawable = parse_clearinghouse_state(adapter.read_clearinghouse_state())
    balances = parse_spot_clearinghouse_state(adapter.read_spot_clearinghouse_state())
    statuses: dict[str, RemoteOrder | None] = {}
    for cloid in sorted(set(queried_cloids)):
        normalized = validate_cloid(cloid)
        statuses[normalized] = parse_order_status(
            adapter.read_order_status(normalized),
            queried_cloid=normalized,
        )
    return RemoteSnapshot(
        captured_at_ms=captured_at_ms,
        fill_start_ms=fill_start_ms,
        open_orders=open_orders,
        order_statuses=statuses,
        fills=fills,
        positions=positions,
        spot_balances=balances,
        equity=equity,
        withdrawable=withdrawable,
    )


def _snapshot_hash(remote: RemoteSnapshot) -> str:
    return canonical_sha256(
        {
            "captured_at_ms": remote.captured_at_ms,
            "fill_start_ms": remote.fill_start_ms,
            "equity": decimal_text(remote.equity),
            "fills": [fill.fill_id for fill in remote.fills],
            "open_orders": [
                {
                    "cloid": order.cloid,
                    "oid": order.oid,
                    "remaining": decimal_text(order.remaining_quantity),
                }
                for order in remote.open_orders
            ],
            "positions": {
                key: decimal_text(value) for key, value in sorted(remote.positions.items())
            },
            "spot_balances": {
                key: decimal_text(value) for key, value in sorted(remote.spot_balances.items())
            },
            "withdrawable": decimal_text(remote.withdrawable),
        }
    )


def _cumulative_fill_projection(
    order: TestnetOrder,
    local: LocalSnapshot,
    new_fills: Sequence[RemoteFill],
) -> tuple[Decimal, Decimal | None]:
    order_id = order.intent.order_id
    stable_quantity = local.stable_fill_quantities.get(order_id, Decimal(0)) + sum(
        (fill.quantity for fill in new_fills),
        start=Decimal(0),
    )
    stable_notional = local.stable_fill_notionals.get(order_id, Decimal(0)) + sum(
        (fill.quantity * fill.price for fill in new_fills),
        start=Decimal(0),
    )
    if stable_quantity > order.intent.quantity:
        raise ReconciliationError("stable fills exceed requested order quantity")
    if (stable_quantity == 0) != (stable_notional == 0):
        raise ReconciliationError("stable fill quantity/notional aggregate is inconsistent")
    projected_quantity = order.filled_quantity
    projected_notional = (
        projected_quantity * order.average_fill_price if order.average_fill_price is not None else Decimal(0)
    )
    if stable_quantity == projected_quantity:
        if stable_notional != projected_notional:
            raise ReconciliationError("stable fill economics differ from the immediate order projection")
        return projected_quantity, order.average_fill_price
    if stable_quantity > projected_quantity:
        return stable_quantity, stable_notional / stable_quantity
    return projected_quantity, order.average_fill_price


def plan_reconciliation(
    local: LocalSnapshot,
    remote: RemoteSnapshot,
    *,
    absence_proven_action_ids: frozenset[str] = frozenset(),
) -> ReconciliationPlan:
    snapshot_hash = _snapshot_hash(remote)
    events: list[ReconciliationEvent] = []
    issues: list[ReconciliationIssue] = []
    action_resolutions: list[ReconciliationActionDecision] = []
    terminal_cancel_action_ids: set[str] = set()
    local_by_cloid: dict[str, TestnetOrder] = {}
    local_by_oid: dict[str, list[TestnetOrder]] = {}
    for local_order in local.orders:
        cloid = local_order.intent.cloid
        if cloid in local_by_cloid:
            issues.append(ReconciliationIssue("DUPLICATE_LOCAL_CLOID", cloid))
        local_by_cloid[cloid] = local_order
        if local_order.venue_order_id is not None:
            oid_orders = local_by_oid.setdefault(local_order.venue_order_id, [])
            if local_order.status.reserves_exposure and any(
                candidate.status.reserves_exposure for candidate in oid_orders
            ):
                issues.append(ReconciliationIssue("DUPLICATE_LOCAL_OID", local_order.venue_order_id))
            oid_orders.append(local_order)

    remote_by_cloid: dict[str, RemoteOrder] = {}
    remote_by_oid: dict[str, RemoteOrder] = {}
    for remote_order in remote.open_orders:
        if remote_order.cloid is None:
            issues.append(ReconciliationIssue("REMOTE_OPEN_ORDER_WITHOUT_CLOID", remote_order.oid))
        elif remote_order.cloid in remote_by_cloid:
            issues.append(ReconciliationIssue("DUPLICATE_REMOTE_CLOID", remote_order.cloid))
        else:
            remote_by_cloid[remote_order.cloid] = remote_order
        if remote_order.oid in remote_by_oid:
            issues.append(ReconciliationIssue("DUPLICATE_REMOTE_OID", remote_order.oid))
        remote_by_oid[remote_order.oid] = remote_order
        if remote_order.cloid is None or remote_order.cloid not in local_by_cloid:
            issues.append(ReconciliationIssue("UNKNOWN_REMOTE_OPEN_ORDER", remote_order.oid))

    resolved_absent_cloids: set[str] = set()
    resolved_cancel_open_cloids: set[str] = set()
    resolved_replace_original_cloids: set[str] = set()
    for action in local.ambiguous_actions:
        if action.expires_after_ms is None:
            issues.append(ReconciliationIssue("AMBIGUOUS_ACTION_EXPIRY_MISSING", action.action_id))
            continue
        if action.expires_after_ms - action.nonce > 60_000:
            issues.append(ReconciliationIssue("AMBIGUOUS_ACTION_EXPIRY_INVALID", action.action_id))
            continue
        absence_proven = action.action_id in absence_proven_action_ids
        if action.kind is ActionKind.SCHEDULE_CANCEL:
            issues.append(ReconciliationIssue("DEADMAN_ACTION_UNRESOLVED", action.action_id))
            continue
        if action.kind is ActionKind.SUBMIT and action.cloid is not None:
            observed = remote_by_cloid.get(action.cloid)
            status_order = remote.order_statuses.get(action.cloid)
            authoritative = observed or status_order
            if authoritative is not None:
                resolved_status = (
                    ActionAttemptStatus.REJECTED
                    if authoritative.status is OrderStatus.REJECTED
                    else ActionAttemptStatus.CONFIRMED
                )
                action_resolutions.append(
                    ReconciliationActionDecision(
                        action.action_id,
                        resolved_status,
                        {
                            "captured_at_ms": remote.captured_at_ms,
                            "cloid": action.cloid,
                            "code": "AUTHORITATIVE_SUBMIT_STATUS",
                            "oid": authoritative.oid,
                            "status": authoritative.status.value,
                        },
                    )
                )
            else:
                issues.append(ReconciliationIssue("AMBIGUOUS_SUBMIT_UNRESOLVED", action.action_id))
            continue
        if action.kind is ActionKind.CANCEL and action.cloid is not None:
            observed = remote_by_cloid.get(action.cloid)
            status_order = remote.order_statuses.get(action.cloid)
            if status_order is not None and status_order.status is OrderStatus.CANCELLED:
                terminal_cancel_action_ids.add(action.action_id)
                action_resolutions.append(
                    ReconciliationActionDecision(
                        action.action_id,
                        ActionAttemptStatus.CONFIRMED,
                        {
                            "captured_at_ms": remote.captured_at_ms,
                            "cloid": action.cloid,
                            "code": "AUTHORITATIVE_CANCELLED_STATUS",
                            "oid": status_order.oid,
                            "snapshot_hash": snapshot_hash,
                            "status": status_order.status.value,
                        },
                    )
                )
            elif status_order is not None and status_order.status in {
                OrderStatus.FILLED,
                OrderStatus.EXPIRED,
                OrderStatus.REJECTED,
            }:
                terminal_cancel_action_ids.add(action.action_id)
                action_resolutions.append(
                    ReconciliationActionDecision(
                        action.action_id,
                        ActionAttemptStatus.REJECTED,
                        {
                            "captured_at_ms": remote.captured_at_ms,
                            "cloid": action.cloid,
                            "code": "AUTHORITATIVE_CANCEL_TERMINAL_STATUS",
                            "oid": status_order.oid,
                            "snapshot_hash": snapshot_hash,
                            "status": status_order.status.value,
                        },
                    )
                )
            elif absence_proven and observed is not None:
                resolved_cancel_open_cloids.add(action.cloid)
                action_resolutions.append(
                    ReconciliationActionDecision(
                        action.action_id,
                        ActionAttemptStatus.REJECTED,
                        {
                            "captured_at_ms": remote.captured_at_ms,
                            "cloid": action.cloid,
                            "code": "CANCEL_NOT_EFFECTIVE_AFTER_MONOTONIC_EXPIRY_GRACE",
                            "expires_after_ms": action.expires_after_ms,
                            "oid": observed.oid,
                            "status": observed.status.value,
                        },
                    )
                )
            else:
                issues.append(ReconciliationIssue("AMBIGUOUS_CANCEL_UNRESOLVED", action.action_id))
            continue
        if (
            action.kind is ActionKind.REPLACE
            and action.cloid is not None
            and action.replacement_cloid is not None
        ):
            replacement = remote_by_cloid.get(action.replacement_cloid) or (
                remote.order_statuses.get(action.replacement_cloid)
            )
            original = remote_by_cloid.get(action.cloid)
            if replacement is not None:
                resolved_status = (
                    ActionAttemptStatus.REJECTED
                    if replacement.status is OrderStatus.REJECTED
                    else ActionAttemptStatus.CONFIRMED
                )
                if resolved_status is ActionAttemptStatus.CONFIRMED:
                    resolved_replace_original_cloids.add(action.cloid)
                elif original is None:
                    issues.append(
                        ReconciliationIssue(
                            "REJECTED_REPLACE_ORIGINAL_NOT_OPEN",
                            action.action_id,
                        )
                    )
                action_resolutions.append(
                    ReconciliationActionDecision(
                        action.action_id,
                        resolved_status,
                        {
                            "captured_at_ms": remote.captured_at_ms,
                            "code": "AUTHORITATIVE_REPLACEMENT_STATUS",
                            "oid": replacement.oid,
                            "original_cloid": action.cloid,
                            "replacement_cloid": action.replacement_cloid,
                            "status": replacement.status.value,
                        },
                    )
                )
            else:
                issues.append(ReconciliationIssue("AMBIGUOUS_REPLACE_UNRESOLVED", action.action_id))
            continue
        issues.append(ReconciliationIssue("AMBIGUOUS_ACTION_IDENTITY_INVALID", action.action_id))

    confirmed_reused_replace_oids: set[str] = set()
    confirmed_action_ids = {
        decision.action_id
        for decision in action_resolutions
        if decision.status is ActionAttemptStatus.CONFIRMED
    }
    for action in local.ambiguous_actions:
        if (
            action.action_id not in confirmed_action_ids
            or action.kind is not ActionKind.REPLACE
            or action.cloid is None
            or action.replacement_cloid is None
        ):
            continue
        local_original = local_by_cloid.get(action.cloid)
        replacement = remote_by_cloid.get(action.replacement_cloid) or (
            remote.order_statuses.get(action.replacement_cloid)
        )
        if (
            local_original is not None
            and local_original.venue_order_id is not None
            and replacement is not None
            and replacement.oid == local_original.venue_order_id
        ):
            confirmed_reused_replace_oids.add(replacement.oid)

    validated_new_fills: dict[str, list[RemoteFill]] = {}
    projected_positions = dict(local.positions)
    for fill in remote.fills:
        if fill.fill_id in local.fill_ids:
            continue
        owner: TestnetOrder | None
        if fill.cloid is not None:
            owner = local_by_cloid.get(fill.cloid)
        else:
            if fill.oid in confirmed_reused_replace_oids:
                issues.append(
                    ReconciliationIssue(
                        "AMBIGUOUS_REMOTE_FILL_DURING_REPLACE",
                        fill.fill_id,
                    )
                )
                continue
            oid_candidates = local_by_oid.get(fill.oid, [])
            if len(oid_candidates) > 1:
                issues.append(ReconciliationIssue("AMBIGUOUS_REMOTE_FILL_REUSED_OID", fill.fill_id))
                continue
            owner = oid_candidates[0] if oid_candidates else None
        if owner is None:
            issues.append(ReconciliationIssue("UNKNOWN_REMOTE_FILL", fill.fill_id))
            continue
        instrument = owner.intent.instrument
        expected_coin = instrument.split(":")[1]
        expected_side = cast(OrderSide, owner.intent.side)
        if fill.coin != expected_coin:
            issues.append(ReconciliationIssue("REMOTE_FILL_INSTRUMENT_DIVERGENCE", fill.fill_id))
            continue
        if fill.side is not expected_side:
            issues.append(ReconciliationIssue("REMOTE_FILL_SIDE_DIVERGENCE", fill.fill_id))
            continue
        if owner.venue_order_id is not None and fill.oid != owner.venue_order_id:
            issues.append(ReconciliationIssue("REMOTE_FILL_OID_DIVERGENCE", fill.fill_id))
            continue
        validated_new_fills.setdefault(owner.intent.cloid, []).append(fill)
        projected_positions[instrument] = projected_positions.get(instrument, Decimal(0)) + (
            fill.side.sign * fill.quantity
        )
        if projected_positions[instrument] == 0:
            projected_positions.pop(instrument)
        events.append(
            ReconciliationEvent.create(
                "REMOTE_FILL_APPLIED",
                {
                    "cloid": owner.intent.cloid,
                    "fee": decimal_text(fill.fee),
                    "fill_id": fill.fill_id,
                    "oid": fill.oid,
                    "price": decimal_text(fill.price),
                    "quantity": decimal_text(fill.quantity),
                    "side": fill.side.value,
                    "timestamp_ms": fill.timestamp_ms,
                    "venue_hash": fill.venue_hash,
                },
            )
        )

    for cloid, order in sorted(local_by_cloid.items()):
        observed = remote_by_cloid.get(cloid)
        terminal = remote.order_statuses.get(cloid)
        if observed is not None:
            if order.status.terminal:
                issues.append(ReconciliationIssue("TERMINAL_LOCAL_ORDER_REMOTE_OPEN", cloid))
                continue
            if order.venue_order_id is not None and order.venue_order_id != observed.oid:
                issues.append(ReconciliationIssue("ORDER_OID_DIVERGENCE", cloid))
                continue
            if observed.coin != order.intent.instrument.split(":")[1]:
                issues.append(ReconciliationIssue("ORDER_INSTRUMENT_DIVERGENCE", cloid))
                continue
            if observed.side is not cast(OrderSide, order.intent.side):
                issues.append(ReconciliationIssue("ORDER_SIDE_DIVERGENCE", cloid))
                continue
            if observed.original_quantity != order.intent.quantity:
                issues.append(ReconciliationIssue("ORDER_SIZE_DIVERGENCE", cloid))
                continue
            if observed.limit_price != order.intent.limit_price:
                issues.append(ReconciliationIssue("ORDER_LIMIT_PRICE_DIVERGENCE", cloid))
                continue
            if order.status is OrderStatus.CANCEL_REQUESTED and cloid not in resolved_cancel_open_cloids:
                issues.append(ReconciliationIssue("CANCEL_ACK_MISSING_ORDER_STILL_OPEN", cloid))
                continue
            remote_filled = order.intent.quantity - observed.remaining_quantity
            if remote_filled < order.filled_quantity:
                issues.append(ReconciliationIssue("REMOTE_FILLED_QUANTITY_REGRESSED", cloid))
                continue
            try:
                cumulative_filled, _ = _cumulative_fill_projection(
                    order,
                    local,
                    validated_new_fills.get(cloid, ()),
                )
            except ReconciliationError:
                issues.append(ReconciliationIssue("ORDER_FILL_AGGREGATE_DIVERGENCE", cloid))
                continue
            if cumulative_filled != remote_filled:
                issues.append(ReconciliationIssue("ORDER_FILL_CONTINUITY_DIVERGENCE", cloid))
                continue
            events.append(
                ReconciliationEvent.create(
                    "REMOTE_ORDER_OPEN_CONFIRMED",
                    {
                        "cloid": cloid,
                        "filled_quantity": decimal_text(remote_filled),
                        "oid": observed.oid,
                        "remaining_quantity": decimal_text(observed.remaining_quantity),
                    },
                )
            )
            continue
        if terminal is not None:
            if order.venue_order_id is not None and order.venue_order_id != terminal.oid:
                issues.append(ReconciliationIssue("ORDER_OID_DIVERGENCE", cloid))
            elif terminal.coin != order.intent.instrument.split(":")[1]:
                issues.append(ReconciliationIssue("ORDER_INSTRUMENT_DIVERGENCE", cloid))
            elif terminal.side is not cast(OrderSide, order.intent.side):
                issues.append(ReconciliationIssue("ORDER_SIDE_DIVERGENCE", cloid))
            elif terminal.original_quantity != order.intent.quantity:
                issues.append(ReconciliationIssue("ORDER_SIZE_DIVERGENCE", cloid))
            elif terminal.limit_price != order.intent.limit_price:
                issues.append(ReconciliationIssue("ORDER_LIMIT_PRICE_DIVERGENCE", cloid))
            elif terminal.status is OrderStatus.OPEN:
                issues.append(ReconciliationIssue("OPEN_STATUS_ABSENT_FROM_OPEN_ORDERS", cloid))
            elif terminal.status is OrderStatus.UNKNOWN:
                issues.append(ReconciliationIssue("REMOTE_ORDER_STATUS_UNKNOWN", cloid))
            elif terminal.status is OrderStatus.FILLED:
                try:
                    cumulative_filled, _ = _cumulative_fill_projection(
                        order,
                        local,
                        validated_new_fills.get(cloid, ()),
                    )
                except ReconciliationError:
                    issues.append(
                        ReconciliationIssue(
                            "ORDER_FILL_AGGREGATE_DIVERGENCE",
                            cloid,
                        )
                    )
                    continue
                if cumulative_filled != order.intent.quantity:
                    issues.append(
                        ReconciliationIssue(
                            "ORDER_FILL_CONTINUITY_DIVERGENCE",
                            cloid,
                        )
                    )
                else:
                    events.append(
                        ReconciliationEvent.create(
                            "REMOTE_ORDER_TERMINAL_CONFIRMED",
                            {
                                "cloid": cloid,
                                "oid": terminal.oid,
                                "status": terminal.status.value,
                            },
                        )
                    )
            else:
                events.append(
                    ReconciliationEvent.create(
                        "REMOTE_ORDER_TERMINAL_CONFIRMED",
                        {
                            "cloid": cloid,
                            "oid": terminal.oid,
                            "status": terminal.status.value,
                        },
                    )
                )
            continue
        if (
            not order.status.terminal
            and cloid not in resolved_absent_cloids
            and cloid not in resolved_replace_original_cloids
        ):
            issues.append(ReconciliationIssue("LOCAL_ACTIVE_ORDER_MISSING_REMOTE", cloid))
        elif cloid in resolved_absent_cloids:
            events.append(
                ReconciliationEvent.create(
                    "AMBIGUOUS_ACTION_ABSENCE_PROVEN",
                    {"cloid": cloid},
                )
            )
        elif cloid in resolved_replace_original_cloids:
            events.append(
                ReconciliationEvent.create(
                    "NATIVE_REPLACE_ORIGINAL_TERMINALIZED",
                    {"cloid": cloid},
                )
            )

    normalized_projected = {key: value for key, value in projected_positions.items() if value != 0}
    if normalized_projected != dict(remote.positions):
        issues.append(ReconciliationIssue("POSITION_DIVERGENCE", "account"))
    else:
        events.append(
            ReconciliationEvent.create(
                "POSITIONS_RECONCILED",
                {
                    "positions": {
                        key: decimal_text(value) for key, value in sorted(remote.positions.items())
                    },
                    "position_hash": canonical_sha256(
                        {key: decimal_text(value) for key, value in sorted(remote.positions.items())}
                    ),
                },
            )
        )
    events.append(
        ReconciliationEvent.create(
            "ACCOUNT_SNAPSHOT_SYNCED",
            {
                "equity": decimal_text(remote.equity),
                "spot_balances": {
                    key: decimal_text(value) for key, value in sorted(remote.spot_balances.items())
                },
                "spot_balance_hash": canonical_sha256(
                    {key: decimal_text(value) for key, value in sorted(remote.spot_balances.items())}
                ),
                "withdrawable": decimal_text(remote.withdrawable),
            },
        )
    )
    if issues and terminal_cancel_action_ids:
        action_resolutions = [
            decision
            for decision in action_resolutions
            if decision.action_id not in terminal_cancel_action_ids
        ]
        issues.extend(
            ReconciliationIssue("AMBIGUOUS_CANCEL_UNRESOLVED", action_id)
            for action_id in terminal_cancel_action_ids
        )
    return ReconciliationPlan(
        snapshot_hash=snapshot_hash,
        events=tuple(sorted(events, key=lambda event: event.event_id)),
        issues=tuple(sorted(set(issues), key=lambda issue: (issue.code, issue.identity))),
        action_resolutions=tuple(sorted(action_resolutions, key=lambda resolution: resolution.action_id)),
    )


class RunScopedStore:
    """Narrow production binding from the engine/reconciler/runtime to TestnetStore."""

    def __init__(
        self,
        store: TestnetStore,
        *,
        run_id: str,
        clock: Callable[[], datetime],
    ) -> None:
        if not isinstance(store, TestnetStore):
            raise TypeError("store must be a TestnetStore")
        if not isinstance(run_id, str) or len(run_id) != 64:
            raise ValueError("run_id must be a deterministic SHA-256")
        if not callable(clock):
            raise TypeError("clock must be callable")
        durable = store.get_run(run_id)
        if durable.run_id != run_id:
            raise ValueError("run-scoped store identity differs")
        self._store = store
        self._run_id = run_id
        self._clock = clock

    @property
    def run_id(self) -> str:
        return self._run_id

    def _now(self) -> datetime:
        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise TypeError("clock must return a timezone-aware datetime")
        return now.astimezone(UTC)

    def acquire_writer_lease(self) -> None:
        self._store.acquire_wallet_lease(self._run_id, acquired_at=self._now())

    def renew_writer_lease(self) -> None:
        self._store.renew_wallet_lease(self._run_id, renewed_at=self._now())

    def release_writer_lease(self) -> None:
        self._store.release_wallet_lease(self._run_id)

    def runtime_state(self) -> RuntimeState:
        return self._store.get_run(self._run_id).runtime_state

    def account_kill_latched(self) -> bool:
        return self._store.account_kill_latched(self._run_id)

    @contextmanager
    def final_send_permit(
        self,
        action_id: str,
    ) -> Iterator[ActionRecord]:
        manager = self._store.final_send_permit(self._run_id, action_id)
        try:
            durable = manager.__enter__()
        except Exception:
            raise FinalSendRefused("serialized Testnet final-send gate refused") from None
        try:
            yield self._action_from_record(durable)
        except BaseException:
            if not manager.__exit__(*sys.exc_info()):
                raise
        else:
            manager.__exit__(None, None, None)

    def runtime_reason(self) -> str | None:
        return self._store.get_run(self._run_id).state_reason

    def verify_integrity(self) -> object:
        return self._store.verify_integrity(self._run_id)

    def set_runtime_state(self, state: RuntimeState, *, reason: str) -> None:
        self._store.set_runtime_state(
            self._run_id,
            state,
            reason=reason,
            at=self._now(),
        )

    def append_audit(
        self,
        kind: str,
        payload: Mapping[str, JsonValue],
    ) -> None:
        self._store.append_audit(
            self._run_id,
            kind,
            cast(Mapping[str, object], payload),
            created_at=self._now(),
        )

    def _orders(self) -> tuple[TestnetOrder, ...]:
        return self._store.list_orders(self._run_id)

    def _orders_by_cloid(self) -> dict[str, TestnetOrder]:
        result: dict[str, TestnetOrder] = {}
        for order in self._orders():
            if order.intent.cloid in result:
                raise ReconciliationError("durable store contains duplicate CLOIDs")
            result[order.intent.cloid] = order
        return result

    @staticmethod
    def _snapshot_decimals(
        payload: Mapping[str, object],
        key: str,
    ) -> dict[str, Decimal]:
        value = payload.get(key, {})
        if not isinstance(value, Mapping):
            raise ReconciliationError(f"durable {key} snapshot is not an object")
        result: dict[str, Decimal] = {}
        for identity, quantity in value.items():
            if not isinstance(identity, str) or not identity:
                raise ReconciliationError(f"durable {key} identity is invalid")
            result[identity] = _decimal(quantity, label=f"durable {key}.{identity}")
        return result

    def _latest_account_state(
        self,
    ) -> tuple[dict[str, Decimal], dict[str, Decimal], Decimal | None]:
        snapshot = self._store.latest_reconciled_snapshot(self._run_id)
        if snapshot is None:
            return {}, {}, None
        positions = self._snapshot_decimals(snapshot.payload, "positions")
        spot = self._snapshot_decimals(snapshot.payload, "spot_balances")
        equity = _decimal(
            snapshot.payload.get("equity"),
            label="durable equity",
            non_negative=True,
        )
        return positions, spot, equity

    def execution_snapshot(self) -> ExecutionSnapshot:
        now = self._now()
        positions, _, _ = self._latest_account_state()
        since = now - timedelta(minutes=1)
        return ExecutionSnapshot(
            runtime_state=self.runtime_state(),
            orders=self._orders(),
            positions=positions,
            marks={},
            last_reconciled_at=self._store.last_reconciled_at(self._run_id),
            submit_requests_in_last_minute=self._store.count_actions_since(
                self._run_id,
                ActionKind.SUBMIT,
                since=since,
            ),
            cancel_requests_in_last_minute=(
                self._store.count_actions_since(
                    self._run_id,
                    ActionKind.CANCEL,
                    since=since,
                )
                + self._store.count_actions_since(
                    self._run_id,
                    ActionKind.SCHEDULE_CANCEL,
                    since=since,
                )
            ),
            replace_requests_in_last_minute=self._store.count_actions_since(
                self._run_id,
                ActionKind.REPLACE,
                since=since,
            ),
        )

    def get_order(self, cloid: str) -> TestnetOrder | None:
        return self._orders_by_cloid().get(validate_cloid(cloid))

    def persist_intent(self, intent: TestnetOrderIntent) -> TestnetOrder:
        if intent.run_id != self._run_id:
            raise ValueError("intent belongs to a different Testnet run")
        return self._store.create_order(intent)

    def update_order(
        self,
        order: TestnetOrder,
        *,
        audit_kind: str,
        audit_payload: Mapping[str, JsonValue],
    ) -> None:
        if order.intent.run_id != self._run_id:
            raise ValueError("order belongs to a different Testnet run")
        if order.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}:
            raise ReconciliationError(
                "fill-bearing order projections require an atomic action/reconciliation"
            )
        self._store.transition_order(
            self._run_id,
            order.intent.order_id,
            order.status,
            at=order.updated_at or self._now(),
            venue_order_id=order.venue_order_id,
            reason=audit_kind,
        )
        self._store.append_audit(
            self._run_id,
            audit_kind,
            cast(Mapping[str, object], audit_payload),
            created_at=order.updated_at or self._now(),
        )

    @staticmethod
    def _action_from_record(record: ActionAttemptRecord) -> ActionRecord:
        kind = record.kind
        payload = record.payload
        if not isinstance(payload, Mapping):
            raise ReconciliationError("durable action payload is invalid")
        raw_cloid = payload.get("cloid")
        raw_replacement = payload.get("replacement_cloid")
        cloid = validate_cloid(raw_cloid) if isinstance(raw_cloid, str) else None
        replacement = validate_cloid(raw_replacement) if isinstance(raw_replacement, str) else None
        response = record.response
        code = "PREPARED_BEFORE_IO"
        outcome_kind = None
        venue_order_id = None
        filled_quantity = None
        average_fill_price = None
        raw_expires = payload.get("expires_after_ms")
        expires_after_ms = (
            raw_expires if isinstance(raw_expires, int) and not isinstance(raw_expires, bool) else None
        )
        if response is not None:
            if not isinstance(response, Mapping):
                raise ReconciliationError("durable action response is invalid")
            raw_code = response.get("code")
            if not isinstance(raw_code, str):
                raise ReconciliationError("durable action response lacks a stable code")
            code = raw_code
            raw_kind = response.get("outcome_kind")
            outcome_kind = OutcomeKind(str(raw_kind)) if raw_kind is not None else None
            raw_oid = response.get("venue_order_id")
            venue_order_id = str(raw_oid) if raw_oid is not None else None
            if response.get("filled_quantity") is not None:
                filled_quantity = _decimal(
                    response["filled_quantity"],
                    label="durable action filled_quantity",
                    non_negative=True,
                )
            if response.get("average_fill_price") is not None:
                average_fill_price = _decimal(
                    response["average_fill_price"],
                    label="durable action average_fill_price",
                    positive=True,
                )
        return ActionRecord(
            action_id=record.action_id,
            kind=kind,
            cloid=cloid,
            replacement_cloid=replacement,
            nonce=record.nonce,
            status=record.status,
            code=code,
            outcome_kind=outcome_kind,
            venue_order_id=venue_order_id,
            filled_quantity=filled_quantity,
            average_fill_price=average_fill_price,
            expires_after_ms=expires_after_ms,
        )

    def get_action(self, action_id: str) -> ActionRecord | None:
        try:
            durable = self._store.get_action(self._run_id, action_id)
        except TestnetStoreError:
            return None
        return self._action_from_record(durable)

    def prepare_action(
        self,
        *,
        action_id: str,
        kind: ActionKind,
        cloid: str | None,
        replacement_cloid: str | None,
        minimum_nonce: int,
        expires_after_delta_ms: int,
    ) -> PreparedAction:
        existing = self.get_action(action_id)
        if existing is not None:
            return PreparedAction(existing, False)
        order_cloid = replacement_cloid if kind is ActionKind.REPLACE else cloid
        order = self.get_order(order_cloid) if order_cloid is not None else None
        if kind is not ActionKind.SCHEDULE_CANCEL and order is None:
            raise ReconciliationError("action order identity is absent")
        durable = self._store.reserve_action(
            self._run_id,
            action_id=action_id,
            kind=kind,
            order_id=order.intent.order_id if order is not None else None,
            payload={
                "cloid": cloid,
                "replacement_cloid": replacement_cloid,
            },
            created_at=self._now(),
            minimum_nonce=minimum_nonce,
            expires_after_delta_ms=expires_after_delta_ms,
        )
        return PreparedAction(self._action_from_record(durable), True)

    @staticmethod
    def _action_response(action: ActionRecord) -> dict[str, object]:
        return {
            "average_fill_price": (
                decimal_text(action.average_fill_price) if action.average_fill_price is not None else None
            ),
            "code": action.code,
            "filled_quantity": (
                decimal_text(action.filled_quantity) if action.filled_quantity is not None else None
            ),
            "outcome_kind": (action.outcome_kind.value if action.outcome_kind is not None else None),
            "venue_order_id": action.venue_order_id,
        }

    def complete_action(
        self,
        action: ActionRecord,
        *,
        order_updates: Sequence[TestnetOrder],
    ) -> None:
        projections = tuple(
            OrderProjectionUpdate(
                order.intent.order_id,
                order.status,
                venue_order_id=order.venue_order_id,
                filled_quantity=order.filled_quantity,
                average_fill_price=order.average_fill_price,
            )
            for order in order_updates
        )
        if action.status is ActionAttemptStatus.AMBIGUOUS:
            self._store.observe_ambiguous_action(
                self._run_id,
                action.action_id,
                response=self._action_response(action),
                order_updates=projections,
                observed_at=self._now(),
            )
            return
        self._store.complete_action(
            self._run_id,
            action.action_id,
            action.status,
            response=self._action_response(action),
            order_updates=projections,
            resolved_at=self._now(),
        )

    def unresolved_actions(self) -> tuple[ActionRecord, ...]:
        return tuple(
            self._action_from_record(record) for record in self._store.list_ambiguous_actions(self._run_id)
        )

    def local_snapshot(self) -> LocalSnapshot:
        orders = self._orders()
        orders_by_id = {order.intent.order_id: order for order in orders}
        fills = self._store.list_fills(self._run_id)
        fill_ids = frozenset(fill.fill_id for fill in fills)
        timestamped: list[tuple[int, str]] = []
        stable_quantities: dict[str, Decimal] = {}
        stable_notionals: dict[str, Decimal] = {}
        for fill in fills:
            owner = orders_by_id.get(fill.order_id)
            if owner is None:
                raise ReconciliationError("durable fill has no local order owner")
            if owner.venue_order_id is not None and owner.venue_order_id != fill.venue_order_id:
                raise ReconciliationError("durable fill venue OID differs from its owner")
            stable_quantities[fill.order_id] = (
                stable_quantities.get(
                    fill.order_id,
                    Decimal(0),
                )
                + fill.quantity
            )
            stable_notionals[fill.order_id] = (
                stable_notionals.get(
                    fill.order_id,
                    Decimal(0),
                )
                + fill.quantity * fill.price
            )
            raw_timestamp = fill.payload.get("timestamp_ms")
            if isinstance(raw_timestamp, bool) or not isinstance(raw_timestamp, int) or raw_timestamp <= 0:
                raise ReconciliationError("durable fill lacks its venue timestamp")
            timestamped.append((raw_timestamp, fill.fill_id))
        durable_snapshot = self._store.latest_reconciled_snapshot(self._run_id)
        if durable_snapshot is not None:
            raw_cursor = durable_snapshot.payload.get("source_cursor")
            if not isinstance(raw_cursor, str) or re.fullmatch(r"fills-ms:[1-9][0-9]*", raw_cursor) is None:
                raise ReconciliationError("durable fill source cursor is invalid")
            cursor = int(raw_cursor.removeprefix("fills-ms:"))
        else:
            cursor = max(
                1,
                int(self._store.get_run(self._run_id).created_at.timestamp() * 1_000),
            )
        overlap = frozenset(fill_id for timestamp, fill_id in timestamped if timestamp == cursor)
        positions, spot, equity = self._latest_account_state()
        return LocalSnapshot(
            orders=orders,
            fill_ids=fill_ids,
            positions=positions,
            spot_balances=spot,
            equity=equity,
            fill_cursor_ms=cursor,
            fill_overlap_ids=overlap,
            ambiguous_actions=self.unresolved_actions(),
            stable_fill_quantities=stable_quantities,
            stable_fill_notionals=stable_notionals,
        )

    def record_reconciliation_issue(self, issue: ReconciliationIssue) -> None:
        self._store.record_reconciliation_issue(
            self._run_id,
            issue.code,
            details={"identity": issue.identity},
            detected_at=self._now(),
            latch_manual_review=False,
        )

    @staticmethod
    def _remote_order_payload(order: RemoteOrder) -> dict[str, object]:
        return {
            "cloid": order.cloid,
            "coin": order.coin,
            "limit_price": decimal_text(order.limit_price),
            "oid": order.oid,
            "original_quantity": decimal_text(order.original_quantity),
            "remaining_quantity": decimal_text(order.remaining_quantity),
            "side": order.side.value,
            "status": order.status.value,
        }

    def _reconciliation_fills(
        self,
        remote: RemoteSnapshot,
        local: LocalSnapshot,
        *,
        allow_unowned_observations: bool = False,
    ) -> tuple[ReconciliationFill, ...]:
        by_cloid = {order.intent.cloid: order for order in local.orders}
        by_oid: dict[str, list[TestnetOrder]] = {}
        for order in local.orders:
            if order.venue_order_id is not None:
                by_oid.setdefault(order.venue_order_id, []).append(order)
        result: list[ReconciliationFill] = []
        for fill in remote.fills:
            if fill.cloid is not None:
                owner = by_cloid.get(fill.cloid)
            else:
                candidates = by_oid.get(fill.oid, [])
                owner = candidates[0] if len(candidates) == 1 else None
            if owner is None and not allow_unowned_observations:
                raise ReconciliationError("clean plan contains an unowned remote fill")
            order_id = (
                owner.intent.order_id
                if owner is not None
                else deterministic_id(
                    "hyperliquid_testnet_unowned_fill_observation_v1",
                    fill.fill_id,
                )
            )
            result.append(
                ReconciliationFill(
                    fill_id=fill.fill_id,
                    order_id=order_id,
                    venue_order_id=fill.oid,
                    quantity=fill.quantity,
                    price=fill.price,
                    fee=fill.fee,
                    payload={
                        "cloid": fill.cloid,
                        "coin": fill.coin,
                        "side": fill.side.value,
                        "timestamp_ms": fill.timestamp_ms,
                        "ownership": "LOCAL_ORDER" if owner is not None else "UNOWNED",
                        "venue_hash_digest": (
                            canonical_sha256(
                                {
                                    "domain": "hyperliquid_testnet_venue_hash_v1",
                                    "value": fill.venue_hash,
                                }
                            )
                            if fill.venue_hash is not None
                            else None
                        ),
                    },
                    received_at=datetime.fromtimestamp(
                        fill.timestamp_ms / 1_000,
                        tz=UTC,
                    ),
                )
            )
        return tuple(result)

    @classmethod
    def _failure_observed_orders(
        cls,
        remote: RemoteSnapshot,
    ) -> tuple[Mapping[str, object], ...]:
        observations: list[Mapping[str, object]] = []
        for order in remote.open_orders:
            payload = cls._remote_order_payload(order)
            payload["observation_kind"] = "OPEN_ORDER"
            observations.append(payload)
        for query_cloid, status_order in sorted(remote.order_statuses.items()):
            if status_order is None:
                observations.append(
                    {
                        "observation_kind": "ORDER_STATUS",
                        "query_cloid": query_cloid,
                        "result": None,
                    }
                )
                continue
            payload = cls._remote_order_payload(status_order)
            payload["observation_kind"] = "ORDER_STATUS"
            payload["query_cloid"] = query_cloid
            observations.append(payload)
        return tuple(observations)

    @staticmethod
    def _failure_order_updates(
        remote: RemoteSnapshot,
        local: LocalSnapshot,
    ) -> tuple[OrderProjectionUpdate, ...]:
        by_cloid = {order.intent.cloid: order for order in local.orders}
        updates: list[OrderProjectionUpdate] = []
        observed: list[tuple[str, RemoteOrder]] = [(order.cloid or "", order) for order in remote.open_orders]
        observed.extend(
            (query_cloid, order) for query_cloid, order in remote.order_statuses.items() if order is not None
        )
        for query_cloid, order in observed:
            owner = by_cloid.get(query_cloid)
            order_id = (
                owner.intent.order_id
                if owner is not None
                else deterministic_id(
                    "hyperliquid_testnet_unowned_order_observation_v1",
                    query_cloid,
                    order.oid,
                )
            )
            updates.append(
                OrderProjectionUpdate(
                    order_id,
                    order.status,
                    venue_order_id=order.oid,
                )
            )
        return tuple(updates)

    @staticmethod
    def _cumulative_fill_projections(
        remote: RemoteSnapshot,
        local: LocalSnapshot,
    ) -> Mapping[str, tuple[Decimal, Decimal | None]]:
        by_cloid = {order.intent.cloid: order for order in local.orders}
        by_oid: dict[str, list[TestnetOrder]] = {}
        new_fills: dict[str, list[RemoteFill]] = {}
        for order in local.orders:
            if order.venue_order_id is not None:
                by_oid.setdefault(order.venue_order_id, []).append(order)
        for fill in remote.fills:
            if fill.fill_id in local.fill_ids:
                continue
            if fill.cloid is not None:
                owner = by_cloid.get(fill.cloid)
            else:
                candidates = by_oid.get(fill.oid, ())
                owner = candidates[0] if len(candidates) == 1 else None
            if owner is None:
                raise ReconciliationError("clean plan contains an unowned remote fill")
            new_fills.setdefault(owner.intent.cloid, []).append(fill)
        return MappingProxyType(
            {
                order.intent.cloid: _cumulative_fill_projection(
                    order,
                    local,
                    new_fills.get(order.intent.cloid, ()),
                )
                for order in local.orders
            }
        )

    @staticmethod
    def _reconciliation_order_updates(
        remote: RemoteSnapshot,
        local: LocalSnapshot,
        plan: ReconciliationPlan,
    ) -> tuple[OrderProjectionUpdate, ...]:
        by_cloid = {order.intent.cloid: order for order in local.orders}
        fill_projections = RunScopedStore._cumulative_fill_projections(remote, local)
        updates: list[OrderProjectionUpdate] = []
        actions = {action.action_id: action for action in local.ambiguous_actions}
        for decision in plan.action_resolutions:
            action = actions[decision.action_id]
            if (
                action.kind is ActionKind.REPLACE
                and action.cloid is not None
                and decision.status is ActionAttemptStatus.CONFIRMED
            ):
                original = by_cloid[action.cloid]
                if not original.status.terminal:
                    filled, average = fill_projections[original.intent.cloid]
                    updates.append(
                        OrderProjectionUpdate(
                            original.intent.order_id,
                            OrderStatus.CANCELLED,
                            venue_order_id=original.venue_order_id,
                            filled_quantity=filled,
                            average_fill_price=average,
                        )
                    )
        for observed in remote.open_orders:
            if observed.cloid is None:
                raise ReconciliationError("clean plan contains an unowned open order")
            owner = by_cloid[observed.cloid]
            filled = owner.intent.quantity - observed.remaining_quantity
            projected_filled, average = fill_projections[observed.cloid]
            if projected_filled != filled:
                raise ReconciliationError("clean plan has divergent cumulative open-order fills")
            updates.append(
                OrderProjectionUpdate(
                    owner.intent.order_id,
                    (OrderStatus.PARTIALLY_FILLED if filled > 0 else OrderStatus.OPEN),
                    venue_order_id=observed.oid,
                    filled_quantity=filled,
                    average_fill_price=average,
                )
            )
        open_cloids = {order.cloid for order in remote.open_orders if order.cloid is not None}
        for cloid, status_order in remote.order_statuses.items():
            if status_order is None or cloid in open_cloids:
                continue
            owner = by_cloid[cloid]
            filled, average = fill_projections[cloid]
            if status_order.status is OrderStatus.FILLED and filled != owner.intent.quantity:
                raise ReconciliationError("clean plan has divergent cumulative terminal-order fills")
            updates.append(
                OrderProjectionUpdate(
                    owner.intent.order_id,
                    status_order.status,
                    venue_order_id=status_order.oid,
                    filled_quantity=filled,
                    average_fill_price=average,
                )
            )
        for decision in plan.action_resolutions:
            action = actions[decision.action_id]
            absent_cloid = (
                action.cloid
                if action.kind is ActionKind.SUBMIT
                else action.replacement_cloid
                if action.kind is ActionKind.REPLACE
                else None
            )
            if absent_cloid is not None and decision.status is ActionAttemptStatus.RESOLVED_NOT_SENT:
                owner = by_cloid[absent_cloid]
                updates.append(
                    OrderProjectionUpdate(
                        owner.intent.order_id,
                        OrderStatus.INVALID,
                        filled_quantity=owner.filled_quantity,
                        average_fill_price=owner.average_fill_price,
                    )
                )
        return tuple(updates)

    def apply_reconciliation(
        self,
        remote: RemoteSnapshot,
        plan: ReconciliationPlan,
    ) -> None:
        captured = datetime.fromtimestamp(remote.captured_at_ms / 1_000, tz=UTC)
        source_cursor = f"fills-ms:{remote.captured_at_ms}"
        open_orders = tuple(self._remote_order_payload(order) for order in remote.open_orders)
        local = self.local_snapshot()
        if plan.issues:
            self._store.apply_reconciliation_failure(
                self._run_id,
                positions=remote.positions,
                spot_balances=remote.spot_balances,
                equity=remote.equity,
                withdrawable=remote.withdrawable,
                open_orders=self._failure_observed_orders(remote),
                order_updates=self._failure_order_updates(remote, local),
                fills=self._reconciliation_fills(
                    remote,
                    local,
                    allow_unowned_observations=True,
                ),
                issues=tuple(
                    StoreReconciliationIssue(
                        issue.code,
                        {"identity": issue.identity},
                    )
                    for issue in plan.issues
                ),
                detected_at=captured,
                action_resolutions=tuple(
                    ReconciliationActionResolution(
                        action_id=decision.action_id,
                        status=decision.status,
                        proof=cast(Mapping[str, object], decision.proof),
                    )
                    for decision in plan.action_resolutions
                ),
                source_cursor=source_cursor,
                snapshot_id=plan.snapshot_hash,
            )
            return
        self._store.apply_reconciliation(
            self._run_id,
            positions=remote.positions,
            spot_balances=remote.spot_balances,
            equity=remote.equity,
            withdrawable=remote.withdrawable,
            open_orders=open_orders,
            order_updates=self._reconciliation_order_updates(remote, local, plan),
            fills=self._reconciliation_fills(remote, local),
            reconciled_at=captured,
            action_resolutions=tuple(
                ReconciliationActionResolution(
                    action_id=decision.action_id,
                    status=decision.status,
                    proof=cast(Mapping[str, object], decision.proof),
                )
                for decision in plan.action_resolutions
            ),
            source_cursor=source_cursor,
            snapshot_id=plan.snapshot_hash,
        )


def _absence_candidate_action_ids(
    local: LocalSnapshot,
    remote: RemoteSnapshot,
) -> frozenset[str]:
    remote_by_cloid = {order.cloid: order for order in remote.open_orders if order.cloid is not None}
    candidates: set[str] = set()
    for action in local.ambiguous_actions:
        if action.expires_after_ms is None or action.expires_after_ms - action.nonce > 60_000:
            continue
        if action.kind is ActionKind.CANCEL and action.cloid is not None:
            status_order = remote.order_statuses.get(action.cloid)
            if (
                action.cloid in remote_by_cloid
                and action.cloid in remote.order_statuses
                and status_order is not None
                and status_order.status in {OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED}
            ):
                candidates.add(action.action_id)
            continue
    return frozenset(candidates)


class ExchangeFirstReconciler:
    def __init__(
        self,
        adapter: SnapshotAdapter,
        store: ReconciliationStore,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        wait: Callable[[float], None] = time.sleep,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        absence_grace_seconds: float = 1.0,
    ) -> None:
        if (
            isinstance(absence_grace_seconds, bool)
            or not isinstance(absence_grace_seconds, (int, float))
            or not 0 < absence_grace_seconds <= 5
        ):
            raise ValueError("absence_grace_seconds must be within (0, 5]")
        self._adapter = adapter
        self._store = store
        self._monotonic = monotonic
        self._wait = wait
        self._clock_ms = clock_ms
        self._absence_grace_seconds = float(absence_grace_seconds)

    def _fetch(
        self,
        local: LocalSnapshot,
        *,
        captured_at_ms: int,
    ) -> RemoteSnapshot:
        query = tuple(order.intent.cloid for order in local.orders if not order.status.terminal)
        return fetch_remote_snapshot(
            self._adapter,
            queried_cloids=query,
            fill_cursor_ms=local.fill_cursor_ms,
            fill_overlap_ids=local.fill_overlap_ids,
            captured_at_ms=captured_at_ms,
        )

    def reconcile(self, *, captured_at_ms: int) -> ReconciliationPlan:
        local = self._store.local_snapshot()
        try:
            remote = self._fetch(local, captured_at_ms=captured_at_ms)
            plan = plan_reconciliation(local, remote)
            absence_candidates = _absence_candidate_action_ids(local, remote)
            if absence_candidates:
                actions = {
                    action.action_id: action
                    for action in local.ambiguous_actions
                    if action.action_id in absence_candidates
                }
                required_elapsed = (
                    max(
                        (action.expires_after_ms - action.nonce) / 1_000
                        for action in actions.values()
                        if action.expires_after_ms is not None
                    )
                    + self._absence_grace_seconds
                )
                started = self._monotonic()
                self._wait(required_elapsed)
                elapsed = self._monotonic() - started
                if elapsed >= required_elapsed:
                    local = self._store.local_snapshot()
                    second_captured_at_ms = max(
                        captured_at_ms + 1,
                        self._clock_ms(),
                    )
                    remote = self._fetch(
                        local,
                        captured_at_ms=second_captured_at_ms,
                    )
                    repeated_absence = absence_candidates.intersection(
                        _absence_candidate_action_ids(local, remote)
                    )
                    plan = plan_reconciliation(
                        local,
                        remote,
                        absence_proven_action_ids=frozenset(repeated_absence),
                    )
        except Exception as error:
            issue = ReconciliationIssue(
                "REMOTE_SNAPSHOT_UNAVAILABLE",
                type(error).__name__,
            )
            self._store.record_reconciliation_issue(issue)
            self._store.set_runtime_state(
                RuntimeState.MANUAL_REVIEW,
                reason=issue.code,
            )
            raise ReconciliationError("exchange-first reconciliation failed closed") from None
        try:
            self._store.apply_reconciliation(remote, plan)
        except Exception:
            raise ReconciliationError("durable reconciliation commit failed closed") from None
        return plan


__all__ = [
    "ExchangeFirstReconciler",
    "LocalSnapshot",
    "ReconciliationError",
    "ReconciliationEvent",
    "ReconciliationIssue",
    "ReconciliationPlan",
    "RemoteFill",
    "RemoteOrder",
    "RemoteSnapshot",
    "RunScopedStore",
    "fetch_remote_snapshot",
    "fetch_user_fills_contiguous",
    "parse_clearinghouse_state",
    "parse_open_orders",
    "parse_order_status",
    "parse_spot_clearinghouse_state",
    "parse_user_fills",
    "plan_reconciliation",
]
