"""Crash-safe Testnet action orchestration over a durable store Protocol.

The store owns atomic persistence and nonce allocation.  The engine persists an
AMBIGUOUS action before every network call, so a restart can only reconcile an
attempt; it can never infer that a retry is safe.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from types import MappingProxyType
from typing import NoReturn, Protocol, cast

from .adapter import ActionOutcome, AdapterError, OutcomeKind, parse_all_mids
from .canonical import JsonValue, decimal_text, deterministic_id, utc_text
from .config import TestnetRiskLimits
from .models import (
    ActionAttemptStatus,
    ActionKind,
    OrderStatus,
    RuntimeState,
    TestnetOrder,
    TestnetOrderIntent,
    require_order_transition,
    validate_cloid,
)
from .risk import (
    RiskDecision,
    evaluate_action_rate,
    evaluate_order_risk,
    market_is_fresh,
    reconciliation_is_fresh,
)


class ExecutionError(RuntimeError):
    """Execution stopped without exposing a venue payload or credential."""


class ActionRequiresReconciliation(ExecutionError):
    pass


class FinalSendRefused(ExecutionError):
    """A durable action was prepared but the serialized send gate refused I/O."""


class RiskRejected(ExecutionError):
    def __init__(self, decision: RiskDecision) -> None:
        super().__init__("Testnet action rejected by fail-closed risk controls")
        self.decision = decision


@dataclass(frozen=True, slots=True)
class ActionRecord:
    action_id: str
    kind: ActionKind
    cloid: str | None
    replacement_cloid: str | None
    nonce: int
    status: ActionAttemptStatus = ActionAttemptStatus.AMBIGUOUS
    code: str = "PREPARED_BEFORE_IO"
    outcome_kind: OutcomeKind | None = None
    venue_order_id: str | None = None
    filled_quantity: Decimal | None = None
    average_fill_price: Decimal | None = None
    expires_after_ms: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", ActionKind(self.kind))
        object.__setattr__(self, "status", ActionAttemptStatus(self.status))
        if self.cloid is not None:
            object.__setattr__(self, "cloid", validate_cloid(self.cloid))
        if self.replacement_cloid is not None:
            object.__setattr__(
                self,
                "replacement_cloid",
                validate_cloid(self.replacement_cloid),
            )
        if isinstance(self.nonce, bool) or not isinstance(self.nonce, int) or self.nonce <= 0:
            raise ValueError("action nonce must be a durable positive integer")
        if not isinstance(self.action_id, str) or len(self.action_id) != 64:
            raise ValueError("action_id must be a deterministic SHA-256")
        if not isinstance(self.code, str) or not self.code or len(self.code) > 128:
            raise ValueError("action code must be a bounded stable identifier")
        if self.outcome_kind is not None:
            object.__setattr__(self, "outcome_kind", OutcomeKind(self.outcome_kind))
        if self.expires_after_ms is not None and (
            isinstance(self.expires_after_ms, bool)
            or not isinstance(self.expires_after_ms, int)
            or self.expires_after_ms <= self.nonce
        ):
            raise ValueError("expires_after_ms must be a durable integer after nonce")

    def with_outcome(
        self,
        outcome: ActionOutcome | None,
        *,
        status: ActionAttemptStatus,
        code: str | None = None,
    ) -> ActionRecord:
        return replace(
            self,
            status=status,
            code=code or (outcome.code if outcome is not None else "ACTION_NOT_SENT"),
            outcome_kind=outcome.kind if outcome is not None else None,
            venue_order_id=outcome.venue_order_id if outcome is not None else None,
            filled_quantity=outcome.filled_quantity if outcome is not None else None,
            average_fill_price=(outcome.average_fill_price if outcome is not None else None),
        )

    def outcome(self) -> ActionOutcome | None:
        if self.outcome_kind is None:
            return None
        return ActionOutcome(
            self.outcome_kind,
            self.code,
            venue_order_id=self.venue_order_id,
            filled_quantity=self.filled_quantity,
            average_fill_price=self.average_fill_price,
        )


@dataclass(frozen=True, slots=True)
class PreparedAction:
    record: ActionRecord
    is_new: bool


@dataclass(frozen=True, slots=True)
class ExecutionSnapshot:
    runtime_state: RuntimeState
    orders: tuple[TestnetOrder, ...]
    positions: Mapping[str, Decimal]
    marks: Mapping[str, Decimal]
    last_reconciled_at: datetime | None
    submit_requests_in_last_minute: int = 0
    cancel_requests_in_last_minute: int = 0
    replace_requests_in_last_minute: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "runtime_state", RuntimeState(self.runtime_state))
        object.__setattr__(self, "orders", tuple(self.orders))
        object.__setattr__(self, "positions", MappingProxyType(dict(self.positions)))
        object.__setattr__(self, "marks", MappingProxyType(dict(self.marks)))
        for count in (
            self.submit_requests_in_last_minute,
            self.cancel_requests_in_last_minute,
            self.replace_requests_in_last_minute,
        ):
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ValueError("request counters must be non-negative integers")


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    action: ActionRecord
    order: TestnetOrder | None
    reused: bool = False

    @property
    def outcome(self) -> ActionOutcome | None:
        return self.action.outcome()


class ExecutionStore(Protocol):
    """Small persistence boundary; each mutator includes a durable audit record."""

    def execution_snapshot(self) -> ExecutionSnapshot: ...

    def get_order(self, cloid: str) -> TestnetOrder | None: ...

    def persist_intent(self, intent: TestnetOrderIntent) -> TestnetOrder: ...

    def update_order(
        self,
        order: TestnetOrder,
        *,
        audit_kind: str,
        audit_payload: Mapping[str, JsonValue],
    ) -> None: ...

    def get_action(self, action_id: str) -> ActionRecord | None: ...

    def prepare_action(
        self,
        *,
        action_id: str,
        kind: ActionKind,
        cloid: str | None,
        replacement_cloid: str | None,
        minimum_nonce: int,
        expires_after_delta_ms: int,
    ) -> PreparedAction: ...

    def complete_action(
        self,
        action: ActionRecord,
        *,
        order_updates: Sequence[TestnetOrder],
    ) -> None: ...

    def unresolved_actions(self) -> tuple[ActionRecord, ...]: ...

    def set_runtime_state(self, state: RuntimeState, *, reason: str) -> None: ...

    def account_kill_latched(self) -> bool: ...

    def final_send_permit(
        self,
        action_id: str,
    ) -> AbstractContextManager[ActionRecord]: ...

    def append_audit(
        self,
        kind: str,
        payload: Mapping[str, JsonValue],
    ) -> None: ...


class ExecutionAdapter(Protocol):
    @property
    def action_ttl_ms(self) -> int: ...

    def verify_live_constraints(self) -> object: ...

    def read_all_mids(self) -> object: ...

    def submit_order(
        self,
        intent: TestnetOrderIntent,
        *,
        nonce: int,
        constraint_verification: object,
    ) -> ActionOutcome: ...

    def cancel_by_cloid(self, *, coin: str, cloid: str, nonce: int) -> ActionOutcome: ...

    def replace_order(
        self,
        *,
        original_cloid: str,
        replacement: TestnetOrderIntent,
        nonce: int,
        constraint_verification: object,
    ) -> ActionOutcome: ...

    def schedule_cancel(self, *, cancel_at_ms: int, nonce: int) -> ActionOutcome: ...


class RecoveryReconciler(Protocol):
    def reconcile(self, *, captured_at_ms: int) -> object: ...


def _action_id(
    kind: ActionKind,
    *,
    cloid: str | None = None,
    replacement_cloid: str | None = None,
    discriminator: object = None,
) -> str:
    return deterministic_id(
        "hyperliquid_testnet_action_v1",
        kind.value,
        cloid,
        replacement_cloid,
        discriminator,
    )


def _order_version(order: TestnetOrder) -> str:
    return deterministic_id(
        "hyperliquid_testnet_order_version_v1",
        order.intent.cloid,
        order.status.value,
        decimal_text(order.filled_quantity),
        order.venue_order_id,
        utc_text(order.updated_at) if order.updated_at is not None else None,
    )


def _transition(
    order: TestnetOrder,
    status: OrderStatus,
    *,
    now: datetime,
    venue_order_id: str | None = None,
    filled_quantity: Decimal | None = None,
    average_fill_price: Decimal | None = None,
) -> TestnetOrder:
    require_order_transition(order.status, status)
    return replace(
        order,
        status=status,
        venue_order_id=(venue_order_id if venue_order_id is not None else order.venue_order_id),
        filled_quantity=(filled_quantity if filled_quantity is not None else order.filled_quantity),
        average_fill_price=(
            average_fill_price if average_fill_price is not None else order.average_fill_price
        ),
        updated_at=now,
    )


class TestnetExecutionEngine:
    def __init__(
        self,
        *,
        adapter: ExecutionAdapter,
        store: ExecutionStore,
        limits: TestnetRiskLimits,
        clock: Callable[[], datetime],
        reconciler: RecoveryReconciler,
    ) -> None:
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._adapter = adapter
        self._store = store
        self._limits = limits
        self._clock = clock
        self._reconciler = reconciler

    def _existing(self, action_id: str, *, result_cloid: str | None) -> ExecutionResult | None:
        record = self._store.get_action(action_id)
        if record is None:
            return None
        order = self._store.get_order(result_cloid) if result_cloid is not None else None
        if record.status is ActionAttemptStatus.AMBIGUOUS:
            self._fail_closed("AMBIGUOUS_ACTION_REQUIRES_RECONCILIATION")
            raise ActionRequiresReconciliation("existing Testnet action is ambiguous; blind resend refused")
        return ExecutionResult(record, order, reused=True)

    def _prepare(
        self,
        *,
        action_id: str,
        kind: ActionKind,
        cloid: str | None,
        replacement_cloid: str | None,
    ) -> ActionRecord:
        prepared = self._store.prepare_action(
            action_id=action_id,
            kind=kind,
            cloid=cloid,
            replacement_cloid=replacement_cloid,
            minimum_nonce=self._now_ms(),
            expires_after_delta_ms=self._adapter.action_ttl_ms,
        )
        record = prepared.record
        if (
            not prepared.is_new
            or record.action_id != action_id
            or record.kind is not kind
            or record.cloid != cloid
            or record.replacement_cloid != replacement_cloid
            or record.status is not ActionAttemptStatus.AMBIGUOUS
        ):
            self._store.set_runtime_state(RuntimeState.MANUAL_REVIEW, reason="ACTION_STORE_DIVERGENCE")
            raise ActionRequiresReconciliation("durable action preparation diverged")
        return record

    def _complete(
        self,
        record: ActionRecord,
        outcome: ActionOutcome,
        *,
        order_updates: Sequence[TestnetOrder],
    ) -> ActionRecord:
        if outcome.ambiguous:
            status = ActionAttemptStatus.AMBIGUOUS
        elif outcome.kind is OutcomeKind.REJECTED:
            status = ActionAttemptStatus.REJECTED
        else:
            status = ActionAttemptStatus.CONFIRMED
        completed = record.with_outcome(outcome, status=status)
        self._store.complete_action(completed, order_updates=tuple(order_updates))
        if status is ActionAttemptStatus.AMBIGUOUS:
            self._fail_closed("ACTION_OUTCOME_AMBIGUOUS")
        return completed

    def _unexpected_failure(self) -> NoReturn:
        self._fail_closed("ACTION_CALL_INTERRUPTED")
        raise ActionRequiresReconciliation(
            "Testnet action call interrupted; durable attempt requires reconciliation"
        ) from None

    def _now_ms(self) -> int:
        return int(self._clock().timestamp() * 1000)

    def _fail_closed(self, reason: str) -> None:
        state = self._store.execution_snapshot().runtime_state
        if state not in {RuntimeState.KILLED, RuntimeState.MANUAL_REVIEW}:
            self._store.set_runtime_state(RuntimeState.PAUSED, reason=reason)

    def _final_risk_inputs_are_fresh(self, *, market_received_at: datetime) -> bool:
        snapshot = self._store.execution_snapshot()
        now = self._clock()
        return market_is_fresh(
            now=now,
            market_received_at=market_received_at,
            limits=self._limits,
        ) and reconciliation_is_fresh(
            now=now,
            last_reconciled_at=snapshot.last_reconciled_at,
            limits=self._limits,
        )

    def _resolve_not_sent(
        self,
        record: ActionRecord,
        *,
        order_updates: Sequence[TestnetOrder],
    ) -> None:
        resolved = record.with_outcome(
            None,
            status=ActionAttemptStatus.RESOLVED_NOT_SENT,
            code="ACTION_NOT_SENT",
        )
        self._store.complete_action(resolved, order_updates=tuple(order_updates))

    def _risk_reject(self, order: TestnetOrder, decision: RiskDecision) -> None:
        invalid = _transition(order, OrderStatus.INVALID, now=self._clock())
        self._store.update_order(
            invalid,
            audit_kind="TESTNET_RISK_REJECTED",
            audit_payload=cast(Mapping[str, JsonValue], decision.to_dict()),
        )
        raise RiskRejected(decision)

    def _require_account_not_killed(self) -> None:
        if not self._store.account_kill_latched():
            return
        if self._store.execution_snapshot().runtime_state is not RuntimeState.KILLED:
            self._store.set_runtime_state(
                RuntimeState.KILLED,
                reason="ACCOUNT_KILL_LATCHED",
            )
        raise ExecutionError("durable account-scoped kill latch blocks new exposure")

    def _invalidate_if_requested(
        self,
        cloid: str,
        *,
        audit_kind: str,
        audit_payload: Mapping[str, JsonValue],
    ) -> None:
        durable = self._store.get_order(cloid)
        if durable is None or durable.status is not OrderStatus.REQUESTED:
            return
        invalid = _transition(durable, OrderStatus.INVALID, now=self._clock())
        self._store.update_order(
            invalid,
            audit_kind=audit_kind,
            audit_payload=audit_payload,
        )

    def submit(
        self,
        intent: TestnetOrderIntent,
        *,
        market_received_at: datetime,
    ) -> ExecutionResult:
        del market_received_at
        self._require_account_not_killed()
        action_id = _action_id(ActionKind.SUBMIT, cloid=intent.cloid)
        existing = self._existing(action_id, result_cloid=intent.cloid)
        if existing is not None:
            return existing
        order = self._store.persist_intent(intent)
        if order.intent != intent or order.status is not OrderStatus.REQUESTED:
            self._store.set_runtime_state(RuntimeState.MANUAL_REVIEW, reason="ORDER_INTENT_DIVERGENCE")
            raise ExecutionError("persisted Testnet intent diverges")

        try:
            constraint_verification = self._adapter.verify_live_constraints()
            raw_marks = self._adapter.read_all_mids()
            fresh_market_received_at = self._clock()
            fresh_marks = parse_all_mids(raw_marks)
        except Exception:
            self._invalidate_if_requested(
                intent.cloid,
                audit_kind="TESTNET_MARKET_READ_FAILED",
                audit_payload={"cloid": intent.cloid},
            )
            self._store.append_audit(
                "TESTNET_MARKET_READ_FAILED",
                {"cloid": intent.cloid},
            )
            raise ExecutionError("fresh Testnet marks are unavailable") from None

        snapshot = self._store.execution_snapshot()
        other_orders = tuple(
            candidate for candidate in snapshot.orders if candidate.intent.cloid != intent.cloid
        )
        decision = evaluate_order_risk(
            intent,
            now=self._clock(),
            market_received_at=fresh_market_received_at,
            last_reconciled_at=snapshot.last_reconciled_at,
            runtime_state=snapshot.runtime_state,
            current_positions=snapshot.positions,
            marks=fresh_marks,
            open_orders=other_orders,
            submit_requests_in_last_minute=snapshot.submit_requests_in_last_minute,
            limits=self._limits,
        )
        if not decision.accepted:
            self._risk_reject(order, decision)

        try:
            record = self._prepare(
                action_id=action_id,
                kind=ActionKind.SUBMIT,
                cloid=intent.cloid,
                replacement_cloid=None,
            )
        except Exception:
            self._invalidate_if_requested(
                intent.cloid,
                audit_kind="TESTNET_SUBMIT_PREPARE_FAILED",
                audit_payload={"action_id": action_id, "cloid": intent.cloid},
            )
            raise
        submitted = _transition(order, OrderStatus.SUBMITTED, now=self._clock())
        self._store.update_order(
            submitted,
            audit_kind="TESTNET_SUBMISSION_PENDING",
            audit_payload={"action_id": action_id, "cloid": intent.cloid},
        )
        final_freshness_refused = False
        try:
            with self._store.final_send_permit(record.action_id):
                if not self._final_risk_inputs_are_fresh(
                    market_received_at=fresh_market_received_at
                ):
                    final_freshness_refused = True
                    raise FinalSendRefused("final Testnet risk inputs are stale")
                outcome = self._adapter.submit_order(
                    intent,
                    nonce=record.nonce,
                    constraint_verification=constraint_verification,
                )
        except FinalSendRefused:
            invalid = _transition(submitted, OrderStatus.INVALID, now=self._clock())
            self._resolve_not_sent(record, order_updates=(invalid,))
            if final_freshness_refused:
                self._fail_closed("FINAL_RISK_INPUTS_STALE")
            else:
                self._require_account_not_killed()
            raise ExecutionError("Testnet submit was blocked before network I/O") from None
        except AdapterError:
            invalid = _transition(submitted, OrderStatus.INVALID, now=self._clock())
            self._resolve_not_sent(record, order_updates=(invalid,))
            raise ExecutionError("Testnet action was not sent") from None
        except Exception:
            self._unexpected_failure()

        if outcome.kind is OutcomeKind.RESTING and outcome.venue_order_id is not None:
            final = _transition(
                submitted,
                OrderStatus.OPEN,
                now=self._clock(),
                venue_order_id=outcome.venue_order_id,
            )
        elif (
            outcome.kind is OutcomeKind.FILLED
            and outcome.venue_order_id is not None
            and outcome.filled_quantity is not None
            and outcome.average_fill_price is not None
            and Decimal(0) < outcome.filled_quantity <= intent.quantity
        ):
            target = (
                OrderStatus.FILLED
                if outcome.filled_quantity == intent.quantity
                else OrderStatus.PARTIALLY_FILLED
            )
            final = _transition(
                submitted,
                target,
                now=self._clock(),
                venue_order_id=outcome.venue_order_id,
                filled_quantity=outcome.filled_quantity,
                average_fill_price=outcome.average_fill_price,
            )
        elif outcome.kind is OutcomeKind.REJECTED:
            final = _transition(submitted, OrderStatus.REJECTED, now=self._clock())
        else:
            if not outcome.ambiguous:
                outcome = ActionOutcome(OutcomeKind.UNKNOWN, "SUBMIT_OUTCOME_UNKNOWN")
            final = _transition(submitted, OrderStatus.UNKNOWN, now=self._clock())
        completed = self._complete(record, outcome, order_updates=(final,))
        return ExecutionResult(completed, final)

    def cancel(self, *, cloid: str) -> ExecutionResult:
        normalized = validate_cloid(cloid)
        order = self._store.get_order(normalized)
        if order is None:
            raise ExecutionError("cancel requires a persisted order")
        action_id = _action_id(
            ActionKind.CANCEL,
            cloid=normalized,
            discriminator=_order_version(order),
        )
        existing = self._existing(action_id, result_cloid=normalized)
        if existing is not None:
            return existing
        if order.status not in {
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
        }:
            raise ExecutionError(
                "cancel requires an authoritatively acknowledged/open order; reconcile first"
            )
        snapshot = self._store.execution_snapshot()
        rate = evaluate_action_rate(
            ActionKind.CANCEL,
            requests_in_last_minute=snapshot.cancel_requests_in_last_minute,
            limits=self._limits,
        )
        if not rate.accepted:
            self._store.append_audit(
                "TESTNET_CANCEL_RATE_REJECTED",
                {"cloid": normalized, "limit": rate.limit},
            )
            raise ExecutionError("Testnet cancel rate limit reached")
        record = self._prepare(
            action_id=action_id,
            kind=ActionKind.CANCEL,
            cloid=normalized,
            replacement_cloid=None,
        )
        pending = _transition(order, OrderStatus.CANCEL_REQUESTED, now=self._clock())
        self._store.update_order(
            pending,
            audit_kind="TESTNET_CANCEL_PENDING",
            audit_payload={"action_id": action_id, "cloid": normalized},
        )
        coin = order.intent.instrument.split(":")[1]
        try:
            with self._store.final_send_permit(record.action_id):
                outcome = self._adapter.cancel_by_cloid(
                    coin=coin,
                    cloid=normalized,
                    nonce=record.nonce,
                )
        except FinalSendRefused:
            unknown = _transition(pending, OrderStatus.UNKNOWN, now=self._clock())
            self._resolve_not_sent(record, order_updates=(unknown,))
            raise ExecutionError("Testnet cancel was blocked before network I/O") from None
        except AdapterError:
            unknown = _transition(pending, OrderStatus.UNKNOWN, now=self._clock())
            self._resolve_not_sent(record, order_updates=(unknown,))
            self._store.set_runtime_state(RuntimeState.PAUSED, reason="CANCEL_NOT_SENT_RECONCILE")
            raise ExecutionError("Testnet cancel was not sent") from None
        except Exception:
            self._unexpected_failure()
        if outcome.kind is OutcomeKind.CANCELLED:
            final = _transition(pending, OrderStatus.CANCELLED, now=self._clock())
        else:
            if outcome.kind not in {OutcomeKind.REJECTED, OutcomeKind.AMBIGUOUS, OutcomeKind.UNKNOWN}:
                outcome = ActionOutcome(OutcomeKind.UNKNOWN, "CANCEL_OUTCOME_UNKNOWN")
            final = _transition(pending, OrderStatus.UNKNOWN, now=self._clock())
        completed = self._complete(record, outcome, order_updates=(final,))
        if outcome.kind is OutcomeKind.REJECTED:
            self._store.set_runtime_state(RuntimeState.PAUSED, reason="CANCEL_REJECTED_RECONCILE")
        return ExecutionResult(completed, final)

    def replace(
        self,
        *,
        original_cloid: str,
        replacement: TestnetOrderIntent,
        market_received_at: datetime,
    ) -> ExecutionResult:
        del market_received_at
        self._require_account_not_killed()
        original_id = validate_cloid(original_cloid)
        original = self._store.get_order(original_id)
        if original is None or original.status not in {
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
        }:
            raise ExecutionError("replace requires an authoritatively active original order")
        if original.intent.instrument != replacement.instrument:
            raise ExecutionError("replace cannot change the instrument")
        action_id = _action_id(
            ActionKind.REPLACE,
            cloid=original_id,
            replacement_cloid=replacement.cloid,
        )
        existing = self._existing(action_id, result_cloid=replacement.cloid)
        if existing is not None:
            return existing
        replacement_order = self._store.persist_intent(replacement)
        if replacement_order.intent != replacement or replacement_order.status is not OrderStatus.REQUESTED:
            raise ExecutionError("replace requires a coherent persisted replacement order")

        try:
            constraint_verification = self._adapter.verify_live_constraints()
            raw_marks = self._adapter.read_all_mids()
            fresh_market_received_at = self._clock()
            fresh_marks = parse_all_mids(raw_marks)
        except Exception:
            self._invalidate_if_requested(
                replacement.cloid,
                audit_kind="TESTNET_REPLACE_MARKET_READ_FAILED",
                audit_payload={"cloid": replacement.cloid},
            )
            self._store.append_audit(
                "TESTNET_MARKET_READ_FAILED",
                {"cloid": replacement.cloid},
            )
            raise ExecutionError("fresh Testnet marks are unavailable") from None

        snapshot = self._store.execution_snapshot()
        rate = evaluate_action_rate(
            ActionKind.REPLACE,
            requests_in_last_minute=snapshot.replace_requests_in_last_minute,
            limits=self._limits,
        )
        if not rate.accepted:
            invalid = _transition(replacement_order, OrderStatus.INVALID, now=self._clock())
            self._store.update_order(
                invalid,
                audit_kind="TESTNET_REPLACE_RATE_REJECTED",
                audit_payload={"cloid": replacement.cloid, "limit": rate.limit},
            )
            raise ExecutionError("Testnet replace rate limit reached")
        other_orders = tuple(
            candidate for candidate in snapshot.orders if candidate.intent.cloid != replacement.cloid
        )
        decision = evaluate_order_risk(
            replacement,
            now=self._clock(),
            market_received_at=fresh_market_received_at,
            last_reconciled_at=snapshot.last_reconciled_at,
            runtime_state=snapshot.runtime_state,
            current_positions=snapshot.positions,
            marks=fresh_marks,
            open_orders=other_orders,
            submit_requests_in_last_minute=0,
            limits=self._limits,
            replaced_order_id=original.intent.order_id,
        )
        if not decision.accepted:
            self._risk_reject(replacement_order, decision)

        try:
            record = self._prepare(
                action_id=action_id,
                kind=ActionKind.REPLACE,
                cloid=original_id,
                replacement_cloid=replacement.cloid,
            )
        except Exception:
            self._invalidate_if_requested(
                replacement.cloid,
                audit_kind="TESTNET_REPLACE_PREPARE_FAILED",
                audit_payload={
                    "action_id": action_id,
                    "original_cloid": original_id,
                    "replacement_cloid": replacement.cloid,
                },
            )
            raise
        submitted = _transition(replacement_order, OrderStatus.SUBMITTED, now=self._clock())
        self._store.update_order(
            submitted,
            audit_kind="TESTNET_REPLACE_PENDING",
            audit_payload={
                "action_id": action_id,
                "original_cloid": original_id,
                "replacement_cloid": replacement.cloid,
            },
        )
        final_freshness_refused = False
        try:
            with self._store.final_send_permit(record.action_id):
                if not self._final_risk_inputs_are_fresh(
                    market_received_at=fresh_market_received_at
                ):
                    final_freshness_refused = True
                    raise FinalSendRefused("final Testnet risk inputs are stale")
                outcome = self._adapter.replace_order(
                    original_cloid=original_id,
                    replacement=replacement,
                    nonce=record.nonce,
                    constraint_verification=constraint_verification,
                )
        except FinalSendRefused:
            invalid = _transition(submitted, OrderStatus.INVALID, now=self._clock())
            self._resolve_not_sent(record, order_updates=(invalid,))
            if final_freshness_refused:
                self._fail_closed("FINAL_RISK_INPUTS_STALE")
            else:
                self._require_account_not_killed()
            raise ExecutionError("Testnet replace was blocked before network I/O") from None
        except AdapterError:
            invalid = _transition(submitted, OrderStatus.INVALID, now=self._clock())
            self._resolve_not_sent(record, order_updates=(invalid,))
            raise ExecutionError("Testnet replace was not sent") from None
        except Exception:
            self._unexpected_failure()

        old_updates: tuple[TestnetOrder, ...] = ()
        if outcome.kind in {OutcomeKind.REPLACED, OutcomeKind.FILLED}:
            old_pending = _transition(
                original,
                OrderStatus.CANCEL_REQUESTED,
                now=self._clock(),
            )
            old_final = _transition(old_pending, OrderStatus.CANCELLED, now=self._clock())
            old_updates = (old_final,)
            if outcome.kind is OutcomeKind.REPLACED and outcome.venue_order_id is not None:
                final = _transition(
                    submitted,
                    OrderStatus.OPEN,
                    now=self._clock(),
                    venue_order_id=outcome.venue_order_id,
                )
            elif (
                outcome.kind is OutcomeKind.FILLED
                and outcome.venue_order_id is not None
                and outcome.filled_quantity is not None
                and outcome.average_fill_price is not None
                and Decimal(0) < outcome.filled_quantity <= replacement.quantity
            ):
                target = (
                    OrderStatus.FILLED
                    if outcome.filled_quantity == replacement.quantity
                    else OrderStatus.PARTIALLY_FILLED
                )
                final = _transition(
                    submitted,
                    target,
                    now=self._clock(),
                    venue_order_id=outcome.venue_order_id,
                    filled_quantity=outcome.filled_quantity,
                    average_fill_price=outcome.average_fill_price,
                )
            else:
                outcome = ActionOutcome(OutcomeKind.UNKNOWN, "REPLACE_OUTCOME_UNKNOWN")
                final = _transition(submitted, OrderStatus.UNKNOWN, now=self._clock())
                old_updates = (_transition(original, OrderStatus.UNKNOWN, now=self._clock()),)
        elif outcome.kind is OutcomeKind.REJECTED:
            final = _transition(submitted, OrderStatus.REJECTED, now=self._clock())
        else:
            if not outcome.ambiguous:
                outcome = ActionOutcome(OutcomeKind.UNKNOWN, "REPLACE_OUTCOME_UNKNOWN")
            final = _transition(submitted, OrderStatus.UNKNOWN, now=self._clock())
            old_updates = (_transition(original, OrderStatus.UNKNOWN, now=self._clock()),)
        completed = self._complete(
            record,
            outcome,
            order_updates=(*old_updates, final),
        )
        return ExecutionResult(completed, final)

    def _arm_deadman(self, *, cancel_at_ms: int, emergency: bool) -> ExecutionResult:
        action_id = _action_id(
            ActionKind.SCHEDULE_CANCEL,
            discriminator=(("emergency-kill-v2", cancel_at_ms) if emergency else ("routine", cancel_at_ms)),
        )
        existing = self._existing(action_id, result_cloid=None)
        if existing is not None:
            return existing
        if not emergency:
            snapshot = self._store.execution_snapshot()
            rate = evaluate_action_rate(
                ActionKind.SCHEDULE_CANCEL,
                requests_in_last_minute=snapshot.cancel_requests_in_last_minute,
                limits=self._limits,
            )
            if not rate.accepted:
                self._store.append_audit(
                    "TESTNET_DEADMAN_RATE_REJECTED",
                    {"cancel_at_ms": cancel_at_ms, "limit": rate.limit},
                )
                raise ExecutionError("Testnet dead-man rate limit reached")
        record = self._prepare(
            action_id=action_id,
            kind=ActionKind.SCHEDULE_CANCEL,
            cloid=None,
            replacement_cloid=None,
        )
        try:
            with self._store.final_send_permit(record.action_id):
                outcome = self._adapter.schedule_cancel(
                    cancel_at_ms=cancel_at_ms,
                    nonce=record.nonce,
                )
        except FinalSendRefused:
            self._resolve_not_sent(record, order_updates=())
            raise ExecutionError("Testnet dead-man was blocked before network I/O") from None
        except AdapterError:
            self._resolve_not_sent(record, order_updates=())
            raise ExecutionError("Testnet dead-man action was not sent") from None
        except Exception:
            self._unexpected_failure()
        if outcome.kind not in {
            OutcomeKind.DEADMAN_ARMED,
            OutcomeKind.REJECTED,
            OutcomeKind.AMBIGUOUS,
            OutcomeKind.UNKNOWN,
        }:
            outcome = ActionOutcome(OutcomeKind.UNKNOWN, "DEADMAN_OUTCOME_UNKNOWN")
        completed = self._complete(record, outcome, order_updates=())
        return ExecutionResult(completed, None)

    def arm_deadman(self, *, cancel_at_ms: int) -> ExecutionResult:
        return self._arm_deadman(cancel_at_ms=cancel_at_ms, emergency=False)

    def pause(self, *, reason: str) -> None:
        if not isinstance(reason, str) or not reason or len(reason) > 128:
            raise ValueError("pause reason must be a bounded stable identifier")
        self._fail_closed(reason)
        self._store.append_audit("TESTNET_RUNTIME_PAUSED", {"reason": reason})

    def kill(self, *, cancel_at_ms: int, reason: str = "OPERATOR_KILL") -> ExecutionResult:
        if not isinstance(reason, str) or not reason or len(reason) > 128:
            raise ValueError("kill reason must be a bounded stable identifier")
        # This durable state transition precedes every possible network operation.
        self._store.set_runtime_state(RuntimeState.KILLED, reason=reason)
        if not self._store.account_kill_latched():
            raise ExecutionError("durable account-scoped kill latch was not proven")
        self._store.append_audit(
            "TESTNET_KILL_PERSISTED",
            {"cancel_at_ms": cancel_at_ms, "reason": reason},
        )
        return self.enforce_persisted_kill(cancel_at_ms=cancel_at_ms)

    def enforce_persisted_kill(self, *, cancel_at_ms: int) -> ExecutionResult:
        """Arm the emergency DMS generation for this exact requested deadline."""

        if self._store.execution_snapshot().runtime_state is not RuntimeState.KILLED:
            raise ExecutionError("emergency dead-man enforcement requires durable KILLED")
        self._store.append_audit(
            "TESTNET_KILL_ENFORCEMENT_STARTED",
            {"cancel_at_ms": cancel_at_ms},
        )
        # The deadline-scoped key deduplicates concurrent owners without reusing a
        # historical confirmation or rejection for a newly requested protection.
        return self._arm_deadman(cancel_at_ms=cancel_at_ms, emergency=True)

    def recover(self, *, captured_at_ms: int) -> object:
        unresolved = self._store.unresolved_actions()
        if unresolved:
            self._fail_closed("RECOVERY_RECONCILIATION_REQUIRED")
            self._store.append_audit(
                "TESTNET_RECOVERY_STARTED",
                {"unresolved_action_count": len(unresolved)},
            )
        return self._reconciler.reconcile(captured_at_ms=captured_at_ms)


__all__ = [
    "ActionRecord",
    "ActionRequiresReconciliation",
    "ExecutionAdapter",
    "ExecutionError",
    "ExecutionResult",
    "ExecutionSnapshot",
    "ExecutionStore",
    "FinalSendRefused",
    "PreparedAction",
    "RecoveryReconciler",
    "RiskRejected",
    "TestnetExecutionEngine",
]
