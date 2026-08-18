from __future__ import annotations

import gc
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import zip_longest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

import pandas as pd

from hyperlab.backtest.costs import adverse_fee_bps
from hyperlab.backtest.protocol import canonical_sha256
from hyperlab.paper.models import (
    AlertSeverity,
    DecisionAction,
    DecisionIntent,
    LedgerEntry,
    MarketEvent,
    MarketExecutionPolicy,
    OrderIntent,
    OrderSide,
    OrderStatus,
    PaperEvent,
    PaperEventType,
    PaperOrder,
    PaperOrderType,
    PaperProjection,
    PaperRunConfig,
    PaperState,
    StoredPaperEvent,
    TimeInForce,
    decimal_text,
    decimal_value,
    deterministic_id,
    ensure_json_object,
    keyed_uniform,
    parse_utc,
    require_transition,
    utc_text,
)
from hyperlab.paper.reducer import (
    apply_event,
    replay_projection,
    transaction_ledger_amounts,
)
from hyperlab.paper.risk import evaluate_order_risk
from hyperlab.paper.store import (
    AppendResult,
    ConcurrentWriteError,
    IntegrityError,
    IntegrityReport,
    PaperStore,
    RunConflictError,
    RunNotFoundError,
)


@dataclass(frozen=True, slots=True)
class PaperCommandResult:
    append: AppendResult
    projection: PaperProjection


@dataclass(frozen=True, slots=True)
class _VerifiedPaperState:
    head_identity: tuple[str, str, str, int, str, int, str, int, str]
    report: IntegrityReport
    projection: PaperProjection


class _LedgerReconciliationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PaperStartupPreparation:
    started: PaperCommandResult
    verification: _VerifiedPaperState


def _raise_if_interrupted(should_stop: Callable[[], bool] | None) -> None:
    if should_stop is not None and should_stop():
        raise InterruptedError("paper startup interrupted")


@dataclass(slots=True)
class _MarketLiquidity:
    maker_trade_remaining: Decimal
    maker_depth_remaining: dict[OrderSide, Decimal]
    taker_depth_remaining: dict[OrderSide, Decimal]

    @classmethod
    def from_market(cls, market: MarketEvent) -> _MarketLiquidity:
        return cls(
            maker_trade_remaining=market.trade_quantity or Decimal(0),
            maker_depth_remaining={
                OrderSide.BUY: market.bid_depth,
                OrderSide.SELL: market.ask_depth,
            },
            taker_depth_remaining={
                OrderSide.BUY: market.ask_depth,
                OrderSide.SELL: market.bid_depth,
            },
        )


class PaperEngine:
    """Offline-only event-sourced simulated execution engine.

    The public command surface accepts strategy decisions and normalized public
    market events.  It deliberately has no venue client, credential, signer, or
    transport dependency.
    """

    def __init__(self, store: PaperStore, config: PaperRunConfig) -> None:
        if not isinstance(store, PaperStore):
            raise TypeError("store must be a PaperStore")
        if not isinstance(config, PaperRunConfig):
            raise TypeError("config must be a PaperRunConfig")
        self.store = store
        self.config = config
        self.store.initialize()

    @property
    def run_id(self) -> str:
        return self.config.run_id

    def start(
        self,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> PaperCommandResult:
        return self.prepare_startup(should_stop=should_stop).started

    def prepare_startup(
        self,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> PaperStartupPreparation:
        """Verify one exact durable head once before startup reconciliation."""

        _raise_if_interrupted(should_stop)
        initial = PaperProjection(
            run_id=self.run_id,
            config_hash=self.config.config_hash,
            initial_cash=self.config.initial_cash,
        )
        try:
            durable = self.store.get_run(self.run_id)
        except RunNotFoundError:
            durable = self.store.create_run(self.config, initial)
        if durable.config_hash != self.config.config_hash:
            raise RunConflictError("paper run configuration drift is forbidden")

        verification: _VerifiedPaperState | None = None
        if durable.event_sequence:
            try:
                verification = self._verify_durable_state(should_stop=should_stop)
            except ValueError as error:
                damaged = self.projection()
                self.reconcile(as_of=damaged.last_received_at or self.config.validation_started_at)
                if isinstance(error, _LedgerReconciliationError):
                    raise ValueError("paper ledger does not reconcile: " + str(error)) from error
                raise

        input_id = deterministic_id("paper_input_run_started", self.run_id)
        input_payload = {
            "config_hash": self.config.config_hash,
            "input_type": "RUN_START",
            "run_id": self.run_id,
        }
        duplicate = self._deduplicate(input_id, input_payload)
        if duplicate is not None:
            started = duplicate
        else:
            projection = self.projection()
            run_payload: dict[str, object] = {
                "config_hash": self.config.config_hash,
                "run_kind": self.config.run_kind,
                "strategy_hash": self.config.strategy_hash,
            }
            if self.config.strategies:
                run_payload["portfolio_id"] = self.config.portfolio_id
                run_payload["strategies"] = [
                    {
                        "strategy_config_hash": strategy.strategy_config_hash,
                        "strategy_hash": strategy.strategy_hash,
                        "strategy_id": strategy.strategy_id,
                        "strategy_name": strategy.strategy_name,
                    }
                    for strategy in self.config.strategy_configs
                ]
            event = PaperEvent.create(
                run_id=self.run_id,
                event_type=PaperEventType.RUN_STARTED,
                occurred_at=self.config.validation_started_at,
                received_at=self.config.validation_started_at,
                causation_id=None,
                correlation_id=self.run_id,
                payload=run_payload,
            )
            transaction_id = deterministic_id("paper_initial_capital", self.run_id)
            entries = (
                LedgerEntry.create(
                    run_id=self.run_id,
                    event_id=event.event_id,
                    transaction_id=transaction_id,
                    account="asset:cash",
                    amount=self.config.initial_cash,
                    ordinal=0,
                ),
                LedgerEntry.create(
                    run_id=self.run_id,
                    event_id=event.event_id,
                    transaction_id=transaction_id,
                    account="equity:initial_capital",
                    amount=-self.config.initial_cash,
                    ordinal=1,
                ),
            )
            started = self._commit(
                input_id=input_id,
                input_payload=input_payload,
                base=projection,
                events=(event,),
                explicit_ledger=entries,
            )
        if verification is None:
            verification = self._verify_durable_state(should_stop=should_stop)
        _raise_if_interrupted(should_stop)
        return PaperStartupPreparation(started=started, verification=verification)

    def _verify_durable_state(
        self,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> _VerifiedPaperState:
        before = self.store.get_run(self.run_id)
        report = self.store.verify_integrity(self.run_id, should_stop=should_stop)
        replayed = self.replay(should_stop=should_stop)
        errors = (
            self._ledger_reconciliation_errors(replayed)
            if should_stop is None
            else self._ledger_reconciliation_errors(
                replayed,
                should_stop=should_stop,
            )
        )
        if errors:
            raise _LedgerReconciliationError("; ".join(errors))
        after = self.store.get_run(self.run_id)
        if after.head_identity != before.head_identity:
            raise ConcurrentWriteError("paper durable head changed during startup verification")
        _raise_if_interrupted(should_stop)
        return _VerifiedPaperState(
            head_identity=after.head_identity,
            report=report,
            projection=replayed,
        )

    def projection(self) -> PaperProjection:
        projection = self.store.get_projection(self.run_id)
        if projection.config_hash != self.config.config_hash:
            raise RunConflictError("durable paper projection has a different frozen config hash")
        return projection

    def start_runtime_session(
        self,
        *,
        as_of: datetime,
        session_id: str,
        generation: int,
        replaces_unclosed_session_id: str | None = None,
    ) -> PaperCommandResult:
        """Persist one OS-lease-bound runtime admission session."""

        started_at = parse_utc(utc_text(as_of))
        session = self._evidence_hash(session_id, label="runtime session_id")
        replacement = (
            self._evidence_hash(
                replaces_unclosed_session_id,
                label="replaces_unclosed_session_id",
            )
            if replaces_unclosed_session_id is not None
            else None
        )
        if isinstance(generation, bool) or not isinstance(generation, int) or generation <= 0:
            raise ValueError("runtime session generation must be a positive integer")
        input_id = deterministic_id(
            "paper_runtime_session_start_input",
            self.run_id,
            session,
        )
        payload = {
            "generation": generation,
            "input_type": "RUNTIME_SESSION_STARTED",
            "replaces_unclosed_session_id": replacement,
            "session_id": session,
            "started_at": utc_text(started_at),
        }
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        active_session = projection.runtime_session_id if projection.runtime_session_active else None
        if replacement != active_session:
            raise ValueError("runtime session replacement differs from durable active session")
        if generation != projection.runtime_session_generation + 1:
            raise ValueError("runtime session generation must advance by exactly one")
        if projection.last_received_at is not None and started_at < projection.last_received_at:
            raise ValueError("runtime session start precedes durable paper state")
        event = self._event(
            PaperEventType.RUNTIME_SESSION_STARTED,
            at=started_at,
            received_at=started_at,
            causation_id=session,
            correlation_id=input_id,
            payload=payload,
        )
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=(event,),
        )

    def stop_runtime_session(
        self,
        *,
        as_of: datetime,
        session_id: str,
        generation: int,
        reason: str,
    ) -> PaperCommandResult:
        """Persist a clean runtime stop while the exact OS lease is still held."""

        stopped_at = parse_utc(utc_text(as_of))
        session = self._evidence_hash(session_id, label="runtime session_id")
        normalized_reason = reason.strip().upper()
        if normalized_reason not in {"NORMAL_COMPLETION", "COOPERATIVE_STOP"}:
            raise ValueError("unsupported clean runtime session stop reason")
        if isinstance(generation, bool) or not isinstance(generation, int) or generation <= 0:
            raise ValueError("runtime session generation must be a positive integer")
        input_id = deterministic_id(
            "paper_runtime_session_stop_input",
            self.run_id,
            session,
        )
        payload = {
            "generation": generation,
            "input_type": "RUNTIME_SESSION_STOPPED",
            "reason": normalized_reason,
            "session_id": session,
            "stopped_at": utc_text(stopped_at),
        }
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        if (
            not projection.runtime_session_active
            or projection.runtime_session_id != session
            or projection.runtime_session_generation != generation
        ):
            raise ValueError("runtime session stop differs from durable active session")
        if projection.last_received_at is not None and stopped_at < projection.last_received_at:
            raise ValueError("runtime session stop precedes durable paper state")
        event = self._event(
            PaperEventType.RUNTIME_SESSION_STOPPED,
            at=stopped_at,
            received_at=stopped_at,
            causation_id=session,
            correlation_id=input_id,
            payload=payload,
        )
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=(event,),
        )

    def submit_decision(
        self,
        decision: DecisionIntent,
        market: MarketEvent | Mapping[str, MarketEvent],
        *,
        processed_at: datetime | None = None,
    ) -> PaperCommandResult:
        if decision.run_id != self.run_id:
            raise ValueError("decision belongs to another paper run")
        strategy_config = None
        if self.config.strategies:
            if decision.strategy_id is None:
                raise ValueError("multi-strategy decisions require explicit strategy identity")
            try:
                strategy_config = self.config.strategy_config(decision.strategy_id)
            except KeyError as error:
                raise ValueError("decision references an unknown strategy_id") from error
            if (
                decision.strategy_name != strategy_config.strategy_name
                or decision.strategy_hash != strategy_config.strategy_hash
                or decision.strategy_config_hash != strategy_config.strategy_config_hash
            ):
                raise ValueError("decision strategy differs from the frozen strategy configuration")
        elif decision.strategy_id is not None or decision.strategy_name != self.config.strategy_name:
            raise ValueError("decision strategy differs from the frozen strategy")
        markets = {market.instrument: market} if isinstance(market, MarketEvent) else dict(market)
        if not markets or any(key != value.instrument for key, value in markets.items()):
            raise ValueError("decision markets must be keyed by their canonical instrument")
        by_id = {item.event_id: item for item in markets.values()}
        primary_market = by_id.get(decision.market_event_id)
        if primary_market is None:
            raise ValueError("decision is not bound to a supplied public market event")
        if decision.received_at != max(item.received_at for item in markets.values()):
            raise ValueError("decision received_at must equal the latest supplied observation")
        if any(item.event_id not in decision.observed_event_ids for item in markets.values()):
            raise ValueError("every supplied market must appear in observed_event_ids")
        if any(order.instrument not in markets for order in decision.orders):
            raise ValueError("every order requires a same-instrument public market observation")
        processed = parse_utc(utc_text(processed_at or decision.decided_at))
        if processed < decision.decided_at:
            raise ValueError("decision processing time cannot precede decided_at")
        input_payload = {
            "decision": decision.to_dict(),
            "input_type": "STRATEGY_DECISION",
            "markets": [markets[key].to_dict() for key in sorted(markets)],
            "processed_at": utc_text(processed),
        }
        duplicate = self._deduplicate(decision.decision_id, input_payload)
        if duplicate is not None:
            return duplicate

        projection = self.projection()
        if projection.last_received_at is not None and processed < projection.last_received_at:
            raise ValueError("decision processing time cannot precede durable paper state")
        if projection.state is PaperState.MANUAL_REVIEW:
            raise IntegrityError(self.store.verify_integrity(self.run_id, raise_on_error=False))
        decision_state = (
            projection.strategy_projection(decision.strategy_id).state
            if decision.strategy_id is not None
            else projection.state
        )
        if decision.action is DecisionAction.ENTRY and decision_state is not PaperState.FLAT:
            raise ValueError("ENTRY decisions are accepted only from FLAT")
        if decision.action is DecisionAction.EXIT and decision_state not in {
            PaperState.HEDGED,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.EMERGENCY_FLATTEN,
        }:
            raise ValueError("EXIT decisions require HEDGED, PAUSED, REDUCE_ONLY, or EMERGENCY_FLATTEN")

        events: list[PaperEvent] = []
        working = projection.clone()

        def emit(event_type: PaperEventType, payload: Mapping[str, object]) -> PaperEvent:
            event_payload = dict(payload)
            if decision.strategy_id is not None:
                event_payload["strategy_id"] = decision.strategy_id
            event = self._event(
                event_type,
                at=processed,
                received_at=processed,
                causation_id=decision.decision_id,
                correlation_id=decision.decision_id,
                payload=event_payload,
                ordinal=len(events),
            )
            apply_event(working, event)
            events.append(event)
            return event

        emit(PaperEventType.DECISION_RECORDED, {"decision": decision.to_dict()})
        if decision.action is DecisionAction.HOLD:
            return self._commit(
                input_id=decision.decision_id,
                input_payload=input_payload,
                base=projection,
                events=tuple(events),
            )

        planning_state = (
            PaperState.ENTRY_PLANNED if decision.action is DecisionAction.ENTRY else PaperState.EXIT_PLANNED
        )
        working_state = (
            working.strategy_projection(decision.strategy_id).state
            if decision.strategy_id is not None
            else working.state
        )
        protective_exit_state = (
            working_state
            if decision.action is DecisionAction.EXIT
            and working_state
            in {
                PaperState.PAUSED,
                PaperState.REDUCE_ONLY,
                PaperState.EMERGENCY_FLATTEN,
            }
            else None
        )
        if protective_exit_state is None:
            emit(
                PaperEventType.STATE_TRANSITIONED,
                self._transition_payload(working_state, planning_state, "strategy decision"),
            )
        accepted = 0
        for order in decision.orders:
            if decision.action is DecisionAction.EXIT and not order.reduce_only:
                raise ValueError("every EXIT order must be explicitly reduce_only")
            emit(
                PaperEventType.ORDER_PLANNED,
                {"action": cast(DecisionAction, decision.action).value, "order": order.to_dict()},
            )
            order_market = markets[order.instrument]
            strategy_risk = (
                evaluate_order_risk(
                    working,
                    order,
                    order_market,
                    strategy_config.risk,
                    strategy_id=decision.strategy_id,
                )
                if strategy_config is not None
                else None
            )
            portfolio_risk = evaluate_order_risk(
                working,
                order,
                order_market,
                self.config.risk,
            )
            risk = (
                strategy_risk if strategy_risk is not None and not strategy_risk.accepted else portfolio_risk
            )
            rejection_scope = (
                "STRATEGY"
                if strategy_risk is not None and not strategy_risk.accepted
                else ("PORTFOLIO" if not portfolio_risk.accepted else None)
            )
            if risk.accepted and self.config.execution.cost_schedule is not None:
                try:
                    self.config.execution.cost_schedule.lookup(
                        pd.Timestamp(order_market.received_at),
                        order.instrument,
                    )
                except ValueError as error:
                    rejection_scope = "EXECUTION"
                    risk = type(risk)(
                        accepted=False,
                        reasons=(f"point-in-time cost schedule unavailable: {error}",),
                        order_notional=risk.order_notional,
                        projected_gross_notional=risk.projected_gross_notional,
                        projected_net_notional=risk.projected_net_notional,
                        projected_instrument_notional=risk.projected_instrument_notional,
                        projected_instrument_quantity=risk.projected_instrument_quantity,
                        projected_active_orders=risk.projected_active_orders,
                        risk_reducing=risk.risk_reducing,
                    )
            risk_payload = risk.to_dict()
            if strategy_risk is not None:
                risk_payload.update(
                    {
                        "portfolio": portfolio_risk.to_dict(),
                        "rejection_scope": rejection_scope,
                        "strategy": strategy_risk.to_dict(),
                    }
                )
            if not risk.accepted:
                emit(
                    PaperEventType.RISK_REJECTED,
                    {"order_id": order.order_id, "risk": risk_payload},
                )
                emit(
                    PaperEventType.ALERT_RAISED,
                    self._alert_payload(
                        code="RISK_REJECTED",
                        severity=AlertSeverity.WARNING,
                        message="; ".join(risk.reasons),
                        causation_id=order.order_id,
                        at=processed,
                    ),
                )
                continue
            accepted += 1
            ack_due = processed + timedelta(
                milliseconds=(
                    self.config.execution.ack_latency_ms
                    + (order.leg_number - 1) * self.config.execution.leg_delay_ms
                )
            )
            emit(
                PaperEventType.RISK_ACCEPTED,
                {
                    "ack_due_at": utc_text(ack_due),
                    "order_id": order.order_id,
                    "risk": risk_payload,
                },
            )
            if ack_due <= processed:
                self._ack_or_reject(
                    working,
                    events,
                    order,
                    order_market,
                    decision.decision_id,
                    received_at=processed,
                )

        if decision.action is DecisionAction.ENTRY:
            target = PaperState.LEG_1_PENDING if accepted else PaperState.FLAT
        elif protective_exit_state in {
            PaperState.EMERGENCY_FLATTEN,
            PaperState.REDUCE_ONLY,
        }:
            target = protective_exit_state
        elif protective_exit_state is not None:
            target = PaperState.EXIT_PENDING if accepted else protective_exit_state
        else:
            target = PaperState.EXIT_PENDING if accepted else PaperState.HEDGED
        final_state = (
            working.strategy_projection(decision.strategy_id).state
            if decision.strategy_id is not None
            else working.state
        )
        if target is not final_state:
            emit(
                PaperEventType.STATE_TRANSITIONED,
                self._transition_payload(final_state, target, "orders planned"),
            )
        return self._commit(
            input_id=decision.decision_id,
            input_payload=input_payload,
            base=projection,
            events=tuple(events),
        )

    def process_market(
        self,
        market: MarketEvent,
        *,
        processed_at: datetime | None = None,
        execution_policy: MarketExecutionPolicy | str = MarketExecutionPolicy.EXECUTE,
    ) -> PaperCommandResult:
        processed = parse_utc(utc_text(processed_at or market.received_at))
        if processed < market.received_at:
            raise ValueError("market processing time cannot precede source received_at")
        policy = MarketExecutionPolicy(execution_policy)
        input_payload = {
            "execution_policy": policy.value,
            "input_type": "PUBLIC_MARKET_EVENT",
            "market": market.to_dict(),
            "processed_at": utc_text(processed),
        }
        durable = self.store.get_input(self.run_id, market.event_id)
        duplicate_payload: Mapping[str, object] = input_payload
        if (
            durable is not None
            and durable.payload.get("input_type") == "PUBLIC_MARKET_EVENT"
            and durable.payload.get("market") == market.to_dict()
        ):
            duplicate_payload = durable.payload
        duplicate = self._deduplicate(market.event_id, duplicate_payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        previous_market = projection.last_market_received_at_by_instrument.get(market.instrument)
        if previous_market is not None and market.received_at < previous_market:
            raise ValueError("public market source chronology regressed for instrument")
        if projection.last_received_at is not None and processed < projection.last_received_at:
            raise ValueError("market processing time cannot precede durable paper state")
        if (
            policy is MarketExecutionPolicy.EXECUTE
            and projection.last_public_source_received_at is not None
            and market.received_at < projection.last_public_source_received_at
        ):
            raise ValueError("executable market cannot precede the public source receipt watermark")
        events: list[PaperEvent] = []
        working = projection.clone()

        def emit(
            event_type: PaperEventType,
            payload: Mapping[str, object],
            *,
            causation_id: str | None = None,
        ) -> PaperEvent:
            event_payload = dict(payload)
            raw_order_id = event_payload.get("order_id")
            if isinstance(raw_order_id, str) and raw_order_id in working.orders:
                event_payload = self._order_event_payload(
                    working.orders[raw_order_id],
                    event_payload,
                )
            event = self._event(
                event_type,
                at=processed,
                received_at=processed,
                causation_id=causation_id or market.event_id,
                correlation_id=causation_id or market.event_id,
                payload=event_payload,
                ordinal=len(events),
            )
            apply_event(working, event)
            events.append(event)
            return event

        if market.stale or market.gap:
            code = "MARKET_GAP" if market.gap else "STALE_MARKET_DATA"
            emit(
                PaperEventType.PUBLIC_SOURCE_HEALTH_RECORDED,
                {
                    "gap": market.gap,
                    "instrument": market.instrument,
                    "market_event_id": market.event_id,
                    "source_connection_epoch": market.source_connection_epoch,
                    "source_connection_id": market.source_connection_id,
                    "source_event_kind": market.source_event_kind,
                    "source_received_at": utc_text(market.received_at),
                    "stale": market.stale,
                    "tradable": market.tradable,
                    "execution_policy": policy.value,
                },
            )
            episode_id = deterministic_id(
                "paper_market_gap_episode" if market.gap else "paper_stale_feed_episode",
                self.run_id,
                utc_text(working.last_market_received_at or self.config.validation_started_at),
            )
            alert_payload = self._alert_payload(
                code=code,
                severity=AlertSeverity.CRITICAL,
                message="matching stopped because public market state is not trustworthy",
                causation_id=episode_id,
                at=processed,
            )
            if not self._alert_is_durable(str(alert_payload["alert_id"])):
                emit(PaperEventType.ALERT_RAISED, alert_payload)
            if working.state not in {
                PaperState.EMERGENCY_FLATTEN,
                PaperState.MANUAL_REVIEW,
                PaperState.PAUSED,
            }:
                emit(
                    PaperEventType.STATE_TRANSITIONED,
                    self._transition_payload(working.state, PaperState.PAUSED, code),
                )
            self._protect_entry_orders(
                working,
                events,
                at=processed,
                causation_id=market.event_id,
                reason=code,
            )
            return self._commit(
                input_id=market.event_id,
                input_payload=input_payload,
                base=projection,
                events=tuple(events),
            )

        if not market.tradable:
            # Connection-health frames deliberately carry the last public book
            # for source observability. They are not executable observations:
            # persist explicit health evidence without changing marks, risk,
            # orders, positions, fees, or PnL.
            emit(
                PaperEventType.PUBLIC_SOURCE_HEALTH_RECORDED,
                {
                    "gap": market.gap,
                    "instrument": market.instrument,
                    "market_event_id": market.event_id,
                    "source_connection_epoch": market.source_connection_epoch,
                    "source_connection_id": market.source_connection_id,
                    "source_event_kind": market.source_event_kind,
                    "source_received_at": utc_text(market.received_at),
                    "stale": market.stale,
                    "tradable": False,
                    "execution_policy": policy.value,
                },
            )
            return self._commit(
                input_id=market.event_id,
                input_payload=input_payload,
                base=projection,
                events=tuple(events),
            )

        emit(
            PaperEventType.MARK_RECORDED,
            {
                "instrument": market.instrument,
                "price": decimal_text((market.bid_price + market.ask_price) / Decimal(2)),
                "source_received_at": utc_text(market.received_at),
                "execution_policy": policy.value,
                "execution_suppressed": policy is not MarketExecutionPolicy.EXECUTE,
            },
        )

        self._apply_protective_risk_state(
            working,
            events,
            at=processed,
            causation_id=market.event_id,
        )
        if policy is not MarketExecutionPolicy.EXECUTE:
            return self._commit(
                input_id=market.event_id,
                input_payload=input_payload,
                base=projection,
                events=tuple(events),
            )

        if working.state in {
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.EMERGENCY_FLATTEN,
        }:
            self._protect_entry_orders(
                working,
                events,
                at=processed,
                causation_id=market.event_id,
                reason=f"state {working.state.value}",
            )

        for order in self._ordered_orders(working):
            if order.intent.instrument != market.instrument:
                continue
            if order.status is OrderStatus.CANCEL_PENDING:
                timeout_wins = (
                    order.expires_at is not None
                    and order.expires_at <= processed
                    and (order.cancel_effective_at is None or order.expires_at <= order.cancel_effective_at)
                )
                if timeout_wins:
                    emit(
                        (
                            PaperEventType.ORDER_NO_FILL
                            if order.filled_quantity == 0
                            else PaperEventType.ORDER_EXPIRED
                        ),
                        {"order_id": order.intent.order_id, "reason": "timeout before cancel"},
                        causation_id=order.intent.order_id,
                    )
                    continue
                if order.cancel_effective_at is not None and order.cancel_effective_at <= processed:
                    emit(
                        PaperEventType.ORDER_CANCELLED,
                        {"order_id": order.intent.order_id},
                        causation_id=order.intent.order_id,
                    )

        for order in self._ordered_orders(working):
            if order.intent.instrument != market.instrument:
                continue
            if (
                order.status is OrderStatus.RISK_ACCEPTED
                and order.active_at is not None
                and order.active_at <= processed
            ):
                if order.action is DecisionAction.ENTRY and working.state in {
                    PaperState.PAUSED,
                    PaperState.REDUCE_ONLY,
                    PaperState.EMERGENCY_FLATTEN,
                }:
                    self._reject_accepted_order(
                        working,
                        events,
                        order,
                        market,
                        processed_at=processed,
                        reason=f"state {working.state.value} blocks new exposure",
                    )
                else:
                    self._ack_or_reject(
                        working,
                        events,
                        order.intent,
                        market,
                        market.event_id,
                        received_at=processed,
                    )

        liquidity = _MarketLiquidity.from_market(market)
        for order in self._ordered_orders(working):
            if order.intent.instrument != market.instrument or order.status not in {
                OrderStatus.ACKED,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.CANCEL_PENDING,
            }:
                continue
            if order.action is DecisionAction.ENTRY and working.state in {
                PaperState.PAUSED,
                PaperState.REDUCE_ONLY,
                PaperState.EMERGENCY_FLATTEN,
            }:
                continue
            if order.expires_at is not None and order.expires_at <= processed:
                terminal = (
                    PaperEventType.ORDER_NO_FILL
                    if order.filled_quantity == 0
                    else PaperEventType.ORDER_EXPIRED
                )
                emit(
                    terminal,
                    {"order_id": order.intent.order_id, "reason": "timeout"},
                    causation_id=order.intent.order_id,
                )
                continue
            if order.intent.created_at >= market.received_at:
                continue
            if order.active_at is None or (
                order.active_at + timedelta(milliseconds=self.config.execution.fill_latency_ms) > processed
            ):
                continue
            self._match_order(
                working,
                events,
                order,
                market,
                liquidity,
                processed_at=processed,
            )

        self._derive_lifecycle(working, events, market, processed_at=processed)
        self._apply_protective_risk_state(
            working,
            events,
            at=processed,
            causation_id=market.event_id,
        )
        return self._commit(
            input_id=market.event_id,
            input_payload=input_payload,
            base=projection,
            events=tuple(events),
        )

    def request_cancel(
        self,
        order_id: str,
        *,
        requested_at: datetime,
        input_id: str | None = None,
    ) -> PaperCommandResult:
        command_id = input_id or deterministic_id(
            "paper_cancel_input", self.run_id, order_id, utc_text(requested_at)
        )
        payload = {
            "input_type": "CANCEL_REQUEST",
            "order_id": order_id,
            "requested_at": utc_text(requested_at),
        }
        duplicate = self._deduplicate(command_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        try:
            order = projection.orders[order_id]
        except KeyError as error:
            raise ValueError(f"unknown paper order {order_id}") from error
        if order.status not in {OrderStatus.ACKED, OrderStatus.PARTIALLY_FILLED}:
            raise ValueError("only an active simulated order may be cancelled")
        effective = requested_at + timedelta(milliseconds=self.config.execution.cancel_latency_ms)
        event = self._event(
            PaperEventType.CANCEL_REQUESTED,
            at=requested_at,
            received_at=requested_at,
            causation_id=command_id,
            correlation_id=order.intent.decision_id,
            payload=self._order_event_payload(
                order,
                {"cancel_effective_at": utc_text(effective), "order_id": order_id},
            ),
        )
        return self._commit(
            input_id=command_id,
            input_payload=payload,
            base=projection,
            events=(event,),
        )

    def post_funding(
        self,
        *,
        instrument: str,
        amount: Decimal,
        occurred_at: datetime,
        source_event_id: str,
        funding_rate: Decimal | None = None,
        funding_interval_seconds: int | None = None,
        rate_kind: str | None = None,
        mark_price: Decimal | None = None,
        source_mark_price: Decimal | None = None,
        oracle_price: Decimal | None = None,
        position_quantity: Decimal | None = None,
        mark_source: str | None = None,
        source_observation_id: str | None = None,
        received_at: datetime | None = None,
        processed_at: datetime | None = None,
        applicability: str = "APPLIED",
        source_activation_cutoff: datetime | None = None,
    ) -> PaperCommandResult:
        normalized_amount = decimal_value(amount, label="funding amount")
        received = received_at or occurred_at
        processed = parse_utc(utc_text(processed_at or received))
        processed_text = utc_text(processed)
        occurred_text = utc_text(occurred_at)
        received_text = utc_text(received)
        if received < occurred_at:
            raise ValueError("funding received_at cannot precede funding_time")
        if processed < received:
            raise ValueError("funding processing time cannot precede source received_at")

        normalized_applicability = applicability.strip().upper()
        if normalized_applicability not in {"APPLIED", "PRE_ACTIVATION_IGNORED"}:
            raise ValueError("unsupported funding applicability")
        activation_text = utc_text(source_activation_cutoff) if source_activation_cutoff is not None else None
        if source_activation_cutoff is not None:
            first_reconciliation = next(
                iter(
                    self.store.iter_inputs(
                        self.run_id,
                        input_type="RECONCILE",
                    )
                ),
                None,
            )
            if first_reconciliation is None or first_reconciliation.payload.get("as_of") != activation_text:
                raise ValueError("funding source activation cutoff differs from first durable reconciliation")
        if normalized_applicability == "PRE_ACTIVATION_IGNORED":
            if source_activation_cutoff is None or occurred_at >= source_activation_cutoff:
                raise ValueError("pre-activation funding requires a later durable source activation cutoff")
        elif source_activation_cutoff is not None and occurred_at < source_activation_cutoff:
            raise ValueError("funding before source activation cannot be applied")

        required_metadata_values = (
            funding_rate,
            funding_interval_seconds,
            rate_kind,
            position_quantity,
            mark_source,
            source_observation_id,
            received_at,
        )
        detailed = (
            mark_price is not None
            or source_mark_price is not None
            or source_activation_cutoff is not None
            or normalized_applicability != "APPLIED"
            or any(value is not None for value in required_metadata_values)
        )
        if detailed and any(value is None for value in required_metadata_values):
            raise ValueError("detailed public funding requires complete calculation metadata")

        normalized_rate: Decimal | None = None
        normalized_mark: Decimal | None = None
        normalized_source_mark: Decimal | None = None
        normalized_oracle: Decimal | None = None
        normalized_position: Decimal | None = None
        normalized_rate_kind: str | None = None
        normalized_mark_source: str | None = None
        normalized_observation: str | None = None
        if detailed:
            assert funding_rate is not None
            assert funding_interval_seconds is not None
            assert rate_kind is not None
            assert position_quantity is not None
            assert mark_source is not None
            assert source_observation_id is not None
            if (
                isinstance(funding_interval_seconds, bool)
                or not isinstance(funding_interval_seconds, int)
                or funding_interval_seconds <= 0
            ):
                raise ValueError("funding_interval_seconds must be a positive integer")
            normalized_rate = decimal_value(funding_rate, label="funding_rate")
            normalized_position = decimal_value(position_quantity, label="funding position")
            if mark_price is not None:
                normalized_mark = decimal_value(
                    mark_price,
                    label="funding mark",
                    positive=True,
                )
            elif normalized_position != 0 and normalized_applicability != "PRE_ACTIVATION_IGNORED":
                raise ValueError("non-flat applied funding requires a positive mark")
            normalized_rate_kind = rate_kind.strip()
            normalized_mark_source = mark_source.strip()
            normalized_observation = source_observation_id.strip()
            if not normalized_rate_kind or any(char.isspace() for char in normalized_rate_kind):
                raise ValueError("funding rate_kind must be a stable identifier")
            if not normalized_mark_source or any(char.isspace() for char in normalized_mark_source):
                raise ValueError("funding mark_source must be a stable identifier")
            if not normalized_observation or any(char.isspace() for char in normalized_observation):
                raise ValueError("funding source_observation_id must be a stable identifier")
        if source_mark_price is not None:
            normalized_source_mark = decimal_value(
                source_mark_price,
                label="funding source_mark_price",
                positive=True,
            )
        if oracle_price is not None:
            normalized_oracle = decimal_value(
                oracle_price,
                label="funding oracle_price",
                positive=True,
            )

        input_id = deterministic_id("paper_funding_input", self.run_id, source_event_id)
        payload: dict[str, object] = {
            "amount": decimal_text(normalized_amount),
            "input_type": "PUBLIC_FUNDING_SETTLEMENT",
            "instrument": instrument,
            "occurred_at": occurred_text,
            "source_event_id": source_event_id,
            "processed_at": processed_text,
        }
        if detailed:
            assert normalized_rate is not None
            assert normalized_position is not None
            assert normalized_rate_kind is not None
            assert normalized_mark_source is not None
            assert normalized_observation is not None
            assert funding_interval_seconds is not None
            payload.update(
                {
                    "applicability": normalized_applicability,
                    "funding_interval_seconds": funding_interval_seconds,
                    "funding_rate": decimal_text(normalized_rate),
                    "mark_source": normalized_mark_source,
                    "oracle_price": (
                        decimal_text(normalized_oracle) if normalized_oracle is not None else None
                    ),
                    "position_quantity": decimal_text(normalized_position),
                    "rate_kind": normalized_rate_kind,
                    "received_at": received_text,
                    "source_activation_cutoff": activation_text,
                    "source_mark_price": (
                        decimal_text(normalized_source_mark) if normalized_source_mark is not None else None
                    ),
                    "source_observation_id": normalized_observation,
                }
            )
            if normalized_mark is not None:
                payload["mark_price"] = decimal_text(normalized_mark)

        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        if projection.last_received_at is not None and processed < projection.last_received_at:
            raise ValueError("funding processing time cannot precede durable paper state")
        current_position = projection.positions.get(instrument, Decimal(0))
        historical: PaperProjection | None = None
        if detailed:
            assert normalized_rate is not None
            assert normalized_position is not None
            historical = self.store.get_projection_before_received_at(
                self.run_id,
                before=occurred_at,
            )
            historical_position = (
                historical.positions.get(instrument, Decimal(0)) if historical is not None else Decimal(0)
            )
            if normalized_applicability == "PRE_ACTIVATION_IGNORED":
                if normalized_position != 0 or normalized_amount != 0:
                    raise ValueError("pre-activation funding must record zero Paper exposure and amount")
            else:
                if normalized_position != historical_position:
                    raise ValueError("funding position differs from durable position before settlement")
                expected_amount = Decimal(0)
                if normalized_position != 0:
                    assert normalized_mark is not None
                    expected_amount = -(normalized_position * normalized_mark * normalized_rate)
                if normalized_amount != expected_amount:
                    raise ValueError("funding amount differs from rate, mark, and position")
            if normalized_mark_source == "PUBLIC_SETTLEMENT_MARK":
                if normalized_source_mark is None or normalized_mark != normalized_source_mark:
                    raise ValueError("public settlement mark source is not exact")
            elif normalized_mark_source == "PUBLIC_SETTLEMENT_ORACLE":
                if normalized_oracle is None or normalized_mark != normalized_oracle:
                    raise ValueError("public settlement oracle source is not exact")
            elif normalized_mark_source == "DURABLE_PUBLIC_BBO_MID":
                historical_mark = (
                    historical.public_bbo_mids.get(instrument) if historical is not None else None
                )
                historical_mark_at = (
                    historical.public_bbo_received_at_by_instrument.get(instrument)
                    if historical is not None
                    else None
                )
                if (
                    normalized_mark is None
                    or historical_mark_at is None
                    or normalized_mark != historical_mark
                ):
                    raise ValueError("durable BBO funding mark is not exact")
                if historical_mark_at > occurred_at:
                    raise ValueError("durable BBO funding mark is after funding_time")
                if occurred_at - historical_mark_at > timedelta(seconds=self.config.risk.stale_after_seconds):
                    raise ValueError("durable BBO funding mark is stale at funding_time")
            elif normalized_mark_source == "FLAT_NO_MARK":
                if normalized_mark is not None or normalized_position != 0:
                    raise ValueError("FLAT_NO_MARK requires zero position and no mark")
            else:
                raise ValueError("unsupported funding mark_source")
        elif current_position == 0 and normalized_amount != 0:
            raise ValueError("non-zero funding cannot be posted while flat")
        if projection.strategy_projections:
            if not detailed:
                if any(
                    strategy.positions.get(instrument, Decimal(0)) != 0
                    for strategy in projection.strategy_projections.values()
                ):
                    raise ValueError("multi-strategy funding requires detailed deterministic attribution")
                strategy_amounts = {
                    strategy_id: Decimal(0) for strategy_id in projection.strategy_projections
                }
            else:
                assert normalized_rate is not None
                if normalized_mark is None and any(
                    strategy.positions.get(instrument, Decimal(0)) != 0
                    for strategy in projection.strategy_projections.values()
                ):
                    raise ValueError("offsetting strategy funding requires an explicit public mark")
                strategy_amounts = {}
                for strategy_id in sorted(projection.strategy_projections):
                    historical_strategy = (
                        historical.strategy_projections.get(strategy_id) if historical is not None else None
                    )
                    strategy_position = (
                        historical_strategy.positions.get(instrument, Decimal(0))
                        if historical_strategy is not None
                        else Decimal(0)
                    )
                    strategy_amounts[strategy_id] = (
                        Decimal(0)
                        if normalized_applicability == "PRE_ACTIVATION_IGNORED" or strategy_position == 0
                        else -(strategy_position * cast(Decimal, normalized_mark) * normalized_rate)
                    )
            if sum(strategy_amounts.values(), Decimal(0)) != normalized_amount:
                raise ValueError("strategy funding attribution differs from account funding")
            payload["strategy_amounts"] = {
                strategy_id: decimal_text(amount) for strategy_id, amount in strategy_amounts.items()
            }

        event_payload = dict(payload)
        event_payload.pop("input_type")
        event_payload["funding_time"] = event_payload.pop("occurred_at")
        event_payload["source_received_at"] = received_text
        event = self._event(
            PaperEventType.FUNDING_POSTED,
            # Public REST settlement history is necessarily observed after the
            # economic funding timestamp. Apply it only at causal receipt time
            # while retaining the exact funding_time in the immutable payload.
            at=processed,
            received_at=processed,
            causation_id=source_event_id,
            correlation_id=input_id,
            payload=event_payload,
        )
        working = projection.clone()
        events = [event]
        apply_event(working, event)
        self._apply_protective_risk_state(
            working,
            events,
            at=processed,
            causation_id=source_event_id,
        )

        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=tuple(events),
        )

    @staticmethod
    def _evidence_hash(value: str, *, label: str) -> str:
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError(f"{label} must be a lowercase SHA-256 digest")
        return value

    def record_stress_result(
        self,
        *,
        artifact_hash: str,
        stressed_net_pnl: Decimal,
        evaluated_at: datetime,
    ) -> PaperCommandResult:
        artifact = self._evidence_hash(artifact_hash, label="stress artifact_hash")
        projection = self.projection()
        input_id = deterministic_id("paper_stress_result_input", self.run_id, artifact)
        payload = {
            "artifact_hash": artifact,
            "config_hash": self.config.config_hash,
            "evaluated_event_head_hash": projection.last_event_hash,
            "evaluated_event_sequence": projection.last_sequence,
            "evaluated_at": utc_text(evaluated_at),
            "input_type": "STRESS_RESULT",
            "stressed_net_pnl": decimal_text(stressed_net_pnl),
        }
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        event = self._event(
            PaperEventType.STRESS_RESULT_RECORDED,
            at=evaluated_at,
            received_at=evaluated_at,
            causation_id=artifact,
            correlation_id=input_id,
            payload=payload,
        )
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=(event,),
        )

    def record_resilience_exercise(
        self,
        *,
        exercise: str,
        artifact_hash: str,
        exercised_at: datetime,
    ) -> PaperCommandResult:
        normalized = exercise.strip().upper()
        allowed = {"RESTART", "DISCONNECT", "PARTIAL_FILL", "CRASH_RECOVERY"}
        if normalized not in allowed:
            raise ValueError(f"resilience exercise must be one of {sorted(allowed)}")
        artifact = self._evidence_hash(artifact_hash, label="resilience artifact_hash")
        input_id = deterministic_id("paper_resilience_exercise_input", self.run_id, normalized, artifact)
        payload = {
            "artifact_hash": artifact,
            "config_hash": self.config.config_hash,
            "exercise": normalized,
            "exercised_at": utc_text(exercised_at),
            "input_type": "RESILIENCE_EXERCISE",
        }
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        event = self._event(
            PaperEventType.RESILIENCE_EXERCISE_RECORDED,
            at=exercised_at,
            received_at=exercised_at,
            causation_id=artifact,
            correlation_id=input_id,
            payload=payload,
        )
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=(event,),
        )

    def record_observation_coverage(
        self,
        *,
        artifact_hash: str,
        window_start: datetime,
        window_end: datetime,
        continuous: bool,
        recorded_at: datetime,
    ) -> PaperCommandResult:
        artifact = self._evidence_hash(artifact_hash, label="coverage artifact_hash")
        if window_start != self.config.validation_started_at:
            raise ValueError("coverage must start at the frozen validation window")
        if window_end < window_start or recorded_at < window_end:
            raise ValueError("coverage timestamps are not chronological")
        input_id = deterministic_id("paper_observation_coverage_input", self.run_id, artifact)
        payload = {
            "artifact_hash": artifact,
            "config_hash": self.config.config_hash,
            "continuous": bool(continuous),
            "input_type": "OBSERVATION_COVERAGE",
            "window_end": utc_text(window_end),
            "window_start": utc_text(window_start),
        }
        if self.config.schema_version >= 2:
            payload["recorded_at"] = utc_text(recorded_at)
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        event = self._event(
            PaperEventType.OBSERVATION_COVERAGE_RECORDED,
            at=recorded_at,
            received_at=recorded_at,
            causation_id=artifact,
            correlation_id=input_id,
            payload=payload,
        )
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=(event,),
        )

    def process_timer(self, *, as_of: datetime) -> PaperCommandResult:
        input_id = deterministic_id("paper_timer", self.run_id, utc_text(as_of))
        payload = {"as_of": utc_text(as_of), "input_type": "TIMER"}
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        if projection.last_received_at is not None and as_of < projection.last_received_at:
            raise ValueError("timer cannot precede the last received paper event")
        working = projection.clone()
        events: list[PaperEvent] = []

        def emit(event_type: PaperEventType, event_payload: Mapping[str, object]) -> None:
            event = self._event(
                event_type,
                at=as_of,
                received_at=as_of,
                causation_id=input_id,
                correlation_id=input_id,
                payload=event_payload,
                ordinal=len(events),
            )
            apply_event(working, event)
            events.append(event)

        emit(PaperEventType.TIMER_TICKED, {"as_of": utc_text(as_of)})
        last_market = working.last_market_received_at
        required_instruments = set(self.config.required_instruments)
        required_instruments.update(working.positions)
        for strategy in working.strategy_projections.values():
            required_instruments.update(strategy.positions)
        required_instruments.update(order.intent.instrument for order in working.active_orders)
        stale_instruments = sorted(
            instrument
            for instrument in required_instruments
            if (
                working.last_market_received_at_by_instrument.get(instrument) is None
                or as_of
                - cast(
                    datetime,
                    working.last_market_received_at_by_instrument.get(instrument),
                )
                > timedelta(seconds=self.config.risk.stale_after_seconds)
            )
        )
        feed_stale = (
            last_market is None
            or as_of - last_market > timedelta(seconds=self.config.risk.stale_after_seconds)
            or bool(stale_instruments)
        )
        pending_states = {
            PaperState.LEG_1_PENDING,
            PaperState.HEDGE_PENDING,
            PaperState.EXIT_PENDING,
        }
        if working.strategy_projections:
            unhedged_strategies = tuple(
                strategy
                for strategy in working.strategy_projections.values()
                if strategy.positions
                and (
                    strategy.state in pending_states
                    or (strategy.state is PaperState.PAUSED and strategy.suspended_from in pending_states)
                )
            )
            unhedged = bool(unhedged_strategies)
            state_since = min(
                (
                    strategy.state_since or self.config.validation_started_at
                    for strategy in unhedged_strategies
                ),
                default=self.config.validation_started_at,
            )
        else:
            unhedged = bool(working.positions) and (
                working.state in pending_states
                or (working.state is PaperState.PAUSED and working.suspended_from in pending_states)
            )
            state_since = working.state_since or self.config.validation_started_at
        if working.strategy_projections:
            unhedged_timed_out = any(
                as_of - (strategy.state_since or self.config.validation_started_at)
                >= timedelta(
                    seconds=self.config.strategy_config(strategy.strategy_id).risk.unhedged_timeout_seconds
                )
                for strategy in unhedged_strategies
            )
        else:
            unhedged_timed_out = unhedged and as_of - state_since >= timedelta(
                seconds=self.config.risk.unhedged_timeout_seconds
            )
        if feed_stale:
            stale_payload = self._alert_payload(
                code="STALE_MARKET_DATA",
                severity=AlertSeverity.CRITICAL,
                message=(
                    "no fresh normalized public market event before the frozen timeout"
                    if not stale_instruments
                    else "stale normalized public market channels: " + ", ".join(stale_instruments)
                ),
                causation_id=deterministic_id(
                    "paper_stale_feed_episode",
                    self.run_id,
                    utc_text(last_market or self.config.validation_started_at),
                ),
                at=as_of,
            )
            if not self._alert_is_durable(str(stale_payload["alert_id"])):
                emit(PaperEventType.ALERT_RAISED, stale_payload)
            if working.state not in {
                PaperState.PAUSED,
                PaperState.MANUAL_REVIEW,
                PaperState.EMERGENCY_FLATTEN,
            }:
                target = PaperState.EMERGENCY_FLATTEN if unhedged_timed_out else PaperState.PAUSED
                emit(
                    PaperEventType.STATE_TRANSITIONED,
                    self._transition_payload(
                        working.state,
                        target,
                        (
                            "unhedged timeout with stale public market feed"
                            if unhedged_timed_out
                            else "stale public market feed"
                        ),
                    ),
                )
            elif working.state is PaperState.PAUSED and unhedged_timed_out:
                emit(
                    PaperEventType.STATE_TRANSITIONED,
                    self._transition_payload(
                        working.state,
                        PaperState.EMERGENCY_FLATTEN,
                        "unhedged timeout while public market feed remained paused",
                    ),
                )
            self._protect_entry_orders(
                working,
                events,
                at=as_of,
                causation_id=input_id,
                reason="stale public market feed",
            )

        if unhedged_timed_out:
            emit(
                PaperEventType.ALERT_RAISED,
                self._alert_payload(
                    code="UNHEDGED_TIMEOUT",
                    severity=AlertSeverity.CRITICAL,
                    message="unhedged simulated exposure exceeded its frozen timeout",
                    causation_id=deterministic_id(
                        "paper_unhedged_episode", self.run_id, utc_text(state_since)
                    ),
                    at=as_of,
                ),
            )
            if working.state is not PaperState.EMERGENCY_FLATTEN:
                emit(
                    PaperEventType.STATE_TRANSITIONED,
                    self._transition_payload(
                        working.state,
                        PaperState.EMERGENCY_FLATTEN,
                        "unhedged timeout",
                    ),
                )
        self._apply_protective_risk_state(
            working,
            events,
            at=as_of,
            causation_id=input_id,
        )
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=tuple(events),
        )

    def record_strategy_failure(
        self,
        *,
        strategy_id: str,
        as_of: datetime,
        phase: str,
        error_type: str,
        market_event_ids: tuple[str, ...],
    ) -> PaperCommandResult:
        try:
            strategy_config = self.config.strategy_config(strategy_id)
        except KeyError as error:
            raise ValueError("strategy failure references unknown strategy_id") from error
        normalized_phase = phase.strip().upper()
        normalized_error = error_type.strip()
        if not normalized_phase or not normalized_error:
            raise ValueError("strategy failure phase and error_type cannot be empty")
        if not market_event_ids:
            raise ValueError("strategy failure requires its observed market_event_ids")
        projection = self.projection()
        if projection.state is PaperState.MANUAL_REVIEW:
            raise IntegrityError(self.store.verify_integrity(self.run_id, raise_on_error=False))
        effective_at = parse_utc(utc_text(as_of))
        if projection.last_received_at is not None and effective_at < projection.last_received_at:
            effective_at = projection.last_received_at
        failure_id = deterministic_id(
            "paper_strategy_local_failure",
            self.run_id,
            strategy_config.strategy_id,
            strategy_config.strategy_config_hash,
            normalized_phase,
            normalized_error,
            tuple(sorted(market_event_ids)),
        )
        input_id = deterministic_id(
            "paper_strategy_local_failure_input",
            failure_id,
        )
        payload = {
            "as_of": utc_text(effective_at),
            "error_type": normalized_error,
            "failure_id": failure_id,
            "input_type": "STRATEGY_LOCAL_FAILURE",
            "market_event_ids": list(sorted(market_event_ids)),
            "phase": normalized_phase,
            "strategy_config_hash": strategy_config.strategy_config_hash,
            "strategy_id": strategy_config.strategy_id,
        }
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        events: list[PaperEvent] = []
        working = projection.clone()
        alert_payload = self._alert_payload(
            code="STRATEGY_LOCAL_FAILURE",
            severity=AlertSeverity.CRITICAL,
            message=(f"strategy {strategy_id} paused after {normalized_phase}: {normalized_error}"),
            causation_id=failure_id,
            at=effective_at,
        )
        alert_payload["strategy_id"] = strategy_id
        alert = self._event(
            PaperEventType.ALERT_RAISED,
            at=effective_at,
            received_at=effective_at,
            causation_id=failure_id,
            correlation_id=input_id,
            payload=alert_payload,
            ordinal=0,
        )
        apply_event(working, alert)
        events.append(alert)
        strategy = working.strategy_projection(strategy_id)
        if strategy.state is not PaperState.PAUSED:
            transition = self._event(
                PaperEventType.STATE_TRANSITIONED,
                at=effective_at,
                received_at=effective_at,
                causation_id=failure_id,
                correlation_id=input_id,
                payload={
                    **self._transition_payload(
                        strategy.state,
                        PaperState.PAUSED,
                        "strategy-local failure isolation",
                    ),
                    "strategy_id": strategy_id,
                },
                ordinal=1,
            )
            apply_event(working, transition)
            events.append(transition)
        if strategy.positions and working.state not in {
            PaperState.REDUCE_ONLY,
            PaperState.EMERGENCY_FLATTEN,
            PaperState.MANUAL_REVIEW,
        }:
            protective = self._event(
                PaperEventType.STATE_TRANSITIONED,
                at=effective_at,
                received_at=effective_at,
                causation_id=failure_id,
                correlation_id=input_id,
                payload=self._transition_payload(
                    working.state,
                    PaperState.REDUCE_ONLY,
                    "strategy-local failure with attributed exposure",
                ),
                ordinal=len(events),
            )
            apply_event(working, protective)
            events.append(protective)
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=tuple(events),
        )

    def pause(
        self,
        *,
        as_of: datetime,
        reason: str,
        operator_artifact_hash: str,
        origin: str = "OPERATOR",
    ) -> PaperCommandResult:
        """Persist an operator, public-source, or runtime-failure pause."""

        artifact = self._evidence_hash(
            operator_artifact_hash,
            label="operator artifact_hash",
        )
        normalized_reason = reason.strip()
        if not normalized_reason:
            raise ValueError("pause reason cannot be empty")
        normalized_origin = origin.strip().upper()
        if normalized_origin not in {
            "OPERATOR",
            "PUBLIC_SOURCE_FAILURE",
            "PAPER_RUNTIME_FAILURE",
        }:
            raise ValueError("unsupported paper pause origin")
        input_type, input_domain = {
            "OPERATOR": ("OPERATOR_PAUSE", "paper_pause_input"),
            "PUBLIC_SOURCE_FAILURE": (
                "PUBLIC_SOURCE_FAILURE",
                "paper_source_failure_input",
            ),
            "PAPER_RUNTIME_FAILURE": (
                "PAPER_RUNTIME_FAILURE",
                "paper_runtime_failure_input",
            ),
        }[normalized_origin]
        input_id = deterministic_id(input_domain, self.run_id, artifact)
        payload = {
            "as_of": utc_text(as_of),
            "input_type": input_type,
            "operator_artifact_hash": artifact,
            "reason": normalized_reason,
        }
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        if projection.state is PaperState.MANUAL_REVIEW:
            raise ValueError("MANUAL_REVIEW cannot be replaced by an operator pause")
        if projection.state is PaperState.PAUSED and normalized_origin == "OPERATOR":
            raise ValueError("paper run is already PAUSED")

        working = projection.clone()
        events: list[PaperEvent] = []
        alert = self._event(
            PaperEventType.ALERT_RAISED,
            at=as_of,
            received_at=as_of,
            causation_id=artifact,
            correlation_id=input_id,
            payload=self._alert_payload(
                code=input_type,
                severity=(
                    AlertSeverity.WARNING if normalized_origin == "OPERATOR" else AlertSeverity.CRITICAL
                ),
                message=normalized_reason,
                causation_id=artifact,
                at=as_of,
            ),
            ordinal=len(events),
        )
        apply_event(working, alert)
        events.append(alert)
        self._protect_entry_orders(
            working,
            events,
            at=as_of,
            causation_id=artifact,
            reason=(
                "operator pause"
                if normalized_origin == "OPERATOR"
                else (
                    "terminal public source failure"
                    if normalized_origin == "PUBLIC_SOURCE_FAILURE"
                    else "terminal paper runtime failure"
                )
            ),
        )
        if working.state not in {PaperState.EMERGENCY_FLATTEN, PaperState.PAUSED}:
            transition = self._event(
                PaperEventType.STATE_TRANSITIONED,
                at=as_of,
                received_at=as_of,
                causation_id=artifact,
                correlation_id=input_id,
                payload=self._transition_payload(
                    working.state,
                    PaperState.PAUSED,
                    normalized_reason,
                ),
                ordinal=len(events),
            )
            apply_event(working, transition)
            events.append(transition)
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=tuple(events),
        )

    def kill(
        self,
        *,
        as_of: datetime,
        reason: str,
        operator_artifact_hash: str,
    ) -> PaperCommandResult:
        """Irreversibly latch MANUAL_REVIEW and terminate all simulated orders."""

        artifact = self._evidence_hash(
            operator_artifact_hash,
            label="operator artifact_hash",
        )
        normalized_reason = reason.strip()
        if not normalized_reason:
            raise ValueError("kill reason cannot be empty")
        input_id = deterministic_id("paper_kill_input", self.run_id, artifact)
        payload = {
            "as_of": utc_text(as_of),
            "input_type": "PAPER_KILL",
            "operator_artifact_hash": artifact,
            "reason": normalized_reason,
        }
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        if projection.state is PaperState.MANUAL_REVIEW:
            raise ValueError("paper run is already latched in MANUAL_REVIEW")

        working = projection.clone()
        events: list[PaperEvent] = []
        alert = self._event(
            PaperEventType.ALERT_RAISED,
            at=as_of,
            received_at=as_of,
            causation_id=artifact,
            correlation_id=input_id,
            payload=self._alert_payload(
                code="PAPER_KILL",
                severity=AlertSeverity.CRITICAL,
                message=normalized_reason,
                causation_id=artifact,
                at=as_of,
            ),
            ordinal=len(events),
        )
        apply_event(working, alert)
        events.append(alert)

        for order in self._ordered_orders(working):
            if order.status is OrderStatus.RISK_ACCEPTED:
                terminal = self._event(
                    PaperEventType.ORDER_REJECTED,
                    at=as_of,
                    received_at=as_of,
                    causation_id=artifact,
                    correlation_id=order.intent.decision_id,
                    payload={
                        "order_id": order.intent.order_id,
                        "reason": "operator kill before simulated acknowledgement",
                    },
                    ordinal=len(events),
                )
                apply_event(working, terminal)
                events.append(terminal)
            elif order.status in {OrderStatus.ACKED, OrderStatus.PARTIALLY_FILLED}:
                requested = self._event(
                    PaperEventType.CANCEL_REQUESTED,
                    at=as_of,
                    received_at=as_of,
                    causation_id=artifact,
                    correlation_id=order.intent.decision_id,
                    payload={
                        "cancel_effective_at": utc_text(as_of),
                        "order_id": order.intent.order_id,
                        "reason": "operator kill",
                    },
                    ordinal=len(events),
                )
                apply_event(working, requested)
                events.append(requested)
                cancelled = self._event(
                    PaperEventType.ORDER_CANCELLED,
                    at=as_of,
                    received_at=as_of,
                    causation_id=artifact,
                    correlation_id=order.intent.decision_id,
                    payload=self._order_event_payload(
                        order,
                        {"order_id": order.intent.order_id},
                    ),
                    ordinal=len(events),
                )
                apply_event(working, cancelled)
                events.append(cancelled)
            elif order.status is OrderStatus.CANCEL_PENDING:
                cancelled = self._event(
                    PaperEventType.ORDER_CANCELLED,
                    at=as_of,
                    received_at=as_of,
                    causation_id=artifact,
                    correlation_id=order.intent.decision_id,
                    payload=self._order_event_payload(
                        order,
                        {"order_id": order.intent.order_id},
                    ),
                    ordinal=len(events),
                )
                apply_event(working, cancelled)
                events.append(cancelled)

        transition = self._event(
            PaperEventType.STATE_TRANSITIONED,
            at=as_of,
            received_at=as_of,
            causation_id=artifact,
            correlation_id=input_id,
            payload=self._transition_payload(
                working.state,
                PaperState.MANUAL_REVIEW,
                f"operator kill: {normalized_reason}",
            ),
            ordinal=len(events),
        )
        apply_event(working, transition)
        events.append(transition)
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=tuple(events),
        )

    def resume_from_pause(
        self,
        *,
        as_of: datetime,
        review_artifact_hash: str,
        reviewed_critical_incident_count: int,
        reviewed_last_critical_incident_at: datetime | None,
        recovery_mode: str,
    ) -> PaperCommandResult:
        """Explicit audited recovery bound to the exact critical-incident head."""

        artifact = self._evidence_hash(review_artifact_hash, label="review artifact_hash")
        if (
            isinstance(reviewed_critical_incident_count, bool)
            or not isinstance(reviewed_critical_incident_count, int)
            or reviewed_critical_incident_count < 0
        ):
            raise ValueError("reviewed_critical_incident_count must be a non-negative integer")
        reviewed_at = (
            parse_utc(utc_text(reviewed_last_critical_incident_at))
            if reviewed_last_critical_incident_at is not None
            else None
        )
        if (reviewed_critical_incident_count == 0) != (reviewed_at is None):
            raise ValueError("reviewed_last_critical_incident_at must match the reviewed incident count")
        normalized_mode = recovery_mode.strip().upper()
        if normalized_mode not in {"STANDARD", "OFFLINE_UNCLOSED_SESSION"}:
            raise ValueError("unsupported paper resume recovery_mode")
        input_id = deterministic_id("paper_resume_input", self.run_id, artifact)
        payload = {
            "as_of": utc_text(as_of),
            "input_type": "RESUME_AFTER_REVIEW",
            "recovery_mode": normalized_mode,
            "review_artifact_hash": artifact,
            "reviewed_critical_incident_count": reviewed_critical_incident_count,
            "reviewed_last_critical_incident_at": (
                utc_text(reviewed_at) if reviewed_at is not None else None
            ),
        }
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        self.store.verify_integrity(self.run_id)
        projection = self.projection()
        if projection.state is not PaperState.PAUSED:
            raise ValueError("resume_from_pause requires PAUSED state")
        if not projection.reconciled:
            raise ValueError("resume_from_pause requires exact reconciliation")
        if (
            projection.critical_incident_count != reviewed_critical_incident_count
            or projection.last_critical_incident_at != reviewed_at
        ):
            raise ValueError("reviewed critical-incident summary differs from durable projection")

        if normalized_mode == "OFFLINE_UNCLOSED_SESSION":
            session_id = projection.runtime_session_id
            if not projection.runtime_session_active or session_id is None:
                raise ValueError("offline unclosed-session recovery requires an active durable session")
            failure_artifact = deterministic_id(
                "paper_runtime_failure_v1",
                self.run_id,
                self.config.config_hash,
                "UNCLOSED_RUNTIME_SESSION",
                "UnclosedRuntimeSessionError",
                session_id,
            )
            failure = self.store.get_input(
                self.run_id,
                deterministic_id(
                    "paper_runtime_failure_input",
                    self.run_id,
                    failure_artifact,
                ),
            )
            expected_reason = (
                "terminal paper runtime failure: UNCLOSED_RUNTIME_SESSION: UnclosedRuntimeSessionError"
            )
            latest_critical = self.store.get_latest_alert(self.run_id, severity="CRITICAL")
            if (
                failure is None
                or failure.payload.get("input_type") != "PAPER_RUNTIME_FAILURE"
                or failure.payload.get("operator_artifact_hash") != failure_artifact
                or failure.payload.get("reason") != expected_reason
                or latest_critical is None
                or latest_critical.commit_sequence != failure.commit_sequence
                or reviewed_at is None
                or failure.payload.get("as_of") != utc_text(reviewed_at)
            ):
                raise ValueError(
                    "offline recovery requires the latest reviewed incident to be "
                    "the durable unclosed-session failure"
                )
        else:
            stale_after = timedelta(seconds=self.config.risk.stale_after_seconds)
            required = self.config.required_instruments
            if required:
                missing_or_stale = tuple(
                    instrument
                    for instrument in required
                    if instrument not in projection.public_bbo_mids
                    or (market_at := projection.public_bbo_received_at_by_instrument.get(instrument)) is None
                    or market_at > as_of
                    or as_of - market_at > stale_after
                )
                if missing_or_stale:
                    raise ValueError(
                        "resume_from_pause requires a fresh uninterrupted public "
                        "BBO for every required instrument"
                    )
        if any(
            order.action is DecisionAction.ENTRY and order.status.active
            for order in projection.orders.values()
        ):
            raise ValueError("resume_from_pause requires all entry orders to be terminal")
        has_attributed_positions = any(
            strategy.positions for strategy in projection.strategy_projections.values()
        )
        target = (
            PaperState.REDUCE_ONLY if projection.positions or has_attributed_positions else PaperState.FLAT
        )
        event = self._event(
            PaperEventType.STATE_TRANSITIONED,
            at=as_of,
            received_at=as_of,
            causation_id=artifact,
            correlation_id=input_id,
            payload=self._transition_payload(
                projection.state,
                target,
                (
                    "offline reviewed unclosed-session recovery"
                    if normalized_mode == "OFFLINE_UNCLOSED_SESSION"
                    else "explicit reviewed recovery"
                ),
            ),
        )
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=(event,),
        )

    def emergency_flatten(
        self,
        markets: Mapping[str, MarketEvent],
        *,
        decided_at: datetime,
        reason: str,
    ) -> PaperCommandResult:
        projection = self.projection()
        if projection.state not in {
            PaperState.EMERGENCY_FLATTEN,
            PaperState.REDUCE_ONLY,
        }:
            raise ValueError("automatic flatten requires a protective paper state")
        if projection.strategy_projections:
            owned_positions = {
                strategy_id: dict(strategy.positions)
                for strategy_id, strategy in projection.strategy_projections.items()
                if strategy.positions
            }
            if not owned_positions:
                raise ValueError("emergency_flatten requires an attributed simulated position")
            required_instruments = {
                instrument for positions in owned_positions.values() for instrument in positions
            }
            if any(instrument not in markets for instrument in required_instruments):
                raise ValueError("emergency_flatten requires a public market for every attributed position")
            last_result: PaperCommandResult | None = None
            for strategy_id in sorted(owned_positions):
                strategy = self.config.strategy_config(strategy_id)
                positions = owned_positions[strategy_id]
                strategy_markets = {instrument: markets[instrument] for instrument in sorted(positions)}
                latest_received = max(market.received_at for market in strategy_markets.values())
                if decided_at < latest_received:
                    raise ValueError("emergency flatten decision cannot precede its public observations")
                primary = max(
                    strategy_markets.values(),
                    key=lambda item: (
                        item.received_at,
                        item.capture_ordinal,
                        item.event_id,
                    ),
                )
                reason_ordinal = int(
                    deterministic_id(
                        "paper_emergency_reason",
                        strategy_id,
                        reason,
                    )[:8],
                    16,
                )
                decision_id = DecisionIntent.identifier(
                    run_id=self.run_id,
                    strategy_id=strategy_id,
                    market_event_id=primary.event_id,
                    action=DecisionAction.EXIT,
                    ordinal=reason_ordinal,
                )
                orders = tuple(
                    OrderIntent.create(
                        decision_id=decision_id,
                        run_id=self.run_id,
                        strategy_id=strategy_id,
                        instrument=instrument,
                        side=(OrderSide.SELL if quantity > 0 else OrderSide.BUY),
                        quantity=abs(quantity),
                        order_type=PaperOrderType.TAKER,
                        time_in_force=TimeInForce.IOC,
                        created_at=decided_at,
                        ordinal=index,
                        reduce_only=True,
                        leg_number=index + 1,
                    )
                    for index, (instrument, quantity) in enumerate(sorted(positions.items()))
                )
                decision = DecisionIntent(
                    decision_id=decision_id,
                    run_id=self.run_id,
                    strategy_id=strategy_id,
                    strategy_name=strategy.strategy_name,
                    strategy_hash=strategy.strategy_hash,
                    strategy_config_hash=strategy.strategy_config_hash,
                    action=DecisionAction.EXIT,
                    decided_at=decided_at,
                    received_at=latest_received,
                    market_event_id=primary.event_id,
                    observed_event_ids=tuple(
                        strategy_markets[instrument].event_id for instrument in sorted(strategy_markets)
                    ),
                    orders=orders,
                    ordinal=reason_ordinal,
                )
                last_result = self.submit_decision(
                    decision,
                    strategy_markets,
                )
            if last_result is None:
                raise AssertionError("attributed emergency flatten produced no decisions")
            return last_result
        if not projection.positions:
            raise ValueError("emergency_flatten requires an open simulated position")
        if any(instrument not in markets for instrument in projection.positions):
            raise ValueError("emergency_flatten requires a public market for every position")
        latest_received = max(markets[instrument].received_at for instrument in projection.positions)
        if decided_at < latest_received:
            raise ValueError("emergency flatten decision cannot precede its public observations")
        primary = max(
            (markets[instrument] for instrument in projection.positions),
            key=lambda item: (item.received_at, item.capture_ordinal, item.event_id),
        )
        reason_ordinal = int(deterministic_id("paper_emergency_reason", reason)[:8], 16)
        decision_id = DecisionIntent.identifier(
            run_id=self.run_id,
            market_event_id=primary.event_id,
            action=DecisionAction.EXIT,
            ordinal=reason_ordinal,
        )
        orders = tuple(
            OrderIntent.create(
                decision_id=decision_id,
                run_id=self.run_id,
                instrument=instrument,
                side=OrderSide.SELL if quantity > 0 else OrderSide.BUY,
                quantity=abs(quantity),
                order_type=PaperOrderType.TAKER,
                time_in_force=TimeInForce.IOC,
                created_at=decided_at,
                ordinal=index,
                reduce_only=True,
                leg_number=index + 1,
            )
            for index, (instrument, quantity) in enumerate(sorted(projection.positions.items()))
        )
        decision = DecisionIntent(
            decision_id=decision_id,
            run_id=self.run_id,
            strategy_name=self.config.strategy_name,
            action=DecisionAction.EXIT,
            decided_at=decided_at,
            received_at=latest_received,
            market_event_id=primary.event_id,
            observed_event_ids=tuple(
                markets[instrument].event_id for instrument in sorted(projection.positions)
            ),
            orders=orders,
            ordinal=reason_ordinal,
        )
        return self.submit_decision(decision, markets)

    def replay(
        self,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> PaperProjection:
        run = self.store.get_run(self.run_id)

        def interruptible_events():  # type: ignore[no-untyped-def]
            for stored in self.store.iter_events(self.run_id):
                _raise_if_interrupted(should_stop)
                yield stored

        replayed = replay_projection(
            run_id=self.run_id,
            config_hash=self.config.config_hash,
            initial_cash=self.config.initial_cash,
            events=interruptible_events(),
        )
        durable = self.projection()
        if replayed.to_dict() != durable.to_dict():
            raise ValueError("event replay differs from the durable paper projection")
        if replayed.last_event_hash != run.event_head_hash:
            raise ValueError("event replay head differs from the durable hash-chain head")
        return replayed

    def verify_input_replay(self) -> PaperProjection:
        """Re-run the canonical inbox through a fresh engine and compare outputs exactly."""

        source_inputs = self.store.iter_inputs(self.run_id)
        legacy_source_events = iter(self.store.iter_events(self.run_id))
        legacy_source_event: StoredPaperEvent | None = None
        with TemporaryDirectory(prefix="hyperlab-paper-replay-") as directory:
            replay_store = PaperStore(Path(directory) / "paper-replay.sqlite3")
            replay_engine = PaperEngine(replay_store, self.config)
            replay_engine.start()
            for record in source_inputs:
                payload = record.payload
                input_type = str(payload.get("input_type", ""))
                if input_type == "RUN_START":
                    continue
                if input_type == "RUNTIME_SESSION_STARTED":
                    raw_replacement = payload.get("replaces_unclosed_session_id")
                    replay_engine.start_runtime_session(
                        as_of=parse_utc(str(payload["started_at"])),
                        session_id=str(payload["session_id"]),
                        generation=int(str(payload["generation"])),
                        replaces_unclosed_session_id=(
                            str(raw_replacement) if raw_replacement is not None else None
                        ),
                    )
                    continue
                if input_type == "RUNTIME_SESSION_STOPPED":
                    replay_engine.stop_runtime_session(
                        as_of=parse_utc(str(payload["stopped_at"])),
                        session_id=str(payload["session_id"]),
                        generation=int(str(payload["generation"])),
                        reason=str(payload["reason"]),
                    )
                    continue
                if input_type == "PUBLIC_MARKET_EVENT":
                    raw_market = payload.get("market")
                    if not isinstance(raw_market, Mapping):
                        raise ValueError("replay market input lacks market payload")
                    market = MarketEvent.from_dict(raw_market)
                    replay_engine.process_market(
                        market,
                        processed_at=parse_utc(
                            str(payload.get("processed_at", utc_text(market.received_at)))
                        ),
                        execution_policy=str(payload.get("execution_policy", "EXECUTE")),
                    )
                elif input_type == "STRATEGY_DECISION":
                    raw_decision = payload.get("decision")
                    raw_markets = payload.get("markets")
                    if not isinstance(raw_decision, Mapping) or not isinstance(raw_markets, Sequence):
                        raise ValueError("replay decision input is incomplete")
                    decision = DecisionIntent.from_dict(raw_decision)
                    markets = {
                        market.instrument: market
                        for item in raw_markets
                        if isinstance(item, Mapping)
                        for market in (MarketEvent.from_dict(item),)
                    }
                    replay_engine.submit_decision(
                        decision,
                        markets,
                        processed_at=parse_utc(
                            str(payload.get("processed_at", utc_text(decision.decided_at)))
                        ),
                    )
                elif input_type == "CANCEL_REQUEST":
                    replay_engine.request_cancel(
                        str(payload["order_id"]),
                        requested_at=datetime.fromisoformat(
                            str(payload["requested_at"]).replace("Z", "+00:00")
                        ),
                        input_id=record.input_id,
                    )
                elif input_type == "PUBLIC_FUNDING_SETTLEMENT":
                    replay_engine.post_funding(
                        instrument=str(payload["instrument"]),
                        amount=Decimal(str(payload["amount"])),
                        occurred_at=datetime.fromisoformat(
                            str(payload["occurred_at"]).replace("Z", "+00:00")
                        ),
                        source_event_id=str(payload["source_event_id"]),
                        funding_rate=(
                            Decimal(str(payload["funding_rate"]))
                            if payload.get("funding_rate") is not None
                            else None
                        ),
                        funding_interval_seconds=(
                            int(str(payload["funding_interval_seconds"]))
                            if payload.get("funding_interval_seconds") is not None
                            else None
                        ),
                        rate_kind=(
                            str(payload["rate_kind"]) if payload.get("rate_kind") is not None else None
                        ),
                        mark_price=(
                            Decimal(str(payload["mark_price"]))
                            if payload.get("mark_price") is not None
                            else None
                        ),
                        source_mark_price=(
                            Decimal(str(payload["source_mark_price"]))
                            if payload.get("source_mark_price") is not None
                            else None
                        ),
                        oracle_price=(
                            Decimal(str(payload["oracle_price"]))
                            if payload.get("oracle_price") is not None
                            else None
                        ),
                        position_quantity=(
                            Decimal(str(payload["position_quantity"]))
                            if payload.get("position_quantity") is not None
                            else None
                        ),
                        mark_source=(
                            str(payload["mark_source"]) if payload.get("mark_source") is not None else None
                        ),
                        source_observation_id=(
                            str(payload["source_observation_id"])
                            if payload.get("source_observation_id") is not None
                            else None
                        ),
                        received_at=(
                            datetime.fromisoformat(str(payload["received_at"]).replace("Z", "+00:00"))
                            if payload.get("received_at") is not None
                            else None
                        ),
                        processed_at=parse_utc(
                            str(
                                payload.get(
                                    "processed_at", payload.get("received_at", payload["occurred_at"])
                                )
                            )
                        ),
                        applicability=str(payload.get("applicability", "APPLIED")),
                        source_activation_cutoff=(
                            datetime.fromisoformat(
                                str(payload["source_activation_cutoff"]).replace("Z", "+00:00")
                            )
                            if payload.get("source_activation_cutoff") is not None
                            else None
                        ),
                    )
                elif input_type == "TIMER":
                    replay_engine.process_timer(
                        as_of=datetime.fromisoformat(str(payload["as_of"]).replace("Z", "+00:00"))
                    )
                elif input_type == "RECONCILE":
                    replay_engine.reconcile(
                        as_of=datetime.fromisoformat(str(payload["as_of"]).replace("Z", "+00:00"))
                    )
                elif input_type == "STRESS_RESULT":
                    replay_engine.record_stress_result(
                        artifact_hash=str(payload["artifact_hash"]),
                        stressed_net_pnl=Decimal(str(payload["stressed_net_pnl"])),
                        evaluated_at=datetime.fromisoformat(
                            str(payload["evaluated_at"]).replace("Z", "+00:00")
                        ),
                    )
                elif input_type == "RESILIENCE_EXERCISE":
                    replay_engine.record_resilience_exercise(
                        exercise=str(payload["exercise"]),
                        artifact_hash=str(payload["artifact_hash"]),
                        exercised_at=datetime.fromisoformat(
                            str(payload["exercised_at"]).replace("Z", "+00:00")
                        ),
                    )
                elif input_type == "OBSERVATION_COVERAGE":
                    raw_recorded_at = payload.get("recorded_at")
                    if isinstance(raw_recorded_at, str):
                        recorded_at = parse_utc(raw_recorded_at)
                    elif self.config.schema_version == 1:
                        target_sequence = record.first_event_sequence
                        if target_sequence is None:
                            raise ValueError("legacy observation coverage input has no durable event")
                        while legacy_source_event is None or legacy_source_event.sequence < target_sequence:
                            try:
                                legacy_source_event = next(legacy_source_events)
                            except StopIteration as error:
                                raise ValueError("legacy observation coverage event is missing") from error
                        if (
                            legacy_source_event.sequence != target_sequence
                            or legacy_source_event.event.correlation_id != record.input_id
                        ):
                            raise ValueError("legacy observation coverage event differs from its input")
                        recorded_at = legacy_source_event.event.received_at
                    else:
                        raise ValueError("schema-v2 observation coverage lacks recorded_at")
                    replay_engine.record_observation_coverage(
                        artifact_hash=str(payload["artifact_hash"]),
                        window_start=datetime.fromisoformat(
                            str(payload["window_start"]).replace("Z", "+00:00")
                        ),
                        window_end=datetime.fromisoformat(str(payload["window_end"]).replace("Z", "+00:00")),
                        continuous=bool(payload["continuous"]),
                        recorded_at=recorded_at,
                    )
                elif input_type in {
                    "OPERATOR_PAUSE",
                    "PUBLIC_SOURCE_FAILURE",
                    "PAPER_RUNTIME_FAILURE",
                }:
                    replay_engine.pause(
                        as_of=datetime.fromisoformat(str(payload["as_of"]).replace("Z", "+00:00")),
                        reason=str(payload["reason"]),
                        operator_artifact_hash=str(payload["operator_artifact_hash"]),
                        origin={
                            "OPERATOR_PAUSE": "OPERATOR",
                            "PUBLIC_SOURCE_FAILURE": "PUBLIC_SOURCE_FAILURE",
                            "PAPER_RUNTIME_FAILURE": "PAPER_RUNTIME_FAILURE",
                        }[input_type],
                    )
                elif input_type == "STRATEGY_LOCAL_FAILURE":
                    raw_market_event_ids = payload.get("market_event_ids")
                    if not isinstance(raw_market_event_ids, Sequence) or isinstance(
                        raw_market_event_ids, (str, bytes)
                    ):
                        raise ValueError("strategy-local failure replay requires market_event_ids")
                    replay_engine.record_strategy_failure(
                        strategy_id=str(payload["strategy_id"]),
                        as_of=parse_utc(str(payload["as_of"])),
                        phase=str(payload["phase"]),
                        error_type=str(payload["error_type"]),
                        market_event_ids=tuple(str(item) for item in raw_market_event_ids),
                    )
                elif input_type == "PAPER_KILL":
                    replay_engine.kill(
                        as_of=datetime.fromisoformat(str(payload["as_of"]).replace("Z", "+00:00")),
                        reason=str(payload["reason"]),
                        operator_artifact_hash=str(payload["operator_artifact_hash"]),
                    )
                elif input_type == "RESUME_AFTER_REVIEW":
                    replay_engine.resume_from_pause(
                        as_of=datetime.fromisoformat(str(payload["as_of"]).replace("Z", "+00:00")),
                        review_artifact_hash=str(payload["review_artifact_hash"]),
                        reviewed_critical_incident_count=cast(
                            int,
                            payload["reviewed_critical_incident_count"],
                        ),
                        reviewed_last_critical_incident_at=(
                            parse_utc(cast(str, payload["reviewed_last_critical_incident_at"]))
                            if isinstance(
                                payload.get("reviewed_last_critical_incident_at"),
                                str,
                            )
                            else None
                        ),
                        recovery_mode=str(payload["recovery_mode"]),
                    )
                else:
                    raise ValueError(f"unsupported durable replay input type {input_type!r}")

            for source_event, replay_event in zip_longest(
                self.store.iter_events(self.run_id),
                replay_store.iter_events(self.run_id),
            ):
                if (
                    source_event is None
                    or replay_event is None
                    or source_event.hash_payload() != replay_event.hash_payload()
                    or source_event.event_hash != replay_event.event_hash
                ):
                    raise ValueError("canonical input replay produced different paper events")
            for source_entry, replay_entry in zip_longest(
                self.store.iter_ledger_entries(self.run_id),
                replay_store.iter_ledger_entries(self.run_id),
            ):
                if source_entry is None or replay_entry is None or source_entry.entry != replay_entry.entry:
                    raise ValueError("canonical input replay produced a different ledger")
            replayed = replay_store.get_projection(self.run_id)
            if replayed.to_dict() != self.projection().to_dict():
                raise ValueError("canonical input replay produced a different projection")
            replay_store.close()
            # sqlite3 connection/cursor cycles can otherwise survive until a
            # later collection and keep the temporary replay DB open on Windows.
            del replay_engine, replay_store
            gc.collect()
            return replayed

    def reconcile(
        self,
        *,
        as_of: datetime,
        should_stop: Callable[[], bool] | None = None,
    ) -> PaperCommandResult:
        return self._reconcile(as_of=as_of, should_stop=should_stop)

    def reconcile_prepared(
        self,
        preparation: PaperStartupPreparation,
        *,
        as_of: datetime,
        should_stop: Callable[[], bool] | None = None,
    ) -> PaperCommandResult:
        if not isinstance(preparation, PaperStartupPreparation):
            raise TypeError("preparation must be a PaperStartupPreparation")
        verification = preparation.verification
        current = self.store.get_run(self.run_id)
        projection = self.projection()
        if current.head_identity != verification.head_identity:
            raise ConcurrentWriteError("paper durable head changed after startup verification")
        if projection.to_dict() != verification.projection.to_dict():
            raise ConcurrentWriteError("paper projection changed after startup verification")
        _raise_if_interrupted(should_stop)
        has_durable_reconciliation = (
            next(
                iter(
                    self.store.iter_inputs(
                        self.run_id,
                        input_type="RECONCILE",
                    )
                ),
                None,
            )
            is not None
        )
        if (
            preparation.started.append.idempotent
            and has_durable_reconciliation
            and projection.reconciled
            and projection.state is not PaperState.MANUAL_REVIEW
        ):
            return PaperCommandResult(
                append=preparation.started.append,
                projection=projection,
            )
        return self._reconcile(
            as_of=as_of,
            should_stop=should_stop,
            verification=verification,
        )

    def _reconcile(
        self,
        *,
        as_of: datetime,
        should_stop: Callable[[], bool] | None = None,
        verification: _VerifiedPaperState | None = None,
    ) -> PaperCommandResult:
        input_id = deterministic_id("paper_reconcile", self.run_id, utc_text(as_of))
        payload = {"as_of": utc_text(as_of), "input_type": "RECONCILE"}
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        try:
            verified = verification or self._verify_durable_state(should_stop=should_stop)
            if verification is not None:
                current = self.store.get_run(self.run_id)
                if current.head_identity != verification.head_identity:
                    raise ConcurrentWriteError("paper durable head changed after startup verification")
                if projection.to_dict() != verification.projection.to_dict():
                    raise ConcurrentWriteError("paper projection changed after startup verification")
            report = verified.report
            _raise_if_interrupted(should_stop)
        except IntegrityError:
            raise
        except ValueError as error:
            events: list[PaperEvent] = [
                self._event(
                    PaperEventType.RECONCILIATION_FAILED,
                    at=as_of,
                    received_at=as_of,
                    causation_id=input_id,
                    correlation_id=input_id,
                    payload={"reason": str(error)},
                )
            ]
            events.append(
                self._event(
                    PaperEventType.ALERT_RAISED,
                    at=as_of,
                    received_at=as_of,
                    causation_id=input_id,
                    correlation_id=input_id,
                    payload=self._alert_payload(
                        code="RECONCILIATION_FAILED",
                        severity=AlertSeverity.CRITICAL,
                        message=str(error),
                        causation_id=input_id,
                        at=as_of,
                    ),
                    ordinal=1,
                )
            )
            if projection.state is not PaperState.MANUAL_REVIEW:
                events.append(
                    self._event(
                        PaperEventType.STATE_TRANSITIONED,
                        at=as_of,
                        received_at=as_of,
                        causation_id=input_id,
                        correlation_id=input_id,
                        payload=self._transition_payload(
                            projection.state,
                            PaperState.MANUAL_REVIEW,
                            "reconciliation failure",
                        ),
                        ordinal=2,
                    )
                )
            return self._commit(
                input_id=input_id,
                input_payload=payload,
                base=projection,
                events=tuple(events),
            )
        _raise_if_interrupted(should_stop)
        event = self._event(
            PaperEventType.RECONCILIATION_SUCCEEDED,
            at=as_of,
            received_at=as_of,
            causation_id=input_id,
            correlation_id=input_id,
            payload={"commit_count": report.commit_count, "event_count": report.event_count},
        )
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=(event,),
        )

    def _deduplicate(
        self,
        input_id: str,
        input_payload: Mapping[str, object],
    ) -> PaperCommandResult | None:
        try:
            exists = self.store.contains_input(self.run_id, input_id)
        except RunNotFoundError:
            return None
        if not exists:
            return None
        projection = self.projection()
        append = self.store.append_atomic(
            self.run_id,
            input_id,
            input_payload,
            (),
            (),
            projection,
            expected_sequence=projection.last_sequence,
        )
        return PaperCommandResult(append=append, projection=self.projection())

    def _ledger_reconciliation_errors(
        self,
        projection: PaperProjection,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> tuple[str, ...]:
        balances: dict[str, Decimal] = {}
        # Decimal arithmetic is exact only within the active finite precision.
        # The reducer applies one economic event net at a time, so reconciliation
        # must preserve those chronological transaction boundaries instead of
        # summing individual postings in arbitrary transaction-hash order.
        ledger_realized_pnl = Decimal(0)
        transaction_id: str | None = None
        transaction_balances: dict[str, Decimal] = {}

        def apply_transaction(amounts: Mapping[str, Decimal]) -> Decimal:
            for account, amount in amounts.items():
                balances[account] = balances.get(account, Decimal(0)) + amount
            return (
                -amounts.get("income:realized_pnl", Decimal(0))
                - amounts.get("expense:fees", Decimal(0))
                - amounts.get("income:funding", Decimal(0))
            )

        for entry in self.store.iter_ledger_entries(self.run_id):
            _raise_if_interrupted(should_stop)
            if transaction_id is not None and entry.transaction_id != transaction_id:
                ledger_realized_pnl += apply_transaction(transaction_balances)
                transaction_balances = {}
            transaction_id = entry.transaction_id
            transaction_balances[entry.account] = (
                transaction_balances.get(entry.account, Decimal(0)) + entry.amount
            )
        if transaction_id is not None:
            ledger_realized_pnl += apply_transaction(transaction_balances)

        errors: list[str] = []
        if balances.get("asset:cash", Decimal(0)) != projection.cash:
            errors.append("ledger cash differs from the replayed projection")
        for instrument in projection.positions:
            expected = projection.inventory_value[instrument]
            if balances.get(f"asset:inventory:{instrument}", Decimal(0)) != expected:
                errors.append(f"ledger inventory differs for {instrument}")
        durable_inventory_accounts = {
            account.removeprefix("asset:inventory:")
            for account, balance in balances.items()
            if account.startswith("asset:inventory:") and balance != 0
        }
        if durable_inventory_accounts != set(projection.positions):
            errors.append("ledger inventory accounts differ from open positions")
        if balances.get("expense:fees", Decimal(0)) != projection.fees:
            errors.append("ledger fees differ from the replayed projection")
        if ledger_realized_pnl != projection.realized_pnl:
            errors.append("ledger realized PnL differs from the replayed projection")
        for strategy_id, strategy in sorted(projection.strategy_projections.items()):
            prefix = f"strategy:{strategy_id}:"
            if balances.get(prefix + "asset:cash", Decimal(0)) != strategy.cash:
                errors.append(f"strategy ledger cash differs for {strategy_id}")
            for instrument in strategy.positions:
                expected_inventory = strategy.inventory_value[instrument]
                if (
                    balances.get(
                        prefix + f"asset:inventory:{instrument}",
                        Decimal(0),
                    )
                    != expected_inventory
                ):
                    errors.append(f"strategy ledger inventory differs for {strategy_id}:{instrument}")
            durable_strategy_inventory = {
                account.removeprefix(prefix + "asset:inventory:")
                for account, balance in balances.items()
                if account.startswith(prefix + "asset:inventory:") and balance != 0
            }
            if durable_strategy_inventory != set(strategy.positions):
                errors.append(f"strategy ledger inventory accounts differ for {strategy_id}")
            if balances.get(prefix + "expense:fees", Decimal(0)) != strategy.fees:
                errors.append(f"strategy ledger fees differ for {strategy_id}")
            strategy_realized = (
                -balances.get(prefix + "income:realized_pnl", Decimal(0))
                - balances.get(prefix + "expense:fees", Decimal(0))
                - balances.get(prefix + "income:funding", Decimal(0))
            )
            if strategy_realized != strategy.realized_pnl:
                errors.append(f"strategy ledger realized PnL differs for {strategy_id}")
        return tuple(errors)

    def _commit(
        self,
        *,
        input_id: str,
        input_payload: Mapping[str, object],
        base: PaperProjection,
        events: Sequence[PaperEvent],
        explicit_ledger: Sequence[LedgerEntry] = (),
    ) -> PaperCommandResult:
        working = base.clone()
        ledger = list(explicit_ledger)
        alerts: list[dict[str, object]] = []
        previous_hash = working.last_event_hash
        if previous_hash is None:
            raise ValueError("paper projection lacks its event-chain genesis/head")
        sequence = working.last_sequence
        for event in events:
            amounts = transaction_ledger_amounts(working, event)
            if amounts:
                transaction_id = deterministic_id("paper_fill_transaction", event.event_id)
                ledger.extend(
                    LedgerEntry.create(
                        run_id=self.run_id,
                        event_id=event.event_id,
                        transaction_id=transaction_id,
                        account=account,
                        amount=amount,
                        ordinal=index,
                    )
                    for index, (account, amount) in enumerate(amounts)
                )
            apply_event(working, event)
            sequence += 1
            previous_hash = canonical_sha256(
                {
                    **event.unsigned_dict(),
                    "previous_event_hash": previous_hash,
                    "sequence": sequence,
                }
            )
            working.last_sequence = sequence
            working.last_event_hash = previous_hash
            if event.event_type is PaperEventType.ALERT_RAISED:
                alerts.append(cast(dict[str, object], ensure_json_object(event.payload)))
        append = self.store.append_atomic(
            self.run_id,
            input_id,
            input_payload,
            events,
            ledger,
            working,
            alerts=alerts,
            expected_sequence=base.last_sequence,
        )
        return PaperCommandResult(append=append, projection=self.projection())

    def _event(
        self,
        event_type: PaperEventType,
        *,
        at: datetime,
        received_at: datetime,
        causation_id: str | None,
        correlation_id: str,
        payload: Mapping[str, object],
        ordinal: int = 0,
    ) -> PaperEvent:
        return PaperEvent.create(
            run_id=self.run_id,
            event_type=event_type,
            occurred_at=at,
            received_at=received_at,
            causation_id=causation_id,
            correlation_id=correlation_id,
            payload=payload,
            ordinal=ordinal,
        )

    @staticmethod
    def _transition_payload(
        source: PaperState,
        target: PaperState,
        reason: str,
    ) -> dict[str, object]:
        require_transition(source, target)
        return {"reason": reason, "source": source.value, "target": target.value}

    @staticmethod
    def _order_event_payload(
        order: OrderIntent | PaperOrder,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        intent = order.intent if isinstance(order, PaperOrder) else order
        result = dict(payload)
        if intent.strategy_id is not None:
            result["strategy_id"] = intent.strategy_id
        return result

    def _alert_payload(
        self,
        *,
        code: str,
        severity: AlertSeverity,
        message: str,
        causation_id: str,
        at: datetime,
    ) -> dict[str, object]:
        return {
            "alert_id": deterministic_id("paper_alert", self.run_id, code, causation_id),
            "code": code,
            "message": message,
            "raised_at": utc_text(at),
            "severity": severity.value,
        }

    def _alert_is_durable(self, alert_id: str) -> bool:
        return self.store.contains_alert(self.run_id, alert_id)

    @staticmethod
    def _ordered_orders(projection: PaperProjection) -> tuple[PaperOrder, ...]:
        return tuple(
            sorted(
                projection.orders.values(),
                key=lambda candidate: (
                    candidate.intent.strategy_id or "",
                    candidate.intent.leg_number,
                    candidate.intent.decision_id,
                    candidate.intent.order_id,
                ),
            )
        )

    def _reject_accepted_order(
        self,
        projection: PaperProjection,
        events: list[PaperEvent],
        order: PaperOrder,
        market: MarketEvent,
        *,
        processed_at: datetime,
        reason: str,
    ) -> None:
        event = self._event(
            PaperEventType.ORDER_REJECTED,
            at=processed_at,
            received_at=processed_at,
            causation_id=market.event_id,
            correlation_id=order.intent.decision_id,
            payload=self._order_event_payload(
                order,
                {"order_id": order.intent.order_id, "reason": reason},
            ),
            ordinal=len(events),
        )
        apply_event(projection, event)
        events.append(event)

    def _apply_protective_risk_state(
        self,
        projection: PaperProjection,
        events: list[PaperEvent],
        *,
        at: datetime,
        causation_id: str,
    ) -> bool:
        has_exposure = (
            any(strategy.positions for strategy in projection.strategy_projections.values())
            if projection.strategy_projections
            else bool(projection.positions)
        )
        if not has_exposure or projection.state in {
            PaperState.REDUCE_ONLY,
            PaperState.EMERGENCY_FLATTEN,
            PaperState.MANUAL_REVIEW,
        }:
            return False

        notionals: dict[str, Decimal] = {}
        missing_marks: list[str] = []
        signed_net = Decimal(0)
        gross = Decimal(0)
        if projection.strategy_projections:
            for strategy in projection.strategy_projections.values():
                for instrument, quantity in sorted(strategy.positions.items()):
                    mark = projection.public_bbo_mids.get(instrument)
                    if mark is None:
                        missing_marks.append(instrument)
                        continue
                    notional = quantity * mark
                    notionals[instrument] = notionals.get(instrument, Decimal(0)) + abs(notional)
                    gross += abs(notional)
            for instrument, quantity in projection.positions.items():
                mark = projection.public_bbo_mids.get(instrument)
                if mark is not None:
                    signed_net += quantity * mark
        else:
            for instrument, quantity in sorted(projection.positions.items()):
                mark = projection.public_bbo_mids.get(instrument)
                if mark is None:
                    missing_marks.append(instrument)
                    continue
                notional = quantity * mark
                notionals[instrument] = abs(notional)
                gross += abs(notional)
                signed_net += notional
        missing_marks = sorted(set(missing_marks))
        net = abs(signed_net)

        code: str | None = None
        reason: str | None = None
        detail: str | None = None
        if missing_marks:
            code = "MISSING_PUBLIC_BBO_MARK"
            reason = "open position lacks a durable public BBO valuation"
            detail = "missing=" + ",".join(missing_marks)
        elif gross > self.config.risk.max_gross_notional:
            code = "MARKED_GROSS_NOTIONAL_LIMIT"
            reason = "marked gross notional limit exceeded"
            detail = f"gross={decimal_text(gross)}; limit={decimal_text(self.config.risk.max_gross_notional)}"
        elif net > self.config.risk.max_net_notional:
            code = "MARKED_NET_NOTIONAL_LIMIT"
            reason = "marked net notional limit exceeded"
            detail = f"net={decimal_text(net)}; limit={decimal_text(self.config.risk.max_net_notional)}"
        else:
            breached_instrument = next(
                (
                    (instrument, notional)
                    for instrument, notional in sorted(notionals.items())
                    if notional > self.config.risk.max_instrument_notional
                ),
                None,
            )
            if breached_instrument is not None:
                instrument, notional = breached_instrument
                code = "MARKED_INSTRUMENT_NOTIONAL_LIMIT"
                reason = "marked instrument notional limit exceeded"
                detail = (
                    f"instrument={instrument}; notional={decimal_text(notional)}; "
                    f"limit={decimal_text(self.config.risk.max_instrument_notional)}"
                )

        current_equity = projection.equity
        if (
            code is None
            and projection.session_start_equity - current_equity >= self.config.risk.max_daily_loss
        ):
            code = "DAILY_LOSS_LIMIT"
            reason = "daily loss limit reached"
            detail = (
                f"equity={decimal_text(current_equity)}; "
                f"session_start={decimal_text(projection.session_start_equity)}"
            )
        elif code is None and projection.peak_equity - current_equity >= self.config.risk.max_drawdown:
            code = "DRAWDOWN_LIMIT"
            reason = "drawdown limit reached"
            detail = f"equity={decimal_text(current_equity)}; peak={decimal_text(projection.peak_equity)}"
        if code is None or reason is None or detail is None:
            return False

        episode_started_at = projection.state_since or at
        episode_id = deterministic_id(
            "paper_protective_risk_episode_v1",
            self.run_id,
            code,
            projection.state.value,
            utc_text(episode_started_at),
        )
        alert_payload = self._alert_payload(
            code=code,
            severity=AlertSeverity.CRITICAL,
            message=f"{reason}; {detail}",
            causation_id=episode_id,
            at=at,
        )
        alert_id = str(alert_payload["alert_id"])
        pending_alert = any(
            event.event_type is PaperEventType.ALERT_RAISED and event.payload.get("alert_id") == alert_id
            for event in events
        )
        if not pending_alert and not self._alert_is_durable(alert_id):
            alert = self._event(
                PaperEventType.ALERT_RAISED,
                at=at,
                received_at=at,
                causation_id=causation_id,
                correlation_id=causation_id,
                payload=alert_payload,
                ordinal=len(events),
            )
            apply_event(projection, alert)
            events.append(alert)
        if projection.state is PaperState.PAUSED:
            self._protect_entry_orders(
                projection,
                events,
                at=at,
                causation_id=causation_id,
                reason=reason,
            )
            return True
        transition = self._event(
            PaperEventType.STATE_TRANSITIONED,
            at=at,
            received_at=at,
            causation_id=causation_id,
            correlation_id=causation_id,
            payload=self._transition_payload(
                projection.state,
                PaperState.REDUCE_ONLY,
                reason,
            ),
            ordinal=len(events),
        )
        apply_event(projection, transition)
        events.append(transition)
        self._protect_entry_orders(
            projection,
            events,
            at=at,
            causation_id=causation_id,
            reason=reason,
        )
        return True

    def _protect_entry_orders(
        self,
        projection: PaperProjection,
        events: list[PaperEvent],
        *,
        at: datetime,
        causation_id: str,
        reason: str,
    ) -> None:
        """Stop risk-increasing entry orders with their frozen cancel latency."""

        for order in self._ordered_orders(projection):
            if order.action is not DecisionAction.ENTRY:
                continue
            if order.status is OrderStatus.RISK_ACCEPTED:
                event = self._event(
                    PaperEventType.ORDER_REJECTED,
                    at=at,
                    received_at=at,
                    causation_id=causation_id,
                    correlation_id=order.intent.decision_id,
                    payload=self._order_event_payload(
                        order,
                        {
                            "order_id": order.intent.order_id,
                            "reason": f"protective state before ack: {reason}",
                        },
                    ),
                    ordinal=len(events),
                )
                apply_event(projection, event)
                events.append(event)
                continue
            if order.status not in {OrderStatus.ACKED, OrderStatus.PARTIALLY_FILLED}:
                continue
            effective = at + timedelta(milliseconds=self.config.execution.cancel_latency_ms)
            requested = self._event(
                PaperEventType.CANCEL_REQUESTED,
                at=at,
                received_at=at,
                causation_id=causation_id,
                correlation_id=order.intent.decision_id,
                payload=self._order_event_payload(
                    order,
                    {
                        "cancel_effective_at": utc_text(effective),
                        "order_id": order.intent.order_id,
                        "reason": reason,
                    },
                ),
                ordinal=len(events),
            )
            apply_event(projection, requested)
            events.append(requested)
            if effective == at:
                cancelled = self._event(
                    PaperEventType.ORDER_CANCELLED,
                    at=at,
                    received_at=at,
                    causation_id=causation_id,
                    correlation_id=order.intent.decision_id,
                    payload=self._order_event_payload(
                        order,
                        {"order_id": order.intent.order_id},
                    ),
                    ordinal=len(events),
                )
                apply_event(projection, cancelled)
                events.append(cancelled)

    def _ack_or_reject(
        self,
        projection: PaperProjection,
        events: list[PaperEvent],
        intent: OrderIntent,
        market: MarketEvent,
        causation_id: str,
        *,
        received_at: datetime | None = None,
    ) -> None:
        command_received_at = received_at or max(market.received_at, intent.created_at)
        if command_received_at < market.received_at or command_received_at < intent.created_at:
            raise ValueError("acknowledgement processing time cannot precede order or market")
        at = command_received_at
        if intent.order_type is PaperOrderType.MAKER and (
            (intent.side is OrderSide.BUY and cast(Decimal, intent.limit_price) >= market.ask_price)
            or (intent.side is OrderSide.SELL and cast(Decimal, intent.limit_price) <= market.bid_price)
        ):
            event = self._event(
                PaperEventType.ORDER_REJECTED,
                at=at,
                received_at=command_received_at,
                causation_id=causation_id,
                correlation_id=intent.decision_id,
                payload=self._order_event_payload(
                    intent,
                    {"order_id": intent.order_id, "reason": "post_only_would_cross"},
                ),
                ordinal=len(events),
            )
        else:
            expires_at = (
                at + timedelta(milliseconds=self.config.execution.maker_timeout_ms)
                if intent.order_type is PaperOrderType.MAKER
                else None
            )
            event = self._event(
                PaperEventType.ORDER_ACKED,
                at=at,
                received_at=command_received_at,
                causation_id=causation_id,
                correlation_id=intent.decision_id,
                payload=self._order_event_payload(
                    intent,
                    {
                        "active_at": utc_text(at),
                        "expires_at": (utc_text(expires_at) if expires_at is not None else None),
                        "order_id": intent.order_id,
                    },
                ),
                ordinal=len(events),
            )
        apply_event(projection, event)
        events.append(event)

    def _fill_hard_cap_reasons(
        self,
        projection: PaperProjection,
        order: PaperOrder,
        *,
        fill_quantity: Decimal,
        fill_price: Decimal,
    ) -> tuple[str, ...]:
        """Recheck risk-increasing exposure at the actual adverse fill price."""

        intent = order.intent
        if intent.reduce_only:
            return ()
        limits = self.config.risk
        reasons: list[str] = []
        prior_notional = (order.average_fill_price or Decimal(0)) * order.filled_quantity
        order_notional = prior_notional + fill_quantity * fill_price
        if order_notional > limits.max_order_notional:
            reasons.append("actual fill notional exceeds max_order_notional")

        signed_fill = cast(OrderSide, intent.side).sign * fill_quantity
        if intent.strategy_id is not None:
            strategy = projection.strategy_projection(intent.strategy_id)
            strategy_limits = self.config.strategy_config(intent.strategy_id).risk
            strategy_positions = dict(strategy.positions)
            strategy_positions[intent.instrument] = (
                strategy_positions.get(intent.instrument, Decimal(0)) + signed_fill
            )
            if strategy_positions[intent.instrument] == 0:
                strategy_positions.pop(intent.instrument)
            strategy_gross = Decimal(0)
            strategy_net = Decimal(0)
            for instrument, quantity in strategy_positions.items():
                mark = (
                    max(
                        fill_price,
                        projection.public_bbo_mids.get(instrument, fill_price),
                    )
                    if instrument == intent.instrument
                    else projection.public_bbo_mids.get(instrument)
                )
                if mark is None:
                    reasons.append(f"strategy missing public BBO mark for position {instrument}")
                    continue
                notional = quantity * mark
                strategy_gross += abs(notional)
                strategy_net += notional
            strategy_reserved_gross = Decimal(0)
            strategy_reserved_net = Decimal(0)
            strategy_reserved_instrument = Decimal(0)
            strategy_reserved_quantity = Decimal(0)
            for pending in projection.strategy_active_orders(intent.strategy_id):
                remaining = pending.remaining_quantity
                if pending.intent.order_id == intent.order_id:
                    remaining -= fill_quantity
                if remaining <= 0:
                    continue
                pending_mark = pending.intent.limit_price or projection.public_bbo_mids.get(
                    pending.intent.instrument
                )
                if pending.intent.instrument == intent.instrument:
                    pending_mark = max(pending_mark or fill_price, fill_price)
                if pending_mark is None:
                    reasons.append(
                        f"strategy missing public BBO mark for pending order {pending.intent.order_id}"
                    )
                    continue
                pending_notional = remaining * pending_mark
                strategy_reserved_gross += pending_notional
                strategy_reserved_net += cast(OrderSide, pending.intent.side).sign * pending_notional
                if pending.intent.instrument == intent.instrument:
                    strategy_reserved_instrument += pending_notional
                    strategy_reserved_quantity += remaining
            strategy_instrument = (
                abs(strategy_positions.get(intent.instrument, Decimal(0)) * fill_price)
                + strategy_reserved_instrument
            )
            strategy_quantity = (
                abs(strategy_positions.get(intent.instrument, Decimal(0))) + strategy_reserved_quantity
            )
            if order_notional > strategy_limits.max_order_notional:
                reasons.append("strategy actual fill notional exceeds max_order_notional")
            if strategy_gross + strategy_reserved_gross > strategy_limits.max_gross_notional:
                reasons.append("strategy actual-fill gross notional exceeds max_gross_notional")
            if abs(strategy_net + strategy_reserved_net) > strategy_limits.max_net_notional:
                reasons.append("strategy actual-fill net notional exceeds max_net_notional")
            if strategy_instrument > strategy_limits.max_instrument_notional:
                reasons.append("strategy actual-fill instrument notional exceeds max_instrument_notional")
            if strategy_quantity > strategy_limits.max_position_quantity:
                reasons.append("strategy actual-fill quantity exceeds max_position_quantity")

        positions = dict(projection.positions)
        positions[intent.instrument] = positions.get(intent.instrument, Decimal(0)) + signed_fill
        if positions[intent.instrument] == 0:
            positions.pop(intent.instrument)

        public_mid = projection.public_bbo_mids.get(intent.instrument)
        fill_valuation = max(fill_price, public_mid or fill_price)
        current_gross = Decimal(0)
        current_net = Decimal(0)
        current_instrument = Decimal(0)
        if projection.strategy_projections and intent.strategy_id is not None:
            gross_positions: list[tuple[str, Decimal]] = []
            for strategy_id, strategy in projection.strategy_projections.items():
                owned_positions = dict(strategy.positions)
                if strategy_id == intent.strategy_id:
                    owned_positions[intent.instrument] = (
                        owned_positions.get(intent.instrument, Decimal(0)) + signed_fill
                    )
                    if owned_positions[intent.instrument] == 0:
                        owned_positions.pop(intent.instrument)
                gross_positions.extend(owned_positions.items())
        else:
            gross_positions = list(positions.items())
        for instrument, quantity in gross_positions:
            mark = (
                fill_valuation
                if instrument == intent.instrument
                else projection.public_bbo_mids.get(instrument)
            )
            if mark is None:
                reasons.append(f"missing public BBO mark for position {instrument}")
                continue
            notional = quantity * mark
            current_gross += abs(notional)
            if instrument == intent.instrument:
                current_instrument += abs(notional)
        for instrument, quantity in positions.items():
            mark = (
                fill_valuation
                if instrument == intent.instrument
                else projection.public_bbo_mids.get(instrument)
            )
            if mark is not None:
                current_net += quantity * mark

        reserved_gross = Decimal(0)
        reserved_net = Decimal(0)
        reserved_instrument = Decimal(0)
        reserved_instrument_quantity = Decimal(0)
        for pending in projection.active_orders:
            remaining = pending.remaining_quantity
            if pending.intent.order_id == intent.order_id:
                remaining -= fill_quantity
            if remaining <= 0:
                continue
            pending_mark = pending.intent.limit_price or projection.public_bbo_mids.get(
                pending.intent.instrument
            )
            if pending.intent.instrument == intent.instrument:
                pending_mark = max(pending_mark or fill_valuation, fill_valuation)
            if pending_mark is None:
                reasons.append(f"missing public BBO mark for pending order {pending.intent.order_id}")
                continue
            pending_notional = remaining * pending_mark
            reserved_gross += pending_notional
            reserved_net += cast(OrderSide, pending.intent.side).sign * pending_notional
            if pending.intent.instrument == intent.instrument:
                reserved_instrument += pending_notional
                reserved_instrument_quantity += remaining

        projected_gross = current_gross + reserved_gross
        projected_net = abs(current_net + reserved_net)
        projected_instrument = current_instrument + reserved_instrument
        if projection.strategy_projections:
            current_quantity = sum(
                (
                    abs(quantity)
                    for instrument, quantity in gross_positions
                    if instrument == intent.instrument
                ),
                Decimal(0),
            )
        else:
            current_quantity = abs(positions.get(intent.instrument, Decimal(0)))
        projected_quantity = current_quantity + reserved_instrument_quantity
        if projected_gross > limits.max_gross_notional:
            reasons.append("actual-fill gross notional exceeds max_gross_notional")
        if projected_net > limits.max_net_notional:
            reasons.append("actual-fill net notional exceeds max_net_notional")
        if projected_instrument > limits.max_instrument_notional:
            reasons.append("actual-fill instrument notional exceeds max_instrument_notional")
        if projected_quantity > limits.max_position_quantity:
            reasons.append("actual-fill quantity exceeds max_position_quantity")
        return tuple(reasons)

    def _match_order(
        self,
        projection: PaperProjection,
        events: list[PaperEvent],
        order: PaperOrder,
        market: MarketEvent,
        liquidity: _MarketLiquidity,
        *,
        processed_at: datetime,
    ) -> None:
        intent = order.intent
        side = cast(OrderSide, intent.side)
        cost_rule = None
        if self.config.execution.cost_schedule is not None:
            try:
                cost_rule = self.config.execution.cost_schedule.lookup(
                    pd.Timestamp(market.received_at),
                    intent.instrument,
                )
            except ValueError as error:
                reason = f"point-in-time cost schedule unavailable at match: {error}"
                if order.status is OrderStatus.RISK_ACCEPTED:
                    self._reject_accepted_order(
                        projection,
                        events,
                        order,
                        market,
                        processed_at=processed_at,
                        reason=reason,
                    )
                elif intent.time_in_force is TimeInForce.IOC:
                    self._terminal_ioc(
                        projection,
                        events,
                        order,
                        market,
                        processed_at=processed_at,
                        filled=order.filled_quantity > 0,
                        reason=reason,
                    )
                else:
                    expiry = self._event(
                        PaperEventType.ORDER_EXPIRED,
                        at=processed_at,
                        received_at=processed_at,
                        causation_id=market.event_id,
                        correlation_id=intent.decision_id,
                        payload=self._order_event_payload(
                            intent,
                            {"order_id": intent.order_id, "reason": reason},
                        ),
                        ordinal=len(events),
                    )
                    apply_event(projection, expiry)
                    events.append(expiry)
                alert = self._event(
                    PaperEventType.ALERT_RAISED,
                    at=processed_at,
                    received_at=processed_at,
                    causation_id=market.event_id,
                    correlation_id=intent.decision_id,
                    payload=self._alert_payload(
                        code="COST_SCHEDULE_GAP",
                        severity=AlertSeverity.CRITICAL,
                        message=reason,
                        causation_id=intent.order_id,
                        at=processed_at,
                    ),
                    ordinal=len(events),
                )
                apply_event(projection, alert)
                events.append(alert)
                if projection.state not in {PaperState.PAUSED, PaperState.MANUAL_REVIEW}:
                    transition = self._event(
                        PaperEventType.STATE_TRANSITIONED,
                        at=processed_at,
                        received_at=processed_at,
                        causation_id=market.event_id,
                        correlation_id=intent.decision_id,
                        payload=self._transition_payload(
                            projection.state,
                            PaperState.PAUSED,
                            "point-in-time cost schedule gap",
                        ),
                        ordinal=len(events),
                    )
                    apply_event(projection, transition)
                    events.append(transition)
                return
        quantity_cap = order.remaining_quantity
        if intent.reduce_only:
            current = (
                projection.strategy_projection(intent.strategy_id).positions.get(
                    intent.instrument,
                    Decimal(0),
                )
                if intent.strategy_id is not None
                else projection.positions.get(intent.instrument, Decimal(0))
            )
            if current == 0 or (
                (current > 0 and intent.side is not OrderSide.SELL)
                or (current < 0 and intent.side is not OrderSide.BUY)
            ):
                if intent.time_in_force is TimeInForce.IOC:
                    self._terminal_ioc(
                        projection, events, order, market, processed_at=processed_at, filled=False
                    )
                return
            quantity_cap = min(quantity_cap, abs(current))
        elif intent.time_in_force is TimeInForce.IOC and intent.leg_number > 1:
            quantity_cap = self._hedge_quantity_cap(projection, order)
            if quantity_cap <= 0:
                self._terminal_ioc(projection, events, order, market, processed_at=processed_at, filled=False)
                return
        is_maker = intent.order_type is PaperOrderType.MAKER
        if is_maker:
            eligible = (
                market.trade_price is not None
                and market.aggressor_side is not None
                and (
                    (
                        intent.side is OrderSide.BUY
                        and market.aggressor_side is OrderSide.SELL
                        and market.trade_price <= cast(Decimal, intent.limit_price)
                    )
                    or (
                        intent.side is OrderSide.SELL
                        and market.aggressor_side is OrderSide.BUY
                        and market.trade_price >= cast(Decimal, intent.limit_price)
                    )
                )
            )
            if not eligible:
                return
            depth = liquidity.maker_depth_remaining[side]
            if depth <= 0 or liquidity.maker_trade_remaining <= 0:
                return
            requested_notional = quantity_cap * cast(Decimal, intent.limit_price)
            participation = float(requested_notional / (depth * cast(Decimal, intent.limit_price)))
            maker_probability = self.config.execution.maker_fill.probability(participation=participation)
            maker_draw = keyed_uniform(
                self.config.seed,
                purpose="maker_fill",
                identity=deterministic_id("paper_maker_attempt", intent.order_id, market.event_id),
            )
            if maker_draw >= maker_probability:
                return
            quantity = min(quantity_cap, liquidity.maker_trade_remaining, depth)
            price = cast(Decimal, intent.limit_price)
            slippage_bps = Decimal(0)
            fee_bps = Decimal(
                str(
                    adverse_fee_bps(
                        float(self.config.execution.maker_fee_bps),
                        float(self.config.execution.cost_multiplier),
                    )
                )
            )
        else:
            ioc_probability = (
                self.config.execution.ioc_fill_probability
                if intent.time_in_force is TimeInForce.IOC
                else Decimal(1)
            )
            ioc_draw = Decimal(
                str(
                    keyed_uniform(
                        self.config.seed,
                        purpose="ioc_fill",
                        identity=intent.order_id,
                        attempt=order.fill_attempts,
                    )
                )
            )
            if ioc_draw >= ioc_probability:
                self._terminal_ioc(projection, events, order, market, processed_at=processed_at, filled=False)
                return
            top = market.ask_price if intent.side is OrderSide.BUY else market.bid_price
            depth = liquidity.taker_depth_remaining[side]
            if depth <= 0:
                self._terminal_ioc(projection, events, order, market, processed_at=processed_at, filled=False)
                return
            slippage_model = self.config.execution.slippage
            taker_fee = self.config.execution.taker_fee_bps
            if cost_rule is not None:
                slippage_model = cost_rule.slippage
                taker_fee = Decimal(str(cost_rule.taker_fee_bps))
            estimate = slippage_model.estimate(
                notional_usd=float(quantity_cap * top),
                depth_usd=float(depth * top),
            )
            quantity = min(
                quantity_cap,
                Decimal(str(estimate.capacity_usd)) / top,
                depth,
            )
            slippage_bps = Decimal(str(estimate.slippage_bps)) + self.config.execution.ioc_extra_slippage_bps
            direction = Decimal(1) if intent.side is OrderSide.BUY else Decimal(-1)
            price = top * (Decimal(1) + direction * slippage_bps / Decimal(10_000))
            fee_bps = Decimal(
                str(
                    adverse_fee_bps(
                        float(taker_fee),
                        float(self.config.execution.cost_multiplier),
                    )
                )
            )

        if quantity <= 0:
            self._terminal_ioc(projection, events, order, market, processed_at=processed_at, filled=False)
            return
        if is_maker and cost_rule is not None:
            fee_bps = Decimal(
                str(
                    adverse_fee_bps(
                        cost_rule.maker_fee_bps,
                        float(self.config.execution.cost_multiplier),
                    )
                )
            )
        hard_cap_reasons = self._fill_hard_cap_reasons(
            projection,
            order,
            fill_quantity=quantity,
            fill_price=price,
        )
        if hard_cap_reasons:
            reason = "fill-time hard risk rejection: " + "; ".join(hard_cap_reasons)
            if intent.time_in_force is TimeInForce.IOC:
                self._terminal_ioc(
                    projection,
                    events,
                    order,
                    market,
                    processed_at=processed_at,
                    filled=order.filled_quantity > 0,
                    reason=reason,
                )
            else:
                terminal = self._event(
                    (
                        PaperEventType.ORDER_NO_FILL
                        if order.filled_quantity == 0
                        else PaperEventType.ORDER_EXPIRED
                    ),
                    at=processed_at,
                    received_at=processed_at,
                    causation_id=market.event_id,
                    correlation_id=intent.decision_id,
                    payload=self._order_event_payload(
                        intent,
                        {"order_id": intent.order_id, "reason": reason},
                    ),
                    ordinal=len(events),
                )
                apply_event(projection, terminal)
                events.append(terminal)
            alert_causation = deterministic_id("paper_fill_hard_cap", intent.order_id, market.event_id)
            alert = self._event(
                PaperEventType.ALERT_RAISED,
                at=processed_at,
                received_at=processed_at,
                causation_id=alert_causation,
                correlation_id=intent.decision_id,
                payload=self._alert_payload(
                    code="FILL_HARD_CAP_REJECTED",
                    severity=AlertSeverity.CRITICAL,
                    message=reason,
                    causation_id=alert_causation,
                    at=processed_at,
                ),
                ordinal=len(events),
            )
            apply_event(projection, alert)
            events.append(alert)
            if projection.state not in {PaperState.PAUSED, PaperState.MANUAL_REVIEW}:
                transition = self._event(
                    PaperEventType.STATE_TRANSITIONED,
                    at=processed_at,
                    received_at=processed_at,
                    causation_id=alert_causation,
                    correlation_id=intent.decision_id,
                    payload=self._transition_payload(
                        projection.state,
                        PaperState.PAUSED,
                        "actual fill price crossed a frozen hard risk cap",
                    ),
                    ordinal=len(events),
                )
                apply_event(projection, transition)
                events.append(transition)
            self._protect_entry_orders(
                projection,
                events,
                at=processed_at,
                causation_id=alert_causation,
                reason="fill-time hard risk rejection",
            )
            return

        fee = quantity * price * fee_bps / Decimal(10_000)
        if is_maker:
            liquidity.maker_trade_remaining -= quantity
            liquidity.maker_depth_remaining[side] -= quantity
        else:
            liquidity.taker_depth_remaining[side] -= quantity
        full = quantity == order.remaining_quantity
        fill_id = deterministic_id(
            "paper_fill", intent.order_id, market.event_id, order.fill_attempts, quantity, price
        )
        event = self._event(
            PaperEventType.ORDER_FILLED if full else PaperEventType.ORDER_PARTIALLY_FILLED,
            at=processed_at,
            received_at=processed_at,
            causation_id=market.event_id,
            correlation_id=intent.decision_id,
            payload=self._order_event_payload(
                intent,
                {
                    "fee": decimal_text(fee),
                    "fill_id": fill_id,
                    "fill_price": decimal_text(price),
                    "fill_quantity": decimal_text(quantity),
                    "liquidity": "MAKER" if is_maker else "TAKER",
                    "order_id": intent.order_id,
                    "slippage_bps": decimal_text(slippage_bps),
                    "source_market_received_at": utc_text(market.received_at),
                },
            ),
            ordinal=len(events),
        )
        apply_event(projection, event)
        events.append(event)
        refreshed = projection.orders[intent.order_id]
        if intent.time_in_force is TimeInForce.IOC and refreshed.remaining_quantity > 0:
            self._terminal_ioc(projection, events, refreshed, market, processed_at=processed_at, filled=True)

    def _terminal_ioc(
        self,
        projection: PaperProjection,
        events: list[PaperEvent],
        order: PaperOrder,
        market: MarketEvent,
        *,
        processed_at: datetime,
        filled: bool,
        reason: str = "IOC remainder expired",
    ) -> None:
        event_type = PaperEventType.ORDER_EXPIRED if filled else PaperEventType.ORDER_NO_FILL
        event = self._event(
            event_type,
            at=processed_at,
            received_at=processed_at,
            causation_id=market.event_id,
            correlation_id=order.intent.decision_id,
            payload=self._order_event_payload(
                order,
                {"order_id": order.intent.order_id, "reason": reason},
            ),
            ordinal=len(events),
        )
        apply_event(projection, event)
        events.append(event)

    @staticmethod
    def _hedge_quantity_cap(
        projection: PaperProjection,
        order: PaperOrder,
    ) -> Decimal:
        """Cap a delayed hedge leg to exposure created by earlier sibling legs."""

        group_id = order.intent.hedge_group_id
        if group_id is None:
            # An ungrouped delayed ENTRY IOC has no auditable sibling exposure
            # to reduce and is therefore cancelled fail-closed.
            return Decimal(0)
        earlier = [
            sibling
            for sibling in projection.orders.values()
            if sibling.intent.decision_id == order.intent.decision_id
            and sibling.intent.hedge_group_id == group_id
            and sibling.intent.leg_number < order.intent.leg_number
        ]
        if not earlier:
            return Decimal(0)
        completion = min(sibling.filled_quantity / sibling.intent.quantity for sibling in earlier)
        allowed_total = order.intent.quantity * completion
        return max(
            Decimal(0),
            min(order.remaining_quantity, allowed_total - order.filled_quantity),
        )

    def _derive_lifecycle(
        self,
        projection: PaperProjection,
        events: list[PaperEvent],
        market: MarketEvent,
        *,
        processed_at: datetime,
    ) -> None:
        if projection.strategy_projections:
            for strategy_id in sorted(projection.strategy_projections):
                self._derive_strategy_lifecycle(
                    projection,
                    events,
                    market,
                    strategy_id=strategy_id,
                    processed_at=processed_at,
                )
            has_attributed_positions = any(
                strategy.positions for strategy in projection.strategy_projections.values()
            )
            has_active_exit = any(
                order.action is DecisionAction.EXIT and order.status.active
                for order in projection.orders.values()
            )
            if (
                projection.state in {PaperState.REDUCE_ONLY, PaperState.EMERGENCY_FLATTEN}
                and not has_attributed_positions
                and not has_active_exit
            ):
                transition = self._event(
                    PaperEventType.STATE_TRANSITIONED,
                    at=processed_at,
                    received_at=processed_at,
                    causation_id=market.event_id,
                    correlation_id=market.event_id,
                    payload=self._transition_payload(
                        projection.state,
                        PaperState.FLAT,
                        "portfolio protective flatten completed",
                    ),
                    ordinal=len(events),
                )
                apply_event(projection, transition)
                events.append(transition)
            return
        if projection.state in {PaperState.MANUAL_REVIEW, PaperState.PAUSED}:
            return
        entry_orders = [
            order
            for order in projection.orders.values()
            if order.action is DecisionAction.ENTRY
            and order.intent.decision_id == projection.current_entry_decision_id
        ]
        exit_orders = [
            order
            for order in projection.orders.values()
            if order.action is DecisionAction.EXIT
            and order.intent.decision_id == projection.current_exit_decision_id
        ]
        target: PaperState | None = None
        complete_cycle = False
        if projection.state in {PaperState.LEG_1_PENDING, PaperState.HEDGE_PENDING}:
            active = any(order.status.active for order in entry_orders)
            fully_filled = bool(entry_orders) and all(
                order.status is OrderStatus.FILLED for order in entry_orders
            )
            if fully_filled:
                target = PaperState.HEDGED
            elif projection.positions:
                target = PaperState.HEDGE_PENDING
            elif not active:
                target = PaperState.FLAT
        elif projection.state in {
            PaperState.EXIT_PENDING,
            PaperState.EMERGENCY_FLATTEN,
            PaperState.REDUCE_ONLY,
        }:
            active = any(order.status.active for order in exit_orders)
            if not projection.positions and not active:
                target = PaperState.FLAT
                complete_cycle = True
            elif exit_orders and not active and projection.positions:
                target = (
                    projection.state
                    if projection.state in {PaperState.REDUCE_ONLY, PaperState.EMERGENCY_FLATTEN}
                    else PaperState.EMERGENCY_FLATTEN
                )

        if target is not None and target is not projection.state:
            transition = self._event(
                PaperEventType.STATE_TRANSITIONED,
                at=processed_at,
                received_at=processed_at,
                causation_id=market.event_id,
                correlation_id=market.event_id,
                payload=self._transition_payload(projection.state, target, "order lifecycle update"),
                ordinal=len(events),
            )
            apply_event(projection, transition)
            events.append(transition)
        if complete_cycle:
            cycle = self._event(
                PaperEventType.CYCLE_COMPLETED,
                at=processed_at,
                received_at=processed_at,
                causation_id=market.event_id,
                correlation_id=market.event_id,
                payload={"completed_at": utc_text(processed_at)},
                ordinal=len(events),
            )
            apply_event(projection, cycle)
            events.append(cycle)

    def _derive_strategy_lifecycle(
        self,
        projection: PaperProjection,
        events: list[PaperEvent],
        market: MarketEvent,
        *,
        strategy_id: str,
        processed_at: datetime,
    ) -> None:
        strategy = projection.strategy_projection(strategy_id)
        if projection.state in {PaperState.MANUAL_REVIEW, PaperState.PAUSED}:
            return
        if strategy.state in {PaperState.MANUAL_REVIEW, PaperState.PAUSED}:
            return
        entry_orders = [
            order
            for order in projection.orders.values()
            if order.intent.strategy_id == strategy_id
            and order.action is DecisionAction.ENTRY
            and order.intent.decision_id == strategy.current_entry_decision_id
        ]
        exit_orders = [
            order
            for order in projection.orders.values()
            if order.intent.strategy_id == strategy_id
            and order.action is DecisionAction.EXIT
            and order.intent.decision_id == strategy.current_exit_decision_id
        ]
        target: PaperState | None = None
        complete_cycle = False
        if strategy.state in {PaperState.LEG_1_PENDING, PaperState.HEDGE_PENDING}:
            active = any(order.status.active for order in entry_orders)
            fully_filled = bool(entry_orders) and all(
                order.status is OrderStatus.FILLED for order in entry_orders
            )
            if fully_filled:
                target = PaperState.HEDGED
            elif strategy.positions:
                target = PaperState.HEDGE_PENDING
            elif not active:
                target = PaperState.FLAT
        elif strategy.state in {
            PaperState.EXIT_PENDING,
            PaperState.EMERGENCY_FLATTEN,
            PaperState.REDUCE_ONLY,
        }:
            active = any(order.status.active for order in exit_orders)
            if not strategy.positions and not active:
                target = PaperState.FLAT
                complete_cycle = True
            elif exit_orders and not active and strategy.positions:
                target = (
                    strategy.state
                    if strategy.state in {PaperState.REDUCE_ONLY, PaperState.EMERGENCY_FLATTEN}
                    else PaperState.EMERGENCY_FLATTEN
                )
        if target is not None and target is not strategy.state:
            transition = self._event(
                PaperEventType.STATE_TRANSITIONED,
                at=processed_at,
                received_at=processed_at,
                causation_id=market.event_id,
                correlation_id=market.event_id,
                payload={
                    **self._transition_payload(
                        strategy.state,
                        target,
                        "strategy order lifecycle update",
                    ),
                    "strategy_id": strategy_id,
                },
                ordinal=len(events),
            )
            apply_event(projection, transition)
            events.append(transition)
        if complete_cycle:
            cycle = self._event(
                PaperEventType.CYCLE_COMPLETED,
                at=processed_at,
                received_at=processed_at,
                causation_id=market.event_id,
                correlation_id=market.event_id,
                payload={
                    "completed_at": utc_text(processed_at),
                    "strategy_id": strategy_id,
                },
                ordinal=len(events),
            )
            apply_event(projection, cycle)
            events.append(cycle)


__all__ = ["PaperCommandResult", "PaperEngine"]
