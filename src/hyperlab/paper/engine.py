from __future__ import annotations

import gc
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
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
    TimeInForce,
    decimal_text,
    deterministic_id,
    ensure_json_object,
    keyed_uniform,
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
    IntegrityError,
    PaperStore,
    RunConflictError,
    RunNotFoundError,
)


@dataclass(frozen=True, slots=True)
class PaperCommandResult:
    append: AppendResult
    projection: PaperProjection


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

    def start(self) -> PaperCommandResult:
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
        if durable.event_sequence:
            self.store.verify_integrity(self.run_id)
            try:
                restored = self.replay()
            except ValueError:
                damaged = self.projection()
                self.reconcile(
                    as_of=damaged.last_received_at
                    or self.config.validation_started_at
                )
                raise
            errors = self._ledger_reconciliation_errors(restored)
            if errors:
                self.reconcile(
                    as_of=restored.last_received_at or self.config.validation_started_at
                )
                raise ValueError("paper ledger does not reconcile: " + "; ".join(errors))

        input_id = deterministic_id("paper_input_run_started", self.run_id)
        input_payload = {
            "config_hash": self.config.config_hash,
            "input_type": "RUN_START",
            "run_id": self.run_id,
        }
        duplicate = self._deduplicate(input_id, input_payload)
        if duplicate is not None:
            return duplicate

        projection = self.projection()
        event = PaperEvent.create(
            run_id=self.run_id,
            event_type=PaperEventType.RUN_STARTED,
            occurred_at=self.config.validation_started_at,
            received_at=self.config.validation_started_at,
            causation_id=None,
            correlation_id=self.run_id,
            payload={
                "config_hash": self.config.config_hash,
                "run_kind": self.config.run_kind,
                "strategy_hash": self.config.strategy_hash,
            },
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
        return self._commit(
            input_id=input_id,
            input_payload=input_payload,
            base=projection,
            events=(event,),
            explicit_ledger=entries,
        )

    def projection(self) -> PaperProjection:
        projection = self.store.get_projection(self.run_id)
        if projection.config_hash != self.config.config_hash:
            raise RunConflictError("durable paper projection has a different frozen config hash")
        return projection

    def submit_decision(
        self,
        decision: DecisionIntent,
        market: MarketEvent | Mapping[str, MarketEvent],
    ) -> PaperCommandResult:
        if decision.run_id != self.run_id:
            raise ValueError("decision belongs to another paper run")
        if decision.strategy_name != self.config.strategy_name:
            raise ValueError("decision strategy differs from the frozen strategy")
        markets = (
            {market.instrument: market}
            if isinstance(market, MarketEvent)
            else dict(market)
        )
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
        input_payload = {
            "decision": decision.to_dict(),
            "input_type": "STRATEGY_DECISION",
            "markets": [markets[key].to_dict() for key in sorted(markets)],
        }
        duplicate = self._deduplicate(decision.decision_id, input_payload)
        if duplicate is not None:
            return duplicate

        projection = self.projection()
        if projection.state is PaperState.MANUAL_REVIEW:
            raise IntegrityError(self.store.verify_integrity(self.run_id, raise_on_error=False))
        if decision.action is DecisionAction.ENTRY and projection.state is not PaperState.FLAT:
            raise ValueError("ENTRY decisions are accepted only from FLAT")
        if decision.action is DecisionAction.EXIT and projection.state not in {
            PaperState.HEDGED,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.EMERGENCY_FLATTEN,
        }:
            raise ValueError(
                "EXIT decisions require HEDGED, PAUSED, REDUCE_ONLY, or EMERGENCY_FLATTEN"
            )

        events: list[PaperEvent] = []
        working = projection.clone()

        def emit(event_type: PaperEventType, payload: Mapping[str, object]) -> PaperEvent:
            event = self._event(
                event_type,
                at=decision.decided_at,
                received_at=decision.received_at,
                causation_id=decision.decision_id,
                correlation_id=decision.decision_id,
                payload=payload,
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
            PaperState.ENTRY_PLANNED
            if decision.action is DecisionAction.ENTRY
            else PaperState.EXIT_PLANNED
        )
        protective_exit_state = (
            working.state
            if decision.action is DecisionAction.EXIT
            and working.state
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
                self._transition_payload(working.state, planning_state, "strategy decision"),
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
            risk = evaluate_order_risk(working, order, order_market, self.config.risk)
            if risk.accepted and self.config.execution.cost_schedule is not None:
                try:
                    self.config.execution.cost_schedule.lookup(
                        pd.Timestamp(order_market.received_at),
                        order.instrument,
                    )
                except ValueError as error:
                    risk = type(risk)(
                        accepted=False,
                        reasons=(f"point-in-time cost schedule unavailable: {error}",),
                        order_notional=risk.order_notional,
                        projected_gross_notional=risk.projected_gross_notional,
                        projected_net_notional=risk.projected_net_notional,
                        projected_instrument_notional=risk.projected_instrument_notional,
                        risk_reducing=risk.risk_reducing,
                    )
            if not risk.accepted:
                emit(
                    PaperEventType.RISK_REJECTED,
                    {"order_id": order.order_id, "risk": risk.to_dict()},
                )
                emit(
                    PaperEventType.ALERT_RAISED,
                    self._alert_payload(
                        code="RISK_REJECTED",
                        severity=AlertSeverity.WARNING,
                        message="; ".join(risk.reasons),
                        causation_id=order.order_id,
                        at=decision.decided_at,
                    ),
                )
                continue
            accepted += 1
            ack_due = decision.decided_at + timedelta(
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
                    "risk": risk.to_dict(),
                },
            )
            if ack_due <= decision.decided_at:
                self._ack_or_reject(
                    working,
                    events,
                    order,
                    order_market,
                    decision.decision_id,
                    received_at=decision.received_at,
                )

        if decision.action is DecisionAction.ENTRY:
            target = PaperState.LEG_1_PENDING if accepted else PaperState.FLAT
        elif protective_exit_state is PaperState.EMERGENCY_FLATTEN:
            target = PaperState.EMERGENCY_FLATTEN
        elif protective_exit_state is not None:
            target = PaperState.EXIT_PENDING if accepted else protective_exit_state
        else:
            target = PaperState.EXIT_PENDING if accepted else PaperState.HEDGED
        if target is not working.state:
            emit(
                PaperEventType.STATE_TRANSITIONED,
                self._transition_payload(working.state, target, "orders planned"),
            )
        return self._commit(
            input_id=decision.decision_id,
            input_payload=input_payload,
            base=projection,
            events=tuple(events),
        )

    def process_market(self, market: MarketEvent) -> PaperCommandResult:
        input_payload = {"input_type": "PUBLIC_MARKET_EVENT", "market": market.to_dict()}
        duplicate = self._deduplicate(market.event_id, input_payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        events: list[PaperEvent] = []
        working = projection.clone()

        def emit(
            event_type: PaperEventType,
            payload: Mapping[str, object],
            *,
            causation_id: str | None = None,
        ) -> PaperEvent:
            event = self._event(
                event_type,
                at=market.received_at,
                received_at=market.received_at,
                causation_id=causation_id or market.event_id,
                correlation_id=causation_id or market.event_id,
                payload=payload,
                ordinal=len(events),
            )
            apply_event(working, event)
            events.append(event)
            return event

        if market.stale or market.gap:
            code = "MARKET_GAP" if market.gap else "STALE_MARKET_DATA"
            episode_id = deterministic_id(
                "paper_market_gap_episode" if market.gap else "paper_stale_feed_episode",
                self.run_id,
                utc_text(
                    working.last_market_received_at
                    or self.config.validation_started_at
                ),
            )
            alert_payload = self._alert_payload(
                code=code,
                severity=AlertSeverity.CRITICAL,
                message="matching stopped because public market state is not trustworthy",
                causation_id=episode_id,
                at=market.received_at,
            )
            if not self._alert_is_durable(str(alert_payload["alert_id"])):
                emit(PaperEventType.ALERT_RAISED, alert_payload)
            if working.state not in {PaperState.MANUAL_REVIEW, PaperState.PAUSED}:
                emit(
                    PaperEventType.STATE_TRANSITIONED,
                    self._transition_payload(working.state, PaperState.PAUSED, code),
                )
            self._protect_entry_orders(
                working,
                events,
                at=market.received_at,
                causation_id=market.event_id,
                reason=code,
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
            },
        )

        current_equity = working.equity
        protective_reason: str | None = None
        protective_code: str | None = None
        if working.session_start_equity - current_equity >= self.config.risk.max_daily_loss:
            protective_reason = "daily loss limit reached"
            protective_code = "DAILY_LOSS_LIMIT"
        elif working.peak_equity - current_equity >= self.config.risk.max_drawdown:
            protective_reason = "drawdown limit reached"
            protective_code = "DRAWDOWN_LIMIT"
        if protective_reason is not None and working.positions and working.state not in {
            PaperState.REDUCE_ONLY,
            PaperState.EMERGENCY_FLATTEN,
            PaperState.MANUAL_REVIEW,
        }:
            assert protective_code is not None
            emit(
                PaperEventType.ALERT_RAISED,
                self._alert_payload(
                    code=protective_code,
                    severity=AlertSeverity.CRITICAL,
                    message=(
                        f"{protective_reason}; equity={decimal_text(current_equity)}; "
                        f"peak={decimal_text(working.peak_equity)}; "
                        f"session_start={decimal_text(working.session_start_equity)}"
                    ),
                    causation_id=market.event_id,
                    at=market.received_at,
                ),
            )
            emit(
                PaperEventType.STATE_TRANSITIONED,
                self._transition_payload(
                    working.state,
                    PaperState.REDUCE_ONLY,
                    protective_reason,
                ),
            )

        if working.state in {
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.EMERGENCY_FLATTEN,
        }:
            self._protect_entry_orders(
                working,
                events,
                at=market.received_at,
                causation_id=market.event_id,
                reason=f"state {working.state.value}",
            )

        for order in self._ordered_orders(working):
            if order.intent.instrument != market.instrument:
                continue
            if order.status is OrderStatus.CANCEL_PENDING:
                timeout_wins = (
                    order.expires_at is not None
                    and order.expires_at <= market.received_at
                    and (
                        order.cancel_effective_at is None
                        or order.expires_at <= order.cancel_effective_at
                    )
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
                if (
                    order.cancel_effective_at is not None
                    and order.cancel_effective_at <= market.received_at
                ):
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
                and order.active_at <= market.received_at
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
                        reason=f"state {working.state.value} blocks new exposure",
                    )
                else:
                    self._ack_or_reject(
                        working,
                        events,
                        order.intent,
                        market,
                        market.event_id,
                    )

        liquidity = _MarketLiquidity.from_market(market)
        for order in self._ordered_orders(working):
            if order.intent.instrument != market.instrument or order.status not in {
                OrderStatus.ACKED,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.CANCEL_PENDING,
            }:
                continue
            if (
                order.action is DecisionAction.ENTRY
                and working.state
                in {
                    PaperState.PAUSED,
                    PaperState.REDUCE_ONLY,
                    PaperState.EMERGENCY_FLATTEN,
                }
                and order.status is not OrderStatus.CANCEL_PENDING
            ):
                continue
            if order.expires_at is not None and order.expires_at <= market.received_at:
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
                order.active_at + timedelta(milliseconds=self.config.execution.fill_latency_ms)
                > market.received_at
            ):
                continue
            self._match_order(working, events, order, market, liquidity)

        self._derive_lifecycle(working, events, market)
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
            payload={"cancel_effective_at": utc_text(effective), "order_id": order_id},
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
    ) -> PaperCommandResult:
        input_id = deterministic_id(
            "paper_funding_input",
            self.run_id,
            source_event_id,
        )
        payload = {
            "amount": decimal_text(amount),
            "input_type": "PUBLIC_FUNDING_SETTLEMENT",
            "instrument": instrument,
            "occurred_at": utc_text(occurred_at),
            "source_event_id": source_event_id,
        }
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        if instrument not in projection.positions:
            raise ValueError("funding cannot be posted without an open simulated position")
        event = self._event(
            PaperEventType.FUNDING_POSTED,
            at=occurred_at,
            received_at=occurred_at,
            causation_id=source_event_id,
            correlation_id=input_id,
            payload={"amount": decimal_text(amount), "instrument": instrument},
        )
        return self._commit(
            input_id=input_id,
            input_payload=payload,
            base=projection,
            events=(event,),
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
        input_id = deterministic_id(
            "paper_resilience_exercise_input", self.run_id, normalized, artifact
        )
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
        required_instruments.update(
            order.intent.instrument for order in working.active_orders
        )
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
        if feed_stale:
            stale_payload = self._alert_payload(
                code="STALE_MARKET_DATA",
                severity=AlertSeverity.CRITICAL,
                message=(
                    "no fresh normalized public market event before the frozen timeout"
                    if not stale_instruments
                    else "stale normalized public market channels: "
                    + ", ".join(stale_instruments)
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
            if working.state not in {PaperState.PAUSED, PaperState.MANUAL_REVIEW}:
                emit(
                    PaperEventType.STATE_TRANSITIONED,
                    self._transition_payload(
                        working.state,
                        PaperState.PAUSED,
                        "stale public market feed",
                    ),
                )
            self._protect_entry_orders(
                working,
                events,
                at=as_of,
                causation_id=input_id,
                reason="stale public market feed",
            )

        state_since = working.state_since or self.config.validation_started_at
        unhedged = working.state in {
            PaperState.LEG_1_PENDING,
            PaperState.HEDGE_PENDING,
            PaperState.EXIT_PENDING,
        } and bool(working.positions)
        if unhedged and as_of - state_since >= timedelta(
            seconds=self.config.risk.unhedged_timeout_seconds
        ):
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
            emit(
                PaperEventType.STATE_TRANSITIONED,
                self._transition_payload(
                    working.state,
                    PaperState.EMERGENCY_FLATTEN,
                    "unhedged timeout",
                ),
            )
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
    ) -> PaperCommandResult:
        """Explicit audited recovery after fresh data and exact reconciliation."""

        artifact = self._evidence_hash(review_artifact_hash, label="review artifact_hash")
        input_id = deterministic_id("paper_resume_input", self.run_id, artifact)
        payload = {
            "as_of": utc_text(as_of),
            "input_type": "RESUME_AFTER_REVIEW",
            "review_artifact_hash": artifact,
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
        if projection.last_market_received_at is None or as_of - projection.last_market_received_at > timedelta(
            seconds=self.config.risk.stale_after_seconds
        ):
            raise ValueError("resume_from_pause requires a fresh normalized public market")
        if any(
            order.action is DecisionAction.ENTRY and order.status.active
            for order in projection.orders.values()
        ):
            raise ValueError("resume_from_pause requires all entry orders to be terminal")
        target = PaperState.REDUCE_ONLY if projection.positions else PaperState.FLAT
        event = self._event(
            PaperEventType.STATE_TRANSITIONED,
            at=as_of,
            received_at=as_of,
            causation_id=artifact,
            correlation_id=input_id,
            payload=self._transition_payload(
                projection.state,
                target,
                "explicit reviewed recovery",
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
        if projection.state is not PaperState.EMERGENCY_FLATTEN:
            raise ValueError("emergency_flatten requires EMERGENCY_FLATTEN state")
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

    def replay(self) -> PaperProjection:
        run = self.store.get_run(self.run_id)
        events = self.store.get_events(self.run_id)
        replayed = replay_projection(
            run_id=self.run_id,
            config_hash=self.config.config_hash,
            initial_cash=self.config.initial_cash,
            events=events,
        )
        durable = self.projection()
        if replayed.to_dict() != durable.to_dict():
            raise ValueError("event replay differs from the durable paper projection")
        if replayed.last_event_hash != run.event_head_hash:
            raise ValueError("event replay head differs from the durable hash-chain head")
        return replayed

    def verify_input_replay(self) -> PaperProjection:
        """Re-run the canonical inbox through a fresh engine and compare outputs exactly."""

        source_inputs = self.store.get_inputs(self.run_id)
        source_events = self.store.get_events(self.run_id)
        source_ledger = self.store.get_ledger_entries(self.run_id)
        with TemporaryDirectory(prefix="hyperlab-paper-replay-") as directory:
            replay_store = PaperStore(Path(directory) / "paper-replay.sqlite3")
            replay_engine = PaperEngine(replay_store, self.config)
            replay_engine.start()
            for record in source_inputs:
                payload = record.payload
                input_type = str(payload.get("input_type", ""))
                if input_type == "RUN_START":
                    continue
                if input_type == "PUBLIC_MARKET_EVENT":
                    raw_market = payload.get("market")
                    if not isinstance(raw_market, Mapping):
                        raise ValueError("replay market input lacks market payload")
                    replay_engine.process_market(MarketEvent.from_dict(raw_market))
                elif input_type == "STRATEGY_DECISION":
                    raw_decision = payload.get("decision")
                    raw_markets = payload.get("markets")
                    if not isinstance(raw_decision, Mapping) or not isinstance(
                        raw_markets, Sequence
                    ):
                        raise ValueError("replay decision input is incomplete")
                    decision = DecisionIntent.from_dict(raw_decision)
                    markets = {
                        market.instrument: market
                        for item in raw_markets
                        if isinstance(item, Mapping)
                        for market in (MarketEvent.from_dict(item),)
                    }
                    replay_engine.submit_decision(decision, markets)
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
                    )
                elif input_type == "TIMER":
                    replay_engine.process_timer(
                        as_of=datetime.fromisoformat(
                            str(payload["as_of"]).replace("Z", "+00:00")
                        )
                    )
                elif input_type == "RECONCILE":
                    replay_engine.reconcile(
                        as_of=datetime.fromisoformat(
                            str(payload["as_of"]).replace("Z", "+00:00")
                        )
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
                    # The event timestamp is the durable received time; the
                    # evidence payload intentionally keeps only its covered window.
                    source_event = next(
                        item.event
                        for item in source_events
                        if item.event.correlation_id == record.input_id
                    )
                    replay_engine.record_observation_coverage(
                        artifact_hash=str(payload["artifact_hash"]),
                        window_start=datetime.fromisoformat(
                            str(payload["window_start"]).replace("Z", "+00:00")
                        ),
                        window_end=datetime.fromisoformat(
                            str(payload["window_end"]).replace("Z", "+00:00")
                        ),
                        continuous=bool(payload["continuous"]),
                        recorded_at=source_event.received_at,
                    )
                elif input_type == "RESUME_AFTER_REVIEW":
                    replay_engine.resume_from_pause(
                        as_of=datetime.fromisoformat(
                            str(payload["as_of"]).replace("Z", "+00:00")
                        ),
                        review_artifact_hash=str(payload["review_artifact_hash"]),
                    )
                else:
                    raise ValueError(f"unsupported durable replay input type {input_type!r}")

            replay_events = replay_store.get_events(self.run_id)
            if [(event.hash_payload(), event.event_hash) for event in replay_events] != [
                (event.hash_payload(), event.event_hash) for event in source_events
            ]:
                raise ValueError("canonical input replay produced different paper events")
            replay_ledger = replay_store.get_ledger_entries(self.run_id)
            if [entry.entry for entry in replay_ledger] != [
                entry.entry for entry in source_ledger
            ]:
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

    def reconcile(self, *, as_of: datetime) -> PaperCommandResult:
        input_id = deterministic_id("paper_reconcile", self.run_id, utc_text(as_of))
        payload = {"as_of": utc_text(as_of), "input_type": "RECONCILE"}
        duplicate = self._deduplicate(input_id, payload)
        if duplicate is not None:
            return duplicate
        projection = self.projection()
        try:
            report = self.store.verify_integrity(self.run_id)
            replayed = self.replay()
            ledger_errors = self._ledger_reconciliation_errors(replayed)
            if ledger_errors:
                raise ValueError("; ".join(ledger_errors))
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

    def _ledger_reconciliation_errors(self, projection: PaperProjection) -> tuple[str, ...]:
        balances: dict[str, Decimal] = {}
        for entry in self.store.get_ledger_entries(self.run_id):
            balances[entry.account] = balances.get(entry.account, Decimal(0)) + entry.amount
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
        expected_realized = -balances.get("income:realized_pnl", Decimal(0))
        expected_realized -= balances.get("expense:fees", Decimal(0))
        expected_realized -= balances.get("income:funding", Decimal(0))
        if expected_realized != projection.realized_pnl:
            errors.append("ledger realized PnL differs from the replayed projection")
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
        return any(
            alert.alert_id == alert_id for alert in self.store.get_alerts(self.run_id)
        )

    @staticmethod
    def _ordered_orders(projection: PaperProjection) -> tuple[PaperOrder, ...]:
        return tuple(
            sorted(
                projection.orders.values(),
                key=lambda candidate: (
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
        reason: str,
    ) -> None:
        event = self._event(
            PaperEventType.ORDER_REJECTED,
            at=market.received_at,
            received_at=market.received_at,
            causation_id=market.event_id,
            correlation_id=order.intent.decision_id,
            payload={"order_id": order.intent.order_id, "reason": reason},
            ordinal=len(events),
        )
        apply_event(projection, event)
        events.append(event)

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
                    payload={
                        "order_id": order.intent.order_id,
                        "reason": f"protective state before ack: {reason}",
                    },
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
                payload={
                    "cancel_effective_at": utc_text(effective),
                    "order_id": order.intent.order_id,
                    "reason": reason,
                },
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
                    payload={"order_id": order.intent.order_id},
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
        at = max(market.received_at, intent.created_at)
        command_received_at = received_at or market.received_at
        if intent.order_type is PaperOrderType.MAKER and (
            (intent.side is OrderSide.BUY and cast(Decimal, intent.limit_price) >= market.ask_price)
            or (
                intent.side is OrderSide.SELL
                and cast(Decimal, intent.limit_price) <= market.bid_price
            )
        ):
            event = self._event(
                PaperEventType.ORDER_REJECTED,
                at=at,
                received_at=command_received_at,
                causation_id=causation_id,
                correlation_id=intent.decision_id,
                payload={"order_id": intent.order_id, "reason": "post_only_would_cross"},
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
                payload={
                    "active_at": utc_text(at),
                    "expires_at": utc_text(expires_at) if expires_at is not None else None,
                    "order_id": intent.order_id,
                },
                ordinal=len(events),
            )
        apply_event(projection, event)
        events.append(event)

    def _match_order(
        self,
        projection: PaperProjection,
        events: list[PaperEvent],
        order: PaperOrder,
        market: MarketEvent,
        liquidity: _MarketLiquidity,
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
                        reason=reason,
                    )
                elif intent.time_in_force is TimeInForce.IOC:
                    self._terminal_ioc(
                        projection,
                        events,
                        order,
                        market,
                        filled=order.filled_quantity > 0,
                        reason=reason,
                    )
                else:
                    expiry = self._event(
                        PaperEventType.ORDER_EXPIRED,
                        at=market.received_at,
                        received_at=market.received_at,
                        causation_id=market.event_id,
                        correlation_id=intent.decision_id,
                        payload={"order_id": intent.order_id, "reason": reason},
                        ordinal=len(events),
                    )
                    apply_event(projection, expiry)
                    events.append(expiry)
                alert = self._event(
                    PaperEventType.ALERT_RAISED,
                    at=market.received_at,
                    received_at=market.received_at,
                    causation_id=market.event_id,
                    correlation_id=intent.decision_id,
                    payload=self._alert_payload(
                        code="COST_SCHEDULE_GAP",
                        severity=AlertSeverity.CRITICAL,
                        message=reason,
                        causation_id=intent.order_id,
                        at=market.received_at,
                    ),
                    ordinal=len(events),
                )
                apply_event(projection, alert)
                events.append(alert)
                if projection.state not in {PaperState.PAUSED, PaperState.MANUAL_REVIEW}:
                    transition = self._event(
                        PaperEventType.STATE_TRANSITIONED,
                        at=market.received_at,
                        received_at=market.received_at,
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
            current = projection.positions.get(intent.instrument, Decimal(0))
            if current == 0 or (
                (current > 0 and intent.side is not OrderSide.SELL)
                or (current < 0 and intent.side is not OrderSide.BUY)
            ):
                if intent.time_in_force is TimeInForce.IOC:
                    self._terminal_ioc(projection, events, order, market, filled=False)
                return
            quantity_cap = min(quantity_cap, abs(current))
        elif intent.time_in_force is TimeInForce.IOC and intent.leg_number > 1:
            quantity_cap = self._hedge_quantity_cap(projection, order)
            if quantity_cap <= 0:
                self._terminal_ioc(projection, events, order, market, filled=False)
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
            maker_probability = self.config.execution.maker_fill.probability(
                participation=participation
            )
            maker_draw = keyed_uniform(
                self.config.seed,
                purpose="maker_fill",
                identity=deterministic_id(
                    "paper_maker_attempt", intent.order_id, market.event_id
                ),
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
                self._terminal_ioc(projection, events, order, market, filled=False)
                return
            top = market.ask_price if intent.side is OrderSide.BUY else market.bid_price
            depth = liquidity.taker_depth_remaining[side]
            if depth <= 0:
                self._terminal_ioc(projection, events, order, market, filled=False)
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
            self._terminal_ioc(projection, events, order, market, filled=False)
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
            at=market.received_at,
            received_at=market.received_at,
            causation_id=market.event_id,
            correlation_id=intent.decision_id,
            payload={
                "fee": decimal_text(fee),
                "fill_id": fill_id,
                "fill_price": decimal_text(price),
                "fill_quantity": decimal_text(quantity),
                "liquidity": "MAKER" if is_maker else "TAKER",
                "order_id": intent.order_id,
                "slippage_bps": decimal_text(slippage_bps),
            },
            ordinal=len(events),
        )
        apply_event(projection, event)
        events.append(event)
        refreshed = projection.orders[intent.order_id]
        if intent.time_in_force is TimeInForce.IOC and refreshed.remaining_quantity > 0:
            self._terminal_ioc(projection, events, refreshed, market, filled=True)

    def _terminal_ioc(
        self,
        projection: PaperProjection,
        events: list[PaperEvent],
        order: PaperOrder,
        market: MarketEvent,
        *,
        filled: bool,
        reason: str = "IOC remainder expired",
    ) -> None:
        event_type = PaperEventType.ORDER_EXPIRED if filled else PaperEventType.ORDER_NO_FILL
        event = self._event(
            event_type,
            at=market.received_at,
            received_at=market.received_at,
            causation_id=market.event_id,
            correlation_id=order.intent.decision_id,
            payload={"order_id": order.intent.order_id, "reason": reason},
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
        completion = min(
            sibling.filled_quantity / sibling.intent.quantity for sibling in earlier
        )
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
    ) -> None:
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
                target = PaperState.EMERGENCY_FLATTEN

        if target is not None and target is not projection.state:
            transition = self._event(
                PaperEventType.STATE_TRANSITIONED,
                at=market.received_at,
                received_at=market.received_at,
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
                at=market.received_at,
                received_at=market.received_at,
                causation_id=market.event_id,
                correlation_id=market.event_id,
                payload={"completed_at": utc_text(market.received_at)},
                ordinal=len(events),
            )
            apply_event(projection, cycle)
            events.append(cycle)

__all__ = ["PaperCommandResult", "PaperEngine"]
