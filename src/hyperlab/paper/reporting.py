from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import cast

from hyperlab.backtest.protocol import canonical_sha256
from hyperlab.paper.models import decimal_text, parse_utc, utc_text
from hyperlab.paper.store import AlertRecord, PaperStore, StoredEventRecord

MAX_TIMELINE_LIMIT = 500
MAX_DAY_LIMIT = 366
MAX_ALERT_LIMIT = 200
MAX_SOURCE_INSTRUMENT_LIMIT = 100

_TIMELINE_EVENT_TYPES = (
    "RUNTIME_SESSION_STARTED",
    "RUNTIME_SESSION_STOPPED",
    "DECISION_RECORDED",
    "ORDER_PLANNED",
    "RISK_ACCEPTED",
    "RISK_REJECTED",
    "ORDER_ACKED",
    "ORDER_REJECTED",
    "CANCEL_REQUESTED",
    "ORDER_PARTIALLY_FILLED",
    "ORDER_FILLED",
    "ORDER_CANCELLED",
    "ORDER_EXPIRED",
    "ORDER_NO_FILL",
    "FUNDING_POSTED",
    "CYCLE_COMPLETED",
    "PUBLIC_SOURCE_HEALTH_RECORDED",
    "STATE_TRANSITIONED",
    "ALERT_RAISED",
    "RECONCILIATION_SUCCEEDED",
    "RECONCILIATION_FAILED",
)
_ACTIVE_ORDER_STATUSES = frozenset({"RISK_ACCEPTED", "ACKED", "CANCEL_PENDING", "PARTIALLY_FILLED"})


class PaperReportIntegrityError(RuntimeError):
    """A report was refused because its durable authority failed verification."""


class PaperReportHeadChangedError(PaperReportIntegrityError):
    """Report assembly raced a durable commit and exhausted its retry bound."""


def _bounded(value: int, *, label: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{label} must be between 1 and {maximum}")
    return value


def _decimal(value: object, *, default: Decimal = Decimal(0)) -> Decimal:
    if value is None:
        return default
    if isinstance(value, (bool, float)):
        raise ValueError("durable monetary values must use exact decimal strings")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError("durable monetary value is not an exact decimal") from error
    if not result.is_finite():
        raise ValueError("durable monetary value must be finite")
    return result


def _decimal_mapping(value: object) -> dict[str, Decimal]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): _decimal(item) for key, item in value.items()}


def _ratio_percent(numerator: Decimal, denominator: Decimal) -> str | None:
    if denominator == 0:
        return None
    return decimal_text(numerator * Decimal(100) / denominator)


def _account_metrics(projection: Mapping[str, object]) -> dict[str, object]:
    cash = _decimal(projection.get("cash"))
    initial_cash = _decimal(projection.get("initial_cash"))
    fees = _decimal(projection.get("fees"))
    realized = _decimal(projection.get("realized_pnl"))
    peak_equity = _decimal(projection.get("peak_equity"), default=initial_cash)
    session_start = _decimal(
        projection.get("session_start_equity"),
        default=initial_cash,
    )
    positions = _decimal_mapping(projection.get("positions"))
    marks = _decimal_mapping(projection.get("marks"))
    cost_basis = _decimal_mapping(projection.get("cost_basis"))
    inventory = _decimal_mapping(projection.get("inventory_value"))
    archived_order_count = projection.get("archived_order_count", 0)
    if (
        isinstance(archived_order_count, bool)
        or not isinstance(archived_order_count, int)
        or archived_order_count < 0
    ):
        raise ValueError("projection archived_order_count must be a non-negative integer")

    marked_value = Decimal(
        sum((quantity * marks.get(instrument, Decimal(0))) for instrument, quantity in positions.items())
    )
    equity = cash + marked_value
    unrealized = Decimal(
        sum(
            (
                quantity * marks.get(instrument, Decimal(0))
                - inventory.get(instrument, quantity * cost_basis.get(instrument, Decimal(0)))
            )
            for instrument, quantity in positions.items()
        )
    )
    drawdown = max(Decimal(0), peak_equity - equity)
    daily_pnl = equity - session_start
    gross_notional = Decimal(
        sum((abs(quantity * marks.get(instrument, Decimal(0)))) for instrument, quantity in positions.items())
    )
    net_notional = marked_value

    position_rows: list[dict[str, object]] = []
    for instrument, quantity in sorted(positions.items()):
        mark = marks.get(instrument)
        basis = cost_basis.get(instrument)
        inventory_value = inventory.get(instrument)
        market_value = quantity * mark if mark is not None else None
        position_unrealized = (
            market_value
            - (inventory_value if inventory_value is not None else quantity * (basis or Decimal(0)))
            if market_value is not None
            else None
        )
        position_rows.append(
            {
                "average_cost": decimal_text(basis) if basis is not None else None,
                "instrument": instrument,
                "inventory_value": (decimal_text(inventory_value) if inventory_value is not None else None),
                "mark": decimal_text(mark) if mark is not None else None,
                "market_value": (decimal_text(market_value) if market_value is not None else None),
                "notional": (decimal_text(abs(market_value)) if market_value is not None else None),
                "quantity": decimal_text(quantity),
                "unrealized_pnl": (
                    decimal_text(position_unrealized) if position_unrealized is not None else None
                ),
            }
        )

    raw_orders = projection.get("orders")
    orders = raw_orders if isinstance(raw_orders, Mapping) else {}
    active_orders = [
        cast(dict[str, object], dict(order))
        for _, order in sorted(orders.items(), key=lambda item: str(item[0]))
        if isinstance(order, Mapping) and order.get("status") in _ACTIVE_ORDER_STATUSES
    ]
    return {
        "active_order_count": len(active_orders),
        "active_orders": active_orders,
        "archived_order_count": archived_order_count,
        "cash": decimal_text(cash),
        "cumulative_pnl": decimal_text(equity - initial_cash),
        "daily_pnl": decimal_text(daily_pnl),
        "drawdown": decimal_text(drawdown),
        "drawdown_percent": _ratio_percent(drawdown, peak_equity),
        "equity": decimal_text(equity),
        "fees": decimal_text(fees),
        "gross_notional": decimal_text(gross_notional),
        "initial_cash": decimal_text(initial_cash),
        "mark_coverage_complete": all(instrument in marks for instrument in positions),
        "nav": decimal_text(equity),
        "net_notional": decimal_text(net_notional),
        "net_pnl": decimal_text(equity - initial_cash),
        "peak_equity": decimal_text(peak_equity),
        "positions": position_rows,
        "realized_pnl": decimal_text(realized),
        "session_date": projection.get("session_date"),
        "session_start_equity": decimal_text(session_start),
        "unrealized_pnl": decimal_text(unrealized),
    }


def _market_context(input_payload: Mapping[str, object] | None) -> list[dict[str, object]]:
    if input_payload is None:
        return []
    raw_markets: list[Mapping[str, object]] = []
    one_market = input_payload.get("market")
    if isinstance(one_market, Mapping):
        raw_markets.append(one_market)
    many_markets = input_payload.get("markets")
    if isinstance(many_markets, Sequence) and not isinstance(many_markets, (str, bytes)):
        raw_markets.extend(item for item in many_markets if isinstance(item, Mapping))

    contexts: list[dict[str, object]] = []
    for market in raw_markets:
        bid = _decimal(market.get("bid_price"))
        ask = _decimal(market.get("ask_price"))
        mid = (bid + ask) / Decimal(2)
        spread = ask - bid
        contexts.append(
            {
                "ask_depth": market.get("ask_depth"),
                "ask_price": market.get("ask_price"),
                "bid_depth": market.get("bid_depth"),
                "bid_price": market.get("bid_price"),
                "event_id": market.get("event_id"),
                "gap": bool(market.get("gap", False)),
                "instrument": market.get("instrument"),
                "received_at": market.get("received_at"),
                "source_connection_epoch": market.get("source_connection_epoch"),
                "source_connection_id": market.get("source_connection_id"),
                "source_event_kind": market.get("source_event_kind"),
                "spread": decimal_text(spread),
                "spread_bps": (decimal_text(spread * Decimal(10_000) / mid) if mid > 0 else None),
                "stale": bool(market.get("stale", False)),
                "tradable": bool(market.get("tradable", True)),
            }
        )
    return contexts


def _timeline_item(
    record: StoredEventRecord,
    input_payload: Mapping[str, object] | None,
) -> dict[str, object]:
    event = record.event
    raw_details = event.get("payload")
    details = dict(raw_details) if isinstance(raw_details, Mapping) else {}
    item: dict[str, object] = {
        "causation_id": event.get("causation_id"),
        "correlation_id": event.get("correlation_id"),
        "created_at": record.created_at,
        "details": details,
        "event_id": record.event_id,
        "event_type": record.event_type,
        "input_id": record.input_id,
        "market_context": _market_context(input_payload),
        "occurred_at": event.get("occurred_at"),
        "received_at": event.get("received_at"),
        "sequence": record.sequence,
    }
    if input_payload is not None:
        item["input_type"] = input_payload.get("input_type")
    if details.get("strategy_id") is not None:
        item["strategy_id"] = details.get("strategy_id")
    if record.event_type == "DECISION_RECORDED":
        decision = details.get("decision")
        if isinstance(decision, Mapping):
            item["action"] = decision.get("action")
            item["decision_id"] = decision.get("decision_id")
            item["intent"] = dict(decision)
            item["signal"] = decision.get("signal")
            item["strategy_id"] = decision.get("strategy_id")
            item["strategy_name"] = decision.get("strategy_name")
    elif record.event_type == "ORDER_PLANNED":
        item["intent"] = details.get("order")
    elif record.event_type in {"ORDER_PARTIALLY_FILLED", "ORDER_FILLED"}:
        item.update(
            {
                "fee": details.get("fee"),
                "fill_id": details.get("fill_id"),
                "fill_price": details.get("fill_price"),
                "fill_quantity": details.get("fill_quantity"),
                "liquidity": details.get("liquidity"),
                "order_id": details.get("order_id"),
                "slippage_bps": details.get("slippage_bps"),
            }
        )
    elif record.event_type == "FUNDING_POSTED":
        item["funding_amount"] = details.get("amount")
        item["instrument"] = details.get("instrument")
    return item


def _daily_series(
    store: PaperStore,
    run_id: str,
    *,
    day_limit: int,
) -> tuple[list[dict[str, object]], bool]:
    history = store.get_daily_projection_records(run_id, limit=day_limit + 1)
    truncated = len(history) > day_limit
    selected = history[-day_limit:]
    predecessor = history[-day_limit - 1] if truncated else None
    previous_equity = (
        _decimal(_account_metrics(predecessor.projection)["equity"]) if predecessor is not None else None
    )
    funding = store.get_funding_by_utc_date(
        run_id,
        utc_dates=tuple(record.utc_date for record in selected),
    )
    rows: list[dict[str, object]] = []
    previous_fees: Decimal | None = (
        _decimal(predecessor.projection.get("fees")) if predecessor is not None else None
    )
    previous_realized: Decimal | None = (
        _decimal(predecessor.projection.get("realized_pnl")) if predecessor is not None else None
    )
    for record in selected:
        metrics = _account_metrics(record.projection)
        equity = _decimal(metrics["equity"])
        initial_cash = _decimal(record.projection.get("initial_cash"))
        fees = _decimal(record.projection.get("fees"))
        realized = _decimal(record.projection.get("realized_pnl"))
        baseline = previous_equity if previous_equity is not None else initial_cash
        fee_baseline = previous_fees if previous_fees is not None else Decimal(0)
        realized_baseline = previous_realized if previous_realized is not None else Decimal(0)
        rows.append(
            {
                "cumulative_pnl": metrics["cumulative_pnl"],
                "date": record.utc_date,
                "daily_fees": decimal_text(fees - fee_baseline),
                "daily_funding": decimal_text(funding.get(record.utc_date, Decimal(0))),
                "daily_pnl": decimal_text(equity - baseline),
                "daily_realized_pnl": decimal_text(realized - realized_baseline),
                "drawdown": metrics["drawdown"],
                "ending_equity": metrics["equity"],
                "ending_nav": metrics["nav"],
                "event_sequence": record.event_sequence,
                "fees": metrics["fees"],
                "projection_hash": record.projection_hash,
                "projection_revision": record.revision,
                "realized_pnl": metrics["realized_pnl"],
                "status": record.status,
                "unrealized_pnl": metrics["unrealized_pnl"],
            }
        )
        previous_equity = equity
        previous_fees = fees
        previous_realized = realized
    return rows, truncated


def _source_health(
    summary: Mapping[str, object],
    *,
    evaluated_at: datetime | None,
    stale_after_seconds: int | None,
) -> dict[str, object]:
    raw_latest = summary.get("latest_by_instrument")
    latest = raw_latest if isinstance(raw_latest, Mapping) else {}
    degraded = any(
        isinstance(market, Mapping)
        and (
            bool(market.get("gap", False))
            or bool(market.get("stale", False))
            or not bool(market.get("tradable", True))
        )
        for market in latest.values()
    )
    event_count = int(str(summary.get("event_count", 0)))
    raw_last_received = summary.get("last_received_at")
    last_received_at = parse_utc(str(raw_last_received)) if raw_last_received is not None else None
    age_seconds: float | None = None
    chronology_invalid = False
    if evaluated_at is not None and last_received_at is not None:
        age_seconds = (evaluated_at - last_received_at).total_seconds()
        chronology_invalid = age_seconds < 0

    if event_count == 0:
        status = "NOT_OBSERVED"
    elif (
        evaluated_at is None or last_received_at is None or stale_after_seconds is None or chronology_invalid
    ):
        status = "UNKNOWN_FRESHNESS_FAIL_CLOSED"
    elif degraded:
        status = "DEGRADED_FAIL_CLOSED"
    elif age_seconds is not None and age_seconds > stale_after_seconds:
        status = "STALE_AT_DURABLE_HEAD_FAIL_CLOSED"
    else:
        status = "OBSERVED_AT_DURABLE_HEAD"
    return {
        **summary,
        "current_wall_clock_evaluated": False,
        "evaluated_at": utc_text(evaluated_at) if evaluated_at is not None else None,
        "freshness_age_seconds": age_seconds,
        "freshness_scope": "DURABLE_HEAD_NOT_WALL_CLOCK",
        "stale_after_seconds": stale_after_seconds,
        "status": status,
    }


def _strategy_reports(
    store: PaperStore,
    run_id: str,
    *,
    projection: Mapping[str, object],
    config: Mapping[str, object],
    alerts: Sequence[AlertRecord],
) -> dict[str, object]:
    raw_projections = projection.get("strategy_projections")
    if not isinstance(raw_projections, Mapping):
        return {}
    raw_orders = projection.get("orders")
    all_orders = raw_orders if isinstance(raw_orders, Mapping) else {}
    marks = projection.get("marks")
    raw_strategies = config.get("strategies")
    config_by_id: dict[str, Mapping[str, object]] = {}
    if isinstance(raw_strategies, Sequence) and not isinstance(raw_strategies, (str, bytes)):
        for item in raw_strategies:
            if isinstance(item, Mapping) and item.get("strategy_id") is not None:
                config_by_id[str(item["strategy_id"])] = item

    result: dict[str, object] = {}
    for raw_strategy_id, raw_local in sorted(raw_projections.items(), key=lambda item: str(item[0])):
        strategy_id = str(raw_strategy_id)
        if not isinstance(raw_local, Mapping):
            raise ValueError("strategy projection must be an object")
        strategy_config = config_by_id.get(strategy_id)
        if strategy_config is None:
            raise ValueError("strategy projection lacks immutable run configuration")
        raw_strategy_risk = strategy_config.get("risk")
        local_orders = {
            str(order_id): order
            for order_id, order in all_orders.items()
            if isinstance(order, Mapping) and order.get("strategy_id") == strategy_id
        }
        metric_projection = {
            **dict(raw_local),
            "archived_order_count": 0,
            "initial_cash": "0",
            "marks": marks if isinstance(marks, Mapping) else {},
            "orders": local_orders,
        }
        metrics = _account_metrics(metric_projection)
        funding_net = -store.get_ledger_account_total(
            run_id,
            account=f"strategy:{strategy_id}:income:funding",
        )
        metrics["funding_net"] = decimal_text(funding_net)
        metrics["trading_realized_pnl_after_fees"] = decimal_text(
            _decimal(raw_local.get("realized_pnl")) - funding_net
        )
        incident_count = raw_local.get("critical_incident_count", 0)
        if isinstance(incident_count, bool) or not isinstance(incident_count, int) or incident_count < 0:
            raise ValueError("strategy critical_incident_count must be non-negative")
        raw_last_incident = raw_local.get("last_critical_incident_at")
        last_incident = utc_text(parse_utc(str(raw_last_incident))) if raw_last_incident is not None else None
        if (incident_count == 0) != (last_incident is None):
            raise ValueError("strategy critical incident summary is inconsistent")
        strategy_alerts = [
            {
                "alert": alert.alert,
                "alert_id": alert.alert_id,
                "code": alert.code,
                "created_at": alert.created_at,
                "event_sequence": alert.event_sequence,
                "severity": alert.severity,
            }
            for alert in alerts
            if isinstance(alert.alert, Mapping) and alert.alert.get("strategy_id") == strategy_id
        ]
        result[strategy_id] = {
            "accounting": metrics,
            "decisions": raw_local.get("decisions", 0),
            "identity": {
                "strategy_config_hash": canonical_sha256(strategy_config),
                "strategy_hash": strategy_config.get("strategy_hash"),
                "strategy_id": strategy_id,
                "strategy_name": strategy_config.get("strategy_name"),
            },
            "incidents": {
                "critical_incident_count": incident_count,
                "last_critical_incident_at": last_incident,
                "recent_alerts": strategy_alerts,
            },
            "risk": {
                "current": {
                    "active_order_count": metrics["active_order_count"],
                    "daily_loss": decimal_text(max(Decimal(0), -_decimal(metrics["daily_pnl"]))),
                    "drawdown": metrics["drawdown"],
                    "gross_notional": metrics["gross_notional"],
                    "net_notional": metrics["net_notional"],
                },
                "limits": dict(raw_strategy_risk) if isinstance(raw_strategy_risk, Mapping) else {},
            },
            "state": raw_local.get("state"),
        }
    return result


def _build_paper_report_once(
    store: PaperStore,
    run_id: str,
    *,
    after_sequence: int = 0,
    timeline_limit: int = 100,
    day_limit: int = 31,
    alert_limit: int = 50,
) -> dict[str, object]:
    """Build one deterministic, bounded, read-only Phase 12 Paper report."""

    if isinstance(after_sequence, bool) or not isinstance(after_sequence, int):
        raise ValueError("after_sequence must be a non-negative integer")
    if after_sequence < 0:
        raise ValueError("after_sequence must be a non-negative integer")
    timeline_limit = _bounded(
        timeline_limit,
        label="timeline_limit",
        maximum=MAX_TIMELINE_LIMIT,
    )
    day_limit = _bounded(day_limit, label="day_limit", maximum=MAX_DAY_LIMIT)
    alert_limit = _bounded(alert_limit, label="alert_limit", maximum=MAX_ALERT_LIMIT)

    run = store.get_run(run_id)
    integrity = store.inspect_head_integrity_readonly(run_id)
    if not integrity.ok:
        codes = ",".join(issue.code for issue in integrity.issues)
        raise PaperReportIntegrityError(
            f"paper report refused for {run_id}: durable head integrity failed ({codes})"
        )

    projection = store.get_projection_payload(run_id)
    account = _account_metrics(projection)
    funding_net = -store.get_ledger_account_total(
        run_id,
        account="income:funding",
    )
    account["funding_net"] = decimal_text(funding_net)
    account["trading_realized_pnl_after_fees"] = decimal_text(
        _decimal(projection.get("realized_pnl")) - funding_net
    )

    event_records = store.get_event_records_by_type(
        run_id,
        event_types=_TIMELINE_EVENT_TYPES,
        after_sequence=after_sequence,
        limit=timeline_limit + 1,
    )
    timeline_has_more = len(event_records) > timeline_limit
    event_page = event_records[:timeline_limit]
    input_payloads = store.get_input_payloads(
        run_id,
        input_ids=tuple(record.input_id for record in event_page),
    )
    timeline = [_timeline_item(record, input_payloads.get(record.input_id)) for record in event_page]

    daily, daily_truncated = _daily_series(store, run_id, day_limit=day_limit)
    raw_alerts = store.get_recent_alerts(run_id, limit=alert_limit + 1)
    alerts_truncated = len(raw_alerts) > alert_limit
    alerts = raw_alerts[-alert_limit:]
    config = run.config_snapshot
    raw_risk = config.get("risk")
    risk_limits = dict(raw_risk) if isinstance(raw_risk, Mapping) else {}
    raw_stale_after = risk_limits.get("stale_after_seconds")
    stale_after_seconds = (
        int(raw_stale_after)
        if isinstance(raw_stale_after, int) and not isinstance(raw_stale_after, bool)
        else None
    )
    raw_head_received_at = projection.get("last_received_at")
    head_received_at = parse_utc(str(raw_head_received_at)) if raw_head_received_at is not None else None

    source = _source_health(
        store.get_public_market_source_summary(
            run_id,
            latest_instrument_limit=MAX_SOURCE_INSTRUMENT_LIMIT,
        ),
        evaluated_at=head_received_at,
        stale_after_seconds=stale_after_seconds,
    )

    config = run.config_snapshot
    run_kind = str(config.get("run_kind", "DEMO"))
    classification = {
        "DEMO": "PAPER_DEMO",
        "TECHNICAL": "PAPER_TECHNICAL",
        "VALIDATION": "PAPER_VALIDATION",
    }.get(run_kind, "PAPER_UNKNOWN")
    raw_risk = config.get("risk")
    risk_limits = dict(raw_risk) if isinstance(raw_risk, Mapping) else {}
    incident_count = projection.get("critical_incident_count", 0)
    if isinstance(incident_count, bool) or not isinstance(incident_count, int) or incident_count < 0:
        raise ValueError("projection critical_incident_count must be a non-negative integer")
    raw_last_incident = projection.get("last_critical_incident_at")
    last_incident = utc_text(parse_utc(str(raw_last_incident))) if raw_last_incident is not None else None
    if (incident_count == 0) != (last_incident is None):
        raise ValueError("projection critical incident summary is inconsistent")

    recent_alerts: dict[str, object] = {
        "has_more": alerts_truncated,
        "items": [
            {
                "alert": alert.alert,
                "alert_id": alert.alert_id,
                "code": alert.code,
                "created_at": alert.created_at,
                "event_sequence": alert.event_sequence,
                "severity": alert.severity,
            }
            for alert in alerts
        ],
        "limit": alert_limit,
        "returned": len(alerts),
    }
    runtime_session = paper_runtime_session_health(projection)
    runtime_session["recent_incidents"] = [
        {
            "alert": alert.alert,
            "alert_id": alert.alert_id,
            "code": alert.code,
            "created_at": alert.created_at,
            "event_sequence": alert.event_sequence,
            "severity": alert.severity,
        }
        for alert in alerts
        if alert.code == "PAPER_RUNTIME_FAILURE"
    ]
    strategies = _strategy_reports(
        store,
        run_id,
        projection=projection,
        config=config,
        alerts=alerts,
    )
    portfolio = dict(account)
    if strategies:
        attributed_gross = Decimal(0)
        attributed_realized = Decimal(0)
        for strategy_report in strategies.values():
            if not isinstance(strategy_report, Mapping):
                raise ValueError("strategy report must be an object")
            strategy_accounting = strategy_report.get("accounting")
            if not isinstance(strategy_accounting, Mapping):
                raise ValueError("strategy report accounting must be an object")
            attributed_gross += _decimal(strategy_accounting.get("gross_notional"))
            attributed_realized += _decimal(strategy_accounting.get("realized_pnl"))
        portfolio["attributed_realized_pnl"] = decimal_text(attributed_realized)
        portfolio["gross_notional"] = decimal_text(attributed_gross)
    report: dict[str, object] = {
        "account": account,
        "classification": {
            "gate_d_status": "NOT_EVALUATED",
            "paper_classification": classification,
            "profitability_evidence": False,
            "run_kind": run_kind,
            "technical_only": run_kind == "TECHNICAL",
        },
        "config": config,
        "daily": {
            "limit": day_limit,
            "returned": len(daily),
            "series": daily,
            "truncated": daily_truncated,
        },
        "identity": {
            "commit_head_hash": run.commit_head_hash,
            "commit_sequence": run.commit_sequence,
            "config_hash": run.config_hash,
            "created_at": run.created_at,
            "event_head_hash": run.event_head_hash,
            "event_sequence": run.event_sequence,
            "projection_hash": run.projection_hash,
            "projection_revision": run.projection_revision,
            "run_id": run.run_id,
            "strategy_hash": config.get("strategy_hash"),
            "strategy_name": config.get("strategy_name"),
            **(
                {
                    "portfolio_id": config.get("portfolio_id"),
                    "strategy_count": len(strategies),
                }
                if strategies
                else {}
            ),
        },
        "integrity": "HEAD_ANCHORS_VERIFIED_READONLY",
        "integrity_scope": {
            "contract": "CURRENT_AND_APPEND_HEAD_V1",
            "full_history_verified": False,
            "same_head_assembly": True,
            "head_read_attempt_limit": 2,
            "full_replay_verified": False,
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
        },
        "mode": "paper-simulation-only",
        "orders_enabled": False,
        **({"portfolio": portfolio, "strategies": strategies} if strategies else {}),
        "risk": {
            "critical_incident_count": incident_count,
            "last_critical_incident_at": last_incident,
            "current": {
                "active_order_count": account["active_order_count"],
                "daily_loss": decimal_text(max(Decimal(0), -_decimal(account["daily_pnl"]))),
                "drawdown": account["drawdown"],
                "gross_notional": portfolio["gross_notional"],
                "net_notional": account["net_notional"],
            },
            "limits": risk_limits,
            "recent_alerts": recent_alerts,
        },
        "runtime": {
            "last_market_received_at": projection.get("last_market_received_at"),
            "last_market_received_at_by_instrument": projection.get(
                "last_market_received_at_by_instrument", {}
            ),
            "last_received_at": projection.get("last_received_at"),
            "reconciled": bool(projection.get("reconciled", False)),
            "session": runtime_session,
            "source": source,
            "state": projection.get("state"),
        },
        "schema_version": 2 if strategies else 1,
        "status": run.status,
        "timeline": {
            "after_sequence": after_sequence,
            "event_types": list(_TIMELINE_EVENT_TYPES),
            "has_more": timeline_has_more,
            "items": timeline,
            "limit": timeline_limit,
            "next_after_sequence": (event_page[-1].sequence if event_page else after_sequence),
            "returned": len(timeline),
        },
    }
    if store.get_run(run_id).head_identity != run.head_identity:
        raise PaperReportHeadChangedError(
            f"paper report retry required for {run_id}: durable head changed during assembly"
        )
    return report


def build_paper_report(
    store: PaperStore,
    run_id: str,
    *,
    after_sequence: int = 0,
    timeline_limit: int = 100,
    day_limit: int = 31,
    alert_limit: int = 50,
) -> dict[str, object]:
    """Build a same-head report with one strictly bounded race retry."""

    last_head_change: PaperReportHeadChangedError | None = None
    for _attempt in range(2):
        try:
            return _build_paper_report_once(
                store,
                run_id,
                after_sequence=after_sequence,
                timeline_limit=timeline_limit,
                day_limit=day_limit,
                alert_limit=alert_limit,
            )
        except PaperReportHeadChangedError as error:
            last_head_change = error
    if last_head_change is None:
        raise AssertionError("paper report retry loop completed without a result")
    raise last_head_change


__all__ = [
    "MAX_ALERT_LIMIT",
    "MAX_DAY_LIMIT",
    "MAX_TIMELINE_LIMIT",
    "PaperReportHeadChangedError",
    "PaperReportIntegrityError",
    "build_paper_report",
]


def paper_runtime_session_health(projection: Mapping[str, object]) -> dict[str, object]:
    """Return the exact bounded, non-sensitive durable runtime-session summary."""

    generation = projection.get("runtime_session_generation", 0)
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise ValueError("projection runtime_session_generation must be non-negative")
    raw_session_id = projection.get("runtime_session_id")
    session_id = str(raw_session_id) if raw_session_id is not None else None
    if session_id is not None and (
        len(session_id) != 64 or any(character not in "0123456789abcdef" for character in session_id)
    ):
        raise ValueError("projection runtime_session_id must be a lowercase SHA-256")
    raw_started_at = projection.get("runtime_session_started_at")
    started_at = utc_text(parse_utc(str(raw_started_at))) if raw_started_at is not None else None
    raw_stopped_at = projection.get("runtime_session_stopped_at")
    stopped_at = utc_text(parse_utc(str(raw_stopped_at))) if raw_stopped_at is not None else None
    if generation == 0:
        if session_id is not None or started_at is not None or stopped_at is not None:
            raise ValueError("zero runtime session generation requires empty session facts")
    elif session_id is None or started_at is None:
        raise ValueError("positive runtime session generation requires identity and start time")
    if stopped_at is not None and started_at is not None and parse_utc(stopped_at) < parse_utc(started_at):
        raise ValueError("runtime session stop cannot precede its start")
    active = generation > 0 and session_id is not None and stopped_at is None
    return {
        "active": active,
        "generation": generation,
        "session_id": session_id,
        "started_at": started_at,
        "stopped_at": stopped_at,
        "unclosed": active,
    }
