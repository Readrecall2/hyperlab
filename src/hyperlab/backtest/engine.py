from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, cast

import numpy as np
import pandas as pd

from hyperlab.backtest.attribution import aggregate_pnl, causal_regimes
from hyperlab.backtest.benchmark import PassiveBenchmarkSpec, build_passive_benchmark
from hyperlab.backtest.costs import (
    CostRule,
    CostSchedule,
    SlippageEstimate,
    SlippageModel,
    adverse_fee_bps,
    parse_instrument,
)
from hyperlab.backtest.execution import ExecutionConfig
from hyperlab.backtest.metrics import compute_metrics
from hyperlab.backtest.risk import apply_risk_limits
from hyperlab.models import BacktestResult, CostModel, MarketPanel, RiskLimits, StrategyOutput

PNL_COMPONENT_COLUMNS = (
    "price_return",
    "funding_return",
    "basis_return",
    "spread_return",
    "fee_return",
    "slippage_return",
    "hedge_return",
)
_TOLERANCE = 1e-12


@dataclass(slots=True)
class _PendingOrder:
    order_id: str
    decision_index: int
    due_index: int
    instrument: str
    requested_weight: float
    order_type: str
    group_id: str | None
    leg_number: int
    emergency_for: str | None = None


def _require_active_data(values: pd.DataFrame, activity: pd.DataFrame, label: str) -> None:
    invalid = ~values.map(
        lambda value: isinstance(value, (int, float, np.number)) and math.isfinite(float(value))
    )
    if bool((invalid & activity.ne(0.0)).any(axis=None)):
        raise ValueError(f"{label} data is missing or non-finite for an active position")


def _as_float(value: object) -> float:
    return float(cast(Any, value))


def _legacy_fee(costs: CostModel, instrument: str) -> float:
    if instrument.endswith(":spot"):
        return costs.spot_fee_bps
    if instrument.startswith("HL:") and instrument.endswith(":perp"):
        return costs.perp_fee_bps
    return costs.external_perp_fee_bps


def _legacy_rule(costs: CostModel, instrument: str) -> CostRule:
    return CostRule(
        instrument=instrument,
        maker_fee_bps=_legacy_fee(costs, instrument) * costs.stress_multiplier,
        taker_fee_bps=_legacy_fee(costs, instrument) * costs.stress_multiplier,
        slippage=SlippageModel(
            base_bps=costs.base_slippage_bps * costs.stress_multiplier,
            impact_coefficient_bps=0.0,
            exponent=1.0,
            max_participation=1.0,
        ),
        source="legacy-uncalibrated-cost-model",
    )


def _data_status(panel: MarketPanel) -> str:
    source = str(panel.metadata.get("source", "")).lower()
    declared = str(panel.metadata.get("calibration_status", "")).upper()
    if "synthetic" in source or declared == "SYNTHETIC":
        return "SYNTHETIC"
    if panel.metadata.get("point_in_time") is True and declared == "CALIBRATED":
        return "CALIBRATED"
    return "UNCALIBRATED"


def _size_bucket(participation: float, *, missed: bool = False) -> str:
    if missed:
        return "missed"
    if participation < 0.01:
        return "<1% depth"
    if participation < 0.05:
        return "1-5% depth"
    if participation < 0.10:
        return "5-10% depth"
    return ">=10% depth"


class PanelBacktester:
    """Causal bar-level portfolio and execution simulator.

    A target produced at ``t`` can be filled only at/after ``t`` and starts earning
    market PnL on ``t -> t+1``. Desired and filled positions are stored separately.
    The simulator has no network transport and cannot send an order.
    """

    def __init__(
        self,
        *,
        costs: CostModel | CostSchedule,
        risk_limits: RiskLimits,
        execution: ExecutionConfig | None = None,
        benchmark: PassiveBenchmarkSpec | None = None,
    ) -> None:
        self.costs = costs
        self.risk_limits = risk_limits
        self.execution = execution or ExecutionConfig()
        self.benchmark_spec = benchmark or PassiveBenchmarkSpec()

    def _cost_rule(self, timestamp: pd.Timestamp, instrument: str) -> CostRule:
        if isinstance(self.costs, CostSchedule):
            return self.costs.lookup(timestamp, instrument)
        return _legacy_rule(self.costs, instrument)

    def _spread_multiplier(self, *, maker: bool) -> float:
        legacy = self.costs.stress_multiplier if isinstance(self.costs, CostModel) else 1.0
        adverse = self.execution.cost_multiplier
        if maker:
            return legacy / adverse
        return legacy * adverse

    def _slippage_estimate(
        self,
        *,
        rule: CostRule,
        notional_usd: float,
        depth_usd: float,
    ) -> SlippageEstimate:
        if math.isinf(depth_usd):
            return SlippageEstimate(
                slippage_bps=rule.slippage.base_bps,
                participation=0.0,
                fill_fraction=1.0,
                capacity_usd=math.inf,
            )
        return rule.slippage.estimate(notional_usd=notional_usd, depth_usd=depth_usd)

    def _validate_strategy_output(
        self,
        panel: MarketPanel,
        output: StrategyOutput,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        weights = output.weights
        if not weights.index.equals(panel.prices.index):
            raise ValueError("strategy weights index must exactly match the market panel")
        if list(weights.columns) != list(panel.prices.columns):
            raise ValueError("strategy weights columns must exactly match the market panel")
        numeric = weights.apply(pd.to_numeric, errors="coerce")
        if bool((~numeric.map(math.isfinite)).any(axis=None)):
            raise ValueError("strategy weights must be finite")
        target = apply_risk_limits(numeric.astype(float), self.risk_limits)

        if output.order_types is None:
            order_types = pd.DataFrame(
                self.execution.default_order_type,
                index=target.index,
                columns=target.columns,
            )
        else:
            order_types = output.order_types
            if not order_types.index.equals(target.index):
                raise ValueError("order_types index must exactly match strategy weights")
            if list(order_types.columns) != list(target.columns):
                raise ValueError("order_types columns must exactly match strategy weights")
            valid = order_types.isin(["maker", "taker"])
            if not bool(valid.all(axis=None)):
                raise ValueError("order_types values must be maker or taker")

        grouped: set[str] = set()
        for group_id, instruments in output.hedge_groups.items():
            if not group_id.strip() or len(instruments) < 2:
                raise ValueError("hedge groups need a name and at least two ordered legs")
            if len(set(instruments)) != len(instruments):
                raise ValueError(f"hedge group {group_id!r} contains duplicate instruments")
            unknown = set(instruments).difference(target.columns)
            if unknown:
                raise ValueError(f"hedge group {group_id!r} has unknown instruments: {unknown}")
            overlap = grouped.intersection(instruments)
            if overlap:
                raise ValueError(f"instruments cannot belong to multiple hedge groups: {overlap}")
            grouped.update(instruments)
        return target, order_types

    def validate_research_panel(self, panel: MarketPanel) -> None:
        """Fail closed before a strategy can fit on non-causal observations.

        Availability and candle finality apply to every supplied observation, not
        merely to rows where the resulting strategy later chooses a non-zero target.
        Otherwise a flat row could leak a future revision into the next decision.
        """

        panel.validate()
        declared_status = str(panel.metadata.get("calibration_status", "UNCALIBRATED")).upper()
        calibration_evidence = panel.metadata.get("calibration_evidence_hash")
        valid_calibration_evidence = (
            isinstance(calibration_evidence, str)
            and len(calibration_evidence) == 64
            and all(character in "0123456789abcdef" for character in calibration_evidence)
        )
        if declared_status == "CALIBRATED" and not valid_calibration_evidence:
            raise ValueError("CALIBRATED market data require a calibration_evidence_hash")
        calibration_source = panel.metadata.get("calibration_source")
        if declared_status == "CALIBRATED" and (
            not isinstance(calibration_source, str)
            or not calibration_source.strip()
            or any(
                marker in calibration_source.casefold()
                for marker in ("uncalibrated", "placeholder", "synthetic")
            )
        ):
            raise ValueError("CALIBRATED market data require a non-placeholder calibration_source")
        required = ("available_at", "finality", "tradable")
        if self.execution.require_point_in_time:
            missing = [name for name in required if getattr(panel, name) is None]
            if missing or panel.metadata.get("point_in_time") is not True:
                raise ValueError(
                    "point-in-time research requires availability, finality, historical universe "
                    f"and metadata; missing={missing}"
                )
            lifecycle_source = panel.metadata.get("historical_universe_source")
            lifecycle_hash = panel.metadata.get("lifecycle_hash")
            if not isinstance(lifecycle_source, str) or not lifecycle_source.strip():
                raise ValueError("point-in-time research requires a historical lifecycle source")
            if (
                not isinstance(lifecycle_hash, str)
                or len(lifecycle_hash) != 64
                or any(character not in "0123456789abcdefABCDEF" for character in lifecycle_hash)
            ):
                raise ValueError("point-in-time research requires a SHA-256 lifecycle_hash")

        observed = (
            panel.prices.notna()
            | panel.funding.notna()
            | panel.spreads_bps.notna()
            | panel.volume_usd.notna()
        )
        if panel.depth_usd is not None:
            observed |= panel.depth_usd.notna()

        if panel.available_at is not None:
            for row_number, timestamp in enumerate(panel.prices.index):
                for column_number, instrument in enumerate(panel.prices.columns):
                    if not bool(observed.iat[row_number, column_number]):
                        continue
                    value = panel.available_at.iat[row_number, column_number]
                    if pd.isna(value):
                        raise ValueError(f"{instrument} is not available at {timestamp}")
                    available = pd.Timestamp(cast(Any, value))
                    if available.tz is None or available.tz_convert("UTC") > timestamp:
                        raise ValueError(f"{instrument} is not available at decision time {timestamp}")
        if panel.finality is not None:
            non_final = panel.finality.ne(True) & observed
            if bool(non_final.any(axis=None)):
                raise ValueError("non-final observation data cannot be exposed to a research decision")
        if panel.regimes is not None and self.execution.require_point_in_time:
            regime_hash = panel.metadata.get("regime_hash")
            if panel.metadata.get("regimes_point_in_time") is not True:
                raise ValueError("external regime labels must be declared point-in-time")
            if (
                not isinstance(regime_hash, str)
                or len(regime_hash) != 64
                or any(character not in "0123456789abcdefABCDEF" for character in regime_hash)
            ):
                raise ValueError("external point-in-time regimes require a SHA-256 regime_hash")

    def _validate_point_in_time(self, panel: MarketPanel, target: pd.DataFrame) -> None:
        self.validate_research_panel(panel)
        if panel.tradable is not None:
            # A delisted instrument may be reduced/closed according to the recorded
            # exit policy, but no non-zero target may survive outside the universe.
            outside = panel.tradable.ne(True) & target.abs().gt(_TOLERANCE)
            if bool(outside.any(axis=None)):
                raise ValueError("strategy traded outside the point-in-time historical universe")

    def run(self, panel: MarketPanel, output: StrategyOutput) -> BacktestResult:
        panel.validate()
        target, order_types = self._validate_strategy_output(panel, output)
        index = panel.prices.index
        terminal_mark_value = output.diagnostics.get("terminal_mark_without_decision")
        terminal_mark: pd.Timestamp | None = None
        validation_target = target
        if terminal_mark_value is not None:
            terminal_mark = pd.Timestamp(terminal_mark_value)
            if terminal_mark.tz is None:
                raise ValueError("terminal_mark_without_decision must be timezone-aware UTC")
            terminal_mark = terminal_mark.tz_convert("UTC")
            if terminal_mark != index[-1]:
                raise ValueError("terminal_mark_without_decision must identify the final panel row")
            # The terminal row is a mark, not a decision. Its repeated target is
            # merely an alignment placeholder and must not count as a universe trade.
            validation_target = target.copy()
            validation_target.loc[terminal_mark] = 0.0
        self._validate_point_in_time(panel, validation_target)

        columns = panel.prices.columns
        column_locations = {str(instrument): location for location, instrument in enumerate(columns)}
        prices = panel.prices.replace([np.inf, -np.inf], np.nan)
        funding = panel.funding.replace([np.inf, -np.inf], np.nan)
        spreads = panel.spreads_bps.replace([np.inf, -np.inf], np.nan)
        price_changes = prices.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan)

        actual = pd.DataFrame(0.0, index=index, columns=columns)
        component_frames = {
            name: pd.DataFrame(0.0, index=index, columns=columns) for name in PNL_COMPONENT_COLUMNS
        }
        due: dict[int, list[_PendingOrder]] = {}
        fill_rows: list[dict[str, Any]] = []
        order_counter = 0
        rng = np.random.default_rng(self.execution.seed)
        group_by_instrument: dict[str, tuple[str, int]] = {}
        for group_id, instruments in output.hedge_groups.items():
            for leg_number, instrument in enumerate(instruments):
                group_by_instrument[instrument] = (group_id, leg_number)

        equity_values: list[float] = []
        previous_equity = 1.0
        previous_actual = pd.Series(0.0, index=columns)
        previous_desired = pd.Series(0.0, index=columns)
        row_buckets: dict[tuple[int, str], str] = {}
        row_sizes: dict[tuple[int, str], float] = {}

        def depth_at(row: int, instrument: str) -> float:
            if panel.depth_usd is None:
                if self.execution.require_depth:
                    raise ValueError(f"executable depth is required for {instrument}")
                return math.inf
            value = panel.depth_usd.at[index[row], instrument]
            if pd.isna(value):
                if self.execution.require_depth:
                    raise ValueError(f"executable depth is missing for {instrument} at {index[row]}")
                return math.inf
            depth = _as_float(value)
            if not math.isfinite(depth) or depth <= 0.0:
                raise ValueError(f"executable depth must be positive for {instrument}")
            return depth

        def add_fill_row(
            order: _PendingOrder,
            row: int,
            *,
            status: str,
            filled_weight: float,
            probability: float,
            draw: float | None,
            estimate: SlippageEstimate | None,
            execution_equity: float,
        ) -> None:
            venue, asset, _kind = parse_instrument(order.instrument)
            participation = estimate.participation if estimate is not None else 0.0
            recorded_depth = depth_at(row, order.instrument) if estimate is not None else math.nan
            fill_rows.append(
                {
                    "order_id": order.order_id,
                    "emergency_for": order.emergency_for,
                    "decision_time": index[order.decision_index],
                    "scheduled_time": index[min(order.due_index, len(index) - 1)],
                    "timestamp": index[row],
                    "instrument": order.instrument,
                    "venue": venue,
                    "asset": asset,
                    "group_id": order.group_id,
                    "leg_number": order.leg_number,
                    "order_type": order.order_type,
                    "status": status,
                    "requested_weight": order.requested_weight,
                    "filled_weight": filled_weight,
                    "requested_notional_usd": abs(order.requested_weight)
                    * self.execution.initial_capital
                    * abs(execution_equity),
                    "filled_notional_usd": abs(filled_weight)
                    * self.execution.initial_capital
                    * abs(execution_equity),
                    "fill_probability": probability,
                    "random_draw": draw,
                    "participation": participation,
                    "depth_usd": recorded_depth,
                    "capacity_usd": estimate.capacity_usd if estimate is not None else math.nan,
                    "size_bucket": _size_bucket(
                        participation,
                        missed=filled_weight == 0.0,
                    ),
                }
            )

        def hedge_gap_exists(order: _PendingOrder, positions: pd.Series) -> bool:
            if order.group_id is None:
                return False
            return any(
                other != order.instrument and abs(_as_float(positions[other])) > _TOLERANCE
                for other in output.hedge_groups[order.group_id]
            )

        def risk_reducing_request(
            order: _PendingOrder,
            positions: pd.Series,
        ) -> float | None:
            """Return a safe IOC request, never allowing an ungrouped reversal."""

            requested = order.requested_weight
            current = _as_float(positions[order.instrument])
            if abs(current) > _TOLERANCE and current * requested < 0.0:
                capped = math.copysign(min(abs(requested), abs(current)), requested)
                if abs(current + capped) < abs(current) - _TOLERANCE:
                    return capped
            if hedge_gap_exists(order, positions):
                return requested
            return None

        def schedule_emergency(
            order: _PendingOrder,
            row: int,
            residual: float,
            positions: pd.Series,
            execution_equity: float,
        ) -> None:
            nonlocal order_counter
            if not self.execution.emergency_ioc or abs(residual) <= _TOLERANCE:
                return
            candidate = _PendingOrder(
                order_id="pending-emergency-check",
                decision_index=order.decision_index,
                due_index=row,
                instrument=order.instrument,
                requested_weight=residual,
                order_type="ioc",
                group_id=order.group_id,
                leg_number=order.leg_number,
                emergency_for=order.order_id,
            )
            safe_request = risk_reducing_request(candidate, positions)
            # A missing leg can become risk-increasing only after a sibling leg
            # fills. Schedule the hedge candidate now, then revalidate at IOC time.
            if safe_request is None and order.group_id is not None:
                safe_request = residual
            if safe_request is None:
                return
            order_counter += 1
            due_index = row + self.execution.maker_timeout_bars
            emergency = _PendingOrder(
                order_id=f"order-{order_counter:08d}",
                decision_index=order.decision_index,
                due_index=due_index,
                instrument=order.instrument,
                requested_weight=safe_request,
                order_type="ioc",
                group_id=order.group_id,
                leg_number=order.leg_number,
                emergency_for=order.order_id,
            )
            if due_index >= len(index):
                add_fill_row(
                    emergency,
                    row,
                    status="EXPIRED",
                    filled_weight=0.0,
                    probability=0.0,
                    draw=None,
                    estimate=None,
                    execution_equity=execution_equity,
                )
            else:
                due.setdefault(due_index, []).append(emergency)

        def execute(
            order: _PendingOrder,
            row: int,
            positions: pd.Series,
            *,
            opening_equity: float,
            execution_equity: float,
        ) -> None:
            instrument = order.instrument
            timestamp = index[row]
            if panel.available_at is not None:
                available = pd.Timestamp(cast(Any, panel.available_at.at[timestamp, instrument]))
                if available.tz is None or available.tz_convert("UTC") > timestamp:
                    raise ValueError(f"{instrument} is not available at execution time {timestamp}")
            if panel.finality is not None:
                final_value = panel.finality.at[timestamp, instrument]
                if pd.isna(final_value) or not bool(final_value):
                    raise ValueError("non-final candle data cannot be used for simulated execution")

            if order.order_type == "ioc":
                safe_request = risk_reducing_request(order, positions)
                if safe_request is None:
                    add_fill_row(
                        order,
                        row,
                        status="IOC_CANCELLED_NOT_RISK_REDUCING",
                        filled_weight=0.0,
                        probability=0.0,
                        draw=None,
                        estimate=None,
                        execution_equity=execution_equity,
                    )
                    return
                order.requested_weight = safe_request

            tradable_value = True if panel.tradable is None else panel.tradable.at[timestamp, instrument]
            tradable_now = not pd.isna(tradable_value) and bool(tradable_value)
            if not tradable_now:
                current = _as_float(positions[instrument])
                requested = order.requested_weight
                if (
                    abs(current) <= _TOLERANCE
                    or current * requested >= 0.0
                    or abs(current + requested) >= abs(current) - _TOLERANCE
                ):
                    add_fill_row(
                        order,
                        row,
                        status="CANCELLED_UNTRADABLE",
                        filled_weight=0.0,
                        probability=0.0,
                        draw=None,
                        estimate=None,
                        execution_equity=execution_equity,
                    )
                    return
                order.requested_weight = math.copysign(
                    min(abs(requested), abs(current)),
                    requested,
                )

            market_price = prices.at[timestamp, instrument]
            if pd.isna(market_price) or not math.isfinite(_as_float(market_price)):
                raise ValueError("price data is missing or non-finite for an active position")
            if _as_float(market_price) <= 0.0:
                raise ValueError("price data must be positive for an active or traded position")
            spread_value = spreads.at[timestamp, instrument]
            if pd.isna(spread_value) or not math.isfinite(_as_float(spread_value)):
                raise ValueError("spread data is missing or non-finite for an active position")
            if _as_float(spread_value) < 0.0:
                raise ValueError("spread data must be non-negative for a traded position")

            rule = self._cost_rule(timestamp, instrument)
            requested_notional = (
                abs(order.requested_weight) * self.execution.initial_capital * abs(execution_equity)
            )
            estimate = self._slippage_estimate(
                rule=rule,
                notional_usd=requested_notional,
                depth_usd=depth_at(row, instrument),
            )
            probability = 1.0
            draw: float | None = None
            eligible = True
            if order.order_type == "maker":
                probability = self.execution.maker_fill.probability(
                    participation=estimate.participation,
                    multiplier=self.execution.maker_fill_multiplier,
                )
                if probability <= 0.0:
                    eligible = False
                elif probability < 1.0:
                    draw = float(rng.random())
                    eligible = draw < probability
            elif order.order_type == "ioc":
                probability = self.execution.ioc_fill_probability
                if probability <= 0.0:
                    eligible = False
                elif probability < 1.0:
                    draw = float(rng.random())
                    eligible = draw < probability

            fill_fraction = estimate.fill_fraction if eligible else 0.0
            filled_weight = order.requested_weight * fill_fraction
            if order.order_type == "ioc":
                if fill_fraction == 0.0:
                    status = "IOC_NO_FILL"
                elif fill_fraction < 1.0:
                    status = "IOC_PARTIAL"
                else:
                    status = "IOC_FILLED"
            elif fill_fraction == 0.0:
                status = "NO_FILL"
            elif fill_fraction < 1.0:
                status = "PARTIAL"
            else:
                status = "FILLED"

            if abs(filled_weight) > _TOLERANCE:
                positions[instrument] += filled_weight
                return_scale = execution_equity / opening_equity
                maker = order.order_type == "maker"
                half_spread = _as_float(spread_value) * 0.5 / 10_000.0
                spread_sign = 1.0 if maker else -1.0
                spread_location = column_locations[instrument]
                component_frames["spread_return"].iat[row, spread_location] = _as_float(
                    component_frames["spread_return"].iat[row, spread_location]
                ) + (
                    abs(filled_weight)
                    * half_spread
                    * spread_sign
                    * self._spread_multiplier(maker=maker)
                    * return_scale
                )
                raw_fee = rule.maker_fee_bps if maker else rule.taker_fee_bps
                fee_bps = adverse_fee_bps(raw_fee, self.execution.cost_multiplier)
                component_frames["fee_return"].iat[row, spread_location] = _as_float(
                    component_frames["fee_return"].iat[row, spread_location]
                ) + (-abs(filled_weight) * fee_bps / 10_000.0 * return_scale)
                slippage_bps = estimate.slippage_bps * self.execution.cost_multiplier
                if order.order_type == "ioc":
                    slippage_bps += self.execution.ioc_extra_slippage_bps * self.execution.cost_multiplier
                component_frames["slippage_return"].iat[row, spread_location] = _as_float(
                    component_frames["slippage_return"].iat[row, spread_location]
                ) + (-abs(filled_weight) * slippage_bps / 10_000.0 * return_scale)
                row_buckets[(row, instrument)] = _size_bucket(estimate.participation)
                row_sizes[(row, instrument)] = row_sizes.get((row, instrument), 0.0) + (
                    abs(filled_weight) * self.execution.initial_capital * abs(execution_equity)
                )
            else:
                row_buckets[(row, instrument)] = "missed"

            add_fill_row(
                order,
                row,
                status=status,
                filled_weight=filled_weight,
                probability=probability,
                draw=draw,
                estimate=estimate,
                execution_equity=execution_equity,
            )
            residual = order.requested_weight - filled_weight
            if order.order_type == "maker":
                schedule_emergency(
                    order,
                    row,
                    residual,
                    positions,
                    execution_equity,
                )

        def process_due(
            row: int,
            positions: pd.Series,
            *,
            opening_equity: float,
            execution_equity: float,
        ) -> None:
            while due.get(row):
                batch = due.pop(row)
                for order in batch:
                    execute(
                        order,
                        row,
                        positions,
                        opening_equity=opening_equity,
                        execution_equity=execution_equity,
                    )

        for row, timestamp in enumerate(index):
            # First settle the interval t-1 -> t on positions established at t-1.
            held = previous_actual
            active_held = held.abs()
            _require_active_data(
                price_changes.iloc[[row]],
                active_held.to_frame().T.set_axis([timestamp]),
                "price",
            )
            _require_active_data(
                funding.iloc[[row]], active_held.to_frame().T.set_axis([timestamp]), "funding"
            )
            invalid_active_price = prices.iloc[row].le(0.0) & active_held.gt(_TOLERANCE)
            if bool(invalid_active_price.any()):
                raise ValueError("price data must be positive for an active position")

            raw_price = held * price_changes.iloc[row].fillna(0.0)
            raw_funding = -held * funding.iloc[row].fillna(0.0)
            component_frames["funding_return"].iloc[row] = raw_funding

            classified: set[str] = set()

            def allocate_hedged_group(
                instruments: list[str],
                *,
                held_positions: pd.Series = held,
                price_pnl: pd.Series = raw_price,
                classified_instruments: set[str] = classified,
                row_number: int = row,
            ) -> None:
                active_group = [
                    instrument
                    for instrument in instruments
                    if abs(_as_float(held_positions[instrument])) > _TOLERANCE
                ]
                classified_instruments.update(instruments)
                long_gross = sum(
                    max(_as_float(held_positions[instrument]), 0.0) for instrument in active_group
                )
                short_gross = sum(
                    max(-_as_float(held_positions[instrument]), 0.0) for instrument in active_group
                )
                matched_gross = min(long_gross, short_gross)
                for instrument in active_group:
                    location = column_locations[instrument]
                    value = _as_float(price_pnl[instrument])
                    side_gross = long_gross if _as_float(held_positions[instrument]) > 0.0 else short_gross
                    basis_fraction = matched_gross / side_gross if side_gross > _TOLERANCE else 0.0
                    component_frames["basis_return"].iat[row_number, location] = (
                        _as_float(component_frames["basis_return"].iat[row_number, location])
                        + value * basis_fraction
                    )
                    component_frames["hedge_return"].iat[row_number, location] = _as_float(
                        component_frames["hedge_return"].iat[row_number, location]
                    ) + value * (1.0 - basis_fraction)

            for instruments in output.hedge_groups.values():
                allocate_hedged_group(list(instruments))

            for _asset, asset_columns in _columns_by_asset(columns).items():
                available = [instrument for instrument in asset_columns if instrument not in classified]
                signs = {
                    math.copysign(1.0, _as_float(held[instrument]))
                    for instrument in available
                    if abs(_as_float(held[instrument])) > _TOLERANCE
                }
                if len(signs) > 1:
                    allocate_hedged_group(available)

            for instrument in columns:
                instrument_name = str(instrument)
                if instrument_name in classified:
                    continue
                component_frames["price_return"].iat[row, column_locations[instrument_name]] = _as_float(
                    raw_price[instrument]
                )

            terminal_mark_only = terminal_mark is not None and timestamp == terminal_mark
            if terminal_mark_only:
                # The unseen terminal close settles existing exposure only. No
                # delayed/new order may execute here, and therefore no execution
                # cost or maker-spread credit can appear without a later horizon.
                for pending_orders in due.values():
                    for pending in pending_orders:
                        add_fill_row(
                            pending,
                            row,
                            status="EXPIRED",
                            filled_weight=0.0,
                            probability=0.0,
                            draw=None,
                            estimate=None,
                            execution_equity=previous_equity,
                        )
                due.clear()

            market_return = sum(float(frame.iloc[row].sum()) for frame in component_frames.values())
            marked_equity = previous_equity * (1.0 + market_return)
            if not math.isfinite(marked_equity):
                raise RuntimeError("portfolio equity became non-finite during mark-to-market")
            if abs(marked_equity) <= _TOLERANCE:
                raise RuntimeError("portfolio equity reached zero; position weights are undefined")

            # Position weights drift with their marked notionals; no implicit free
            # rebalance takes place while a target state is unchanged. Signed equity
            # deliberately permits losses below -100% without clipping.
            price_multipliers = 1.0 + price_changes.iloc[row].fillna(0.0)
            positions = held * price_multipliers * previous_equity / marked_equity

            if terminal_mark_only:
                equity_values.append(marked_equity)
                actual.iloc[row] = positions
                previous_equity = marked_equity
                previous_actual = positions
                previous_desired = target.iloc[row]
                continue

            desired = target.iloc[row]
            desired_changed = desired.sub(previous_desired).abs().gt(_TOLERANCE)
            exposure_moved = bool(
                (
                    held.abs().gt(_TOLERANCE)
                    & (
                        price_changes.iloc[row].fillna(0.0).abs().gt(_TOLERANCE)
                        | funding.iloc[row].fillna(0.0).abs().gt(_TOLERANCE)
                    )
                ).any()
            )
            risk_rebalance = exposure_moved and (
                positions.abs().sum() > self.risk_limits.max_gross_leverage + _TOLERANCE
                or abs(float(positions.sum())) > self.risk_limits.max_net_exposure + _TOLERANCE
                or bool(positions.abs().gt(self.risk_limits.max_instrument_weight + _TOLERANCE).any())
            )
            # A missed or partial close must remain an outstanding objective. Without
            # this reconciliation, a target transition to zero is attempted only once
            # and any execution residual can drift for the rest of the backtest.
            pending_instruments = {
                pending.instrument
                for pending_orders in due.values()
                for pending in pending_orders
            }
            closing_gap = desired.abs().le(_TOLERANCE) & positions.abs().gt(_TOLERANCE)
            closing_gap &= ~closing_gap.index.to_series().isin(pending_instruments).to_numpy()
            changed = (
                desired_changed
                | pd.Series(bool(risk_rebalance), index=columns)
                | closing_gap
            )
            for instrument in columns[changed.to_numpy()]:
                # The t decision is known before orders due at t execute. It cancels
                # every obsolete latent order for the same leg, including due-now.
                if bool(desired_changed[instrument]):
                    for future_row in tuple(due):
                        retained: list[_PendingOrder] = []
                        for pending in due[future_row]:
                            if pending.instrument != instrument:
                                retained.append(pending)
                                continue
                            add_fill_row(
                                pending,
                                row,
                                status="CANCELLED",
                                filled_weight=0.0,
                                probability=0.0,
                                draw=None,
                                estimate=None,
                                execution_equity=marked_equity,
                            )
                        if retained:
                            due[future_row] = retained
                        else:
                            due.pop(future_row, None)
                elif any(
                    pending.instrument == instrument
                    for pending_orders in due.values()
                    for pending in pending_orders
                ):
                    # Preserve an already scheduled risk reduction. Replacing it on
                    # every breached bar would starve all non-zero-latency orders.
                    continue

                delta = _as_float(desired[instrument]) - _as_float(positions[instrument])
                if abs(delta) <= _TOLERANCE:
                    continue
                order_counter += 1
                pending_group_id: str | None = None
                leg_number = 0
                if instrument in group_by_instrument:
                    pending_group_id, leg_number = group_by_instrument[instrument]
                due_index = (
                    row + self.execution.base_latency_bars + leg_number * self.execution.leg_delay_bars
                )
                pending = _PendingOrder(
                    order_id=f"order-{order_counter:08d}",
                    decision_index=row,
                    due_index=due_index,
                    instrument=instrument,
                    requested_weight=delta,
                    order_type=str(order_types.at[timestamp, instrument]),
                    group_id=pending_group_id,
                    leg_number=leg_number,
                )
                if due_index >= len(index):
                    add_fill_row(
                        pending,
                        row,
                        status="EXPIRED",
                        filled_weight=0.0,
                        probability=0.0,
                        draw=None,
                        estimate=None,
                        execution_equity=marked_equity,
                    )
                else:
                    due.setdefault(due_index, []).append(pending)

            process_due(
                row,
                positions,
                opening_equity=previous_equity,
                execution_equity=marked_equity,
            )

            row_return = sum(float(frame.iloc[row].sum()) for frame in component_frames.values())
            ending_equity = previous_equity * (1.0 + row_return)
            if not math.isfinite(ending_equity):
                raise RuntimeError("portfolio equity became non-finite after execution costs")
            if abs(ending_equity) <= _TOLERANCE:
                raise RuntimeError("portfolio equity reached zero after execution costs")
            # Fills are expressed as fractions of marked equity. Execution cash flows
            # alter equity, so rescale the filled notionals onto end-of-bar equity.
            positions *= marked_equity / ending_equity
            equity_values.append(ending_equity)
            actual.iloc[row] = positions
            previous_equity = ending_equity
            previous_actual = positions
            previous_desired = desired

        portfolio_components = pd.DataFrame(
            {name: frame.sum(axis=1) for name, frame in component_frames.items()},
            index=index,
        )
        portfolio_components["cost_return"] = portfolio_components[
            ["spread_return", "fee_return", "slippage_return"]
        ].sum(axis=1)
        portfolio_components["net_return"] = portfolio_components[list(PNL_COMPONENT_COLUMNS)].sum(axis=1)
        equity = pd.Series(equity_values, index=index, name=output.name)
        benchmark = build_passive_benchmark(panel, self.benchmark_spec)
        fills = pd.DataFrame(fill_rows)
        if panel.regimes is None:
            aggregate_market_return = price_changes.mean(axis=1, skipna=True).fillna(0.0)
            regime_labels = causal_regimes(aggregate_market_return, lookback=24)
        else:
            regime_labels = panel.regimes.astype("string")
        attribution = _build_attribution(
            panel=panel,
            component_frames=component_frames,
            equity=equity,
            positions=actual,
            row_buckets=row_buckets,
            row_sizes=row_sizes,
            regimes=regime_labels,
            initial_capital=self.execution.initial_capital,
        )
        breakdowns = _attribution_breakdowns(attribution)
        metrics = compute_metrics(
            portfolio_components,
            equity,
            actual,
            benchmark=benchmark,
            fills=fills,
        )
        metrics.turnover = (
            float(fills["filled_weight"].abs().sum()) if not fills.empty and "filled_weight" in fills else 0.0
        )

        cost_status = (
            self.costs.calibration_status if isinstance(self.costs, CostSchedule) else "UNCALIBRATED"
        )
        statuses = {
            _data_status(panel),
            cost_status,
            self.execution.maker_fill.calibration_status,
        }
        if "SYNTHETIC" in statuses:
            audit_status = "SYNTHETIC"
        elif statuses == {"CALIBRATED"}:
            audit_status = "CALIBRATED"
        else:
            audit_status = "UNCALIBRATED"
        warnings: list[str] = []
        if audit_status != "CALIBRATED":
            warnings.append(
                "Execution and/or data assumptions are not calibrated; this run is not evidence of live performance."
            )
        if panel.depth_usd is None:
            warnings.append("No executable depth supplied; legacy runs assume unlimited capacity.")

        target_active = target.abs().sum(axis=1).gt(_TOLERANCE)
        position_active = actual.abs().sum(axis=1).gt(_TOLERANCE)
        target_entry_signals = int(
            (target_active & ~target_active.shift(1, fill_value=False)).sum()
        )
        target_exit_signals = int(
            (~target_active & target_active.shift(1, fill_value=False)).sum()
        )
        position_entries = int(
            (position_active & ~position_active.shift(1, fill_value=False)).sum()
        )
        position_exits = int(
            (~position_active & position_active.shift(1, fill_value=False)).sum()
        )

        diagnostics = {
            **output.diagnostics,
            "audit_status": audit_status,
            "data_status": _data_status(panel),
            "cost_calibration_status": cost_status,
            "cost_calibration_evidence_hash": (
                self.costs.calibration_evidence_hash if isinstance(self.costs, CostSchedule) else None
            ),
            "maker_calibration_status": self.execution.maker_fill.calibration_status,
            "maker_calibration_id": self.execution.maker_fill.calibration_id,
            "maker_calibration_evidence_hash": (self.execution.maker_fill.calibration_evidence_hash),
            "data_calibration_evidence_hash": panel.metadata.get("calibration_evidence_hash"),
            "execution_seed": self.execution.seed,
            "initial_capital": self.execution.initial_capital,
            "target_entry_signals": target_entry_signals,
            "target_exit_signals": target_exit_signals,
            "target_active_bars": int(target_active.sum()),
            "position_entries": position_entries,
            "position_exits": position_exits,
            "position_active_bars": int(position_active.sum()),
            "hedge_groups": {
                group_id: list(instruments) for group_id, instruments in output.hedge_groups.items()
            },
            "orders": len(fills),
            "missed_orders": int(fills["status"].isin(["NO_FILL", "IOC_NO_FILL", "EXPIRED"]).sum())
            if not fills.empty
            else 0,
            "partial_fills": int(fills["status"].isin(["PARTIAL", "IOC_PARTIAL"]).sum())
            if not fills.empty
            else 0,
            "emergency_ioc_attempts": int(fills["order_type"].eq("ioc").sum()) if not fills.empty else 0,
            "warnings": warnings,
        }
        return BacktestResult(
            strategy_name=output.name,
            risk_tier=output.risk_tier,
            returns=portfolio_components,
            equity=equity,
            weights=actual,
            metrics=metrics,
            diagnostics=diagnostics,
            target_weights=target,
            fills=fills,
            attribution=attribution,
            benchmark=benchmark,
            breakdowns=breakdowns,
        )


def _columns_by_asset(columns: pd.Index) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for instrument in columns:
        _venue, asset, _kind = parse_instrument(str(instrument))
        grouped.setdefault(asset, []).append(str(instrument))
    return grouped


def _build_attribution(
    *,
    panel: MarketPanel,
    component_frames: dict[str, pd.DataFrame],
    equity: pd.Series,
    positions: pd.DataFrame,
    row_buckets: dict[tuple[int, str], str],
    row_sizes: dict[tuple[int, str], float],
    regimes: pd.Series,
    initial_capital: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    start_equity = equity.shift(1, fill_value=1.0)
    for row, timestamp in enumerate(panel.prices.index):
        regime = str(regimes.iloc[row]) if not pd.isna(regimes.iloc[row]) else "unclassified"
        capital = float(start_equity.iloc[row]) * initial_capital
        held_positions = positions.shift(1, fill_value=0.0).iloc[row]
        for instrument in panel.prices.columns:
            venue, asset, kind = parse_instrument(str(instrument))
            record: dict[str, Any] = {
                "timestamp": timestamp,
                "instrument": instrument,
                "venue": venue,
                "asset": asset,
                "kind": kind,
                "regime": regime,
                "size_bucket": row_buckets.get((row, str(instrument)), "no_trade"),
                "size_usd": max(
                    row_sizes.get((row, str(instrument)), 0.0),
                    abs(_as_float(held_positions[instrument])) * abs(capital),
                ),
            }
            net_pnl = 0.0
            for component, frame in component_frames.items():
                name = component.removesuffix("_return") + "_pnl"
                value = _as_float(frame.at[timestamp, instrument]) * capital
                record[name] = value
                net_pnl += value
            record["net_pnl"] = net_pnl
            rows.append(record)
    attribution = pd.DataFrame(rows)
    expected = (float(equity.iloc[-1]) - 1.0) * initial_capital
    actual = float(attribution["net_pnl"].sum())
    if not math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-8):
        raise RuntimeError(f"PnL attribution failed to reconcile: ledger={actual} equity={expected}")
    return attribution


def _attribution_breakdowns(attribution: pd.DataFrame) -> dict[str, pd.DataFrame]:
    pnl_columns = [component.removesuffix("_return") + "_pnl" for component in PNL_COMPONENT_COLUMNS]
    long = attribution.melt(
        id_vars=["timestamp", "asset", "regime", "size_usd"],
        value_vars=pnl_columns,
        var_name="component",
        value_name="pnl",
    )
    long["component"] = long["component"].str.removesuffix("_pnl")
    summaries = aggregate_pnl(long)
    return {
        "asset": summaries.by_asset,
        "month": summaries.by_month,
        "regime": summaries.by_regime,
        "size": summaries.by_size_bucket,
    }
