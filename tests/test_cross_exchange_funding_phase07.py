from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

from hyperlab.backtest.cross_exchange import (
    CrossVenueConfig,
    CrossVenueMarketData,
    FundingCalendar,
    FundingConvention,
    VenueRiskRule,
    audit_cross_venue_data,
    default_funding_conventions,
    hyperliquid_hourly_rate_from_premium,
    run_cross_exchange_validation,
    simulate_cross_exchange_funding,
    write_cross_exchange_report,
)
from hyperlab.data.io import load_cross_venue_csv, save_cross_venue_csv

HL = "HL"
BINANCE = "BINANCE_USDM"
VENUES = (HL, BINANCE)


def _rules(
    *,
    initial_margin_fraction: float = 0.20,
    maintenance_margin_fraction: float = 0.10,
    fee_bps: float = 1.0,
    slippage_bps: float = 1.0,
) -> dict[str, VenueRiskRule]:
    return {
        venue: VenueRiskRule(
            venue=venue,
            initial_margin_fraction=initial_margin_fraction,
            maintenance_margin_fraction=maintenance_margin_fraction,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            liquidation_penalty_bps=20.0,
            calibration_evidence_hash=("a" if venue == HL else "b") * 64,
        )
        for venue in VENUES
    }


def _metadata(*, calibrated: bool = True) -> dict[str, object]:
    if not calibrated:
        return {
            "source": "synthetic-phase07-fixture",
            "point_in_time": True,
            "calibration_status": "SYNTHETIC",
            "warning": "Synthetic data for deterministic tests only.",
        }
    return {
        "source": "versioned-real-cross-venue-fixture",
        "point_in_time": True,
        "calibration_status": "CALIBRATED",
        "calibration_evidence_hash": "c" * 64,
        "funding_convention_evidence_hashes": {HL: "d" * 64, BINANCE: "e" * 64},
        "transfer_policy_evidence_hash": "f" * 64,
        "parameters_frozen_before_period": True,
        "evaluation_split": "final_test",
        "venue_identity": {
            HL: "BTC linear perp, USDC collateral",
            BINANCE: "BTCUSDT USD-M linear perp, USDT collateral",
        },
    }


def _market(
    *,
    hours: int = 72,
    start: str = "2025-01-01",
    hl_marks: np.ndarray | None = None,
    binance_marks: np.ndarray | None = None,
    hl_rate: float = -0.00010,
    binance_rate: float = 0.00080,
    calibrated: bool = True,
) -> CrossVenueMarketData:
    index = pd.date_range(start, periods=hours, freq="1h", tz="UTC")
    hl_values = np.full(hours, 100.0) if hl_marks is None else hl_marks.astype(float)
    binance_values = (
        np.full(hours, 100.5) if binance_marks is None else binance_marks.astype(float)
    )
    marks = pd.DataFrame({HL: hl_values, BINANCE: binance_values}, index=index)
    oracles = pd.DataFrame(
        {
            HL: hl_values * 0.99,
            BINANCE: binance_values * 0.995,
        },
        index=index,
    )
    rates = pd.DataFrame(np.nan, index=index, columns=list(VENUES))
    rates[HL] = hl_rate
    rates.loc[index.hour.isin([0, 8, 16]), BINANCE] = binance_rate
    return CrossVenueMarketData(
        asset="BTC",
        mark_prices=marks,
        oracle_prices=oracles,
        funding_rates=rates,
        metadata=_metadata(calibrated=calibrated),
    )


def _config(**overrides: object) -> CrossVenueConfig:
    values: dict[str, object] = {
        "initial_capital_by_venue": {HL: 10_000.0, BINANCE: 10_000.0},
        "target_notional_usd": 20_000.0,
        "lookback_hours": 1,
        "min_funding_edge_hourly": 0.0,
        "position_rebalance_hours": 24,
        "collateral_rebalance_trigger_fraction": 0.30,
        "collateral_rebalance_target_fraction": 0.50,
        "transfer_delay_hours": 1,
        "transfer_fee_bps": 5.0,
        "transfer_fixed_fee_usd": 1.0,
    }
    values.update(overrides)
    return CrossVenueConfig(**values)  # type: ignore[arg-type]


def test_venue_funding_calendars_and_formulas_are_explicit() -> None:
    conventions = default_funding_conventions()
    timestamp = pd.Timestamp("2025-01-01T08:00:00Z")

    assert conventions[HL].calendar.settles_at(timestamp)
    assert conventions[BINANCE].calendar.settles_at(timestamp)
    assert not conventions[BINANCE].calendar.settles_at(timestamp + pd.Timedelta(hours=1))
    assert conventions[HL].notional_price_source == "oracle"
    assert conventions[BINANCE].notional_price_source == "mark"
    assert hyperliquid_hourly_rate_from_premium(0.001) == 0.0000625

    dynamic = FundingCalendar(
        interval_hours=None,
        explicit_settlements=(timestamp, timestamp + pd.Timedelta(hours=4)),
    )
    assert dynamic.settles_at(timestamp + pd.Timedelta(hours=4))
    assert not dynamic.settles_at(timestamp + pd.Timedelta(hours=8))


def test_funding_uses_each_venue_price_source_and_positions_open_only_after_signal() -> None:
    market = _market(hours=10)
    conventions = default_funding_conventions()
    result = simulate_cross_exchange_funding(
        market,
        conventions=conventions,
        risk_rules=_rules(fee_bps=0.0, slippage_bps=0.0),
        config=_config(target_notional_usd=10_000.0, enable_collateral_rebalancing=False),
    )

    first = result.timeline.iloc[0]
    assert first[f"{HL}_quantity"] > 0.0
    assert first[f"{BINANCE}_quantity"] < 0.0
    # No position existed before the first observation, so t=0 funding is not earned.
    assert first[f"{HL}_funding_pnl"] == 0.0
    assert first[f"{BINANCE}_funding_pnl"] == 0.0

    quantity = float(first[f"{HL}_quantity"])
    expected_hl = -quantity * float(market.oracle_prices.iloc[1][HL]) * -0.00010
    assert result.timeline.iloc[1][f"{HL}_funding_pnl"] == expected_hl
    binance_quantity = float(first[f"{BINANCE}_quantity"])
    expected_binance = -binance_quantity * float(market.mark_prices.iloc[8][BINANCE]) * 0.00080
    assert result.timeline.iloc[8][f"{BINANCE}_funding_pnl"] == expected_binance
    assert np.isclose(
        result.metrics.gross_return_on_total_capital,
        result.metrics.return_on_total_capital,
    )
    assert result.metrics.turnover > 0.0
    assert result.metrics.max_gross_exposure > 0.0
    assert result.metrics.max_net_exposure < 0.01


def test_phase07_decisions_are_causal_when_future_funding_is_rewritten() -> None:
    market = _market(hours=48)
    cutoff = market.mark_prices.index[20]
    changed_rates = market.funding_rates.copy()
    changed_rates.loc[changed_rates.index > cutoff, HL] = 0.05
    changed_rates.loc[changed_rates.index > cutoff, BINANCE] = -0.05
    changed = replace(market, funding_rates=changed_rates)
    kwargs = {
        "conventions": default_funding_conventions(),
        "risk_rules": _rules(fee_bps=0.0, slippage_bps=0.0),
        "config": _config(enable_collateral_rebalancing=False),
    }

    base = simulate_cross_exchange_funding(market, **kwargs)
    counterfactual = simulate_cross_exchange_funding(changed, **kwargs)

    pd.testing.assert_frame_equal(base.timeline.loc[:cutoff], counterfactual.timeline.loc[:cutoff])
    pd.testing.assert_frame_equal(base.trades.loc[base.trades["timestamp"] <= cutoff], counterfactual.trades.loc[counterfactual.trades["timestamp"] <= cutoff])


def test_margin_is_local_and_one_venue_liquidates_while_total_capital_is_positive() -> None:
    hours = 12
    rising = np.concatenate((np.full(3, 100.0), np.linspace(100.0, 130.0, hours - 3)))
    market = _market(hours=hours, hl_marks=rising, binance_marks=rising)
    result = simulate_cross_exchange_funding(
        market,
        conventions=default_funding_conventions(),
        risk_rules=_rules(
            initial_margin_fraction=0.20,
            maintenance_margin_fraction=0.10,
            fee_bps=0.0,
            slippage_bps=0.0,
        ),
        config=_config(
            target_notional_usd=50_000.0,
            enable_collateral_rebalancing=False,
        ),
    )

    assert len(result.liquidations) == 1
    assert result.liquidations.iloc[0]["venue"] == BINANCE
    assert result.metrics.worst_local_margin_deficit_usd[BINANCE] > 0.0
    liquidation_time = pd.Timestamp(result.liquidations.iloc[0]["timestamp"])
    total_equity = float(result.timeline.loc[liquidation_time, "total_equity"])
    assert total_equity > 0.0
    assert result.metrics.liquidation_count_by_venue == {HL: 0, BINANCE: 1}


def test_collateral_rebalancing_has_a_cost_and_crisis_can_block_transfers() -> None:
    hours = 24
    rising = np.linspace(100.0, 108.0, hours)
    market = _market(hours=hours, hl_marks=rising, binance_marks=rising)
    rules = _rules(
        initial_margin_fraction=0.15,
        maintenance_margin_fraction=0.05,
        fee_bps=0.0,
        slippage_bps=0.0,
    )
    config = _config(
        target_notional_usd=30_000.0,
        collateral_rebalance_trigger_fraction=0.15,
        collateral_rebalance_target_fraction=0.20,
    )

    available = simulate_cross_exchange_funding(
        market,
        conventions=default_funding_conventions(),
        risk_rules=rules,
        config=config,
    )
    locked_market = replace(
        market,
        transfers_available=pd.Series(False, index=market.mark_prices.index),
    )
    locked = simulate_cross_exchange_funding(
        locked_market,
        conventions=default_funding_conventions(),
        risk_rules=rules,
        config=config,
    )

    assert not available.transfers.empty
    assert available.metrics.rebalancing_cost_usd > 0.0
    assert available.metrics.blocked_transfer_hours == 0.0
    assert locked.transfers.empty
    assert locked.metrics.blocked_transfer_hours > 0.0
    expected_net_pnl = (
        sum(available.metrics.price_pnl_by_venue.values())
        + sum(available.metrics.funding_pnl_by_venue.values())
        - sum(available.metrics.execution_cost_by_venue.values())
        - available.metrics.rebalancing_cost_usd
    )
    assert np.isclose(
        available.metrics.return_on_total_capital * 20_000.0,
        expected_net_pnl,
    )


def test_outage_matrix_reports_1h_6h_24h_and_uncovered_time(tmp_path: Path) -> None:
    hours = 80
    rising = np.concatenate((np.full(20, 100.0), np.linspace(100.0, 140.0, hours - 20)))
    market = _market(
        hours=hours,
        hl_marks=rising,
        binance_marks=rising,
        calibrated=False,
    )
    audit = audit_cross_venue_data(
        market,
        conventions=default_funding_conventions(),
        risk_rules=_rules(),
        minimum_history_hours=48,
    )
    validation = run_cross_exchange_validation(
        market,
        conventions=default_funding_conventions(),
        risk_rules=_rules(
            initial_margin_fraction=0.20,
            maintenance_margin_fraction=0.10,
            fee_bps=0.0,
            slippage_bps=0.0,
        ),
        config=_config(
            target_notional_usd=50_000.0,
            enable_collateral_rebalancing=False,
        ),
        failed_venue=HL,
        outage_start=market.mark_prices.index[20],
        audit=audit,
    )
    report = write_cross_exchange_report(validation, output_dir=tmp_path)

    assert set(validation.scenarios) == {"base", "outage_1h", "outage_6h", "outage_24h"}
    assert validation.scenarios["outage_24h"].metrics.uncovered_hours >= validation.scenarios[
        "outage_6h"
    ].metrics.uncovered_hours
    payload = json.loads(
        (tmp_path / "cross_exchange_funding_summary.json").read_text(encoding="utf-8")
    )
    for scenario in ("base", "outage_1h", "outage_6h", "outage_24h"):
        summary = payload["scenarios"][scenario]
        assert "return_on_total_capital" in summary
        assert "return_by_venue" in summary
        assert "worst_local_margin_deficit_usd" in summary
        assert "uncovered_hours" in summary
        assert "rebalancing_cost_usd" in summary
    html = report.read_text(encoding="utf-8")
    for label in (
        "Rendement sur capital total",
        "Rendement par venue",
        "Pire déficit de marge local",
        "Temps non couvert",
        "Frais de rééquilibrage",
        "Indisponibilité 24 h",
        "SYNTHETIC",
    ):
        assert label in html


def test_cross_venue_audit_fails_closed_then_accepts_complete_calibrated_fixture() -> None:
    synthetic = _market(hours=48, calibrated=False)
    real = _market(hours=48, calibrated=True)
    conventions = default_funding_conventions()
    rules = _rules()

    failed = audit_cross_venue_data(
        synthetic,
        conventions=conventions,
        risk_rules=rules,
        minimum_history_hours=48,
    )
    passed = audit_cross_venue_data(
        real,
        conventions=conventions,
        risk_rules=rules,
        minimum_history_hours=48,
    )

    assert not failed.passed
    assert not failed.checks["real_not_synthetic"]
    assert passed.passed
    assert passed.venues == VENUES


def test_custom_venue_convention_requires_observed_settlement_rate() -> None:
    convention = FundingConvention(
        venue="VENUE_X",
        calendar=FundingCalendar(interval_hours=4, anchor_hour_utc=1),
        notional_price_source="mark",
        formula_name="published_realized_rate",
        documentation_url="https://example.invalid/public-spec",
    )
    timestamp = pd.Timestamp("2025-01-01T05:00:00Z")

    assert convention.calendar.settles_at(timestamp)
    assert convention.settlement_rate(0.001) == 0.001


def test_cross_venue_export_round_trips_without_inferred_calendar(tmp_path: Path) -> None:
    market = _market(hours=48)
    conventions = default_funding_conventions()

    save_cross_venue_csv(market, tmp_path, conventions=conventions)
    restored = load_cross_venue_csv(tmp_path, conventions=conventions)

    pd.testing.assert_frame_equal(restored.mark_prices, market.mark_prices, check_freq=False)
    pd.testing.assert_frame_equal(restored.oracle_prices, market.oracle_prices, check_freq=False)
    pd.testing.assert_frame_equal(restored.funding_rates, market.funding_rates, check_freq=False)
    assert restored.metadata == market.metadata
