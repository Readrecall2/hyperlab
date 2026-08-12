from __future__ import annotations

import hashlib
import html
import json
import math
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, cast

import pandas as pd

from hyperlab.backtest.engine import PanelBacktester
from hyperlab.models import BacktestResult, MarketPanel
from hyperlab.strategies.momentum import (
    RobustMomentumStrategy,
    SignalVariant,
    classify_market_regimes,
)


@dataclass(frozen=True, slots=True)
class MomentumSelectionConfig:
    signal_variants: tuple[SignalVariant, ...] = ("time_series", "breakout", "combined")
    horizons: tuple[int, ...] = (24, 72, 168)
    breakout_lookback_bars: int = 30 * 24
    volatility_lookback_bars: int = 72
    regime_lookback_bars: int = 72
    regime_baseline_bars: int = 30 * 24
    correlation_lookback_bars: int = 168
    liquidation_lookback_bars: int = 168
    minimum_train_bars: int = 180 * 24
    minimum_validation_bars: int = 60 * 24

    def __post_init__(self) -> None:
        allowed = {"time_series", "breakout", "combined"}
        if not self.signal_variants or len(set(self.signal_variants)) != len(self.signal_variants):
            raise ValueError("signal_variants must be non-empty and unique")
        if not set(self.signal_variants).issubset(allowed):
            raise ValueError("signal_variants contains an unknown variant")
        if self.minimum_train_bars < 24 or self.minimum_validation_bars < 12:
            raise ValueError("train and validation samples are too short")
        RobustMomentumStrategy(
            signal_variant=self.signal_variants[0],
            horizons=self.horizons,
            breakout_lookback_bars=self.breakout_lookback_bars,
            volatility_lookback_bars=self.volatility_lookback_bars,
            regime_lookback_bars=self.regime_lookback_bars,
            regime_baseline_bars=self.regime_baseline_bars,
            correlation_lookback_bars=self.correlation_lookback_bars,
            liquidation_lookback_bars=self.liquidation_lookback_bars,
        )

    def strategy(self, variant: SignalVariant, *, trade_start: pd.Timestamp) -> RobustMomentumStrategy:
        return RobustMomentumStrategy(
            signal_variant=variant,
            horizons=self.horizons,
            breakout_lookback_bars=self.breakout_lookback_bars,
            volatility_lookback_bars=self.volatility_lookback_bars,
            regime_lookback_bars=self.regime_lookback_bars,
            regime_baseline_bars=self.regime_baseline_bars,
            correlation_lookback_bars=self.correlation_lookback_bars,
            liquidation_lookback_bars=self.liquidation_lookback_bars,
            trade_start=trade_start,
        )


@dataclass(frozen=True, slots=True)
class MomentumGateConfig:
    train_fraction: float = 0.60
    validation_fraction: float = 0.20
    minimum_non_bull_pnl: float = 0.0
    maximum_bull_profit_fraction: float = 0.80

    def __post_init__(self) -> None:
        if not 0.0 < self.train_fraction < 1.0 or not 0.0 < self.validation_fraction < 1.0:
            raise ValueError("split fractions must be in (0, 1)")
        if self.train_fraction + self.validation_fraction >= 1.0:
            raise ValueError("train and validation must leave a final test")
        if not math.isfinite(self.minimum_non_bull_pnl):
            raise ValueError("minimum_non_bull_pnl must be finite")
        if not 0.0 <= self.maximum_bull_profit_fraction <= 1.0:
            raise ValueError("maximum_bull_profit_fraction must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class MomentumDataAudit:
    observed_hours: int
    minimum_history_hours: int
    assets: tuple[str, ...]
    minimum_assets: int
    delisted_assets: tuple[str, ...]
    checks: dict[str, bool]
    reasons: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return all(self.checks.values())


@dataclass(frozen=True, slots=True)
class MomentumVariantResult:
    signal_variant: SignalVariant
    validation_score: float
    total_return: float
    max_drawdown: float
    turnover: float
    funding_return: float
    cost_return: float


@dataclass(frozen=True, slots=True)
class MomentumSelection:
    selected_variant: SignalVariant
    variant_results: tuple[MomentumVariantResult, ...]
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    validation_start: pd.Timestamp
    selection_end: pd.Timestamp


@dataclass(slots=True)
class MomentumValidation:
    audit: MomentumDataAudit
    selection: MomentumSelection
    result: BacktestResult
    regime_performance: dict[str, dict[str, float]]
    gate_checks: dict[str, bool]
    bull_profit_fraction: float
    non_bull_pnl: float
    status: str


def _perp_assets(panel: MarketPanel) -> tuple[str, ...]:
    return tuple(
        sorted(
            str(column)
            for column in panel.prices.columns
            if str(column).startswith("HL:") and str(column).endswith(":perp")
        )
    )


def _delisted_assets(panel: MarketPanel, assets: tuple[str, ...]) -> tuple[str, ...]:
    if panel.tradable is None:
        return ()
    declared = panel.metadata.get("delisted_assets")
    if not isinstance(declared, (list, tuple)):
        return ()
    names = {str(value).upper() for value in declared if isinstance(value, str)}
    result = []
    for instrument in assets:
        asset = instrument.split(":")[1]
        lifecycle = panel.tradable[instrument]
        final = lifecycle.iloc[-1]
        if asset in names and bool(lifecycle.eq(True).any()) and (pd.isna(final) or not bool(final)):
            result.append(asset)
    return tuple(result)


def audit_momentum_panel(
    panel: MarketPanel,
    *,
    minimum_history_hours: int = 365 * 24,
    minimum_assets: int = 6,
) -> MomentumDataAudit:
    """Fail closed on missing lifecycle, OI, liquidation or execution observations."""

    panel.validate()
    if minimum_history_hours < 24 or minimum_assets < 2:
        raise ValueError("momentum audit requires at least 24 hours and two assets")
    index = pd.DatetimeIndex(panel.prices.index)
    hourly = len(index) >= 2 and bool(
        index.to_series().diff().dropna().eq(pd.Timedelta(hours=1)).all()
    )
    observed_hours = len(index) if hourly else 0
    assets = _perp_assets(panel)
    delisted = _delisted_assets(panel, assets)
    source = str(panel.metadata.get("source", "")).casefold()
    evidence = panel.metadata.get("calibration_evidence_hash")
    evidence_ok = isinstance(evidence, str) and len(evidence) == 64 and all(
        character in "0123456789abcdefABCDEF" for character in evidence
    )
    lifecycle_hash = panel.metadata.get("lifecycle_hash")
    lifecycle_ok = (
        isinstance(panel.metadata.get("historical_universe_source"), str)
        and isinstance(lifecycle_hash, str)
        and len(lifecycle_hash) == 64
    )
    active = {
        instrument: (
            panel.tradable[instrument].eq(True)
            if panel.tradable is not None
            else panel.prices[instrument].notna()
        )
        for instrument in assets
    }

    def observed(frame: pd.DataFrame | None) -> bool:
        return frame is not None and bool(assets) and all(
            bool(mask.any()) and not bool(frame.loc[mask, instrument].isna().any())
            for instrument, mask in active.items()
        )

    checks = {
        "hourly_regular": observed_hours > 0,
        "minimum_history": observed_hours >= minimum_history_hours,
        "minimum_cross_section": len(assets) >= minimum_assets,
        "real_not_synthetic": bool(source) and "synthetic" not in source,
        "point_in_time": panel.metadata.get("point_in_time") is True,
        "point_in_time_matrices": all(
            value is not None for value in (panel.available_at, panel.finality, panel.tradable)
        ),
        "calibrated_data": (
            str(panel.metadata.get("calibration_status", "")).upper() == "CALIBRATED"
            and evidence_ok
        ),
        "historical_lifecycle_universe": lifecycle_ok and panel.tradable is not None,
        "delisted_markets_included": bool(delisted),
        "realized_hourly_funding": panel.metadata.get("funding_semantics") == "realized_hourly",
        "funding_observed": observed(panel.funding),
        "volume_observed": observed(panel.volume_usd),
        "open_interest_observed": observed(panel.open_interest_usd),
        "liquidation_semantics": (
            panel.metadata.get("liquidation_semantics") == "observed_hourly_notional"
        ),
        "liquidations_observed": observed(panel.liquidation_usd),
        "depth_observed": observed(panel.depth_usd),
    }
    messages = {
        "hourly_regular": "La grille UTC n'est pas horaire, reguliere et complete.",
        "minimum_history": (
            f"Couverture insuffisante : {observed_hours} h, {minimum_history_hours} h requises."
        ),
        "minimum_cross_section": f"Univers insuffisant : {len(assets)} actifs.",
        "real_not_synthetic": "La provenance reelle versionnee n'est pas etablie.",
        "point_in_time": "Le panel n'est pas declare point-in-time.",
        "point_in_time_matrices": "Disponibilite, finalite ou tradabilite point-in-time manque.",
        "calibrated_data": "La calibration des donnees n'est pas verifiable.",
        "historical_lifecycle_universe": "La source ou le hash lifecycle historique manque.",
        "delisted_markets_included": "Aucun marche deliste n'est conserve dans l'univers.",
        "realized_hourly_funding": "Le funding n'est pas un paiement horaire realise.",
        "funding_observed": "Le funding manque pendant une periode tradable.",
        "volume_observed": "Le volume manque pendant une periode tradable.",
        "open_interest_observed": "L'open interest manque pendant une periode tradable.",
        "liquidation_semantics": "La semantique des liquidations horaires n'est pas declaree.",
        "liquidations_observed": "Le notionnel de liquidations manque pendant une periode tradable.",
        "depth_observed": "La profondeur executable manque pendant une periode tradable.",
    }
    return MomentumDataAudit(
        observed_hours=observed_hours,
        minimum_history_hours=minimum_history_hours,
        assets=assets,
        minimum_assets=minimum_assets,
        delisted_assets=delisted,
        checks=checks,
        reasons=tuple(messages[name] for name, passed in checks.items() if not passed),
    )


def _slice_panel(panel: MarketPanel, index: pd.DatetimeIndex) -> MarketPanel:
    return MarketPanel(
        prices=panel.prices.loc[index].copy(),
        funding=panel.funding.loc[index].copy(),
        spreads_bps=panel.spreads_bps.loc[index].copy(),
        volume_usd=panel.volume_usd.loc[index].copy(),
        metadata={**panel.metadata, "research_slice": (index[0].isoformat(), index[-1].isoformat())},
        depth_usd=panel.depth_usd.loc[index].copy() if panel.depth_usd is not None else None,
        open_interest_usd=(
            panel.open_interest_usd.loc[index].copy()
            if panel.open_interest_usd is not None
            else None
        ),
        liquidation_usd=(
            panel.liquidation_usd.loc[index].copy() if panel.liquidation_usd is not None else None
        ),
        available_at=panel.available_at.loc[index].copy() if panel.available_at is not None else None,
        finality=panel.finality.loc[index].copy() if panel.finality is not None else None,
        tradable=panel.tradable.loc[index].copy() if panel.tradable is not None else None,
    )


def _with_causal_regimes(
    panel: MarketPanel,
    strategy: RobustMomentumStrategy,
) -> MarketPanel:
    regimes = classify_market_regimes(panel, strategy.regime_config)
    encoded = "\n".join(
        f"{pd.Timestamp(cast(Any, timestamp)).isoformat()}|{value}"
        for timestamp, value in regimes.items()
    )
    regime_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return replace(
        panel,
        regimes=regimes,
        metadata={
            **panel.metadata,
            "regimes_point_in_time": True,
            "regime_hash": regime_hash,
            "regime_method": "phase09_past_only_market_mean_and_realized_volatility",
        },
    )


def _validate_split(
    panel: MarketPanel,
    train_index: pd.DatetimeIndex,
    validation_index: pd.DatetimeIndex,
    config: MomentumSelectionConfig,
) -> None:
    if len(train_index) < config.minimum_train_bars:
        raise ValueError("insufficient train history for momentum selection")
    if len(validation_index) < config.minimum_validation_bars:
        raise ValueError("insufficient validation history for momentum selection")
    if train_index[-1] >= validation_index[0]:
        raise ValueError("train must end before validation begins")
    combined = train_index.append(validation_index)
    if not combined.isin(panel.prices.index).all():
        raise ValueError("split contains timestamps outside the market panel")


def select_momentum_variant(
    panel: MarketPanel,
    *,
    train_index: pd.DatetimeIndex,
    validation_index: pd.DatetimeIndex,
    config: MomentumSelectionConfig,
    engine: PanelBacktester,
) -> MomentumSelection:
    """Compare every preregistered signal on validation without exposing final data."""

    _validate_split(panel, train_index, validation_index, config)
    selection_index = pd.DatetimeIndex(
        panel.prices.index[
            (panel.prices.index >= train_index[0])
            & (panel.prices.index <= validation_index[-1])
        ]
    )
    selection_panel = _slice_panel(panel, selection_index)
    results: list[MomentumVariantResult] = []
    for variant in config.signal_variants:
        strategy = config.strategy(variant, trade_start=validation_index[0])
        regime_panel = _with_causal_regimes(selection_panel, strategy)
        result = engine.run(regime_panel, strategy.generate(selection_panel))
        score = result.metrics.total_return - 0.25 * abs(result.metrics.max_drawdown)
        results.append(
            MomentumVariantResult(
                signal_variant=variant,
                validation_score=score,
                total_return=result.metrics.total_return,
                max_drawdown=result.metrics.max_drawdown,
                turnover=result.metrics.turnover,
                funding_return=result.metrics.funding_contribution,
                cost_return=result.metrics.cost_contribution,
            )
        )
    selected = max(results, key=lambda item: (item.validation_score, item.signal_variant))
    return MomentumSelection(
        selected_variant=selected.signal_variant,
        variant_results=tuple(results),
        train_start=train_index[0],
        train_end=train_index[-1],
        validation_start=validation_index[0],
        selection_end=validation_index[-1],
    )


def _regime_performance(
    result: BacktestResult,
    *,
    start: pd.Timestamp,
) -> dict[str, dict[str, float]]:
    if result.attribution.empty:
        raise ValueError("backtest result does not contain regime attribution")
    ledger = result.attribution.copy()
    timestamps = pd.to_datetime(ledger["timestamp"], utc=True)
    ledger = ledger.loc[timestamps.ge(start)]
    frame = ledger.groupby("regime", observed=True)[
        [
            "price_pnl",
            "funding_pnl",
            "basis_pnl",
            "spread_pnl",
            "fee_pnl",
            "slippage_pnl",
            "hedge_pnl",
        ]
    ].sum()
    performance: dict[str, dict[str, float]] = {}
    for regime, row in frame.iterrows():
        performance[str(regime)] = {
            "price_pnl": float(row.get("price_pnl", 0.0)),
            "funding_pnl": float(row.get("funding_pnl", 0.0)),
            "cost_pnl": float(row.get("spread_pnl", 0.0))
            + float(row.get("fee_pnl", 0.0))
            + float(row.get("slippage_pnl", 0.0)),
            "total_pnl": float(row.sum()),
        }
    return performance


def run_momentum_validation(
    panel: MarketPanel,
    *,
    engine: PanelBacktester,
    selection_config: MomentumSelectionConfig | None = None,
    gate_config: MomentumGateConfig | None = None,
    audit: MomentumDataAudit | None = None,
) -> MomentumValidation:
    selection_config = selection_config or MomentumSelectionConfig()
    gate_config = gate_config or MomentumGateConfig()
    audit = audit or audit_momentum_panel(panel)
    rows = len(panel.prices)
    train_end = int(rows * gate_config.train_fraction)
    validation_end = train_end + int(rows * gate_config.validation_fraction)
    if train_end <= 0 or validation_end >= rows:
        raise ValueError("momentum split leaves an empty train, validation or final test")
    train = pd.DatetimeIndex(panel.prices.index[:train_end])
    validation = pd.DatetimeIndex(panel.prices.index[train_end:validation_end])
    selection = select_momentum_variant(
        panel,
        train_index=train,
        validation_index=validation,
        config=selection_config,
        engine=engine,
    )
    final_start = panel.prices.index[validation_end]
    strategy = selection_config.strategy(selection.selected_variant, trade_start=final_start)
    if engine.risk_limits.max_gross_leverage > 1.0:
        raise ValueError("Phase 09 deployable engine cannot allow leverage above 1x")
    regime_panel = _with_causal_regimes(panel, strategy)
    result = engine.run(regime_panel, strategy.generate(panel))
    regime_performance = _regime_performance(result, start=final_start)
    bull_pnl = regime_performance.get("trend_up", {}).get("total_pnl", 0.0)
    non_bull_pnl = sum(
        values["total_pnl"]
        for regime, values in regime_performance.items()
        if regime != "trend_up"
    )
    positive_pnl = sum(max(0.0, values["total_pnl"]) for values in regime_performance.values())
    bull_profit_fraction = max(0.0, bull_pnl) / positive_pnl if positive_pnl > 0.0 else 0.0
    events = cast(dict[str, int], result.diagnostics.get("events", {}))
    required_regimes = {"trend_up", "trend_down", "chaos"}
    gate_checks = {
        "deployable_leverage_at_most_1x": bool(
            result.metrics.max_gross_leverage <= 1.0 + 1e-12
        ),
        "asset_exposure_bounded": bool(
            result.weights.abs().max(axis=1).max() <= strategy.maximum_asset_weight + 1e-12
        ),
        "correlation_limit_enabled": bool(strategy.maximum_pairwise_correlation < 1.0),
        "volatility_stop_exercised": bool(events.get("volatility_stops", 0) > 0),
        "liquidation_cooldown_exercised": bool(
            events.get("liquidation_spikes", 0) > 0
            and events.get("liquidation_cooldown_bars", 0) > 0
        ),
        "required_regimes_observed": bool(required_regimes.issubset(regime_performance)),
        "non_bull_pnl_above_floor": bool(
            non_bull_pnl >= gate_config.minimum_non_bull_pnl
        ),
        "bull_profit_concentration_bounded": bool(
            bull_profit_fraction <= gate_config.maximum_bull_profit_fraction
        ),
    }
    gate_checks["not_only_bull_market"] = (
        gate_checks["required_regimes_observed"]
        and gate_checks["non_bull_pnl_above_floor"]
        and gate_checks["bull_profit_concentration_bounded"]
    )
    if not audit.checks.get("minimum_history", False):
        status = "BLOCKED_INSUFFICIENT_REAL_DATA"
    elif not audit.passed:
        status = "BLOCKED_UNCALIBRATED_OR_SURVIVORSHIP_BIAS"
    elif result.diagnostics.get("audit_status") != "CALIBRATED":
        status = "BLOCKED_UNCALIBRATED_EXECUTION_MODEL"
    elif not all(gate_checks.values()):
        status = "REJECTED_DIRECTIONAL_ROBUSTNESS_GATE"
    else:
        status = "VALIDATED_RESEARCH_ONLY"
    return MomentumValidation(
        audit=audit,
        selection=selection,
        result=result,
        regime_performance=regime_performance,
        gate_checks=gate_checks,
        bull_profit_fraction=bull_profit_fraction,
        non_bull_pnl=non_bull_pnl,
        status=status,
    )


def write_momentum_report(validation: MomentumValidation, *, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics = validation.result.metrics
    payload = {
        "schema_version": 1,
        "status": validation.status,
        "data_audit": asdict(validation.audit),
        "selection": {
            "selected_variant": validation.selection.selected_variant,
            "variant_results": [asdict(item) for item in validation.selection.variant_results],
            "train_start": validation.selection.train_start.isoformat(),
            "train_end": validation.selection.train_end.isoformat(),
            "validation_start": validation.selection.validation_start.isoformat(),
            "selection_end": validation.selection.selection_end.isoformat(),
            "final_test_exposed_to_selection": False,
        },
        "performance": metrics.as_dict(),
        "performance_by_regime": validation.regime_performance,
        "bull_market_dependence": {
            "bull_profit_fraction": validation.bull_profit_fraction,
            "non_bull_pnl": validation.non_bull_pnl,
            "not_only_bull_market": validation.gate_checks["not_only_bull_market"],
        },
        "gate_checks": validation.gate_checks,
        "risk": {
            "maximum_deployable_leverage": 1.0,
            "events": validation.result.diagnostics.get("events", {}),
            "martingale": False,
        },
    }
    summary_path = output_dir / "momentum_regime_summary.json"
    temporary = summary_path.with_name(f".{summary_path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(summary_path)
    variant_rows = "".join(
        "<tr>"
        f"<td>{html.escape(item.signal_variant)}</td>"
        f"<td>{item.validation_score:.6f}</td>"
        f"<td>{item.total_return * 100:.2f} %</td>"
        f"<td>{item.max_drawdown * 100:.2f} %</td>"
        f"<td>{item.funding_return * 100:.3f} %</td>"
        f"<td>{item.cost_return * 100:.3f} %</td>"
        f"<td>{item.turnover:.3f}</td>"
        "</tr>"
        for item in validation.selection.variant_results
    )
    regime_rows = "".join(
        "<tr>"
        f"<td>{html.escape(regime)}</td>"
        f"<td>{values['price_pnl']:.2f}</td>"
        f"<td>{values['funding_pnl']:.2f}</td>"
        f"<td>{values['cost_pnl']:.2f}</td>"
        f"<td>{values['total_pnl']:.2f}</td>"
        "</tr>"
        for regime, values in sorted(validation.regime_performance.items())
    )
    reasons = "".join(f"<li>{html.escape(reason)}</li>" for reason in validation.audit.reasons)
    document = f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><title>HyperLab Phase 09 - momentum et regimes</title>
<style>body{{font-family:Segoe UI,sans-serif;max-width:1200px;margin:auto;padding:32px;background:#091321;color:#edf5ff}}table{{width:100%;border-collapse:collapse;margin:16px 0}}th,td{{padding:9px;border:1px solid #29405d;text-align:right}}th:first-child,td:first-child{{text-align:left}}section{{padding:16px;border:1px solid #496786;border-radius:10px;margin:16px 0}}</style></head>
<body><h1>Phase 09 - momentum et regimes</h1>
<section><strong>{html.escape(validation.status)}</strong><ul>{reasons}</ul></section>
<h2>Comparaison des signaux sur validation</h2>
<table><thead><tr><th>Variante</th><th>Score</th><th>Rendement</th><th>Drawdown</th><th>Funding</th><th>Couts</th><th>Turnover</th></tr></thead><tbody>{variant_rows}</tbody></table>
<p>Variante gelee avant le test final : <strong>{html.escape(validation.selection.selected_variant)}</strong>.</p>
<h2>Performance par regime</h2>
<table><thead><tr><th>Regime</th><th>Prix</th><th>Funding</th><th>Couts</th><th>PnL total</th></tr></thead><tbody>{regime_rows}</tbody></table>
<h2>Dependance au bull market</h2>
<p>Fraction des profits positifs en trend_up : {validation.bull_profit_fraction * 100:.2f} %. PnL hors trend_up : {validation.non_bull_pnl:.2f}. Gate : {validation.gate_checks['not_only_bull_market']}.</p>
<h2>Risque directionnel</h2>
<p>Volatilite cible, stop de volatilite, exposition brute 1x maximum, limite par actif et de correlation. Liquidation spike : cooldown obligatoire. Aucune martingale.</p>
</body></html>"""
    report_path = output_dir / "momentum_regime_report.html"
    report_path.write_text(document, encoding="utf-8")
    return report_path


__all__ = [
    "MomentumDataAudit",
    "MomentumGateConfig",
    "MomentumSelection",
    "MomentumSelectionConfig",
    "MomentumValidation",
    "MomentumVariantResult",
    "audit_momentum_panel",
    "run_momentum_validation",
    "select_momentum_variant",
    "write_momentum_report",
]
