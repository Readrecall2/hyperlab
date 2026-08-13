from __future__ import annotations

from collections.abc import Callable
from typing import TypedDict

from hyperlab.strategies.base import Strategy
from hyperlab.strategies.carry import CashAndCarryStrategy
from hyperlab.strategies.cross_exchange import CrossExchangeFundingStrategy
from hyperlab.strategies.funding_basket import FundingBasketStrategy
from hyperlab.strategies.lead_lag import LeadLagStrategy
from hyperlab.strategies.momentum import RobustMomentumStrategy
from hyperlab.strategies.pairs import PairsMeanReversionStrategy


class CatalogEntry(TypedDict):
    label: str
    tier: str
    status: str
    data: str
    summary: str


STRATEGY_FACTORIES: dict[str, Callable[[], Strategy]] = {
    "cash_and_carry": CashAndCarryStrategy,
    "funding_basket": FundingBasketStrategy,
    "cross_exchange_funding": CrossExchangeFundingStrategy,
    "pairs_mean_reversion": PairsMeanReversionStrategy,
    "momentum_regime": RobustMomentumStrategy,
    "lead_lag": LeadLagStrategy,
}

STRATEGY_CATALOG: dict[str, CatalogEntry] = {
    "cash_and_carry": {
        "label": "Cash-and-carry spot/perp",
        "tier": "Niveau 1 — défensif",
        "status": "Validateur Phase 05 inclus — gate fermée sans données calibrées",
        "data": "Spot/perp, funding, BBO/profondeur, volume, OI et frais du compte",
        "summary": "Long spot et short perp sur edge net 8/24/72 h, avec hedge IOC simulé.",
    },
    "funding_basket": {
        "label": "Basket de funding",
        "tier": "Niveau 2 — équilibré",
        "status": "Validateur Phase 06 inclus — gate fermée sans univers historique calibré",
        "data": "Perps HL, funding, profondeur, lifecycle, volatilité et bêta BTC/ETH",
        "summary": "Ranking inverse-vol comparé à une optimisation dollar/bêta neutre et stressée.",
    },
    "cross_exchange_funding": {
        "label": "Arbitrage de funding inter-venues",
        "tier": "Niveau 2 — équilibré",
        "status": "Simulateur Phase 07 inclus — gate fermée sans données calibrées",
        "data": "Marks/oracles, funding réalisé, marges, frais et transferts HL + Binance",
        "summary": "Même actif sur deux comptes de marge, avec pannes et liquidation locale.",
    },
    "pairs_mean_reversion": {
        "label": "Pairs trading / retour à la moyenne",
        "tier": "Niveau 3 — offensif",
        "status": "Validateur Phase 08 inclus — gate fermée sans univers historique calibré",
        "data": "Perps, funding, profondeur et lifecycle point-in-time avec marchés délistés",
        "summary": "Sélection train-only, hedge validé puis stress de rupture sans martingale.",
    },
    "momentum_regime": {
        "label": "Momentum avec filtre de régime",
        "tier": "Niveau 3 — offensif",
        "status": "Validateur Phase 09 inclus — gate fermée sans historique calibré multi-régimes",
        "data": "Perps, volume, OI, funding, liquidations, profondeur et lifecycle point-in-time",
        "summary": "Momentum/breakout, régimes causaux, stop volatilité et exposition 1x maximum.",
    },
    "lead_lag": {
        "label": "Lead-lag multi-exchange",
        "tier": "Niveau 4 — agressif",
        "status": "Prototype bar-level seulement",
        "data": "Flux sub-seconde synchronisés + latence",
        "summary": "Cherche si une venue de référence anticipe temporairement Hyperliquid.",
    },
    "inventory_market_making": {
        "label": "Market making adaptatif",
        "tier": "Niveau 4 — agressif",
        "status": "Replay L2 Phase 11 inclus; démo synthétique toujours TOY",
        "data": "Snapshots/deltas/trades multi-venues, séquences et timestamps de réception",
        "summary": "Fair value, queue, latences, markouts et pannes; aucune route d'ordre.",
    },
}


def create_strategy(name: str) -> Strategy:
    try:
        return STRATEGY_FACTORIES[name]()
    except KeyError as exc:
        available = ", ".join(sorted(STRATEGY_FACTORIES))
        raise ValueError(f"unknown strategy {name!r}; available: {available}") from exc
