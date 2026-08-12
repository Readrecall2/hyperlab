from __future__ import annotations

from collections.abc import Callable
from typing import TypedDict

from hyperlab.strategies.base import Strategy
from hyperlab.strategies.carry import CashAndCarryStrategy
from hyperlab.strategies.cross_exchange import CrossExchangeFundingStrategy
from hyperlab.strategies.funding_basket import FundingBasketStrategy
from hyperlab.strategies.lead_lag import LeadLagStrategy
from hyperlab.strategies.momentum import MomentumRegimeStrategy
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
    "momentum_regime": MomentumRegimeStrategy,
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
        "status": "Backtester de base inclus",
        "data": "Perps HL, funding, volatilité, bêta",
        "summary": "Long des fundings faibles, short des fundings élevés, avec neutralisation.",
    },
    "cross_exchange_funding": {
        "label": "Arbitrage de funding inter-venues",
        "tier": "Niveau 2 — équilibré",
        "status": "Backtester de base inclus",
        "data": "HL + seconde plateforme synchronisée",
        "summary": "Même actif, long sur le funding bas et short sur le funding haut.",
    },
    "pairs_mean_reversion": {
        "label": "Pairs trading / retour à la moyenne",
        "tier": "Niveau 3 — offensif",
        "status": "Baseline incluse",
        "data": "Historique multi-actifs propre et sans survivorship bias",
        "summary": "Trade un écart statistique entre deux actifs corrélés sans martingale.",
    },
    "momentum_regime": {
        "label": "Momentum avec filtre de régime",
        "tier": "Niveau 3 — offensif",
        "status": "Baseline incluse",
        "data": "OHLCV, funding, OI, volatilité",
        "summary": "Directionnel, sizing par volatilité et pénalité de funding défavorable.",
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
        "status": "Simulateur jouet, pas validateur live",
        "data": "Replay L2 event-by-event + file d'attente + rejets",
        "summary": "Cotation bid/ask avec skew d'inventaire et filtre de toxicité.",
    },
}


def create_strategy(name: str) -> Strategy:
    try:
        return STRATEGY_FACTORIES[name]()
    except KeyError as exc:
        available = ", ".join(sorted(STRATEGY_FACTORIES))
        raise ValueError(f"unknown strategy {name!r}; available: {available}") from exc
