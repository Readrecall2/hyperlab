# Phase 05 — cash-and-carry spot/perp

## Objectif

Valider la stratégie défensive sur données réelles, sans ordre.

## Signal

- funding 8 h, 24 h et 72 h ;
- part d'heures positives ;
- tendance du funding ;
- basis et vitesse de convergence ;
- liquidité spot/perp ;
- volatilité et OI ;
- edge net prévu sur plusieurs horizons.

## Simulation

- deux jambes non atomiques ;
- maker puis hedge IOC si nécessaire ;
- frais spot/perp du compte ;
- slippage réel par taille ;
- marge perp conservatrice ;
- inversion du funding ;
- fermeture et coût d'opportunité.

## Rapport

Afficher rendement sur capital total immobilisé, temps investi, funding encaissé, basis, frais, hedge, max drawdown et capacité.

## Gate

Ne pas promouvoir la stratégie si la surperformance stressée face au benchmark passif est insuffisante.
