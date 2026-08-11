# Phase 08 — pairs trading robuste

## Objectif

Étudier le retour à la moyenne entre actifs sans martingale.

## Recherche

- univers historique sans survivorship bias ;
- sélection de paires sur train uniquement ;
- hedge ratio rolling, Kalman ou cointegration selon validation ;
- tests de stabilité et rupture ;
- z-score causal ;
- stop de spread, time stop et cooldown ;
- sizing par volatilité du spread ;
- funding intégré ;
- coût de turnover.

## Interdictions

- aucune augmentation automatique après perte ;
- aucune moyenne à la baisse non bornée ;
- aucune sélection de paire à partir du test final.

## Gate

Les résultats doivent survivre au retrait des meilleures paires et à des ruptures de corrélation simulées.
