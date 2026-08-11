# Phase 06 — basket de funding Hyperliquid

## Objectif

Construire un portefeuille long/short de perps recevant un spread de funding avec risque de marché réduit.

## Modèle

- score de funding persistant ;
- liquidité minimale et âge du marché ;
- poids inverse-vol ;
- neutralité dollar ;
- neutralité bêta BTC/ETH ;
- matrice de covariance shrinkée ;
- contraintes par actif ;
- pénalité de turnover ;
- filtre de squeeze/momentum pour les shorts.

## Validation

- comparer ranking simple et optimisation contrainte ;
- attribuer PnL funding vs performance relative ;
- stress de corrélation cassée ;
- stress de squeeze simultané ;
- exclusion d'un actif à la fois ;
- validation sur marchés délistés inclus.
