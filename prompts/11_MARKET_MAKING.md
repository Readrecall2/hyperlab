# Phase 11 — market making adaptatif

## Objectif

Construire un simulateur L2 crédible avant toute cotation testnet.

## Modèle

- fair value multi-venue ;
- microprice, imbalance et order flow ;
- spread minimal couvrant frais et toxicité ;
- skew d'inventaire ;
- taille adaptative ;
- retrait des quotes en régime toxique ;
- queue position ;
- cancel/replace et priorité perdue ;
- fills partiels ;
- adverse selection après fill ;
- hedge éventuel.

## Données

Replay event-by-event avec snapshots de resynchronisation, séquences, trades et timestamps de réception.

## Validation

- PnL spread vs markout 100 ms/1 s/5 s ;
- inventaire maximal ;
- taux maker/taker ;
- fill ratio ;
- cancel-to-fill ;
- pertes pendant spikes ;
- panne et quotes abandonnées.

Le simulateur synthétique actuel doit rester étiqueté « toy » jusqu'à cette phase terminée.
