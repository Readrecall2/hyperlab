# Phase 12 — paper trading live

## Objectif

Faire tourner les stratégies figées sur données live avec ordres simulés réalistes.

## Machine à états

`FLAT`, `ENTRY_PLANNED`, `LEG_1_PENDING`, `HEDGE_PENDING`, `HEDGED`, `EXIT_PLANNED`, `EXIT_PENDING`, `PAUSED`, `REDUCE_ONLY`, `MANUAL_REVIEW`, `EMERGENCY_FLATTEN`.

## Exigences

- état persistant ;
- événements idempotents ;
- identifiants déterministes ;
- latence et fill model calibrés ;
- aucune modification de paramètres pendant la fenêtre de validation ;
- replay exact des décisions ;
- dashboard read-only ;
- alertes ;
- tests de crash et redémarrage.

## Gate

6 à 8 semaines, nombre suffisant de cycles, 14 jours sans incident critique et résultat positif sous coûts stressés avant de créer un executor testnet.
