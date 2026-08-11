# Phase 01 — modèle de données et qualité

Lis `AGENTS.md`, `docs/DATA_PLAN.md` et `docs/BACKTEST_PROTOCOL.md`.

## Objectif

Créer une couche de données immuable, auditable et adaptée aux stratégies lentes comme aux futures données L2.

## Livrables

- schémas versionnés pour bougies, BBO, carnet L2, trades, funding, OI, frais et événements de connexion ;
- timestamps `event_time`, `exchange_time`, `received_time`, tous en UTC ;
- stockage Parquet partitionné par venue/date/actif/type ;
- catalogue DuckDB local ;
- manifestes de partitions avec hash, nombre de lignes, bornes temporelles et trous ;
- commandes CLI `data validate`, `data inventory`, `data export` ;
- tests de doublons, ordering, trous, timezone et évolution de schéma.

## Contraintes

- jamais de forward-fill silencieux ;
- conserver les actifs délistés ;
- ne pas mélanger snapshots et deltas L2 ;
- rendre toute resynchronisation visible ;
- ne pas introduire de clé ou d'ordre.

## Définition de terminé

Une partition volontairement corrompue doit être rejetée avec un message clair ; un rapport quotidien de qualité doit être reproductible.
