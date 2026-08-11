# Phase 02 — collecteur public Hyperliquid

Lis les docs officielles Hyperliquid actuelles avant de coder. Utilise le SDK officiel épinglé et vérifie ses signatures réelles.

## Objectif

Remplacer le snapshot REST minimal par un collecteur public robuste, toujours incapable de trader.

## Données

- metadata spot/perp ;
- mark, oracle, mid, funding, OI et volume ;
- historique de funding ;
- candles ;
- BBO et carnet L2 ;
- trades ;
- statut des connexions, ping/pong, reconnexions et séquences.

## Fonctionnement

- bootstrap REST, puis WebSocket ;
- resynchronisation explicite après déconnexion ;
- backoff borné avec jitter ;
- détection de stale data ;
- écriture batchée et atomique ;
- métriques de fraîcheur et de trous ;
- fixtures de messages réels anonymisés ;
- mode replay sans réseau.

## Sécurité

Interdire l'import de `Exchange`. Le module doit fonctionner sans adresse utilisateur et sans secret.

## Définition de terminé

24 heures de collecte test sans fuite mémoire, trous visibles, reprise après coupure et tests de replay déterministes.
