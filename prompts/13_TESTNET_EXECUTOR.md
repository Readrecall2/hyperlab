# Phase 13 — exécuteur testnet, composant séparé

Cette phase ne commence qu'après validation humaine explicite de la phase paper. Elle ne doit jamais transformer silencieusement la branche read-only `0.2.x` en bot exécutable.

## Frontière de version obligatoire

Avant tout code d'ordre :

1. créer une branche dédiée ;
2. démarrer une version `0.3.0-dev` ou un dépôt/service séparé ;
3. conserver le collecteur et le dashboard `0.2.x` read-only ;
4. proposer une politique `AGENTS.md` propre au testnet dans un commit revu séparément ;
5. obtenir une approbation humaine avant de l'appliquer.

## Objectif

Ajouter un service testnet séparé, impossible à pointer accidentellement vers mainnet.

## Exigences

- constante testnet codée et vérifiée ;
- refus de démarrer si une URL, un chain ID ou un environnement mainnet est détecté ;
- API wallet testnet dédiée ;
- keystore chiffré hors dépôt ;
- CLOID déterministes ;
- post-only, IOC et reduce-only ;
- réconciliation exchange-first ;
- timeout ambigu = recherche de l'ordre, jamais renvoi aveugle ;
- dead-man switch ;
- limites de notionnel minuscules ;
- scénarios de fill partiel, doublon, perte WebSocket et redémarrage.

## Interdiction

Aucun réseau mainnet, aucun capital réel et aucun secret dans les tests, logs, prompts ou commits.
