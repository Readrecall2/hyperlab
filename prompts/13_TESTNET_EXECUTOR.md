# Phase 13 — exécuteur Testnet, composant séparé

Cette phase peut être développée et préparée avant Gate D. Elle ne doit jamais
transformer silencieusement la branche read-only `0.2.x` en bot exécutable ni
considérer un reçu Paper comme une autorisation Testnet.

## Frontière de version obligatoire

Avant tout code d'ordre :

1. créer une branche dédiée ;
2. démarrer une version `0.3.0-dev` ou un dépôt/service séparé ;
3. conserver collecteur, dashboard et `HYPERLAB_MODE` 0.2.x read-only/research ;
4. proposer une politique `AGENTS.md` Testnet dans un commit revu séparément ;
5. obtenir une approbation humaine de cette politique technique.

Cette revue n'est pas un PASS Gate D et n'exige aucune preuve de rentabilité.

## Préparation `TESTNET`

Le reçu exact `TESTNET` / `TESTNET_EXECUTION` doit être lié au build, à la
configuration, à l'endpoint, à la stratégie et
aux limites. Toute identité absente, inconnue, composite ou ambiguë échoue fermée.
Il ne peut jamais être consommé par `MICRO_MAINNET` ou `MAINNET`.

Exigences :

- endpoint, chain ID et namespace Testnet explicitement allowlistés ;
- aucun fallback ou remplacement silencieux vers Mainnet ;
- credentials Testnet dédiés, hors dépôt, non réutilisés depuis Mainnet et dont le
  scope est contrôlé lorsque la venue le permet ;
- CLOID déterministes et machine à états persistante des ordres ;
- post-only, IOC, reduce-only et cancel/replace ;
- timeout ambigu = recherche/réconciliation, jamais renvoi aveugle ;
- réconciliation exchange-first des ordres, positions et comptes au démarrage et
  après restart ;
- reprise, fills partiels, doublons et pertes WebSocket testés ;
- limites bornées de notionnel/position, pause d'urgence, dead-man switch et journal
  d'audit complet ;
- aucun réseau Mainnet, capital réel ou secret dans tests, logs, prompts ou commits.

Les Gates économiques B/C/D ne sont pas des prérequis de cette préparation. Elles
continuent en parallèle.

## Gate E

Gate E est la preuve durable des exercices Testnet terminés. Elle est requise avant
tout argent réel, mais ne sert pas à autoriser le démarrage ou le développement
Testnet. Dans le checkout 0.2.x actuel, aucun adaptateur Testnet n'existe : le
développement est débloqué par la politique, l'exécution reste techniquement
bloquée jusqu'à l'implémentation et la revue du service séparé.
