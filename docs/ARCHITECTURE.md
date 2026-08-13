# Architecture

```text
WINDOWS 11 — développement et recherche
  Git + Codex + Python + backtests + rapports + builds Docker
          |
          | image versionnée / code revu
          v
UMBREL — fonctionnement 24/24, sans accès privé aux venues
  collector public ---> lake SQLite/Parquet ---> dashboard read-only
          |
          +------------> export recherche

WINDOWS / SERVICE LOCAL SÉPARÉ — jamais inclus dans le paquet Umbrel 0.2.x
  source publique normalisée ---> paper engine local ---> journal SQLite

PHASES ULTÉRIEURES, services et versions séparés
  Phase 13 testnet executor -> Phase 14 micro-mainnet executor
```

## Principe de séparation

Le collecteur, le moteur de stratégie, le paper engine, le dashboard et un éventuel
exécuteur réel ne doivent pas partager le même niveau de privilège. HyperLab 0.2.1
reste read-only vis-à-vis des venues : le paper engine transforme les décisions en
événements d'ordre **simulés localement**, sans clé, signature, client privé ou
transport d'ordre. Le dashboard ne peut ni soumettre une décision, ni annuler un
ordre simulé, ni modifier un paramètre. Le paquet Umbrel reste limité au dashboard
et à la collecte publique ; il n'embarque pas le runtime paper.

## Modules

- `api/public.py` : appels publics au SDK officiel ;
- `storage/` : stockage local ;
- `data/` : schémas, import/export et données synthétiques ;
- `strategies/` : baselines sans exécution ;
- `backtest/` : coûts, limites, PnL et rapports ;
- `paper/` : configuration figée, machine à états, journal SQLite, simulation
  d'ordres, risque, réconciliation, replay et gate Phase 12 ;
- `dashboard/` : interface locale read-only ;
- `umbrel-app-store.yml` et `jjlab-hyperlab/` : store Umbrel à la racine, conforme au template officiel ;
- `prompts/` : phases Codex séquentielles.

## Pipeline de recherche Phase 04

```text
lake Parquet + manifestes vérifiés
  → point_in_time.py (received_time, finalité, lifecycle, staleness)
  → protocol.py (split hashé, walk-forward, verrou final)
  → registry.py (toutes les variantes, JSONL append-only)
  → engine.py + costs.py + execution.py (fills simulés, jambes, IOC)
  → attribution.py + bootstrap.py + benchmark.py
  → report.py (actif, mois UTC, régime, taille, incertitude)
```

`target_weights` appartient au modèle de stratégie; `weights` appartient au
simulateur d'exécution et représente seulement les fills obtenus. Cette séparation
empêche un ordre manqué d'être compté comme position réelle. Tous les objets d'ordre
de cette couche sont inertes : aucun module de transport ou SDK d'exécution n'est
importé.

## Pipeline paper Phase 12

```text
flux public point-in-time + qualité/fraîcheur
  → stratégie et paramètres figés (hash de configuration + seed)
  → décision déterministe
  → contrôle de risque pré-acceptation
  → cycle d'ordre simulé Phase 04 (ack/reject, non-fill, partial, IOC, délais)
  → événements append-only idempotents + état persistant
  → ledger cash/positions/frais/PnL
  → réconciliation et replay déterministe
  → projection SQLite reconstruisible → dashboard read-only + alertes
```

SQLite est l'autorité opérationnelle du paper engine. Son journal est append-only,
ordonné et chaîné par hashes ; la projection d'état servie au dashboard est
reconstruisible. Au redémarrage, aucune nouvelle entrée n'est acceptée avant
vérification de la chaîne, replay et réconciliation exacte. Une divergence force
`MANUAL_REVIEW`.

`paper/runtime.py` supervise une source normalisée strictement publique : reprise
et réconciliation avant le premier poll, cadence des timers, filtrage des
redéliveries et arrêt propre. La CLI ne charge aucun module utilisateur ; elle ne
peut démarrer que des factories inscrites statiquement pour le `config_hash`
exact. Le registre est vide dans ce checkout, car aucun adaptateur live et aucune
stratégie Phase 10/11 ne sont encore admissibles. Les commandes de statut et de
replay restent read-only sur le store source ; le replay réexécute l'inbox dans un
store temporaire isolé.

La machine à états contient `FLAT`, `ENTRY_PLANNED`, `LEG_1_PENDING`,
`HEDGE_PENDING`, `HEDGED`, `EXIT_PLANNED`, `EXIT_PENDING`, `PAUSED`,
`REDUCE_ONLY`, `MANUAL_REVIEW` et `EMERGENCY_FLATTEN`. Les états de protection
ne fournissent aucune route privilégiée : même une réduction
d'urgence reste un fill simulé qui peut être partiel ou manqué.

Les modèles de coûts, latence et fills conservent les étiquettes `CALIBRATED`,
`UNCALIBRATED` ou `SYNTHETIC` et les hashes de leurs preuves. Les frais viennent
d'un artefact public versionné/hashé, jamais de données privées de compte. La
description normative complète se trouve dans
[`PAPER_ENGINE_PHASE12.md`](PAPER_ENGINE_PHASE12.md).
