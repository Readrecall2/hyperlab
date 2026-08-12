# Architecture

```text
WINDOWS 11 — développement et recherche
  Git + Codex + Python + backtests + rapports + builds Docker
          |
          | image versionnée / code revu
          v
UMBREL — fonctionnement 24/24 en lecture seule
  collector public ---> SQLite/Parquet ---> dashboard local
          |
          +------------> export vers Windows pour backtests

PHASES ULTÉRIEURES, composants séparés
  paper engine -> testnet executor -> micro-mainnet executor
```

## Principe de séparation

Le collecteur, le moteur de stratégie et l'éventuel exécuteur ne doivent pas partager le même niveau de privilège. HyperLab 0.2.0 ne livre que les deux premiers sous forme de recherche et interdit les ordres.

## Modules

- `api/public.py` : appels publics au SDK officiel ;
- `storage/` : stockage local ;
- `data/` : schémas, import/export et données synthétiques ;
- `strategies/` : baselines sans exécution ;
- `backtest/` : coûts, limites, PnL et rapports ;
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
