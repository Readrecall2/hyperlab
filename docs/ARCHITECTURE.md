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
