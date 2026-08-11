# Phase 00 — audit initial sans modification

Lis `AGENTS.md`, `README.md`, tout `docs/`, `pyproject.toml`, `src/`, `tests/`, `Dockerfile`, `compose.yaml` et `umbrel-app-store.yml` et `jjlab-hyperlab/`.

Ne modifie aucun fichier pendant le premier passage.

## Travaux

1. Cartographie les modules et les flux de données.
2. Prouve qu'aucun chemin ne peut signer ou envoyer un ordre.
3. Recherche les imports ou chaînes liés à une clé privée, seed ou `hyperliquid.exchange.Exchange`.
4. Exécute `ruff check .`, `mypy src/hyperlab`, `pytest` et la démo de 1200 heures.
5. Vérifie l'absence de look-ahead dans `PanelBacktester`.
6. Vérifie que les limites de risque ne créent pas de positions absentes du signal.
7. Inspecte le Dockerfile et le package Umbrel : non-root, read-only, aucun Docker socket, aucun secret.
8. Liste uniquement les défauts confirmés, classés par criticité.

## Après l'audit

Applique les corrections minimales dans un second passage, ajoute les tests de régression et refais tous les contrôles.

## Interdictions

Aucun nouveau collecteur, aucune authentification et aucun exécuteur dans cette phase.
