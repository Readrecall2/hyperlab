# Validation de l'archive 0.2.0

Contrôles exécutés dans l'environnement de génération :

- compilation Python de `src/`, `tests/` et `scripts/` ;
- **10 tests locaux réussis** ;
- test anti-look-ahead du moteur bar-level ;
- test des limites d'exposition ;
- test du parser public Hyperliquid ;
- test du dashboard read-only et de l'échappement HTML ;
- test des protections et de la structure racine du package Umbrel ;
- génération du rapport multi-stratégies synthétique ;
- validation syntaxique des fichiers YAML ;
- recherche de l'import d'exécution Hyperliquid et de champs secrets dans `src/`.

## Contrôles volontairement reportés à Windows/Codex/CI

L'environnement de génération ne disposait pas d'accès PyPI pour installer `ruff` et `mypy`. La CI GitHub et le premier audit Codex les exécutent avant toute phase suivante.

Docker n'était pas disponible dans cet environnement : le Dockerfile et les fichiers Compose ont été inspectés et testés structurellement, mais l'image doit encore être construite sur Windows/GitHub Actions.

Aucun appel réel à l'API Hyperliquid n'a été réalisé ici. La commande `hyperlab snapshot` sert de premier test d'intégration réseau sur Windows.

Les résultats du rapport de démonstration reposent sur des données synthétiques. Ils ne constituent ni un backtest historique réel, ni une estimation de rendement.
