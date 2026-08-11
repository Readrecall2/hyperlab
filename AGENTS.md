# AGENTS.md — règles permanentes HyperLab

## Mission

Construire un laboratoire quantitatif reproductible, puis un paper trader fiable. La priorité est l'intégrité des données, l'absence de look-ahead et la sécurité opérationnelle — jamais l'obtention d'un rendement cible.

## Interdictions absolues dans la branche 0.2.x

- Ne jamais ajouter de clé privée, seed, API wallet ou signer.
- Ne jamais importer ou instancier `hyperliquid.exchange.Exchange`.
- Ne jamais envoyer, modifier ou annuler un ordre.
- Ne jamais ajouter une commande `live`, `trade`, `mainnet` ou équivalente.
- Ne jamais stocker un secret dans `.env`, Git, SQLite, logs, dashboard ou tests.
- Ne jamais implémenter martingale, doublement après perte ou grid sans stop dur.
- Ne jamais supposer qu'un ordre maker est rempli sans modèle explicite.

## Règles de recherche

- Un signal calculé à `t` ne peut gagner que sur `t → t+1` ou plus tard.
- Enregistrer toutes les variantes testées ; ne pas cacher les variantes perdantes.
- Séparer chronologiquement train, validation et test final.
- Le test final ne doit jamais servir à régler les paramètres.
- Inclure frais, spread, slippage, funding, remplissages partiels et ordres manqués.
- Rapporter rendement brut, net, drawdown, turnover, PnL par composante et exposition.
- Ne pas plafonner, lisser ou forcer les rendements.
- Toute donnée synthétique doit porter un avertissement visible.

## Umbrel

- Le paquet Umbrel reste read-only : dashboard + collecte publique.
- Aucun `privileged`, aucun montage de `/var/run/docker.sock`, aucune exposition Internet.
- Exécution en utilisateur non-root, `read_only`, `cap_drop: ALL`, `no-new-privileges`.
- Les données persistantes vont uniquement dans `${APP_DATA_DIR}/data`.

## Méthode de travail Codex

1. Lire ce fichier, le guide et le prompt de phase.
2. Auditer avant de modifier.
3. Créer un checkpoint Git.
4. Ajouter ou corriger les tests avant le refactor risqué.
5. Exécuter `ruff check .`, `mypy src/hyperlab` et `pytest`.
6. Faire une revue critique du diff.
7. Documenter les hypothèses et limites.
8. Arrêter la phase si une donnée indispensable manque au lieu de fabriquer un résultat.

## Définition de « terminé »

Une phase n'est terminée que lorsque les tests passent, les limites sont documentées, le rapport est reproductible et aucun chemin de trading réel n'a été introduit sans phase dédiée et revue humaine explicite.
