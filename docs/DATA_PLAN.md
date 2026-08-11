# Plan de données

## Pourquoi collecter nous-mêmes

Les archives officielles sont utiles pour remonter dans le temps, mais leur publication n'est pas garantie en temps réel. Le mini-PC doit donc commencer immédiatement une collecte forward, même pendant le développement.

## Couche lente — toutes les minutes ou heures

- metadata perp et spot ;
- mark, oracle et mid ;
- funding courant et historique ;
- open interest ;
- volumes ;
- BBO et profondeur agrégée ;
- frais du compte lors des phases paper/live ;
- événements de connexion et trous de données.

## Couche sub-seconde — seulement pour lead-lag et market making

- carnet L2 complet ou mises à jour permettant sa reconstruction ;
- trades avec direction/agresseur ;
- BBO ;
- timestamps source et réception locale ;
- séquences, reconnexions et snapshots de resynchronisation ;
- prix de référence sur au moins une autre venue ;
- latence réseau observée.

## Format recommandé

- SQLite pour l'état léger et les snapshots opérationnels ;
- Parquet partitionné par `venue/date/asset/type` pour la recherche ;
- DuckDB pour les requêtes locales ;
- fichiers immuables et manifestes de qualité des données.

## Contrôles obligatoires

- timestamps UTC ;
- index trié et sans doublon ;
- trous explicitement marqués ;
- pas de forward-fill silencieux sur funding, trades ou carnet ;
- mapping des actifs versionné ;
- marchés délistés conservés pour éviter le survivorship bias ;
- hash de chaque partition ;
- rapport quotidien de fraîcheur.
