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

SQLite reste une zone opérationnelle mutable. Il ne constitue ni l'archive de
recherche ni une preuve d'intégrité. La racine du lake ne tolère qu'un writer
actif : le root lock couvre aussi la récupération d'orphelins et l'index de
déduplication dérivé. Une capture simultanée multi-venue doit donc partager un
writer coordonné qui sérialise les publications ; elle ne doit jamais contourner
le verrou. Les vues du writer refusent toute ligne dont la venue ne correspond
pas à leur périmètre.

La couche de recherche utilise cette

```text
data/lake/
  venue=<venue>/date=<YYYY-MM-DD>/asset=<asset>/type=<record_type>/
    part-<sha256>.parquet
    part-<sha256>.manifest.json
  catalog.duckdb
data/quality/
  <YYYY-MM-DD>.json
```

La date de partition est toujours la date UTC de `event_time`. Les noms de
venue et d'actif sont des dimensions historiques : un actif délisté reste dans
l'inventaire et dans les exports. Le catalogue DuckDB est dérivé des manifestes
et peut être reconstruit. Il matérialise les octets vérifiés au moment de sa
construction : une modification ultérieure d'un Parquet ne change pas le
catalogue déjà produit et sera rejetée à la validation suivante. Les fichiers
Parquet et leurs manifestes restent la source de vérité.

## Schémas versionnés

Les types versionnés sont `candle`, `bbo`, `l2_snapshot`, `l2_delta`, `trade`,
`funding`, `open_interest`, `fee`, `connection_event` et
`instrument_lifecycle`. Ils couvrent les bougies, le BBO, le carnet L2, les
trades, le funding, l'open interest, les frais, les événements de connexion et
le cycle de vie des instruments. Chaque fichier déclare un nom de schéma, une
version et une empreinte du schéma physique. Une évolution additive exige une
nouvelle version ; elle ne modifie jamais un fichier déjà écrit. Une version
inconnue ou un fichier dont l'empreinte ne correspond pas à la version annoncée
est rejeté.

Chaque famille contient `event_time`, `exchange_time` et `received_time` en
UTC. Une valeur absente reste absente : l'écriture, la validation, le catalogue
et l'export n'effectuent aucun forward-fill. Les snapshots L2 et les deltas L2
ont des types de partitions distincts et ne peuvent pas être mélangés.

## Immutabilité et audit

Une partition est publiée atomiquement et n'est jamais écrasée. Son manifeste
enregistre au minimum :

- chemin relatif du fichier et SHA-256 de ses octets ;
- version et empreinte du schéma ;
- nombre de lignes et bornes des trois timestamps ;
- doublons, messages hors ordre, trous et valeurs nulles ;
- bornes de séquence et état de qualité ;
- reconnexions et resynchronisations applicables.

Une nouvelle synchronisation L2 ouvre une séquence explicitement identifiable.
Un saut de séquence sans événement de connexion ou snapshot de
resynchronisation est une erreur de qualité, jamais un trou masqué.

## Contrôles obligatoires

- timestamps UTC ;
- index trié et sans doublon ;
- trous explicitement marqués ;
- pas de forward-fill silencieux sur funding, trades ou carnet ;
- mapping des actifs versionné ;
- marchés délistés conservés pour éviter le survivorship bias ;
- hash de chaque partition ;
- rapport quotidien de fraîcheur.

## Vue de recherche point-in-time

Le backtester ne consomme pas directement la dernière ligne connue aujourd'hui. Une
vue point-in-time sélectionne, pour chaque décision, uniquement les événements dont
`received_time` est antérieur ou égal à cette décision. Pour les candles, la fenêtre
doit aussi être fermée et `is_final=false` est exclu. Une finalité `null` conserve sa
provenance et suit un délai d'éligibilité préenregistré; elle n'est jamais remplacée
par `true`.

Les jointures multi-venues sont backward sur le temps de réception, avec âge et
staleness visibles. L'univers vient des événements `instrument_lifecycle` connus à
la date simulée. Sans lifecycle, une sélection cross-sectionnelle échoue fermé au
lieu d'utiliser la liste actuelle des actifs.

Chaque run doit référencer les hashes des partitions, versions de schéma, bornes UTC
et états qualité qui ont produit cette vue.

Lorsqu'une vue multi-champs est aplatie en `MarketPanel`, son `available_at` par
instrument/barre est le maximum des `received_time` de tous les champs non nuls
exposés dans cette cellule. Sa finalité agrégée n'est vraie que si chaque observation
de bougie correspondante est éligible. Cette règle empêche qu'un timestamp de prix
précoce masque, par exemple, un funding ou une profondeur reçus plus tard.

La cadence n'est jamais déduite des observations. Elle vient du schéma ou des
métadonnées pour les flux cadencés, comme les bougies. Les trades, BBO et
événements L2 sont irréguliers : leurs pertes se détectent avec les séquences et
événements de connexion, pas avec un intervalle temporel inventé.

Les commandes et le format du rapport sont décrits dans
[`DATA_QUALITY.md`](DATA_QUALITY.md).
