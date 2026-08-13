# Collecte publique multi-venue coordonnée

## Portée

Cette commande satisfait uniquement la précondition de capture de la Phase 10.
Elle ne lance aucune étude lead-lag, ne calcule aucun signal économique et
n'introduit aucune route d'ordre. Les deux transports restent publics, sans clé,
sans adresse de compte et sans signature.

## Audit du stockage

`data/lake` contient trois états qui doivent évoluer sous une seule autorité :

1. les Parquet immuables et leurs manifestes publiés exclusivement ;
2. la récupération des Parquet orphelins après interruption ;
3. `.collector-observations.sqlite3`, index de déduplication dérivé des
   manifestes validés.

`_RootWriterLock` est un verrou non bloquant conservé pendant toute la vie du
`BatchingLakeSink`. Il empêche deux processus de publier, récupérer des
orphelins ou mettre à jour l'index sur la même racine en parallèle. Deux sinks
indépendants restent donc volontairement refusés, même si leurs venues diffèrent.
La collecte multi-venue ne supprime, ne partage et ne contourne jamais ce verrou.

L'index SQLite n'est pas la source de vérité. À l'ouverture, il est réconcilié
depuis les Parquet dont les manifestes passent la validation. Il conserve :

- la dernière révision logique des candles, identifiée par
  `(venue, asset, interval, open_time)` ;
- la dernière révision logique du funding, identifiée par
  `(venue, asset, funding_time, rate_kind)` ;
- la clé primaire stable des trades, qui commence par `(venue, asset, trade_id)` ;
- les couples fichier/hash des manifestes déjà indexés.

Toutes les clés primaires versionnées contiennent `venue`. Un même trade ID ou
une même fenêtre temporelle sur Hyperliquid et Binance ne peuvent donc pas entrer
en collision dans la déduplication.

Les groupes d'écriture restent séparés par venue, type, actif, date UTC et stream.
Chaque publication produit un Parquet adressé par son contenu puis un manifeste
validable sous :

```text
data/lake/venue=<venue>/date=<YYYY-MM-DD>/asset=<asset>/type=<record_type>/
```

La publication est atomique par fichier et manifeste, pas comme transaction
globale couvrant toutes les partitions. Les noms de venue conservés sont
exactement `hyperliquid` et `binance_usdm`.

## Ordre logique et frontières de flush

Un Parquet est un run immuable trié par la clé d'ordre du schéma. Plusieurs
Parquet d'un même stream forment donc un ensemble de runs triés, et non un
journal dont l'ordre serait celui des noms de fichiers ou des instants de flush.
L'inventaire valide chaque run séparément, puis effectue une fusion ordonnée des
runs avant les contrôles globaux de clés primaires, cadence, séquences et epochs
L2. Un chevauchement de bornes entre deux segments n'est pas une inversion.

Le smoke test du 13 août 2026 a exercé ce cas réel sur
`binance_usdm/ETH/l2_snapshot` : le snapshot reçu à
`2026-08-13T00:34:15.602230Z`, `arrival_sequence=40862`, a été coupé après cinq
asks et vingt bids ; les quinze asks restants ont formé un second segment. Les
champs `event_time`, `received_time`, `connection_id`, `snapshot_id`,
`book_epoch_id` et `last_sequence` concordaient. Le wire 40862 était encadré par
40861 et 40863 dans le même epoch, et le resync ETH était déjà terminé : ce
n'était ni une reconnexion, ni une arrivée source hors ordre.

Les runtimes soumettent désormais toutes les lignes normalisées d'une frame
WebSocket comme un lot logique. Le mutex du writer coordonné interdit à un flush
d'une autre venue d'entrer au milieu de ce lot. Si le lot entier ne tient pas
dans la capacité restante, il est refusé avant tout ajout ; une erreur pendant
l'ajout restaure l'état mémoire antérieur. Le seuil `batch_size` peut donc être
dépassé par un seul message atomique, sans perte et dans la limite stricte de
`queue_capacity`.

## Writer coordonné

`collect-multi-venue` construit un unique `CoordinatedLakeWriter`. Celui-ci
possède exactement un `BatchingLakeSink`, donc un root lock, une récupération
d'orphelins et un index SQLite. Il expose deux vues sérialisées :

- le client Hyperliquid refuse toute ligne dont `venue != hyperliquid` ;
- le client Binance refuse toute ligne dont `venue != binance_usdm`.

Un mutex couvre les ajouts, flushes, contrôles de déduplication et transactions
SQLite. À chaque flush, le coordinateur rapproche les lignes acceptées et
publiées par venue. Une venue inconnue, un deuxième client pour la même venue,
une ligne attribuée à la mauvaise venue ou un désaccord de comptage arrête le
run. Un processus externe qui ouvre le même lake reste rejeté par
`_RootWriterLock`.

## Flux simultanés BTC/ETH

| Venue | Flux publics conservés |
|---|---|
| Hyperliquid | BBO, états `l2Book`, trades, contextes d'actifs, candles, funding REST et wire WebSocket brut |
| Binance USD-M | BBO `bookTicker`, snapshots complets top-20 `depth20@100ms`, trades agrégés, mark/funding, candles, mesure d'horloge et wire WebSocket brut |

Chaque frame WebSocket conserve `received_time` UTC, l'identifiant de connexion,
l'epoch et la séquence d'arrivée locale. Les snapshots Binance sont les états
top-20 complets publiés par ce stream : ils deviennent `l2_book_state` et
`l2_snapshot`, avec le dernier update ID source. Aucun faux delta et aucun
niveau au-delà des vingt niveaux publiés ne sont fabriqués.

Au premier snapshot L2 de chaque actif et de chaque connexion Binance, le runtime
enregistre un couple `resync_start` / `resync_complete` lié au snapshot et au
`book_epoch_id`. BBO et L2 sont surveillés séparément pour BTC et ETH : un flux
critique silencieux ne peut pas être masqué par l'activité d'un trade, d'une
candle ou de l'autre actif. Après le seuil de staleness, le run journalise le gap
et reconnecte en mode fail-closed.

## Démarrage, panne et arrêt

Les deux workers attendent la même barrière de départ. Dès qu'un worker termine,
échoue ou reçoit une demande d'arrêt, le coordinateur arrête aussi l'autre. Après
la sortie des deux threads, chaque collecteur ferme son socket et publie son état,
puis le writer commun effectue le flush final et libère le root lock une seule fois.

Les statuts restent séparés :

- `data/runtime_status.json` pour Hyperliquid ;
- `data/runtime_status_binance_usdm.json` pour Binance USD-M.

## Commande longue destinée à la Phase 10

Capture simultanée de 24 heures, BTC/ETH, avec le batch maximal exposé par la CLI :

```powershell
.\.venv\Scripts\python.exe -m hyperlab collect-multi-venue `
  --assets BTC,ETH `
  --candle-intervals 1m `
  --duration-seconds 86400 `
  --batch-size 10000 `
  --history-lookback-hours 24
```

Ne lancez pas `collect` ou `collect-reference` en parallèle de cette commande
sur le même `data/lake`. Leur erreur `active writer` est le comportement
fail-closed attendu.

Après l'arrêt propre, validez et inventoriez les artefacts avant toute Phase 10 :

```powershell
.\.venv\Scripts\python.exe -m hyperlab data validate data\lake --json
.\.venv\Scripts\python.exe -m hyperlab data inventory data\lake --json
```

La précondition n'est satisfaite que si l'inventaire confirme le chevauchement
BTC/ETH des deux venues et si la validation ne signale ni corruption ni rupture
incompatible. Vingt-quatre heures constituent une capture technique longue, pas
une preuve de représentativité économique, de latence stable ou de rentabilité.

## Limites explicites

- Binance fournit ici vingt niveaux par côté, pas le carnet complet au-delà de
  cette profondeur.
- Les trades Binance sont agrégés par ordre taker.
- Une reconnexion peut laisser une fenêtre de couverture inconnue ; elle reste
  visible dans le wire, les epochs et les événements de connexion.
- Les horloges source appartiennent à chaque venue. Toute analyse future devra
  rester causale sur `received_time` et tenir compte des mesures de drift.
- Ce patch ne réalise aucune analyse économique lead-lag.

