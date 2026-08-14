# Collecte publique multi-venue coordonnée

## Portée

Cette commande ne fait que produire les données nécessaires à une future
validation de la Phase 10. Le statut reste explicitement
`BLOCKED_PRECONDITION_NOT_MET` tant qu'une nouvelle collecte réelle n'a pas
passé l'audit de continuité. Elle ne lance aucune étude lead-lag, ne calcule
aucun signal économique et n'introduit aucune route d'ordre. Les transports
restent publics, sans clé, sans adresse de compte et sans signature.

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
| Binance USD-M | socket public : BBO `bookTicker` et snapshots complets top-20 `depth20@100ms` ; socket marché : trades `aggTrade`, mark/funding et candles ; mesures d'horloge continues et wire WebSocket brut |

Chaque frame WebSocket conserve `received_time` UTC, l'identifiant de connexion
physique, l'epoch, la séquence d'arrivée locale et l'identifiant de génération
coordonnée. Le trade normalisé conserve en plus `event_time` (temps de
transaction `T`), `exchange_time` (temps d'événement `E`) et la même lignée
physique que son wire `aggTrade`. Les snapshots Binance sont les états
top-20 complets publiés par ce stream : ils deviennent `l2_book_state` et
`l2_snapshot`, avec le dernier update ID source. Aucun faux delta et aucun
niveau au-delà des vingt niveaux publiés ne sont fabriqués.

Les sockets Binance `public` et `market` forment une seule génération supervisée.
La panne ou la staleness d'un membre invalide la génération entière, ferme les
deux sockets et impose une reconnexion commune. Au premier snapshot L2 de chaque
actif et de chaque nouvelle génération Binance, le runtime
enregistre un couple `resync_start` / `resync_complete` lié au snapshot et au
`book_epoch_id`. BBO et L2 sont surveillés séparément pour BTC et ETH : un flux
critique silencieux ne peut pas être masqué par l'autre actif. `aggTrade` est lui
aussi obligatoire pour chaque actif. Après le seuil de staleness, le run
journalise le gap et reconnecte en mode fail-closed.

Hyperliquid traite lui aussi, par actif, `activeAssetCtx`, `bbo`, `l2Book`
et `trades` comme des flux critiques. Le silence de l'un d'eux journalise un
gap et force la reconnexion publique.

L'horloge Binance est échantillonnée pendant toute la collecte, par défaut toutes
les 5 secondes. Chaque mesure persiste le RTT, l'offset estimé et l'incertitude
`RTT / 2`. Une mesure est valide uniquement si son incertitude est au plus de
50 ms ; sa validité causale est alors l'intervalle semi-ouvert
`[response_received_time, response_received_time + 15 s)`. Une mesure trop
incertaine est persistée comme invalide sans intervalle. Une déconnexion, une
génération différente ou une mesure devenue stale coupe la couverture : aucun
point n'est interpolé à travers cette période.

## Démarrage, panne et arrêt

Les deux workers attendent la même barrière de départ. Dès qu'un worker termine,
échoue ou reçoit une demande d'arrêt, le coordinateur arrête aussi l'autre. Après
la sortie des deux threads, chaque collecteur ferme son socket et publie son état,
et Binance ferme ses deux sockets physiques requis (`public` et `market`). Le
writer commun effectue ensuite le flush final et libère le root lock une seule fois.

Les statuts restent séparés :

- `data/runtime_status.json` pour Hyperliquid ;
- `data/runtime_status_binance_usdm.json` pour Binance USD-M.

## Smoke test causal de 15 minutes

Les bornes explicites isolent les lignes de la nouvelle collecte sans supprimer
ni réécrire les données déjà présentes dans le lake :

```powershell
$SmokeStart = [DateTimeOffset]::UtcNow.ToString("o")
.\.venv\Scripts\python.exe -m hyperlab collect-multi-venue `
  --assets "BTC,ETH" `
  --candle-intervals 1m `
  --duration-seconds 900 `
  --batch-size 10000 `
  --history-lookback-hours 1
$SmokeEnd = [DateTimeOffset]::UtcNow.ToString("o")
```

Après l'arrêt propre, l'audit exact est :

```powershell
.\.venv\Scripts\python.exe -m hyperlab data continuity data\lake `
  --assets "BTC,ETH" `
  --start $SmokeStart `
  --end $SmokeEnd `
  --json
```

Le code de sortie est `0` uniquement si, pour BTC et ETH, les trades normalisés
et les wires `aggTrade` sont non nuls et reliés exactement. Le premier état L2
Binance de chaque actif et génération doit être lié exactement à son couple
`resync_start` / `resync_complete` et au même snapshot. Les états L2 exacts
suivants ne rafraîchissent la couverture que dans cette connexion publique,
epoch et carnet déjà armés. L'horloge respecte aussi l'identité wire v2, la
politique stricte et la cadence réelle.
Chaque observation de trade exactement reliée reste utilisable causalement au
plus 30 secondes après son `received_time`. Ce TTL opérationnel borne la
fraîcheur pour l'audit ; il ne prétend pas inférer une cadence de trades propre à
la venue.
Il exige aussi l'absence de gap borné de séquence ou de connexion, une couverture
`clock_sync.coverage_continuous` vraie et un
`strict_phase_10_overlap.duration_seconds` strictement positif. Un succès reste
seulement un gate technique : `phase_10_status` vaut toujours
`BLOCKED_PRECONDITION_NOT_MET` dans ce rapport.

## Commande longue destinée à la Phase 10

Capture simultanée de 24 heures, BTC/ETH, avec le batch maximal exposé par la CLI :

```powershell
.\.venv\Scripts\python.exe -m hyperlab collect-multi-venue `
  --assets "BTC,ETH" `
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

Exécutez ensuite `data continuity` avec les bornes UTC de cette collecte. Une
capture de vingt-quatre heures et un audit technique réussi ne constituent pas
une preuve de représentativité économique, de latence stable ou de rentabilité,
et ne débloquent pas à eux seuls la Phase 10.

## Limites explicites

- Binance fournit ici vingt niveaux par côté, pas le carnet complet au-delà de
  cette profondeur.
- Les trades Binance sont agrégés par ordre taker.
- Une reconnexion peut laisser une fenêtre de couverture inconnue ; elle reste
  visible dans le wire, les epochs et les événements de connexion.
- Les horloges source appartiennent à chaque venue. Toute analyse future devra
  rester causale sur `received_time` et tenir compte des mesures de drift.
- Ce patch ne réalise aucune analyse économique lead-lag.
