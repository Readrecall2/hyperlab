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

`collect-multi-venue` construit un unique `CoordinatedWriterProcess`. Celui-ci
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

Le processus writer possède le seul `CoordinatedLakeWriter` et le seul root
lock. Les superviseurs lui soumettent des frames logiques complètes dans un
budget exact de lignes : les capacités par venue restent 10 000 lignes et la
capacité totale reste 20 000. Ces réservations couvrent les frames en file, en
cours et acceptées mais pas encore publiées. Une frame qui ne tient pas est
refusée intégralement par une erreur fatale ; aucune capacité, aucun timeout et
aucune politique de reconnexion n'ont été augmentés pour absorber un backlog.

La disponibilité d'un auto-flush est maintenant calculée par groupe exact
`(venue, record_type, asset, date UTC, stream)`, et non par la somme de tous les
groupes clairsemés. Lorsqu'un groupe publiable atteint `batch_size`, seuls les
groupes prêts sont publiés ; les groupes clairsemés, ainsi que les candles et le
funding volontairement réservés aux barrières, restent en mémoire jusqu'à la
barrière FIFO de durabilité inchangée. Les barrières explicites de bootstrap,
reconnexion, arrêt et flush demandé, ainsi que la cadence Hyperliquid
`flush_interval_seconds`, publient toujours tout ce qui les précède et attendent
Parquet, manifeste, validation et `fsync`.

Une publication partielle rend immédiatement au parent les crédits durables
exacts par venue et libère exactement le même nombre de réservations ; les
crédits des groupes clairsemés et les caps restent inchangés. `add_many` demeure
atomique à l'admission d'une frame : refus ou exception restaure toutes ses
mutations. La publication n'est toutefois pas une transaction globale entre
plusieurs fichiers. Chaque Parquet immuable et son manifeste gardent leur contrat
atomique, adressé par contenu et récupérable après crash ; toute lignée de frame
incomplète entre fichiers est visible et fait échouer l'audit fermé, jamais
réparée ou masquée.

`BatchingLakeSink.add_many` now journals only mutations caused by the current
logical frame. It no longer copies all pending groups and the historical
100,000-key recent cache before every frame. Rollback remains exact, including
deduplication-cache order, observations, counters, and pending primary keys.

## Diagnostic writer Singapore (2026-08-14)

Le smoke après isolation du writer a traité 452/452 frames, persisté 10 080
lignes durables et conservé zéro gap ou déconnexion non propre sur les deux
venues, avec la lignée `aggTrade` exacte, jusqu'à l'échec de capacité Binance.
La high-water était 9 958/10 000 avec un rejet ; la résidence en file mesurait
environ 7 779 ms en médiane, 14 436 ms au p95 et 15 064 ms au p99.

Pour environ 11 398 lignes écrites, l'ancien seuil global a déclenché 1 342
partitions immuables. Le flush était à environ 142 ms au p50 mais atteignait
15 078 ms, alors que l'écriture Parquet et chaque `fsync` pris isolément
restaient de l'ordre de quelques millisecondes. La cause mesurée est donc le
fan-out de publications : chaque petit groupe déclenchait tri, Arrow, analyse,
Parquet, hash, publication et `fsync`, puis manifeste, `fsync`, relecture et
validation. La disponibilité par groupe exact coalesce ces mêmes lignes avant
publication ; le critère recherché est un débit durable confortablement supérieur
au pic Binance avec une résidence bornée, pas une file plus grande.

Le stress de référence validé reproduit maintenant le chemin de production :
452 frames Binance `depth20@100ms`, alternées BTC/ETH et cadencées à cinq fois
le débit nominal, sont ingérées avec 452 trades Hyperliquid simultanés. Cinq
lignes `clock_sync` valides sont ajoutées pendant le flux (RTT 80 ms,
incertitude 40 ms, donc sous le seuil inchangé de 50 ms). Les barrières complètes
FIFO sont demandées tous les 100 frames, soit toutes les cinq secondes au débit
nominal, puis une barrière finale publie le reliquat.

Le résultat est 19 441 lignes Binance et 452 lignes Hyperliquid, soit 19 893
lignes durables dans 71 partitions validées. Ces 71 partitions incluent
explicitement le fan-out des barrières de durabilité et remplacent le résultat
idéalisé antérieur sans barrières, qui n'est pas une référence de production.
Les capacités restent 10 000 lignes par venue et 20 000 au total : zéro rejet
de capacité sur les deux venues, high-water totale strictement inférieure à
8 000, high-water Binance strictement inférieure à 7 000 et résidence maximale
Binance strictement inférieure à 5 000 ms. Ces bornes sont des assertions du
stress, pas une augmentation des caps ni un relâchement de la cadence FIFO.

## Diagnostic réseau REST Singapore (2026-08-14)

Le benchmark autonome initial avait mesuré un RTT médian de 76,05 ms, un p99 de
84,19 ms et une incertitude p99 de 42,10 ms. Après le smoke, collecteur
complètement arrêté, le même benchmark de 120 requêtes a mesuré 394,48 ms au
minimum, 399,74 ms en médiane, 404,81 ms au p95 et 822,89 ms au p99
(incertitude p99 411,44 ms, offset médian 21,67 ms). Cette reproduction hors
HyperLab exclut le scheduling Python comme cause de ce changement.

Le DNS système sélectionnait alors des POP CloudFront européens, Amsterdam ou
Marseille, avec TCP autour de 137--178 ms. D'autres résolveurs retournaient des
adresses Singapore, notamment `13.35.36.x` (`sin2`) et `65.8.76.x` (`sin3`),
avec TCP autour de 1 ms. Un forçage opérateur temporaire de
`13.35.36.9 fapi.binance.com`, utilisé uniquement pour diagnostiquer la route,
a ramené 120 requêtes à 72,49 ms au minimum, 74,28 ms en médiane et 78,32 ms au
p95, mais avec 213,78 ms au p99 et 106,89 ms d'incertitude p99. L'application
ne contient et ne doit contenir ni IP CloudFront codée en dur, ni substitution
DNS, ni rotation de session destinée à améliorer artificiellement les chiffres.

Sur 180 requêtes persistantes à une seconde, les 180 réponses sont restées sur
`SIN2-P11` avec `x-cache: Miss from cloudfront`. La latence normale était stable
autour de 72--76 ms ; seulement cinq requêtes (2,78 %) ont atteint 208,22,
209,15, 211,26, 212,50 et 212,64 ms. Le temps `requests` jusqu'aux headers était
essentiellement égal au temps total et le POP n'a pas changé : ces pointes sont
des observations réseau réelles, mais ne prouvent pas un épinglage à un mauvais
edge. Le diagnostic conserve donc seulement des preuves passives DNS, famille,
pair effectivement sélectionné, POP/cache et identité de connexion.

## Smoke Singapore du 15 août 2026

La nouvelle collecte réelle bornée à dix minutes a conservé une seule génération
éligible par venue, zéro gap en fenêtre, zéro déconnexion non propre, une lignée
Binance brute/normalisée exacte et 2 323 trades normalisés. Le runtime est resté
sur `SIN2-P11` ; ses RTT persistants étaient principalement de 74 à 78 ms et ses
incertitudes de 37 à 39 ms, sous le seuil inchangé de 50 ms.

Les compteurs d'horloge étaient exactement `valid_v2_samples=118`,
`invalid_v2_samples=2`, `rejected_probe_samples=2`,
`hard_invalid_v2_samples=0`, `max_consecutive_rejected_probes=1`,
`consecutive_rejection_violations=0`, `offset_discontinuities=0`,
`generation_gap_count=0` et `market_active_without_valid_clock=[]`.
La couverture valide atteignait 599,301479 secondes et ne laissait que
0,037051 seconde hors couverture ; `relevant_gap_count=0`. Le FAIL
provenait exclusivement du calcul historique de cadence sur les seules
observations acceptées : `sample_spacing_violations=2`, chacune des deux
séquences isolées de forme `VALID -> REJECTED -> VALID` produisant
artificiellement un écart accepté supérieur à 10 000 ms, avec un maximum de
10 019,006 ms, puis `clock_sync_not_continuous`,
`clock_sync_sample_spacing_exceeded` et `strict_phase10_overlap_zero`.

Le calcul corrigé ci-dessous ne requalifie jamais une probe rejetée en preuve :
il vérifie la cadence sur tous les lancements persistés et liés, tout en
construisant la couverture uniquement avec les observations acceptées. Le zéro
de chevauchement strict était un effet fail-closed parallèle du même drapeau de
cadence ; aucune logique de chevauchement n'a été modifiée. Cette collecte reste
un FAIL historique et la Phase 10 reste `BLOCKED_PRECONDITION_NOT_MET` jusqu'à
un nouveau smoke Singapore réel.

## Flux simultanés BTC/ETH

| Venue | Flux publics conservés |
|---|---|
| Hyperliquid | BBO, états `l2Book`, trades, contextes d'actifs, candles, funding REST et wire WebSocket brut |
| Binance USD-M | socket public : snapshots complets top-20 `depth20@100ms`, avec BBO dérivé exactement des premiers niveaux du même wire ; socket marché : trades `aggTrade`, mark/funding et candles ; mesures d'horloge continues et wire WebSocket brut |

Chaque frame WebSocket conserve `received_time` UTC, l'identifiant de connexion
physique, l'epoch, la séquence d'arrivée locale et l'identifiant de génération
coordonnée. Le trade normalisé conserve en plus `event_time` (temps de
transaction `T`), `exchange_time` (temps d'événement `E`) et la même lignée
physique que son wire `aggTrade`. Chaque wire Binance `depth20@100ms` produit
atomiquement un BBO à partir de son premier niveau bid/ask, un
`l2_book_state` et ses `l2_snapshot`, tous reliés au même timestamp de réception
et au dernier update ID source. Il n'existe plus d'abonnement `bookTicker`
dédié. Aucun faux delta et aucun niveau au-delà des vingt niveaux publiés ne
sont fabriqués.

Les sockets Binance `public` et `market` forment une seule génération supervisée.
Leurs handshakes s'effectuent en parallèle avec lecteurs suspendus. Le runtime
persiste ensuite les deux événements `connect` au même instant d'activation,
puis démarre les deux lecteurs ; un socket sans démarrage différé est rejeté.
La panne ou la staleness d'un membre invalide la génération entière, ferme les
deux sockets et impose une reconnexion commune. Au premier snapshot L2 de chaque
actif et de chaque nouvelle génération Binance, le runtime enregistre un couple
`resync_start` / `resync_complete` lié au snapshot et au `book_epoch_id`. Le
stream depth unique prouve simultanément le BBO et le L2 pour chaque actif ;
`aggTrade` reste obligatoire pour chaque actif. Après le seuil de staleness, le
run journalise le gap et reconnecte en mode fail-closed.

Hyperliquid traite lui aussi, par actif, `activeAssetCtx`, `bbo`, `l2Book`
et `trades` comme des flux critiques. Le silence de l'un d'eux journalise un
gap et force la reconnexion publique. Les snapshots L2 de restauration REST et
leur BBO dérivé portent une provenance `rest:` explicite. L'audit ne les traite
pas comme du wire WebSocket uniquement si l'identité REST, le groupe complet de
niveaux et les prix/quantités du meilleur bid/ask correspondent exactement ;
toute provenance incomplète ou incohérente reste rejetée.

L'horloge Binance est échantillonnée pendant toute la collecte, par défaut toutes
les 5 secondes. Chaque mesure persiste le RTT, l'offset estimé et l'incertitude
`RTT / 2`. Une mesure est valide uniquement si son incertitude est au plus de
50 ms ; sa validité causale est alors l'intervalle semi-ouvert
`[response_received_time, response_received_time + 15 s)`. Une mesure trop
incertaine reste persistée `INVALID`, avec son RTT et sa raison, et ne crée aucun
intervalle valide. Elle n'annule toutefois pas rétroactivement un intervalle
accepté antérieur qui couvre encore causalement cet instant. L'audit utilise
uniquement l'union des intervalles acceptés de la même génération éligible.

La conformité de cadence est un contrôle séparé. Chaque ligne `clock_sync` v2
persistée et liée exactement à la génération et au wire public compte comme une
tentative au temps `request_sent_time`, qu'elle soit `VALID` ou `INVALID`.
Le contrôle couvre la génération active, de son activation liée ou du début de
la fenêtre jusqu'à son événement terminal lié ou la fin de fenêtre : la première
et la dernière tentative sont donc contrôlées comme les paires internes. L'écart
entre deux bornes ou lancements consécutifs doit rester inférieur ou égal à
10 000 ms exactement ; il n'existe aucun epsilon. Une probe rejetée atteste
uniquement son lancement pour ce contrôle, mais ne crée toujours aucun intervalle
causal et ne contribue jamais aux bandes d'offset. Une identité non liée ne
crédite aucune cadence, et un échec de requête reste un événement fatal. Si une
requête est lancée avant la fin de fenêtre mais répond juste après, son
`request_sent_time` est retenu uniquement pour la cadence ; sa réponse ne
devient pas une preuve causale dans la fenêtre et ne modifie pas les séries de
rejets de celle-ci.

Au plus une rejection haute-RTT consécutive peut être franchie dans une
génération active, uniquement si les observations acceptées maintiennent une
couverture continue bornée à 50 ms. Une observation valide suivante remet cette
série à zéro. La deuxième probe rejetée consécutive révoque la couverture à
partir de son propre `response_received_time`, même si l'intervalle valide
antérieur de 15 secondes n'a pas encore expiré. Cette outage reste ouverte
jusqu'à la prochaine probe acceptée dans la même génération ; celle-ci peut
rétablir la couverture pour les données marché ultérieures, sans rendre valide
la période révoquée.

Le gate échoue seulement si l'intervalle causal effectivement évalué pour cette
génération intersecte cette outage, ou pour les autres causes fermées déjà
définies : absence de récupération, expiration après une seule rejection,
écart supérieur à 10 secondes entre deux tentatives liées, absence de mesure
valide, discontinuité des bandes d'offset, identité/policy invalide, événement
d'échec, déconnexion ou
changement de génération. Une outage pré-fenêtre ou antérieure à l'activité
évaluée reste comptée et publiée même si sa récupération précède l'assessment ;
elle ne condamne pas les données marché causalement postérieures. Aucun point
invalide n'est interpolé, promu ou silencieusement supprimé.

Le transport REST réutilise une session HTTPS afin de ne pas inclure une nouvelle
négociation DNS/TCP/TLS dans chaque échantillon. Le RTT de chaque requête reste
mesuré en entier : cette optimisation ne réduit ni ne contourne le seuil de
50 ms. Avant toute étude économique, le plus court horizon `H_min` devra être
préenregistré. Sa politique d'acceptation sera
`U_budget = min(50 ms, H_min / 10)`, avec l'incertitude effective comprenant
`RTT / 2`, une allocation conservatrice de 1 ms pour la quantification du temps
serveur Binance et toute erreur locale mesurée séparément. Une calibration
indépendante devra satisfaire ce budget au p99 global et horaire ; sinon la
Phase 10 reste bloquée ou l'horizon minimal est restreint prospectivement,
jamais après lecture des résultats économiques.

## Démarrage, panne et arrêt

Les deux workers attendent la même barrière de départ. Dès qu'un worker termine,
échoue ou reçoit une demande d'arrêt, le coordinateur arrête aussi l'autre. Après
la sortie des deux threads, chaque collecteur ferme son socket et publie son état,
et Binance ferme ses deux sockets physiques requis (`public` et `market`). Le
writer commun effectue ensuite le flush final et libère le root lock une seule fois.

Les statuts restent séparés :

- `data/runtime_status.json` pour Hyperliquid ;
- `data/runtime_status_binance_usdm.json` pour Binance USD-M.

## Runtime observability

Each status path is relative to the isolated `HYPERLAB_DATA_DIR`. The final
status retains both active and terminal socket-generation snapshots, so a clean
one-generation run does not lose its queue high-water evidence.

- `observability.process` reports process CPU, RSS/peak RSS, Python allocation
  and GC pause summaries, context switches, thread counts, watchdog/worker lag,
  and Linux `/proc` scheduler, cgroup CPU throttle/quota, PSI, iowait and steal
  counters when available.
- `observability.worker_phases` separates normalization time from bounded writer
  admission time, plus Hyperliquid REST worker materialization and batched apply.
- socket telemetry reports reader identity/state, queue depth/capacity/high-water,
  enqueue delay, dequeue residence/oldest age, overflow count, and terminal reason.
- Binance `clock_observability` separates executor dispatch, authoritative clock
  RTT, HTTP adapter/header and request/decode phases, future drain delay, and
  best-effort HTTP keep-alive/TLS evidence. It reports urllib3 connection objects
  created and requests started with precise names; post-request connection/socket
  identity is sampled without removing it from the pool, together with the
  selected peer IP/port, socket family and allowlisted `X-Amz-Cf-Pop`/`X-Cache`
  response headers. Ambiguous observations remain null. Only the authoritative
  persisted RTT still feeds uncertainty and the unchanged 50 ms gate.
- `diagnose-binance-http` takes one passive system DNS snapshot, a bounded number
  of fresh connections and one persistent session. It compares their actual
  peers/POP with `clock_observability.latest` from the isolated runtime status.
  It never injects a resolver result into the collector, overrides an address,
  changes keepalive policy or rotates the runtime session.
- writer telemetry reports exact outstanding/high-water/rejected rows by venue,
  queue residence/admission/write time, child PID/start method/cache age,
  child CPU/RSS/GC/scheduling, active phase, flush latency, and storage stages
  for sort, Arrow, analysis, Parquet write/hash/fsync/publication, directory fsync,
  immediate validation, and SQLite commit. `storage.coalescing` exposes le nombre
  de groupes en attente/prêts et la taille maximale d'un groupe exact.
- `reconnect_reasons_by_generation` distinguishes the initiating socket role,
  collateral closes, exact exception/reason, queue/writer/clock state, and whether
  a reconnect was attempted. The continuity report persists corresponding
  `failure_events_by_capture_generation` evidence.

## Smoke test causal de 10 minutes

Une racine additive propre rend l'audit rapide et laisse le lake historique
intact. Ne supprimez pas cette racine après le test : elle reste un artefact
reproductible. N'exécutez aucun autre collecteur ou inventaire sur cette racine
pendant les dix minutes.

### Linux Singapore VPS

Run this in a fresh shell from the repository root. It uses the existing
`batch_size=500`, the unchanged 10 000/20 000 row capacities and every existing
gate. The bounded HTTP probe runs concurrently but uses its own sessions; it
only observes the current DNS/peer/POP path and never changes the collector.

```bash
set -uo pipefail
PHASE10_SMOKE_DIR="$PWD/data/phase10-singapore-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p -- "$PHASE10_SMOKE_DIR"
printf 'smoke_root=%s\n' "$PHASE10_SMOKE_DIR"
SMOKE_START="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
HYPERLAB_DATA_DIR="$PHASE10_SMOKE_DIR" \
  .venv/bin/python -m hyperlab collect-multi-venue \
    --assets BTC,ETH \
    --candle-intervals 1m \
    --duration-seconds 600 \
    --batch-size 500 \
    --history-lookback-hours 1 \
  >"$PHASE10_SMOKE_DIR/collector.log" 2>&1 &
COLLECTOR_PID="$!"
BINANCE_STATUS="$PHASE10_SMOKE_DIR/runtime_status_binance_usdm.json"
for _ in $(seq 1 30); do
  if [[ -s "$BINANCE_STATUS" ]] && \
      jq -e '.clock_observability.latest.peer_ip? != null' \
        "$BINANCE_STATUS" >/dev/null 2>&1; then
    break
  fi
  kill -0 "$COLLECTOR_PID" 2>/dev/null || break
  sleep 1
done
HYPERLAB_DATA_DIR="$PHASE10_SMOKE_DIR" \
  .venv/bin/python -m hyperlab diagnose-binance-http \
    --persistent-samples 10 \
    --fresh-samples 1 \
    --interval-seconds 1 \
  >"$PHASE10_SMOKE_DIR/binance-http-path.json" \
  2>"$PHASE10_SMOKE_DIR/binance-http-path.stderr.log" &
HTTP_DIAG_PID="$!"
wait "$COLLECTOR_PID"
COLLECTOR_EXIT="$?"
SMOKE_END="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
wait "$HTTP_DIAG_PID"
HTTP_DIAG_EXIT="$?"
printf 'start=%s\nend=%s\n' "$SMOKE_START" "$SMOKE_END" | \
  tee "$PHASE10_SMOKE_DIR/smoke-window.txt"
.venv/bin/python -m hyperlab data continuity \
  "$PHASE10_SMOKE_DIR/lake" \
  --assets BTC,ETH \
  --start "$SMOKE_START" \
  --end "$SMOKE_END" \
  --json | tee "$PHASE10_SMOKE_DIR/phase10-continuity.json"
CONTINUITY_EXIT="${PIPESTATUS[0]}"
printf 'collector_exit=%s\nhttp_diagnostic_exit=%s\ncontinuity_exit=%s\n' \
  "$COLLECTOR_EXIT" "$HTTP_DIAG_EXIT" "$CONTINUITY_EXIT" | \
  tee "$PHASE10_SMOKE_DIR/exit-status.txt"
if (( COLLECTOR_EXIT != 0 )); then
  exit "$COLLECTOR_EXIT"
fi
if (( HTTP_DIAG_EXIT != 0 )); then
  exit "$HTTP_DIAG_EXIT"
fi
exit "$CONTINUITY_EXIT"
```

In a second SSH session, this concise sampler captures Linux CPU/thread, memory,
IO and scheduling evidence without modifying the lake:

```bash
PHASE10_SMOKE_DIR="/absolute/path/printed-by-the-first-command"
COLLECTOR_PID="$(pgrep -n -f '[p]ython.*hyperlab collect-multi-venue')"
WRITER_PID="$(pgrep -P "$COLLECTOR_PID" -f 'multiprocessing-fork' | head -n 1)"
PID_LIST="$COLLECTOR_PID${WRITER_PID:+,$WRITER_PID}"
pidstat -h -u -r -d -w -t -p "$PID_LIST" 1 | \
  tee "$PHASE10_SMOKE_DIR/pidstat.txt"
```

After collection, print the selected DNS/peer/POP path, writer pressure and the
technical decision without dumping every runtime field:

```bash
jq '{dns, comparison,
     fresh: [.fresh_samples[] |
       {outcome, round_trip_latency_ms, drift_uncertainty_ms, peer_ip,
        socket_family, response_cloudfront_pop, response_cache}],
     persistent: [.persistent_samples[] |
       {outcome, round_trip_latency_ms, drift_uncertainty_ms, peer_ip,
        socket_family, response_cloudfront_pop, response_cache}]}' \
  "$PHASE10_SMOKE_DIR/binance-http-path.json"
jq -s '{
  hyperliquid: {state: .[0].metrics.state},
  binance: {state: .[1].state,
            clock_latest: .[1].clock_observability.latest,
            writer: .[1].observability.writer}
}' "$PHASE10_SMOKE_DIR/runtime_status.json" \
   "$PHASE10_SMOKE_DIR/runtime_status_binance_usdm.json"
jq '{technical_capture_gate, failure_reasons, binance_trades,
    connection_lineage, connection_events, clock_sync,
    clock_rejection_policy: {
      rejected_probe_samples: .clock_sync.rejected_probe_samples,
      consecutive_rejection_violations:
        .clock_sync.consecutive_rejection_violations,
      consecutive_rejection_violation_capture_generations:
        .clock_sync.consecutive_rejection_violation_capture_generations,
      consecutive_rejection_outages:
        .clock_sync.consecutive_rejection_outages,
      max_consecutive_rejected_probes:
        .clock_sync.max_consecutive_rejected_probes,
      strict_max_consecutive_rejected_probes:
        .clock_sync.strict_max_consecutive_rejected_probes,
      assessed_capture_generations:
        .clock_sync.assessed_capture_generations,
      market_ready_at_by_capture:
        .clock_sync.market_ready_at_by_capture,
      assessed_span: .clock_sync.assessed_span},
    requested_window,
    strict_phase_10_overlap,
    validation: {relevant_gap_count: .validation.relevant_gap_count}}' \
  "$PHASE10_SMOKE_DIR/phase10-continuity.json"
```

### PowerShell alternative

```powershell
$Phase10SmokeDir = Join-Path (Get-Location) `
  ("data\phase10-singapore-" + [DateTimeOffset]::UtcNow.ToString("yyyyMMddTHHmmssZ"))
New-Item -ItemType Directory -Path $Phase10SmokeDir -ErrorAction Stop | Out-Null
$PreviousDataDir = $env:HYPERLAB_DATA_DIR
$CollectorExit = -1
$SmokeStart = $null
$SmokeEnd = $null
try {
  $env:HYPERLAB_DATA_DIR = $Phase10SmokeDir
  $SmokeStart = [DateTimeOffset]::UtcNow.ToString("o")
  .\.venv\Scripts\python.exe -m hyperlab collect-multi-venue `
    --assets "BTC,ETH" `
    --candle-intervals 1m `
    --duration-seconds 600 `
    --batch-size 500 `
    --history-lookback-hours 1
  $CollectorExit = $LASTEXITCODE
  $SmokeEnd = [DateTimeOffset]::UtcNow.ToString("o")
  [pscustomobject]@{
    root = $Phase10SmokeDir
    start = $SmokeStart
    end = $SmokeEnd
    collector_exit_code = $CollectorExit
  } | ConvertTo-Json | Set-Content `
    (Join-Path $Phase10SmokeDir "smoke-window.json") -Encoding utf8
}
finally {
  if ($null -eq $PreviousDataDir) {
    Remove-Item Env:HYPERLAB_DATA_DIR -ErrorAction SilentlyContinue
  }
  else {
    $env:HYPERLAB_DATA_DIR = $PreviousDataDir
  }
}
if ($CollectorExit -ne 0) {
  throw "Singapore collector failed with exit code $CollectorExit"
}
```

L'audit ciblé rapide relit uniquement cette racine isolée et ses bornes exactes :

```powershell
$Smoke = Get-Content (Join-Path $Phase10SmokeDir "smoke-window.json") `
  -Raw | ConvertFrom-Json
$AuditPath = Join-Path $Phase10SmokeDir "phase10-continuity.json"
.\.venv\Scripts\python.exe -m hyperlab data continuity `
  (Join-Path $Phase10SmokeDir "lake") `
  --assets "BTC,ETH" `
  --start $Smoke.start `
  --end $Smoke.end `
  --json | Tee-Object -FilePath $AuditPath
$AuditExit = $LASTEXITCODE
if ($AuditExit -ne 0) {
  throw "Singapore technical capture gate failed; report: $AuditPath"
}
```

Cette commande n'inventorie que la nouvelle racine. Sur `data\lake`, les bornes
`--start` / `--end` sont exactes mais l'implémentation actuelle inventorie encore
toutes les partitions requises avant de filtrer les lignes ; elle n'est donc pas
un fast path physique pour le lake historique.

Les critères de PASS sont cumulatifs et exacts :

- `technical_capture_gate == "PASS"`, `failure_reasons == []` et le code de
  sortie vaut `0` ; `phase_10_status` reste intentionnellement
  `BLOCKED_PRECONDITION_NOT_MET` ;
- les quatre compteurs `binance_trades` sont égaux et strictement positifs ;
- chaque venue possède exactement une génération éligible, aucune génération
  active invalide ou incomplète, aucune identité de rôle ambiguë, aucun connect
  non lié, aucune reconnexion Hyperliquid multiple et aucune rejection de lignée
  normalisée ;
- tous les compteurs `connection_events` de gap/déconnexion non propre ou non
  lié valent zéro, `validation.relevant_gap_count == 0`,
  `required_wire_lineage.orphan_required_wire_total == 0`,
  `normalized_l2_level_lineage.orphan_level_total == 0` et
  `binance_l2_resync.missing_count == 0` ;
- `clock_sync.valid_v2_samples > 0`,
  `clock_sync.hard_invalid_v2_samples == 0`, les
  compteurs d'échec, de policy/identity rejection, de cadence et de
  discontinuité valent zéro, `market_active_without_valid_clock == []`,
  `clock_sync.coverage_continuous == true`,
  `clock_sync.uncovered_seconds == 0`,
  `clock_sync.strict_max_consecutive_rejected_probes == 1` et
  `clock_sync.consecutive_rejection_violation_capture_generations == []` ;
  `clock_sync.rejected_probe_samples` peut être positif, mais chacune de ces
  lignes reste `INVALID`, ne fournit aucune preuve et n'est tolérée que comme
  rejection isolée couverte ; la limite reste 50 ms. Sur cette racine fraîche,
  le critère pratique exact est l'absence de génération dont l'assessment
  intersecte une `consecutive_rejection_outage`, plus la couverture continue
  sans secondes découvertes. `consecutive_rejection_violations`,
  `max_consecutive_rejected_probes` et `consecutive_rejection_outages` peuvent
  rester non nuls si toute outage historique a récupéré avant l'assessment ;
  `clock_sync.sample_spacing_population` vaut
  `all_persisted_identity_bound_v2_clock_sync_attempts`,
  `clock_sync.sample_spacing_timestamp == "request_sent_time"` et
  `clock_sync.sample_spacing_bounds` vaut
  `active_generation_clipped_to_requested_window`. Une vraie tentative
  manquante garde `clock_sync.causal_coverage_continuous` séparé de la
  cadence, mais
  `clock_sync.sample_spacing_violation_capture_generations != []` fait
  toujours échouer le gate et doit donc valoir `[]` pour un PASS ;
- `requested_window.leading_margin_within_limit == true`,
  `requested_window.trailing_margin_within_limit == true` et
  `requested_window.trailing_terminal_roles_complete == true` ;
- `strict_phase_10_overlap.duration_seconds > 0` et la durée est strictement
  positive séparément pour BTC et ETH.

Chaque observation de trade exactement reliée reste utilisable causalement au
plus 30 secondes après son `received_time`. Ce TTL opérationnel borne la
fraîcheur pour l'audit ; il ne prétend pas inférer une cadence de trades propre à
la venue. Aucun PASS technique ne lance ni ne débloque l'analyse économique de
Phase 10.
## Commande longue destinée à la Phase 10

Après un PASS réel du smoke isolé seulement, la capture de 24 heures conserve le
même `batch_size=500` et les mêmes capacités :

```powershell
.\.venv\Scripts\python.exe -m hyperlab collect-multi-venue `
  --assets "BTC,ETH" `
  --candle-intervals 1m `
  --duration-seconds 86400 `
  --batch-size 500 `
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
