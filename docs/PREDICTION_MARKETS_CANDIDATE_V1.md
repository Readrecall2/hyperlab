# HyperLab Prediction Markets Candidate V1

Verdict technique du jalon :
`PREDICTION_MARKETS_CANDIDATE_V1_TECHNICALLY_READY_FOR_PROSPECTIVE_EVIDENCE`.

Verdict économique : `ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE`.

Sous-verdicts d'accès observés le 27 août 2026 depuis le poste Windows local :

- Polymarket : `PUBLIC_SOURCE_UNAVAILABLE` dans l'unique probe borné, échec DNS
  avant la première frame ;
- Kalshi : `PUBLIC_SOURCE_UNAVAILABLE` dans l'unique probe borné, échec DNS avant
  la première frame.

Ces sous-verdicts décrivent seulement ce chemin réseau et cet instant. Ils ne
prétendent pas que les APIs officielles sont globalement indisponibles. Le code
offline, les contrats et les tests restent utilisables ; aucune donnée ni aucun
hash raw n'a été inventé pour compenser l'absence de frames.

## Frontière absolue

Tout le jalon est `PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY` :

- aucun wallet, signer, seed, secret, compte privé ou API privée ;
- aucun ordre, amendement, annulation, route `live`, `trade` ou `mainnet` ;
- aucun import ou instanciation de `hyperliquid.exchange.Exchange` ;
- aucun proxy, contournement de 403, géoblocage ou contrôle d'accès ;
- aucun accès SSH, VPS, systemd ou campagne H1 ;
- toute fixture est marquée `SYNTHETIC/FIXTURE` et n'est jamais une preuve
  économique.

Le candidat n'ajoute donc aucune capacité d'exécution. Il ajoute une chaîne
reproductible de collecte publique, reconstruction point-in-time, replay Ghost
et évaluation scellée.

## Quatre classes de preuve

Les deux contrats versionnés utilisent exclusivement :

| Classe | Signification |
|---|---|
| `DOCUMENTED` | Décrit par une source officielle, sans preuve d'accès local. |
| `OBSERVED_PUBLICLY` | Reçu sans credential et conservé dans un raw authentifié. |
| `INFERRED` | Déduction explicite et non promue en fait observé. |
| `UNKNOWN_NOT_OBSERVED` | Absent, ambigu ou non vérifié ; fermeture fail-closed. |

Les contrats sont :

- `config/research/polymarket-public-contract-v1.json`, SHA-256 logique
  `9f955ec3586d9724f4afaac911eac88f6ba4a58a4f175343a59d783387f9e7a6` ;
- `config/research/kalshi-public-contract-v1.json`, SHA-256 logique
  `68380ceaceefbd2f1bfd2998eb07a56d92c54329b9579371998e434d33c7b8fe`.

Ils figent endpoints publics, pagination, identités, timestamps, précision,
ticks, états, books, trades, résolution et frais observables. Une documentation
ne change jamais automatiquement `UNKNOWN_NOT_OBSERVED` en
`OBSERVED_PUBLICLY`.

## Sources officielles consultées

Polymarket :

- https://docs.polymarket.com/llms.txt
- https://docs.polymarket.com/getting-started/api
- https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination
- https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination
- https://docs.polymarket.com/market-data/overview
- https://docs.polymarket.com/api-reference/wss/market
- https://docs.polymarket.com/trading/fees
- https://docs.polymarket.com/concepts/resolution
- https://docs.polymarket.com/concepts/negative-risk
- https://docs.polymarket.com/api-reference/rate-limits

Kalshi :

- https://docs.kalshi.com/llms.txt
- https://docs.kalshi.com/getting_started/quick_start_market_data
- https://docs.kalshi.com/getting_started/pagination
- https://docs.kalshi.com/api-reference/market/get-markets
- https://docs.kalshi.com/api-reference/market/get-market-orderbook
- https://docs.kalshi.com/api-reference/market/get-trades
- https://docs.kalshi.com/api-reference/historical/get-historical-trades
- https://docs.kalshi.com/getting_started/fixed_point_migration
- https://docs.kalshi.com/getting_started/market_lifecycle
- https://docs.kalshi.com/getting_started/market_settlement
- https://docs.kalshi.com/getting_started/fee_rounding
- https://docs.kalshi.com/getting_started/rate_limits
- https://kalshi.com/docs/kalshi-fee-schedule.pdf, version effective
  2026-07-07.

La liste exhaustive et les champs figés sont dans chaque contrat JSON.

## Preuves d'accès bornées

Une seule tentative effective a été faite par venue, en direct, sans retry et
avec 120 secondes, 5 marchés de census, 500 frames et 16 MiB au maximum.

| Venue | Appels | Durée | Frames/segments/octets | Résultat | SHA-256 `result.json` |
|---|---:|---:|---:|---|---|
| Polymarket | 1 | 11 235 ms | 0 / 0 / 0 | DNS de `gamma-api.polymarket.com` non résolu | `4d9b15d44fd99ae1cf13fd032350e220b5dc477cfc42acdad83550a299751c5a` |
| Kalshi | 1 | 11 139 ms | 0 / 0 / 0 | DNS de `external-api.kalshi.com` non résolu | `ad4ede07a268ee372127a52ba0ad406aeb8b552d5cac0a4a732a4f296736bcf7` |

Les deux résultats portent `manifest_sha256=null` et `root_sha256=null`. C'est
la seule représentation valide lorsque zéro frame a été admise. Les preuves
sont conservées sous `docs/evidence/prediction-markets-candidate-v1/`.

Le bundle d'accès reconstruit uniquement depuis ces deux reçus et les entrées
gelées est conservé sous
`docs/evidence/prediction-markets-candidate-v1/access-bundle-v1/`, SHA-256
`965a42f2169c16201323477c0eb1ba7a8b540b24109c1d9252d5d9fcce55bbe5`.
Il a été revérifié offline avec ce pin exact. C'est un inventaire d'accès sans
raw frame, dataset ou replay ; il ne vaut ni reçu de campagne ni preuve alpha.

## Data Plane commun

Les adaptateurs Polymarket et Kalshi complètent le Research Data Plane existant.
Ils ne créent pas un second stockage :

1. chaque payload reçu devient un `PublicDataEnvelope` avec identité venue,
   source, collection/session/collector, `source_time` lorsqu'il est
   interprétable, UTC local et horloge monotone locale ;
2. le writer publie des segments append-only et une chaîne de manifests
   authentifiée ;
3. pagination, cursors, limites appels/frames/octets/segments, déduplication,
   gaps, reconnexions et arrêt terminal restent bornés ;
4. `PredictionRawEvidenceIndex` revalide host, chemin officiel, version de
   métadonnées, record et hash avant toute projection ;
5. les graphes event→market→outcome/token/contract sont reconstruits selon la
   position raw exacte `(arrival_sequence, raw_record_index)`. Après gap ou
   reconnexion, une observation n'est réadmise que lorsque toutes ses références
   ont été réauthentifiées dans le même domaine collector/session ; chaque ligne
   porte le SHA-256 de l'observation de graphe réellement utilisée. Un changement
   silencieux de règle ou d'identité est refusé. Après une reconnexion Polymarket,
   Gamma, CLOB, events, frais et ticks sélectionnés sont recapturés avant la
   souscription WebSocket ; un bootstrap partiel, tardif ou interrompu ne peut
   admettre aucun nouveau book ;
6. books et trades produisent des datasets point-in-time séparés. Une preuve
   future ne peut pas réparer rétroactivement une ancienne ligne.

Les shards publics sont non chevauchants et ordonnés par ordinal planifié, puis
par manifeste raw enfant et venue. Leur identité recalcule manifeste de
campagne, venue, ordinal et début du créneau ; toute frame positive doit avoir
son UTC local reçu dans ce créneau authentifié. Plusieurs shards d'une venue
restent plusieurs autorités enfants. Le bundle ne les écrase pas en une vue
« dernière venue ».

Chaque créneau prospectif antérieur au cutoff scellé train+validation appartient
exactement à une des quatre classes hashées dans le bundle : raw rejouable, raw
positif non rejouable, reçu terminal explicitement exclu, ou créneau manquant.
Un raw positif arrêté fail-closed reste copié et authentifié, mais n'est jamais
projeté dans l'économie. `schedule_accounted=true` signifie seulement qu'aucun
créneau n'est silencieusement absent ; il ne vaut pas corpus économique complet.
`economic_corpus_complete=true` exige exclusivement du raw rejouable, sans
créneau manquant, exclu ou non rejouable. Le holdout reste hors de ce ledger et
scellé.

## Bundle brut → replay → évaluation

`Prediction Research Bundle V1` copie les segments et toute la chaîne de
manifests, lie contrats/candidat/campagne et matérialise :

- couverture exhaustive de chaque record raw ;
- timelines de graphes, frais, ticks et règlements ;
- datasets depth/trades et catalogue sémantique ;
- statut calculé par venue ;
- seal de replay, cutoff de preuve et inventaire hashé des dérivés.

`prediction-bundle-verify` reconstruit tous ces dérivés depuis le raw et compare
les octets. Il refuse aussi un descripteur, une racine raw, un reçu terminal, un
ordre de shards ou un statut de venue falsifié, même si l'attaquant recalcule
l'auto-hash du manifeste.

`ghost prediction-replay --bundle-root ...` effectue cette vérification avant
le replay. `ghost prediction-evaluate` recommence la reconstruction et exige
que le rapport fourni soit byte-identical au replay en mémoire. L'utilisateur
ne peut plus injecter un statut source ou une liste de rapports parsés non
vérifiés.

## Réalisme Ghost prédictif

Le moteur commun conserve des prix dans `[0,1]`, ticks exacts, profondeur finie,
latence, fraîcheur, partial/missed fills, queue pessimiste et interdiction de
remplir un maker au simple contact. Il ajoute :

- payout binaire et capital immobilisé jusqu'à sortie/résolution ;
- états trading, closed, disputed, finalized/settled et exposition non résolue ;
- frais Polymarket seulement si Gamma, CLOB et chaque token convergent ;
- frais Kalshi `UNKNOWN` pour l'économie primaire tant que précision de compte,
  overrides effectifs et accumulation d'arrondi ne sont pas observés ensemble ;
- règlement lié à un record raw, une version de règle, les deux horloges et un
  payout exact ; Kalshi exige en plus `yes→1`, `no→0` et `settlement_ts` RFC3339 ;
- PnL brut/net, fees, spread, slippage, turnover, drawdown, exposition, capital
  immobilisé et attribution venue/market/outcome.

Un signal reçu à `t` ne peut agir qu'après `t`; toute action au même timestamp
est refusée. L'ordre de campagne suit le shard puis la position raw, jamais une
horloge UTC susceptible de régresser. Les siblings YES/NO atomiques Kalshi ne
produisent qu'une décision K4 par variante et état causal ; une divergence de
décision pour le même état arrête le replay fail-closed.

## Candidat fingerprint n°2

La préinscription canonique est
`config/research/prediction-markets-candidate-v1.json`, SHA-256
`aa60c0ff0ef95813d79f56b6ea93a31952061b562905dc9729162f7b16e41964`.

Elle conserve toutes les variantes :

- K4 `COMPLETE_SET_LOGICAL_RV`, famille technique principale mais pas sélection
  économique ;
- K5 `INCENTIVE_QUEUE`, contrôle négatif avec reward primaire nul ;
- K6 `CROSS_VENUE_EQUIVALENCE`, exploratoire et admise seulement après relation
  humaine formelle, jamais par similarité de titre.

Le statut est `READY_FOR_PROSPECTIVE_EVIDENCE`, sans fingerprint économique
sélectionné. Train 14 jours, validation 7 jours et test final 7 jours sont
chronologiques. Le holdout reste `SEALED`, sans métriques exposées. Les LCB de
Cantelli avec correction Bonferroni couvrent toutes les variantes, perdantes
incluses. Les minima futurs sont 100 observations par variante et 20 marchés ;
ils ne sont pas satisfaits par des fixtures ou les probes d'accès.

## Campagne future non lancée

Le pack sous
`ops/prediction_markets_candidate_v1/prediction-markets-v1-20260901t000000z-aa60c0ff/`
porte :

- `campaign_id=prediction-markets-v1-20260901t000000z-aa60c0ff` ;
- manifeste logique `c9eb654d077ba1c3cf4cf709c2633077f40fc55d5f9533ce82d498564cd66388` ;
- 672 créneaux par venue, cadence une heure, collecte 120 s ;
- `STRICT_NON_OVERLAP`, `RECORD_GAP_NO_BACKFILL` et aucun retry après résultat
  terminal ;
- identité de shard liée au manifeste, venue, ordinal et début planifié ;
- `AWAITING_HUMAN_EXECUTION`, `vps_or_h1_path=NONE`.

Ce pack est du texte opérateur local. Il n'a pas été lancé et ne doit jamais
viser le chemin H1 actif.

## CLI locale

Préparer une nouvelle campagne, sans réseau :

```powershell
& '.\.venv\Scripts\python.exe' -m hyperlab research-data prediction-prepare `
  --output-root 'D:\hyperlab-evidence\prediction-markets-v1' `
  --campaign-id 'prediction-markets-v1-unique' `
  --starts-at-utc '2026-09-01T00:00:00Z'
```

Après des collectes humaines terminalisées, construire puis vérifier le bundle :

```powershell
& '.\.venv\Scripts\python.exe' -m hyperlab research-data prediction-bundle-build `
  --output-root 'D:\hyperlab-evidence\prediction-bundle-v1' `
  --campaign-manifest 'D:\hyperlab-evidence\prediction-markets-v1\campaign-manifest.json' `
  --collection-roots 'D:\...\pm-shard-0000,D:\...\kalshi-shard-0000' `
  --unavailable-roots 'D:\...\pm-terminal-excluded,D:\...\kalshi-terminal-excluded'

$bundleSha256 = '<BUNDLE_SHA256_RETOURNE_PAR_LA_CONSTRUCTION>'

& '.\.venv\Scripts\python.exe' -m hyperlab research-data prediction-bundle-verify `
  --bundle-root 'D:\hyperlab-evidence\prediction-bundle-v1' `
  --expected-bundle-sha256 $bundleSha256
```

Malgré son nom historique conservé pour compatibilité, `--unavailable-roots`
accepte les reçus terminaux exclus authentifiés, qu'ils aient zéro frame ou un
raw positif arrêté fail-closed. Les sorties build/verify exposent le ledger
`prospective_slot_coverage` ; l'opérateur doit inspecter ses classes et ne doit
jamais assimiler `schedule_accounted` à `economic_corpus_complete`.

Replay et évaluation offline :

```powershell
& '.\.venv\Scripts\python.exe' -m hyperlab ghost prediction-replay `
  --bundle-root 'D:\hyperlab-evidence\prediction-bundle-v1' `
  --expected-bundle-sha256 $bundleSha256 `
  --output 'D:\hyperlab-evidence\prediction-replay-v1.json'

& '.\.venv\Scripts\python.exe' -m hyperlab ghost prediction-evaluate `
  --bundle-root 'D:\hyperlab-evidence\prediction-bundle-v1' `
  --expected-bundle-sha256 $bundleSha256 `
  --campaign-replay 'D:\hyperlab-evidence\prediction-replay-v1.json' `
  --output 'D:\hyperlab-evidence\prediction-evaluation-v1.json'
```

Chaque sortie doit être neuve ou byte-identical. Aucun bloc de cette page ne
contient une commande VPS.

## Limites et critères futurs

- Il n'existe encore aucun corpus public réel dans ce jalon : statut source
  actuel `PUBLIC_SOURCE_UNAVAILABLE`, puis `INSUFFICIENT_PUBLIC_CORPUS` si
  l'accès revient mais que les minima ne sont pas atteints. Le statut économique
  reste `ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE` dans les deux cas.
- Les probes courts qualifient l'accès, jamais l'alpha, la capacité ou la
  rentabilité.
- Les probes actuels sont des observations d'accès non liées à une campagne,
  pas des reçus de shard. Le schéma zéro-frame V1 authentifie exactement
  identité, plan, budgets et résultat terminal, mais sans frame ni champ
  `started_at` il ne peut pas prouver indépendamment l'heure d'exécution réelle.
  Cette limite est technique et ne devient jamais une preuve économique.
- Zéro gap observé ne prouve pas la continuité lorsque la venue ne publie pas de
  séquence exploitable.
- Les issues negative-risk, multi-outcome, void/cancelled et scalar restent
  fail-closed sans contrat explicite complet.
- Un futur GO reste `GHOST_ONLY/PAPER_ONLY`, nécessite corpus public non
  synthétique, coûts exacts, résolutions complètes, réconciliation exacte,
  validation OOS suffisante et revue humaine. Il ne créerait toujours aucune
  route d'ordre réel.
