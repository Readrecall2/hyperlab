# Phase 11 — market making adaptatif et replay L2

## Statut

La Phase 11 ajoute un simulateur **event-by-event**, déterministe et strictement
hors ligne. Il reconstruit un carnet L2 à partir de snapshots et de deltas,
consomme les trades selon leur timestamp de réception et simule des quotes
locales. Il ne contient ni client d'exécution, ni clé, ni signature, ni envoi,
modification ou annulation d'ordre réel.

La gate économique reste `BLOCKED_INSUFFICIENT_REAL_DATA` dans ce checkout : le
dossier `data` ne contient aucun replay réel multi-venue, et aucune calibration
auditée des frais, latences, files ou markouts. La démo
`InventoryAwareMarketMaker` reste donc explicitement étiquetée `TOY`. Le nouveau
moteur porte l'étiquette `EVENT_REPLAY_RESEARCH_ONLY`, même lorsque ses preuves de
calibration sont présentes. Aucun de ces statuts n'autorise le testnet.

## Contrat événementiel et causalité

Le moteur accepte uniquement les enregistrements publics normalisés déjà définis
par le lake :

- `l2_snapshot`, groupé atomiquement par `snapshot_id` ;
- `l2_delta`, groupé atomiquement par `update_id` ;
- `trade`, avec agresseur, prix et quantité ;
- `connection_event`, pour gap, coupure et resynchronisation.

L'ordre causal est `received_time`, puis l'ordre de capture en cas d'égalité. Le
temps exchange reste une mesure, jamais une permission de réordonner le flux. Une
quote calculée ou activée à un timestamp ne peut pas être remplie par un trade au
même timestamp. Les snapshots remplacent atomiquement le carnet ; les deltas
exigent une séquence strictement croissante et contiguë. Un gap suspend le carnet
et retire les quotes jusqu'à un snapshot et un `resync_complete` explicites.
L'audit rapproche chaque `l2_book_state` des niveaux du même `snapshot_id` et
refuse un nombre de bids ou d'asks incomplet.

Hyperliquid ne fournit actuellement pas de numéro de séquence public dans les
snapshots `l2Book` collectés. Le replay peut les lire, mais la gate Phase 11 échoue
alors avec `BLOCKED_SEQUENCE_UNOBSERVABLE` au lieu de prétendre que la couverture
est complète. La venue de référence Binance conserve son `last_sequence`, sans
transformer un top-20 périodique en flux order-by-order.

## Fair value, quotes et toxicité

Pour chaque venue synchronisée et fraîche, le moteur calcule :

```text
imbalance  = (bid_qty - ask_qty) / (bid_qty + ask_qty)
microprice = (ask * bid_qty + bid * ask_qty) / (bid_qty + ask_qty)
fair value = moyenne pondérée des microprices multi-venues fraîches
```

Le flux signé des trades agresseurs alimente un EWMA causal. Sa valeur absolue
forme la mesure de toxicité utilisée pour :

- élargir le demi-spread ;
- réduire la taille ;
- retirer les deux quotes au-delà de la limite ;
- refuser de coter sur un carnet stale ou non synchronisé.

Le demi-spread minimal couvre au moins le paramètre de frais maker et la réserve
de toxicité. La fair value est décalée contre l'inventaire ; chaque côté est borné
par la capacité restante jusqu'à l'inventaire maximal. Les quotes restent
passives : le bid ne dépasse pas le meilleur bid observé et l'ask ne passe pas
sous le meilleur ask.

## File, cancel/replace et fills

La position initiale dans la file est une fraction préenregistrée de la quantité
L2 affichée au même prix. Les trades au prix de la quote consomment d'abord cette
quantité devant nous. Seul le reliquat peut remplir la quote, éventuellement en
plusieurs fills partiels. Il n'existe aucun tirage aléatoire ni fill maker
automatique.

Un cancel reste exposé jusqu'à `cancel_latency_ms`. Un remplacement n'est posé
qu'après l'annulation simulée et repart derrière la quantité alors affichée : sa
priorité antérieure est perdue. Si la connexion tombe, les quotes encore ouvertes
sont comptées comme abandonnées et aucun fill fantôme n'est inventé. Le statut
reste `BLOCKED_UNRECONCILED_QUOTES` tant qu'une resynchronisation explicite n'a pas
rendu l'état de nouveau connu.

Si un nouveau carnet traverse une quote active sans trade public préalable, le
moteur ne suppose pas un fill maker : il marque un trade-through non résolu et
bloque la cotation. Si le mouvement arrive avant l'activation due à la latence, la
quote est comptée comme rejet post-only simulé, jamais comme fill.

Un hedge est optionnel. Lorsqu'il est configuré, il est toujours exécuté comme
taker simulé au meilleur prix de la seconde venue, avec ses frais et son PnL
d'exécution séparés. L'absence de carnet frais empêche le hedge ; elle ne fabrique
pas un prix.

## Validation et attribution

Chaque fill maker conserve sa fair value contemporaine et calcule au premier
événement futur disponible :

```text
spread capture     = sens * (fair_value_fill - prix_fill) * quantité
markout(h)          = sens * (fair_value_t+h - prix_fill) * quantité
adverse selection  = markout(h) - spread capture
```

Les horizons normatifs sont 100 ms, 1 s et 5 s. Le rapport JSON/HTML inclut aussi :

- inventaire absolu maximal ;
- taux maker/taker ;
- fill ratio en unités remplies / unités cotées ;
- cancel-to-fill ;
- fills partiels ;
- spread minimal et tailles min/max effectivement soumis ;
- retraits toxiques ;
- hedges et PnL de hedge ;
- gaps, resynchronisations et pannes ;
- pertes marquées pendant les mouvements dépassant le seuil de spike ;
- quotes abandonnées et état de réconciliation final ;
- trade-throughs non résolus et rejets post-only simulés.

Le fichier `market_making_summary.json` contient le hash de la configuration et les
hashes des manifestes du lake via l'audit. Les résultats négatifs ne sont ni
plafonnés ni masqués.

## Gates fail-closed

`market-making-audit` exige simultanément :

- assez d'événements ;
- snapshots L2 et trades sur la venue cible ;
- en-têtes `l2_book_state` cohérents avec tous les niveaux de chaque snapshot ;
- L2 sur au moins une venue de référence ;
- timestamps de réception UTC ordonnés, sans égalité inter-frame ambiguë ;
- séquences L2 cibles observables ;
- aucune coupure ou gap déclaré ;
- au moins une resynchronisation observable ;
- un hash SHA-256 de preuve de calibration.

Le replay ajoute ses propres blocages : quotes non réconciliées, gap rencontré,
séquence inobservable, fair value mono-venue ou paramètres non calibrés. Une
étiquette `SYNTHETIC`, `TOY`, `UNVERIFIED`, `DEFAULT` ou `PLACEHOLDER` ne peut pas
être déclarée `CALIBRATED`.

## Commandes

```powershell
.\.venv\Scripts\python.exe -m hyperlab market-making-audit `
  --data data\lake `
  --asset BTC `
  --target-venue hyperliquid `
  --reference-venues binance_usdm `
  --calibration-evidence-hash VOTRE_SHA256 `
  --output reports\market-making-readiness.json

.\.venv\Scripts\python.exe -m hyperlab market-making-replay `
  --data data\lake `
  --asset BTC `
  --target-venue hyperliquid `
  --reference-venues binance_usdm `
  --calibration-evidence-hash VOTRE_SHA256 `
  --output reports\market-making
```

Sans preuve, séquences et historique réel suffisant, ces commandes doivent sortir
avec un statut bloqué. C'est le comportement attendu ; il est interdit de remplacer
une donnée manquante par un résultat synthétique présenté comme réel.

## Limites restantes avant paper/testnet

- Le L2 agrégé n'identifie ni notre ordre ni la liquidité cachée.
- Les rejets privés, acknowledgements et règles précises de self-trade prevention
  ne sont pas observables dans un flux public.
- Le modèle de queue doit être calibré par régime, venue, actif, niveau et latence.
- Les frais réels du compte et la latence aller/retour doivent être gelés sur train,
  validés hors échantillon, puis testés sur une fenêtre finale jamais utilisée pour
  régler les paramètres.
- La Phase 12 devra réconcilier un paper engine persistant ; la Phase 13 restera une
  branche/version distincte avec revue humaine explicite.
