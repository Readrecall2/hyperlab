# Phase 12 — paper trading live

## Objectif

Faire tourner des stratégies figées sur données publiques live avec ordres simulés
réalistes, sans ajouter de capacité de trading réel. La préparation technique
`PAPER` peut commencer sans attendre Gates B/C/D ; la promotion avec argent réel
reste une décision séparée.

## Frontière absolue

- identité exacte `PAPER`, but unique `PAPER_RUNTIME` et
  `authorizes_real_money=false` ;
- paper-only : aucune clé, wallet, signature, authentification privée ou route
  permettant d'envoyer, modifier ou annuler un ordre ;
- ne jamais importer ou instancier `hyperliquid.exchange.Exchange` ;
- acknowledgements, rejets, cancels, fills et `EMERGENCY_FLATTEN` sont simulés
  localement ;
- le dashboard reste read-only et ne possède aucun endpoint de commande ;
- un reçu Paper ne peut autoriser ni Testnet ni Mainnet ;
- aucun passage automatique à la Phase 13.

## Machine à états

`FLAT`, `ENTRY_PLANNED`, `LEG_1_PENDING`, `HEDGE_PENDING`, `HEDGED`,
`EXIT_PLANNED`, `EXIT_PENDING`, `PAUSED`, `REDUCE_ONLY`, `MANUAL_REVIEW`,
`EMERGENCY_FLATTEN`.

Définir une table explicite des transitions autorisées et des invariants. Un fill
partiel conserve son reliquat ; une transition de sécurité ne peut jamais augmenter
l'exposition ; une réduction d'urgence simulée peut échouer ou rester partielle.

## Préparation technique `PAPER`

Le reçu exact `PAPER` / `PAPER_RUNTIME` exige : source publique et schéma
`MarketEvent` valides, stratégie/configuration/source/risque figés et versionnés, modèles de coûts
et fills conservateurs clairement étiquetés, comptabilité déterministe, journal
append-only, reprise, replay, réconciliation et absence structurelle de transport
d'ordre. Une identité absente ou ambiguë échoue fermée.

Elle ne requiert ni PASS Gate B/C, ni rentabilité, ni 42 jours Gate D. Les runs
`SYNTHETIC` ou `UNCALIBRATED` restent explicitement non-promouvables, mais peuvent
tester le logiciel et exercer l'instrumentation des futures preuves. Leur temps et
leurs cycles ne créditent jamais Gate D. Gates B/C continuent en
parallèle.

## Exigences d'intégrité

- état SQLite persistant, journal append-only chaîné par hashes et transactions
  atomiques ;
- événements idempotents, avec collision même-ID/contenu-différent fail-closed ;
- identifiants SHA-256 déterministes sur sérialisation canonique ;
- configuration immuable couvrant stratégie, paramètres, risque, seed, versions,
  source et hashes d'artefacts ; toute modification crée un nouveau run ;
- une seule voie stratégie → contrôle de risque → paper engine ;
- risque appliqué avant chaque acceptation simulée ;
- spread, profondeur, slippage, non-fills, fills partiels, IOC, timeouts, délais
  entre jambes, frais et coûts simulés sans hypothèse de fill maker ;
- coûts/fills `CALIBRATED`, `UNCALIBRATED` ou `SYNTHETIC` toujours visibles ;
- frais issus d'un artefact public versionné/hashé, jamais d'un compte privé ;
- traçabilité décision → ordre simulé → ack/reject → fill/cancel → ledger ;
- incohérence = `MANUAL_REVIEW`, blocage des entrées et aucune correction implicite ;
- replay exact et redémarrage fail-closed avant toute nouvelle décision ;
- snapshot JSON atomique, dashboard read-only et alertes persistées ;
- tests de crash, restart, fill partiel, hedge/cancel en attente, duplication,
  troncature et projection obsolète.

## Gate D — preuve pour argent réel

Gate D reste cumulative : Gates B/C satisfaites, coûts/latence/fills calibrés,
minimum 42 jours forward, seuil préenregistré d'au moins 30 cycles, 14 jours
consécutifs
sans incident critique, exercices de résilience, replay/réconciliation exacts et
résultat net positif sous coûts stressés sans masquer les runs perdants.

Gate D ne sert pas à démarrer Paper ou Testnet. Elle reste obligatoire, avec revue
humaine, avant toute autorisation `MICRO_MAINNET` ou `MAINNET`.

## Validation technique

Exécuter `ruff check .`, `mypy src/hyperlab` et `pytest`, puis rechercher l'absence
de secret, signer, client privé et route d'ordre réel. La réussite des tests ne vaut
pas validation économique.
