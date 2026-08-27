# HyperLab 0.2.1

Laboratoire **multi-stratégies**, orienté sécurité, pour rechercher, backtester et
faire tourner en paper des stratégies sur données publiques live.

> Le paquet racine `hyperlab` 0.2.1 et l'application Umbrel restent volontairement
> **read-only vis-à-vis des venues**. Ils contiennent un collecteur public, un
> dashboard, un moteur de backtest, des baselines de recherche et un paper engine
> local, sans portefeuille, clé privée, signataire, client privé ou route d'ordre.
> La Phase 13 ajoute uniquement sous `services/testnet-executor` un service
> `0.3.0.dev0` isolé et strictement Hyperliquid Testnet ; il n'est jamais embarqué
> par le paquet racine ou Umbrel.

## Stratégies incluses

1. cash-and-carry spot/perp ;
2. basket de funding ;
3. arbitrage de funding inter-exchanges ;
4. pairs trading / retour à la moyenne ;
5. momentum avec sizing par volatilité ;
6. lead-lag multi-exchange, baseline bar-level ;
7. market making avec replay L2 événementiel de recherche ; la démo synthétique reste `TOY`.

Les résultats de démonstration utilisent des données synthétiques et servent uniquement à vérifier l'installation. La fixture Cash & Carry comprend une fenêtre BTC explicitement étiquetée pour exercer une entrée, les deux jambes, le hedge IOC, le funding, les coûts et une sortie ; elle ne valide aucune rentabilité. Le basket Phase 06 compare ranking inverse-vol et optimisation dollar/bêta BTC/ETH neutre, avec covariance shrinkée, pénalité de turnover et stress dédiés ; ses scénarios synthétiques ne valident pas davantage une rentabilité. La Phase 07 ajoute deux comptes de marge indépendants, transferts, liquidation locale et pannes 1 h/6 h/24 h ; sa démo reste elle aussi strictement synthétique.

## Windows 11

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\bootstrap.ps1
.\.venv\Scripts\python.exe -m hyperlab demo
```

Ouvrez ensuite `reports\demo\comparison.html`.

La recherche Phase 08 dispose désormais d'une sélection de paires train-only,
d'un choix rolling/Kalman/cointégration sur validation, de stops bornés et des
gates de retrait de la meilleure paire et de rupture de corrélation. Voir
[`docs/PAIRS_TRADING_PHASE08.md`](docs/PAIRS_TRADING_PHASE08.md).

Le rapport de démonstration porte le statut visible `SYNTHETIC`. Le moteur Phase 04
sépare cibles et fills, simule profondeur/slippage, non-fills maker, jambes retardées
et IOC d'urgence, puis réconcilie le PnL par composante, actif, mois UTC, régime et
taille. Il inclut un benchmark passif. L'intervalle bootstrap reste explicitement
indisponible pour cette démo in-sample; il n'est publié que sur une série OOS tracée.

## Paper trading Phase 12

Le paper engine persiste une machine à onze états, un journal idempotent chaîné par
hashes, des identifiants SHA-256 déterministes et un ledger réconciliable. Toute
décision passe par les limites de risque avant l'acceptation simulée, puis conserve
la trace décision → ordre → ack/reject → fills partiels ou complets/cancel →
position/cash/frais/PnL. Le replay utilise la configuration figée et son seed ; le
dashboard ne lit qu'un snapshot read-only.

Les frais proviennent d'un artefact **public, versionné et hashé**, jamais d'un
compte ou d'un endpoint privé. Sans preuve publique d'une remise, le palier public
conservateur est retenu. Le statut de promotion économique Phase 12 reste
**`BLOCKED`** : les 42 jours minimum, le nombre suffisant de cycles, les 14 jours
sans incident critique et le résultat positif sous coûts stressés ne sont pas
encore observés. Gate D reste obligatoire avant tout argent réel, mais ne
conditionne plus la préparation technique `PAPER` ou `TESTNET`. Les fixtures
`SYNTHETIC` valident seulement le logiciel. Voir
[`docs/PAPER_ENGINE_PHASE12.md`](docs/PAPER_ENGINE_PHASE12.md).

Le runtime continu et les commandes `paper status|gate|replay|reconcile|run` sont
présents. `paper gate` évalue en lecture seule les diagnostics Gate D internes et
rattache les métriques à une seule tête durable stable, sans override de preuve ou
de seuil. Dans ce checkout, il est volontairement **non autorisant** : aucune preuve
durable ne lie encore le run au runtime/source compilé, et les octets des artefacts
de stress, résilience et couverture ne sont pas revérifiés. Le registre runtime et
le protocole candidat complet restent vides/non implémentés ; aucun vérificateur
sémantique scope-bound n'est compilé. `paper run` échoue fermé avant les factories
et le store. Ce blocage est technique (stratégie + source publique non inscrites),
pas causé par Gates B/C/D. Aucun mode global `paper` et aucune route d'ordre réelle
ne sont ajoutés.

Les classes exactes `RESEARCH_REPLAY`, `PAPER`, `TESTNET`, `MICRO_MAINNET` et
`MAINNET` ainsi que leurs buts non réutilisables sont définis dans
[`docs/RELEASE_GATES.md`](docs/RELEASE_GATES.md). Paper et Testnet peuvent être
préparés sans preuve de rentabilité ; leurs reçus ne peuvent jamais autoriser
du capital réel.

## Exécuteur Testnet Phase 13

Le service séparé `hyperlab-testnet-executor` porte l'identité exacte
`0.3.0.dev0 / TESTNET / TESTNET_EXECUTION`. Il n'accepte que
`https://api.hyperliquid-testnet.xyz` et
`wss://api.hyperliquid-testnet.xyz/ws`, sans endpoint implicite ni fallback
Mainnet. Sa configuration, son credential scope `HYPERLAB_TESTNET`, son reçu, ses
limites et sa base SQLite sont liés avant tout démarrage.

Il fournit une FSM d'ordre persistante, des CLOID déterministes, un outbox durable,
la réconciliation exchange-first, la reprise après crash, les limites de risque,
le dead-man switch, pause/kill et un audit append-only. Les commandes dédiées sont
`hyperlab-testnet build-identity|validate-software|evidence|preflight|status|reconcile|run|pause|kill|smoke-order|cancel`.
`preflight` n'envoie aucun ordre ; le premier `smoke-order` reste une opération
manuelle et exige `--confirm TESTNET-ORDER`.

Aucun install editable ou mélange avec `.venv` racine n'est autorisé pour
l'exploitation. Le service s'installe dans un venv dédié depuis exactement deux
wheels locaux revus (`hyperlab` et `hyperlab-testnet-executor`) avec `--no-deps` ;
le graphe externe et le backend de build viennent des locks hashés
`requirements-external.lock` et `requirements-build.lock`, servis par le
wheelhouse offline fixe. `validate-software` doit réussir dans ce même venv avant
`evidence`. Il est invoqué par ce venv opérateur minimal, mais exécute les gates
Ruff/mypy/pytest/manifest/release avec le Python revu au chemin fixe `.venv` de la
racine ; le rapport lie les deux exécutables et leurs hashes. Chaque commande online
exige ensuite ensemble config, reçu, manifest, racine des preuves, rapport logiciel
et base.

Avant toute commande active, un administrateur doit pré-provisionner pour le SID
Windows exact de l'opérateur le registre non-reparse
`%ProgramData%\HyperLab\TestnetExecutor\control-v1`. La DACL restreinte s'applique
indépendamment aux trois composants projet `HyperLab/TestnetExecutor/control-v1`.
Le service ne les crée pas et n'accepte aucun autre chemin en CLI. Lease, nonce,
rate ledger, send gate et kill latch y restent durables entre bases ; ce registre
ne doit jamais être supprimé pour réinitialiser un run. La commande exacte est
donnée dans le runbook Phase 13.

Les douze valeurs de risque publiées (`1000/500/100`, `5/1`, `4`,
`12/24/6`, `5/10 s`, DMS `30 s`) sont des plafonds compilés autant que des
défauts ; la configuration peut seulement les abaisser. Les décimaux exacts ont des
bornes avant formatage : coefficient de 64 chiffres au plus et valeurs absolues de
l'exposant et de l'exposant ajusté de 64 au plus.

Les identités ordinaires submit/cancel/replace sont des tombstones account-global
permanents, capacité compilée `100000`, sans éviction ni reset online. Au débit
maximal agrégé de `42/min`, arrêter largement avant environ `39,7 h` pour revue
offline. Le `scheduleCancel` protecteur est exempt de cette capacité.

`kill --database <DB> --run-id <RUN_ID> --confirm TESTNET-KILL` persiste d'abord un latch compte
`KILLED` irréversible. La protection venue reste best-effort : sans les cinq
chemins d'autorisation, si un autre runtime détient le lease, ou si
`scheduleCancel` n'est pas confirmé, la commande sort avec le code `3` tout en
conservant le latch. Un dead-man switch armé ne prouve ni l'annulation déjà
effective de chaque ordre, ni la fermeture d'une position.

Aucun workflow live Testnet ni Gate E n'est déclaré validé par le checkout seul.
Aucun chemin micro-mainnet/Mainnet et aucun flag argent réel ne sont ajoutés. La
configuration, le modèle de credentials sans secret embarqué, les états, les
limites et le protocole manuel A–H sont documentés dans
[`docs/TESTNET_EXECUTOR_PHASE13.md`](docs/TESTNET_EXECUTOR_PHASE13.md).

## Données publiques Hyperliquid

```powershell
.\.venv\Scripts\python.exe -m hyperlab snapshot --save
.\.venv\Scripts\python.exe -m hyperlab collect --assets BTC,ETH --duration-seconds 600
```

## Venue de référence publique

```powershell
.\.venv\Scripts\python.exe -m hyperlab collect-multi-venue `
  --assets BTC,ETH `
  --candle-intervals 1m `
  --duration-seconds 600
```

Cette commande lance Hyperliquid et Binance USD-M simultanément avec un unique
writer coordonné sur `data/lake`. Elle conserve BBO, L2, trades, données de
contexte/replay et timestamps de réception, sans clé et sans route de trading.
`collect-reference` reste disponible pour une capture Binance seule, mais ne
doit jamais être lancé en parallèle d'un autre collecteur sur le même lake.

Voir [`docs/MULTI_VENUE_COLLECTION.md`](docs/MULTI_VENUE_COLLECTION.md) pour
l'architecture, la commande longue Phase 10 et les contrôles post-capture, puis
[`docs/EXTERNAL_VENUES.md`](docs/EXTERNAL_VENUES.md) pour les différences de
contrat, mark et index.

## Docker local

```powershell
.\scripts\build_docker.ps1
docker compose up -d
```

Dashboard : `http://127.0.0.1:8000`.

## Umbrel

Le dépôt contient un Community App Store à sa racine (`umbrel-app-store.yml` et
`jjlab-hyperlab/`). Publiez d'abord le tag source contrôlé :

```powershell
.\.venv\Scripts\python.exe scripts\verify_release.py --template --tag v0.2.1 --check-manifest
git tag v0.2.1
git push origin v0.2.1
```

Après réussite de tous les tests, scans pré-publication, attestations, SBOM et signature,
téléchargez le reçu et son bundle Sigstore depuis le run vert du tag exact. Le
préparateur vérifie ces preuves et l'égalité du tag SemVer dans GHCR avant toute
écriture :

Le dépôt doit auparavant protéger `refs/tags/v*`, la branche/le workflow de release et
l'environment `signed-release` avec revue humaine indépendante. Tant que cette
configuration GitHub externe n'est pas prouvée, la publication reste bloquée.

```powershell
.\.venv\Scripts\python.exe scripts\prepare_umbrel_store.py VOTRE_NOM_GITHUB `
  --repository hyperlab --image-version 0.2.1 `
  --image-digest DIGEST_MULTIARCH_64_HEX `
  --release-receipt .\release-evidence\release-receipt.json `
  --receipt-bundle .\release-evidence\release-receipt.sigstore.json
```

Le guide [`docs/UMBREL_SETUP.md`](docs/UMBREL_SETUP.md) couvre installation, health,
backup/restore, update, rollback et désinstallation. Umbrel peut supprimer
`${APP_DATA_DIR}` à l'uninstall : une sauvegarde vérifiée hors de ce répertoire et un
restore-smoke réussi sont obligatoires avant toute suppression.

## Documentation

Commencez par [`docs/GUIDE_COMPLET_FR.md`](docs/GUIDE_COMPLET_FR.md).
Le candidat public/Ghost Polymarket + Kalshi, ses contrats officiels, ses probes
bornés, son bundle raw→replay et son statut économique non prouvé sont décrits
dans [`docs/PREDICTION_MARKETS_CANDIDATE_V1.md`](docs/PREDICTION_MARKETS_CANDIDATE_V1.md).
Son pack prospectif humain, avec collecteurs systemd indépendants, preflight de
cohabitation H1 et cockpit local read-only sur 18081, est documenté dans
[`docs/PREDICTION_MARKETS_PROSPECTIVE_LAUNCH_V1.md`](docs/PREDICTION_MARKETS_PROSPECTIVE_LAUNCH_V1.md).
Le contrat détaillé du collecteur public, ses limites de reprise et le protocole
soak non encore certifié sont dans
[`docs/HYPERLIQUID_COLLECTOR.md`](docs/HYPERLIQUID_COLLECTOR.md). Suivez ensuite les prompts Codex dans `prompts/` dans l'ordre.

Le protocole de recherche verrouillé, le walk-forward, le registre append-only et
les limites de calibration Phase 04 sont décrits dans
[`docs/BACKTEST_PROTOCOL.md`](docs/BACKTEST_PROTOCOL.md). La commande `backtest`
locale exécute le plan hashé, le walk-forward OOS, le registre central et le gel de la
variante. Le test final reste verrouillé sans `--reveal-final`. Elle exige une
exportation point-in-time complète (profondeur, disponibilité, finalité,
lifecycle/tradabilité, hash du lifecycle et métadonnées); elle refuse un ancien panel
de quatre CSV au lieu d'inventer les données manquantes.

Le validateur cash-and-carry Phase 05, ses signaux 8/24/72 h, son stress
d'inversion du funding et sa gate face au benchmark passif sont documentés dans
[`docs/CASH_AND_CARRY_PHASE05.md`](docs/CASH_AND_CARRY_PHASE05.md). Le lake local
reste insuffisant pour une validation économique : le statut actuel est
`BLOCKED_INSUFFICIENT_REAL_DATA`.

La Phase 09 ajoute un validateur directionnel séparé : comparaison momentum
multi-horizons/breakout sur validation, régimes causaux, volatilité cible, stop de
volatilité, limite de corrélation, cooldown après liquidations et exposition
déployable plafonnée à 1×. Le rapport ventile le PnL par régime et vérifie qu'il ne
provient pas uniquement de `trend_up`. Sans historique point-in-time multi-régimes
et modèles calibrés, aucune performance économique n'est revendiquée. Voir
[`docs/MOMENTUM_REGIME_PHASE09.md`](docs/MOMENTUM_REGIME_PHASE09.md).

La Phase 11 ajoute un replay L2 déterministe : fair value multi-venue, microprice,
imbalance, order flow, spread couvrant frais/toxicité, skew et taille d'inventaire,
retrait toxique, file, cancel/replace avec perte de priorité, fills partiels,
markouts 100 ms/1 s/5 s, hedge taker optionnel et traitement fail-closed des gaps
et pannes. Le checkout ne contient pas encore les données et calibrations réelles
requises ; le statut économique reste `BLOCKED_INSUFFICIENT_REAL_DATA` et la démo
historique reste `TOY`. Voir
[`docs/MARKET_MAKING_PHASE11.md`](docs/MARKET_MAKING_PHASE11.md).

Le basket de funding Phase 06, son filtre de squeeze, sa neutralisation, ses stress
de corrélation/squeeze et sa validation leave-one-out avec marchés délistés sont
décrits dans [`docs/FUNDING_BASKET_PHASE06.md`](docs/FUNDING_BASKET_PHASE06.md).
La gate reste fermée sans 90 jours d'univers Hyperliquid point-in-time calibré.

Le funding inter-exchanges Phase 07, ses calendriers Hyperliquid/Binance, ses
marks/oracles distincts, ses marges locales, ses transferts et sa matrice de panne
sont décrits dans
[`docs/CROSS_EXCHANGE_FUNDING_PHASE07.md`](docs/CROSS_EXCHANGE_FUNDING_PHASE07.md).
Le lake local n'offre pas encore trente jours synchronisés sur les deux venues :
statut `BLOCKED_INSUFFICIENT_REAL_DATA`.
