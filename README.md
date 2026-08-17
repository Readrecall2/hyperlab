# HyperLab 0.2.1

Laboratoire **multi-stratégies**, orienté sécurité, pour rechercher, backtester et
faire tourner en paper des stratégies sur données publiques live.

> Cette version est volontairement **read-only vis-à-vis des venues**. Elle contient
> un collecteur public, un dashboard, un moteur de backtest, des baselines de
> recherche et un paper engine local. Elle ne contient ni portefeuille, ni clé
> privée, ni signataire, ni client privé, ni route d'ordre réel. Tous les ordres,
> acknowledgements, rejets, fills et cancels Phase 12 sont simulés.

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
