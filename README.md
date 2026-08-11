# HyperLab 0.2.0

Laboratoire **multi-stratégies**, orienté sécurité, pour rechercher et backtester des stratégies sur Hyperliquid.

> Cette version est volontairement **read-only**. Elle contient un collecteur public, un dashboard, un moteur de backtest et des baselines de recherche. Elle ne contient ni portefeuille, ni clé privée, ni signataire, ni exécuteur d'ordres.

## Stratégies incluses

1. cash-and-carry spot/perp ;
2. basket de funding ;
3. arbitrage de funding inter-exchanges ;
4. pairs trading / retour à la moyenne ;
5. momentum avec sizing par volatilité ;
6. lead-lag multi-exchange, baseline bar-level ;
7. market making avec inventaire, simulateur microstructurel simplifié.

Les résultats de démonstration utilisent des données synthétiques et servent uniquement à vérifier l'installation.

## Windows 11

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\bootstrap.ps1
.\.venv\Scripts\python.exe -m hyperlab demo
```

Ouvrez ensuite `reports\demo\comparison.html`.

## Données publiques Hyperliquid

```powershell
.\.venv\Scripts\python.exe -m hyperlab snapshot --save
.\.venv\Scripts\python.exe -m hyperlab collect --interval-seconds 60 --samples 10
```

## Docker local

```powershell
.\scripts\build_docker.ps1
docker compose up -d
```

Dashboard : `http://127.0.0.1:8000`.

## Umbrel

Le dépôt contient un Community App Store à sa racine (`umbrel-app-store.yml` et `jjlab-hyperlab/`). Après avoir créé un dépôt GitHub public nommé `hyperlab` :

```powershell
.\.venv\Scripts\python.exe scripts\prepare_umbrel_store.py VOTRE_NOM_GITHUB
git tag v0.2.0
git push origin v0.2.0
```

Le workflow GitHub publie une image `amd64`/`arm64`. Le guide explique ensuite comment ajouter l'URL du dépôt dans **App Store → Community App Stores**.

## Documentation

Commencez par [`docs/GUIDE_COMPLET_FR.md`](docs/GUIDE_COMPLET_FR.md), puis suivez les prompts Codex dans `prompts/` dans l'ordre.
