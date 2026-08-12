# HyperLab — guide complet Windows 11 + Umbrel + Codex

**Version : 0.2.0 — 11 août 2026**

Ce guide remplace le précédent tutoriel centré presque uniquement sur le cash-and-carry. HyperLab devient un **laboratoire multi-stratégies** : il commence par les approches les plus défensives, puis permet de tester des stratégies plus ambitieuses, sans plafonner les résultats et sans confondre un beau backtest avec une preuve de rentabilité.

---

## 1. Ce que le pack fait aujourd'hui

Le dépôt livré contient :

- un moteur de backtest portefeuille causal ;
- des coûts configurables ;
- des limites d'exposition ;
- six baselines bar-level ;
- un simulateur simplifié de market making ;
- un générateur synthétique déterministe pour vérifier l'installation ;
- un lecteur public Hyperliquid spot/perp ;
- un stockage SQLite ;
- un dashboard local ;
- Docker et un package Community App Store Umbrel ;
- des tests ;
- une CI GitHub ;
- seize prompts Codex séquentiels ;
- des portes de validation jusqu'au futur micro-mainnet.

### Ce qu'il ne fait pas encore

- aucun ordre ;
- aucune clé ;
- aucun wallet ;
- aucun paper engine événementiel complet ;
- aucun replay L2 réel ;
- aucune preuve qu'une stratégie est rentable ;
- aucun passage automatique vers testnet ou mainnet.

C'est volontaire. La première version doit construire une base que l'on peut auditer, pas une boîte noire autorisée à risquer de l'argent.

---

## 2. Architecture retenue

```text
┌──────────────────────────────────────────────────────────────────┐
│ WINDOWS 11                                                       │
│                                                                  │
│ Codex + Git + Python + tests + backtests + rapports + Docker     │
│ Aucun secret                                                     │
└───────────────────────────────┬──────────────────────────────────┘
                                │ code revu / image versionnée
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│ MINI-PC UMBREL 24/24                                             │
│                                                                  │
│ Collecteur public → SQLite/Parquet → dashboard local             │
│ Aucun secret, aucun ordre                                        │
└───────────────────────────────┬──────────────────────────────────┘
                                │ export des données
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│ RECHERCHE                                                        │
│                                                                  │
│ Backtests → stress tests → forward paper → testnet → micro-live  │
└──────────────────────────────────────────────────────────────────┘
```

### Pourquoi les deux machines

**Windows 11** est plus confortable pour Codex, Git, les rapports, les notebooks et les backtests. Il évite aussi de développer directement sur un serveur qui héberge peut-être ton nœud Bitcoin.

**Umbrel** est idéal pour la collecte publique 24/24 et le paper trading lent. Il accumule les données pendant que le code évolue sur Windows.

Pour une stratégie sub-seconde, le mini-PC domestique pourra servir de laboratoire, mais le live nécessitera probablement une machine dédiée et une meilleure latence.

---

## 3. Les sept stratégies

| Niveau | Stratégie | Exposition | Complexité | État du pack |
|---|---|---|---|---|
| 1 | Cash-and-carry spot/perp | Presque neutre | Moyenne | Baseline incluse |
| 2 | Basket de funding | Long/short relatif | Moyenne | Baseline incluse |
| 2 | Funding inter-exchanges | Même actif, deux venues | Élevée | Baseline incluse |
| 3 | Pairs trading | Relative value | Élevée | Baseline incluse |
| 3 | Momentum/régime | Directionnelle | Élevée | Baseline incluse |
| 4 | Lead-lag | Très court terme | Très élevée | Prototype horaire seulement |
| 4 | Market making | Inventaire + spread | Extrême | Simulateur jouet seulement |

### 3.1 Cash-and-carry

Le bot achète le spot et shorte le perp du même actif. Il recherche le funding reçu par le short, diminué des frais, du slippage, du coût de couverture et du risque de basis.

Le backtest doit calculer le rendement sur **tout le capital immobilisé**, pas uniquement sur la marge du short.

### 3.2 Basket de funding

Le bot classe les perps : il achète les fundings faibles ou négatifs et shorte les fundings élevés. La version sérieuse ajoutera neutralité bêta, inverse-vol, covariance et pénalité de turnover.

Ce n'est pas un arbitrage pur : deux actifs différents peuvent diverger violemment.

### 3.3 Arbitrage de funding inter-exchanges

Même actif, deux plateformes : long sur la venue au funding inférieur et short sur celle au funding supérieur.

Le risque principal vient de la marge séparée : le portefeuille global peut être neutre, mais une jambe locale peut être liquidée.

### 3.4 Pairs trading

Le bot trade l'écart entre deux actifs corrélés. Il entre lorsque le spread est anormal, sort au retour à la moyenne et ferme également sur rupture ou délai maximal.

Aucune martingale n'est permise.

### 3.5 Momentum/régime

Le bot prend une exposition directionnelle quand la tendance dépasse le bruit, avec sizing par volatilité et pénalité si le funding coûte trop cher.

Cette stratégie peut produire plus, mais son drawdown peut être nettement supérieur.

### 3.6 Lead-lag

Une venue de référence bouge ; Hyperliquid réagit légèrement plus tard ; le bot tente d'exploiter ce délai.

Le prototype inclus utilise des barres horaires uniquement pour exercer le moteur. Une conclusion réelle exige flux sub-seconde, horloges synchronisées, mesure de latence et replay du carnet.

### 3.7 Market making

Le bot place bid et ask autour d'une fair value, réduit son risque selon l'inventaire et retire ses ordres quand le flux devient toxique.

Un simple backtest OHLC est invalide ici. Il faut estimer la place dans la file, les fills, les cancel/replace et l'adverse selection.

---

## 4. Philosophie des résultats

HyperLab ne contient aucun objectif du type « 1 % par mois ». Le backtest peut afficher :

```text
-20 %, +3 %, +40 %, +5 % par mois ou davantage
```

Le résultat n'est jamais plafonné. En revanche, le profil déployable impose des limites d'exposition. C'est la différence entre :

- falsifier un résultat ;
- mesurer une stratégie que l'on accepterait réellement de faire tourner.

Un résultat exceptionnel déclenche davantage d'audits : frais oubliés, fills irréalistes, look-ahead, levier implicite, survivorship bias, sélection de paramètres ou régime exceptionnel.

---

# PARTIE A — INSTALLATION WINDOWS 11

## 5. Installer les outils

Ouvre PowerShell normalement :

```powershell
winget install -e --id Python.Python.3.12
winget install -e --id Git.Git
winget install -e --id Docker.DockerDesktop
```

Redémarre PowerShell et vérifie :

```powershell
python --version
git --version
docker --version
```

Python 3.12 est recommandé. Python 3.11 et 3.13 sont également acceptés par le projet, mais la CI de référence utilise 3.12.

## 6. Extraire le pack

```powershell
New-Item -ItemType Directory -Force C:\Dev
Expand-Archive `
  -Path "$HOME\Downloads\hyperlab-multistrategy-v0.2.0.zip" `
  -DestinationPath "C:\Dev" `
  -Force
```

Entre dans le dossier qui contient `pyproject.toml` :

```powershell
cd C:\Dev\hyperlab-multistrategy
```

## 7. Installer l'environnement Python

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\bootstrap.ps1
```

Le script :

1. crée `.venv` ;
2. installe les dépendances ;
3. installe HyperLab en mode éditable ;
4. lance les tests ;
5. lance le diagnostic.

La sortie attendue contient :

```text
Exécuteur d'ordres  ABSENT — BLOQUÉ
Installation saine
```

## 8. Afficher les stratégies

```powershell
.\.venv\Scripts\python.exe -m hyperlab strategies
```

## 9. Lancer la démonstration complète

```powershell
.\.venv\Scripts\python.exe -m hyperlab demo `
  --strategy all `
  --hours 1200 `
  --output reports\demo
```

Ouvre le rapport :

```powershell
Start-Process .\reports\demo\comparison.html
```

**Attention :** les données synthétiques ont volontairement des structures reconnaissables par plusieurs baselines. Un rendement élevé dans ce rapport ne signifie rien sur le vrai marché.

## 10. Tester une stratégie seule

```powershell
.\.venv\Scripts\python.exe -m hyperlab demo --strategy cash_and_carry
.\.venv\Scripts\python.exe -m hyperlab demo --strategy funding_basket
.\.venv\Scripts\python.exe -m hyperlab demo --strategy cross_exchange_funding
.\.venv\Scripts\python.exe -m hyperlab demo --strategy pairs_mean_reversion
.\.venv\Scripts\python.exe -m hyperlab demo --strategy momentum_regime
.\.venv\Scripts\python.exe -m hyperlab demo --strategy lead_lag
.\.venv\Scripts\python.exe -m hyperlab demo --strategy inventory_market_making
```

## 11. Lire les données publiques Hyperliquid

```powershell
.\.venv\Scripts\python.exe -m hyperlab snapshot `
  --network mainnet `
  --save
```

Cette commande :

- utilise uniquement l'Info API ;
- ne demande pas d'adresse ;
- n'utilise pas de secret ;
- repère les actifs présents en spot USDC et perp ;
- stocke le résultat dans `data\hyperlab.sqlite3`.

## 12. Lancer une collecte courte

```powershell
.\.venv\Scripts\python.exe -m hyperlab collect `
  --network mainnet `
  --assets BTC,ETH `
  --candle-intervals 1m `
  --duration-seconds 600
```

Le contrat technique complet est documenté dans
[`HYPERLIQUID_COLLECTOR.md`](HYPERLIQUID_COLLECTOR.md). Le premier bootstrap REST
et toute resynchronisation après coupure ont lieu avant l’ouverture du socket ;
le collecteur passe ensuite de `SUBSCRIBING` à `LIVE` seulement après tous les
acquittements. Une fois `LIVE`, le refresh REST périodique s’exécute dans un
worker pendant que la lecture WebSocket continue.

Les révisions d’une candle restent des observations immuables : aucune horloge
locale ne les marque finales. L’intervalle calendaire `1M` est volontairement
refusé ; utilise un intervalle fixe tel que `1m`, `1h`, `1d` ou `1w`.

Cette collecte courte est un smoke test, pas la certification soak de 24 heures,
qui reste explicitement en attente.

Vérifie :

```powershell
.\.venv\Scripts\python.exe -m hyperlab status
```

Le statut runtime expose notamment l’état, les connexions/reconnexions, les gaps
visibles et les flux stale.

## 13. Dashboard Windows

```powershell
.\.venv\Scripts\python.exe -m hyperlab serve `
  --host 127.0.0.1 `
  --port 8000
```

Ouvre :

```text
http://127.0.0.1:8000
```

La page « Santé du collecteur public » lit `data/runtime_status.json` et affiche
les connexions, gaps et flux stale. Le compteur SQLite est conservé uniquement
comme indicateur **legacy** de la commande `snapshot --save`; il ne compte pas
les lignes Parquet du collecteur Phase 02.

La bannière doit dire `READ-ONLY — ORDRES IMPOSSIBLES`.

---

# PARTIE B — GIT ET CODEX

## 14. Créer le premier checkpoint Git

```powershell
git init
git add .
git commit -m "chore: initial HyperLab multi-strategy lab"
```

Crée une branche :

```powershell
git switch -c phase-00-audit
```

## 15. Ouvrir dans Codex

Ouvre le dossier `C:\Dev\hyperlab-multistrategy` dans Codex.

Garde :

- sandbox workspace ;
- approbation à la demande ;
- accès réseau seulement si nécessaire ;
- aucun Full Access ;
- aucune connexion au wallet ou à Umbrel de production.

## 16. Premier prompt Codex

Donne le contenu de `prompts/00_AUDIT_INITIAL.md`.

L'audit doit arriver **avant** les modifications. Ensuite seulement, autorise les corrections confirmées.

## 17. Contrôler les changements

```powershell
git status
git diff
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m hyperlab demo --strategy all --hours 1200
```

Puis :

```powershell
git add .
git commit -m "fix: harden initial read-only research lab"
```

## 18. Exécuter les phases dans l'ordre

```text
00 audit
01 modèle et qualité des données
02 collecteur Hyperliquid public
03 venues externes
04 backtester réaliste
05 cash-and-carry
06 basket funding
07 funding inter-exchanges
08 pairs trading
09 momentum
10 lead-lag
11 market making
12 paper engine
13 exécuteur testnet
14 revue sécurité / micro-mainnet
15 durcissement Umbrel
```

Les phases 13 et 14 ne sont ouvertes qu'après validation explicite. Elles ne doivent pas être fusionnées avec les phases de recherche.

---

# PARTIE C — DOCKER LOCAL

## 19. Construire l'image

Lance Docker Desktop, puis :

```powershell
.\scripts\build_docker.ps1
```

## 20. Démarrer collecteur et dashboard

```powershell
docker compose up -d
docker compose ps
```

Logs :

```powershell
docker compose logs -f collector
```

Dashboard :

```text
http://127.0.0.1:8000
```

Arrêt :

```powershell
docker compose down
```

Le collecteur reçoit `SIGINT`/`SIGTERM` de façon coopérative. Les compositions
locale et Umbrel lui accordent 30 secondes pour fermer le socket, publier le
dernier statut et flusher le dernier batch avant un éventuel arrêt forcé.

Les services sont non-root, read-only, sans capacités Linux et sans Docker socket.

---

# PARTIE D — INSTALLATION UMBREL

## 21. Pourquoi passer par GitHub Container Registry

Umbrel doit télécharger une image compatible avec l'architecture de ton mini-PC. Le workflow GitHub construit :

```text
linux/amd64
linux/arm64
```

Il publie ensuite une image versionnée dans GHCR.

## 22. Créer un dépôt GitHub

Crée un dépôt GitHub **public** nommé exactement `hyperlab`, sans secret. Le store et l’image read-only doivent être téléchargeables par Umbrel sans identifiants.

```powershell
git remote add origin https://github.com/VOTRE_NOM_GITHUB/hyperlab.git
git branch -M main
git push -u origin main
```

## 23. Personnaliser le package Umbrel

Les fichiers `umbrel-app-store.yml` et `jjlab-hyperlab/` sont volontairement à la **racine du dépôt** : c'est la structure attendue par le template officiel Umbrel.


```powershell
.\.venv\Scripts\python.exe scripts\prepare_umbrel_store.py VOTRE_NOM_GITHUB
```

Vérifie :

```powershell
Get-ChildItem .\jjlab-hyperlab -Recurse -File | Select-String "REPLACE_WITH_"
```

Aucun résultat attendu.

Commit :

```powershell
git add umbrel-app-store.yml jjlab-hyperlab scripts\prepare_umbrel_store.py
git commit -m "chore: configure Umbrel community app store"
git push
```

## 24. Publier l'image

```powershell
git tag v0.2.0
git push origin v0.2.0
```

Dans GitHub, vérifie l'Action `Publish container` et le package `hyperlab:0.2.0`.

Ouvre ensuite la page du package, puis **Package settings → Change visibility → Public**. Umbrel ne possède pas d'identifiants GHCR pour télécharger une image privée. La première version ne contient aucun secret ; le code et l'image peuvent donc être publics.

## 25. Ajouter le store dans umbrelOS

Dans Umbrel :

1. ouvre **App Store** ;
2. clique sur les trois points en haut à droite ;
3. clique sur **Community App Stores** ;
4. colle l'URL du dépôt GitHub ;
5. clique sur **Add** ;
6. ouvre `Jj HyperLab` ;
7. installe `HyperLab`.

N'ajoute pas un store tiers inconnu : Umbrel ne vérifie pas les apps communautaires.

## 26. Vérifier Umbrel

Ouvre HyperLab. Tu dois voir :

```text
READ-ONLY — ORDRES IMPOSSIBLES
```

La section runtime doit indiquer l’état du collecteur, ses connexions, ses gaps
et ses flux stale. Le compteur « legacy SQLite » peut rester à zéro lorsque seul
le collecteur Parquet Phase 02 tourne. Vérifie aussi :

```text
/health
/api/status
```

Le dashboard n'est pas un panneau d'administration de trading. Il ne possède aucun bouton d'ordre.

## 27. Collecter pendant le développement

Laisse Umbrel tourner pendant que Codex implémente les phases 01 à 04. Cela crée un jeu de données forward jamais vu pendant la calibration.

La collecte officielle historique est utile, mais sa publication peut être espacée. Notre propre flux réduit la dépendance à sa fraîcheur.

---

# PARTIE E — CONSTRUCTION DES DONNÉES RÉELLES

## 28. Données lentes

Pour carry, basket, inter-exchange, pairs et momentum :

- candles ;
- mark/oracle/mid ;
- funding ;
- OI ;
- volume ;
- BBO ;
- profondeur ;
- statut de marché ;
- listes historiques d'actifs.

## 29. Données rapides

Pour lead-lag et market making :

- carnet L2 event-by-event ;
- trades ;
- BBO ;
- séquences ;
- timestamps source/réception ;
- reconnexions ;
- venue de référence ;
- latence.

Ces datasets deviennent rapidement volumineux. Parquet + DuckDB sont préférables à une grosse table SQLite unique.

## 30. Qualité

Chaque journée doit produire un manifeste :

```text
nombre de lignes
premier/dernier timestamp
trous
messages hors ordre
duplicats
reconnexions
hash des fichiers
```

Un trou ne doit jamais être masqué par un forward-fill silencieux.

---

# PARTIE F — PROTOCOLE DE BACKTEST

## 31. Découpage temporel

Point de départ :

```text
60 % train
20 % validation
20 % test final verrouillé
```

Le test final n'est regardé qu'après avoir figé la stratégie.

## 32. Walk-forward

Pour chaque fenêtre :

1. estimer les paramètres sur le passé ;
2. les figer ;
3. trader la fenêtre suivante ;
4. avancer ;
5. agréger les résultats hors échantillon.

## 33. Coûts

Le backtest réaliste inclut :

```text
fees maker/taker
spread
slippage
taille/profondeur
fills partiels
ordres manqués
latence
sorties d'urgence
funding réel
transferts inter-venues
rendement passif abandonné
```

Les frais de `config/research.toml` sont des placeholders. La future phase paper doit récupérer les frais réels du compte.

## 34. Trois scénarios minimum

```text
informatif/optimiste
réaliste
défavorable : frais et slippage ×2
```

Ajouter ensuite :

- taux de fill maker réduit ;
- funding prévu réduit ;
- suppression des meilleurs trades ;
- choc de volatilité ;
- coupure d'une venue ;
- délai de couverture plus long.

## 35. Rapport obligatoire

Pour chaque stratégie :

- rendement total ;
- rendement annualisé ;
- max drawdown ;
- pire jour/heure ;
- Sharpe et Calmar ;
- turnover ;
- temps investi ;
- exposition brute/net ;
- PnL par composante ;
- répartition par actif/mois/régime ;
- capacité ;
- comparaison au passif 4–5 %.

## 36. Interpréter un résultat très élevé

Exemple : 5 % net par mois.

On ne le réduit pas. On cherche d'abord :

- levier implicite ;
- PnL calculé sur la mauvaise base de capital ;
- look-ahead ;
- frais manquants ;
- fill maker automatique ;
- survivorship bias ;
- sélection excessive de paramètres ;
- période exceptionnellement favorable ;
- risque rare absent.

---

# PARTIE G — ORDRE DE VALIDATION DES STRATÉGIES

## 37. Vague 1 : défensive

### Cash-and-carry

Objectif : fiabiliser deux jambes, coûts et funding.

Gate : rendement stressé supérieur au passif avec drawdown et marge raisonnables.

## 38. Vague 2 : market-neutral relatif

### Basket funding

Objectif : optimiser le portefeuille sans bêta caché.

### Funding inter-exchanges

Objectif : même actif, neutralité de prix, gestion de marge séparée.

## 39. Vague 3 : alpha moyen risque

### Pairs

Objectif : spread robuste, stop et time stop.

### Momentum

Objectif : rendement directionnel contrôlé, régime explicite.

## 40. Vague 4 : haute performance / haute exigence

### Lead-lag

Objectif : edge après latence et coûts réels.

### Market making

Objectif : spread net après markout adverse, queue et inventaire.

Ces deux modules ne doivent pas retarder le déploiement du collecteur lent : leurs données sont collectées en parallèle.

---

# PARTIE H — PAPER, TESTNET ET VRAI ARGENT

## 41. Paper live

Après backtest :

- stratégie figée ;
- décisions en temps réel ;
- ordres simulés ;
- modèle de fill réaliste ;
- aucun ajustement pendant la fenêtre de validation.

Minimum recommandé :

```text
6 à 8 semaines
30 à 50 cycles pour les stratégies lentes
beaucoup plus pour les rapides
14 jours sans incident critique
```

## 42. Testnet

Le futur exécuteur testnet doit vivre dans une branche/version séparée (`0.3.0-dev` ou service distinct). Le collecteur et le dashboard `0.2.x` restent read-only ; on ne leur ajoute jamais une clé « temporairement ».

Le testnet valide :

- signature ;
- ordres post-only/IOC/reduce-only ;
- annulations ;
- CLOID ;
- fills partiels ;
- WebSocket coupé ;
- réponse perdue ;
- redémarrage ;
- réconciliation ;
- dead-man switch.

Il ne valide pas parfaitement la rentabilité ou la liquidité.

## 43. Micro-mainnet

Seulement après toutes les gates :

```text
100 à 300 USDC
levier 1× maximum
une stratégie
une position
validation humaine
aucune hausse automatique
```

Le but est de mesurer :

```text
fill prévu vs réel
slippage prévu vs réel
fees prévus vs réels
PnL paper vs PnL live
```

## 44. Augmentation

```text
100–300 USDC : 4 à 8 semaines
500–1 000 USDC : nouvelle période d’observation
augmentation suivante : +50 à +100 % maximum
puis nouvelle observation avant chaque palier
```

Aucune augmentation si le rendement additionnel ne justifie pas le risque par rapport au placement passif.

---

# PARTIE I — SÉCURITÉ

## 45. Secrets

Ne donne jamais à ChatGPT ou Codex :

- seed ;
- clé principale ;
- fichier wallet ;
- keystore et mot de passe ensemble.

La future clé API doit être dédiée, révocable et limitée au capital du sous-compte.

## 46. Umbrel

Le package actuel :

- tourne en utilisateur 1000 ;
- filesystem read-only ;
- `cap_drop: ALL` ;
- `no-new-privileges` ;
- aucun Docker socket ;
- aucun port public direct ;
- aucun secret.

## 47. Séparation future

```text
collector : public
research : sans clé
dashboard : lecture seule
paper : sans clé
executor testnet : clé testnet
executor mainnet : clé dédiée et service séparé
```

## 48. Kill switches

Le futur live doit avoir :

- données stale → aucune entrée ;
- erreurs répétées → pause ;
- position inconnue → revue manuelle ;
- perte journalière → arrêt ;
- drawdown → arrêt ;
- jambe non couverte → hedge/débouclement ;
- process mort → annulation programmée des ordres.

---

# PARTIE J — ROUTINE PRATIQUE

## 49. Cette semaine

1. installe le pack sur Windows ;
2. lance les tests et la démo ;
3. initialise Git ;
4. donne le prompt 00 à Codex ;
5. crée le dépôt GitHub ;
6. publie l'image ;
7. installe l'app read-only sur Umbrel ;
8. commence la collecte 24/24.

## 50. Ensuite

Pendant qu'Umbrel collecte :

1. phase 01 données ;
2. phase 02 Hyperliquid ;
3. phase 03 seconde venue ;
4. phase 04 backtester ;
5. validation des stratégies de niveau 1 et 2 ;
6. recherche parallèle des niveaux 3 et 4.

## 51. Commandes quotidiennes utiles

```powershell
git status
git diff
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m hyperlab doctor
.\.venv\Scripts\python.exe -m hyperlab status
```

Docker :

```powershell
docker compose ps
docker compose logs --tail 100 collector
```

## 52. Quand abandonner une stratégie

- edge disparaît après coûts réalistes ;
- résultat dépend d'un seul mois ;
- stress test devient fortement négatif ;
- drawdown trop grand pour la surperformance ;
- capacité trop faible ;
- avantage absorbé par latence ;
- besoin d'un risque terminal caché ;
- rendement inférieur ou à peine supérieur au passif.

Abandonner une mauvaise stratégie est un résultat positif du laboratoire.

---

# Conclusion

Le projet ne vise plus un unique bot de carry. Il construit une **plateforme de recherche** capable de comparer plusieurs sources de rendement : défensives, relatives, directionnelles et microstructurelles.

L'ordre rationnel reste :

```text
infrastructure sûre
→ collecte
→ backtests non bridés
→ stress tests
→ forward paper
→ testnet
→ micro-mainnet
```

Umbrel commence à accumuler les données immédiatement, tandis que Windows et Codex développent et auditent les modèles. C'est la façon la plus rapide d'être ambitieux sans sacrifier la rigueur.

---

# Sources officielles

- Hyperliquid API : https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Hyperliquid historical data : https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data
- Hyperliquid WebSocket subscriptions : https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- Hyperliquid funding : https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding
- SDK Python officiel : https://github.com/hyperliquid-dex/hyperliquid-python-sdk
- Umbrel Community App Store : https://umbrel.com/support/apps/how-to-add-a-community-app-store
- Template Community App Store : https://github.com/getumbrel/umbrel-community-app-store
- Codex security and approvals : https://developers.openai.com/codex/agent-approvals-security

Vérifie de nouveau ces pages avant toute phase testnet ou mainnet : les APIs, frais, limites et formats évoluent.
