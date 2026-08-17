# Architecture

```text
WINDOWS 11 — développement et recherche
  Git + Codex + Python + backtests + rapports + builds Docker
          |
          | image versionnée / code revu
          v
UMBREL — fonctionnement 24/24, sans accès privé aux venues
  collector public ---> lake SQLite/Parquet ---> dashboard read-only
          |
          +------------> export recherche

WINDOWS / SERVICE LOCAL SÉPARÉ — jamais inclus dans le paquet Umbrel 0.2.x
  source publique normalisée ---> paper engine local ---> journal SQLite

ENVIRONNEMENTS SANS ARGENT RÉEL, autorisations exactes et non convertibles
  Paper local (`PAPER_RUNTIME`)
  Phase 13 `services/testnet-executor` 0.3.0.dev0 (`TESTNET_EXECUTION`)
    ---> Hyperliquid Testnet allowlisté ---> journal/audit SQLite séparé

ARGENT RÉEL, services/versions séparés et preuves B/C/D/E
  Phase 14 micro-mainnet -> autorisation Mainnet distincte
```

## Principe de séparation

Le collecteur, le moteur de stratégie, le paper engine, le dashboard et l'exécuteur
Testnet ne partagent pas le même niveau de privilège. Le paquet racine HyperLab
0.2.1 reste read-only vis-à-vis des venues : le paper engine transforme les
décisions en événements d'ordre **simulés localement**, sans clé, signature, client
privé ou transport d'ordre. Le dashboard ne peut ni soumettre une décision, ni
annuler un ordre simulé, ni modifier un paramètre. Le paquet Umbrel reste limité au
dashboard et à la collecte publique ; il n'embarque ni runtime paper, ni service
Testnet, ni secret.

Le seul composant capable de signer est le service local distinct
`services/testnet-executor`, version `0.3.0.dev0`. Son identité, ses endpoints, son
namespace de credentials, son reçu, sa configuration et son store sont propres à
`TESTNET` / `TESTNET_EXECUTION`. Il ne fournit aucun endpoint, profil, reçu ou CLI
micro-mainnet/Mainnet. Il s'exécute dans un venv dédié et n'est jamais installé en
editable dans le paquet racine. Son graphe externe vient du lock hashé et du
wheelhouse offline ; le paquet racine requis est construit depuis ce checkout puis
installé comme wheel local avec `--no-index --no-deps`, jamais résolu par son nom
sur un index. Le code de production exécuteur n'importe ni `hyperlab.paper` ni
`hyperlab.data` ; les primitives canoniques sont locales au service. Le wheel
racine fournit seulement l'autorisation d'environnement et les artefacts partagés
explicitement liés à l'identité de build.

La coordination critique ne dépend pas du chemin d'une base SQLite. Le registre
Windows pré-provisionné `%ProgramData%\HyperLab\TestnetExecutor\control-v1`, sans
reparse point et avec DACL restreinte sur chacun de ses trois composants projet,
porte le lease writer, le rate ledger, le
send gate et le kill latch à portée compte, ainsi que le nonce à portée API wallet.
Il est commun à tous les processus et toutes les bases, n'a aucun override CLI et
ne constitue ni un cache ni un répertoire temporaire.

La rate ledger conserve aussi jusqu'à `100000` identités ordinaires account-global
comme tombstones permanents, sans reset online. Cette borne limite la Phase 13 à une
campagne smoke supervisée ; `scheduleCancel` emprunte une voie protectrice exemptée.

## Modules

- `api/public.py` : appels publics au SDK officiel ;
- `storage/` : stockage local ;
- `data/` : schémas, import/export et données synthétiques ;
- `strategies/` : baselines sans exécution ;
- `backtest/` : coûts, limites, PnL et rapports ;
- `paper/` : configuration figée, machine à états, journal SQLite, simulation
  d'ordres, risque, réconciliation, replay et gate Phase 12 ;
- `services/testnet-executor/` : service Python `0.3.0.dev0` isolé, adaptateur
  Hyperliquid Testnet, credentials dédiés, FSM d'ordre, outbox/nonce durables,
  réconciliation exchange-first, reprise, risque, audit et CLI Phase 13 ;
- `dashboard/` : interface locale read-only ;
- `umbrel-app-store.yml` et `jjlab-hyperlab/` : store Umbrel à la racine, conforme au template officiel ;
- `prompts/` : phases Codex séquentielles.

## Pipeline de recherche Phase 04

```text
lake Parquet + manifestes vérifiés
  → point_in_time.py (received_time, finalité, lifecycle, staleness)
  → protocol.py (split hashé, walk-forward, verrou final)
  → registry.py (toutes les variantes, JSONL append-only)
  → engine.py + costs.py + execution.py (fills simulés, jambes, IOC)
  → attribution.py + bootstrap.py + benchmark.py
  → report.py (actif, mois UTC, régime, taille, incertitude)
```

`target_weights` appartient au modèle de stratégie; `weights` appartient au
simulateur d'exécution et représente seulement les fills obtenus. Cette séparation
empêche un ordre manqué d'être compté comme position réelle. Tous les objets d'ordre
de cette couche sont inertes : aucun module de transport ou SDK d'exécution n'est
importé.

## Pipeline paper Phase 12

```text
flux public point-in-time + qualité/fraîcheur
  → stratégie et paramètres figés (hash de configuration + seed)
  → décision déterministe
  → contrôle de risque pré-acceptation
  → cycle d'ordre simulé Phase 04 (ack/reject, non-fill, partial, IOC, délais)
  → événements append-only idempotents + état persistant
  → ledger cash/positions/frais/PnL
  → réconciliation et replay déterministe
  → projection SQLite reconstruisible → dashboard read-only + alertes
```

SQLite est l'autorité opérationnelle du paper engine. Son journal est append-only,
ordonné et chaîné par hashes ; la projection d'état servie au dashboard est
reconstruisible. Au redémarrage, aucune nouvelle entrée n'est acceptée avant
vérification de la chaîne, replay et réconciliation exacte. Une divergence force
`MANUAL_REVIEW`.

`paper/runtime.py` supervise une source normalisée strictement publique : reprise
et réconciliation avant le premier poll, cadence des timers, filtrage des
redéliveries et arrêt propre. La CLI ne charge aucun module utilisateur ; elle ne
peut démarrer que des factories inscrites statiquement pour le `config_hash` exact
et un reçu exact `PAPER` / `PAPER_RUNTIME`, dont chaque preuve est validée par
un vérificateur compilé exact environnement/but/check. Cette préparation technique
n'exige pas Gates B/C/D et ne peut jamais autoriser une autre classe. Le registre
reste néanmoins vide : aucune stratégie + source publique candidate complète
n'est encore inscrite et aucun jeu de vérificateurs candidat n'est compilé.
L'adaptateur générique accepte uniquement les BBO et événements de connexion
normalisés ; les trades restent bloqués jusqu'à une identité durable aux
redémarrages, et le raccord au writer unique n'existe pas. Les commandes de statut,
Gate D et replay restent read-only. Gate D lie son diagnostic à une tête stable mais
ne peut pas produire un PASS de promotion avec argent réel tant que l'attestation
runtime/source et les octets des artefacts Gate D ne sont pas persistés puis
revérifiés. Elle conserve les 42 jours et exigences économiques sans bloquer le
démarrage technique Paper ou Testnet. Le replay
réexécute l'inbox dans un store temporaire isolé. La Phase 10 n'est une dépendance
que pour une stratégie qui consomme explicitement ses artefacts, jamais pour la
Phase 12 entière.

La machine à états contient `FLAT`, `ENTRY_PLANNED`, `LEG_1_PENDING`,
`HEDGE_PENDING`, `HEDGED`, `EXIT_PLANNED`, `EXIT_PENDING`, `PAUSED`,
`REDUCE_ONLY`, `MANUAL_REVIEW` et `EMERGENCY_FLATTEN`. Les états de protection
ne fournissent aucune route privilégiée : même une réduction
d'urgence reste un fill simulé qui peut être partiel ou manqué.

Les modèles de coûts, latence et fills conservent les étiquettes `CALIBRATED`,
`UNCALIBRATED` ou `SYNTHETIC` et les hashes de leurs preuves. Les frais viennent
d'un artefact public versionné/hashé, jamais de données privées de compte. La
description normative complète se trouve dans
[`PAPER_ENGINE_PHASE12.md`](PAPER_ENGINE_PHASE12.md).

## Pipeline Testnet Phase 13

```text
deux wheels locaux + graphe externe hash-locké dans un venv opérateur dédié
  → build-identity lié au Python et aux octets installés
  → validation offline invoquée par l'opérateur, gates via `.venv` racine fixe
  → worktree, deux exécutables et build isolé liés et stables
  → quatorze preuves + manifest + reçu TESTNET/TESTNET_EXECUTION
  → configuration canonique TESTNET exactement liée à ces identités
  → vérification endpoint/chain/credential scope/build/source/stratégie/limites
  → registre ProgramData pré-provisionné, DACL/reparse vérifiés
  → preflight read-only (SQLite + TLS/API + métadonnée + compte)
  → intent et CLOID déterministes persistés
  → contrôle de risque avec réservations worst-case
  → nonce + tentative ambiguë persistés avant I/O
  → signature L1 et endpoint Hyperliquid Testnet exact
  → ack/reject/open/fills/cancel/replace observés
  → FSM + audit append-only + projections SQLite
  → réconciliation exchange-first et recovery avant reprise
```

Les endpoints exacts sont `https://api.hyperliquid-testnet.xyz` et
`wss://api.hyperliquid-testnet.xyz/ws`. Aucun défaut réseau, redirect ou fallback
ne peut sélectionner Mainnet. Le transport brut n'importe jamais
`hyperliquid.exchange.Exchange`. Une réponse perdue conserve la tentative ambiguë
et déclenche une recherche par CLOID/OID ; pour submit/replace, l'absence distante
ne prouve jamais que l'action n'a pas été acceptée et n'autorise jamais un resubmit
aveugle.
`UNKNOWN`, ordre distant sans propriétaire, fill divergent, position incohérente ou
snapshot incomplet force `MANUAL_REVIEW`.

La machine d'ordre couvre `REQUESTED`, `SUBMITTED`, `ACKNOWLEDGED`, `OPEN`,
`PARTIALLY_FILLED`, `FILLED`, `CANCEL_REQUESTED`, `CANCELLED`, `REJECTED`,
`EXPIRED`, `INVALID` et `UNKNOWN`. La machine runtime couvre `STOPPED`, `STARTING`,
`RUNNING`, `PAUSED`, `MANUAL_REVIEW` et `KILLED`. Le protocole complet, les limites
et le workflow opérateur A–H sont décrits dans
[`TESTNET_EXECUTOR_PHASE13.md`](TESTNET_EXECUTOR_PHASE13.md). Le checkout ne
revendique ni exercice live Testnet terminé, ni Gate E.

Le kill est account-scoped et durable avant le réseau. `scheduleCancel` reste une
protection best-effort : le kill sans bundle complet, délégué à un runtime qui
détient le lease, ou non confirmé retourne le code opérateur `3` tout en conservant
`KILLED`. Un DMS armé ne liquide pas une position et ne prouve pas qu'un cancel est
déjà appliqué.
