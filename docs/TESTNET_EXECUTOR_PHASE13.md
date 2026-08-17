# Exécuteur Hyperliquid Testnet — Phase 13

## Statut et portée

La Phase 13 ajoute un service local séparé, `services/testnet-executor`, version
`0.3.0.dev0`. Il peut exercer le cycle d'ordres sur **Hyperliquid Testnet
uniquement**. Il n'est pas inclus dans le paquet Python racine `hyperlab` 0.2.1,
dans le collecteur, dans le dashboard, dans le paper engine ou dans l'application
Umbrel.

Ce checkout prépare le logiciel et ses tests avec des transports simulés. Il ne
constitue pas une validation live de l'API Hyperliquid Testnet, ne produit pas Gate
E et ne prouve aucune rentabilité. Les commandes réseau et le premier ordre restent
des opérations humaines ultérieures. Aucun chemin Mainnet, micro-mainnet, transfert,
retrait ou argent réel n'est fourni.

| Élément | Valeur exacte |
|---|---|
| service | `hyperlab-testnet-executor` |
| version | `0.3.0.dev0` |
| environnement | `TESTNET` |
| but | `TESTNET_EXECUTION` |
| chain identity | `TESTNET` |
| namespace de credentials | `HYPERLAB_TESTNET` |
| HTTP | `https://api.hyperliquid-testnet.xyz` |
| WebSocket | `wss://api.hyperliquid-testnet.xyz/ws` |
| CLI | `hyperlab-testnet` |

Ces valeurs ne sont pas des valeurs par défaut modifiables : la configuration doit
les contenir exactement. Une URL Mainnet, une URL voisine, un schéma différent, un
redirect, une identité absente ou une tentative de fallback fait échouer le service
avant l'envoi. L'adresse principale publique et l'API wallet sont également liées à
la configuration immuable.

## Frontières d'architecture

```text
hyperlab 0.2.1 / Umbrel
  collecte publique + recherche + dashboard read-only
  aucun secret, signer ou transport d'ordre

paper Phase 12
  ordres/fills/cancels simulés localement
  aucune autorité venue

services/testnet-executor 0.3.0.dev0
  config TESTNET exacte + reçu TESTNET exact
  → risk gate
  → intent + CLOID + tentative persistés
  → signature L1 Testnet
  → adaptateur HTTP/WS Testnet allowlisté
  → observations venue
  → FSM + fills + positions + audit SQLite
  → réconciliation exchange-first

micro-mainnet / Mainnet
  absents du service Phase 13
```

Le service possède ses propres primitives dependency-light pour JSON canonique,
décimaux exacts, temps UTC, identifiants déterministes et schéma d'instrument. Il
n'importe aucun module `hyperlab.paper` ou `hyperlab.data` en production. Le wheel
racine revu reste requis localement pour l'identité de distribution,
`hyperlab.environment_authorization` et l'artefact partagé
`hyperlab.backtest.protocol` explicitement hashé par l'identité de build ; il
n'est jamais résolu depuis un index. Le service ne réutilise ni runtime Paper, ni
acknowledgements simulés, ni modèle de fill paper, ni ledger paper comme vérité de
venue. Sur Testnet, l'exchange et ses réponses réconciliées sont l'autorité.

L'adaptateur expose les opérations L1 strictement nécessaires : submit, cancel par
CLOID, amendement natif interne lorsque le protocole le permet et renouvellement
du dead-man switch. La CLI opérateur n'expose pas de commande `replace`. Les
lectures portent sur la métadonnée des actifs, les ordres ouverts, les statuts,
les fills, les positions, les soldes et l'equity. Les indices d'actifs et
`szDecimals` sont construits depuis la métadonnée Testnet puis figés pour le run ;
le service relit et compare les contraintes live avant chaque `submit` ou
`replace`. Un `cancel` protecteur utilise le mapping d'actif figé et le CLOID afin
qu'une panne de `meta` ne puisse pas le supprimer. `scheduleCancel` reste lui aussi
indépendant de `meta`. Le service n'importe ni
n'instancie `hyperliquid.exchange.Exchange`, dont l'endpoint implicite est
incompatible avec cette frontière.

## Installation isolée et chaîne logicielle

Le runtime opérateur doit être un venv dédié. Il ne faut jamais installer
l'exécuteur dans `.venv` racine, en editable, avec résolution automatique des
dépendances ou avec un paquet d'index nommé `hyperlab`. Le même venv opérateur doit
invoquer `build-identity`, `validate-software`, `evidence`, `preflight` et toutes
les commandes réseau. Recréer ou déplacer ce venv invalide le rapport.

Le venv opérateur reste minimal et ne reçoit pas les outils de développement. Le
plan `validate-software` sélectionne le Python revu au chemin fixe
`.venv\Scripts\python.exe` de la racine pour Ruff, mypy, pytest et les vérifications
manifest/release. Le scan de conflits et le build isolé restent exécutés par le
Python opérateur. Le rapport lie les chemins et SHA-256 de ces deux exécutables ;
remplacer l'un d'eux après validation invalide les preuves. Le `.venv` racine est
un environnement de gate revu, jamais le runtime Testnet.

Le checkout actuel est revu avec Python `3.12` AMD64 et le wheelhouse local fixe
`services/testnet-executor/.wheelhouse`. Ce répertoire est ignoré par Git, ne doit
contenir que des wheels réguliers compatibles et doit être préparé avant la
validation. La validation est offline : elle ne télécharge rien et refuse un
wheelhouse absent, muté pendant le run ou contenant autre chose que des `.whl`.

Bootstrap local exact, depuis la racine du dépôt, sans secret :

```powershell
$Repo = (Resolve-Path .).Path
$Service = Join-Path $Repo 'services\testnet-executor'
$GatePython = Join-Path $Repo '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $GatePython -PathType Leaf)) {
  throw 'le Python de gate fixe .venv est absent'
}
$GateAbi = & $GatePython -c `
  'import platform,sys; print(f"{sys.version_info.major}.{sys.version_info.minor}|{platform.machine()}")'
if ($LASTEXITCODE -ne 0 -or $GateAbi -ne '3.12|AMD64') {
  throw 'le Python de gate revu doit être CPython 3.12 AMD64'
}
$SeedPython = $GatePython
$Wheelhouse = Join-Path $Service '.wheelhouse'
$BuildVenv = Join-Path $Service 'local\bootstrap-build'
$BuiltWheels = Join-Path $Service 'local\bootstrap-wheels'
$ExecutorVenv = Join-Path $Service '.venv'

if (Test-Path $BuildVenv) { throw 'bootstrap-build doit être neuf' }
if (Test-Path $BuiltWheels) { throw 'bootstrap-wheels doit être neuf' }
if (Test-Path $ExecutorVenv) { throw 'le venv exécuteur doit être neuf' }
New-Item -ItemType Directory -Path $BuiltWheels | Out-Null

& $SeedPython -m venv --copies $BuildVenv
$BuildPython = Join-Path $BuildVenv 'Scripts\python.exe'
& $BuildPython -m pip install `
  --disable-pip-version-check --no-cache-dir --no-input --no-index `
  --only-binary=:all: --require-hashes --no-deps `
  --find-links $Wheelhouse `
  --requirement (Join-Path $Service 'requirements-build.lock')

& $BuildPython -m pip wheel `
  --disable-pip-version-check --no-cache-dir --no-input --no-index `
  --no-deps --no-build-isolation --wheel-dir $BuiltWheels $Repo
& $BuildPython -m pip wheel `
  --disable-pip-version-check --no-cache-dir --no-input --no-index `
  --no-deps --no-build-isolation --wheel-dir $BuiltWheels $Service

$RootWheel = @(Get-ChildItem -LiteralPath $BuiltWheels -File -Filter 'hyperlab-0.2.1-*.whl')
$ServiceWheel = @(
  Get-ChildItem -LiteralPath $BuiltWheels -File `
    -Filter 'hyperlab_testnet_executor-0.3.0.dev0-*.whl'
)
if ($RootWheel.Count -ne 1 -or $ServiceWheel.Count -ne 1) {
  throw 'exactement un wheel racine et un wheel exécuteur sont requis'
}

& $SeedPython -m venv --copies $ExecutorVenv
$ExecutorPython = Join-Path $ExecutorVenv 'Scripts\python.exe'
& $ExecutorPython -m pip install `
  --disable-pip-version-check --no-cache-dir --no-input --no-index `
  --only-binary=:all: --require-hashes --no-deps `
  --find-links $Wheelhouse `
  --requirement (Join-Path $Service 'requirements-external.lock')
& $ExecutorPython -m pip install `
  --disable-pip-version-check --no-cache-dir --no-input --no-index --no-deps `
  $RootWheel[0].FullName $ServiceWheel[0].FullName

Remove-Item Env:PYTHONHOME -ErrorAction SilentlyContinue
Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
Remove-Item Env:PYTHONUSERBASE -ErrorAction SilentlyContinue
& $ExecutorPython -m hyperlab_testnet.cli --help
```

`requirements-build.lock` et `requirements-external.lock` contiennent les pins et
hashes. Le premier sert uniquement à construire les deux wheels locaux ; le second
installe le graphe runtime externe. Les deux wheels locaux sont ensuite installés
avec `--no-index --no-deps`. Le gate `DEPENDENCY_BUILD_ISOLATION` répète ce
processus dans deux environnements frais, exige exactement les distributions
`hyperlab` et `hyperlab-testnet-executor`, vérifie que leurs imports proviennent du
venv frais et conserve les hashes/provenances.

Toute modification du code, des locks, du wheelhouse, de Python ou du venv impose
un nouveau bootstrap, une nouvelle identité, un nouveau rapport et un nouveau reçu.

## Registre de contrôle Windows durable

Les commandes de production utilisent exclusivement le registre fixé par Windows
à `%ProgramData%\HyperLab\TestnetExecutor\control-v1` via
`CSIDL_COMMON_APPDATA`. `LOCALAPPDATA`, `HOME` et les variables utilisateur ne
peuvent pas le déplacer. Le paramètre interne `lease_root` existe uniquement pour
les fixtures synthétiques et n'est exposé par aucune commande opérateur.

Les trois composants appartenant au projet (`HyperLab`, `TestnetExecutor` et
`control-v1`) doivent être pré-provisionnés une seule fois : le service refuse de
les créer. Ouvrir un PowerShell élevé **sous le compte Windows exact qui exécutera
le service**, exiger un chemin projet neuf, puis appliquer la même DACL
localisation-indépendante à chacun :

```powershell
$CommonData = [Environment]::GetFolderPath(
  [Environment+SpecialFolder]::CommonApplicationData
)
if ([string]::IsNullOrWhiteSpace($CommonData)) {
  throw 'Windows Common Application Data est indisponible'
}
$HyperLabRoot = Join-Path $CommonData 'HyperLab'
$ExecutorRoot = Join-Path $HyperLabRoot 'TestnetExecutor'
$ControlRoot = Join-Path $ExecutorRoot 'control-v1'
$ProjectRoots = @($HyperLabRoot, $ExecutorRoot, $ControlRoot)
foreach ($ProjectRoot in $ProjectRoots) {
  if (Test-Path -LiteralPath $ProjectRoot) {
    throw "composant projet existant : auditer sans écraser $ProjectRoot"
  }
  New-Item -ItemType Directory -Path $ProjectRoot | Out-Null
}

$OperatorSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$Icacls = Join-Path $env:SystemRoot 'System32\icacls.exe'
foreach ($ProjectRoot in $ProjectRoots) {
  & $Icacls $ProjectRoot /inheritance:r
  if ($LASTEXITCODE -ne 0) { throw "échec héritage DACL : $ProjectRoot" }
  & $Icacls $ProjectRoot /grant:r `
    "*${OperatorSid}:(OI)(CI)F" `
    '*S-1-5-18:(OI)(CI)F' `
    '*S-1-5-32-544:(OI)(CI)F'
  if ($LASTEXITCODE -ne 0) { throw "échec DACL : $ProjectRoot" }
  & $Icacls $ProjectRoot /setowner "*${OperatorSid}"
  if ($LASTEXITCODE -ne 0) { throw "échec propriétaire : $ProjectRoot" }
}
```

Sur chacun des trois composants projet, le propriétaire doit être le SID de
l'opérateur, `SYSTEM` (`S-1-5-18`) ou le groupe Administrateurs
(`S-1-5-32-544`). La DACL doit être non nulle, composée uniquement d'ACE allow/deny
simples ; aucun autre SID ne peut posséder ajout, écriture, suppression,
`WRITE_DAC`, `WRITE_OWNER`, generic-write ou full-control. `ProgramData` et tous les
composants jusqu'à la feuille doivent être de vrais répertoires, sans symlink,
junction ou autre reparse point ; la politique DACL propre au projet commence à
`HyperLab`. Une violation bloque le store avant toute opération active.

Le registre contient le lease writer, le kill latch, la rate ledger et le send gate
à portée compte, ainsi que le watermark de nonce à portée API wallet. Ces états sont
communs à toutes les bases et à tous les processus. Ils ne contiennent aucun secret,
mais sont des contrôles de sécurité durables : ne jamais supprimer, vider, déplacer
ou recréer ce répertoire pour « débloquer » un run, même après suppression de la
base SQLite.

Chaque identité ordinaire `SUBMIT`, `CANCEL` ou `REPLACE` devient en outre un
tombstone permanent à portée compte. Il n'existe ni éviction silencieuse ni reset
online ; une autre base ou un autre processus retrouve les mêmes identités. La
capacité compilée est `100000`. À la somme maximale configurée
`12 + 24 + 6 = 42` actions ordinaires/minute, elle représente environ `39,7 h` :
la Phase 13 est une campagne smoke bornée, pas un moteur d'ordres continu. Surveiller
`account_action_identity_usage(run_id) -> (used, capacity)` et les champs
`account_action_identity_count/capacity` de chaque audit, puis arrêter largement
avant la capacité pour revue humaine offline. `SCHEDULE_CANCEL` est une voie
protectrice exemptée et ne consomme pas ces tombstones.

## Configuration immuable

La configuration est un objet JSON canonique, sans clés supplémentaires, avec les
champs suivants :

```text
schema_version          = 1
executor_version        = 0.3.0.dev0
environment             = TESTNET
purpose                 = TESTNET_EXECUTION
chain_identity          = TESTNET
credential_namespace    = HYPERLAB_TESTNET
http_endpoint           = https://api.hyperliquid-testnet.xyz
ws_endpoint             = wss://api.hyperliquid-testnet.xyz/ws
candidate_id            = identifiant stable
account_address         = adresse publique Testnet en minuscules
api_wallet_address      = adresse API wallet Testnet distincte, en minuscules
strategy_name           = identifiant de stratégie figé
strategy_hash           = SHA-256 du candidat figé
build_hash              = SHA-256 du build revu
source_identity         = identité de source figée
source_hash             = SHA-256 de la source
risk_limits             = objet exact décrit ci-dessous
```

Le hash de configuration identifie le run. Modifier une adresse, un endpoint, un
hash, une stratégie ou une limite crée une autre identité ; le reçu précédent ne
peut plus être consommé. Les instruments perpétuels suivent la forme canonique
`HL:<COIN>:perp`.

## Credentials Testnet

Le service lit uniquement ces trois variables de processus :

- `HYPERLAB_TESTNET_PRIVATE_KEY` ;
- `HYPERLAB_TESTNET_ACCOUNT_ADDRESS` ;
- `HYPERLAB_TESTNET_API_WALLET_ADDRESS`.

La clé privée doit appartenir à une API wallet **Testnet dédiée et révocable**.
L'adresse dérivée de la clé doit être égale à
`HYPERLAB_TESTNET_API_WALLET_ADDRESS`, et les deux adresses doivent correspondre
exactement à la configuration. L'API wallet doit être distincte de l'adresse
principale.

Le compte doit être un compte utilisateur Testnet dédié, jamais un vault,
sous-compte ou agent principal. Le preflight exige le rôle venue exact `user` et
exactement une API wallet active dans `extraAgents` : celle configurée. Une seconde
API wallet encore valide, ou une opération manuelle simultanée avec le wallet
principal, viole l'hypothèse de writer unique et bloque l'exploitation. Les agents
historiques expirés peuvent rester visibles.

Ne placez jamais la valeur de la clé dans Git, un fichier `.env`, le JSON de
configuration, une ligne de commande, un ticket, un chat, un log ou un artefact de
preuve. Injectez les trois variables depuis le gestionnaire de secrets local dans
le seul processus exécuteur. Les représentations et erreurs restent redacted ; les
objets secrets refusent la sérialisation. La présence d'un namespace générique,
Paper, micro-mainnet ou Mainnet ambigu fait échouer le chargement. N'utilisez jamais
une seed ou la clé principale du compte.

## Autorisation sémantique

Le runtime consomme un reçu exact `TESTNET` / `TESTNET_EXECUTION`. Le reçu lie le
candidat, le hash de configuration, le build, la stratégie, la source et le hash
des limites. Les quatorze checks compilés sont :

1. `EXPLICIT_TESTNET_ENDPOINT` ;
2. `TESTNET_CONFIG_NAMESPACE` ;
3. `NO_MAINNET_FALLBACK` ;
4. `ISOLATED_TESTNET_CREDENTIALS` ;
5. `CREDENTIAL_SCOPE_VALIDATION` ;
6. `DETERMINISTIC_CLIENT_ORDER_IDS` ;
7. `ORDER_LIFECYCLE_STATE_MACHINE` ;
8. `CANCEL_REPLACE_SEMANTICS` ;
9. `RECONCILIATION` ;
10. `RESTART_RECOVERY` ;
11. `BOUNDED_POSITION_NOTIONAL` ;
12. `KILL_SWITCH` ;
13. `FAIL_CLOSED_ENVIRONMENT_IDENTITY` ;
14. `FULL_AUDIT_LOG`.

`hyperlab-testnet validate-software` doit précéder `evidence`. Il exige la branche
`phase-13-testnet`, l'ancêtre baseline
`88e8224797684b9bf44be426b79ee01cccbe6e46`, une identité runtime égale à la
configuration, un wheelhouse offline intact et un répertoire de sortie frais
directement sous `services/testnet-executor/evidence`. Le plan compilé exécute
Ruff/mypy racine et service, les suites Phase 13, les régressions Phase 12, la
suite racine complète, `git diff --check`, le scan de conflits, les vérifications
manifest/release et le build isolé. Il lie aussi l'inventaire worktree avant/après,
les exécutables, Python, les locks, les deux wheels et chaque artefact. La commande
est invoquée par `$ExecutorPython`, mais les gates de développement utilisent
exactement `$GatePython` ; leurs chemins et hashes sont enregistrés, pas résolus
depuis `PATH`.

Séquence locale exacte dans le venv exécuteur déjà installé :

```powershell
$Config = (Resolve-Path (Join-Path $Service 'config\testnet.local.json')).Path
& $ExecutorPython -m hyperlab_testnet.cli build-identity
# Reporter exactement les cinq valeurs imprimées dans la configuration,
# avec les deux adresses publiques Testnet, puis ne plus modifier le checkout.

$ValidationName = 'software-YYYYMMDD-HHMMSS'  # nom neuf, sans sous-répertoire
& $ExecutorPython -m hyperlab_testnet.cli validate-software `
  --config $Config --output-directory $ValidationName
$ValidationReport = (
  Resolve-Path (Join-Path $Service "evidence\$ValidationName\software-validation.json")
).Path

$ReadinessRoot = Join-Path $Service 'evidence\readiness-YYYYMMDD-HHMMSS'
$EvidenceRoot = Join-Path $ReadinessRoot 'artifacts'
$Manifest = Join-Path $ReadinessRoot 'manifest.json'
$Receipt = Join-Path $ReadinessRoot 'receipt.json'
& $ExecutorPython -m hyperlab_testnet.cli evidence `
  --config $Config --validation-report $ValidationReport `
  --evidence-root $EvidenceRoot --manifest $Manifest --receipt $Receipt

$AuthorizationArgs = @(
  '--config', $Config,
  '--receipt', $Receipt,
  '--manifest', $Manifest,
  '--evidence-root', $EvidenceRoot,
  '--validation-report', $ValidationReport
)
$Db = Join-Path $Service 'local\testnet.sqlite3'
```

Le fichier de configuration local doit être créé à partir de
`config/testnet.example.json` dans un chemin ignoré ; il ne contient aucune clé.
`build-identity` imprime les valeurs `build_hash`, `source_hash`,
`source_identity`, `strategy_hash` et `strategy_name` à recopier exactement.
`evidence` recharge le rapport absolu, en revérifie tous les octets, crée les
quatorze artefacts canoniques, leur manifest et le reçu, sans écraser un fichier
existant. Un nom de fichier ou un champ texte `PASS` n'autorise rien. Toute preuve
absente, mutée, dupliquée, non canonique ou liée à un autre rapport bloque la
décision. Un reçu Testnet ne peut autoriser ni `MICRO_MAINNET_EXECUTION` ni
`MAINNET_EXECUTION`, et le flag de build argent réel reste faux.

## Machine à états persistante

Les états d'ordre exacts sont :

```text
REQUESTED → SUBMITTED → ACKNOWLEDGED → OPEN → PARTIALLY_FILLED → FILLED
                  └────────────────────────→ REJECTED / INVALID / EXPIRED
OPEN / PARTIALLY_FILLED → CANCEL_REQUESTED → CANCELLED
toute réponse ambiguë → UNKNOWN → réconciliation obligatoire
```

`UNKNOWN` n'est pas un succès ni un état terminal. Il réserve l'exposition dans le
pire cas et interdit tout renvoi aveugle. Une transition impossible, un CLOID
dupliqué ou une réponse incohérente force `MANUAL_REVIEW`.

Les CLOID ont la forme `0x` suivie de 16 octets hexadécimaux minuscules et dérivent
de l'intent figé dans le domaine `hyperliquid_testnet_cloid_v1`. Submit, cancel,
replace et dead-man switch possèdent chacun une tentative persistante. Les états du
runtime sont `STOPPED`, `STARTING`, `RUNNING`, `PAUSED`, `MANUAL_REVIEW` et
`KILLED`.

## Persistance, audit et crash recovery

Avant tout appel signé, une transaction SQLite durable enregistre l'intent, le
CLOID, le type d'action, le nonce monotone et une tentative initialement ambiguë.
Le transport ne devient donc jamais la seule trace d'une action. L'audit
append-only chaîné couvre au minimum : démarrage/arrêt, autorisation, intents,
risque, submit/ack/reject, fills, cancel/replace, réconciliation, pause, kill et
recovery. Aucun payload signé ou secret n'est conservé.

Après un crash avant ou après l'envoi, le recovery vérifie l'intégrité locale puis
recherche le CLOID et l'OID sur la venue. Pour un submit ou replace potentiellement
envoyé, l'absence distante, même répétée après expiration signée, ne prouve jamais
qu'il n'a pas été accepté : la tentative reste ambiguë et force
`MANUAL_REVIEW` jusqu'à une preuve venue autoritaire. Seul un refus durable du
permit final avant tout POST peut prouver `RESOLVED_NOT_SENT`. Les fills exigent
une identité venue `tid` stable et sont comptés une seule fois ; une identité
absente ou une collision de contenu bloque le run. Une annulation non confirmée reste
`CANCEL_REQUESTED` ou `UNKNOWN` jusqu'à observation. Le redémarrage ne peut ni
oublier un ordre ouvert, ni inventer une position, ni compter un fill deux fois.

## Réconciliation exchange-first

La réconciliation lit un snapshot cohérent de la venue avant de permettre une
nouvelle entrée :

- ordres locaux actifs contre ordres ouverts et statuts distants ;
- mapping CLOID/OID, y compris acknowledgements manquants ;
- fills distants dédupliqués ;
- positions perpétuelles ;
- soldes spot, equity et montant retirable ;
- tentatives ambiguës après timeout, reconnexion ou restart.

L'application d'un snapshot est idempotente. Un ordre distant inconnu, un CLOID ou
OID dupliqué, un fill sans propriétaire, une quantité qui régresse, une position
divergente, un cancel toujours ouvert ou un snapshot illisible place le runtime en
`MANUAL_REVIEW`. Aucune position locale n'écrase silencieusement la venue.

Une perte WebSocket rend l'état non frais et déclenche une reprise/réconciliation ;
elle ne justifie jamais de supposer qu'aucun événement n'a eu lieu.

## Limites Testnet conservatrices

Ces douze valeurs sont à la fois les défauts et les plafonds conservateurs compilés.
Elles servent à exercer le logiciel, pas à préfigurer une autorisation Mainnet :

| Champ | Défaut = plafond compilé |
|---|---:|
| notionnel brut maximal | `1000` |
| notionnel maximal par position | `500` |
| notionnel maximal par ordre | `100` |
| quantité maximale par position | `5` |
| quantité maximale par ordre | `1` |
| ordres simultanés | `4` |
| submits par minute | `12` |
| cancels par minute | `24` |
| replaces par minute | `6` |
| âge maximal des données marché | `5 s` |
| âge maximal de la réconciliation | `10 s` |
| intervalle du dead-man switch | `30 s` |

Chaque champ doit rester strictement positif. La configuration peut choisir une
valeur inférieure ou égale au plafond, jamais supérieure ; une valeur abaissée fait
partie du hash de configuration et exige ses propres preuves/reçu. Ni stratégie,
CLI, nouvelle base ni override runtime ne peut relever un plafond. Le relever dans
le code crée un autre build et requiert une nouvelle revue humaine.

Toute valeur décimale exacte admise par le service est bornée avant formatage : au
plus 64 chiffres de coefficient, `|exponent| <= 64` et `|adjusted| <= 64`. Les
floats, non-finis et formes au-delà de ces bornes sont refusés avant toute expansion
en chaîne ; une notation compacte telle qu'un exposant extrême ne peut donc pas
provoquer une allocation non bornée.

Le risk gate projette positions et ordres actifs/ambigus dans le pire cas. Il
refuse une taille ou un prix invalide, une donnée marché stale, une réconciliation
stale, une limite de débit atteinte, trop d'ordres, un état protecteur ou un
dépassement de quantité/notionnel. `reduce_only` ne peut qu'abaisser l'exposition
absolue. Les limites sont hashées avec la configuration et ne peuvent pas être
augmentées par la configuration, la stratégie ou le runtime.

`pause` exige `--database --run-id --confirm TESTNET-PAUSE` et persiste seulement
`PAUSED` localement, sans action réseau. `kill` exige
`--database --run-id --confirm TESTNET-KILL`. Il crée
d'abord un latch `KILLED` irréversible à la portée du compte, partagé entre bases,
qui bloque submit/replace mais laisse les actions protectrices possibles.

La protection venue est dégradée et explicitement observable :

- sans les cinq chemins config/reçu/manifest/preuves/validation, la commande ne
  signe rien, retourne le latch avec `deadman_confirmed=false` puis sort avec le
  code `3` ;
- si un runtime détient le lease, la commande sort `3` avec
  `EXISTING_RUNTIME_WILL_ENFORCE_LATCH` ; le runtime propriétaire doit détecter le
  latch, tenter le dead-man switch et s'arrêter ;
- si aucun owner ne détient le lease et que les cinq chemins sont fournis, la
  commande tente immédiatement un `scheduleCancel` ; tout résultat autre que
  `DEADMAN_ARMED` sort `3` ;
- le dead-man switch programme l'annulation d'ordres, ne liquide aucune position et
  ne prouve jamais que chaque ordre est déjà annulé.

Le latch reste actif même si la protection réseau échoue. L'opérateur doit alors
vérifier la venue, annuler manuellement ou révoquer l'API wallet selon le runbook
hors bande. Une erreur de kill/cancel reste visible ; le logiciel n'invente jamais
un acknowledgement.

## CLI opérateur

L'interface Phase 13 est dédiée au service :

| Commande | Effet |
|---|---|
| `hyperlab-testnet build-identity` | imprime l'identité source/build/stratégie du venv courant ; aucun réseau |
| `hyperlab-testnet validate-software` | exécute le plan local/offline compilé et écrit un rapport neuf |
| `hyperlab-testnet evidence` | prépare les preuves sémantiques liées à la configuration ; aucun réseau, aucun ordre |
| `hyperlab-testnet preflight` | vérifie identité, credentials, reçu, SQLite, risque et connectivité ; aucun ordre |
| `hyperlab-testnet status` | exige `--database`, lit l'état local sans credential ni réseau ; `--run-id` est requis si la base contient plusieurs runs |
| `hyperlab-testnet reconcile` | exécute une réconciliation exchange-first |
| `hyperlab-testnet run` | reprend/réconcilie, puis supervise le runtime et le dead-man switch ; ne crée pas seul de stratégie ou d'ordre |
| `hyperlab-testnet pause` | persiste une pause locale avec `--database --run-id --confirm TESTNET-PAUSE` ; aucun réseau |
| `hyperlab-testnet kill` | latch compte durable avec `--database --run-id --confirm TESTNET-KILL` ; DMS best-effort seulement |
| `hyperlab-testnet smoke-order` | soumet un unique intent manuel explicite après `--confirm TESTNET-ORDER` ; `--ordinal` optionnel, entier `>= 0`, défaut `0` |
| `hyperlab-testnet cancel` | annule le CLOID fourni après `--confirm TESTNET-CANCEL` |

Avant la première utilisation, consultez `hyperlab-testnet <commande> --help` sur
le commit validé : les chemins et options doivent être copiés depuis cette version,
jamais depuis une ancienne note.

### Preflight obligatoire

Le preflight doit réussir simultanément : configuration canonique et endpoint
exact, credential scope et adresse dérivée, reçu exact et preuves intactes,
intégrité/écriture SQLite, limites cohérentes, lecture API/TLS, métadonnée d'actifs
et lecture du compte. Il n'appelle ni submit, ni cancel, ni replace. Un échec laisse
le runtime arrêté.

Exemple de forme de commande, à adapter uniquement avec des chemins locaux sans
secret :

```powershell
& $ExecutorPython -m hyperlab_testnet.cli preflight @AuthorizationArgs --database $Db
```

Les cinq chemins d'autorisation sont obligatoires ensemble pour `preflight`,
`reconcile`, `run`, `smoke-order` et `cancel`. Ils ne sont jamais réduits au seul
reçu : le manifest, les quatorze preuves et le rapport logiciel courant sont
revérifiés à chaque construction de contexte.

## Workflow smoke manuel A–H

Ce workflow n'a pas été exécuté pendant l'implémentation Phase 13. Il doit être
réalisé par un opérateur sur Hyperliquid Testnet, depuis une machine locale dédiée,
après revue du commit, création d'une API wallet Testnet et approvisionnement
Testnet. Conservez les preuves brutes et n'interprétez jamais un fill Testnet comme
un résultat économique.

### A. Preflight seulement

1. Injecter les trois variables `HYPERLAB_TESTNET_*` depuis le gestionnaire de
   secrets local.
2. Vérifier le commit, la configuration canonique, les preuves et le reçu.
3. Exécuter `preflight` avec les cinq chemins d'autorisation et la base explicites.
4. Arrêter au premier blocker ; aucun ordre ne doit apparaître.

### B. Lecture compte et réconciliation

```powershell
& $ExecutorPython -m hyperlab_testnet.cli reconcile @AuthorizationArgs --database $Db
& $ExecutorPython -m hyperlab_testnet.cli status --database $Db
```

Attendre une réconciliation fraîche, zéro divergence et un runtime non protecteur.

### C. Plus petit ordre Testnet valide

Choisir manuellement un instrument et un prix Testnet actuels. `meta` fournit le
coin, l'asset ID et `szDecimals`, mais pas un minimum de taille/notionnel garanti :
utiliser un minimum confirmé sur la venue et rester sous toutes les limites. Le
service valide localement la précision de quantité et l'encodage de prix
Hyperliquid ; un rejet de minimum est définitif et ne déclenche aucun fallback.
Préférer un ordre ALO/post-only éloigné du marché pour le premier cycle. La
commande exige la confirmation littérale `TESTNET-ORDER` :

```powershell
& $ExecutorPython -m hyperlab_testnet.cli smoke-order @AuthorizationArgs `
  --database $Db --instrument HL:<COIN>:perp --side BUY `
  --quantity <SMALLEST_VALID_TESTNET_QUANTITY> `
  --limit-price <REVIEWED_TESTNET_PRICE> --time-in-force ALO `
  --allow-increase `
  --confirm TESTNET-ORDER
```

`--reduce-only` est le défaut et ne peut pas ouvrir une exposition. Un premier
ordre qui ouvre une position doit donc employer explicitement `--allow-increase`.
Ces deux noms sont des flags exclusifs ; `--reduce-only <TRUE_OR_FALSE>` n'est pas
une syntaxe valide. `--side` et `--time-in-force` sont sensibles à la casse
(`BUY|SELL` et `GTC|IOC|ALO`).

`--ordinal` vaut `0` par défaut et doit être un entier positif ou nul. Il fait
partie de l'identité déterministe d'un intent : l'incrémenter ne sert que pour un
nouvel ordre manuel distinct, revu et approuvé. Ne jamais changer l'ordinal pour
réessayer un submit ambigu ou perdu ; rechercher/réconcilier le CLOID existant.

Ne copiez jamais une valeur Mainnet. Vérifiez ensuite CLOID, OID, ack et état
`OPEN` avec `status` et la venue Testnet.

### D. Cancel

```powershell
& $ExecutorPython -m hyperlab_testnet.cli cancel @AuthorizationArgs `
  --database $Db --cloid <CLOID_FROM_STEP_C> --confirm TESTNET-CANCEL
& $ExecutorPython -m hyperlab_testnet.cli reconcile @AuthorizationArgs --database $Db
& $ExecutorPython -m hyperlab_testnet.cli status --database $Db
```

Ne conclure `CANCELLED` qu'après acknowledgement/observation venue.

### E. Fills partiel et complet

Planifier des cycles séparés, de taille minimale, avec une contrepartie Testnet
contrôlée si nécessaire. Observer un fill partiel puis complet lorsque la venue le
permet ; ne jamais fabriquer ces états. Vérifier quantités cumulées, frais,
déduplication et position après chaque réconciliation.

### F. Restart avec état ouvert

Créer un ordre minimal contrôlé, confirmer qu'il est durablement ouvert, puis
arrêter et redémarrer le processus selon la procédure opérateur. Tenir compte du
dead-man switch : une annulation programmée est un résultat valide à réconcilier,
pas une raison de désactiver la protection. Aucun submit ne doit être répété.

### G. Réconciliation après restart/perte de flux

Couper/reprendre le flux dans un exercice contrôlé, exécuter `reconcile`, puis
`status`. Vérifier que chaque ordre, fill, position, solde et tentative ambiguë a
une résolution unique. Toute divergence non explicable reste en `MANUAL_REVIEW`.

### H. Runtime Testnet soutenu

```powershell
& $ExecutorPython -m hyperlab_testnet.cli run @AuthorizationArgs --database $Db
```

Superviser reconnexions, fraîcheur, débit, renouvellement du dead-man switch,
taille du journal et absence de secrets. Exercer `pause` puis, dans une campagne
séparée et approuvée, `kill`. Gate E ne peut être envisagée qu'après conservation
et revue humaine de tous les exercices requis ; elle reste distincte de
l'autorisation Testnet et de toute future autorisation argent réel.

```powershell
& $ExecutorPython -m hyperlab_testnet.cli pause `
  --database $Db --run-id <RUN_ID> --confirm TESTNET-PAUSE
& $ExecutorPython -m hyperlab_testnet.cli kill @AuthorizationArgs `
  --database $Db --run-id <RUN_ID> --confirm TESTNET-KILL
```

Conserver la sortie JSON et le code de sortie du kill. `runtime_state=KILLED` et
`account_kill_latched=true` prouvent le blocage durable ; seule la combinaison
`deadman_confirmed=true` et `deadman_outcome=DEADMAN_ARMED` confirme que cette
génération de DMS a été armée. Elle ne confirme toujours pas l'annulation effective
de chaque ordre.

## Ce que la Phase 13 ne permet jamais

- aucun endpoint ou fallback Mainnet ;
- aucun reçu `MICRO_MAINNET` ou `MAINNET` ;
- aucun flag `REAL_MONEY_EXECUTION_ENABLED_IN_BUILD` activé ;
- aucune conversion d'un reçu Paper/Testnet ;
- aucun transfert, retrait, bridge, staking ou gestion de clés ;
- aucun vault, sous-compte, builder fee ou approbation/délégation d'agent ;
- aucun ordre spot, market ou autonome : seulement un intent manuel perp limit
  `GTC|IOC|ALO` et son cancel explicite ;
- aucune commande opérateur `replace` ou génération autonome de stratégie ;
- aucun reset online du latch compte `KILLED` ;
- aucune commande `mainnet`, `live` ou `trade` générique ;
- aucun secret dans Umbrel, le dashboard, SQLite, Git ou les preuves ;
- aucune promotion économique fondée sur des fixtures ou des fills Testnet.

La préparation logicielle Phase 13 et la validation manuelle Testnet sont deux
étapes séparées. La seconde ne commence qu'après validation locale complète,
revue P0/P1, configuration opérateur dédiée et décision humaine explicite.
