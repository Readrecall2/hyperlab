# Prediction Markets Prospective Launch V1

Verdict technique visé :
`PREDICTION_MARKETS_PROSPECTIVE_LAUNCH_V1_GREEN_ROOT_MOUNT_HOME_NAMESPACE_FIXED_COMMITTED_LOCALLY_AWAITING_PUSH`.

Statut économique permanent avant preuve prospective réelle :
`ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE`.

Ce pack rend le candidat Polymarket + Kalshi installable par un humain sur un
hôte Linux, sans ajouter de transport de données, de moteur de replay ou de
route d'ordre. Il réutilise strictement `prediction-collect`, les envelopes raw,
les segments/manifests immuables, la récupération authentifiée et le ledger de
créneaux du Prediction Markets Candidate V1.

## Frontière

Le pack est `PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY` : aucun compte, credential,
wallet, seed, signer, API privée, ordre, amendement, annulation ou route argent
réel. Il ne contient aucune commande SSH exécutée par Codex. Tous les blocs VPS
sont des fichiers texte à lancer plus tard par l'opérateur.

La campagne H1 active n'est jamais sondée, arrêtée, redémarrée ou modifiée. Les
services, racines et ports Prediction Markets sont distincts. Le seul lien est
une réservation conservatrice de capacité : le preflight conserve l'intégralité
du budget H1 connu dans son calcul avant d'admettre le nouveau pack.

## Architecture

Trois unités persistantes et deux probes oneshot uniques sont rendus pour chaque
`run_slug` :

- `hyperlab-pm-<slug>-polymarket.service` : ordonnanceur Polymarket mono-writer ;
- `hyperlab-pm-<slug>-kalshi.service` : ordonnanceur Kalshi mono-writer ;
- `hyperlab-pm-<slug>-dashboard.service` : cockpit read-only sur
  `127.0.0.1:18081`.
- `hyperlab-pm-<slug>-<venue>-namespace-probe.service` : admission préalable,
  `Restart=no`, sous exactement le même durcissement filesystem que la venue.

Les deux collecteurs ont leurs propres racines `runs/`, `state.json`,
`ledger.jsonl`, locks, journaux systemd et politiques de restart. Un échec
DNS/403/429/maintenance d'une venue n'arrête jamais l'autre. Un créneau terminal
n'est jamais rejoué. Un crash sans résultat terminal récupère seulement les
segments déjà publiés; sinon le créneau devient explicitement
`PROCESS_ERROR_NO_TERMINAL_RECEIPT` ou `MISSING_SLOT_NO_BACKFILL`.

Le démarrage par défaut est immédiat après installation réussie. Pour un départ
explicite, définir `HYPERLAB_PM_START_AT_UTC` dans le shell Tabby avant le bloc B.
La valeur doit être UTC, comprise entre maintenant et +24 h. Elle ne permet
jamais un backfill.

Un reçu `PUBLIC_SOURCE_INVALID` n'est admis que si son JSON canonique, son plan,
ses identités, ses compteurs, son cutoff, son manifest/root raw et sa provenance
sont tous authentiques. Son champ `error` doit être non vide et borné. Le slot
est alors terminalement comptabilisé, exclu des données et de toute évaluation
économique, puis le service attend le slot suivant sans rejeu. Une corruption de
l'un de ces contrôles reste `INTEGRITY_FAILED`, code 4, avec
`RestartPreventExitStatus=4`.
Une campagne ne contenant que des slots invalides reste
`INSUFFICIENT_PUBLIC_CORPUS`; ces reçus ne prouvent jamais une source exploitable.

### Correctif du bootstrap moniteur

La tentative historique `pm-20260827t183515z-664bef6e` a prouvé le preflight
hôte puis a refusé B avant tout démarrage de collecteur. La cause primaire était
`ModuleNotFoundError: No module named 'numpy'` : `monitor.sh` lançait le Python
système avec `-I`, alors que le graphe d'import cockpit → runner → candidat →
backtest exige NumPy, attesté uniquement dans le venv offline. L'exception était
capturée, puis le moniteur appelait hors du `try` un helper jamais lié et
affichait le `NameError` secondaire `prepared_state_is_stale`.

Le moniteur est maintenant lié au chemin canonique du script et exécute
exclusivement `<source_root>/.venv/bin/python -I`. Il lie ce source root au
handoff authentifié avant tout import. Les imports des helpers et la validation
des preuves initiales sont deux phases distinctes. Toute absence de runtime,
erreur d'import ou divergence handoff/preflight/manifest/activation produit un
JSON borné fail-closed avec `alert=true`, `operational_failure=true`,
`preflight_error` et une classification explicite, sans traceback masquant la
cause primaire.

B ne démarre aucun collecteur avant d'avoir simultanément prouvé le endpoint
loopback readonly, `orders_enabled=false`, le PID et la commande systemd exacts,
le chemin d'unité exact et que ce PID possède réellement le listener IPv4
`127.0.0.1:18081`. Le premier refus de connexion ou 503 de démarrage est toléré
par une attente bornée; l'expiration ou toute preuve divergente désarme
uniquement le dashboard de cette tentative.

La liste des collecteurs à activer provient ensuite du même JSON moniteur
authentifié, jamais d'une relecture mutable de l'incoming root. Son parseur
propage son code d'échec avant tout collecteur. La liste vide reste un état
honnête `BOTH_UNAVAILABLE` : le dashboard peut rester read-only, les deux unités
collecteur restent inactives et `PREDICTION_ELIGIBLE_VENUES=NONE` est explicite.

### Correctif runtime pack V2 : LF immuable et préparation transactionnelle

La tentative `pm-20260828t144733z-1e4c8ac6` reste une preuve historique
intouchable. Son pack a copié les octets CRLF du checkout Windows au lieu des
blobs LF du commit. Bash a donc lu le caractère `CR` de
`set -Eeuo pipefail\r` comme une partie du nom d'option et a refusé avec
`set: p: invalid option name`. `bash -n` ne pouvait pas découvrir ce défaut :
il parse la grammaire mais n'exécute pas le builtin `set`. Le test causal lance
désormais réellement le bootstrap matérialisé sous Git Bash, au-delà de cette
ligne. Son pack, son incoming, son source root partiel et toutes ses preuves
restent intacts et ne seront jamais réutilisés. L'ancienne campagne désarmée
reste elle aussi immuable; l'exécution E bloquée n'a fourni aucun signal GREEN,
donc son état final ne doit pas être supposé sans le diagnostic humain read-only.

Tous les fichiers repo transférés sont désormais lus comme blobs binaires du
`source_commit` exact. Le résultat ne dépend plus du cwd, de l'OS ni de
`core.autocrlf`. Chaque `.sh` suivi ou généré est contrôlé avant écriture et
après matérialisation : UTF-8 sans BOM, aucun NUL, aucun `CR`, shebang exact
`#!/usr/bin/env bash` et LF final. Le bloc A réapplique le même contrôle sur le
pack local puis sur l'incoming distant, en plus de chaque taille et SHA-256. La
règle `.gitattributes` impose aussi `text eol=lf` aux shells du launch pack.

B ne désarme plus l'ancienne campagne pendant la préparation. Il authentifie
une seule fois sudo au premier plan, puis toute commande interne privilégiée
est `sudo -n`. Un keepalive `sudo -n -v` maintient ce cache sans prompt pendant
le travail et est toujours arrêté par le trap de sortie. Pendant que l'ancienne
campagne reste active, il termine le
preflight hôte, authentifie que le listener 18081 appartient exactement à
l'ancien dashboard attendu, vérifie bundle/source/capacité/NTP/ext4, crée un
clone neuf, exécute le bootstrap offline réel, prouve le venv et les imports.
Après `PREDICTION_RUNTIME_PREPARED_BEFORE_CUTOVER`, il réauthentifie immédiatement
les cinq anciennes unités et leurs preuves. Ce n'est qu'alors qu'il écrit le
reçu pré-mutation, désarme l'ancien slug, exige zéro collecteur concurrent et le
port libre, puis active le nouveau. Tout échec antérieur laisse l'ancienne
campagne active et n'émet pas la demande E; tout échec postérieur émet exactement
`PREDICTION_NEW_ACTIVATION_FAILED_RUN_E_RESTORE_OLD`.

### Correctif runtime pack V3 : admission d'import Python isolée

La tentative `pm-20260828t161704z-2624806a` reste intégralement immuable, y
compris son incoming et son source root partiel. Elle a terminé le bootstrap LF
et créé le venv, puis a échoué avant cutover avec `No module named 'hyperlab'`.
La commande fautive combinait `PYTHONPATH=SOURCE_ROOT/src:SOURCE_ROOT` et
`python -I`; le mode isolé ignore volontairement `PYTHONPATH`. Le bootstrap
n'installe pas HyperLab comme wheel ou paquet editable : cet import était donc
impossible par construction. L'ancienne campagne étant restée active, aucune
restauration E n'était requise pour cette tentative.

Le sous-contrat `runtime-import-admission` conserve `-I` et
`PYTHONNOUSERSITE=1`. Il authentifie le handoff, le commit, l'inventaire Git
complet et propre, les racines canoniques non symlinkées et leur device, puis
insère explicitement et uniquement `SOURCE_ROOT/src` et `SOURCE_ROOT` dans
`sys.path`. Il importe les modules HyperLab/ops nécessaires et les dépendances
runtime, vérifie les helpers utilisés par le runner et le cockpit, puis contrôle
chaque `module.__file__` chargé : fichiers HyperLab exacts sous le source,
dépendances sous le `site-packages` du venv, stdlib seulement sous ses racines
interpréteur. Cwd implicite, user-site, site-packages global, ancien source,
symlink ou origine hors allowlist sont refusés. Le résultat est un unique JSON
canonique auto-hashé, lié au commit/inventaire et terminé par
`PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN`.

B exécute réellement ce contrat avec le venv frais, depuis le fichier Git
canonique `SOURCE_ROOT/ops/prediction_markets_launch_v1/preflight.py` et sous
une borne de 180 secondes, avant `verify-old` et
`disarm-old`. Le même contrat précède l'installation, chaque monitor, le
démarrage des trois services, le resume/recovery et l'admission runner; la
réauthentification de l'ancienne campagne l'utilise aussi avant sa preuve de
ledger. Les unités bornent leur phase de démarrage à 180 secondes. Aucun retrait
de `-I`, aucune installation réseau/editable, aucun system-site-packages et
aucune confiance implicite dans le cwd ou `PYTHONPATH` ne sont admis.

Le script d'admission est lui-même authentifié comme fichier régulier,
non-symlinké, présent une seule fois dans l'inventaire Git exact et
byte-identique à son blob. Les contrôles strictement pré-clone restent exécutés
depuis la copie `incoming`, puisque le clone n'existe pas encore; aucune copie
`incoming`, externe, ancienne ou non inventoriée n'est admise après clone.

La restauration historique qui a dépassé trente minutes était bloquée avant
toute mutation dans une substitution de commande
`timeout 5 sudo systemctl show ...` : `sudo` et son `timeout` parent étaient tous
deux stoppés par l'interaction TTY, alors que `systemctl list-jobs` ne montrait
aucun job. E effectue maintenant une unique authentification sudo synchrone au
premier plan, puis le même keepalive strictement non interactif. Ensuite, les
lectures `systemctl show`, `is-enabled` et
`list-units` n'utilisent jamais sudo; chaque mutation utilise
`sudo -n timeout ... systemctl`, avec progression BEGIN/GREEN, borne extérieure
de secours et diagnostic terminal `operation=<...>:service=<...>`. Un timeout
ne produit jamais de GREEN. Stop/disable/enable/start et les probes oneshot sont
reprenables : une relance du même mode saute seulement les états déjà terminaux,
sans supprimer ni réécrire raw, manifest, receipt, ledger ou slot.

Avant de reprendre E après Ctrl+C ou timeout, le diagnostic humain strictement
read-only est : `systemctl list-jobs --no-pager`, puis `systemctl show` sur les
cinq unités anciennes et les cinq unités nouvelles avec
`LoadState,ActiveState,SubState,Result,MainPID,NRestarts,FragmentPath`,
`systemctl is-enabled` pour chacune, `systemctl list-units --type=service
--state=active --no-pager 'hyperlab-pm-*'` et `ss -H -ltnp 'sport = :18081'`.
Ne pas employer de glob mutateur. Conserver la sortie, puis relancer exactement
`bash operator/E-recovery-rollback.sh restore-old`. Le signal terminal n'est
valide qu'après la seconde vérification bornée des trois services, des deux
probes, du listener, PID/commande, `NRestarts=0` et de l'absence de nouveau
collecteur :
`PREDICTION_OLD_CAMPAIGN_RESTORE_VERIFIED_NO_NEW_COLLECTOR`.

## Capacité et règle de cohabitation H1

Le preflight exige simultanément :

```text
réservation H1 complète       154 618 822 656 octets (144 GiB)
budget raw Prediction Markets  22 548 578 304 octets (21 GiB)
marge de sécurité              17 179 869 184 octets (16 GiB)
minimum libre total           194 347 270 144 octets (181 GiB)
```

Le volume 200 GB n'est jamais supposé disponible. Le script découvre avec
`findmnt` et `df -PB1` le montage réel, le device, ext4, les options `rw` et les
octets libres. Si le minimum n'est plus disponible, le verdict est
`PREDICTION_CAPACITY_REFUSED_COEXISTENCE_NOT_PROVEN` avec recommandation d'un
hôte ou volume ext4 distinct. Aucun chemin H1 n'est lu pour obtenir ce verdict.

L'essai `73c6d2d2` annonçait environ 296,3 GB disponibles pour
`194 347 270 144` octets requis. Cette valeur est historique, pas une admission
future. Après bootstrap, B
réauthentifie le handoff, le source, les inventaires et les unités, puis mesure
à nouveau NTP/montage/capacité avant toute mutation systemd. Il refait encore le
contrôle NTP/montage/capacité juste avant le premier collecteur. Si H1 ou les
preuves historiques ont consommé la marge, B doit refuser et demander d'agrandir
ou de choisir un autre volume ext4; il est interdit de réduire les réserves, de
supprimer une tentative ou de contourner ce gate.

Chaque runner recalcule ensuite la réservation avant chaque créneau. Il ne lit
que son propre ledger : le budget de l'autre venue reste donc volontairement
réservé en double dans ce calcul conservateur, et une corruption ou écriture
concurrente de l'autre venue ne peut pas l'arrêter. Une perte de marge arrête
uniquement la venue concernée fail-closed.

À chaque démarrage ou redémarrage systemd, avant même la sélection du prochain
ordinal, le runner réauthentifie le handoff, l'admission d'installation, le
transfert, le commit/inventaire source, l'utilisateur/HOME, NTP, les racines
canoniques et le même device ext4. Le budget ledger-accounted est contrôlé
juste après. Un refus sort en code 4 avant tout enfant de collecte et
`RestartPreventExitStatus=4` interdit la boucle de restart.

Dans le namespace systemd durci, le mount du volume admis doit rester le target
exact read-only. systemd peut coalescer les `ReadOnlyPaths` imbriqués devenus
redondants : `volume_base`, source et campagne doivent alors résoudre seulement
vers le mount volume, le `volume_base` ou leur target exact suivant une allowlist
fixe. Pour l'incoming, le target observé doit être exactement l'incoming ou un
membre canonique de sa chaîne d'ancêtres entre `/home` et cet incoming. Lorsque
`/home` appartient au filesystem racine, systemd peut aussi exposer `TARGET=/`
pour les vues RO de `/home` et de l'incoming. Ce cas n'est admis que si le chemin
logique `/home`, chaque membre canonique jusqu'à l'incoming, `SOURCE`, `FSROOT`,
le fstype ext4 et le couple `MAJ:MIN`/`stat(2)` authentifient tous le même
filesystem racine, avec `ro` présent et `rw` absent. Le target `/` reste refusé
pour le volume `/dev/sdb`, pour toute vue RW ou si `/home` est un montage
distinct. Un autre home, un cousin, un descendant arbitraire et tout symlink
sont refusés.
L'identité est liée au même superblock ext4 par `MAJ:MIN` concordant avec
`stat(2)`, ainsi qu'à la relation `SOURCE`/`FSROOT` dérivée du target effectif.
`ReadOnlyPaths` ne modifie pas H1. Seul `campaign_root/<venue>` doit être un bind
exact `rw` vers le sous-chemin attendu.
Le probe oneshot y effectue une création exclusive, fsync fichier, fsync
répertoire, suppression et second fsync; le runner répète cette preuve avant la
sélection d'un ordinal. Un device, bind, fstype, chemin ou fsync divergent refuse
avant toute collecte.

## Construction locale du bundle

Après le commit final propre, depuis Windows PowerShell local :

```powershell
$commit = git rev-parse HEAD
$runSlug = 'pm-20260827t120000z-deadbeef' # choisir une valeur neuve
$outputRoot = "D:\hyperlab-evidence\$runSlug"
& '.\ops\prediction_markets_launch_v1\New-PredictionMarketsLaunchBundle.ps1' `
  -Commit $commit `
  -RunSlug $runSlug `
  -OutputRoot $outputRoot
```

Le générateur exige la branche exacte, le commit exact, la base autoritaire dans
son ascendance et un worktree propre. Il
crée et vérifie un Git bundle, construit sur Windows un wheelhouse Linux x86_64
multi-tags `manylinux_2_28` + `manylinux_2_17` strictement depuis
`requirements-runtime.lock` avec `--require-hashes`, puis authentifie chaque
wheel. Cette union est requise par les tags complémentaires du lock; le
preflight refuse néanmoins une glibc inférieure à 2.28. Sur le VPS,
l'installation utilise uniquement
`--no-index`; aucun pip réseau ni `system-site-packages` n'est admis.

Chaque tentative exige un nouveau `run_slug`, un nouvel output local, un nouvel
incoming root, un nouveau clone et une nouvelle campaign root. Aucun ancien run
n'est supprimé, écrasé ou réutilisé.

Le pack final contient également un `README.md` non expert lié au run, au commit,
aux trois racines, aux trois services persistants et aux deux probes exacts. Il fait partie de
`transfer-inventory.json` et est donc vérifié avant activation.

## Blocs opérateur générés, dans l'ordre

Le répertoire final contient cinq fichiers distincts, avec chemins, services et
hashes exacts :

1. `operator/A-windows-bundle-verify-transfer.ps1` — Windows PowerShell,
   vérification et transfert vers le nouvel incoming root ;
2. `operator/B-tabby-preflight-install-activate.sh` — Tabby/VPS Bash, preflight
   synchrone puis installation offline et activation ;
3. `operator/C-tabby-readonly-monitor.sh` — second onglet Tabby, monitoring
   automatique read-only, arrêt à la première transition/alerte ;
4. `operator/D-windows-dashboard-tunnel.ps1` — tunnel SSH loopback 18081 et URL ;
5. `operator/E-recovery-rollback.sh` — reprise ou désarmement ciblé, sans
   supprimer raw, manifests, ledgers, runs ou unités.

Les blocs A et D lisent la cible SSH depuis `HYPERLAB_PM_SSH_TARGET`. Le bloc A
exige en plus le chemin local de la clé dédiée dans `HYPERLAB_PM_SSH_KEY`, résout
la racine canonique comme le parent de son dossier `operator`, puis vérifie
localement le bundle, `handoff.sha256` et `wheelhouse.sha256` avant toute commande
SSH. Les vérifications Git locale, distante et du preflight B utilisent chacune
un dépôt bare temporaire neuf : elles restent valides depuis un cwd non-Git et
nettoient uniquement leur racine temporaire bornée. Aucune adresse, credential
ou clé n'est inscrite dans le bundle. Chaque
fichier annonce lieu, durée attendue/maximale, prompts, effet de Ctrl+C et signal
terminal.

Dans B, un Ctrl+C pendant la préparation laisse l'ancienne campagne active. Un
Ctrl+C après le reçu pré-mutation peut laisser uniquement un sous-ensemble des
unités Prediction Markets dans un état partiel ; E `restore-old` reprend alors
la restauration. E annonce 2–8 minutes en moyenne et un maximum borné de
45 minutes couvrant les bornes d'arrêt des dix unités possibles sans prétendre
qu'une interruption les a toutes arrêtées. Chaque interruption imprime le mode
exact à relancer. Aucune de ces actions ne supprime les preuves ni ne cible H1.

Le source, l'incoming et la campagne historiques de
`pm-20260827t183515z-664bef6e` restent des preuves immuables. Le rollback humain
a rendu `PREDICTION_ROLLBACK_DISARMED_RAW_PRESERVED`. Aucun nouveau pack ne peut
réutiliser ce slug ou l'une de ces racines.

La tentative `pm-20260827t210353z-9fa239e4` et toutes ses racines restent aussi
immuables. Elle a prouvé le dashboard et l'activation guard, puis les deux
collecteurs ont refusé avant state/ledger parce que l'ancien runner exigeait à
tort le parent `rw` dans le namespace systemd. Son rollback humain est acquis.

La tentative `pm-20260827t234404z-73c6d2d2` et toutes ses racines restent
également immuables. Tous ses contrôles hôte et l'activation guard étaient verts,
mais le premier probe Kalshi a refusé un ancêtre intermédiaire authentique de
l'incoming à cause de l'ancienne allowlist littérale. Aucun collecteur n'a
démarré et la cleanup ciblée a désarmé les cinq unités sans supprimer de preuve.

La tentative `pm-20260828t010120z-9e2987aa` et toutes ses racines restent elles
aussi immuables. Les admissions hôte et volume étaient vertes, puis le premier
probe Polymarket a observé la représentation Linux authentique
`TARGET=/`, `SOURCE=/dev/sda1`, `FSTYPE=ext4`, `MAJ:MIN=8:1`, `FSROOT=/` et
`VFS_OPTIONS=ro,nosuid,relatime` pour le chemin logique `/home`. L'ancien appel
exigeait prématurément `TARGET=/home` et refusait avant la preuve `stat(2)`.
Aucun service persistant n'a été activé et la cleanup ciblée a préservé toutes
les preuves. Le correctif ne rend aucune nouvelle surface writable : les vues
`/home`/incoming restent strictement RO et la seule surface RW reste le
sous-répertoire exact de la venue sur `/dev/sdb`.

La tentative `pm-20260828t013545z-c15607ae` et toutes ses racines restent
également immuables. Son probe Polymarket a authentifié le namespace complet,
dont la vue root-backed RO de `/home`, le volume `/dev/sdb` RO et la seule venue
RW avec le write probe durable. Le wrapper B l'a pourtant refusé après la fin
réussie du `Type=oneshot` parce qu'il exigeait littéralement
`ExecMainCode=1`; ce systemd a exposé `ExecMainCode=0`, tout en attestant
`Result=success`, `ExecMainStatus=0`, `inactive/dead`, `MainPID=0`, zéro restart
et le fragment exact. B accepte désormais uniquement les deux représentations
`ExecMainCode` 0 ou 1 compatibles avec une fin normale réussie lorsque tous ces
invariants concordent. Il authentifie en plus l'unique JSON canonique GREEN du
probe, son identité venue/service, ses sept preuves mount et son write probe;
un payload absent, ambigu, malformé ou refusé désarme les cinq unités avant tout
service persistant.

## Preflight cible

Avant le cutover, pendant que l'ancienne campagne reste active, le bloc B
vérifie de façon bornée :

- handoff, inventaire de transfert, Git bundle, arbre Git et SHA-256 ;
- CPython 3.12 x86_64, glibc >= 2.28 et primitives stdlib/venv/SSL ;
- wheelhouse complet et hashé, sans environnement système ;
- utilisateur `hyperlab`, HOME et chemins réels ;
- parents dédiés `volume_base`, `sources/`, `campaigns/` non-symlinks, chemins
  réels exacts, propriétaire `hyperlab` et mode `0700` réattestés avant clone ;
- ext4 réel `rw`, capacité avec réservation H1, NTP ;
- port loopback 18081 occupé exactement par l'ancien dashboard authentifié,
  absence des cinq nouvelles unités et nouveauté des racines exactes ;
- DNS officiel via un processus `getent` borné à 3 s par host, puis TLS/HTTPS
  officiels, une seule tentative totale bornée à 40 s par venue ;
- WebSocket public Polymarket ; Kalshi WSS reste
  `NOT_EXECUTED_CREDENTIALS_FORBIDDEN` car son handshake documenté exige une
  authentification ;
- probe de création/fsync/suppression dans la seule base Prediction Markets,
  après admission read-only et avant clone/activation.

Un HTTP 403/429 ou échec DNS est attaché à sa venue. L'installation du cockpit et
de l'autre venue reste admissible. Une unité inéligible est installée mais non
activée; le bloc recovery ne peut la démarrer que lors d'une nouvelle décision
humaine et après un nouveau preflight propre à cette venue.

## Cockpit read-only

Le cockpit n'accepte que GET/HEAD. Toutes les réponses déclarent
`mode=readonly`, `orders_enabled=false`; aucune route de commande n'existe. Il
affiche synthèse puis détails Polymarket/Kalshi : service/venue, DNS/connectivité,
frames/segments/octets, gaps/duplicates/reconnects, fraîcheur, dernier
manifest/root, capacité et reprise. Une métrique absente est `NON DISPONIBLE`,
jamais zéro.

Le holdout reste `SEALED`, sans lecture ni téléchargement de métrique dérivée.
Les lectures refusent symlinks, fichiers spéciaux, traversées, payloads trop gros
et incohérences avant/après. Les téléchargements sont une allowlist fixe de six
artefacts techniques. La readiness exige le preflight et le reçu d'activation
liés au `campaign_id`, au SHA logique du manifest, au commit et à la racine; le
moniteur authentifie aussi le ledger et le state avant toute classification.
Il refuse avant lecture une campagne, `state/` ou un répertoire de venue lié,
spécial ou non canonique, afin qu'un alias filesystem ne puisse jamais produire
un faux GREEN.
Comme la boucle du runner se réveille au plus tard toutes les 30 secondes, un
state encore `PREPARED` plus de 35 secondes après `starts_at_utc` devient
`PREPARED_STALE` : le cockpit refuse alors sa readiness et le moniteur publie
une alerte opérationnelle au lieu d'afficher `RUNNING`. Ce blocage n'est pas
confondu avec une corruption `INTEGRITY_FAILED`.

Fixtures synthétiques de QA : `PREPARED`, `BOTH_RUNNING`,
`POLYMARKET_UNAVAILABLE_KALSHI_RUNNING`,
`KALSHI_UNAVAILABLE_POLYMARKET_RUNNING`, `BOTH_UNAVAILABLE`,
`STALE_RECONNECTING`, `INTEGRITY_FAILED`, `INTERRUPTED_RECOVERABLE`,
`COMPLETE_WINDOW`, `HOLDOUT_SEALED`, `POLYMARKET_PUBLIC_SOURCE_INVALID`,
`KALSHI_PUBLIC_SOURCE_INVALID`, `BOTH_PUBLIC_SOURCE_INVALID`, états mixtes
invalid/unavailable et `CAPACITY_REFUSED`. Elles ne constituent aucune preuve
économique. Toute métrique issue d'un slot invalide reste `NON DISPONIBLE` même
si son reçu conserve honnêtement des compteurs, un manifest et un root.

## Recovery et rollback

`bash operator/E-recovery-rollback.sh recovery` revalide d'abord le handoff,
l'inventaire transféré, le HEAD Git propre, l'inventaire source, NTP, ext4 `rw`,
la réserve de capacité, les racines et imports offline. Il
redémarre ensuite le cockpit, refait un preflight public borné séparé pour
Polymarket et Kalshi, puis ne redémarre que les collecteurs de ce handoff dont le
nouveau verdict est vert. Les rapports de refus sont conservés dans l'incoming
root. Les ledgers authentifiés déterminent les créneaux déjà terminaux et
interdisent leur rejeu. Après une reprise partielle, une exécution suivante
tolère uniquement l'autre collecteur déjà actif si son unité, sa commande, son
state et son ledger sont tous authentifiés.

La reprise exige aussi le `FragmentPath` exact pour les trois unités, et pour le
dashboard le PID/commande ainsi que la possession du listener loopback. Son gate
final exige `activation_admissible=true` et `operational_failure=false`; il ne
refuse pas une simple alerte de qualité `PUBLIC_SOURCE_INVALID` authentique, mais
ne peut jamais publier le signal de reprise sur une divergence opérationnelle.

`bash operator/E-recovery-rollback.sh rollback` arrête et désactive uniquement
ces trois services et les deux probes oneshot du même slug. Il ne supprime ni
unité, source, venv, campagne, raw, manifest, ledger, run ou rapport; il ne nomme
aucun service H1. À l'installation, B exécute et authentifie désormais les deux
probes avant d'activer le dashboard ou un collecteur. Un refus expose de façon
bornée `TARGET`, `SOURCE`, `FSTYPE`, `VFS_OPTIONS`, `MAJ:MIN`, `FSROOT` et le
chemin logique, puis laisse tous les services persistants inactifs.

## Limites

- Aucun probe public n'a été rejoué pendant la construction de ce pack.
- Les échecs DNS Windows acquis restent locaux à cet environnement historique.
- La première disponibilité réelle, les volumes réellement libres, NTP, ports,
  services et imports Linux ne peuvent être attestés qu'au preflight humain.
- Le parseur borné `/proc/net/tcp` → `/proc/<pid>/fd` est couvert offline avec
  métadonnées synthétiques; seule l'exécution humaine Linux atteste le `/proc`
  réel de la nouvelle tentative.
- `schedule_accounted` et un logiciel opérationnel ne valent pas corpus
  économique complet, alpha, rentabilité, capacité ni autorisation d'ordre.
