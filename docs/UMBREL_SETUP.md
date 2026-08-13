# Installation et exploitation sur umbrelOS

## Frontière de sécurité

Le paquet Umbrel `0.2.1` exécute uniquement un collecteur de données publiques et
un dashboard read-only. Il ne contient ni clé, ni wallet, ni signer, ni client privé,
ni exécuteur d'ordre. Les phases 10 à 14 restent bloquées ; ce déploiement n'est pas
une validation économique ou opérationnelle du trading.

Le dashboard est accessible uniquement via le proxy Umbrel. Le collecteur seul possède
un réseau avec egress public ; aucun port hôte ne lui est publié. Les services applicatifs
sont non-root, rootfs read-only, `cap_drop: ALL`, sans privilège supplémentaire et sans
Docker socket.

## 1. Prérequis

- un dépôt GitHub public sans secret ;
- GitHub Container Registry pour l'image `linux/amd64` et `linux/arm64` ;
- Ruff, mypy et pytest entièrement verts ;
- un support de backup durable situé hors du répertoire de l'application Umbrel ;
- les outils Cosign, GitHub CLI, Docker Buildx et Trivy sur la machine qui vérifie la
  release ; une authentification en lecture à GHCR et à GitHub si le dépôt est privé.

Avant de pousser le premier tag, configurez côté GitHub :

- un ruleset qui interdit aux contributeurs ordinaires de créer, modifier ou supprimer
  `refs/tags/v*`, protège la branche par défaut et `.github/workflows/container.yml`,
  exige les checks CI et une revue humaine ;
- l'environment `signed-release`, référencé par le job `publish`, avec au moins un
  required reviewer indépendant et sans droit de contournement administrateur ;
- des règles qui interdisent le force-push et la suppression de la branche protégée.

Ce contrôle n'est pas contenu dans Git. Exportez le ruleset et les paramètres de
l'environment comme preuve de release. Tant qu'ils ne sont pas configurés et relus, le
pipeline est **BLOCKED** : un simple rôle write ne doit pas pouvoir créer un tag et
obtenir seul `packages: write` et un jeton OIDC de signature.

Ne configurez aucun secret GitHub ou Umbrel pour cette version. Cosign utilise l'OIDC
éphémère du workflow, pas une clé de signature longue durée.

## 2. Publier le candidat signé

Le commit tagué conserve les placeholders contrôlés : le digest multi-architecture
n'existe pas encore. Préparez le candidat sur une branche de release afin qu'un échec
ne modifie jamais la branche de store actuellement déployable.

```powershell
.\.venv\Scripts\python.exe scripts\prepare_umbrel_store.py --reset-template
.\.venv\Scripts\python.exe scripts\verify_manifest.py --write
.\.venv\Scripts\python.exe scripts\verify_release.py --template --tag v0.2.1 --check-manifest
git tag v0.2.1
git push origin v0.2.1
```

Le workflow doit réussir le préflight complet, Gitleaks et les scans Trivy `amd64` et
`arm64` **avant** d'obtenir le droit de pousser. Il publie ensuite le manifest, la
provenance, le SBOM, les attestations et la signature Cosign. Un build Docker vert ne
suffit pas.

Dans GitHub, vérifiez que **Publish signed container** est intégralement vert, que le
package public `hyperlab:0.2.1` contient les deux architectures, puis relevez le digest
du manifest multi-architecture, pas celui d'une seule architecture. Téléchargez depuis
ce run l'artefact `signed-release-receipt-v0.2.1` dans un répertoire neuf :

```powershell
New-Item -ItemType Directory -Path .\release-evidence -ErrorAction Stop
gh run download RUN_ID --repo OWNER/REPO `
  --name signed-release-receipt-v0.2.1 `
  --dir .\release-evidence
```

```text
cosign verify ghcr.io/OWNER/hyperlab@sha256:DIGEST \
  --certificate-identity https://github.com/OWNER/REPO/.github/workflows/container.yml@refs/tags/v0.2.1 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
gh attestation verify oci://ghcr.io/OWNER/hyperlab@sha256:DIGEST --repo OWNER/REPO
trivy image --exit-code 1 --ignore-unfixed=false --severity HIGH,CRITICAL \
  ghcr.io/OWNER/hyperlab@sha256:DIGEST
```

Toute vérification manquante ou en échec bloque la suite.
Le digest de base épinglé doit lui aussi passer le run Trivy réel au moment de publier ;
un digest immuable peut être vulnérable. Aucun résultat de scan n'est simulé par cette
configuration statique.

## 3. Épingler la branche de store au digest vérifié

Les fichiers `umbrel-app-store.yml` et `jjlab-hyperlab/` restent à la racine du dépôt.
N'injectez le digest qu'après tous les gates et ne poussez jamais un store contenant
des placeholders.

```powershell
$Digest = "REMPLACER_PAR_64_HEXADECIMAUX_SANS_SHA256"
.\.venv\Scripts\python.exe scripts\prepare_umbrel_store.py VOTRE_NOM_GITHUB `
  --repository hyperlab `
  --image-version 0.2.1 `
  --image-digest $Digest `
  --release-receipt .\release-evidence\release-receipt.json `
  --receipt-bundle .\release-evidence\release-receipt.sigstore.json
.\.venv\Scripts\python.exe scripts\verify_manifest.py --write
git diff -- MANIFEST_SHA256.txt jjlab-hyperlab
.\.venv\Scripts\python.exe scripts\verify_release.py --prepared --tag v0.2.1 --check-manifest
Get-ChildItem .\jjlab-hyperlab -Recurse -File | Select-String "REPLACE_WITH_"
```

La dernière commande ne doit rien retourner. Les services dashboard et collector
doivent référencer exactement le même `0.2.1@sha256:DIGEST`. Faites relire le diff puis
commitez la branche de store. Pour la release suivante, créez une branche dédiée qui
remet le package en état template ; la branche de store conserve le dernier digest sain
jusqu'à publication et vérification du nouveau.

Le préparateur échoue avant toute écriture si le reçu ou son bundle manque, si leur
identité OIDC ne correspond pas au workflow/tag exact, si la provenance de l'index ou
un SBOM `amd64`/`arm64` manque, si le gate Trivy n'est pas `pass`, ou si le tag
`0.2.1` du registre ne résout pas exactement vers le digest signé. Un digest copié à la
main n'est donc pas une preuve de release et ne peut pas préparer le store.

Sans Docker ou Umbrel local, la validation reste seulement statique : exécutez les
vérificateurs ci-dessus et `docker compose config` sur une copie préparée. Ne prétendez
pas avoir validé l'installation, le redémarrage ou la release réelle dans ce cas.

## 4. Installer le Community App Store

Dans umbrelOS :

1. ouvrez **App Store**, puis **Community App Stores** ;
2. ajoutez uniquement l'URL de votre dépôt relu ;
3. ouvrez `Jj HyperLab` et vérifiez la version `0.2.1` ;
4. relisez encore le `docker-compose.yml` préparé ;
5. installez l'application.

Les stores communautaires ne sont pas audités par Umbrel. N'utilisez pas un dépôt tiers
et ne remplacez jamais le digest par `latest`.

## 5. Vérifier installation, liveness et readiness

Le dashboard doit afficher `READ-ONLY — ORDRES IMPOSSIBLES`. Vérifiez :

```text
/health/live
/ready
/api/status
```

`/health/live` confirme uniquement que le processus répond. `/ready` doit rester en 503
tant que le collecteur n'est pas frais, connecté et sans canal stale, ou si l'état
SQLite est illisible. En état sain, les réponses conservent :

```json
{"ok": true, "mode": "readonly", "orders_enabled": false}
```

Le dashboard ne possède aucune route mutative ni bouton d'ordre. Les rapports sous
`reports/` sont téléchargeables en lecture seule.

Le collecteur reçoit `SIGINT` et `SIGTERM`, puis dispose de 60 secondes pour fermer le
socket, flusher et publier son statut final. Après un arrêt forcé ou une coupure de
courant, il signale la session non propre, vérifie l'intégrité et ne redevient ready
qu'après reprise saine. Un test d'arrêt brutal local ne prouve pas le comportement du
matériel Umbrel réel.

## 6. Racine persistante unifiée

Les montages runtime sont séparés, mais l'outil d'opérations doit voir une racine
unifiée contenant exactement :

```text
backups/  config/  market/  paper/  reports/  runtime/
```

Sur l'hôte, cette racine est `${APP_DATA_DIR}/data`. Déterminez le chemin réel fourni
par umbrelOS ; ne le devinez pas. Les commandes ci-dessous montent cette racine sur
`/persistent` et utilisent l'image **signée et épinglée par digest**.

Avant toute maintenance, arrêtez l'application depuis l'interface umbrelOS, puis
confirmez qu'aucun conteneur du projet n'est actif. `ops backup` et
`ops export-parquet` refusent un writer actif ou une session qui ne s'est pas arrêtée
proprement.

```bash
export APP_DATA_DIR=/chemin/umbrel/app-data/jjlab-hyperlab
export HYPERLAB_EXTERNAL_BACKUP=/mnt/support-externe/hyperlab-0.2.1
export HYPERLAB_IMAGE=ghcr.io/OWNER/hyperlab:0.2.1@sha256:DIGEST

test -d "${APP_DATA_DIR}/data"
mkdir -p "${HYPERLAB_EXTERNAL_BACKUP}"
chmod 0700 "${HYPERLAB_EXTERNAL_BACKUP}"
APP_REAL=$(realpath "${APP_DATA_DIR}")
BACKUP_REAL=$(realpath "${HYPERLAB_EXTERNAL_BACKUP}")
case "${BACKUP_REAL}" in "${APP_REAL}"|"${APP_REAL}"/*) exit 2;; esac
docker ps --filter label=com.docker.compose.project=jjlab-hyperlab
```

La dernière commande ne doit lister aucun service actif. Le support externe doit être
inscriptible par UID/GID `1000:1000` et disposer d'assez d'espace pour une copie
complète plus la réserve fail-closed.

## 7. Backup externe, vérification et restore-smoke

Contrôlez d'abord la persistance et sa configuration read-only :

```bash
docker run --rm --network none --user 1000:1000 --read-only \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 128 \
  --memory 1g --cpus 1 --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
  --mount type=bind,src="${APP_DATA_DIR}/data",dst=/persistent \
  "${HYPERLAB_IMAGE}" ops check-layout /persistent --writable
```

Créez directement la sauvegarde hors de `${APP_DATA_DIR}` :

```bash
docker run --rm --network none --user 1000:1000 --read-only \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 128 \
  --memory 1g --cpus 1 --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
  --mount type=bind,src="${APP_DATA_DIR}/data",dst=/persistent \
  --mount type=bind,src="${HYPERLAB_EXTERNAL_BACKUP}",dst=/external \
  "${HYPERLAB_IMAGE}" ops backup /persistent --backup-root /external \
  --backup-id pre-update-v0.2.1
```

Vérifiez-la, puis restaurez-la vers une racine neuve également externe :

```bash
docker run --rm --network none --user 1000:1000 --read-only \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 128 \
  --memory 1g --cpus 1 --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
  --mount type=bind,src="${HYPERLAB_EXTERNAL_BACKUP}",dst=/external \
  "${HYPERLAB_IMAGE}" ops verify-backup /external/backup-pre-update-v0.2.1

test ! -e "${HYPERLAB_EXTERNAL_BACKUP}/restore-smoke-v0.2.1"
docker run --rm --network none --user 1000:1000 --read-only \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 128 \
  --memory 1g --cpus 1 --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
  --mount type=bind,src="${HYPERLAB_EXTERNAL_BACKUP}",dst=/external \
  "${HYPERLAB_IMAGE}" ops restore /external/backup-pre-update-v0.2.1 \
  /external/restore-smoke-v0.2.1

docker run --rm --network none --user 1000:1000 --read-only \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 128 \
  --memory 1g --cpus 1 --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
  --mount type=bind,src="${HYPERLAB_EXTERNAL_BACKUP}",dst=/external:ro \
  "${HYPERLAB_IMAGE}" ops check-layout /external/restore-smoke-v0.2.1 --read-only
```

Conservez les sorties JSON, notamment `manifest_sha256`, sur un second support. Toute
erreur, arborescence `.partial-*`, divergence de hash, SQLite/Parquet invalide ou
restore-smoke incomplet bloque update, rollback, reinstall et uninstall.

## 8. Export Parquet et téléchargement de rapport

Writer arrêté, produisez un export durable avec un type de record exact :

```bash
docker run --rm --network none --user 1000:1000 --read-only \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 128 \
  --memory 1g --cpus 1 --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
  --mount type=bind,src="${APP_DATA_DIR}/data",dst=/persistent \
  "${HYPERLAB_IMAGE}" ops export-parquet /persistent public-wire-v0.2.1.parquet \
  --type wire_message
```

Le fichier et son résumé SHA-256 sont publiés atomiquement dans `reports/`. Après
redémarrage de l'app et retour de `/ready`, le bouton du dashboard télécharge ce fichier
sans autoriser d'écriture.

## 9. Update et rollback

1. publiez et vérifiez le nouveau digest sans toucher au store sain ;
2. arrêtez l'app et terminez backup externe, vérification et restore-smoke ;
3. archivez version, ancien digest, nouveau digest et hashes de configuration ;
4. mettez à jour le store avec le nouveau `tag@digest` vérifié ;
5. lancez l'update Umbrel et attendez `/health/live`, puis `/ready` ;
6. exécutez les contrôles SQLite/Parquet et freshness avant d'accepter l'update.

Si l'update échoue, arrêtez les services, remettez le digest précédent signé et
redémarrez. Si le schéma n'est pas rétrocompatible, restaurez la sauvegarde externe
vers une **nouvelle** racine ; `ops restore` refuse toute fusion ou écrasement. Un échec
de rollback laisse les writers arrêtés avec statut `MANUAL_REVIEW`. Ne déplacez jamais
un tag existant et n'utilisez jamais `latest`.

## 10. Désinstallation et conservation des données

**Aucune conservation automatique ne doit être supposée.** Le chemin de compatibilité
legacy d'umbrelOS exécute `compose down`, puis supprime `${APP_DATA_DIR}` lors d'un
uninstall ([source officielle Umbrel, lignes 501-527](https://github.com/getumbrel/umbrel/blob/119c7dbe4c59b736476c4bec6c5a15fd6a8a7a91/packages/umbreld/source/modules/apps/legacy-compat/app-script#L501-L527)).
Une copie dans `${APP_DATA_DIR}/data/backups` disparaîtrait elle aussi.

Avant uninstall ou reinstall, terminez le runbook précédent : backup complet **hors de
`${APP_DATA_DIR}`**, `ops verify-backup`, `ops restore` vers une racine neuve hors de
`${APP_DATA_DIR}`, puis archivage des hashes. Sans ces preuves, ne désinstallez pas.
Après désinstallation, seule la copie externe est considérée conservée.

## 11. Limites de validation

Sans daemon Docker et sans matériel Umbrel local, cette phase ne prouve ni le pull du
manifest, ni la signature réelle, ni le comportement après coupure secteur, ni le
rollback via l'interface. Ces preuves doivent être conservées lors du premier
déploiement contrôlé. Elles ne lèvent aucun gate économique des phases 10 à 14.
