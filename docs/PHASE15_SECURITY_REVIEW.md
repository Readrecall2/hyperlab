# Phase 15 — revue sécurité, supply chain et rollback

## Statut et frontière

Cette phase durcit uniquement le collecteur public et le dashboard read-only `0.2.1`.
Elle ne valide ni une stratégie, ni le paper économique, ni le testnet, ni le
mainnet. L'image ne contient aucun wallet, secret, signer, client privé ou transport
d'ordre. Le SDK Hyperliquid complet a été retiré du graphe runtime : le seul client
Hyperliquid est un transport standard-library limité aux hôtes publics allowlistés et
au chemin `/info`.

Un build Docker réussi ne constitue pas une autorisation de déploiement. La release
reste interdite si un P0/P1, un test en échec, une divergence du manifeste, un secret,
une vulnérabilité HIGH/CRITICAL ou une attestation absente est observé.

## Politique de supply chain

Les invariants exécutables sont dans `scripts/verify_release.py` et
`scripts/verify_manifest.py` :

- dépendances runtime et CI verrouillées transitivement avec hashes SHA-256 dans
  `requirements-runtime.lock` et `requirements-ci.lock` ;
- image Python de base référencée par digest ; aucun `pip install .`, upgrade de pip
  ou résolution de build backend dans l'image ;
- toutes les GitHub Actions référencées par SHA de commit complet, avec une allowlist
  exécutable des couples action/SHA pour éviter un SHA appartenant à un autre dépôt ou
  un objet tag non exécutable ;
- Gitleaks, Trivy, Syft et Buildx sont téléchargés à version exacte et contrôlés par
  SHA-256, sans script d'installation mutable ; QEMU et BuildKit sont épinglés par
  digest ;
- source canonique inventoriée par `MANIFEST_SHA256.txt`, avec normalisation LF pour
  que CRLF Windows ne change pas artificiellement le hash ;
- aucun tag `latest` : le build pousse d'abord un tag candidat unique non déployable ;
  le tag SemVer exact n'est créé qu'après attestations, signature et vérification, et
  le workflow refuse d'écraser un tag SemVer existant. Umbrel consomme toujours
  `tag@sha256:digest` ;
- Gitleaks inspecte l'historique du tag exact, jamais seulement le worktree courant ;
- `amd64` et `arm64` sont construites puis scannées localement par Trivy **avant**
  login ou push. Tout HIGH/CRITICAL, corrigé ou non, bloque la publication ;
- le job de publication dépend explicitement du préflight complet, du secret scan et
  des deux scans d'architecture ;
- après le push du candidat, le workflow exige un index composé d'exactement deux
  enfants uniques `linux/amd64` et `linux/arm64`, puis Trivy rescane **chacun de leurs
  digests exacts** avant toute attestation, signature ou promotion SemVer ;
- chaque digest enfant reçoit son SBOM SPDX attesté, tandis que l'index reçoit une
  provenance GitHub et une signature Cosign keyless liée à l'identité OIDC du workflow ;
- un reçu JSON lie commit source, tag, index, enfants, hashes SBOM et gate Trivy. Il
  est signé par Cosign ; `prepare_umbrel_store.py` vérifie ce bundle, la signature de
  l'index, provenance/SBOM, puis l'égalité `tag SemVer == digest signé` avant d'écrire ;
- les SARIF et SBOM sont conservés comme preuves. Il n'existe aucune allowlist de CVE
  silencieuse. Une exception future exige une revue humaine documentée, une échéance
  et un nouveau passage de ce gate.

Les SHA d'actions sont immuables mais pas éternellement sûrs. Une mise à jour d'action
doit résoudre un nouveau tag officiel, examiner le diff amont, remplacer le SHA et
rejouer la suite complète. Dependabot surveille mensuellement Python, Actions et image de base,
mais aucun bot ne peut fusionner seul ce changement.

## Pipeline de publication fail-closed

1. Le tag demandé doit être exactement `v0.2.1` et correspondre aux versions Python,
   OCI et Umbrel.
2. Le préflight installe uniquement le lock hashé, vérifie manifeste et règles de
   release, puis exécute Ruff, mypy et tout pytest.
3. Gitleaks scanne l'historique complet au même tag.
4. Chaque architecture est construite sans identifiant de registre puis scannée.
5. Seulement après les quatre gates, le workflow obtient `packages: write` et l'OIDC,
   pousse un candidat unique, contrôle ses plateformes et rescane son digest exact.
6. Si ce scan est vert, il génère/atteste le SBOM et la provenance, signe le digest,
   puis Cosign revérifie immédiatement l'identité exacte du workflow.
7. Le tag SemVer immuable est créé à partir de ce digest et son égalité brute avec le
   digest signé est recontrôlée. Le digest n'est copié dans le package Umbrel qu'après
   ces succès et une vérification Cosign indépendante.

La configuration GitHub est elle-même un gate externe : `refs/tags/v*`, la branche par
défaut et le workflow de publication doivent être protégés par ruleset/revue humaine,
et l'environment `signed-release` du job `publish` doit avoir un required reviewer
indépendant sans bypass administrateur. Sans export relu de ces réglages, la release
reste `BLOCKED_EXTERNAL_CONTROL_NOT_VERIFIED` : un rôle write ne doit pas pouvoir
signer seul son propre commit en créant un tag.
Le préflight exige aussi `github.ref_type == tag` et `github.ref_protected == true` ;
ce booléen prouve qu'un ruleset s'applique, mais pas qu'il est suffisamment strict,
d'où l'obligation de conserver l'export et la revue humaine.

Un déclenchement manuel doit lui aussi exécuter le workflow **depuis le tag exact** :
`gh workflow run container.yml --ref v0.2.1 -f release_tag=v0.2.1`. Une exécution
depuis une branche, même si son input nomme ce tag, échoue avant tout checkout ou push.

Commandes de préparation et de contrôle :

```powershell
python scripts/verify_manifest.py
python scripts/verify_release.py --template --tag v0.2.1 --check-manifest

python scripts/prepare_umbrel_store.py VOTRE_COMPTE `
  --repository hyperlab `
  --image-version 0.2.1 `
  --image-digest DIGEST_MULTIARCH_64_HEX `
  --release-receipt .\release-evidence\release-receipt.json `
  --receipt-bundle .\release-evidence\release-receipt.sigstore.json
python scripts/verify_manifest.py --write
git diff -- MANIFEST_SHA256.txt jjlab-hyperlab
python scripts/verify_release.py --prepared --tag v0.2.1 --check-manifest
```

Vérification indépendante après publication :

```text
cosign verify ghcr.io/OWNER/hyperlab@sha256:DIGEST \
  --certificate-identity https://github.com/OWNER/REPO/.github/workflows/container.yml@refs/tags/v0.2.1 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
gh attestation verify oci://ghcr.io/OWNER/hyperlab@sha256:DIGEST --repo OWNER/REPO
trivy image --exit-code 1 --ignore-unfixed=false --severity HIGH,CRITICAL \
  ghcr.io/OWNER/hyperlab@sha256:DIGEST
```

## Mise à jour et rollback

Une mise à jour est un nouveau SemVer et un nouveau digest ; un tag existant n'est
jamais déplacé. La branche de store déployable conserve le dernier digest sain tant
que le nouveau workflow n'est pas intégralement vert. Le tag de build porte le
template contrôlé ; le digest issu de ce tag n'est injecté dans le store qu'après les
scans, attestations et signature. Avant mise à jour :

1. arrêter proprement le writer et vérifier readiness/integrity ;
2. produire une sauvegarde complète, hashée et vérifiée **hors de `${APP_DATA_DIR}`**,
   incluant données, paper state, rapports et configuration ;
3. enregistrer version, digest, schémas, hashes de configuration et dernier manifest
   sain ;
4. vérifier signature, attestations, SBOM et résultat Trivy du nouveau digest ;
5. valider statiquement le package préparé ;
6. démarrer la nouvelle version et attendre liveness **puis** readiness réelle avant
   de considérer l'update accepté.

En cas d'échec, ne jamais éditer le tag. Réinstaller le digest précédent enregistré,
sans supposer que les données seront conservées par Umbrel, puis vérifier intégrité
SQLite/Parquet et freshness. Une migration
de schéma non rétrocompatible exige une restauration de la sauvegarde correspondante,
jamais une ouverture opportuniste par l'ancien binaire. Si le rollback ou la
restauration échoue, tous les writers restent arrêtés et le statut est
`MANUAL_REVIEW`; aucune collecte concurrente n'est lancée pour « réparer ».

Une sauvegarde partielle, sans manifeste, avec hash divergent, prise pendant un writer
non quiescent ou dont le test de restauration échoue est **inutilisable**. Elle ne doit
jamais remplacer la dernière sauvegarde connue saine.

**Aucune conservation automatique ne doit être supposée.** Le chemin de compatibilité
legacy d'umbrelOS décrit explicitement `uninstall` comme destructif et supprime le
répertoire de l'application après `compose down` ([source officielle Umbrel,
`app-script`, lignes 501-527](https://github.com/getumbrel/umbrel/blob/119c7dbe4c59b736476c4bec6c5a15fd6a8a7a91/packages/umbreld/source/modules/apps/legacy-compat/app-script#L501-L527)).
La désinstallation ou réinstallation est donc interdite tant qu'une sauvegarde
complète n'a pas été créée et vérifiée hors de `${APP_DATA_DIR}`, puis restaurée avec
succès vers une racine neuve elle aussi hors de `${APP_DATA_DIR}`. Une copie située
dans `data/backups` serait supprimée avec l'application et ne constitue pas une
sauvegarde de désinstallation.

Le runbook exécutable complet, y compris la présentation des six répertoires
persistants au conteneur d'opérations, se trouve dans `docs/UMBREL_SETUP.md`.

## Prérequis hérités toujours BLOCKED

Le durcissement Umbrel ne lève aucun gate :

- **Phase 10 — `BLOCKED_PRECONDITION_NOT_MET`** : pas encore de preuve de flux
  multi-venues sub-seconde continus, horodatés et rejouables, clock sync, latences
  décision/réseau/ordre/fill, prix exécutables, rejets, non-fills, partials et sortie
  adverse dans un replay d'exécution event-driven ;
- **Phase 11 — `BLOCKED_INSUFFICIENT_REAL_DATA` / séquences inobservables** : pas de
  replay L2 réel suffisant ni de calibration auditée des frais, files, latences,
  toxicité et markouts ;
- **Phase 12 — Gate D économique `BLOCKED`** : Gates B/C non prouvées, registre live
  vide, minimum 42 jours forward, cycles préenregistrés, 14 jours sans incident,
  coûts/latence/fills calibrés et exercices crash/recovery non démontrés ;
- **Phase 13 — Gate E `BLOCKED`** : aucune revue humaine de Gate D et aucun service
  testnet séparé validé ; aucun code testnet n'est promu par Phase 15 ;
- **Phase 14 — Gate F `BLOCKED`** : aucun testnet validé, audit indépendant ou décision
  humaine micro-mainnet. Umbrel ne reçoit jamais automatiquement un exécuteur.

## Revue adversariale de déploiement

| Sévérité | Scénario | Contrôle / statut |
|---|---|---|
| P0 | Secret, wallet, signer ou route d'ordre dans l'image | Absent du graphe runtime ; interdictions AST, Gitleaks et exclusions build. |
| P0 | Image vulnérable publiée malgré le gate | Résolu : scans `amd64`/`arm64` avant toute authentification/poussée. |
| P1 | Dépendance ou image de base mutable | Résolu : locks hashés et digest de base. |
| P1 | Compromission d'une Action via tag mutable | Résolu : SHA complets vérifiés. |
| P1 | Contributeur write créant seul un tag et une signature valide | Contrôle logiciel : le job privilégié cible l'environment `signed-release`. **BLOCKED externe** jusqu'à preuve d'un ruleset protégeant `refs/tags/v*`, branche/workflow et d'un required reviewer sans bypass. Ne pas publier avant fermeture. |
| P1 | Digest de base immuable mais vulnérable au jour de publication | **BLOCKED externe** jusqu'au premier run Trivy `amd64`/`arm64` réellement vert ; aucun succès n'est déduit du pin ou d'un build local. |
| P1 | Image non traçable ou substituée | Résolu dans l'architecture : digest, provenance/SBOM attestés et Cosign OIDC. L'exécution réelle du workflow reste une preuve externe à conserver. |
| P1 | Échec d'update/rollback écrasant les données | Résolu par digest précédent, sauvegarde externe vérifiée et testée, aucune fusion/écriture sur une racine existante, puis arrêt `MANUAL_REVIEW`. |
| P1 | Désinstallation Umbrel supprimant les données de l'app | Résolu dans le runbook : aucune promesse de conservation ; backup et restore-smoke hors `${APP_DATA_DIR}` obligatoires avant uninstall/reinstall. |
| P1 | Élévation de privilèges ou writable mounts trop larges | Résolu statiquement : UID 1000, rootfs read-only, aucune capacité, `no-new-privileges`, pas de Docker socket ; dashboard limité aux mounts read-only runtime/reports/paper/config. Validation dynamique Umbrel encore externe. |
| P1 | Writers SQLite/Parquet concurrents | Résolu par lock de racine exclusif et maintenance refusée pendant collecte ; une seule instance collector est déclarée. |
| P1 | Coupure brutale ou état SQLite/Parquet corrompu | Résolu dans le code par marqueur de session non propre, publication atomique/fsync, contrôles d'intégrité et readiness fermée. Le drill secteur sur matériel réel reste une preuve externe. |
| P1 | Disque plein ou données stale | Résolu par réserve bytes/pourcentage avant write, conservation des buffers en erreur, freshness/canaux stale dans readiness et healthcheck collector. |
| P1 | Backup partiel ou restore corrompu | Résolu par staging, marqueur `COMPLETE`, manifeste hashé, contrôle SQLite/Parquet, racine cible neuve uniquement et restore-smoke obligatoire hors Umbrel. |
| P2 | Base ou dépendance devenue vulnérable après publication | Risque permanent : rescan périodique et nouvelle release, jamais mutation du tag. |
| P2 | Indisponibilité GitHub/Sigstore au moment de publier | Publication bloquée ; l'ancienne version signée reste en place. |
| P2 | Tags candidats uniques laissés par une release interrompue | Non déployables par le validateur et jamais SemVer ; appliquer une rétention GHCR après conservation des preuves, sans supprimer un digest final référencé. |
| P2 | Exposition du dashboard via le proxy Umbrel | Nécessaire à l'accès local Umbrel ; aucun port hôte direct, aucun egress du dashboard et aucune mutation. La politique d'accès du proxy reste une frontière de plateforme. |

Au moment de ce document, aucun P0/P1 logiciel de conception supply-chain connu n'est
laissé ouvert, mais les deux P1 externes ci-dessus bloquent toute publication tant que
leurs preuves ne sont pas conservées. La Phase 15 entière ne peut pas être déclarée
terminée avant validation finale
du diff complet, suite locale complète, validation statique Compose/Umbrel et
conservation des preuves du premier workflow de release réellement exécuté et des
drills Umbrel/coupure/rollback. Aucun P0/P1 logiciel ne peut rester ouvert lors de cette
validation finale ; une preuve externe non encore exécutée bloque la déclaration de
déploiement, mais n'est pas présentée comme un succès simulé.
