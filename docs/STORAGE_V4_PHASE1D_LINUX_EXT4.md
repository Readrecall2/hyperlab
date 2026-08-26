# Storage V4 Phase 1D — certification Linux/ext4 offline

Cette phase certifie les sémantiques filesystem et recovery Linux du chemin
Storage V4 natif déjà acquis en Phase 1C. Le corpus est synthétique, borné et
PAPER_ONLY. Il ne constitue ni une nouvelle preuve de capacité 1M, ni une preuve
économique, ni une autorisation de trading réel.

## Contrat exécuté

Le certifieur refuse tout verdict terminal si le workspace n'est pas frais, si
le kernel n'est pas Linux, si le mount réel détecté dans `/proc/self/mountinfo`
n'est pas ext4, ou si les options ext4 affaiblissent la durabilité. Il exige des
répertoires 0700 et des fichiers 0600 appartenant à l'UID/GID du processus. Les
symlinks, reparse points et hardlinks de fichiers sont refusés.

Les publications immuables utilisent un rename atomique sans remplacement
(`renameat2(RENAME_NOREPLACE)`), suivi du fsync du répertoire parent. Les
fichiers, ancres SQLite en `DELETE/FULL`, manifests, checkpoints et caches
`CURRENT` conservent leurs barrières de durabilité. `CURRENT` est supprimé dans
les probes puis reconstruit depuis l'ancre externe authentifiée.

La matrice lance un processus distinct pour chaque frontière critique Paper et
raw, sous SIGTERM puis SIGKILL. Le parent exige le returncode du signal, rouvre
le store, normalise uniquement son scratch frais, effectue l'audit complet,
redémarre une seconde fois et atteste l'arbre. Les tailles de tail 0, 1, 17 et
64 démontrent que le startup n'ouvre aucun segment historique et rejoue
exactement le suffixe borné. Un oracle natif indépendant vérifie chaque corpus.

## Exécution persistante

Le launcher crée un nouveau répertoire horodaté, un venv Python sans pip et
exécute le code du checkout via `PYTHONPATH`. Il n'effectue aucun accès réseau,
n'accepte aucun secret et ne touche aucun store préexistant. Le processus est
détaché avec `nohup` et `setsid`; son PID, stdout, stderr, progression et bundle
terminal restent sous le nouveau run root.

Avant le lancement, annoncer à l'opérateur :

- emplacement : Bash dans Tabby/VPS ;
- durée moyenne attendue : 2 à 8 minutes, maximum opérationnel : 20 minutes ;
- prompt : aucun ;
- monitoring : `monitor_offline_certification.sh RUN_ROOT --follow` dans un
  second onglet Tabby ;
- Ctrl+C : pendant le launcher avant affichage du PID, il interrompt seulement
  la préparation ; après affichage du PID, il n'arrête pas le certifieur détaché.
  Dans le moniteur, il arrête seulement le moniteur ;
- signal terminal : `COMPLETE.json` contenant exactement
  `STORAGE_V4_PHASE_1D_LINUX_EXT4_CERTIFIED`. Toute sortie sans `COMPLETE.json`
  est un échec.

Commande de lancement :

```bash
./ops/storage_v4_phase1d/run_offline_certification.sh \
  "$HOME/hyperlab-phase1d/source" \
  "$HOME/hyperlab-phase1d/runs" \
  SOURCE_COMMIT \
  hyperlab-paper.service
```

Le nom de service est optionnel et répétable. Les unités découvertes contenant
`hyperlab` ou `paper` sont également inspectées en lecture seule. Si un service
ou un handle étranger référence le workspace candidat, le certifieur s'arrête
sans arrêter le service.

## Artefacts terminaux

- `progress.jsonl` : transitions durables de phase ;
- `phase1d-report.json` : mount/options, kernel/Python, ownership/modes,
  crash/recovery, leases, audit/oracle, O(tail), temps/CPU/RSS/écritures ;
- `COMPLETE.json` : liaison SHA-256 du rapport et verdict terminal.

L'absence d'exécution réelle sur ext4 limite le verdict de revue à
`STORAGE_V4_PHASE_1D_READY_FOR_OFFLINE_LINUX_CERTIFICATION`.

## Bloc Windows PowerShell — identité et upload

Remplacer uniquement la valeur de `$SshTarget`. Le client reste en BatchMode :
un accès nécessitant un mot de passe ou une confirmation interactive échoue au
lieu d'afficher un prompt.

```powershell
$ErrorActionPreference = 'Stop'
$Repo = 'C:\Dev\hyperlab-multistrategy'
$Branch = 'codex/storage-v4-phase-1d-linux-ext4'
$SshTarget = 'user@vps-or-existing-ssh-alias'
$Commit = (git -C $Repo rev-parse HEAD).Trim()
if ((git -C $Repo branch --show-current).Trim() -ne $Branch) { throw 'Wrong branch' }
if (git -C $Repo status --porcelain --untracked-files=no) { throw 'Tracked tree or index is dirty' }
if ($Commit -notmatch '^[0-9a-f]{40}$') { throw 'Invalid commit identity' }
$Stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$Nonce = [guid]::NewGuid().ToString('N')
$Transfer = Join-Path $env:TEMP "hyperlab-phase1d-$Stamp-$Nonce"
New-Item -ItemType Directory -Path $Transfer | Out-Null
$Bundle = Join-Path $Transfer 'hyperlab-storage-v4-phase1d.bundle'
$HashFile = "$Bundle.sha256"
git -C $Repo bundle create $Bundle $Branch
git -C $Repo bundle verify $Bundle
$BundleHash = (Get-FileHash -LiteralPath $Bundle -Algorithm SHA256).Hash.ToLowerInvariant()
"$BundleHash  $(Split-Path -Leaf $Bundle)" | Set-Content -LiteralPath $HashFile -Encoding ascii
$RemoteBase = (& 'C:\Windows\System32\OpenSSH\ssh.exe' -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10 -- $SshTarget 'printf %s "$HOME/hyperlab-phase1d"').Trim()
if ($LASTEXITCODE -ne 0 -or -not $RemoteBase) { throw 'SSH BatchMode preflight failed' }
$RemoteIncoming = "$RemoteBase/incoming/$Stamp-$Nonce"
& 'C:\Windows\System32\OpenSSH\ssh.exe' -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10 -- $SshTarget "install -d -m 0700 '$RemoteIncoming'"
if ($LASTEXITCODE -ne 0) { throw 'SSH BatchMode preflight failed' }
& 'C:\Windows\System32\OpenSSH\scp.exe' -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10 -- $Bundle $HashFile "${SshTarget}:$RemoteIncoming/"
if ($LASTEXITCODE -ne 0) { throw 'SCP upload failed' }
"SOURCE_COMMIT=$Commit"
"REMOTE_INCOMING=$RemoteIncoming"
"BUNDLE_SHA256=$BundleHash"
```

## Bloc Bash Tabby/VPS — installation, lancement et monitoring

Reporter exactement les deux valeurs imprimées par PowerShell. Aucun ancien
source clone ou run n'est réutilisé ou supprimé.

```bash
set -euo pipefail
SOURCE_COMMIT='paste-SOURCE_COMMIT-here'
REMOTE_INCOMING='paste-REMOTE_INCOMING-here'
BASE="$HOME/hyperlab-phase1d"
BUNDLE="$REMOTE_INCOMING/hyperlab-storage-v4-phase1d.bundle"
HASH_FILE="$BUNDLE.sha256"
cd "$REMOTE_INCOMING"
sha256sum --check "$(basename "$HASH_FILE")"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
NONCE=$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')
SOURCE_ROOT="$BASE/sources/source-${SOURCE_COMMIT:0:12}-$STAMP-$NONCE"
install -d -m 0700 "$BASE" "$BASE/sources" "$BASE/runs"
git clone --no-checkout "$BUNDLE" "$SOURCE_ROOT"
git -C "$SOURCE_ROOT" checkout --detach "$SOURCE_COMMIT"
test "$(git -C "$SOURCE_ROOT" rev-parse HEAD)" = "$SOURCE_COMMIT"
test -z "$(git -C "$SOURCE_ROOT" status --porcelain --untracked-files=no)"
bash "$SOURCE_ROOT/ops/storage_v4_phase1d/run_offline_certification.sh" \
  "$SOURCE_ROOT" \
  "$BASE/runs" \
  "$SOURCE_COMMIT" \
  hyperlab-paper.service | tee "$REMOTE_INCOMING/launch-result.txt"
RUN_ROOT=$(sed -n 's/^PHASE1D_RUN_ROOT=//p' "$REMOTE_INCOMING/launch-result.txt")
bash "$SOURCE_ROOT/ops/storage_v4_phase1d/monitor_offline_certification.sh" "$RUN_ROOT"
printf 'Second Tabby tab follow command:\n'
printf 'bash %q %q --follow\n' \
  "$SOURCE_ROOT/ops/storage_v4_phase1d/monitor_offline_certification.sh" \
  "$RUN_ROOT"
```
