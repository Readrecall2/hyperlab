from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
EXPECTED_BRANCH = "codex/prediction-markets-prospective-launch-v1"
PACK_ID = "prediction-markets-prospective-launch-v1"
_RUN_SLUG = re.compile(r"^pm-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SERVICE = re.compile(r"^hyperlab-pm-[a-z0-9-]+-(?:polymarket|kalshi|dashboard)\.service$")
_SCRIPTS = (
    "bootstrap-offline.sh",
    "cockpit.py",
    "install.sh",
    "launch_pack.py",
    "monitor.sh",
    "preflight.py",
    "rollback.sh",
    "runner.py",
)


class LaunchPackError(RuntimeError):
    """Launch-pack generation or verification failure."""


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def _object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LaunchPackError(f"invalid JSON object: {path}") from error
    if not isinstance(value, dict):
        raise LaunchPackError(f"JSON root must be an object: {path}")
    return value


def _positive_int(value: object, *, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise LaunchPackError(f"{label} is invalid")
    return value


def _text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise LaunchPackError(f"{label} is missing")
    return value


def _sha(value: object, *, label: str) -> str:
    text = _text(value, label=label)
    if _SHA256.fullmatch(text) is None:
        raise LaunchPackError(f"{label} is not a lowercase SHA-256")
    return text


def validate_plan(plan: Mapping[str, object]) -> dict[str, Any]:
    expected = {
        "access_bundle_sha256",
        "base_commit",
        "boundary",
        "campaign_manifest_sha256",
        "campaign_pack_path",
        "candidate_config_sha256",
        "dashboard_port",
        "disk",
        "economic_evidence_status",
        "expected_branch",
        "expected_shards_per_venue",
        "incoming_staging_max_bytes",
        "pack_id",
        "python",
        "remote",
        "schema_version",
        "service_user",
    }
    if set(plan) != expected:
        raise LaunchPackError("launch plan schema diverged")
    if (
        plan.get("schema_version") != 1
        or plan.get("boundary") != BOUNDARY
        or plan.get("pack_id") != PACK_ID
        or plan.get("expected_branch") != EXPECTED_BRANCH
        or plan.get("economic_evidence_status") != "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE"
        or plan.get("service_user") != "hyperlab"
        or plan.get("dashboard_port") != 18081
        or plan.get("expected_shards_per_venue") != 672
    ):
        raise LaunchPackError("launch plan immutable controls diverged")
    if _COMMIT.fullmatch(_text(plan.get("base_commit"), label="base commit")) is None:
        raise LaunchPackError("base commit is invalid")
    _sha(plan.get("candidate_config_sha256"), label="candidate config hash")
    _sha(plan.get("campaign_manifest_sha256"), label="candidate campaign manifest hash")
    _sha(plan.get("access_bundle_sha256"), label="candidate access bundle hash")
    disk = plan.get("disk")
    if not isinstance(disk, Mapping):
        raise LaunchPackError("disk contract is absent")
    prediction = _positive_int(disk.get("prediction_maximum_raw_bytes"), label="prediction raw budget")
    h1 = _positive_int(disk.get("h1_reserved_bytes"), label="H1 reserved budget")
    margin = _positive_int(disk.get("safety_margin_bytes"), label="safety margin")
    required = _positive_int(disk.get("required_free_bytes"), label="required free bytes")
    if prediction != 2 * 672 * 16 * 1024**2 or required != h1 + prediction + margin:
        raise LaunchPackError("capacity reservation arithmetic diverged")
    remote = plan.get("remote")
    if not isinstance(remote, Mapping):
        raise LaunchPackError("remote root contract is absent")
    if remote != {
        "home_parent": "/home/hyperlab/hyperlab-prediction-markets/incoming",
        "volume_base": "/mnt/HC_Volume_106716684/hyperlab-prediction-markets",
        "volume_mount": "/mnt/HC_Volume_106716684",
    }:
        raise LaunchPackError("Prediction Markets root isolation diverged")
    if "hyperlab-h1" in json.dumps(plan) or ":18080" in json.dumps(plan):
        raise LaunchPackError("launch plan collides with H1")
    python = plan.get("python")
    if python != {
        "implementation": "CPython",
        "major": 3,
        "minimum_glibc": "2.28",
        "minor": 12,
        "target_architecture": "x86_64",
    }:
        raise LaunchPackError("target Python contract diverged")
    return dict(plan)


def _git(repo_root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *arguments],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise LaunchPackError(completed.stderr.strip() or "Git command failed")
    return completed.stdout.strip()


def _git_is_ancestor(repo_root: Path, ancestor: str, descendant: str) -> bool:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), "merge-base", "--is-ancestor", ancestor, descendant],
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode not in (0, 1):
        raise LaunchPackError(completed.stderr.strip() or "Git ancestry check failed")
    return completed.returncode == 0


def build_source_inventory(repo_root: Path, commit: str) -> dict[str, object]:
    if _COMMIT.fullmatch(commit) is None:
        raise LaunchPackError("source commit is invalid")
    rows: list[dict[str, object]] = []
    output = _git(repo_root, "ls-tree", "-r", "--long", commit)
    for line in output.splitlines():
        metadata, separator, path = line.partition("\t")
        fields = metadata.split()
        if not separator or len(fields) != 4 or fields[1] != "blob" or not fields[3].isdigit():
            raise LaunchPackError("Git tree inventory line is malformed")
        rows.append(
            {
                "blob_sha1": fields[2],
                "mode": fields[0],
                "path": path,
                "size": int(fields[3]),
            }
        )
    if not rows:
        raise LaunchPackError("Git source inventory is empty")
    body = {
        "boundary": BOUNDARY,
        "commit": commit,
        "files": rows,
        "schema_version": 1,
    }
    return {**body, "inventory_sha256": sha256_bytes(canonical_json_bytes(body))}


def verify_source(source_root: Path, inventory_path: Path, expected_commit: str) -> dict[str, object]:
    if source_root.is_symlink() or source_root.resolve(strict=True) != source_root:
        raise LaunchPackError("source root is not an exact real directory")
    if _git(source_root, "rev-parse", "HEAD") != expected_commit:
        raise LaunchPackError("detached source commit diverged")
    if _git(source_root, "status", "--porcelain"):
        raise LaunchPackError("detached source checkout is not clean")
    expected = _object(inventory_path)
    actual = build_source_inventory(source_root, expected_commit)
    if expected != actual:
        raise LaunchPackError("Git source inventory diverged")
    files = actual.get("files")
    if not isinstance(files, list):
        raise LaunchPackError("Git source inventory files are invalid")
    return {
        "commit": expected_commit,
        "files": len(files),
        "inventory_sha256": actual["inventory_sha256"],
        "status": "PREDICTION_SOURCE_IDENTITY_GREEN",
    }


def _remote_path(parent: str, slug: str) -> str:
    if _RUN_SLUG.fullmatch(slug) is None:
        raise LaunchPackError("run slug must be pm-YYYYMMDDtHHMMSSz-8hex")
    pure_parent = PurePosixPath(parent)
    result = pure_parent / slug
    if not result.is_absolute() or ".." in result.parts:
        raise LaunchPackError("remote path is unsafe")
    return result.as_posix()


def _service_names(slug: str) -> dict[str, str]:
    suffix = slug.removeprefix("pm-")
    values = {
        "polymarket": f"hyperlab-pm-{suffix}-polymarket.service",
        "kalshi": f"hyperlab-pm-{suffix}-kalshi.service",
        "dashboard": f"hyperlab-pm-{suffix}-dashboard.service",
    }
    if any(_SERVICE.fullmatch(value) is None for value in values.values()):
        raise LaunchPackError("rendered service identity is invalid")
    return values


def _common_unit(service_user: str, source_root: str, campaign_root: str) -> str:
    return f"""User={service_user}
Group={service_user}
WorkingDirectory={source_root}
Environment=HOME=/home/{service_user}
Environment=USER={service_user}
Environment=PYTHONPATH={source_root}/src:{source_root}
Environment=PYTHONNOUSERSITE=1
Environment=PYTHONDONTWRITEBYTECODE=1
Environment=PYTHONUNBUFFERED=1
Environment=TZ=UTC
UnsetEnvironment=HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
UnsetEnvironment=HYPERLIQUID_PRIVATE_KEY HYPERLAB_TESTNET_PRIVATE_KEY HYPERLAB_TESTNET_ACCOUNT_ADDRESS
UMask=0077
NoNewPrivileges=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectSystem=strict
ProtectHome=read-only
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
ProtectClock=yes
ProtectHostname=yes
ProtectProc=invisible
ProcSubset=pid
RestrictSUIDSGID=yes
RestrictRealtime=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
CapabilityBoundingSet=
AmbientCapabilities=
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
ReadOnlyPaths={source_root} {campaign_root}
"""


def render_units(handoff: Mapping[str, object]) -> dict[str, str]:
    source_root = _text(handoff.get("source_root"), label="source root")
    campaign_root = _text(handoff.get("campaign_root"), label="campaign root")
    incoming_root = _text(handoff.get("incoming_root"), label="incoming root")
    service_user = _text(handoff.get("service_user"), label="service user")
    services = handoff.get("services")
    if not isinstance(services, Mapping):
        raise LaunchPackError("service map is absent")
    python = f"{source_root}/.venv/bin/python"
    common = _common_unit(service_user, source_root, campaign_root)
    units: dict[str, str] = {}
    for venue in ("polymarket", "kalshi"):
        service = _text(services.get(venue), label=f"{venue} service")
        if _SERVICE.fullmatch(service) is None:
            raise LaunchPackError("collector service name is invalid")
        units[service] = f"""[Unit]
Description=HyperLab Prediction Markets {venue} public prospective collector
After=network-online.target time-sync.target
Wants=network-online.target time-sync.target
StartLimitIntervalSec=1800
StartLimitBurst=3

[Service]
Type=simple
{common}ReadWritePaths={campaign_root}/{venue}
ExecStart={python} {source_root}/ops/prediction_markets_launch_v1/runner.py --handoff {incoming_root}/handoff.json --venue {venue}
KillSignal=SIGINT
TimeoutStopSec=180
SendSIGKILL=no
Restart=on-failure
RestartSec=60
SuccessExitStatus=0 130
RestartPreventExitStatus=4

[Install]
WantedBy=multi-user.target
"""
    dashboard = _text(services.get("dashboard"), label="dashboard service")
    if _SERVICE.fullmatch(dashboard) is None:
        raise LaunchPackError("dashboard service name is invalid")
    units[dashboard] = f"""[Unit]
Description=HyperLab Prediction Markets read-only cockpit
After=network.target
StartLimitIntervalSec=1800
StartLimitBurst=3

[Service]
Type=simple
{common}ExecStart={python} {source_root}/ops/prediction_markets_launch_v1/cockpit.py --campaign-root {campaign_root} --host 127.0.0.1 --port 18081
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
"""
    return units


def render_operator_readme(handoff: Mapping[str, object]) -> str:
    services = handoff["services"]
    assert isinstance(services, Mapping)
    return f"""# Prediction Markets — exécution humaine unique

Ce pack est strictement `{BOUNDARY}`. Il ne contient aucun ordre, wallet,
signer, secret ou accès privé. Il ne touche pas la campagne H1. Son statut
économique est `ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE`.

## Ordre des blocs

1. Sur Windows PowerShell, exécuter
   `operator/A-windows-bundle-verify-transfer.ps1`. Signal attendu :
   `PREDICTION_WINDOWS_TRANSFER_VERIFIED`.
2. Dans un premier onglet Tabby/VPS, exécuter
   `operator/B-tabby-preflight-install-activate.sh`. Il refuse avant activation
   si identité, capacité, NTP, filesystem, port, service ou dépendance diverge.
   Signal attendu : `PREDICTION_INSTALL_ACTIVATION_GREEN`.
3. Dans un second onglet Tabby, exécuter
   `operator/C-tabby-readonly-monitor.sh`. Il s'arrête à la première transition
   ou alerte avec `PREDICTION_MONITOR_TRANSITION_OR_ALERT`.
4. Sur Windows PowerShell, exécuter
   `operator/D-windows-dashboard-tunnel.ps1`. Ouvrir l'URL seulement après
   `PREDICTION_TUNNEL_READY http://127.0.0.1:18081`.
5. `operator/E-recovery-rollback.sh recovery|rollback` reprend sans rejouer un
   slot terminal, ou arrête/désactive uniquement les trois services ci-dessous.
   Aucun raw, manifest, ledger, run ou rapport n'est supprimé.

Les blocs Windows lisent `HYPERLAB_PM_SSH_TARGET` et `HYPERLAB_PM_SSH_KEY` dans
le shell opérateur. Aucune valeur de connexion n'est enregistrée dans ce pack.

## Identité et isolation de cette tentative

- run : `{handoff['run_slug']}`
- commit source : `{handoff['source_commit']}`
- incoming root : `{handoff['incoming_root']}`
- source root : `{handoff['source_root']}`
- campaign root : `{handoff['campaign_root']}`
- collecteur Polymarket : `{services['polymarket']}`
- collecteur Kalshi : `{services['kalshi']}`
- cockpit : `{services['dashboard']}` sur `127.0.0.1:18081`

Polymarket et Kalshi sont indépendants. Un reçu authentique
`PUBLIC_SOURCE_INVALID` est comptabilisé comme slot terminal de qualité de
donnée, mais reste `source_usable=false`, `economic_eligible=false` et n'est
jamais rejoué. Une divergence de reçu, plan, identité, hash, manifest ou ledger
reste `INTEGRITY_FAILED` et ne redémarre pas en boucle.

Le moniteur utilise exclusivement le Python du venv offline lié au source root
authentifié. Avant tout collecteur, B exige en même temps le HTTP loopback
readonly, `orders_enabled=false`, le PID/commande et l'unité systemd exacts, et
la preuve que ce PID possède `127.0.0.1:18081`. Une course de premier bind ou un
503 est retenté de façon bornée; aucune preuve divergente n'est admise. Le
moniteur refuse aussi les racines de campagne/state/venue liées ou non
canoniques.

La sélection des collecteurs provient de ce même moniteur authentifié et tout
échec de parsing refuse avant activation. Si les deux sources sont indisponibles,
`PREDICTION_ELIGIBLE_VENUES=NONE` conserve honnêtement le dashboard seul. E lie
également fragments, PID/commande et listener; son signal de reprise exige
`operational_failure=false`, tout en admettant une alerte
`PUBLIC_SOURCE_INVALID` authentique.

La capacité est remesurée après bootstrap puis immédiatement avant les
collecteurs. Si les 194 347 270 144 octets réservés ne sont plus libres, B refuse
et demande un volume ext4 plus grand ou distinct; il ne réduit jamais le budget
H1 ni la marge. Chaque reprise systemd réauthentifie handoff, admission,
transfert, source, NTP, racines et device ext4 avant de sélectionner un ordinal;
le runner applique ensuite son gate capacité lié au ledger. Un refus antérieur
au slot sort en code 4 sans boucle de restart.
"""


def render_windows_transfer(handoff: Mapping[str, object]) -> str:
    incoming = handoff["incoming_root"]
    bundle = handoff["bundle_filename"]
    bundle_sha = handoff["bundle_sha256"]
    return f"""# Lieu: Windows PowerShell local. Durée attendue: 2-8 min; maximum: 20 min.
# Prompts: SSH host-key/password possibles. Ctrl+C interrompt uniquement le transfert;
# le nouvel incoming root reste non activé. Signal terminal: PREDICTION_WINDOWS_TRANSFER_VERIFIED.
$ErrorActionPreference = 'Stop'
$OperatorRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path
if (-not [StringComparer]::OrdinalIgnoreCase.Equals((Split-Path -Leaf $OperatorRoot), 'operator')) {{ throw 'Block A must remain in the pack operator directory.' }}
$BundleRoot = (Resolve-Path -LiteralPath (Join-Path $OperatorRoot '..')).Path
$SshTarget = $env:HYPERLAB_PM_SSH_TARGET
if ([string]::IsNullOrWhiteSpace($SshTarget) -or $SshTarget -notmatch '^[a-z0-9._-]+@[a-z0-9.-]+$') {{ throw 'Set HYPERLAB_PM_SSH_TARGET to user@host.' }}
$SshKeyRaw = $env:HYPERLAB_PM_SSH_KEY
if ([string]::IsNullOrWhiteSpace($SshKeyRaw)) {{ throw 'Set HYPERLAB_PM_SSH_KEY to the dedicated private-key path.' }}
$SshKeyPath = (Resolve-Path -LiteralPath $SshKeyRaw).Path
if (-not (Test-Path -LiteralPath $SshKeyPath -PathType Leaf)) {{ throw 'HYPERLAB_PM_SSH_KEY is not a regular file.' }}
$IncomingRoot = '{incoming}'

function Get-Sha256Hex {{
    param([string] $Path)
    $Stream = [IO.File]::OpenRead($Path)
    try {{
        $Hasher = [Security.Cryptography.SHA256]::Create()
        try {{
            return [BitConverter]::ToString($Hasher.ComputeHash($Stream)).Replace('-', '').ToLowerInvariant()
        }} finally {{
            $Hasher.Dispose()
        }}
    }} finally {{
        $Stream.Dispose()
    }}
}}

function Assert-Sha256Manifest {{
    param([string] $ManifestPath, [string] $ContentRoot)
    $ResolvedRoot = (Resolve-Path -LiteralPath $ContentRoot).Path
    $ResolvedManifest = (Resolve-Path -LiteralPath $ManifestPath).Path
    $Lines = @(Get-Content -LiteralPath $ResolvedManifest)
    if ($Lines.Count -eq 0) {{ throw "Empty SHA-256 manifest: $ResolvedManifest" }}
    foreach ($Line in $Lines) {{
        if ($Line -notmatch '^([0-9a-f]{{64}})  ([^/\\\\]+)$') {{ throw "Invalid SHA-256 manifest line: $ResolvedManifest" }}
        $ExpectedHash = $Matches[1]
        $LeafName = $Matches[2]
        if ([IO.Path]::GetFileName($LeafName) -cne $LeafName) {{ throw "Unsafe SHA-256 manifest leaf: $LeafName" }}
        $ResolvedTarget = (Resolve-Path -LiteralPath (Join-Path $ResolvedRoot $LeafName)).Path
        if (-not [StringComparer]::OrdinalIgnoreCase.Equals((Split-Path -Parent $ResolvedTarget), $ResolvedRoot)) {{ throw "SHA-256 target escaped its root: $LeafName" }}
        $ActualHash = Get-Sha256Hex -Path $ResolvedTarget
        if ($ActualHash -cne $ExpectedHash) {{ throw "SHA-256 diverged: $LeafName" }}
    }}
}}

function Assert-GitBundle {{
    param([string] $Path)
    $VerifyParent = (Resolve-Path -LiteralPath ([IO.Path]::GetTempPath())).Path.TrimEnd('\')
    $VerifyLeaf = 'hyperlab-pm-git-bundle-verify-' + [Guid]::NewGuid().ToString('N')
    $VerifyRoot = Join-Path $VerifyParent $VerifyLeaf
    if (Test-Path -LiteralPath $VerifyRoot) {{ throw 'Git bundle verification root must be new.' }}
    try {{
        git init --bare --quiet $VerifyRoot
        if ($LASTEXITCODE -ne 0) {{ throw 'Temporary bare repository initialization failed.' }}
        git -C $VerifyRoot bundle verify $Path
        if ($LASTEXITCODE -ne 0) {{ throw 'git bundle verify failed.' }}
    }} finally {{
        if (Test-Path -LiteralPath $VerifyRoot) {{
            $ResolvedVerifyRoot = (Resolve-Path -LiteralPath $VerifyRoot).Path.TrimEnd('\')
            $ExpectedVerifyRoot = [IO.Path]::GetFullPath($VerifyRoot).TrimEnd('\')
            if (-not [StringComparer]::OrdinalIgnoreCase.Equals($ResolvedVerifyRoot, $ExpectedVerifyRoot)) {{ throw 'Refusing unsafe Git bundle verification cleanup.' }}
            Remove-Item -LiteralPath $ResolvedVerifyRoot -Recurse -Force
        }}
    }}
}}

$BundlePath = (Resolve-Path -LiteralPath (Join-Path $BundleRoot '{bundle}')).Path
if (-not [StringComparer]::OrdinalIgnoreCase.Equals((Split-Path -Parent $BundlePath), $BundleRoot)) {{ throw 'Git bundle escaped the pack root.' }}
if ((Get-Sha256Hex -Path $BundlePath) -ne '{bundle_sha}') {{ throw 'Git bundle SHA-256 diverged.' }}
Assert-Sha256Manifest -ManifestPath (Join-Path $BundleRoot 'handoff.sha256') -ContentRoot $BundleRoot
Assert-Sha256Manifest -ManifestPath (Join-Path $BundleRoot 'wheelhouse.sha256') -ContentRoot (Join-Path $BundleRoot 'wheelhouse')
Assert-GitBundle -Path $BundlePath
ssh -i $SshKeyPath $SshTarget "test ! -e '$IncomingRoot' && install -d -m 0700 '$IncomingRoot'"
if ($LASTEXITCODE -ne 0) {{ throw 'Unique incoming root creation failed.' }}
Get-ChildItem -LiteralPath $BundleRoot -Force | ForEach-Object {{
    scp -i $SshKeyPath -r -- $_.FullName "${{SshTarget}}:${{IncomingRoot}}/"
    if ($LASTEXITCODE -ne 0) {{ throw "Transfer failed: $($_.Name)" }}
}}
$RemoteVerify = @'
set -Eeuo pipefail
INCOMING_ROOT='{incoming}'
BUNDLE_PATH="$INCOMING_ROOT/{bundle}"
VERIFY_REPO=''
cleanup() {{
  status=$?
  trap - EXIT
  if [[ -n "$VERIFY_REPO" ]]; then
    case "$VERIFY_REPO" in
      "$INCOMING_ROOT"/.git-bundle-verify.*) ;;
      *) printf 'PREDICTION_REMOTE_BUNDLE_CLEANUP_REFUSED\n' >&2; exit 4 ;;
    esac
    rm -rf -- "$VERIFY_REPO" || exit 4
  fi
  exit "$status"
}}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
cd "$INCOMING_ROOT"
sha256sum -c handoff.sha256
(cd wheelhouse && sha256sum -c ../wheelhouse.sha256)
VERIFY_REPO=$(mktemp -d "$INCOMING_ROOT/.git-bundle-verify.XXXXXXXX")
git init --bare --quiet "$VERIFY_REPO"
git -C "$VERIFY_REPO" bundle verify "$BUNDLE_PATH"
printf 'PREDICTION_REMOTE_BUNDLE_VERIFIED\n'
'@
ssh -i $SshKeyPath $SshTarget $RemoteVerify
if ($LASTEXITCODE -ne 0) {{ throw 'Transferred bundle or hashes diverged; do not run the Tabby block.' }}
Write-Output 'PREDICTION_WINDOWS_TRANSFER_VERIFIED'
"""


def render_tabby_install(handoff: Mapping[str, object]) -> str:
    incoming = handoff["incoming_root"]
    source = handoff["source_root"]
    campaign = handoff["campaign_root"]
    base = handoff["volume_base"]
    commit = handoff["source_commit"]
    bundle = handoff["bundle_filename"]
    return f"""#!/usr/bin/env bash
# Lieu: Tabby/VPS Bash sous hyperlab. Durée attendue: 5-15 min; maximum: 35 min.
# Prompts: sudo peut demander le mot de passe; aucun pip réseau. Ctrl+C avant la
# première activation laisse seulement de nouvelles racines isolées. Après activation,
# les services déjà démarrés peuvent rester actifs: utiliser E rollback pour les arrêter.
# Signal terminal exact: PREDICTION_INSTALL_ACTIVATION_GREEN.
set -Eeuo pipefail
umask 077
INCOMING_ROOT='{incoming}'
SOURCE_ROOT='{source}'
CAMPAIGN_ROOT='{campaign}'
VOLUME_BASE='{base}'
[[ $(id -un) == hyperlab ]] || {{ printf 'PREDICTION_TABBY_REFUSED:user\n' >&2; exit 4; }}
python3.12 -I "$INCOMING_ROOT/scripts/preflight.py" host --handoff "$INCOMING_ROOT/handoff.json" --report "$INCOMING_ROOT/host-preflight-report.json"
sudo install -d -o hyperlab -g hyperlab -m 0700 "$VOLUME_BASE" "$VOLUME_BASE/sources" "$VOLUME_BASE/campaigns"
python3.12 -I "$INCOMING_ROOT/scripts/preflight.py" fsync --handoff "$INCOMING_ROOT/handoff.json" --report "$INCOMING_ROOT/filesystem-fsync-report.json"
[[ ! -e "$SOURCE_ROOT" && ! -L "$SOURCE_ROOT" && ! -e "$CAMPAIGN_ROOT" && ! -L "$CAMPAIGN_ROOT" ]] || {{ printf 'PREDICTION_TABBY_REFUSED:attempt_roots_must_be_new\n' >&2; exit 4; }}
git clone --no-checkout "$INCOMING_ROOT/{bundle}" "$SOURCE_ROOT"
git -C "$SOURCE_ROOT" checkout --detach '{commit}'
python3.12 -I "$INCOMING_ROOT/scripts/launch_pack.py" verify-source --source-root "$SOURCE_ROOT" --inventory "$INCOMING_ROOT/source-inventory.json" --expected-commit '{commit}'
bash "$INCOMING_ROOT/scripts/bootstrap-offline.sh" "$SOURCE_ROOT" "$INCOMING_ROOT/wheelhouse"
printf 'PREDICTION_SOURCE_ROOT=%s\n' "$SOURCE_ROOT"
printf 'PREDICTION_CAMPAIGN_ROOT=%s\n' "$CAMPAIGN_ROOT"
bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/install.sh" "$INCOMING_ROOT"
"""


def render_tabby_monitor(handoff: Mapping[str, object]) -> str:
    incoming = handoff["incoming_root"]
    source = handoff["source_root"]
    return f"""#!/usr/bin/env bash
# Lieu: second onglet Tabby. Durée: jusqu'à la première transition/alerte.
# Prompts: aucun. Ctrl+C arrête seulement ce moniteur read-only.
# Signal terminal: PREDICTION_MONITOR_TRANSITION_OR_ALERT.
set -Eeuo pipefail
PREVIOUS=''
while :; do
  if ! CURRENT=$(bash '{source}/ops/prediction_markets_launch_v1/monitor.sh' '{incoming}/handoff.json'); then
    printf 'PREDICTION_MONITOR_EXECUTION_FAILED\n' >&2
    printf 'PREDICTION_MONITOR_TRANSITION_OR_ALERT\n'
    exit 4
  fi
  printf '%s\n' "$CURRENT"
  if ! PARSED=$(printf '%s' "$CURRENT" | python3.12 -I -c 'import json,re,sys; d=json.load(sys.stdin); f=d.get("semantic_fingerprint_sha256"); assert isinstance(f,str) and re.fullmatch(r"[0-9a-f]{{64}}",f); assert isinstance(d.get("alert"),bool); print(f,"yes" if d["alert"] else "no")'); then
    printf 'PREDICTION_MONITOR_JSON_INVALID\n' >&2
    printf 'PREDICTION_MONITOR_TRANSITION_OR_ALERT\n'
    exit 4
  fi
  read -r FINGERPRINT ALERT <<< "$PARSED"
  if [[ $ALERT == yes || ( -n $PREVIOUS && $FINGERPRINT != "$PREVIOUS" ) ]]; then
    printf 'PREDICTION_MONITOR_TRANSITION_OR_ALERT\n'
    break
  fi
  PREVIOUS=$FINGERPRINT
  sleep 10
done
"""


def render_windows_tunnel(handoff: Mapping[str, object]) -> str:
    return """# Lieu: Windows PowerShell local. Durée: service continu; maximum: aucune.
# Prompts: SSH host-key/password possibles. Ctrl+C ferme uniquement le tunnel.
# Signal: ouvrir http://127.0.0.1:18081 après l'affichage PREDICTION_TUNNEL_READY.
$ErrorActionPreference = 'Stop'
$SshTarget = $env:HYPERLAB_PM_SSH_TARGET
if ([string]::IsNullOrWhiteSpace($SshTarget) -or $SshTarget -notmatch '^[a-z0-9._-]+@[a-z0-9.-]+$') { throw 'Set HYPERLAB_PM_SSH_TARGET to user@host.' }
$SshKeyRaw = $env:HYPERLAB_PM_SSH_KEY
if ([string]::IsNullOrWhiteSpace($SshKeyRaw)) { throw 'Set HYPERLAB_PM_SSH_KEY to the dedicated private-key path.' }
$SshKeyPath = (Resolve-Path -LiteralPath $SshKeyRaw).Path
if (-not (Test-Path -LiteralPath $SshKeyPath -PathType Leaf)) { throw 'HYPERLAB_PM_SSH_KEY is not a regular file.' }
$ReadyCommand = 'cmd.exe /d /c echo PREDICTION_TUNNEL_READY http://127.0.0.1:18081'
ssh -i $SshKeyPath -N -o ExitOnForwardFailure=yes -o PermitLocalCommand=yes -o "LocalCommand=$ReadyCommand" -L 127.0.0.1:18081:127.0.0.1:18081 $SshTarget
if ($LASTEXITCODE -ne 0) { throw 'Dashboard tunnel failed before or after authenticated establishment.' }
"""


def render_recovery_rollback(handoff: Mapping[str, object]) -> str:
    incoming = handoff["incoming_root"]
    source = handoff["source_root"]
    return f"""#!/usr/bin/env bash
# Lieu: Tabby/VPS Bash. Durée attendue: 1-4 min; maximum: 12 min.
# Prompts: sudo peut demander le mot de passe. Ctrl+C ne supprime aucune preuve,
# mais peut laisser un sous-ensemble des trois services Prediction Markets actif.
# RECOVERY redémarre seulement les services Prediction Markets admissibles.
# ROLLBACK arrête/désactive seulement ces trois services et préserve tous les raw/manifests/runs.
set -Eeuo pipefail
MODE=${{1:-}}
case "$MODE" in
  recovery)
    bash '{source}/ops/prediction_markets_launch_v1/rollback.sh' recovery '{incoming}/handoff.json'
    ;;
  rollback)
    bash '{source}/ops/prediction_markets_launch_v1/rollback.sh' rollback '{incoming}/handoff.json'
    ;;
  *) printf 'usage: bash E-recovery-rollback.sh recovery|rollback\n' >&2; exit 4 ;;
esac
"""


def _write_new(path: Path, payload: bytes, *, executable: bool = False) -> None:
    if path.exists():
        raise LaunchPackError(f"output path already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700 if executable else 0o600)
    with os.fdopen(descriptor, "wb", closefd=True) as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _wheelhouse_manifest(wheelhouse: Path) -> bytes:
    wheels = sorted(wheelhouse.glob("*.whl"), key=lambda item: item.name.lower())
    if not wheels or any(item.is_symlink() or not item.is_file() for item in wheels):
        raise LaunchPackError("Linux wheelhouse must contain only real wheel files")
    return "".join(f"{sha256_file(item)}  {item.name}\n" for item in wheels).encode("ascii")


def _transfer_inventory(output_root: Path, relative_paths: Sequence[str]) -> dict[str, object]:
    rows = []
    for relative in sorted(relative_paths):
        path = output_root / relative
        rows.append({"path": relative.replace("\\", "/"), "sha256": sha256_file(path), "size": path.stat().st_size})
    return {"files": rows, "schema_version": 1}


def finalize(
    *,
    repo_root: Path,
    plan_path: Path,
    output_root: Path,
    bundle_path: Path,
    source_commit: str,
    run_slug: str,
) -> dict[str, object]:
    plan = validate_plan(_object(plan_path))
    if _COMMIT.fullmatch(source_commit) is None:
        raise LaunchPackError("source commit is invalid")
    if _git(repo_root, "rev-parse", "HEAD") != source_commit:
        raise LaunchPackError("source HEAD differs from requested final commit")
    if _git(repo_root, "rev-parse", f"refs/heads/{EXPECTED_BRANCH}") != source_commit:
        raise LaunchPackError("target branch differs from requested final commit")
    if _git(repo_root, "status", "--porcelain"):
        raise LaunchPackError("launch worktree must be clean before finalization")
    if not _git_is_ancestor(repo_root, str(plan["base_commit"]), source_commit):
        raise LaunchPackError("authoritative base is not an ancestor of the final commit")
    candidate_config = _object(
        repo_root / "config" / "research" / "prediction-markets-candidate-v1.json"
    )
    if sha256_bytes(canonical_json_bytes(candidate_config)) != plan["candidate_config_sha256"]:
        raise LaunchPackError("candidate config logical identity diverged")
    candidate_pack = repo_root.joinpath(*PurePosixPath(str(plan["campaign_pack_path"])).parts)
    candidate_manifest_path = candidate_pack / "campaign-manifest.json"
    candidate_manifest = _object(candidate_manifest_path)
    claimed_manifest = candidate_manifest.get("manifest_sha256")
    manifest_body = {
        key: value
        for key, value in candidate_manifest.items()
        if key != "manifest_sha256"
    }
    pin_fields = (candidate_pack / "campaign-manifest.sha256").read_text(
        encoding="ascii"
    ).strip().split()
    if (
        claimed_manifest != plan["campaign_manifest_sha256"]
        or sha256_bytes(canonical_json_bytes(manifest_body)) != claimed_manifest
        or len(pin_fields) != 2
        or pin_fields[1] != "campaign-manifest.json"
        or sha256_file(candidate_manifest_path) != pin_fields[0]
    ):
        raise LaunchPackError("candidate campaign manifest identity diverged")
    access_manifest = _object(
        repo_root
        / "docs"
        / "evidence"
        / "prediction-markets-candidate-v1"
        / "access-bundle-v1"
        / "bundle-manifest.json"
    )
    if access_manifest.get("bundle_sha256") != plan["access_bundle_sha256"]:
        raise LaunchPackError("candidate access bundle identity diverged")
    if not output_root.is_dir() or output_root.resolve() != output_root:
        raise LaunchPackError("output root must be an existing exact directory")
    if bundle_path.parent != output_root or not bundle_path.is_file():
        raise LaunchPackError("Git bundle must already exist in the new output root")
    if _RUN_SLUG.fullmatch(run_slug) is None:
        raise LaunchPackError("run slug is invalid")
    remote = plan["remote"]
    assert isinstance(remote, Mapping)
    incoming = _remote_path(str(remote["home_parent"]), run_slug)
    volume_base = str(remote["volume_base"])
    source = _remote_path(f"{volume_base}/sources", run_slug)
    campaign = _remote_path(f"{volume_base}/campaigns", run_slug)
    services = _service_names(run_slug)
    source_inventory = build_source_inventory(repo_root, source_commit)
    _write_new(output_root / "source-inventory.json", canonical_json_bytes(source_inventory) + b"\n")
    wheelhouse_payload = _wheelhouse_manifest(output_root / "wheelhouse")
    _write_new(output_root / "wheelhouse.sha256", wheelhouse_payload)
    script_root = repo_root / "ops" / "prediction_markets_launch_v1"
    for name in _SCRIPTS:
        source_path = script_root / name
        if not source_path.is_file() or source_path.is_symlink():
            raise LaunchPackError(f"launch script is absent or unsafe: {name}")
        _write_new(output_root / "scripts" / name, source_path.read_bytes(), executable=name.endswith(".sh"))
    handoff_base: dict[str, object] = {
        "access_bundle_sha256": plan["access_bundle_sha256"],
        "base_commit": plan["base_commit"],
        "boundary": BOUNDARY,
        "bundle_filename": bundle_path.name,
        "bundle_sha256": sha256_file(bundle_path),
        "campaign_root": campaign,
        "candidate_config_sha256": plan["candidate_config_sha256"],
        "candidate_pack_manifest_sha256": plan["campaign_manifest_sha256"],
        "dashboard_port": 18081,
        "disk": plan["disk"],
        "economic_evidence_status": plan["economic_evidence_status"],
        "incoming_root": incoming,
        "pack_id": PACK_ID,
        "quick_start_default": "IMMEDIATE_AFTER_SUCCESSFUL_INSTALL",
        "run_slug": run_slug,
        "schema_version": 1,
        "service_user": plan["service_user"],
        "services": services,
        "source_commit": source_commit,
        "source_inventory_sha256": source_inventory["inventory_sha256"],
        "source_root": source,
        "start_at_override": "HYPERLAB_PM_START_AT_UTC_OPTIONAL",
        "volume_base": volume_base,
        "volume_mount": remote["volume_mount"],
        "wheelhouse_manifest_sha256": sha256_bytes(wheelhouse_payload),
    }
    _write_new(
        output_root / "README.md",
        render_operator_readme(handoff_base).encode("utf-8"),
    )
    units = render_units(handoff_base)
    for name, content in units.items():
        _write_new(output_root / "systemd" / name, content.encode("utf-8"))
    operator_blocks = {
        "A-windows-bundle-verify-transfer.ps1": render_windows_transfer(handoff_base),
        "B-tabby-preflight-install-activate.sh": render_tabby_install(handoff_base),
        "C-tabby-readonly-monitor.sh": render_tabby_monitor(handoff_base),
        "D-windows-dashboard-tunnel.ps1": render_windows_tunnel(handoff_base),
        "E-recovery-rollback.sh": render_recovery_rollback(handoff_base),
    }
    for name, content in operator_blocks.items():
        _write_new(output_root / "operator" / name, content.encode("utf-8"), executable=name.endswith(".sh"))
    transfer_paths = [
        bundle_path.name,
        "README.md",
        "source-inventory.json",
        "wheelhouse.sha256",
        *[f"scripts/{name}" for name in _SCRIPTS],
        *[f"systemd/{name}" for name in units],
        *[f"operator/{name}" for name in operator_blocks],
    ]
    transfer = _transfer_inventory(output_root, transfer_paths)
    transfer_payload = canonical_json_bytes(transfer) + b"\n"
    _write_new(output_root / "transfer-inventory.json", transfer_payload)
    handoff = {**handoff_base, "transfer_inventory_sha256": sha256_bytes(transfer_payload)}
    handoff_payload = canonical_json_bytes(handoff) + b"\n"
    _write_new(output_root / "handoff.json", handoff_payload)
    _write_new(
        output_root / "handoff.sha256",
        f"{sha256_bytes(handoff_payload)}  handoff.json\n".encode("ascii"),
    )
    return handoff


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prediction Markets launch-pack generator")
    subparsers = parser.add_subparsers(dest="command", required=True)
    final = subparsers.add_parser("finalize")
    final.add_argument("--repo-root", type=Path, required=True)
    final.add_argument("--plan", type=Path, required=True)
    final.add_argument("--output-root", type=Path, required=True)
    final.add_argument("--bundle", type=Path, required=True)
    final.add_argument("--source-commit", required=True)
    final.add_argument("--run-slug", required=True)
    verify = subparsers.add_parser("verify-source")
    verify.add_argument("--source-root", type=Path, required=True)
    verify.add_argument("--inventory", type=Path, required=True)
    verify.add_argument("--expected-commit", required=True)
    return parser


def _fail(message: str) -> NoReturn:
    print(f"PREDICTION_LAUNCH_PACK_REFUSED:{message}", file=sys.stderr)
    raise SystemExit(4)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "finalize":
            result = finalize(
                repo_root=arguments.repo_root.resolve(strict=True),
                plan_path=arguments.plan.resolve(strict=True),
                output_root=arguments.output_root.resolve(strict=True),
                bundle_path=arguments.bundle.resolve(strict=True),
                source_commit=arguments.source_commit,
                run_slug=arguments.run_slug,
            )
        else:
            result = verify_source(
                arguments.source_root.resolve(strict=True),
                arguments.inventory.resolve(strict=True),
                arguments.expected_commit,
            )
        print(canonical_json_bytes(result).decode("utf-8"))
        return 0
    except (LaunchPackError, OSError, subprocess.SubprocessError) as error:
        _fail(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
