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
EXPECTED_BRANCH = "codex/prediction-markets-runtime-data-quality-v1"
PACK_ID = "prediction-markets-runtime-data-quality-v1"
SUPERSEDED_RUN_SLUG = "pm-20260828t024827z-bcb5280f"
SUPERSEDED_SOURCE_COMMIT = "bcb5280f87393992e2aa4528188009186cd8bdc3"
_RUN_SLUG = re.compile(r"^pm-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SERVICE = re.compile(r"^hyperlab-pm-[a-z0-9-]+-(?:polymarket|kalshi|dashboard)\.service$")
_PROBE_SERVICE = re.compile(
    r"^hyperlab-pm-[a-z0-9-]+-(?:polymarket|kalshi)-namespace-probe\.service$"
)
_SCRIPTS = (
    "bootstrap-offline.sh",
    "cockpit.py",
    "cutover.sh",
    "install.sh",
    "launch_pack.py",
    "monitor.sh",
    "preflight.py",
    "rollback.sh",
    "runner.py",
    "systemd_cutover.py",
)
_SHELL_SHEBANG = b"#!/usr/bin/env bash\n"
_EXPECTED_TRANSFERRED_SHELLS = frozenset(
    {
        *(f"scripts/{name}" for name in _SCRIPTS if name.endswith(".sh")),
        "operator/B-tabby-preflight-install-activate.sh",
        "operator/C-tabby-readonly-monitor.sh",
        "operator/E-recovery-rollback.sh",
    }
)


def _superseded_campaign_contract() -> dict[str, object]:
    slug = SUPERSEDED_RUN_SLUG
    volume_base = "/mnt/HC_Volume_106716684/hyperlab-prediction-markets"
    return {
        "campaign_root": f"{volume_base}/campaigns/{slug}",
        "dashboard_port": 18081,
        "incoming_root": f"/home/hyperlab/hyperlab-prediction-markets/incoming/{slug}",
        "namespace_probe_services": _namespace_probe_service_names(slug),
        "run_slug": slug,
        "services": _service_names(slug),
        "source_commit": SUPERSEDED_SOURCE_COMMIT,
        "source_root": f"{volume_base}/sources/{slug}",
    }


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


def _authenticate_source_branch(
    repo_root: Path,
    *,
    expected_branch: str,
    source_commit: str,
) -> None:
    try:
        checked_branch = _git(
            repo_root,
            "check-ref-format",
            "--branch",
            expected_branch,
        )
    except LaunchPackError as error:
        raise LaunchPackError("expected branch is invalid") from error
    if checked_branch != expected_branch:
        raise LaunchPackError("expected branch is invalid")
    if (
        _git(repo_root, "rev-parse", "--verify", f"refs/heads/{expected_branch}")
        != source_commit
    ):
        raise LaunchPackError("target branch differs from requested final commit")


def _git_blob(repo_root: Path, commit: str, relative_path: str) -> bytes:
    if _COMMIT.fullmatch(commit) is None:
        raise LaunchPackError("source commit is invalid")
    pure = PurePosixPath(relative_path)
    if pure.is_absolute() or not pure.parts or any(part in {"", ".", ".."} for part in pure.parts):
        raise LaunchPackError("Git blob path is unsafe")
    completed = subprocess.run(
        ["git", "-C", str(repo_root), "cat-file", "blob", f"{commit}:{pure.as_posix()}"],
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        diagnostic = completed.stderr.decode("utf-8", errors="replace").strip()
        raise LaunchPackError(diagnostic or f"Git blob is absent: {pure.as_posix()}")
    return completed.stdout


def validate_posix_shell_payload(payload: bytes, *, label: str) -> bytes:
    if payload.startswith(b"\xef\xbb\xbf"):
        raise LaunchPackError(f"POSIX shell payload has a UTF-8 BOM: {label}")
    if b"\x00" in payload:
        raise LaunchPackError(f"POSIX shell payload contains NUL: {label}")
    if b"\r" in payload:
        raise LaunchPackError(f"POSIX shell payload contains CR: {label}")
    try:
        payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise LaunchPackError(f"POSIX shell payload is not UTF-8: {label}") from error
    if not payload.startswith(_SHELL_SHEBANG):
        raise LaunchPackError(f"POSIX shell payload has an invalid Bash shebang: {label}")
    if not payload.endswith(b"\n"):
        raise LaunchPackError(f"POSIX shell payload lacks a final LF: {label}")
    return payload


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


def _namespace_probe_service_names(slug: str) -> dict[str, str]:
    suffix = slug.removeprefix("pm-")
    values = {
        venue: f"hyperlab-pm-{suffix}-{venue}-namespace-probe.service"
        for venue in ("polymarket", "kalshi")
    }
    if any(_PROBE_SERVICE.fullmatch(value) is None for value in values.values()):
        raise LaunchPackError("rendered namespace probe service identity is invalid")
    return values


def _common_unit(
    service_user: str,
    source_root: str,
    campaign_root: str,
    incoming_root: str,
    volume_base: str,
    volume_mount: str,
) -> str:
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
ReadOnlyPaths={volume_mount} {volume_base} {incoming_root} {source_root} {campaign_root}
"""


def render_units(handoff: Mapping[str, object]) -> dict[str, str]:
    source_root = _text(handoff.get("source_root"), label="source root")
    campaign_root = _text(handoff.get("campaign_root"), label="campaign root")
    incoming_root = _text(handoff.get("incoming_root"), label="incoming root")
    volume_base = _text(handoff.get("volume_base"), label="volume base")
    volume_mount = _text(handoff.get("volume_mount"), label="volume mount")
    service_user = _text(handoff.get("service_user"), label="service user")
    services = handoff.get("services")
    if not isinstance(services, Mapping):
        raise LaunchPackError("service map is absent")
    python = f"{source_root}/.venv/bin/python"
    runtime_import_admission = (
        f"{python} -I {source_root}/ops/prediction_markets_launch_v1/preflight.py "
        f"runtime-import-admission --handoff {incoming_root}/handoff.json "
        f"--source-root {source_root} "
        f"--source-inventory {incoming_root}/source-inventory.json"
    )
    common = _common_unit(
        service_user,
        source_root,
        campaign_root,
        incoming_root,
        volume_base,
        volume_mount,
    )
    run_slug = _text(handoff.get("run_slug"), label="run slug")
    namespace_probes = _namespace_probe_service_names(run_slug)
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
ExecStartPre={runtime_import_admission}
ExecStart={python} {source_root}/ops/prediction_markets_launch_v1/runner.py --handoff {incoming_root}/handoff.json --venue {venue}
TimeoutStartSec=180
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
        probe_service = namespace_probes[venue]
        units[probe_service] = f"""[Unit]
Description=HyperLab Prediction Markets {venue} systemd namespace admission probe
After=network.target time-sync.target
Wants=time-sync.target

[Service]
Type=oneshot
{common}ReadWritePaths={campaign_root}/{venue}
ExecStart={python} -I {source_root}/ops/prediction_markets_launch_v1/preflight.py runner-namespace-guard --handoff {incoming_root}/handoff.json --install-admission-report {campaign_root}/state/install-admission-report.json --venue {venue}
TimeoutStartSec=30
Restart=no

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
{common}ExecStartPre={runtime_import_admission}
ExecStart={python} {source_root}/ops/prediction_markets_launch_v1/cockpit.py --campaign-root {campaign_root} --host 127.0.0.1 --port 18081
TimeoutStartSec=180
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
"""
    return units


def render_operator_readme(handoff: Mapping[str, object]) -> str:
    services = handoff["services"]
    assert isinstance(services, Mapping)
    namespace_probes = _namespace_probe_service_names(str(handoff["run_slug"]))
    superseded = handoff["superseded_campaign"]
    assert isinstance(superseded, Mapping)
    old_services = superseded["services"]
    old_probes = superseded["namespace_probe_services"]
    assert isinstance(old_services, Mapping) and isinstance(old_probes, Mapping)
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
   `operator/C-tabby-readonly-monitor.sh`. Il authentifie séparément dashboard,
   Polymarket, Kalshi, zéro restart, les deux ledgers et l'ordinal 0 de la
   nouvelle campagne. Signal : `PREDICTION_MONITOR_FIRST_SLOTS_AUTHENTICATED`.
4. Sur Windows PowerShell, exécuter
   `operator/D-windows-dashboard-tunnel.ps1`. Ouvrir l'URL seulement après
   `PREDICTION_TUNNEL_READY http://127.0.0.1:18081`.
5. `operator/E-recovery-rollback.sh recovery|rollback-new|restore-old` reprend
   la nouvelle campagne sans rejouer un slot terminal, désarme ses cinq unités,
   ou restaure explicitement l'ancienne campagne après preuve qu'aucun nouveau
   collecteur ne tourne.
   Aucun raw, manifest, ledger, run ou rapport n'est supprimé. Le signal final
   de `restore-old` est
   `PREDICTION_OLD_CAMPAIGN_RESTORE_VERIFIED_NO_NEW_COLLECTOR`.

A vérifie tous les hashes et tous les shells, localement puis dans le nouvel
incoming : UTF-8 sans BOM, aucun NUL/CR, shebang Bash exact et LF final. B fait
une seule authentification sudo foreground, maintient son cache uniquement par
`sudo -n -v`, prépare complètement clone, venv et imports pendant que l'ancienne
campagne reste active, puis réauthentifie ses
cinq unités immédiatement avant le cutover. Un échec de préparation n'appelle
jamais `disarm-old` et ne demande pas E. Après cette authentification, B et E
n'emploient que `sudo -n`; chaque mutation systemd est bornée et annonce son
opération et son service avant/après.

L'import runtime n'est jamais confié au cwd ou à `PYTHONPATH` sous `python -I`.
B exécute avant cutover le contrat isolé `runtime-import-admission` : il lie le
venv au source root/commit/inventaire exacts, insère explicitement les deux
racines source et refuse tout module provenant du user-site, du système global,
d'un ancien source ou d'un chemin symlinké. Son unique signal terminal est
`PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN`. Le même contrat précède install,
monitor et chaque démarrage des services; aucun pip editable/réseau ou retrait
de `-I` n'est admis.

`verify-old` ne demande jamais une sous-commande récente au code historique.
Le Python du venv superseded exécute l'outil `preflight.py` du clone candidat
authentifié avec l'adaptateur versionné `prediction-markets-bcb5280f-runtime-v1`.
L'outil et la target gardent des identités séparées : commit/inventaire/script
candidats d'un côté, source/commit/inventaire/venv/modules historiques de
l'autre. La target inconnue, sale, symlinkée, arrêtée ou hors schéma est refusée.

## Diagnostic read-only puis reprise de restore-old

Si E est interrompu ou affiche un timeout, conserver toute sa sortie et lancer
dans un autre onglet Tabby uniquement ces lectures :

```bash
systemctl list-jobs --no-pager
for service in \
  '{old_services['polymarket']}' \
  '{old_services['kalshi']}' \
  '{old_services['dashboard']}' \
  '{old_probes['polymarket']}' \
  '{old_probes['kalshi']}' \
  '{services['polymarket']}' \
  '{services['kalshi']}' \
  '{services['dashboard']}' \
  '{namespace_probes['polymarket']}' \
  '{namespace_probes['kalshi']}'
do
  systemctl show "$service" --property=LoadState,ActiveState,SubState,Result,MainPID,NRestarts,FragmentPath --no-pager
  systemctl is-enabled "$service" --no-pager || true
done
systemctl list-units --type=service --state=active --no-legend --no-pager 'hyperlab-pm-*'
ss -H -ltnp 'sport = :18081'
```

Ces commandes ne modifient rien. Ne pas employer de glob avec stop/disable et
ne supprimer aucun fichier. Après conservation du diagnostic, reprendre
exactement `bash operator/E-recovery-rollback.sh restore-old`; les services déjà
dans leur état terminal seront sautés, les autres reprendront sous timeout.

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
- probe namespace Polymarket : `{namespace_probes['polymarket']}`
- probe namespace Kalshi : `{namespace_probes['kalshi']}`

Polymarket et Kalshi sont indépendants. Un reçu authentique
`PUBLIC_SOURCE_INVALID` est comptabilisé comme slot terminal de qualité de
donnée, mais reste `source_usable=false`, `economic_eligible=false` et n'est
jamais rejoué. Une divergence de reçu, plan, identité, hash, manifest ou ledger
reste `INTEGRITY_FAILED` et ne redémarre pas en boucle.

B exécute et authentifie d'abord les deux probes namespace oneshot, avant tout
service persistant. Pour chacun, il exige simultanément `Result=success`,
`ExecMainStatus=0`, `inactive/dead`, `MainPID=0`, zéro restart, le fragment exact
et l'unique JSON canonique GREEN (`namespace_admissible=true`, `errors=[]`) avec
son graphe mount/write authentifié. `ExecMainCode` est borné aux deux formes 0
ou 1 compatibles avec une fin oneshot normale réussie et ne constitue jamais à
lui seul un oracle. Le moniteur utilise exclusivement le Python du venv offline
lié au source root authentifié. Avant tout collecteur, B exige ensuite le HTTP loopback
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
H1 ni la marge. L'essai `73c6d2d2` a observé environ 296,3 GB disponibles, mais
cette preuve historique ne vaut jamais admission future et aucun historique ne
peut être supprimé pour gagner de la place. Chaque reprise systemd réauthentifie handoff, admission,
transfert, source, NTP, racines et device ext4 avant de sélectionner un ordinal.
Sous le namespace durci, le target volume admis reste exact read-only. systemd
peut coalescer les `ReadOnlyPaths` imbriqués : `volume_base`, source et campagne
ne peuvent résoudre que vers leurs targets RO allowlistés. L'incoming accepte
uniquement son target exact ou un ancêtre canonique de sa chaîne entre `/home`
et lui-même. Si `/home` appartient au filesystem racine, `TARGET=/` est admis
uniquement pour les vues RO `/home`/incoming lorsque la chaîne canonique, ext4,
`MAJ:MIN`/`stat(2)`, `SOURCE` et `FSROOT=/` concordent; il reste refusé pour le
volume `/dev/sdb` et toute vue RW. Autre home, cousin, descendant arbitraire et
symlink restent refusés. Leur device ext4, `MAJ:MIN` et relation `SOURCE`/`FSROOT` sont
réauthentifiés depuis le target effectif ; seule la venue doit rester un bind
exact read-write. Le runner répète create/fsync/unlink/fsync avant chaque slot.
Un refus produit un diagnostic borné avec `TARGET`, `SOURCE`, `FSTYPE`,
`VFS_OPTIONS`, `MAJ:MIN`, `FSROOT` et chemin logique, puis désarme les cinq unités du
slug; aucun raw n'est supprimé. Le runner applique ensuite son gate capacité lié
au ledger. Un refus antérieur au slot sort en code 4 sans boucle de restart.
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

function Assert-PosixShellPayload {{
    param([string] $Path)
    [byte[]] $Bytes = [IO.File]::ReadAllBytes($Path)
    if ($Bytes.Length -lt 21) {{ throw "POSIX shell payload is too short: $Path" }}
    if ($Bytes[0] -eq 0xEF -and $Bytes[1] -eq 0xBB -and $Bytes[2] -eq 0xBF) {{ throw "POSIX shell payload has a UTF-8 BOM: $Path" }}
    if ([Array]::IndexOf($Bytes, [byte]0) -ge 0) {{ throw "POSIX shell payload contains NUL: $Path" }}
    if ([Array]::IndexOf($Bytes, [byte]13) -ge 0) {{ throw "POSIX shell payload contains CR: $Path" }}
    $StrictUtf8 = New-Object System.Text.UTF8Encoding($false, $true)
    try {{ $null = $StrictUtf8.GetString($Bytes) }} catch {{ throw "POSIX shell payload is not strict UTF-8: $Path" }}
    [byte[]] $ExpectedShebang = [Text.Encoding]::ASCII.GetBytes("#!/usr/bin/env bash`n")
    for ($Index = 0; $Index -lt $ExpectedShebang.Length; $Index++) {{
        if ($Bytes[$Index] -ne $ExpectedShebang[$Index]) {{ throw "POSIX shell payload has an invalid Bash shebang: $Path" }}
    }}
    if ($Bytes[$Bytes.Length - 1] -ne 10) {{ throw "POSIX shell payload lacks a final LF: $Path" }}
}}

function Assert-TransferInventory {{
    param([string] $Root)
    $ResolvedRoot = (Resolve-Path -LiteralPath $Root).Path.TrimEnd('\')
    $HandoffPath = Join-Path $ResolvedRoot 'handoff.json'
    $InventoryPath = Join-Path $ResolvedRoot 'transfer-inventory.json'
    $Handoff = Get-Content -LiteralPath $HandoffPath -Raw | ConvertFrom-Json
    if ((Get-Sha256Hex -Path $InventoryPath) -cne [string]$Handoff.transfer_inventory_sha256) {{ throw 'Transfer inventory hash diverged from handoff.' }}
    $Inventory = Get-Content -LiteralPath $InventoryPath -Raw | ConvertFrom-Json
    if ([int]$Inventory.schema_version -ne 1) {{ throw 'Transfer inventory schema diverged.' }}
    $Seen = @{{}}
    $Shells = New-Object System.Collections.Generic.List[string]
    foreach ($Entry in @($Inventory.files)) {{
        if ($null -eq $Entry.PSObject.Properties['path'] -or $null -eq $Entry.PSObject.Properties['sha256'] -or $null -eq $Entry.PSObject.Properties['size'] -or @($Entry.PSObject.Properties).Count -ne 3) {{ throw 'Transfer inventory entry schema diverged.' }}
        $Relative = [string]$Entry.path
        if ($Relative -notmatch '^(?!/)(?!.*(?:^|/)\\.\\.(?:/|$))(?!.*//)[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*$' -or $Seen.ContainsKey($Relative)) {{ throw "Unsafe or duplicate transfer path: $Relative" }}
        $Seen[$Relative] = $true
        $Target = [IO.Path]::GetFullPath((Join-Path $ResolvedRoot ($Relative.Replace('/', [IO.Path]::DirectorySeparatorChar))))
        $RootPrefix = $ResolvedRoot + [IO.Path]::DirectorySeparatorChar
        if (-not $Target.StartsWith($RootPrefix, [StringComparison]::OrdinalIgnoreCase)) {{ throw "Transfer path escaped root: $Relative" }}
        $Item = Get-Item -LiteralPath $Target -Force
        if ($Item.PSIsContainer -or (($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0)) {{ throw "Transferred file is unsafe: $Relative" }}
        if ([long]$Item.Length -ne [long]$Entry.size -or (Get-Sha256Hex -Path $Target) -cne [string]$Entry.sha256) {{ throw "Transferred file identity diverged: $Relative" }}
        if ($Relative.EndsWith('.sh', [StringComparison]::Ordinal)) {{
            Assert-PosixShellPayload -Path $Target
            $Shells.Add($Relative)
        }}
    }}
    $ExpectedShells = @(
        'operator/B-tabby-preflight-install-activate.sh',
        'operator/C-tabby-readonly-monitor.sh',
        'operator/E-recovery-rollback.sh',
        'scripts/bootstrap-offline.sh',
        'scripts/cutover.sh',
        'scripts/install.sh',
        'scripts/monitor.sh',
        'scripts/rollback.sh'
    )
    if ((@($Shells | Sort-Object) -join "`n") -cne (($ExpectedShells | Sort-Object) -join "`n")) {{ throw 'Transferred POSIX shell set diverged.' }}
    Write-Output 'PREDICTION_LOCAL_TRANSFER_SHELLS_LF_AUTHENTICATED'
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
Assert-TransferInventory -Root $BundleRoot
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
python3.12 -I "$INCOMING_ROOT/scripts/launch_pack.py" verify-transfer --incoming-root "$INCOMING_ROOT" --handoff "$INCOMING_ROOT/handoff.json"
printf 'PREDICTION_REMOTE_TRANSFER_SHELLS_LF_AUTHENTICATED\n'
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
# Lieu: Tabby/VPS Bash sous hyperlab. Durée attendue: 7-18 min; maximum: 40 min.
# Prompts: une authentification sudo foreground au début; aucun autre prompt sudo
# ni pip réseau. Ctrl+C pendant
# la préparation laisse l'ancienne campagne active. Après le début du cutover,
# Ctrl+C préserve les preuves mais exige E restore-old avant toute autre activation.
# Signal terminal exact: PREDICTION_INSTALL_ACTIVATION_GREEN.
set -Eeuo pipefail
umask 077
INCOMING_ROOT='{incoming}'
SOURCE_ROOT='{source}'
CAMPAIGN_ROOT='{campaign}'
VOLUME_BASE='{base}'
[[ $(id -un) == hyperlab ]] || {{ printf 'PREDICTION_TABBY_REFUSED:user\n' >&2; exit 4; }}
CUTOVER_STARTED=false
SUDO_KEEPALIVE_PID=''
stop_sudo_keepalive() {{
  if [[ -n $SUDO_KEEPALIVE_PID ]]; then
    kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
    wait "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
  fi
}}
cutover_exit() {{
  status=$?
  stop_sudo_keepalive
  if (( status != 0 )) && [[ $CUTOVER_STARTED == true \
    && -f "$INCOMING_ROOT/cutover-old-premutation.json" \
    && ! -L "$INCOMING_ROOT/cutover-old-premutation.json" ]]; then
    printf 'PREDICTION_NEW_ACTIVATION_FAILED_RUN_E_RESTORE_OLD\n' >&2
  fi
  exit "$status"
}}
trap cutover_exit EXIT
printf 'PREDICTION_SUDO_FOREGROUND_AUTH_BEGIN\n'
if ! sudo -v; then
  printf 'PREDICTION_TABBY_REFUSED:sudo_foreground_authentication_failed\n' >&2
  exit 4
fi
sudo -n true || {{ printf 'PREDICTION_TABBY_REFUSED:sudo_noninteractive_cache_unavailable\n' >&2; exit 4; }}
printf 'PREDICTION_SUDO_FOREGROUND_AUTH_GREEN\n'
(
  SUDO_SLEEP_PID=''
  stop_keepalive_sleep() {{
    [[ -z $SUDO_SLEEP_PID ]] || kill "$SUDO_SLEEP_PID" 2>/dev/null || true
    exit 0
  }}
  trap stop_keepalive_sleep HUP INT TERM
  while kill -0 "$$" 2>/dev/null; do
    sudo -n -v || {{ printf 'PREDICTION_SUDO_KEEPALIVE_FAILED_NO_PROMPT\n' >&2; exit 4; }}
    /usr/bin/sleep 30 </dev/null >/dev/null 2>&1 &
    SUDO_SLEEP_PID=$!
    wait "$SUDO_SLEEP_PID" || exit 0
    SUDO_SLEEP_PID=''
  done
) &
SUDO_KEEPALIVE_PID=$!
python3.12 -I "$INCOMING_ROOT/scripts/preflight.py" host --handoff "$INCOMING_ROOT/handoff.json" --report "$INCOMING_ROOT/host-preflight-report.json"
sudo -n install -d -o hyperlab -g hyperlab -m 0700 "$VOLUME_BASE" "$VOLUME_BASE/sources" "$VOLUME_BASE/campaigns"
python3.12 -I "$INCOMING_ROOT/scripts/preflight.py" fsync --handoff "$INCOMING_ROOT/handoff.json" --report "$INCOMING_ROOT/filesystem-fsync-report.json"
[[ ! -e "$SOURCE_ROOT" && ! -L "$SOURCE_ROOT" && ! -e "$CAMPAIGN_ROOT" && ! -L "$CAMPAIGN_ROOT" ]] || {{ printf 'PREDICTION_TABBY_REFUSED:attempt_roots_must_be_new\n' >&2; exit 4; }}
git clone --no-checkout "$INCOMING_ROOT/{bundle}" "$SOURCE_ROOT"
git -C "$SOURCE_ROOT" checkout --detach '{commit}'
python3.12 -I "$INCOMING_ROOT/scripts/launch_pack.py" verify-source --source-root "$SOURCE_ROOT" --inventory "$INCOMING_ROOT/source-inventory.json" --expected-commit '{commit}'
bash "$INCOMING_ROOT/scripts/bootstrap-offline.sh" "$SOURCE_ROOT" "$INCOMING_ROOT/wheelhouse"
VENV_PYTHON="$SOURCE_ROOT/.venv/bin/python"
[[ -x "$VENV_PYTHON" && ! -L "$VENV_PYTHON" ]] || {{ printf 'PREDICTION_TABBY_REFUSED:prepared_runtime_absent_or_unsafe\n' >&2; exit 4; }}
"$VENV_PYTHON" -I "$INCOMING_ROOT/scripts/launch_pack.py" verify-source --source-root "$SOURCE_ROOT" --inventory "$INCOMING_ROOT/source-inventory.json" --expected-commit '{commit}'
RUNTIME_IMPORT_REPORT="$INCOMING_ROOT/runtime-import-admission.json"
timeout --signal=TERM --kill-after=5s 180s env PYTHONNOUSERSITE=1 \
  "$VENV_PYTHON" -I "$SOURCE_ROOT/ops/prediction_markets_launch_v1/preflight.py" runtime-import-admission \
  --handoff "$INCOMING_ROOT/handoff.json" \
  --source-root "$SOURCE_ROOT" \
  --source-inventory "$INCOMING_ROOT/source-inventory.json" \
  --report "$RUNTIME_IMPORT_REPORT"
[[ -s "$RUNTIME_IMPORT_REPORT" && ! -L "$RUNTIME_IMPORT_REPORT" ]] \
  || {{ printf 'PREDICTION_TABBY_REFUSED:runtime_import_admission_report_absent_or_unsafe\n' >&2; exit 4; }}
printf 'PREDICTION_RUNTIME_PREPARED_BEFORE_CUTOVER\n'
printf 'PREDICTION_SOURCE_ROOT=%s\n' "$SOURCE_ROOT"
printf 'PREDICTION_CAMPAIGN_ROOT=%s\n' "$CAMPAIGN_ROOT"
sudo -n -v || {{ printf 'PREDICTION_TABBY_REFUSED:sudo_cache_expired_before_cutover\n' >&2; exit 4; }}
bash "$INCOMING_ROOT/scripts/cutover.sh" verify-old "$INCOMING_ROOT/handoff.json"
CUTOVER_STARTED=true
bash "$INCOMING_ROOT/scripts/cutover.sh" disarm-old "$INCOMING_ROOT/handoff.json"
bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/install.sh" "$INCOMING_ROOT"
stop_sudo_keepalive
SUDO_KEEPALIVE_PID=''
trap - EXIT
"""


def render_tabby_monitor(handoff: Mapping[str, object]) -> str:
    incoming = handoff["incoming_root"]
    source = handoff["source_root"]
    campaign = handoff["campaign_root"]
    return f"""#!/usr/bin/env bash
# Lieu: second onglet Tabby. Durée attendue: 2-5 min; maximum opérateur: 12 min.
# Prompts: aucun. Ctrl+C arrête seulement ce moniteur read-only et ne change
# aucun service, ledger, receipt, raw ou manifest.
# Signal terminal exact: PREDICTION_MONITOR_FIRST_SLOTS_AUTHENTICATED.
set -Eeuo pipefail
while :; do
  if ! CURRENT=$(bash '{source}/ops/prediction_markets_launch_v1/monitor.sh' '{incoming}/handoff.json'); then
    printf 'PREDICTION_MONITOR_EXECUTION_FAILED\n' >&2
    printf 'PREDICTION_MONITOR_OPERATIONAL_FAILURE\n'
    exit 4
  fi
  printf '%s\n' "$CURRENT"
  if ! printf '%s' "$CURRENT" | '{source}/.venv/bin/python' -I -c 'import json,re,sys; d=json.load(sys.stdin); f=d.get("semantic_fingerprint_sha256"); assert isinstance(f,str) and re.fullmatch(r"[0-9a-f]{{64}}",f); assert isinstance(d.get("alert"),bool)'; then
    printf 'PREDICTION_MONITOR_JSON_INVALID\n' >&2
    printf 'PREDICTION_MONITOR_OPERATIONAL_FAILURE\n'
    exit 4
  fi
  set +e
  PROOF=$(CURRENT="$CURRENT" '{source}/.venv/bin/python' -I - '{campaign}' '{source}' <<'PY'
from datetime import datetime
from pathlib import Path
import json,os,sys
source=Path(sys.argv[2]); sys.path[:0]=[str(source/'src'),str(source)]
from hyperlab.research_data.envelope import Venue
from ops.prediction_markets_launch_v1.runner import _validate_result,canonical_json_bytes,load_campaign_context,read_ledger,sha256_bytes,validate_service_ledger_against_manifest
value=json.loads(os.environ['CURRENT'])
if value.get('preflight_error') is not None or value.get('operational_failure') is not False or value.get('activation_admissible') is not True:
 raise SystemExit(4)
services=value.get('services')
if not isinstance(services,dict) or set(services)!=set(('polymarket','kalshi','dashboard')): raise SystemExit(4)
for name in ('polymarket','kalshi','dashboard'):
 service=services[name]; props=service.get('properties')
 if (not isinstance(props,dict) or props.get('NRestarts')!='0' or service.get('restarts_verified') is not True
     or service.get('fragment_verified') is not True): raise SystemExit(4)
if (services['dashboard'].get('command_verified') is not True or services['dashboard'].get('listener_verified') is not True
    or services['dashboard']['properties'].get('ActiveState')!='active'): raise SystemExit(4)
root=Path(sys.argv[1]); context=load_campaign_context(root,source); result={{}}
for venue in (Venue.POLYMARKET,Venue.KALSHI):
 rows=read_ledger(root/venue.value/'ledger.jsonl')
 validate_service_ledger_against_manifest(rows,campaign_manifest=context.manifest,venue=venue)
 if not rows or rows[0].get('ordinal')!=0: raise SystemExit(20)
 first=rows[0]
 scheduled=datetime.fromisoformat(str(first['scheduled_start_utc']).replace('Z','+00:00'))
 run=root/venue.value/'runs'/f"shard-0000-{{scheduled.strftime('%Y%m%dT%H%M%SZ')}}"
 receipt=_validate_result(run,context,venue,ordinal=0)
 if sha256_bytes(canonical_json_bytes(receipt))!=first.get('terminal_result_sha256'): raise SystemExit(4)
 result[venue.value]={{key:first.get(key) for key in ('economic_eligible','manifest_sha256','receipt_classification','root_sha256','source_usable','terminal_health','terminal_result_sha256')}}
 service=services[venue.value]
 state=service.get('state')
 if (service.get('ledger_error') is not None or service.get('command_verified') is not True
     or not isinstance(state,dict) or state.get('recorded_slots',0)<1 or state.get('last_terminal')!=first.get('terminal_health')): raise SystemExit(4)
print(json.dumps({{'dashboard':{{'listener_verified':True,'nrestarts':0}},'first_slots':result}},ensure_ascii=False,separators=(',',':'),sort_keys=True))
PY
  )
  PROOF_STATUS=$?
  set -e
  if (( PROOF_STATUS == 0 )); then
    printf '%s\n' "$PROOF"
    printf 'PREDICTION_MONITOR_DASHBOARD_AUTHENTICATED\n'
    printf 'PREDICTION_MONITOR_NRESTARTS_ZERO_AUTHENTICATED\n'
    printf 'PREDICTION_MONITOR_POLYMARKET_LEDGER_FIRST_SLOT_AUTHENTICATED\n'
    printf 'PREDICTION_MONITOR_KALSHI_LEDGER_FIRST_SLOT_AUTHENTICATED\n'
    printf 'PREDICTION_MONITOR_FIRST_SLOTS_AUTHENTICATED\n'
    exit 0
  fi
  if (( PROOF_STATUS != 20 )); then
    printf 'PREDICTION_MONITOR_OPERATIONAL_FAILURE\n' >&2
    exit 4
  fi
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
# Lieu: Tabby/VPS Bash. Durée attendue: 2-8 min; maximum borné: 45 min.
# Prompts: une authentification sudo foreground au début; aucun prompt interne.
# Ctrl+C ne supprime aucune preuve, imprime un signal de reprise et peut laisser
# un sous-ensemble des cinq unités Prediction Markets dans un état partiel.
# recovery reprend seulement la nouvelle campagne; rollback-new désarme ses cinq
# unités; restore-old désarme d'abord toute unité nouvelle, puis réarme l'ancienne.
# Signaux: PREDICTION_RECOVERY_RESUME_REQUESTED_NO_SLOT_RETRY,
# PREDICTION_ROLLBACK_DISARMED_RAW_PRESERVED ou
# PREDICTION_OLD_CAMPAIGN_RESTORE_VERIFIED_NO_NEW_COLLECTOR.
set -Eeuo pipefail
MODE=${{1:-}}
SUDO_KEEPALIVE_PID=''
stop_sudo_keepalive() {{
  if [[ -n $SUDO_KEEPALIVE_PID ]]; then
    kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
    wait "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
  fi
}}
interrupted() {{
  trap - HUP INT TERM
  printf 'PREDICTION_E_INTERRUPTED_RETRY_SAME_MODE_NO_EVIDENCE_DELETED:mode=%s\n' "$MODE" >&2
  exit 130
}}
trap interrupted HUP INT TERM
trap stop_sudo_keepalive EXIT
case "$MODE" in recovery|rollback-new|restore-old) ;; *) printf 'usage: bash E-recovery-rollback.sh recovery|rollback-new|restore-old\n' >&2; exit 4 ;; esac
printf 'PREDICTION_SUDO_FOREGROUND_AUTH_BEGIN\n'
if ! sudo -v; then
  printf 'PREDICTION_E_REFUSED:sudo_foreground_authentication_failed\n' >&2
  exit 4
fi
sudo -n true || {{ printf 'PREDICTION_E_REFUSED:sudo_noninteractive_cache_unavailable\n' >&2; exit 4; }}
printf 'PREDICTION_SUDO_FOREGROUND_AUTH_GREEN\n'
(
  SUDO_SLEEP_PID=''
  stop_keepalive_sleep() {{
    [[ -z $SUDO_SLEEP_PID ]] || kill "$SUDO_SLEEP_PID" 2>/dev/null || true
    exit 0
  }}
  trap stop_keepalive_sleep HUP INT TERM
  while kill -0 "$$" 2>/dev/null; do
    sudo -n -v || {{ printf 'PREDICTION_SUDO_KEEPALIVE_FAILED_NO_PROMPT\n' >&2; exit 4; }}
    /usr/bin/sleep 30 </dev/null >/dev/null 2>&1 &
    SUDO_SLEEP_PID=$!
    wait "$SUDO_SLEEP_PID" || exit 0
    SUDO_SLEEP_PID=''
  done
) &
SUDO_KEEPALIVE_PID=$!
case "$MODE" in
  recovery)
    bash '{source}/ops/prediction_markets_launch_v1/rollback.sh' recovery '{incoming}/handoff.json'
    ;;
  rollback-new)
    bash '{source}/ops/prediction_markets_launch_v1/rollback.sh' rollback '{incoming}/handoff.json'
    ;;
  restore-old)
    bash '{incoming}/scripts/cutover.sh' restore-old '{incoming}/handoff.json'
    timeout --signal=TERM --kill-after=5s 240s \
      bash '{incoming}/scripts/cutover.sh' verify-restored '{incoming}/handoff.json'
    printf 'PREDICTION_OLD_CAMPAIGN_RESTORE_VERIFIED_NO_NEW_COLLECTOR\n'
    ;;
esac
stop_sudo_keepalive
SUDO_KEEPALIVE_PID=''
trap - EXIT
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


def _write_shell_new(path: Path, payload: bytes) -> None:
    expected = validate_posix_shell_payload(payload, label=path.as_posix())
    _write_new(path, expected, executable=True)
    materialized = path.read_bytes()
    validate_posix_shell_payload(materialized, label=path.as_posix())
    if materialized != expected:
        raise LaunchPackError(f"POSIX shell payload changed during materialization: {path}")


def _materialize_commit_file(
    *,
    repo_root: Path,
    commit: str,
    relative_path: str,
    output_path: Path,
) -> None:
    payload = _git_blob(repo_root, commit, relative_path)
    if output_path.suffix == ".sh":
        _write_shell_new(output_path, payload)
    else:
        _write_new(output_path, payload)


def _canonical_object(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8", errors="strict"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LaunchPackError(f"invalid canonical JSON object: {label}") from error
    if not isinstance(value, dict) or raw != canonical_json_bytes(value) + b"\n":
        raise LaunchPackError(f"JSON object is not canonical UTF-8/LF: {label}")
    return value, raw


def verify_materialized_transfer(
    incoming_root: Path,
    handoff_path: Path,
) -> dict[str, object]:
    if (
        incoming_root.is_symlink()
        or not incoming_root.is_dir()
        or incoming_root.resolve(strict=True) != incoming_root
        or handoff_path != incoming_root / "handoff.json"
    ):
        raise LaunchPackError("incoming transfer root or handoff path is unsafe")
    handoff, handoff_raw = _canonical_object(handoff_path, label="handoff")
    pin = (incoming_root / "handoff.sha256").read_text(encoding="ascii").strip().split()
    if len(pin) != 2 or pin[1] != "handoff.json" or pin[0] != sha256_bytes(handoff_raw):
        raise LaunchPackError("handoff pin diverged")
    transfer, transfer_raw = _canonical_object(
        incoming_root / "transfer-inventory.json",
        label="transfer inventory",
    )
    if (
        transfer.get("schema_version") != 1
        or sha256_bytes(transfer_raw) != handoff.get("transfer_inventory_sha256")
    ):
        raise LaunchPackError("transfer inventory identity diverged")
    rows = transfer.get("files")
    if not isinstance(rows, list) or not rows:
        raise LaunchPackError("transfer inventory entries are absent")
    seen: set[str] = set()
    shell_paths: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != {"path", "sha256", "size"}:
            raise LaunchPackError("transfer inventory entry schema diverged")
        relative = row.get("path")
        size = row.get("size")
        digest = row.get("sha256")
        if not isinstance(relative, str) or "\\" in relative:
            raise LaunchPackError("transfer inventory path is invalid")
        pure = PurePosixPath(relative)
        if (
            pure.is_absolute()
            or not pure.parts
            or any(part in {"", ".", ".."} for part in pure.parts)
            or relative in seen
        ):
            raise LaunchPackError("transfer inventory path is unsafe or duplicated")
        if type(size) is not int or size < 0 or not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
            raise LaunchPackError(f"transfer inventory metadata is invalid: {relative}")
        seen.add(relative)
        target = incoming_root.joinpath(*pure.parts)
        if target.is_symlink() or not target.is_file() or target.resolve(strict=True) != target:
            raise LaunchPackError(f"transferred file is absent or unsafe: {relative}")
        payload = target.read_bytes()
        if len(payload) != size or sha256_bytes(payload) != digest:
            raise LaunchPackError(f"transferred file identity diverged: {relative}")
        if target.suffix == ".sh":
            validate_posix_shell_payload(payload, label=relative)
            shell_paths.add(relative)
    if shell_paths != set(_EXPECTED_TRANSFERRED_SHELLS):
        raise LaunchPackError("transferred POSIX shell set diverged")
    return {
        "files": len(rows),
        "handoff_sha256": sha256_bytes(handoff_raw),
        "shell_files": len(shell_paths),
        "terminal_signal": "PREDICTION_TRANSFER_SHELLS_LF_AUTHENTICATED",
        "transfer_inventory_sha256": sha256_bytes(transfer_raw),
    }


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
    expected_branch: str = EXPECTED_BRANCH,
) -> dict[str, object]:
    plan = validate_plan(_object(plan_path))
    if _COMMIT.fullmatch(source_commit) is None:
        raise LaunchPackError("source commit is invalid")
    if _git(repo_root, "rev-parse", "HEAD") != source_commit:
        raise LaunchPackError("source HEAD differs from requested final commit")
    _authenticate_source_branch(
        repo_root,
        expected_branch=expected_branch,
        source_commit=source_commit,
    )
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
    for name in _SCRIPTS:
        _materialize_commit_file(
            repo_root=repo_root,
            commit=source_commit,
            relative_path=f"ops/prediction_markets_launch_v1/{name}",
            output_path=output_root / "scripts" / name,
        )
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
        "superseded_campaign": _superseded_campaign_contract(),
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
        path = output_root / "operator" / name
        payload = content.encode("utf-8")
        if name.endswith(".sh"):
            _write_shell_new(path, payload)
        else:
            _write_new(path, payload)
    transfer_paths = [
        bundle_path.name,
        "README.md",
        "source-inventory.json",
        "wheelhouse.sha256",
        *[f"scripts/{name}" for name in _SCRIPTS],
        *[f"systemd/{name}" for name in units],
        *[f"operator/{name}" for name in operator_blocks],
    ]
    materialized_shells = {relative for relative in transfer_paths if relative.endswith(".sh")}
    if materialized_shells != set(_EXPECTED_TRANSFERRED_SHELLS):
        raise LaunchPackError("materialized POSIX shell set diverged")
    for relative in sorted(materialized_shells):
        validate_posix_shell_payload(
            (output_root / relative).read_bytes(),
            label=relative,
        )
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
    final.add_argument("--expected-branch", default=EXPECTED_BRANCH)
    verify = subparsers.add_parser("verify-source")
    verify.add_argument("--source-root", type=Path, required=True)
    verify.add_argument("--inventory", type=Path, required=True)
    verify.add_argument("--expected-commit", required=True)
    verify_transfer = subparsers.add_parser("verify-transfer")
    verify_transfer.add_argument("--incoming-root", type=Path, required=True)
    verify_transfer.add_argument("--handoff", type=Path, required=True)
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
                expected_branch=arguments.expected_branch,
            )
        elif arguments.command == "verify-source":
            result = verify_source(
                arguments.source_root.resolve(strict=True),
                arguments.inventory.resolve(strict=True),
                arguments.expected_commit,
            )
        else:
            result = verify_materialized_transfer(
                arguments.incoming_root.resolve(strict=True),
                arguments.handoff.resolve(strict=True),
            )
        print(canonical_json_bytes(result).decode("utf-8"))
        return 0
    except (LaunchPackError, OSError, subprocess.SubprocessError) as error:
        _fail(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
