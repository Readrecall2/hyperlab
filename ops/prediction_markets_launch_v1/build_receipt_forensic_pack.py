from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
import textwrap
from pathlib import Path

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
EXPECTED_BRANCH = "codex/prediction-markets-prospective-launch-v1"
FAILED_RUN_SLUG = "pm-20260827t131512z-6f59caae"
FAILED_SOURCE_COMMIT = "6f59caae46e7f473cee9dec00103f4157920f8cb"
FORENSIC_SLUG = "receipt-auth-20260827t133835z-6f59caae"
CAMPAIGN_ROOT = (
    "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/campaigns/"
    + FAILED_RUN_SLUG
)
SOURCE_ROOT = (
    "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/sources/"
    + FAILED_RUN_SLUG
)
INCOMING_ROOT = (
    "/home/hyperlab/hyperlab-prediction-markets/incoming/" + FAILED_RUN_SLUG
)
FORENSIC_ROOT = f"{INCOMING_ROOT}/forensics/{FORENSIC_SLUG}"
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class BuildError(RuntimeError):
    """Forensic operator-pack build refusal."""


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


def _git(repo_root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *arguments],
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        raise BuildError(completed.stderr.strip() or "Git command failed")
    return completed.stdout.strip()


def render_tabby_export(tool_raw: bytes) -> str:
    encoded = base64.b64encode(tool_raw).decode("ascii")
    wrapped = "\n".join(textwrap.wrap(encoded, width=76))
    tool_hash = sha256_bytes(tool_raw)
    return f"""#!/usr/bin/env bash
# Lieu: Tabby/VPS sous hyperlab. Durée attendue: <2 min; maximum: 5 min.
# Prompts: aucun. Ctrl+C ne modifie jamais la campagne échouée; une racine
# forensic incomplète reste isolée et ne doit pas être réutilisée.
# Signal terminal: PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_EXPORT_READY_FOR_TRANSFER.
set -Eeuo pipefail
umask 077
CAMPAIGN_ROOT='{CAMPAIGN_ROOT}'
SOURCE_ROOT='{SOURCE_ROOT}'
INCOMING_ROOT='{INCOMING_ROOT}'
FORENSIC_ROOT='{FORENSIC_ROOT}'
FORENSICS_PARENT="$INCOMING_ROOT/forensics"
TOOL_PATH="$FORENSICS_PARENT/.{FORENSIC_SLUG}.py"
[[ $(id -un) == hyperlab ]] || {{ printf 'PREDICTION_FORENSIC_REFUSED:user\n' >&2; exit 4; }}
[[ -d "$CAMPAIGN_ROOT" && ! -L "$CAMPAIGN_ROOT" ]] || {{ printf 'PREDICTION_FORENSIC_REFUSED:campaign_root\n' >&2; exit 4; }}
[[ -d "$SOURCE_ROOT" && ! -L "$SOURCE_ROOT" ]] || {{ printf 'PREDICTION_FORENSIC_REFUSED:source_root\n' >&2; exit 4; }}
[[ -d "$INCOMING_ROOT" && ! -L "$INCOMING_ROOT" ]] || {{ printf 'PREDICTION_FORENSIC_REFUSED:incoming_root\n' >&2; exit 4; }}
[[ ! -e "$FORENSIC_ROOT" ]] || {{ printf 'PREDICTION_FORENSIC_REFUSED:forensic_root_must_be_new\n' >&2; exit 4; }}
if [[ -e "$FORENSICS_PARENT" || -L "$FORENSICS_PARENT" ]]; then
  [[ -d "$FORENSICS_PARENT" && ! -L "$FORENSICS_PARENT" ]] || {{ printf 'PREDICTION_FORENSIC_REFUSED:forensics_parent\n' >&2; exit 4; }}
else
  install -d -m 0700 "$FORENSICS_PARENT"
fi
[[ ! -e "$TOOL_PATH" ]] || {{ printf 'PREDICTION_FORENSIC_REFUSED:tool_path_must_be_new\n' >&2; exit 4; }}
cleanup() {{
  status=$?
  trap - EXIT HUP INT TERM
  case "$TOOL_PATH" in
    "$FORENSICS_PARENT"/.{FORENSIC_SLUG}.py) rm -f -- "$TOOL_PATH" ;;
    *) printf 'PREDICTION_FORENSIC_TOOL_CLEANUP_REFUSED\n' >&2; exit 4 ;;
  esac
  exit "$status"
}}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
base64 -d > "$TOOL_PATH" <<'HYPERLAB_PM_FORENSIC_TOOL_BASE64'
{wrapped}
HYPERLAB_PM_FORENSIC_TOOL_BASE64
chmod 0600 "$TOOL_PATH"
printf '%s  %s\n' '{tool_hash}' "$TOOL_PATH" | sha256sum -c -
python3.12 -I "$TOOL_PATH" export \
  --campaign-root "$CAMPAIGN_ROOT" \
  --incoming-root "$INCOMING_ROOT" \
  --source-root "$SOURCE_ROOT" \
  --output-root "$FORENSIC_ROOT" \
  --expected-source-commit '{FAILED_SOURCE_COMMIT}'
(cd "$FORENSIC_ROOT" && sha256sum -c forensic-inventory.json.sha256)
(cd "$FORENSIC_ROOT" && sha256sum -c receipt-auth-forensic.tar.sha256)
printf 'PREDICTION_FORENSIC_ROOT=%s\n' "$FORENSIC_ROOT"
printf 'PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_EXPORT_READY_FOR_TRANSFER\n'
"""


def render_windows_fetch() -> str:
    return f"""# Lieu: Windows PowerShell local. Durée attendue: <2 min; maximum: 10 min.
# Prompts: SSH host-key/password possibles. Ctrl+C interrompt uniquement la copie.
# Signal terminal: PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_FETCHED.
$ErrorActionPreference = 'Stop'
$SshTarget = $env:HYPERLAB_PM_SSH_TARGET
if ([string]::IsNullOrWhiteSpace($SshTarget) -or $SshTarget -notmatch '^[a-z0-9._-]+@[a-z0-9.-]+$') {{ throw 'Set HYPERLAB_PM_SSH_TARGET to user@host.' }}
$SshKeyRaw = $env:HYPERLAB_PM_SSH_KEY
if ([string]::IsNullOrWhiteSpace($SshKeyRaw)) {{ throw 'Set HYPERLAB_PM_SSH_KEY.' }}
$SshKey = (Resolve-Path -LiteralPath $SshKeyRaw).Path
if (-not (Test-Path -LiteralPath $SshKey -PathType Leaf)) {{ throw 'SSH key is not a regular file.' }}
$LocalRootRaw = $env:HYPERLAB_PM_FORENSIC_LOCAL_ROOT
if ([string]::IsNullOrWhiteSpace($LocalRootRaw)) {{ throw 'Set HYPERLAB_PM_FORENSIC_LOCAL_ROOT to a new local directory.' }}
$LocalRoot = [IO.Path]::GetFullPath($LocalRootRaw)
if (Test-Path -LiteralPath $LocalRoot) {{ throw 'Local forensic root must be new.' }}
New-Item -ItemType Directory -Path $LocalRoot | Out-Null
$RemoteRoot = '{FORENSIC_ROOT}'
$Names = @('forensic-scope.json','forensic-inventory.json','forensic-inventory.json.sha256','receipt-auth-forensic.tar','receipt-auth-forensic.tar.sha256')
foreach ($Name in $Names) {{
    scp -i $SshKey -- "${{SshTarget}}:${{RemoteRoot}}/${{Name}}" (Join-Path $LocalRoot $Name)
    if ($LASTEXITCODE -ne 0) {{ throw "Forensic transfer failed: $Name" }}
}}
function Get-Sha256Hex {{
    param([string] $Path)
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}}
function Assert-Pin {{
    param([string] $PinName, [string] $TargetName)
    $Line = (Get-Content -LiteralPath (Join-Path $LocalRoot $PinName) -Raw).Trim()
    if ($Line -notmatch '^([0-9a-f]{{64}})  ([^/\\]+)$' -or $Matches[2] -cne $TargetName) {{ throw "Malformed pin: $PinName" }}
    if ((Get-Sha256Hex (Join-Path $LocalRoot $TargetName)) -cne $Matches[1]) {{ throw "SHA-256 diverged: $TargetName" }}
}}
Assert-Pin 'forensic-inventory.json.sha256' 'forensic-inventory.json'
Assert-Pin 'receipt-auth-forensic.tar.sha256' 'receipt-auth-forensic.tar'
$Inventory = Get-Content -LiteralPath (Join-Path $LocalRoot 'forensic-inventory.json') -Raw | ConvertFrom-Json
if ($Inventory.source_commit -cne '{FAILED_SOURCE_COMMIT}' -or $Inventory.failed_campaign_root -cne '{CAMPAIGN_ROOT}') {{ throw 'Forensic identity diverged.' }}
$ScopeEntry = @($Inventory.files | Where-Object {{ $_.path -ceq 'forensic-scope.json' }})
if ($ScopeEntry.Count -ne 1 -or (Get-Sha256Hex (Join-Path $LocalRoot 'forensic-scope.json')) -cne $ScopeEntry[0].sha256) {{ throw 'Forensic scope SHA-256 diverged.' }}
Write-Output "PREDICTION_FORENSIC_LOCAL_ROOT=$LocalRoot"
Write-Output 'PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_FETCHED'
"""


def render_windows_diagnose() -> str:
    return rf"""# Lieu: Windows PowerShell local. Durée attendue: <1 min; maximum: 5 min.
# Aucun réseau, prompt ou écriture dans les preuves. Le diagnostic est imprimé sur stdout.
$ErrorActionPreference = 'Stop'
$PackRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$Tool = (Resolve-Path -LiteralPath (Join-Path $PackRoot 'tools\receipt_auth_forensics.py')).Path
$BundleRootRaw = $env:HYPERLAB_PM_FORENSIC_LOCAL_ROOT
if ([string]::IsNullOrWhiteSpace($BundleRootRaw)) {{ throw 'Set HYPERLAB_PM_FORENSIC_LOCAL_ROOT.' }}
$BundleRoot = (Resolve-Path -LiteralPath $BundleRootRaw).Path
$Python = 'C:\Dev\hyperlab-multistrategy\.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {{ throw 'Canonical local Python is absent.' }}
& $Python -I $Tool diagnose --bundle-root $BundleRoot --expected-source-commit '{FAILED_SOURCE_COMMIT}'
if ($LASTEXITCODE -ne 0) {{ throw 'Offline forensic diagnosis refused the bundle.' }}
"""


def render_readme(tool_commit: str, tool_sha256: str) -> str:
    return f"""# Prediction Markets receipt-auth forensic export

Verdict avant réception: `PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_EXPORT_REQUIRED`.

Ce pack ne lance aucun collector, probe, recovery, service ou replay. Le bloc F lit
uniquement les fichiers allowlistés de la campagne échouée `{FAILED_RUN_SLUG}` et
écrit une nouvelle racine sous son incoming root. Les segments raw ne sont ni lus
ni copiés. Le bloc G rapatrie cinq fichiers exacts avec la clé SSH explicite. Le
bloc H diagnostique offline la première divergence du résultat sans extraire ni
modifier les preuves.

- frontière: `{BOUNDARY}`
- failed source commit: `{FAILED_SOURCE_COMMIT}`
- forensic tool commit: `{tool_commit}`
- embedded tool SHA-256: `{tool_sha256}`
- remote forensic root: `{FORENSIC_ROOT}`
- terminal export: `PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_EXPORT_READY_FOR_TRANSFER`
- terminal fetch: `PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_FETCHED`
"""


def build_pack(*, repo_root: Path, output_root: Path, tool_commit: str) -> dict[str, object]:
    repo = repo_root.resolve(strict=True)
    if _COMMIT.fullmatch(tool_commit) is None:
        raise BuildError("tool commit is invalid")
    if _git(repo, "rev-parse", "HEAD") != tool_commit:
        raise BuildError("HEAD differs from the requested forensic tool commit")
    if _git(repo, "branch", "--show-current") != EXPECTED_BRANCH:
        raise BuildError("forensic tool branch diverged")
    if _git(repo, "status", "--porcelain"):
        raise BuildError("forensic tool worktree must be clean")
    output = output_root.absolute()
    if output.exists():
        raise BuildError("forensic pack output root must be new")
    output.mkdir(parents=True)
    (output / "operator").mkdir()
    (output / "tools").mkdir()
    tool_source = repo / "ops" / "prediction_markets_launch_v1" / "receipt_auth_forensics.py"
    tool_raw = tool_source.read_bytes()
    files = {
        "operator/F-tabby-export-receipt-auth-forensic.sh": render_tabby_export(tool_raw).encode(),
        "operator/G-windows-fetch-receipt-auth-forensic.ps1": render_windows_fetch().encode(),
        "operator/H-windows-offline-diagnose.ps1": render_windows_diagnose().encode(),
        "tools/receipt_auth_forensics.py": tool_raw,
    }
    readme = render_readme(tool_commit, sha256_bytes(tool_raw)).encode()
    files["README.md"] = readme
    for relative, raw in files.items():
        path = output.joinpath(*_path_parts(relative))
        path.write_bytes(raw)
        if path.suffix == ".sh":
            os.chmod(path, 0o700)
    inventory_body = {
        "boundary": BOUNDARY,
        "failed_campaign_root": CAMPAIGN_ROOT,
        "failed_source_commit": FAILED_SOURCE_COMMIT,
        "files": [
            {"path": path, "sha256": sha256_bytes(raw), "size": len(raw)}
            for path, raw in sorted(files.items())
        ],
        "forensic_root": FORENSIC_ROOT,
        "schema_version": 1,
        "tool_commit": tool_commit,
    }
    inventory = {
        **inventory_body,
        "inventory_sha256": sha256_bytes(canonical_json_bytes(inventory_body)),
    }
    inventory_raw = canonical_json_bytes(inventory) + b"\n"
    (output / "tool-inventory.json").write_bytes(inventory_raw)
    (output / "tool-inventory.json.sha256").write_text(
        f"{sha256_bytes(inventory_raw)}  tool-inventory.json\n",
        encoding="ascii",
    )
    return inventory


def _path_parts(value: str) -> tuple[str, ...]:
    parts = tuple(value.split("/"))
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise BuildError("unsafe generated forensic pack path")
    return parts


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build one immutable receipt-auth forensic pack")
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--tool-commit", required=True)
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    result = build_pack(
        repo_root=arguments.repo_root,
        output_root=arguments.output_root,
        tool_commit=arguments.tool_commit,
    )
    print(canonical_json_bytes(result).decode())
    print("PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_TOOL_PACK_GREEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
